{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE ViewPatterns #-}
{-# OPTIONS_GHC -Wno-deprecations #-}

{- | The meat of the Legion runtime implementation. -}
module OM.Legion.Runtime (
  -- * Starting the framework runtime.
  forkLegionary,
  Runtime,
  StartupMode(..),

  -- * Runtime Interface
  applyFast,
  applyConsistent,
  readState,
  call,
  cast,
  broadcall,
  broadcast,
  eject,
  getSelf,
  getAsync,

  -- * Other types
  ClusterId,
  Peer,
) where


import Control.Concurrent (Chan, writeChan, newChan, threadDelay,
   readChan)
import Control.Concurrent.Async (Async, async, race_)
import Control.Concurrent.STM (TVar, atomically, newTVar, writeTVar,
   readTVar, retry)
import Control.Exception.Safe (MonadCatch, tryAny, finally)
import Control.Monad (void, join, when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Logger (MonadLoggerIO, logDebug, logInfo, logWarn,
   logError, askLoggerIO, runLoggingT, LoggingT, LogStr)
import Control.Monad.Morph (hoist)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except (runExceptT)
import Control.Monad.Trans.State (evalStateT, StateT, get, put, modify)
import Data.Aeson (ToJSON, toJSON, ToJSONKey, toJSONKey,
   ToJSONKeyFunction(ToJSONKeyText))
import Data.Aeson.Encoding (text)
import Data.Binary (Binary, Word64)
import Data.ByteString.Lazy (ByteString)
import Data.Conduit (runConduit, (.|), awaitForever, Source, yield)
import Data.Default.Class (Default)
import Data.Map (Map)
import Data.Monoid ((<>))
import Data.Set (Set, (\\))
import Data.UUID (UUID)
import Data.Void (Void)
import GHC.Generics (Generic)
import OM.Fork (Actor, actorChan, Msg, Responder, respond)
import OM.Legion.Conduit (chanToSink)
import OM.Legion.UUID (getUUID)
import OM.Logging (withPrefix)
import OM.PowerState (PowerState, Event, StateId, projParticipants,
   EventPack, events, Output, State, infimumId)
import OM.PowerState.Monad (event, acknowledge, runPowerStateT, merge,
   PowerStateT, disassociate, participate)
import OM.Show (showt)
import OM.Socket (connectServer, bindAddr,
   AddressDescription(AddressDescription), openEgress, Endpoint(Endpoint),
   openIngress, openServer)
import System.Random.Shuffle (shuffleM)
import Web.HttpApiData (FromHttpApiData, parseUrlPiece)
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.Text as T
import qualified Data.UUID as UUID
import qualified OM.Fork as Fork
import qualified OM.PowerState as PS


{- | The Legionary runtime state. -}
data RuntimeState s = RuntimeState {
            self :: Peer,
    clusterState :: PowerState ClusterId Peer s,
     connections :: Map
                      Peer
                      (PeerMessage s -> StateT (RuntimeState s) IO ()),
         waiting :: Map (StateId Peer) (Responder (Output s)),
           calls :: Map MessageId (Responder ByteString),
      broadcalls :: Map
                      MessageId
                      (
                        Map Peer (Maybe ByteString),
                        Responder (Map Peer ByteString)
                      ),
          nextId :: MessageId,
          notify :: PowerState ClusterId Peer s -> IO (),
           joins :: Map
                      (StateId Peer)
                      (Responder (JoinResponse s), Peer)
                    {- ^
                      The infimum of the powerstate we send to a new
                      participant must have moved past the participation
                      event itself. In other words, the join must be
                      totally consistent across the cluster. The reason is
                      that we can't make the new participant responsible
                      for applying events that occur before it joined
                      the cluster, because it has no way to ensure
                      that it can collect all such events.  Therefore,
                      this field tracks the outstanding joins until they
                      become consistent.
                    -}
  }


{- | Fork the Legion runtime system. -}
forkLegionary :: (
      Binary s, Binary (Event s), Default s, Eq (Event s), State s,
      MonadCatch m, MonadLoggerIO m, Show (Event s), ToJSON s, ToJSON
      (Event s)
    )
  => Endpoint
     {- ^
       The address on which the legion framework will listen for
       rebalancing and cluster management commands.
     -}
  -> Endpoint
     {- ^
       The address on which the legion framework will listen for cluster
       join requests.
     -}
  -> (ByteString -> IO ByteString)
     {- ^ Handle a user call request.  -}
  -> (ByteString -> IO ())
     {- ^ Handle a user cast message. -}
  -> (Peer -> PowerState ClusterId Peer s -> IO ())
     {- ^ Callback when the cluster-wide powerstate changes. -}
  -> StartupMode s
     {- ^
       How to start the runtime, by creating new cluster or joining an
       existing cluster.
     -}
  -> m (Runtime s)
forkLegionary
    peerBindAddr
    joinBindAddr
    handleUserCall
    handleUserCast
    notify
    startupMode
  = do
    rts <- makeRuntimeState peerBindAddr notify startupMode
    runtimeChan <- RChan <$> liftIO newChan
    logging <- withPrefix (logPrefix (self rts)) <$> askLoggerIO
    asyncHandle <- liftIO . async . (`runLoggingT` logging) $
      executeRuntime
        joinBindAddr
        handleUserCall
        handleUserCast
        rts
        runtimeChan
    return (Runtime runtimeChan (self rts) asyncHandle)
  where
    logPrefix :: Peer -> LogStr
    logPrefix self_ = "[" <> showt self_ <> "]"


{- | A handle on the Legion runtime. -}
data Runtime s = Runtime {
     rChan :: RChan s,
     rSelf :: Peer,
    rAsync :: Async Void
  }
instance Actor (Runtime s) where
  type Msg (Runtime s) = RuntimeMessage s
  actorChan = actorChan . rChan


{- | The type of the runtime message channel. -}
newtype RChan s = RChan {
    unRChan :: Chan (RuntimeMessage s)
  }
instance Actor (RChan s) where
  type Msg (RChan s) = RuntimeMessage s
  actorChan = writeChan . unRChan


{- |
  Update the distributed cluster state by applying an event. The event
  output will be returned immediately and may not reflect a totally
  consistent view of the cluster. The state update itself, however,
  is guaranteed to be applied atomically and consistently throughout
  the cluster.
-}
applyFast :: (MonadIO m)
  => Runtime s     {- ^ The runtime handle. -}
  -> Event s       {- ^ The event to be applied. -}
  -> m (Output s)  {- ^ Returns the possibly inconsistent event output. -}
applyFast runtime e = Fork.call runtime (ApplyFast e)


{- |
  Update the distributed cluster state by applying an event. Both the
  event output and resulting state will be totally consistent throughout
  the cluster.
-}
applyConsistent :: (MonadIO m)
  => Runtime s     {- ^ The runtime handle. -}
  -> Event s       {- ^ The event to be applied. -}
  -> m (Output s)  {- ^ Returns the strongly consistent event output. -}
applyConsistent runtime e = Fork.call runtime (ApplyConsistent e)


{- | Read the current powerstate value. -}
readState :: (MonadIO m)
  => Runtime s
  -> m (PowerState ClusterId Peer s)
readState runtime = Fork.call runtime ReadState


{- |
  Send a user message to some other peer, and block until a response
  is received.
-}
call :: (MonadIO m) => Runtime s -> Peer -> ByteString -> m ByteString
call runtime target msg = Fork.call runtime (Call target msg)


{- | Send the result of a call back to the peer that originated it. -}
sendCallResponse :: (MonadIO m)
  => RChan s
  -> Peer
  -> MessageId
  -> ByteString
  -> m ()
sendCallResponse runtimeChan target mid msg =
  Fork.cast runtimeChan (SendCallResponse target mid msg)


{- | Send a user message to some other peer, without waiting on a response. -}
cast :: (MonadIO m) => Runtime s -> Peer -> ByteString -> m ()
cast runtime target message = Fork.cast runtime (Cast target message)


{- |
  Send a user message to all peers, and block until a response is received
  from all of them.
-}
broadcall :: (MonadIO m)
  => Runtime s
  -> ByteString
  -> m (Map Peer ByteString)
broadcall runtime msg = Fork.call runtime (Broadcall msg)


{- | Send a user message to all peers, without wating on a response. -}
broadcast :: (MonadIO m) => Runtime s -> ByteString -> m ()
broadcast runtime msg = Fork.cast runtime (Broadcast msg)


{- | Eject a peer from the cluster. -}
eject :: (MonadIO m) => Runtime s -> Peer -> m ()
eject runtime peer = Fork.cast runtime (Eject peer)


{- | Get the identifier for the local peer. -}
getSelf :: Runtime s -> Peer
getSelf = rSelf


{- |
  Get the async handle on the background legion thread, in case you want
  to wait for it to complete (which should never happen except in the
  case of an error).
-}
getAsync :: Runtime s -> Async Void
getAsync = rAsync


{- | The types of messages that can be sent to the runtime. -}
data RuntimeMessage s
  = ApplyFast (Event s) (Responder (Output s))
  | ApplyConsistent (Event s) (Responder (Output s))
  | Eject Peer
  | Merge (EventPack ClusterId Peer s)
  | Join JoinRequest (Responder (JoinResponse s))
  | ReadState (Responder (PowerState ClusterId Peer s))
  | Call Peer ByteString (Responder ByteString)
  | Cast Peer ByteString
  | Broadcall ByteString (Responder (Map Peer ByteString))
  | Broadcast ByteString
  | SendCallResponse Peer MessageId ByteString
  | HandleCallResponse Peer MessageId ByteString
  | Resend (Responder ())
deriving instance (Show (Event s)) => Show (RuntimeMessage s)


{- | The types of messages that can be sent from one peer to another. -}
data PeerMessage s
  = PMMerge (EventPack ClusterId Peer s)
    {- ^ Send a powerstate merge. -}
  | PMCall Peer MessageId ByteString
    {- ^ Send a user call message from one peer to another. -}
  | PMCast ByteString
    {- ^ Send a user cast message from one peer to another. -}
  | PMCallResponse Peer MessageId ByteString
    {- ^ Send a response to a user call message. -}
  deriving (Generic)
deriving instance (Show (Event s)) => Show (PeerMessage s)
instance (Binary (Event s)) => Binary (PeerMessage s)


{- | An opaque value that identifies a cluster participant. -}
data Peer = Peer {
      peerId :: UUID,
    peerAddy :: AddressDescription
  }
  deriving (Generic, Eq, Ord)
instance Show Peer where
  show peer = show (peerId peer) ++ ":" ++ show (peerAddy peer)
instance ToJSONKey Peer where
  toJSONKey = ToJSONKeyText showt (text . showt)
instance ToJSON Peer where
  toJSON = toJSON . show
instance Binary Peer
instance FromHttpApiData Peer where
  parseUrlPiece str =
    case T.span (/= ':') str of
      (UUID.fromText -> Just uuid, T.span (== ':') -> (_, addr)) ->
        Right (Peer uuid (AddressDescription addr))
      _ -> Left $ "Can't parse peer: " <> showt str


{- | An opaque value that identifies a cluster. -}
newtype ClusterId = ClusterId UUID
  deriving (Binary, Show, Eq, ToJSON)


{- |
  Execute the Legion runtime, with the given user definitions, and
  framework settings. This function never returns (except maybe with an
  exception if something goes horribly wrong).
-}
executeRuntime :: (
      Constraints s, MonadCatch m, MonadLoggerIO m
    )
  => Endpoint
     {- ^
       The address on which the legion framework will listen for cluster
       join requests.
     -}
  -> (ByteString -> IO ByteString)
     {- ^ Handle a user call request.  -}
  -> (ByteString -> IO ())
     {- ^ Handle a user cast message. -}
  -> RuntimeState s
  -> RChan s
    {- ^ A source of requests, together with a way to respond to the requets. -}
  -> m Void
executeRuntime
    joinBindAddr
    handleUserCall
    handleUserCast
    rts
    runtimeChan
  = do
    {- Start the various messages sources. -}
    runPeerListener
      `raceLog_` runJoinListener
      `raceLog_` runPeriodicResent
      `raceLog_`
        (
          (`evalStateT` rts) $
            let
              -- handleMessages :: StateT (RuntimeState e3) m Void
              handleMessages = do
                msg <- liftIO $ readChan (unRChan runtimeChan)
                RuntimeState {clusterState = cluster1} <- get
                handleRuntimeMessage msg
                RuntimeState {clusterState = cluster2} <- get
                when (cluster1 /= cluster2)
                  ($(logDebug) $ "New Cluster State: " <> showt cluster2)
                handleJoins
                handleMessages
            in
              handleMessages
        )
    fail "Legion runtime stopped."
  where
    {- | Like 'race_', but with logging. -}
    raceLog_ :: (MonadLoggerIO m) => LoggingT IO a -> LoggingT IO b -> m ()
    raceLog_ a b = do
      logging <- askLoggerIO
      liftIO $ race_ (runLoggingT a logging) (runLoggingT b logging)

    runPeerListener :: (MonadLoggerIO m) => m ()
    runPeerListener =
      runConduit (
        openIngress (Endpoint (peerAddy (self rts)) Nothing)
        .| awaitForever (\ (msgSource, msg) -> do
            $(logDebug) $ "Handling: " <> showt (msgSource :: Peer, msg)
            case msg of
              PMMerge ps -> yield (Merge ps)
              PMCall source mid callMsg -> liftIO $
                sendCallResponse runtimeChan source mid
                =<< handleUserCall callMsg
              PMCast castMsg -> liftIO (handleUserCast castMsg)
              PMCallResponse source mid responseMsg ->
                yield (HandleCallResponse source mid responseMsg)
           )
        .| chanToSink (unRChan runtimeChan)
      )

    runJoinListener :: (MonadLoggerIO m) => m ()
    runJoinListener =
      runConduit (
        openServer joinBindAddr
        .| awaitForever (\(req, respond_) -> lift $
             Fork.call runtimeChan (Join req) >>= respond_
           )
      )

    runPeriodicResent :: (MonadIO m) => m ()
    runPeriodicResent =
      let
        periodicResend :: (MonadIO m) => m ()
        periodicResend = do
          liftIO $ threadDelay 100000
          Fork.call runtimeChan Resend
          periodicResend
      in
        periodicResend


{- | Handle any outstanding joins. -}
handleJoins :: (MonadIO m) => StateT (RuntimeState s) m ()
handleJoins = do
  state@RuntimeState {joins, clusterState} <- get
  let
    (consistent, pending) =
      Map.partitionWithKey
        (\k _ -> k <= infimumId clusterState)
        joins
  put state {joins = pending}
  sequence_ [
      respond responder (JoinOk peer clusterState)
      | (_, (responder, peer)) <- Map.toList consistent
    ]


{- | Execute the incoming messages. -}
handleRuntimeMessage :: (
      Constraints s, MonadCatch m, MonadLoggerIO m
    )
  => RuntimeMessage s
  -> StateT (RuntimeState s) m ()

handleRuntimeMessage (ApplyFast e responder) =
  updateCluster $ do
    (o, _sid) <- event e
    respond responder o

handleRuntimeMessage (ApplyConsistent e responder) =
  updateCluster $ do
    (_v, sid) <- event e
    lift (waitOn sid responder)

handleRuntimeMessage (Eject peer) =
  updateClusterAs peer $ disassociate peer

handleRuntimeMessage (Merge other) =
  updateCluster $
    runExceptT (merge other) >>= \case
      Left err -> $(logError) $ "Bad cluster merge: " <> showt err
      Right () -> return ()

handleRuntimeMessage (Join (JoinRequest addr) responder) = do
  peer <- newPeer addr
  sid <- updateCluster (participate peer)
  RuntimeState {clusterState} <- get
  if sid <= infimumId clusterState
    then respond responder (JoinOk peer clusterState)
    else modify (\s -> s {joins = Map.insert sid (responder, peer) (joins s)})

handleRuntimeMessage (ReadState responder) =
  respond responder . clusterState =<< get

handleRuntimeMessage (Call target msg responder) = do
    mid <- newMessageId
    source <- self <$> get
    setCallResponder mid
    sendPeer (PMCall source mid msg) target
  where
    setCallResponder :: (Monad m)
      => MessageId
      -> StateT (RuntimeState e) m ()
    setCallResponder mid = do
      state@RuntimeState {calls} <- get
      put state {
          calls = Map.insert mid responder calls
        }

handleRuntimeMessage (Cast target msg) =
  sendPeer (PMCast msg) target

handleRuntimeMessage (Broadcall msg responder) = do
    mid <- newMessageId
    source <- self <$> get
    setBroadcallResponder mid
    mapM_ (sendPeer (PMCall source mid msg)) =<< getPeers
  where
    setBroadcallResponder :: (Monad m)
      => MessageId
      -> StateT (RuntimeState s) m ()
    setBroadcallResponder mid = do
      peers <- getPeers
      state@RuntimeState {broadcalls} <- get
      put state {
          broadcalls =
            Map.insert
              mid
              (
                Map.fromList [(peer, Nothing) | peer <- Set.toList peers],
                responder
              )
              broadcalls
        }

handleRuntimeMessage (Broadcast msg) =
  mapM_ (sendPeer (PMCast msg)) =<< getPeers

handleRuntimeMessage (SendCallResponse target mid msg) = do
  source <- self <$> get
  sendPeer (PMCallResponse source mid msg) target

handleRuntimeMessage (HandleCallResponse source mid msg) = do
  state@RuntimeState {calls, broadcalls} <- get
  case Map.lookup mid calls of
    Nothing ->
      case Map.lookup mid broadcalls of
        Nothing -> return ()
        Just (responses, responder) ->
          let
            responses2 = Map.insert source (Just msg) responses
            response = Map.fromList [
                (peer, r)
                | (peer, Just r) <- Map.toList responses2
              ]
            peers = Map.keysSet responses2
          in
            if Set.null (Map.keysSet response \\ peers)
              then do
                respond responder response
                put state {
                    broadcalls = Map.delete mid broadcalls
                  }
              else
                put state {
                    broadcalls =
                      Map.insert mid (responses2, responder) broadcalls
                  }
    Just responder -> do
      respond responder msg
      put state {calls = Map.delete mid calls}

handleRuntimeMessage (Resend responder) =
  propagate >>= respond responder


{- | Get the projected peers. -}
getPeers :: (Monad m) => StateT (RuntimeState s) m (Set Peer)
getPeers = projParticipants . clusterState <$> get


{- | Get a new messageId. -}
newMessageId :: (Monad m) => StateT (RuntimeState s) m MessageId
newMessageId = do
  state@RuntimeState {nextId} <- get
  put state {nextId = nextMessageId nextId}
  return nextId


{- |
  Like 'runPowerStateT', plus automatically take care of doing necessary
  IO implied by the cluster update.
-}
updateCluster :: (
      Constraints s, MonadCatch m, MonadLoggerIO m
    )
  => PowerStateT ClusterId Peer s (StateT (RuntimeState s) m) a
  -> StateT (RuntimeState s) m a
updateCluster action = do
  RuntimeState {self} <- get
  updateClusterAs self action


{- |
  Like 'updateCluster', but perform the operation on behalf of a specified
  peer. This is required for e.g. the peer eject case, when the ejected peer
  may not be able to perform acknowledgements on its own behalf.
-}
updateClusterAs :: (
      Constraints s, MonadCatch m, MonadLoggerIO m
    )
  => Peer
  -> PowerStateT ClusterId Peer s (StateT (RuntimeState s) m) a
  -> StateT (RuntimeState s) m a
updateClusterAs asPeer action = do
  state@RuntimeState {clusterState} <- get
  runPowerStateT asPeer clusterState (action <* acknowledge) >>=
    \(v, _propAction, newClusterState, infs) -> do
      liftIO . ($ newClusterState) . notify =<< get
      put state {clusterState = newClusterState}
      respondToWaiting infs
      return v


{- | Wait on a consistent response for the given state id. -}
waitOn :: (Monad m)
  => StateId Peer
  -> Responder (Output s)
  -> StateT (RuntimeState s) m ()
waitOn sid responder =
  modify (\state@RuntimeState {waiting} -> state {
    waiting = Map.insert sid responder waiting
  })


{- | Propagates cluster information if necessary. -}
propagate :: (Constraints s, MonadCatch m, MonadLoggerIO m)
  => StateT (RuntimeState s) m ()
propagate = do
    RuntimeState {self, clusterState} <- get
    let
      targets = Set.delete self $
        PS.allParticipants clusterState

    liftIO (shuffleM (Set.toList targets)) >>= \case
      [] -> return ()
      target:_ -> sendPeer (PMMerge (events clusterState)) target
    disconnectObsolete
  where
    {- |
      Shut down connections to peers that are no longer participating
      in the cluster.
    -}
    disconnectObsolete :: (MonadIO m) => StateT (RuntimeState s) m ()
    disconnectObsolete = do
        RuntimeState {clusterState, connections} <- get
        mapM_ disconnect $
          PS.allParticipants clusterState \\ Map.keysSet connections


{- | Send a peer message, creating a new connection if need be. -}
sendPeer :: (Constraints s, MonadCatch m, MonadLoggerIO m)
  => PeerMessage s
  -> Peer
  -> StateT (RuntimeState s) m ()
sendPeer msg peer = do
  state@RuntimeState {connections} <- get
  case Map.lookup peer connections of
    Nothing -> do
      conn <- createConnection peer
      put state {connections = Map.insert peer conn connections}
      sendPeer msg peer
    Just conn ->
      (hoist liftIO . tryAny) (conn msg) >>= \case
        Left err -> do
          $(logWarn) $ "Failure sending to peer: " <> showt (peer, err)
          disconnect peer
        Right () -> return ()


{- | Disconnect the connection to a peer. -}
disconnect :: (MonadIO m) => Peer -> StateT (RuntimeState s) m ()
disconnect peer =
  modify (\state@RuntimeState {connections} -> state {
    connections = Map.delete peer connections
  })


{- | Create a connection to a peer. -}
createConnection :: (Constraints s, MonadCatch m, MonadLoggerIO m)
  => Peer
  -> StateT (RuntimeState s) m (PeerMessage s -> StateT (RuntimeState s) IO ())
createConnection peer = do
    RuntimeState {self} <- get
    latest <- liftIO $ atomically (newTVar (Just []))
    logging <- askLoggerIO
    liftIO . void . async . (`runLoggingT` logging) $
      finally 
        (
          (tryAny . runConduit) (
            latestSource self latest .| openEgress (peerAddy peer)
          )
        )
        (liftIO $ atomically (writeTVar latest Nothing))
      
    return (\msg ->
        join . liftIO . atomically $
          readTVar latest >>= \case
            Nothing ->
              return $ modify (\state -> state {
                  connections = Map.delete peer (connections state)
                })
            Just msgs -> do
              writeTVar latest (
                  Just $ case msg of
                    PMMerge _ ->
                      msg : filter (\case {PMMerge _ -> False; _ -> True}) msgs
                    _ -> msgs ++ [msg] 
                )
              return (return ())
      )
  where
    latestSource :: (MonadIO m)
      => Peer
      -> TVar (Maybe [PeerMessage s])
      -> Source m (Peer, PeerMessage s)
    latestSource self_ latest =
      (liftIO . atomically) (
        readTVar latest >>= \case
          Nothing -> return Nothing
          Just [] -> retry
          Just messages -> do
            writeTVar latest (Just [])
            return (Just messages)
      ) >>= \case
        Nothing -> return ()
        Just messages -> do
          mapM_ (yield . (self_,)) messages
          latestSource self_ latest


{- |
  Respond to event applications that are waiting on a consistent result,
  if such a result is available.
-}
respondToWaiting :: (MonadIO m)
  => Map (StateId Peer) (Output s)
  -> StateT (RuntimeState s) m ()
respondToWaiting available =
    mapM_ respondToOne (Map.toList available)
  where
    respondToOne :: (MonadIO m)
      => (StateId Peer, Output s)
      -> StateT (RuntimeState s) m ()
    respondToOne (sid, o) = do
      state@RuntimeState {waiting} <- get
      case Map.lookup sid waiting of
        Nothing -> return ()
        Just responder -> do
          respond responder o
          put state {waiting = Map.delete sid waiting}


{- | This defines the various ways a node can be spun up. -}
data StartupMode s
  = NewCluster
    {- ^ Indicates that we should bootstrap a new cluster at startup. -}
  | JoinCluster AddressDescription
    {- ^ Indicates that the node should try to join an existing cluster. -}
  | Recover Peer (PowerState ClusterId Peer s)
deriving instance (ToJSON s, ToJSON (Event s)) => Show (StartupMode s)


{- | Initialize the runtime state. -}
makeRuntimeState :: (Constraints s, MonadLoggerIO m)
  => Endpoint
  -> (Peer -> PowerState ClusterId Peer s -> IO ())
     {- ^ Callback when the cluster-wide powerstate changes. -}
  -> StartupMode s
  -> m (RuntimeState s)

makeRuntimeState
    peerBindAddr
    notify
    NewCluster
  = do
    {- Build a brand new node state, for the first node in a cluster. -}
    self <- newPeer (bindAddr peerBindAddr)
    clusterId <- ClusterId <$> getUUID
    makeRuntimeState
      peerBindAddr
      notify
      (Recover self (PS.new clusterId (Set.singleton self)))

makeRuntimeState
    peerBindAddr
    notify
    (JoinCluster addr)
  = do
    {- Join a cluster an existing cluster. -}
    $(logInfo) "Trying to join an existing cluster."
    JoinOk self cluster <-
      requestJoin
      . JoinRequest
      . bindAddr
      $ peerBindAddr
    makeRuntimeState peerBindAddr notify (Recover self cluster)
  where
    requestJoin :: (Constraints s, MonadLoggerIO m)
      => JoinRequest
      -> m (JoinResponse s)
    requestJoin joinMsg = ($ joinMsg) =<< connectServer addr

makeRuntimeState
    _peerBindAddr
    notify
    (Recover self clusterState)
  = do
    nextId <- newSequence
    return RuntimeState {
        self,
        clusterState,
        connections = mempty,
        waiting = mempty,
        calls = mempty,
        broadcalls = mempty,
        nextId,
        notify = notify self,
        joins = mempty
      }


{- | Make a new peer. -}
newPeer :: (MonadIO m) => AddressDescription -> m Peer
newPeer addr = Peer <$> getUUID <*> pure addr


{- | This is the type of a join request message. -}
newtype JoinRequest = JoinRequest AddressDescription
  deriving (Generic, Show)
instance Binary JoinRequest


{- | The response to a JoinRequest message -}
data JoinResponse s
  = JoinOk Peer (PowerState ClusterId Peer s)
  deriving (Generic)
deriving instance (Constraints s) => Show (JoinResponse s)
instance (Constraints s) => Binary (JoinResponse s)


{- | Message Identifier. -}
data MessageId = M UUID Word64 deriving (Generic, Show, Eq, Ord)
instance Binary MessageId


{- |
  Initialize a new sequence of messageIds. It would be perfectly fine to ensure
  unique message ids by generating a unique UUID for each one, but generating
  UUIDs is not free, and we are probably going to be generating a lot of these.
-}
newSequence :: (MonadIO m) => m MessageId
newSequence = do
  sid <- getUUID
  return (M sid 0)


{- |
  Generate the next message id in the sequence. We would normally use
  `succ` for this kind of thing, but making `MessageId` an instance of
  `Enum` really isn't appropriate.
-}
nextMessageId :: MessageId -> MessageId
nextMessageId (M sequenceId ord) = M sequenceId (ord + 1)


type Constraints s = (
    Binary s, Binary (Event s), Default s, Eq (Event s), State s, Show
    (Event s), ToJSON s, ToJSON (Event s)
  )


