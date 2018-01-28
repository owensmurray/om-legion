{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

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

  -- * Other types
  ClusterId,
  Peer,
) where


import Control.Concurrent (Chan, newEmptyMVar, putMVar, takeMVar,
   writeChan, newChan)
import Control.Concurrent.STM (TVar, atomically, newTVar, writeTVar,
   readTVar, retry)
import Control.Exception.Safe (MonadCatch, tryAny)
import Control.Monad (void, forever)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Logger (MonadLoggerIO, logDebug, logInfo, logWarn,
   logError)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except (runExceptT)
import Control.Monad.Trans.State (runStateT, StateT, get, put, modify)
import Data.Binary (Binary, Word64)
import Data.ByteString.Lazy (ByteString)
import Data.Conduit (runConduit, (.|), awaitForever, Source, yield)
import Data.Default.Class (Default)
import Data.Map (Map)
import Data.Monoid ((<>))
import Data.Set (Set, (\\))
import Data.UUID (UUID)
import GHC.Generics (Generic)
import OM.Legion.Conduit (chanToSource, chanToSink)
import OM.Legion.Fork (ForkM, forkC, forkM)
import OM.Legion.PowerState (PowerState, Event, StateId, projParticipants)
import OM.Legion.PowerState.Monad (PropAction(DoNothing, Send), event,
   acknowledge, runPowerStateT, merge, PowerStateT, disassociate,
   participate)
import OM.Legion.Settings (RuntimeSettings(RuntimeSettings), peerBindAddr,
   joinBindAddr, handleUserCall, handleUserCast)
import OM.Legion.UUID (getUUID)
import OM.Show (showt)
import OM.Socket (connectServer, bindAddr, AddressDescription, openEgress,
   Endpoint(Endpoint), openIngress, openServer)
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified OM.Legion.PowerState as PS


{- | The Legionary runtime state. -}
data RuntimeState e o s = RuntimeState {
            self :: Peer,
    clusterState :: PowerState ClusterId s Peer e o,
     connections :: Map Peer (PeerMessage e o s -> IO ()),
         waiting :: Map (StateId Peer) (Responder o),
           calls :: Map MessageId (Responder ByteString),
      broadcalls :: Map
                      MessageId
                      (
                        Map Peer (Maybe ByteString),
                        Responder (Map Peer ByteString)
                      ),
          nextId :: MessageId
  }


{- | Fork the Legion runtime system. -}
forkLegionary :: (
      Binary e, Binary o, Binary s, Default s, Eq e, Event e o s, ForkM m,
      MonadCatch m, MonadLoggerIO m, Show e, Show o, Show s
    )
  => RuntimeSettings
     {- ^ The general runtime configuration. -}
  -> StartupMode
     {- ^
       How to start the runtime, by creating new cluster or joining an
       existing cluster.
     -}
  -> m (Runtime e o s)
forkLegionary settings startupMode = do
  runtime <- Runtime <$> liftIO newChan
  forkC "main legion thread" $
    executeRuntime settings startupMode runtime
  return runtime


{- | A handle on the Legion runtime. -}
newtype Runtime e o s = Runtime {
    unRuntime :: Chan (RuntimeMessage e o s)
  }


{- |
  Update the distributed cluster state by applying an event. The event
  output will be returned immediately and may not reflect a totally
  consistent view of the cluster. The state update itself, however,
  is guaranteed to be applied atomically and consistently throughout
  the cluster.
-}
applyFast :: (MonadIO m)
  => Runtime e o s {- ^ The runtime handle. -}
  -> e             {- ^ The event to be applied. -}
  -> m o           {- ^ Returns the possibly inconsistent event output. -}
applyFast runtime e = runtimeCall runtime (ApplyFast e)


{- |
  Update the distributed cluster state by applying an event. Both the
  event output and resulting state will be totally consistent throughout
  the cluster.
-}
applyConsistent :: (MonadIO m)
  => Runtime e o s {- ^ The runtime handle. -}
  -> e             {- ^ The event to be applied. -}
  -> m o           {- ^ Returns the strongly consistent event output. -}
applyConsistent runtime e = runtimeCall runtime (ApplyConsistent e)


{- | Read the current powerstate value. -}
readState :: (MonadIO m)
  => Runtime e o s
  -> m (PowerState ClusterId s Peer e o)
readState runtime = runtimeCall runtime ReadState


{- |
  Send a user message to some other peer, and block until a response
  is received.
-}
call :: (MonadIO m) => Runtime e o s -> Peer -> ByteString -> m ByteString
call runtime target msg = runtimeCall runtime (Call target msg)


{- | Send the result of a call back to the peer that originated it. -}
sendCallResponse :: (MonadIO m)
  => Runtime e o s
  -> Peer
  -> MessageId
  -> ByteString
  -> m ()
sendCallResponse runtime target mid msg =
  runtimeCast runtime (SendCallResponse target mid msg)


{- | Send a user message to some other peer, without waiting on a response. -}
cast :: (MonadIO m) => Runtime e o s -> Peer -> ByteString -> m ()
cast runtime target message = runtimeCast runtime (Cast target message)


{- |
  Send a user message to all peers, and block until a response is received
  from all of them.
-}
broadcall :: (MonadIO m)
  => Runtime e o s
  -> ByteString
  -> m (Map Peer ByteString)
broadcall runtime msg = runtimeCall runtime (Broadcall msg)


{- | Send a user message to all peers, without wating on a response. -}
broadcast :: (MonadIO m) => Runtime e o s -> ByteString -> m ()
broadcast runtime msg = runtimeCast runtime (Broadcast msg)


{- | Eject a peer from the cluster. -}
eject :: (MonadIO m) => Runtime e o s -> Peer -> m ()
eject runtime peer = runtimeCast runtime (Eject peer)


{- | Get the identifier for the local peer. -}
getSelf :: (MonadIO m) => Runtime e o s ->  m Peer
getSelf runtime = runtimeCall runtime GetSelf


{- | The types of messages that can be sent to the runtime. -}
data RuntimeMessage e o s
  = ApplyFast e (Responder o)
  | ApplyConsistent e (Responder o)
  | Eject Peer
  | Merge (PowerState ClusterId s Peer e o)
  | Join JoinRequest (Responder (JoinResponse e o s))
  | ReadState (Responder (PowerState ClusterId s Peer e o))
  | Call Peer ByteString (Responder ByteString)
  | Cast Peer ByteString
  | Broadcall ByteString (Responder (Map Peer ByteString))
  | Broadcast ByteString
  | SendCallResponse Peer MessageId ByteString
  | HandleCallResponse Peer MessageId ByteString
  | GetSelf (Responder Peer)
  deriving (Show)


{- | The types of messages that can be sent from one peer to another. -}
data PeerMessage e o s
  = PMMerge (PowerState ClusterId s Peer e o)
    {- ^ Send a powerstate merge. -}
  | PMCall Peer MessageId ByteString
    {- ^ Send a user call message from one peer to another. -}
  | PMCast ByteString
    {- ^ Send a user cast message from one peer to another. -}
  | PMCallResponse Peer MessageId ByteString
    {- ^ Send a response to a user call message. -}

  deriving (Generic)
instance (Binary e, Binary s) => Binary (PeerMessage e o s)


{- | An opaque value that identifies a cluster participant. -}
data Peer = Peer {
      _peerId :: UUID,
    peerAddy :: AddressDescription
  }
  deriving (Generic, Show, Eq, Ord)
instance Binary Peer


{- | An opaque value that identifies a cluster. -}
newtype ClusterId = ClusterId UUID
  deriving (Binary, Show, Eq)


{- | A way for the runtime to respond to a message. -}
newtype Responder a = Responder {
    unResponder :: a -> IO ()
  }
instance Show (Responder a) where
  show _ = "Responder"


{- | Respond to a message, using the given responder, in 'MonadIO'. -}
respond :: (MonadIO m) => Responder a -> a -> m ()
respond responder = liftIO . unResponder responder


{- | Send a message to the runtime that blocks on a response. -}
runtimeCall :: (MonadIO m)
  => Runtime e o s
  -> (Responder a -> RuntimeMessage e o s)
  -> m a
runtimeCall runtime withResonder = liftIO $ do
  mvar <- newEmptyMVar
  runtimeCast runtime (withResonder (Responder (putMVar mvar)))
  takeMVar mvar


{- | Send a message to the runtime. Do not wait for a result. -}
runtimeCast :: (MonadIO m) => Runtime e o s -> RuntimeMessage e o s -> m ()
runtimeCast runtime = liftIO . writeChan (unRuntime runtime)


{- |
  Execute the Legion runtime, with the given user definitions, and
  framework settings. This function never returns (except maybe with an
  exception if something goes horribly wrong).
-}
executeRuntime :: (
      Binary e,
      Binary o,
      Binary s,
      Default s,
      Eq e,
      Event e o s,
      ForkM m,
      MonadCatch m,
      MonadLoggerIO m,
      Show e,
      Show o,
      Show s
    )
  => RuntimeSettings
    {- ^ Settings and configuration of the legionframework.  -}
  -> StartupMode
  -> Runtime e o s
    {- ^ A source of requests, together with a way to respond to the requets. -}
  -> m ()
executeRuntime settings startupMode runtime = do
    {- Start the various messages sources. -}
    startPeerListener
    startJoinListener

    rts <- makeRuntimeState settings startupMode
    void . (`runStateT` rts) . runConduit $
      chanToSource (unRuntime runtime)
      .| awaitForever (\msg -> do
          $(logDebug) $ "Receieved: " <> showt msg
          lift (handleRuntimeMessage msg)
        )
  where
    startPeerListener :: (ForkM m, MonadCatch m, MonadLoggerIO m) => m ()
    startPeerListener = forkC "peer listener" $ 
      runConduit (
        openIngress (peerBindAddr settings)
        .| awaitForever (\case
             PMMerge ps -> yield (Merge ps)
             PMCall source mid msg -> liftIO . forkM $
               sendCallResponse runtime source mid
               =<< handleUserCall settings msg
             PMCast msg -> (liftIO . forkM) (handleUserCast settings msg)
             PMCallResponse source mid msg ->
               yield (HandleCallResponse source mid msg)
           )
        .| chanToSink (unRuntime runtime)
      )

    startJoinListener :: (ForkM m, MonadCatch m, MonadLoggerIO m) => m ()
    startJoinListener = forkC "join listener" $
      runConduit (
        openServer (joinBindAddr settings)
        .| awaitForever (\(req, respond_) -> lift $
            runtimeCall runtime (Join req) >>= respond_
          )
      )


{- | Execute the incoming messages. -}
handleRuntimeMessage :: (
      Binary e, Binary s, Eq e, Event e o s, ForkM m, MonadCatch m,
      MonadLoggerIO m
    )
  => RuntimeMessage e o s
  -> StateT (RuntimeState e o s) m ()

handleRuntimeMessage (ApplyFast e responder) =
  updateCluster $ do
    (o, _sid) <- event e
    respond responder o

handleRuntimeMessage (ApplyConsistent e responder) =
  updateCluster $ do
    (_v, sid) <- event e
    lift (waitOn sid responder)

handleRuntimeMessage (Eject peer) =
  updateCluster $ disassociate peer

handleRuntimeMessage (Merge other) =
  updateCluster $
    runExceptT (merge other) >>= \case
      Left err -> $(logError) $ "Bad cluster merge: " <> showt err
      Right () -> return ()

handleRuntimeMessage (Join (JoinRequest addr) responder) = do
  peer <- newPeer addr
  updateCluster (participate peer)
  respond responder . JoinOk peer . clusterState =<< get

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
      -> StateT (RuntimeState e o s) m ()
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
      -> StateT (RuntimeState e o s) m ()
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

handleRuntimeMessage (GetSelf responder) =
  respond responder =<< self <$> get


{- | Get the projected peers. -}
getPeers :: (Monad m) => StateT (RuntimeState e o s) m (Set Peer)
getPeers = projParticipants . clusterState <$> get


{- | Get a new messageId. -}
newMessageId :: (Monad m) => StateT (RuntimeState e o s) m MessageId
newMessageId = do
  state@RuntimeState {nextId} <- get
  put state {nextId = nextMessageId nextId}
  return nextId


{- |
  Like 'runPowerStateT', plus automatically take care of doing necessary
  IO implied by the cluster update.
-}
updateCluster :: (
      Binary e, Binary s, Eq e, Event e o s, ForkM m, MonadCatch m,
      MonadLoggerIO m
    )
  => PowerStateT ClusterId s Peer e o (StateT (RuntimeState e o s) m) a
  -> StateT (RuntimeState e o s) m a
updateCluster action = do
  state@RuntimeState {self, clusterState} <- get
  runPowerStateT self clusterState (action <* acknowledge) >>=
    \(v, propAction, newClusterState, infs) -> do
      put state {clusterState = newClusterState}
      respondToWaiting infs
      propagate propAction
      return v


{- | Wait on a consistent response for the given state id. -}
waitOn :: (Monad m)
  => StateId Peer
  -> Responder o
  -> StateT (RuntimeState e o s) m ()
waitOn sid responder =
  modify (\state@RuntimeState {waiting} -> state {
    waiting = Map.insert sid responder waiting
  })


{- | Propagates cluster information if necessary. -}
propagate :: (Binary e, Binary s, ForkM m, MonadCatch m, MonadLoggerIO m)
  => PropAction
  -> StateT (RuntimeState e o s) m ()
propagate DoNothing = return ()
propagate Send = do
    RuntimeState {self, clusterState} <- get
    mapM_ (sendPeer (PMMerge clusterState))
      . Set.delete self
      . PS.allParticipants
      $ clusterState
    disconnectObsolete
  where
    {- |
      Shut down connections to peers that are no longer participating
      in the cluster.
    -}
    disconnectObsolete :: (MonadIO m) => StateT (RuntimeState e o s) m ()
    disconnectObsolete = do
        RuntimeState {clusterState, connections} <- get
        mapM_ disconnect $
          PS.allParticipants clusterState \\ Map.keysSet connections


{- | Send a peer message, creating a new connection if need be. -}
sendPeer :: (Binary e, Binary s, ForkM m, MonadCatch m, MonadLoggerIO m)
  => PeerMessage e o s
  -> Peer
  -> StateT (RuntimeState e o s) m ()
sendPeer msg peer = do
  state@RuntimeState {connections} <- get
  case Map.lookup peer connections of
    Nothing -> do
      conn <- lift (createConnection peer)
      put state {connections = Map.insert peer conn connections}
      sendPeer msg peer
    Just conn ->
      (liftIO . tryAny) (conn msg) >>= \case
        Left err -> do
          $(logWarn) $ "Failure sending to peer: " <> showt (peer, err)
          disconnect peer
        Right () -> return ()


{- | Disconnect the connection to a peer. -}
disconnect :: (MonadIO m) => Peer -> StateT (RuntimeState e o s) m ()
disconnect peer =
  modify (\state@RuntimeState {connections} -> state {
    connections = Map.delete peer connections
  })


{- | Create a connection to a peer. -}
createConnection :: (
      Binary e, Binary s, ForkM m, MonadCatch m, MonadLoggerIO m
    )
  => Peer
  -> m (PeerMessage e o s -> IO ())
createConnection peer = do
    latest <- liftIO $ atomically (newTVar [])
    forkM . forever $
      (tryAny . runConduit) (
        latestSource latest .| openEgress (peerAddy peer)
      ) >>= \case
        Left err -> $(logWarn) $ "Connection crashed: " <> showt (peer, err)
        Right () -> $(logWarn) "Connection closed for no reason."
    return (\msg ->
        atomically $
          readTVar latest >>= \msgs ->
            writeTVar latest (
              case msg of
                PMMerge _ ->
                  msg : filter (\case {PMMerge _ -> True; _ -> False}) msgs
                _ -> msgs ++ [msg] 
            )
      )
  where
    latestSource :: (MonadIO m)
      => TVar [PeerMessage e o s]
      -> Source m (PeerMessage e o s)
    latestSource latest = forever $ mapM_ yield =<< (liftIO . atomically) (
        readTVar latest >>= \case
          [] -> retry
          messages -> do
            writeTVar latest []
            return messages
      )


{- |
  Respond to event applications that are waiting on a consistent result,
  if such a result is available.
-}
respondToWaiting :: (MonadIO m)
  => Map (StateId Peer) o
  -> StateT (RuntimeState e o s) m ()
respondToWaiting available =
    mapM_ respondToOne (Map.toList available)
  where
    respondToOne :: (MonadIO m)
      => (StateId Peer, o)
      -> StateT (RuntimeState e o s) m ()
    respondToOne (sid, o) = do
      state@RuntimeState {waiting} <- get
      case Map.lookup sid waiting of
        Nothing -> return ()
        Just responder -> do
          respond responder o
          put state {waiting = Map.delete sid waiting}


{- | This defines the various ways a node can be spun up. -}
data StartupMode
  = NewCluster
    {- ^ Indicates that we should bootstrap a new cluster at startup. -}
  | JoinCluster AddressDescription
    {- ^ Indicates that the node should try to join an existing cluster. -}
  deriving (Show)


{- | Initialize the runtime state. -}
makeRuntimeState :: (
      Binary e,
      Binary o,
      Binary s,
      Default s,
      MonadLoggerIO m,
      Show e,
      Show o,
      Show s
    )
  => RuntimeSettings
  -> StartupMode
  -> m (RuntimeState e o s)

makeRuntimeState
    RuntimeSettings {peerBindAddr = Endpoint {bindAddr}}
    NewCluster
  = do
    {- Build a brand new node state, for the first node in a cluster. -}
    self <- newPeer bindAddr
    clusterId <- ClusterId <$> getUUID
    nextId <- newSequence
    return RuntimeState {
        self,
        clusterState = PS.new clusterId (Set.singleton self),
        connections = mempty,
        waiting = mempty,
        calls = mempty,
        broadcalls = mempty,
        nextId
      }

makeRuntimeState
    RuntimeSettings {peerBindAddr}
    (JoinCluster addr)
  = do
    {- Join a cluster an existing cluster. -}
    $(logInfo) "Trying to join an existing cluster."
    JoinOk self cluster <-
      requestJoin
      . JoinRequest
      . bindAddr
      $ peerBindAddr
    nextId <- newSequence
    return RuntimeState {
        self,
        clusterState = cluster,
        connections = mempty,
        waiting = mempty,
        calls = mempty,
        broadcalls = mempty,
        nextId
      }
  where
    requestJoin :: (
          Binary e, Binary o, Binary s, MonadLoggerIO m, Show e, Show o,
          Show s
        )
      => JoinRequest
      -> m (JoinResponse e o s)
    requestJoin joinMsg = ($ joinMsg) =<< connectServer addr


{- | Make a new peer. -}
newPeer :: (MonadIO m) => AddressDescription -> m Peer
newPeer addr = Peer <$> getUUID <*> pure addr


{- | This is the type of a join request message. -}
newtype JoinRequest = JoinRequest AddressDescription
  deriving (Generic, Show)
instance Binary JoinRequest


{- | The response to a JoinRequest message -}
data JoinResponse e o s
  = JoinOk Peer (PowerState ClusterId s Peer e o)
  deriving (Show, Generic)
instance (Binary e, Binary s) => Binary (JoinResponse e o s)


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


