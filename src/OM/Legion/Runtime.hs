{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

{- | The meat of the Legion runtime implementation. -}
module OM.Legion.Runtime (
  -- * Starting the framework runtime.
  forkLegionary,
  Runtime,
  StartupMode(..),

  -- * Constraints
  MonadConstraints,

  -- * Runtime Interface.
  applyFast,
  applyConsistent,
  readState,
  call,
  cast,
  broadcall,
  broadcast,
  eject,
  getSelf,
  getClusterName,
  getStats,
  Stats(..),
) where

import Control.Arrow (Arrow((&&&)))
import Control.Concurrent (threadDelay)
import Control.Exception.Safe (MonadCatch, tryAny)
import Control.Monad (unless, void, when)
import Control.Monad.IO.Class (MonadIO(liftIO))
import Control.Monad.IO.Unlift (MonadUnliftIO)
import Control.Monad.Logger.CallStack (LoggingT(runLoggingT),
  MonadLoggerIO(askLoggerIO), LogStr, MonadLogger, logDebug, logError,
  logInfo)
import Control.Monad.State (MonadState(get, put), StateT, evalStateT,
  gets, modify')
import Control.Monad.Trans.Class (MonadTrans(lift))
import Data.Aeson (ToJSON)
import Data.Binary (Binary)
import Data.ByteString.Lazy (ByteString)
import Data.CRDT.EventFold (Event(Output, State),
  UpdateResult(urEventFold, urOutputs), EventFold, EventId, diffSize,
  events, infimumId, projParticipants)
import Data.CRDT.EventFold.Monad (MonadUpdateEF(diffMerge, disassociate,
  event, fullMerge, participate), EventFoldT, runEventFoldT)
import Data.Default.Class (Default)
import Data.Function ((&))
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import Data.Map (Map)
import Data.Set ((\\), Set)
import Data.Time (DiffTime, diffTimeToPicoseconds, picosecondsToDiffTime)
import Data.UUID (UUID)
import Data.UUID.V1 (nextUUID)
import GHC.Generics (Generic)
import Network.Socket (PortNumber)
import OM.Fork (Actor(Msg, actorChan), Race, Responder, race)
import OM.Legion.Connection (JoinResponse(JoinOk),
  RuntimeState(RuntimeState, rsBroadcalls, rsCalls, rsClusterState,
  rsConnections, rsJoins, rsNextId, rsNotify, rsSelf, rsWaiting),
  EventConstraints, disconnect, peerMessagePort, sendPeer)
import OM.Legion.MsgChan (MessageId(M), Peer(unPeer), PeerMessage(PMCall,
  PMCallResponse, PMCast, PMFullMerge, PMMerge, PMOutputs), ClusterName)
import OM.Legion.RChan (JoinRequest(JoinRequest),
  RuntimeMessage(ApplyConsistent, ApplyFast, Broadcall, Broadcast,
  Call, Cast, Eject, FullMerge, GetStats, HandleCallResponse, Join,
  Merge, Outputs, ReadState, Resend, SendCallResponse), RChan, newRChan,
  readRChan)
import OM.Logging (withPrefix)
import OM.Show (showj, showt)
import OM.Socket (AddressDescription(AddressDescription), connectServer,
  openIngress, openServer)
import OM.Time (MonadTimeSpec(getTime), addTime, diffTimeSpec)
import Prelude (Applicative(pure), Either(Left, Right), Enum(succ),
  Eq((/=)), Functor(fmap), Maybe(Just, Nothing), Monad((>>), (>>=),
  return), Monoid(mempty), Ord((<=), (>=)), Semigroup((<>)), ($), (.),
  (<$>), (=<<), IO, Int, MonadFail, Show, String, fst, mapM_, maybe,
  otherwise, seq, sequence_)
import System.Clock (TimeSpec)
import System.Random.Shuffle (shuffleM)
import qualified Data.Binary as Binary
import qualified Data.CRDT.EventFold as EF
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified OM.Fork as Fork
import qualified Streaming.Prelude as Stream

{-# ANN module ("HLint: ignore Redundant <$>" :: String) #-}
{-# ANN module ("HLint: ignore Use underscore" :: String) #-}

{- |
  Shorthand for all the monad constraints, mainly use so that
  documentation renders better.
-}
type MonadConstraints m =
  ( MonadCatch m
  , MonadFail m
  , MonadLoggerIO m
  , MonadTimeSpec m
  , MonadUnliftIO m
  , Race
  )


{- | Fork the Legion runtime system. -}
forkLegionary
  :: ( EventConstraints e
     , MonadConstraints m
     )
  => (ByteString -> IO ByteString) {- ^ Handle a user call request. -}
  -> (ByteString -> IO ()) {- ^ Handle a user cast message. -}
  -> (Peer -> EventFold ClusterName Peer e -> IO ())
     {- ^ Callback when the cluster-wide eventfold changes. -}
  -> Int
     {- ^
       The propagation interval, in microseconds (for use with
       `threadDelay`).
     -}
  -> StartupMode e
     {- ^
       How to start the runtime, by creating new cluster or joining an
       existing cluster.
     -}
  -> m (Runtime e)
forkLegionary
    handleUserCall
    handleUserCast
    notify
    resendInterval
    startupMode
  = do
    logInfo $ "Starting up with the following Mode: " <> showt startupMode
    rts <- makeRuntimeState notify startupMode
    runtimeChan <- newRChan
    logging <- withPrefix (logPrefix (rsSelf rts)) <$> askLoggerIO
    rStats <- liftIO $ newIORef mempty
    (`runLoggingT` logging) $
      executeRuntime
        handleUserCall
        handleUserCast
        resendInterval
        rts
        runtimeChan
        rStats
    let
      clusterId :: ClusterName
      clusterId = EF.origin (rsClusterState rts)
    return
      Runtime
        { rChan = runtimeChan
        , rSelf = rsSelf rts
        , rClusterId = clusterId
        , rStats
        }
  where
    logPrefix :: Peer -> LogStr
    logPrefix self_ = "[" <> showt self_ <> "] "


{- | A handle on the Legion runtime. -}
data Runtime e = Runtime
  {      rChan :: RChan e
  ,      rSelf :: Peer
  , rClusterId :: ClusterName
  ,     rStats :: IORef (Map Peer TimeSpec)
  }
instance Actor (Runtime e) where
  type Msg (Runtime e) = RuntimeMessage e
  actorChan = actorChan . rChan


{- |
  Some basic stats that can be used to intuit the health of the cluster.
  We currently only report on how long it has been since some peer has
  made some progress.
-}
newtype Stats = Stats
  { timeWithoutProgress :: Map Peer DiffTime
                           {- ^
                             How long it has been since a 'Peer' has
                             made progress (if it is 'divergent'). If
                             the peer is completely up to date as far as
                             we know, then it does not appear in the map
                             at all. Only peers which we are expecting
                             to make progress appear.
                           -}
  }
  deriving stock (Generic, Show, Eq)
  deriving anyclass (ToJSON)
instance Binary Stats where
  get =
    Stats <$> (fmap picosecondsToDiffTime <$> Binary.get)
  put (Stats timeWithoutProgress) =
    Binary.put (diffTimeToPicoseconds <$> timeWithoutProgress)


{- |
  Update the distributed cluster state by applying an event. The event
  output will be returned immediately and may not reflect a totally
  consistent view of the cluster. The state update itself, however,
  is guaranteed to be applied atomically and consistently throughout
  the cluster.
-}
applyFast :: (MonadIO m)
  => Runtime e     {- ^ The runtime handle. -}
  -> e             {- ^ The event to be applied. -}
  -> m (Output e)  {- ^ Returns the possibly inconsistent event output. -}
applyFast runtime e = Fork.call runtime (ApplyFast e)


{- |
  Update the distributed cluster state by applying an event. Both the
  event output and resulting state will be totally consistent throughout
  the cluster.
-}
applyConsistent :: (MonadIO m)
  => Runtime e     {- ^ The runtime handle. -}
  -> e             {- ^ The event to be applied. -}
  -> m (Output e)  {- ^ Returns the strongly consistent event output. -}
applyConsistent runtime e = Fork.call runtime (ApplyConsistent e)


{- | Read the current powerstate value. -}
readState :: (MonadIO m)
  => Runtime e
  -> m (EventFold ClusterName Peer e)
readState runtime = Fork.call runtime ReadState


{- |
  Send a user message to some other peer, and block until a response
  is received.
-}
call :: (MonadIO m) => Runtime e -> Peer -> ByteString -> m ByteString
call runtime target msg = Fork.call runtime (Call target msg)


{- | Send the result of a call back to the peer that originated it. -}
sendCallResponse
  :: (MonadIO m)
  => RChan e
  -> Peer
  -> MessageId
  -> ByteString
  -> m ()
sendCallResponse runtimeChan target mid msg =
  Fork.cast runtimeChan (SendCallResponse target mid msg)


{- | Send a user message to some other peer, without waiting on a response. -}
cast :: (MonadIO m) => Runtime e -> Peer -> ByteString -> m ()
cast runtime target message = Fork.cast runtime (Cast target message)


{- |
  Send a user message to all peers, and block until a response is received
  from all of them.
-}
broadcall :: (MonadIO m)
  => Runtime e
  -> DiffTime {- ^ The timeout. -}
  -> ByteString
  -> m (Map Peer (Maybe ByteString))
broadcall runtime timeout msg = Fork.call runtime (Broadcall timeout msg)


{- | Send a user message to all peers, without wating on a response. -}
broadcast :: (MonadIO m) => Runtime e -> ByteString -> m ()
broadcast runtime msg = Fork.cast runtime (Broadcast msg)


{- | Eject a peer from the cluster. -}
eject :: (MonadIO m) => Runtime e -> Peer -> m ()
eject runtime peer = Fork.call runtime (Eject peer)


{- | Get the identifier for the local peer. -}
getSelf :: Runtime e -> Peer
getSelf = rSelf


{- |
  Execute the Legion runtime, with the given user definitions, and
  framework settings. This function never returns (except maybe with an
  exception if something goes horribly wrong).
-}
executeRuntime
  :: ( Binary (Output e)
     , Binary (State e)
     , Binary e
     , Default (State e)
     , Eq (Output e)
     , Eq e
     , Event Peer e
     , MonadCatch m
     , MonadFail m
     , MonadLoggerIO m
     , MonadTimeSpec m
     , MonadUnliftIO m
     , Race
     , Show (Output e)
     , Show (State e)
     , Show e
     , ToJSON (Output e)
     , ToJSON (State e)
     , ToJSON e
     )
  => (ByteString -> IO ByteString)
     {- ^ Handle a user call request.  -}
  -> (ByteString -> IO ())
     {- ^ Handle a user cast message. -}
  -> Int
     {- ^
       The propagation interval, in microseconds (for use with
       `threadDelay`).
     -}
  -> RuntimeState e
  -> RChan e
     {- ^
       A source of requests, together with a way to respond to the
       requets.
     -}
  -> IORef (Map Peer TimeSpec)
  -> m ()
executeRuntime
    handleUserCall
    handleUserCast
    resendInterval
    rts
    runtimeChan
    peerStats
  = do
    {- Start the various messages sources. -}
    race "om-legion peer listener" runPeerListener
    race "om-legion join listener" runJoinListener
    race "om-legion periodic resend" runPeriodicResent
    race "om-legion message handler" $
      (`evalStateT` rts)
        (
          let
            -- handleMessages :: StateT (RuntimeState e3) m Void
            handleMessages = do
              msg <- readRChan runtimeChan
              RuntimeState {rsClusterState = cluster1} <- get
              logDebug $ "Handling: " <> showt msg
              handleRuntimeMessage msg
              RuntimeState {rsClusterState = cluster2} <- get
              when (cluster1 /= cluster2) $
                logDebug $ "New Cluster State: " <> showj cluster2
                -- propagate
              handleBroadcallTimeouts
              handleOutstandingJoins
              handleMessages
          in do
            liftIO $ rsNotify rts (rsClusterState rts)
            handleMessages
        )
  where
    runPeerListener :: (MonadLoggerIO m, MonadFail m) => m ()
    runPeerListener =
      let
        addy :: AddressDescription
        addy =
          AddressDescription
            (
              unPeer (rsSelf rts)
              <> ":" <> showt peerMessagePort
            )
      in
        openIngress addy
        `Stream.for`
          (\ (msgSource, msg) -> do
            liftIO $ do
              now <- getTime
              atomicModifyIORef'
                peerStats
                (\peerTimes -> (Map.insert msgSource now peerTimes, ()))
            lift $ logDebug $ "Handling: " <> showt (msgSource :: Peer, msg)
            case msg of
              PMFullMerge ps -> Stream.yield (FullMerge ps)
              PMOutputs outputs -> Stream.yield (Outputs outputs)
              PMMerge ps -> Stream.yield (Merge ps)
              PMCall source mid callMsg ->
                (liftIO . tryAny) (handleUserCall callMsg) >>= \case
                  Left err ->
                    lift . logError
                      $ "User call handling failed with: " <> showt err
                  Right v -> sendCallResponse runtimeChan source mid v
              PMCast castMsg -> liftIO (handleUserCast castMsg)
              PMCallResponse source mid responseMsg ->
                Stream.yield (HandleCallResponse source mid responseMsg)
          )
        & Stream.mapM_ (Fork.cast runtimeChan)

    runJoinListener
      :: ( MonadCatch m
         , MonadFail m
         , MonadLogger m
         , MonadUnliftIO m
         , Race
         )
      => m ()
    runJoinListener =
      let
        addy :: AddressDescription
        addy =
          AddressDescription
            (
              unPeer (rsSelf rts)
              <> ":" <> showt joinMessagePort
            )
      in
        openServer addy Nothing
        & Stream.mapM_
            (\(req, respond_) ->
              Fork.call runtimeChan (Join req) >>= respond_
            )

    runPeriodicResent :: (MonadIO m) => m ()
    runPeriodicResent =
      let
        periodicResend :: (MonadIO m) => m ()
        periodicResend = do
          liftIO $ threadDelay resendInterval
          Fork.call runtimeChan Resend
          periodicResend
      in
        periodicResend


{- | Handle any outstanding joins. -}
handleOutstandingJoins :: (MonadLoggerIO m) => StateT (RuntimeState e) m ()
handleOutstandingJoins = do
  state@RuntimeState {rsJoins, rsClusterState} <- get
  let
    (consistent, pending) =
      Map.partitionWithKey
        (\k _ -> k <= infimumId rsClusterState)
        rsJoins
  put state {rsJoins = pending}
  sequence_ [
      do
        logInfo $ "Completing join (" <> showt sid <> ")."
        respond responder (JoinOk rsClusterState)
      | (sid, responder) <- Map.toList consistent
    ]


{- | Handle any broadcall timeouts. -}
handleBroadcallTimeouts
  :: ( MonadIO m
     , MonadTimeSpec m
     )
  => StateT (RuntimeState e) m ()
handleBroadcallTimeouts = do
  broadcalls <- gets rsBroadcalls
  now <- getTime
  sequence_ [
      do
        respond responder responses
        modify' (\rs -> rs {
            rsBroadcalls = Map.delete messageId (rsBroadcalls rs)
          })
      | (messageId, (responses, responder, expiry)) <- Map.toList broadcalls
      , now >= expiry
    ]


{- | Execute the incoming messages. -}
handleRuntimeMessage
  :: ( Binary (Output e)
     , Binary (State e)
     , Binary e
     , Default (State e)
     , Eq (Output e)
     , Eq e
     , Event Peer e
     , MonadLoggerIO m
     , MonadTimeSpec m
     , Show (Output e)
     , Show (State e)
     , Show e
     , ToJSON (Output e)
     , ToJSON (State e)
     , ToJSON e
     )
  => RuntimeMessage e
  -> StateT (RuntimeState e) m ()

handleRuntimeMessage (Outputs outputs) =
  {-# SCC "Outputs" #-}
  respondToWaiting outputs

handleRuntimeMessage (GetStats responder) =
  {-# SCC "GetStats" #-}
  respond responder =<< gets rsClusterState

handleRuntimeMessage (ApplyFast e responder) =
  {-# SCC "ApplyFast" #-}
  updateCluster $
    fst <$> event e >>= respond responder

handleRuntimeMessage (ApplyConsistent e responder) =
  {-# SCC "ApplyConsistent" #-}
  (
    do
      updateCluster $ do
        (_v, sid) <- event e
        lift (waitOn sid responder)
      rs <- get
      logDebug $ "Waiting: " <> showt (rsWaiting rs)
  )

handleRuntimeMessage (Eject peer responder) =
  {-# SCC "Eject" #-}
  do
    updateClusterAs peer $
      void $ disassociate peer
    propagate
    {- ↓
      This is an awful hack. The problem is that 'propagate' uses
      'sendPeer', but 'sendPeer' itself is asynchronous (though it should be
      very fast). The correct solution is a bit tricky. We can either figure
      out some way to block all the way down through the internals of the
      connection management, or else maybe extend the "join port" server
      endpoint to accept eject notifications as well as join requests.
    -}
    liftIO $ threadDelay 500_000
    respond responder ()

handleRuntimeMessage (Merge other) =
  {-# SCC "Merge" #-}
  updateCluster $
    diffMerge other >>= \case
      Left err -> logError $ "Bad cluster merge: " <> showt err
      Right () -> return ()

handleRuntimeMessage (FullMerge other) =
  {-# SCC "FullMerge" #-}
  updateCluster $
    fullMerge other >>= \case
      Left err -> logError $ "Bad cluster merge: " <> showt err
      Right () -> return ()

handleRuntimeMessage (Join (JoinRequest peer) responder) =
  {-# SCC "Join" #-}
  do
    logInfo $ "Handling join from peer: " <> showt peer
    updateCluster (do
        void $ disassociate peer
        void $ participate peer
      )
    RuntimeState {rsClusterState} <- get
    logInfo $ "Join immediately with: " <> showt rsClusterState
    respond responder (JoinOk rsClusterState)

handleRuntimeMessage (ReadState responder) =
  {-# SCC "ReadState" #-}
  respond responder . rsClusterState =<< get

handleRuntimeMessage (Call target msg responder) =
    {-# SCC "Call" #-}
    do
      mid <- newMessageId
      source <- gets rsSelf
      setCallResponder mid
      sendPeer (PMCall source mid msg) target
  where
    setCallResponder :: (Monad m)
      => MessageId
      -> StateT (RuntimeState e) m ()
    setCallResponder mid = do
      state@RuntimeState {rsCalls} <- get
      put state {
          rsCalls = Map.insert mid responder rsCalls
        }

handleRuntimeMessage (Cast target msg) =
  {-# SCC "Cast" #-}
  sendPeer (PMCast msg) target

handleRuntimeMessage (Broadcall timeout msg responder) =
    {-# SCC "Broadcall" #-}
    do
      expiry <- addTime timeout <$> getTime
      mid <- newMessageId
      source <- gets rsSelf
      setBroadcallResponder expiry mid
      mapM_ (sendPeer (PMCall source mid msg)) =<< getPeers
  where
    setBroadcallResponder :: (Monad m)
      => TimeSpec
      -> MessageId
      -> StateT (RuntimeState e) m ()
    setBroadcallResponder expiry mid = do
      peers <- getPeers
      state@RuntimeState {rsBroadcalls} <- get
      put state {
          rsBroadcalls =
            Map.insert
              mid
              (
                Map.fromList [(peer, Nothing) | peer <- Set.toList peers],
                responder,
                expiry
              )
              rsBroadcalls
        }

handleRuntimeMessage (Broadcast msg) =
  {-# SCC "Broadcast" #-}
  mapM_ (sendPeer (PMCast msg)) =<< getPeers

handleRuntimeMessage (SendCallResponse target mid msg) =
  {-# SCC "SendCallResponse" #-}
  do
    source <- gets rsSelf
    sendPeer (PMCallResponse source mid msg) target

handleRuntimeMessage (HandleCallResponse source mid msg) =
  {-# SCC "HandleCallResponse" #-}
  do
    state@RuntimeState {rsCalls, rsBroadcalls} <- get
    case Map.lookup mid rsCalls of
      Nothing ->
        case Map.lookup mid rsBroadcalls of
          Nothing -> return ()
          Just (responses, responder, expiry) ->
            let
              responses2 = Map.insert source (Just msg) responses
              response = Map.fromList [
                  (peer, r)
                  | (peer, Just r) <- Map.toList responses2
                ]
              peers = Map.keysSet responses2
            in
              if Set.null (peers \\ Map.keysSet response)
                then do
                  respond responder (Just <$> response)
                  put state {
                      rsBroadcalls = Map.delete mid rsBroadcalls
                    }
                else
                  put state {
                      rsBroadcalls =
                        Map.insert
                          mid
                          (responses2, responder, expiry)
                          rsBroadcalls
                    }
      Just responder -> do
        respond responder msg
        put state {rsCalls = Map.delete mid rsCalls}

handleRuntimeMessage (Resend responder) =
  {-# SCC "Resend" #-}
  propagate >>= respond responder


{- | Get the projected peers. -}
getPeers :: (Monad m) => StateT (RuntimeState e) m (Set Peer)
getPeers = gets (projParticipants . rsClusterState)


{- | Get a new messageId. -}
newMessageId :: (Monad m) => StateT (RuntimeState e) m MessageId
newMessageId = do
  state@RuntimeState {rsNextId} <- get
  put state {rsNextId = nextMessageId rsNextId}
  return rsNextId


{- |
  Like 'runEventFoldT', plus automatically take care of doing necessary
  IO implied by the cluster update.
-}
updateCluster
  :: ( EventConstraints e
     , MonadLoggerIO m
     , MonadState (RuntimeState e) m
     )
  => EventFoldT ClusterName Peer e m a
  -> m a
updateCluster action = do
  RuntimeState {rsSelf} <- get
  updateClusterAs rsSelf action


{- |
  Like 'updateCluster', but perform the operation on behalf of a specified
  peer. This is required for e.g. the peer eject case, when the ejected peer
  may not be able to perform acknowledgements on its own behalf.
-}
updateClusterAs
  :: forall e m a.
     ( EventConstraints e
     , MonadLoggerIO m
     , MonadState (RuntimeState e) m
     )
  => Peer
  -> EventFoldT ClusterName Peer e m a
  -> m a
updateClusterAs asPeer action = do
  (oldCluster, notify)
    <- gets (rsClusterState &&& rsNotify)
  (v, ur) <- runEventFoldT asPeer oldCluster action
  do {- Update the cluster -}
    let
      newCluster :: EventFold ClusterName Peer e
      newCluster = urEventFold ur
    when (oldCluster /= newCluster) $ liftIO (notify newCluster)
    modify'
      (
        let
          doModify state =
            newCluster `seq`
            state
              { rsClusterState = newCluster
              }
        in
          doModify
      )
  do {- Dispatch outputs. -}
    self <- gets rsSelf
    let
      outputs :: Map (EventId Peer) (Output e)
      outputs = urOutputs ur

      byRemotePeer :: Map Peer (Map (EventId Peer) (Output e))
      byRemotePeer =
        Map.unionsWith
          (<>)
          [ Map.singleton peer (Map.singleton eid o)
          | (eid, o) <- Map.toList outputs
          , Just peer <- [EF.source eid]
          ]
    respondToWaiting outputs
    sequence_
      [ do
          logDebug $ "Sending remote outputs: " <> showt byRemotePeer
          sendPeer (PMOutputs outputsForPeer) peer
      | (peer, outputsForPeer) <- Map.toList byRemotePeer
      , peer /= self
      ]
  pure v


{- | Wait on a consistent response for the given state id. -}
waitOn :: (Monad m)
  => EventId Peer
  -> Responder (Output e)
  -> StateT (RuntimeState e) m ()
waitOn sid responder =
  modify' (\state@RuntimeState {rsWaiting} -> state {
    rsWaiting = Map.insert sid responder rsWaiting
  })


{- | Propagates cluster information if necessary. -}
propagate
  :: ( EventConstraints e
     , MonadLoggerIO m
     , MonadState (RuntimeState e) m
     )
  => m ()
propagate = do
    (self, cluster) <- gets (rsSelf &&& rsClusterState)
    let
      targets = Set.delete self $
        EF.allParticipants cluster

    liftIO (shuffleM (Set.toList targets)) >>= \case
      [] -> return ()
      target:_ ->
        case events target cluster of
          Nothing -> do
            logInfo
              $ "Sending full merge because the target's join event "
              <> "has not reaached the infimum"
            sendPeer (PMFullMerge cluster) target
          Just diff
            | diffSize diff <= 0 ->
                sendPeer (PMMerge diff) target
            | otherwise ->
                sendPeer (PMFullMerge cluster) target
    disconnectObsolete
  where
    {- |
      Shut down connections to peers that are no longer participating
      in the cluster.
    -}
    disconnectObsolete
      :: ( MonadState (RuntimeState e) m
         , MonadLogger m
         )
      => m ()
    disconnectObsolete = do
      (cluster, conns) <- gets (rsClusterState &&& rsConnections)
      let obsolete = Map.keysSet conns \\ EF.allParticipants cluster
      unless (Set.null obsolete) $
        logInfo $ "Disconnecting obsolete: " <> showt obsolete
      mapM_ disconnect obsolete


{- |
  Respond to event applications that are waiting on a consistent result,
  if such a result is available.
-}
respondToWaiting
  :: forall m e.
     ( MonadLoggerIO m
     , MonadState (RuntimeState e) m
     , Show (Output e)
     )
  => Map (EventId Peer) (Output e)
  -> m ()
respondToWaiting available = do
    rs <- get
    logDebug
      $ "Responding to: " <> showt (available, Map.keysSet (rsWaiting rs))
    mapM_ respondToOne (Map.toList available)
  where
    respondToOne
      :: (EventId Peer, Output e)
      -> m ()
    respondToOne (sid, output) = do
      state@RuntimeState {rsWaiting} <- get
      case Map.lookup sid rsWaiting of
        Nothing -> return ()
        Just responder -> do
          respond responder output
          put state {rsWaiting = Map.delete sid rsWaiting}


{- | This defines the various ways a node can be spun up. -}
data StartupMode e
  {- | Indicates that we should bootstrap a new cluster at startup. -}
  = NewCluster
      Peer {- ^ The peer being launched. -}
      ClusterName {- ^ The name of the cluster being launched. -}

  {- | Indicates that the node should try to join an existing cluster. -}
  | JoinCluster
      Peer {- ^ The peer being launched. -}
      ClusterName {- ^ The name of the cluster we are trying to join. -}
      Peer {- ^ The existing peer we are attempting to join with. -}

  {- | Resume operation given the previously saved state. -}
  | Recover
      Peer {- ^ The Peer being recovered. -}
      (EventFold ClusterName Peer e)
      {- ^ The last acknowledged state we had before we crashed. -}
deriving stock instance
    ( Show e
    , Show (Output e)
    , Show (State e)
    )
  =>
    Show (StartupMode e)


{- | Initialize the runtime state. -}
makeRuntimeState :: (EventConstraints e, MonadLoggerIO m)
  => (Peer -> EventFold ClusterName Peer e -> IO ())
     {- ^ Callback when the cluster-wide powerstate changes. -}
  -> StartupMode e
  -> m (RuntimeState e)

makeRuntimeState
    notify
    (NewCluster self clusterId)
  = do
    logInfo "Starting a new cluster."
    {- Build a brand new node state, for the first node in a cluster. -}
    makeRuntimeState
      notify
      (Recover self (EF.new clusterId self))

makeRuntimeState
    notify
    (JoinCluster self _clusterName targetPeer)
  = do
    {- Join a cluster an existing cluster. -}
    logInfo $ "Trying to join an existing cluster on " <> showt addr
    JoinOk cluster <-
      requestJoin
      . JoinRequest
      $ self
    logInfo $ "Join response with cluster: " <> showt cluster
    makeRuntimeState notify (Recover self cluster)
  where
    requestJoin :: (EventConstraints e, MonadLoggerIO m)
      => JoinRequest
      -> m (JoinResponse e)
    requestJoin joinMsg = ($ joinMsg) =<< connectServer addr Nothing

    addr :: AddressDescription
    addr =
      AddressDescription
        (
          unPeer targetPeer
          <> ":" <> showt joinMessagePort
        )

makeRuntimeState
    notify
    (Recover self clusterState)
  = do
    rsNextId <- newSequence
    return
      RuntimeState
        { rsSelf = self
        , rsClusterState = clusterState
        , rsConnections = mempty
        , rsWaiting = mempty
        , rsCalls = mempty
        , rsBroadcalls = mempty
        , rsNextId
        , rsNotify = notify self
        , rsJoins = mempty
        }


{- |
  Initialize a new sequence of messageIds. It would be perfectly fine to ensure
  unique message ids by generating a unique UUID for each one, but generating
  UUIDs is not free, and we are probably going to be generating a lot of these.
-}
newSequence :: (MonadIO m) => m MessageId
newSequence = do
    sid <- getUUID
    pure (M sid 0)
  where
    {- | A utility function that makes a UUID, no matter what.  -}
    getUUID :: (MonadIO m) => m UUID
    getUUID = liftIO nextUUID >>= maybe (wait >> getUUID) return
      where
        wait = liftIO (threadDelay oneMillisecond)
        oneMillisecond = 1000


{- |
  Generate the next message id in the sequence. We would normally use
  `succ` for this kind of thing, but making `MessageId` an instance of
  `Enum` really isn't appropriate.
-}
nextMessageId :: MessageId -> MessageId
nextMessageId (M sequenceId ord) = M sequenceId (succ ord)


{- | Obtain the 'ClusterName'. -}
getClusterName :: Runtime e -> ClusterName
getClusterName = rClusterId


{- | The join message port  -}
joinMessagePort :: PortNumber
joinMessagePort = 5289


{- | Like 'Fork.respond', but returns '()'. -}
respond :: (MonadIO m) => Responder a -> a -> m ()
respond responder = void . Fork.respond responder


{- |
  Retrieve some basic stats that can be used to intuit the health of
  the cluster.
-}
getStats :: (MonadIO m) => Runtime e -> m Stats
getStats runtime = do
  now <- liftIO getTime
  stats <- liftIO $ readIORef (rStats runtime)
  pure
    Stats
      { timeWithoutProgress = diffTimeSpec now <$> stats
      }


