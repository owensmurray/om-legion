{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE ViewPatterns #-}
{-# OPTIONS_GHC -Wmissing-import-lists #-}

{- | The meat of the Legion runtime implementation. -}
module OM.Legion.Runtime (
  -- * Starting the framework runtime.
  forkLegionary,
  Runtime,
  StartupMode(..),

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

  -- * Cluster Topology
  ClusterName(..),
  parseLegionPeer,
  legionPeer,
) where


import Control.Arrow ((&&&))
import Control.Concurrent (Chan, newChan, readChan, threadDelay,
  writeChan)
import Control.Concurrent.Async (Async, async, race_)
import Control.Concurrent.STM (TVar, atomically, newTVar, readTVar,
  retry, writeTVar)
import Control.Exception.Safe (MonadCatch, finally, tryAny)
import Control.Monad (join, mzero, void, when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Identity (runIdentity)
import Control.Monad.Logger (LogStr, LoggingT, MonadLogger, MonadLoggerIO,
  askLoggerIO, logDebug, logError, logInfo, logWarn, runLoggingT)
import Control.Monad.State (MonadState, StateT, evalStateT, get, gets,
  modify, put, runStateT)
import Control.Monad.Trans.Class (lift)
import Data.Aeson (FromJSON, ToJSON)
import Data.Binary (Binary, Word64)
import Data.ByteString.Lazy (ByteString)
import Data.CRDT.EventFold (Event(Output, State),
  UpdateResult(urEventFold, urOutputs), Diff, EventFold, EventId,
  divergent, events, infimumId, infimumValue, origin, projParticipants)
import Data.CRDT.EventFold.Monad (MonadUpdateEF(diffMerge, disassociate,
  event, fullMerge, participate), EventFoldT, runEventFoldT)
import Data.Conduit ((.|), ConduitT, awaitForever, runConduit, yield)
import Data.Default.Class (Default)
import Data.Int (Int64)
import Data.Map (Map)
import Data.Proxy (Proxy(Proxy))
import Data.Set ((\\), Set)
import Data.String (IsString, fromString)
import Data.Text (Text)
import Data.Time (DiffTime, UTCTime, addUTCTime, diffUTCTime,
  getCurrentTime, utctDayTime)
import Data.UUID (UUID)
import Data.UUID.V1 (nextUUID)
import Data.Void (Void)
import GHC.Generics (Generic)
import Network.Socket (PortNumber)
import Numeric.Natural (Natural)
import OM.Fork (Actor, Background, Msg, Responder, actorChan)
import OM.Legion.Conduit (chanToSink)
import OM.Legion.Management (Action(Commission, Decommission), Peer(Peer),
  TopologyEvent(CommissionComplete, Terminated, UpdateClusterGoal),
  ClusterEvent, ClusterGoal, RebalanceOrdinal, TopologySensitive,
  allowDecommission, cOrd, cPlan, cgNumNodes, topEvent, unPeerOrdinal,
  userEvent)
import OM.Logging (withPrefix)
import OM.Show (showt)
import OM.Socket (AddressDescription(AddressDescription),
  Endpoint(Endpoint), connectServer, openEgress, openIngress, openServer)
import System.Clock (TimeSpec)
import System.Random.Shuffle (shuffleM)
import Text.Megaparsec (MonadParsec, Parsec, Token, anySingle, eof,
  lookAhead, manyTill, parseMaybe, satisfy)
import Web.HttpApiData (FromHttpApiData, ToHttpApiData)
import qualified Data.CRDT.EventFold as EF
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.Text as T
import qualified OM.Fork as Fork
import qualified System.Clock as Clock
import qualified Text.Megaparsec as M


{-# ANN module ("HLint: ignore Redundant <$>" :: String) #-}


{- | The Legionary runtime state. -}
data RuntimeState e = RuntimeState {
              rsSelf :: Peer,
      rsClusterState :: EventFold ClusterName Peer (ClusterEvent e),
       rsConnections :: Map
                          Peer
                          (PeerMessage e -> StateT (RuntimeState e) IO ()),
           rsWaiting :: Map (EventId Peer) (Responder (Output e)),
             rsCalls :: Map MessageId (Responder ByteString),
        rsBroadcalls :: Map
                          MessageId
                          (
                            Map Peer (Maybe ByteString),
                            Responder (Map Peer (Maybe ByteString)),
                            TimeSpec
                          ),
            rsNextId :: MessageId,
            rsNotify :: EventFold ClusterName Peer (ClusterEvent e) -> IO (),
             rsJoins :: Map
                          (EventId Peer)
                          (Responder (JoinResponse e)),
                        {- ^
                          The infimum of the powerstate we send to
                          a new participant must have moved past the
                          participation event itself. In other words,
                          the join must be totally consistent across the
                          cluster. The reason is that we can't make the
                          new participant responsible for applying events
                          that occur before it joined the cluster, because
                          it has no way to ensure that it can collect all
                          such events.  Therefore, this field tracks the
                          outstanding joins until they become consistent.
                        -}
     rsCheckpointSid :: EventId Peer,
    rsCheckpointTime :: UTCTime,
           rsLastOrd :: RebalanceOrdinal,
                        {- ^ The last attempted rebalancing step. -}
            rsLaunch :: Peer -> IO (), {- ^ How to launch a new peer. -}
         rsTerminate :: forall void. IO void {- ^ How to terminate ourself. -}
  }


{- | Fork the Legion runtime system. -}
forkLegionary
  :: ( Default (State e)
     , Event e
     , Show e
     , Show (State e)
     , Show (Output e)
     , Eq e
     , Eq (Output e)
     , Binary (State e)
     , Binary e
     , Binary (Output e)
     , TopologySensitive e
     , MonadLoggerIO m
     )
  => IO ClusterGoal
     {- ^ How to get the cluster goal from the connonical source. -}
  -> (Peer -> IO ()) {- ^ How to launch a new peer. -}
  -> (forall void. IO void) {- ^ How to terminate ourself. -}
  -> (ByteString -> IO ByteString) {- ^ Handle a user call request. -}
  -> (ByteString -> IO ()) {- ^ Handle a user cast message. -}
  -> (Peer -> EventFold ClusterName Peer (ClusterEvent e) -> IO ())
     {- ^ Callback when the cluster-wide powerstate changes. -}
  -> StartupMode e
     {- ^
       How to start the runtime, by creating new cluster or joining an
       existing cluster.
     -}
  -> m (Runtime e)
forkLegionary
    getClusterGoal
    launch
    terminate
    handleUserCall
    handleUserCast
    notify
    startupMode
  = do
    rts <- makeRuntimeState notify startupMode launch terminate
    runtimeChan <- RChan <$> liftIO newChan
    logging <- withPrefix (logPrefix (rsSelf rts)) <$> askLoggerIO
    asyncHandle <- liftIO . async . (`runLoggingT` logging) $
      executeRuntime
        getClusterGoal
        handleUserCall
        handleUserCast
        rts
        runtimeChan
    let
      clusterId :: ClusterName
      clusterId = EF.origin (rsClusterState rts)
    return Runtime {
             rChan = runtimeChan,
             rSelf = rsSelf rts,
            rAsync = asyncHandle,
        rClusterId = clusterId
      }
  where
    logPrefix :: Peer -> LogStr
    logPrefix self_ = "[" <> showt self_ <> "] "


{- | A handle on the Legion runtime. -}
data Runtime e = Runtime {
         rChan :: RChan e,
         rSelf :: Peer,
        rAsync :: Async Void,
    rClusterId :: ClusterName
  }
instance Actor (Runtime e) where
  type Msg (Runtime e) = RuntimeMessage e
  actorChan = actorChan . rChan


{- | The type of the runtime message channel. -}
newtype RChan e = RChan {
    unRChan :: Chan (RuntimeMessage e)
  }
instance Actor (RChan e) where
  type Msg (RChan e) = RuntimeMessage e
  actorChan = writeChan . unRChan


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
  -> m (EventFold ClusterName Peer (ClusterEvent e))
readState runtime = Fork.call runtime ReadState


{- |
  Send a user message to some other peer, and block until a response
  is received.
-}
call :: (MonadIO m) => Runtime e -> Peer -> ByteString -> m ByteString
call runtime target msg = Fork.call runtime (Call target msg)


{- | Send the result of a call back to the peer that originated it. -}
sendCallResponse :: (MonadIO m)
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
eject runtime peer = Fork.cast runtime (Eject peer)


{- | Get the identifier for the local peer. -}
getSelf :: Runtime e -> Peer
getSelf = rSelf


{- |
  Get the async handle on the background legion thread, in case you want
  to wait for it to complete (which should never happen except in the
  case of an error).
-}
instance Background (Runtime e) where
  getAsync = rAsync


{- | The types of messages that can be sent to the runtime. -}
data RuntimeMessage e
  = ApplyFast e (Responder (Output e))
  | ManagementEvent TopologyEvent
  | ApplyConsistent e (Responder (Output e))
  | Eject Peer
  | Merge (Diff ClusterName Peer (ClusterEvent e))
  | FullMerge (EventFold ClusterName Peer (ClusterEvent e))
  | Join JoinRequest (Responder (JoinResponse e))
  | ReadState (Responder (EventFold ClusterName Peer (ClusterEvent e)))
  | Call Peer ByteString (Responder ByteString)
  | Cast Peer ByteString
  | Broadcall
      DiffTime
      ByteString
      (Responder (Map Peer (Maybe ByteString)))
  | Broadcast ByteString
  | SendCallResponse Peer MessageId ByteString
  | HandleCallResponse Peer MessageId ByteString
  | Resend (Responder ())
deriving instance
    ( Show e
    , Show (Output e)
    , Show (State e)
    )
  =>
    Show (RuntimeMessage e)


{- | The types of messages that can be sent from one peer to another. -}
data PeerMessage e
  = PMMerge (Diff ClusterName Peer (ClusterEvent e))
    {- ^ Send a powerstate merge. -}
  | PMFullMerge (EventFold ClusterName Peer (ClusterEvent e))
    {- ^ Send a full merge. -}
  | PMCall Peer MessageId ByteString
    {- ^ Send a user call message from one peer to another. -}
  | PMCast ByteString
    {- ^ Send a user cast message from one peer to another. -}
  | PMCallResponse Peer MessageId ByteString
    {- ^ Send a response to a user call message. -}
  deriving (Generic)
deriving instance
    ( Show e
    , Show (Output e)
    , Show (State e)
    )
  =>
    Show (PeerMessage e)
instance (Binary e, Binary (Output e), Binary (State e)) => Binary (PeerMessage e)


{- | Get the node name for a particular peer within a legion cluster. -}
legionPeer :: (IsString a) => ClusterName -> Peer -> a
legionPeer name peer =
    fromString . T.unpack $ unClusterName name <> "-" <> suffix
  where
    suffix :: Text
    suffix = 
      let
        numStr :: Text
        numStr = showt peer
      in
        foldr (<>) "" (replicate (2 - T.length numStr) "0")
        <> numStr

{- | Parse a node name into it's legion peer components. -}
parseLegionPeer :: Text -> Maybe (ClusterName, Peer)
parseLegionPeer =
  let
    dash :: (MonadParsec e s m, Token s ~ Char) => m ()
    dash = void $ satisfy (== '-')

    digit :: (MonadParsec e s m, Token s ~ Char) => m Natural
    digit =
      M.try $
        anySingle >>= \case
          '0' -> pure 0
          '1' -> pure 1
          '2' -> pure 2
          '3' -> pure 3
          '4' -> pure 4
          '5' -> pure 5
          '6' -> pure 6
          '7' -> pure 7
          '8' -> pure 8
          '9' -> pure 9
          _ -> mzero

    ord :: (MonadParsec e s m, Token s ~ Char) => m Peer
    ord =
      M.try (do
        tens <- digit
        ones <- digit
        return (Peer ((tens * 10) + ones))
      )
    
    suffix :: (MonadParsec e s m, Token s ~ Char) => m Peer
    suffix = M.try $ (dash >> ord) <* eof

    name :: (MonadParsec e s m, Token s ~ Char) => m ClusterName
    name = ClusterName . T.pack <$> manyTill anySingle (lookAhead suffix)

    parser :: Parsec () Text (ClusterName, Peer)
    parser = do
      n <- name
      s <- suffix
      return (n, s)
  in
    parseMaybe parser




{- |
  Execute the Legion runtime, with the given user definitions, and
  framework settings. This function never returns (except maybe with an
  exception if something goes horribly wrong).
-}
executeRuntime
  :: ( Constraints e
     , MonadCatch m
     , MonadLoggerIO m
     , MonadFail m
    )
  => IO ClusterGoal
  -> (ByteString -> IO ByteString)
     {- ^ Handle a user call request.  -}
  -> (ByteString -> IO ())
     {- ^ Handle a user cast message. -}
  -> RuntimeState e
  -> RChan e
    {- ^ A source of requests, together with a way to respond to the requets. -}
  -> m Void
executeRuntime
    getClusterGoal
    handleUserCall
    handleUserCast
    rts
    runtimeChan
  = do
    {- Start the various messages sources. -}
    runPeerListener
      `raceLog_` runJoinListener
      `raceLog_` runPeriodicResent
      `raceLog_` clusterResizeLoop
      `raceLog_`
        (`evalStateT` rts)
          (
            let
              -- handleMessages :: StateT (RuntimeState e3) m Void
              handleMessages = do
                msg <- liftIO $ readChan (unRChan runtimeChan)
                RuntimeState {rsClusterState = cluster1} <- get
                $(logDebug) $ "Handling: " <> showt msg
                handleRuntimeMessage msg
                RuntimeState {rsClusterState = cluster2} <- get
                when (cluster1 /= cluster2) $ do
                  $(logDebug) $ "New Cluster State: " <> showt cluster2
                  propagate
                handleBroadcallTimeouts
                handleOutstandingJoins
                handleMessages
            in
              handleMessages
          )
    fail "Legion runtime stopped."
  where
    clusterResizeLoop :: (MonadCatch m, MonadLoggerIO m) => m Void
    clusterResizeLoop =
        tryAny (liftIO getClusterGoal) >>= \case
          Left err -> do
            $(logWarn)
              $ "Problem when checking to see if we need to "
              <> "resize the cluser: " <> showt err
            liftIO (threadDelay 5_000_000)
            clusterResizeLoop
          Right goal -> do
            Fork.cast runtimeChan . ManagementEvent . UpdateClusterGoal $ goal
            sleepUntilTime goal
            clusterResizeLoop
      where
        sleepUntilTime :: (MonadLoggerIO m) => ClusterGoal -> m ()
        sleepUntilTime goal = do
          now <- liftIO getCurrentTime
          let
            periodLength :: Int
            periodLength = cgNumNodes goal * 5

            periodStart :: Int
            periodStart =
              periodLength * (truncate (utctDayTime now) `quot` periodLength)
            
            offset :: Int
            offset = (fromIntegral (unPeerOrdinal (rsSelf rts)) - 1) * 5

            firstOffsetAfterCurrentPeriod =
              addUTCTime (fromIntegral (periodStart + offset)) now {utctDayTime = 0}

            nextTime :: UTCTime
            nextTime =
              if firstOffsetAfterCurrentPeriod <= now
                then
                  addUTCTime
                    (fromIntegral periodLength)
                    firstOffsetAfterCurrentPeriod
                else
                  firstOffsetAfterCurrentPeriod

            sleepTime :: Int
            sleepTime =
              truncate $
                (realToFrac (diffUTCTime nextTime now) :: Rational)
                * 1_000_000

          $(logDebug)
            $ "Sleeping until " <> showt nextTime
            <> " (" <> showt sleepTime <> ")."
            <> showt (now, periodLength, periodStart, offset, firstOffsetAfterCurrentPeriod)

          liftIO (threadDelay sleepTime)


    {- | Like 'race_', but with logging. -}
    raceLog_ :: (MonadLoggerIO m) => LoggingT IO a -> LoggingT IO b -> m ()
    raceLog_ a b = do
      logging <- askLoggerIO
      liftIO $ race_ (runLoggingT a logging) (runLoggingT b logging)

    runPeerListener :: (MonadLoggerIO m, MonadFail m) => m ()
    runPeerListener =
      let
        addy :: AddressDescription
        addy =
          AddressDescription
            (
              legionPeer (origin (rsClusterState rts)) (rsSelf rts)
              <> ":" <> showt peerMessagePort
            )
      in
        runConduit (
          openIngress (Endpoint addy Nothing)
          .| awaitForever (\ (msgSource, msg) -> do
              $(logDebug) $ "Handling: " <> showt (msgSource :: Peer, msg)
              case msg of
                PMFullMerge ps -> yield (FullMerge ps)
                PMMerge ps -> yield (Merge ps)
                PMCall source mid callMsg ->
                  (liftIO . tryAny) (handleUserCall callMsg) >>= \case
                    Left err ->
                      $(logError)
                        $ "User call handling failed with: " <> showt err
                    Right v -> sendCallResponse runtimeChan source mid v
                PMCast castMsg -> liftIO (handleUserCast castMsg)
                PMCallResponse source mid responseMsg ->
                  yield (HandleCallResponse source mid responseMsg)
             )
          .| chanToSink (unRChan runtimeChan)
        )

    runJoinListener :: (MonadLoggerIO m, MonadFail m) => m ()
    runJoinListener =
      let
        addy :: AddressDescription
        addy =
          AddressDescription
            (
              legionPeer (origin (rsClusterState rts)) (rsSelf rts)
              <> ":" <> showt joinMessagePort
            )
      in
        runConduit (
          openServer (Endpoint addy Nothing)
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
        $(logInfo) $ "Completing join (" <> showt sid <> ")."
        respond responder (JoinOk rsClusterState)
      | (sid, responder) <- Map.toList consistent
    ]


{- | Handle any broadcall timeouts. -}
handleBroadcallTimeouts :: (MonadIO m) => StateT (RuntimeState e) m ()
handleBroadcallTimeouts = do
  broadcalls <- gets rsBroadcalls
  now <- getTime
  sequence_ [
      do
        respond responder responses
        modify (\rs -> rs {
            rsBroadcalls = Map.delete messageId (rsBroadcalls rs)
          })
      | (messageId, (responses, responder, expiry)) <- Map.toList broadcalls
      , now >= expiry
    ]


{- | Execute the incoming messages. -}
handleRuntimeMessage :: (
      Constraints e, MonadCatch m, MonadLoggerIO m
    )
  => RuntimeMessage e
  -> StateT (RuntimeState e) m ()

handleRuntimeMessage (ManagementEvent e) =
  updateCluster . void $ event (topEvent e)

handleRuntimeMessage (ApplyFast e responder) =
  updateCluster $
    fst <$> event (userEvent e) >>= \case
      Right o -> respond responder o
      Left o ->
        $(logError) $ "Impossible event output, dropping response: " <> showt o

handleRuntimeMessage (ApplyConsistent e responder) = do
  updateCluster $ do
    (_v, sid) <- event (userEvent e)
    lift (waitOn sid responder)
  rs <- get
  $(logDebug) $ "Waiting: " <> showt (rsWaiting rs)

handleRuntimeMessage (Eject peer) =
  updateClusterAs peer $ do
    void $ event (topEvent (Terminated peer))
    void $ disassociate peer

handleRuntimeMessage (Merge other) =
  updateCluster $
    diffMerge other >>= \case
      Left err -> $(logError) $ "Bad cluster merge: " <> showt err
      Right () -> return ()

handleRuntimeMessage (FullMerge other) =
  updateCluster $
    fullMerge other >>= \case
      Left err -> $(logError) $ "Bad cluster merge: " <> showt err
      Right () -> return ()

handleRuntimeMessage (Join (JoinRequest peer) responder) = do
  $(logInfo) $ "Handling join from peer: " <> showt peer
  sid <- updateCluster (do
      void $ disassociate peer
      void $ event (topEvent (CommissionComplete peer))
      participate peer
    )
  RuntimeState {rsClusterState} <- get
  if sid <= infimumId rsClusterState
    then do
      $(logInfo) $ "Join immediately with: " <> showt rsClusterState
      respond responder (JoinOk rsClusterState)
    else do
      $(logInfo) $ "Join delayed (" <> showt sid <> ")."
      modify (\s -> s {rsJoins = Map.insert sid responder (rsJoins s)})

handleRuntimeMessage (ReadState responder) =
  respond responder . rsClusterState =<< get

handleRuntimeMessage (Call target msg responder) = do
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
  sendPeer (PMCast msg) target

handleRuntimeMessage (Broadcall timeout msg responder) = do
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
  mapM_ (sendPeer (PMCast msg)) =<< getPeers

handleRuntimeMessage (SendCallResponse target mid msg) = do
  source <- gets rsSelf
  sendPeer (PMCallResponse source mid msg) target

handleRuntimeMessage (HandleCallResponse source mid msg) = do
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
updateCluster :: (
      Constraints e, MonadCatch m, MonadLoggerIO m
    )
  => EventFoldT ClusterName Peer (ClusterEvent e) (StateT (RuntimeState e) m) a
  -> StateT (RuntimeState e) m a
updateCluster action = do
  RuntimeState {rsSelf} <- get
  updateClusterAs rsSelf action


{- |
  Like 'updateCluster', but perform the operation on behalf of a specified
  peer. This is required for e.g. the peer eject case, when the ejected peer
  may not be able to perform acknowledgements on its own behalf.
-}
updateClusterAs :: (
      Constraints e, MonadCatch m, MonadLoggerIO m
    )
  => Peer
  -> EventFoldT
       ClusterName
       Peer
       (ClusterEvent e)
       (StateT (RuntimeState e) m)
       a
  -> StateT (RuntimeState e) m a
updateClusterAs asPeer action = do
  RuntimeState {rsClusterState} <- get
  (v, ur) <- runEventFoldT asPeer rsClusterState action
  liftIO . ($ urEventFold ur) . rsNotify =<< get
  modify (\state -> state {rsClusterState = urEventFold ur})
  respondToWaiting (urOutputs ur)
  kickoffRebalance
  return v

kickoffRebalance
  :: forall m e.
     ( Constraints e
     , MonadCatch m
     , MonadLoggerIO m
     , MonadState (RuntimeState e) m
     )
  => m ()
kickoffRebalance = do
  ((cluster, appState), (lastOrd, (self, (launch, terminate)))) <-
    gets (
      (infimumValue . rsClusterState)
      &&& rsLastOrd
      &&& rsSelf
      &&& rsLaunch
      &&& rsTerminate
    )
  $(logDebug)
    $ "Kicking off rebalance: " <> showt (lastOrd, cOrd cluster, cPlan cluster)
  when (lastOrd < cOrd cluster) $
    case cPlan cluster of
      [] -> pure ()
      Decommission peer : _
        | peer == self && allowDecommission (Proxy @e) peer appState -> do
            modify (\rs ->
                rs {
                  rsClusterState =
                    let
                      (_, ur) =
                        runIdentity . runEventFoldT self (rsClusterState rs) $ do
                          void $ event (topEvent (Terminated self))
                          disassociate self
                    in
                      urEventFold ur
                }
              )
            propagate
            $(logDebug) "About to terminate"
            liftIO terminate
        | otherwise -> pure ()
      Commission peer : _ -> do
        $(logDebug) $ "Launching peer: " <> showt peer
        logging <- askLoggerIO
        void . liftIO . async . (`runLoggingT` logging) $ do
          $(logInfo) "Calling launch."
          lift (tryAny (launch peer)) >>= \case
            Left err -> $(logError) $ "Launch failed with: " <> showt (peer, err)
            Right () -> $(logInfo) $ "Launch of peer complete: " <> showt peer
        modify (\rs -> rs { rsLastOrd = succ (rsLastOrd rs)})


{- | Wait on a consistent response for the given state id. -}
waitOn :: (Monad m)
  => EventId Peer
  -> Responder (Output e)
  -> StateT (RuntimeState e) m ()
waitOn sid responder =
  modify (\state@RuntimeState {rsWaiting} -> state {
    rsWaiting = Map.insert sid responder rsWaiting
  })


{- | Propagates cluster information if necessary. -}
propagate
  :: ( Constraints e
     , MonadCatch m
     , MonadLoggerIO m
     , MonadState (RuntimeState e) m
     )
  => m ()
propagate = do
    (self, (cluster, (checkSid, checkTime))) <-
      gets
        (
          rsSelf
          &&& rsClusterState
          &&& rsCheckpointSid
          &&& rsCheckpointTime
        )
    let
      targets = Set.delete self $
        EF.allParticipants cluster

    now <- liftIO getCurrentTime
    let
      isDivergent :: Bool
      isDivergent = not (Map.null (divergent cluster))

    liftIO (shuffleM (Set.toList targets)) >>= \case
      [] -> return ()
      target:_ ->
        {-
          If it has been more than 10 seconds since progress on the
          infimum was made, try sending out a full merge instead of just
          the event pack.
        -}
        if isDivergent
           && infimumId cluster == checkSid
           && diffUTCTime now checkTime > 10
        then do
          $(logWarn)
            $ "Sending full merge because no progress has been "
            <> "made on the cluster state."
          sendPeer (PMFullMerge cluster) target
          modify (\rs -> rs {rsCheckpointTime = now})
        else sendPeer (PMMerge (events target cluster)) target
    modify (\rs -> rs {
        rsCheckpointSid = infimumId cluster,
        rsCheckpointTime =
          {-
            Don't advance the checkpoint time unless the infimum has
            also advanced.
          -}
          if infimumId cluster == checkSid && isDivergent
            then rsCheckpointTime rs
            else now
      })
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
      mapM_ disconnect $
        Map.keysSet conns \\ EF.allParticipants cluster


{- | Send a peer message, creating a new connection if need be. -}
sendPeer
  :: ( Constraints e
     , MonadCatch m
     , MonadLoggerIO m
     , MonadState (RuntimeState e) m
     )
  => PeerMessage e
  -> Peer
  -> m ()
sendPeer msg peer = do
  state@RuntimeState {rsConnections} <- get
  case Map.lookup peer rsConnections of
    Nothing -> do
      conn <- createConnection peer
      put state {rsConnections = Map.insert peer conn rsConnections}
      sendPeer msg peer
    Just conn -> do
      rs <- get
      (liftIO . tryAny) (runStateT (conn msg) rs) >>= \case
        Left err -> do
          $(logWarn) $ "Failure sending to peer: " <> showt (peer, err)
          disconnect peer
        Right ((), rs2) -> do
          put rs2
          $(logDebug) $ "Sent message to peer: " <> showt (peer, msg)
          return ()


{- | Disconnect the connection to a peer. -}
disconnect
  :: ( MonadLogger m
     , MonadState (RuntimeState e) m
     )
  => Peer
  -> m ()
disconnect peer = do
  $(logInfo) $ "Disconnecting: " <> showt peer
  modify (\state@RuntimeState {rsConnections} -> state {
    rsConnections = Map.delete peer rsConnections
  })


{- | Create a connection to a peer. -}
createConnection
  :: ( Constraints e
     , MonadCatch m
     , MonadIO w
     , MonadLoggerIO m
     , MonadState (RuntimeState e) m
     , MonadState (RuntimeState e) w
     )
  => Peer
  -> m (PeerMessage e -> w ())
createConnection peer = do
    $(logInfo) $ "Creating connection to: " <> showt peer
    rts@RuntimeState {rsSelf} <- get
    latest <- liftIO $ atomically (newTVar (Just []))
    logging <- askLoggerIO
    liftIO . void . async . (`runLoggingT` logging) $
      let
        addy :: AddressDescription
        addy =
          AddressDescription
            (
              legionPeer (origin (rsClusterState rts)) peer
              <> ":" <> showt peerMessagePort
            )
      in
        finally 
          (
            (tryAny . runConduit) (
              latestSource rsSelf latest .| openEgress addy
            )
          )
          (liftIO $ atomically (writeTVar latest Nothing))
      
    return (\msg ->
        join . liftIO . atomically $
          readTVar latest >>= \case
            Nothing ->
              (pure . (`runLoggingT` logging) . disconnect) peer
            Just msgs -> do
              writeTVar latest (
                  Just $ case msg of
                    PMMerge _ ->
                      msg : filter (\case {PMMerge _ -> False; _ -> True}) msgs
                    PMFullMerge _ ->
                      {-
                        Full merges override both older full merges and
                        older partial merges.
                      -}
                      msg : filter
                              (\case
                                PMMerge _ -> False
                                PMFullMerge _ -> False
                                _ -> True
                              )
                              msgs
                    _ -> msgs ++ [msg] 
                )
              return (return ())
      )
  where
    latestSource :: (MonadIO m)
      => Peer
      -> TVar (Maybe [PeerMessage e])
      -> ConduitT void (Peer, PeerMessage e) m ()
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
respondToWaiting :: (MonadLoggerIO m, Show (Output e))
  => Map (EventId Peer) (Output (ClusterEvent e))
  -> StateT (RuntimeState e) m ()
respondToWaiting available = do
    rs <- get
    $(logDebug)
      $ "Responding to: " <> showt (available, Map.keysSet (rsWaiting rs))
    mapM_ respondToOne (Map.toList available)
  where
    respondToOne :: (Show (Output (ClusterEvent e)), MonadLoggerIO m)
      => (EventId Peer, Output (ClusterEvent e))
      -> StateT (RuntimeState e) m ()
    respondToOne (sid, output) = do
      state@RuntimeState {rsWaiting} <- get
      case Map.lookup sid rsWaiting of
        Nothing -> return ()
        Just responder -> do
          case output of
            Left _ ->
              $(logError)
                $ "Impossible event output, dropping response: "
                <> showt output
            Right o ->
              respond responder o
          put state {rsWaiting = Map.delete sid rsWaiting}


{- | This defines the various ways a node can be spun up. -}
data StartupMode e
  {- | Indicates that we should bootstrap a new cluster at startup. -}
  = NewCluster
      Peer {- ^ The peer being launched. -}
      ClusterGoal {- ^ The inital cluster size goal. -}
      ClusterName {- ^ The name of the cluster being launched. -}

  {- | Indicates that the node should try to join an existing cluster. -}
  | JoinCluster
      Peer {- ^ The peer being launched. -}
      ClusterName {- ^ The name of the cluster we are trying to join. -}
      Peer {- ^ The existing peer we are attempting to join with. -}

  {- | Resume operation given the previously saved state. -}
  | Recover
      Peer {- ^ The Peer being recovered. -}
      (EventFold ClusterName Peer (ClusterEvent e))
      {- ^ The last acknowledged state we had before we crashed. -}
deriving instance
    ( Show e
    , Show (Output e)
    , Show (State e)
    )
  =>
    Show (StartupMode e)


{- | Initialize the runtime state. -}
makeRuntimeState :: (Constraints e, MonadLoggerIO m)
  => (Peer -> EventFold ClusterName Peer (ClusterEvent e) -> IO ())
     {- ^ Callback when the cluster-wide powerstate changes. -}
  -> StartupMode e
  -> (Peer -> IO ()) {- ^ Launch a peer -}
  -> (forall v. IO v) {- ^ kill ourself -}
  -> m (RuntimeState e)

makeRuntimeState
    notify
    (NewCluster self goal clusterId)
    launch
    terminate
  =
    {- Build a brand new node state, for the first node in a cluster. -}
    let
      (_, ur) =
        runIdentity $
          runEventFoldT self (EF.new clusterId self) (do
            void $ event (topEvent (UpdateClusterGoal goal))
            void $ event (topEvent (CommissionComplete self))
          )
    in
      makeRuntimeState
        notify
        (Recover self (urEventFold ur))
        launch
        terminate

makeRuntimeState
    notify
    (JoinCluster self clusterName targetPeer)
    launch
    terminate
  = do
    {- Join a cluster an existing cluster. -}
    $(logInfo) $ "Trying to join an existing cluster on " <> showt addr
    JoinOk cluster <-
      requestJoin
      . JoinRequest
      $ self
    $(logInfo) $ "Join response with cluster: " <> showt cluster
    makeRuntimeState notify (Recover self cluster) launch terminate
  where
    requestJoin :: (Constraints e, MonadLoggerIO m)
      => JoinRequest
      -> m (JoinResponse e)
    requestJoin joinMsg = ($ joinMsg) =<< connectServer addr

    addr :: AddressDescription
    addr =
      legionPeer clusterName targetPeer
      <> ":" <> showt joinMessagePort

makeRuntimeState
    notify
    (Recover self clusterState)
    launch
    terminate
  = do
    now <- liftIO getCurrentTime
    rsNextId <- newSequence
    return RuntimeState {
        rsSelf = self,
        rsClusterState = clusterState,
        rsConnections = mempty,
        rsWaiting = mempty,
        rsCalls = mempty,
        rsBroadcalls = mempty,
        rsNextId,
        rsNotify = notify self,
        rsJoins = mempty,
        rsCheckpointTime = now,
        rsCheckpointSid = infimumId clusterState,
        rsLastOrd = minBound,
        rsLaunch = launch,
        rsTerminate = terminate
      }


{- | This is the type of a join request message. -}
newtype JoinRequest = JoinRequest Peer
  deriving (Generic, Show)
instance Binary JoinRequest


{- | The response to a JoinRequest message -}
newtype JoinResponse e
  = JoinOk (EventFold ClusterName Peer (ClusterEvent e))
  deriving (Generic)
deriving instance (Constraints e) => Show (JoinResponse e)
instance (Constraints e) => Binary (JoinResponse e)


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


type Constraints e =
  ( Binary (State e)
  , Binary e
  , Default (State e)
  , Eq e
  , Event e
  , Show e
  , Show (State e)
  , Show (Output e)
  , Eq (Output e)
  , Binary (Output e)
  , TopologySensitive e
  )


{- | Obtain the 'ClusterName'. -}
getClusterName :: Runtime e -> ClusterName
getClusterName = rClusterId


{- | The peer message port. -}
peerMessagePort :: PortNumber
peerMessagePort = 5288


{- | The join message port  -}
joinMessagePort :: PortNumber
joinMessagePort = 5289


{- | Like 'Fork.respond', but returns '()'. -}
respond :: (MonadIO m) => Responder a -> a -> m ()
respond responder = void . Fork.respond responder


{- | Add a 'DiffTime' to a 'TimeSpec'. -}
addTime :: DiffTime -> TimeSpec -> TimeSpec
addTime diff time =
  let
    rat = toRational diff

    secDiff :: Int64
    secDiff = truncate rat

    nsecDiff :: Int64
    nsecDiff = truncate ((toRational diff - toRational secDiff) * 1_000_000_000)
  in
    Clock.TimeSpec {
      Clock.sec = Clock.sec time + secDiff,
      Clock.nsec = Clock.nsec time + nsecDiff
    }


{- | Specialized 'Clock.getTime'. -}
getTime :: (MonadIO m) => m TimeSpec
getTime = liftIO $ Clock.getTime Clock.Monotonic


{- | The name of a cluster. -}
newtype ClusterName = ClusterName {
    unClusterName :: Text
  }
  deriving newtype (
    IsString, Show, Binary, Eq, ToJSON, FromJSON, Ord, ToHttpApiData,
    FromHttpApiData
  )


-- {- |
--   Property that ensures anything produced by `legionPeer` can be parsed by
--   `parseLegionPeer`.
-- -}
-- prop_parseLegionPeer :: Property
-- prop_parseLegionPeer =
--   let
--     values = do
--       name <- ClusterName . T.pack <$> arbitrary
--       ord <- PeerOrdinal . fromInteger <$> choose (0, 99)
--       let nodeName = legionPeer name ord
--       return $
--         if Just (name, ord) == parseLegionPeer nodeName
--           then Nothing
--           else Just $ "Failed on: " <> show nodeName
--   in
--     forAll values (== Nothing)


