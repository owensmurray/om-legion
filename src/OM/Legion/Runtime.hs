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
  eject,
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
import Data.Binary (Binary)
import Data.Conduit (runConduit, (.|), awaitForever, Source, yield)
import Data.Default.Class (Default)
import Data.Map (Map)
import Data.Monoid ((<>))
import Data.Set ((\\))
import Data.UUID (UUID)
import GHC.Generics (Generic)
import OM.Legion.Conduit (chanToSource, chanToSink)
import OM.Legion.Fork (ForkM, forkC, forkM)
import OM.Legion.PowerState (PowerState, Event, StateId)
import OM.Legion.PowerState.Monad (PropAction(DoNothing, Send), event,
   acknowledge, runPowerStateT, merge, PowerStateT, disassociate,
   participate)
import OM.Legion.Settings (RuntimeSettings(RuntimeSettings), peerBindAddr,
   joinBindAddr)
import OM.Legion.UUID (getUUID)
import OM.Show (showt)
import OM.Socket (connectServer, bindAddr, AddressDescription, openEgress,
   Endpoint(Endpoint), openIngress, openServer)
import qualified Data.Conduit.List as CL
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified OM.Legion.PowerState as PS


{- | The Legionary runtime state. -}
data RuntimeState e o s = RuntimeState {
            self :: Peer,
    clusterState :: PowerState ClusterId s Peer e o,
     connections :: Map
                      Peer
                      (PowerState ClusterId s Peer e o -> IO ()),
         waiting :: Map (StateId Peer) (Responder o)
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
applyFast runtime e = call runtime (ApplyFast e)


{- |
  Update the distributed cluster state by applying an event. Both the
  event output and resulting state will be totally consistent throughout
  the cluster.
-}
applyConsistent :: (MonadIO m)
  => Runtime e o s {- ^ The runtime handle. -}
  -> e             {- ^ The event to be applied. -}
  -> m o           {- ^ Returns the strongly consistent event output. -}
applyConsistent runtime e = call runtime (ApplyConsistent e)


{- | Eject a peer from the cluster. -}
eject :: (MonadIO m) => Runtime e o s -> Peer -> m ()
eject runtime peer = cast runtime (Eject peer)


{- | The types of messages that can be sent to the runtime. -}
data RuntimeMessage e o s
  = ApplyFast e (Responder o)
  | ApplyConsistent e (Responder o)
  | Eject Peer
  | Merge (PowerState ClusterId s Peer e o)
  | Join JoinRequest (Responder (JoinResponse e o s))
  deriving (Show)


{- | The thing that identifies a cluster participant. -}
data Peer = Peer {
      _peerId :: UUID,
    peerAddy :: AddressDescription
  }
  deriving (Generic, Show, Eq, Ord)
instance Binary Peer


{- | The thing the identifies a cluster. -}
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
call :: (MonadIO m)
  => Runtime e o s
  -> (Responder a -> RuntimeMessage e o s)
  -> m a
call runtime withResonder = liftIO $ do
  mvar <- newEmptyMVar
  cast runtime (withResonder (Responder (putMVar mvar)))
  takeMVar mvar


{- | Send a message to the runtime. Do not wait for a result. -}
cast :: (MonadIO m) => Runtime e o s -> RuntimeMessage e o s -> m ()
cast runtime = liftIO . writeChan (unRuntime runtime)


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
          lift (handleRuntimeMessage runtime msg)
        )
  where
    startPeerListener :: (ForkM m, MonadCatch m, MonadLoggerIO m) => m ()
    startPeerListener = forkC "peer listener" $ 
      runConduit (
        openIngress (peerBindAddr settings)
        .| CL.map Merge
        .| chanToSink (unRuntime runtime)
      )

    startJoinListener :: (ForkM m, MonadCatch m, MonadLoggerIO m) => m ()
    startJoinListener = forkC "join listener" $
      runConduit (
        openServer (joinBindAddr settings)
        .| awaitForever (\(req, respond_) -> lift $
            call runtime (Join req) >>= respond_
          )
      )


{- | Execute the incoming messages. -}
handleRuntimeMessage :: (
      Binary e, Binary s, Eq e, Event e o s, ForkM m, MonadCatch m,
      MonadLoggerIO m
    )
  => Runtime e o s
  -> RuntimeMessage e o s
  -> StateT (RuntimeState e o s) m ()

handleRuntimeMessage _runtime (ApplyFast e responder) =
  updateCluster $ do
    (o, _sid) <- event e
    respond responder o

handleRuntimeMessage _runtime (ApplyConsistent e responder) =
  updateCluster $ do
    (_v, sid) <- event e
    lift (waitOn sid responder)

handleRuntimeMessage _runtime (Eject peer) =
  updateCluster $ disassociate peer

handleRuntimeMessage _runtime (Merge other) =
  updateCluster $
    runExceptT (merge other) >>= \case
      Left err -> $(logError) $ "Bad cluster merge: " <> showt err
      Right () -> return ()

handleRuntimeMessage _runtime (Join (JoinRequest addr) responder) = do
  peer <- newPeer addr
  updateCluster (participate peer)
  respond responder . JoinOk peer . clusterState =<< get
    

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
    RuntimeState {self} <- get
    mapM_ send . Set.delete self . PS.allParticipants . clusterState =<< get
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

    {- | Disconnect the connection to a peer. -}
    disconnect :: (MonadIO m) => Peer -> StateT (RuntimeState e o s) m ()
    disconnect peer =
      modify (\state@RuntimeState {connections} -> state {
        connections = Map.delete peer connections
      })

    {- | Send a propagation, creating a new connection if need be. -}
    send :: (Binary e, Binary s, ForkM m, MonadCatch m, MonadLoggerIO m)
      => Peer
      -> StateT (RuntimeState e o s) m ()
    send peer = do
      state@RuntimeState {clusterState, connections} <- get
      case Map.lookup peer connections of
        Nothing -> do
          conn <- lift (createConnection peer)
          put state {connections = Map.insert peer conn connections}
          send peer
        Just conn ->
          (liftIO . tryAny) (conn clusterState) >>= \case
            Left err -> do
              $(logWarn) $ "Failure sending to peer: " <> showt (peer, err)
              disconnect peer
            Right () -> return ()

    {- | Create a connection to a peer. -}
    createConnection :: (
          Binary e, Binary s, ForkM m, MonadCatch m, MonadLoggerIO m
        )
      => Peer
      -> m (PowerState ClusterId s Peer e o -> IO ())
    createConnection peer = do
        latest <- liftIO $ atomically (newTVar Nothing)
        forkM . forever $
          (tryAny . runConduit) (
            latestSource latest .| openEgress (peerAddy peer)
          ) >>= \case
            Left err -> $(logWarn) $ "Connection crashed: " <> showt (peer, err)
            Right () -> $(logWarn) "Connection closed for no reason."
        return (atomically . writeTVar latest . Just)
      where
        latestSource :: (MonadIO m)
          => TVar (Maybe (PowerState ClusterId s Peer e o))
          -> Source m (PowerState ClusterId s Peer e o)
        latestSource latest = forever $ yield =<< (liftIO . atomically) (
            readTVar latest >>= \case
              Nothing -> retry
              Just clusterState -> do
                writeTVar latest Nothing
                return clusterState
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
    return RuntimeState {
        self,
        clusterState = PS.new clusterId (Set.singleton self),
        connections = mempty,
        waiting = mempty
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
    return RuntimeState {
        self,
        clusterState = cluster,
        connections = mempty,
        waiting = mempty
      }
  where
    requestJoin :: (
          Binary e,
          Binary o,
          Binary s,
          MonadLoggerIO m,
          Show e,
          Show o,
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


