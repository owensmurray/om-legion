{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE UndecidableInstances #-}

{- | Description: Manage connections to other peers. -}
module OM.Legion.Connection (
  JoinResponse(..),
  RuntimeState(..),
  EventConstraints,
  disconnect,
  peerMessagePort,
  sendPeer,
) where

import Control.Concurrent.Async (async)
import Control.Exception.Safe (MonadCatch, finally, tryAny)
import Control.Monad (void)
import Control.Monad.IO.Class (MonadIO(liftIO))
import Control.Monad.Logger.CallStack (LoggingT(runLoggingT),
  MonadLoggerIO(askLoggerIO), MonadLogger, logDebug, logInfo)
import Control.Monad.State (MonadState(get), modify)
import Data.Binary (Binary)
import Data.ByteString.Lazy (ByteString)
import Data.CRDT.EventFold (Event(Output, State), EventFold, EventId)
import Data.Conduit ((.|), ConduitT, awaitForever, runConduit, yield)
import Data.Default.Class (Default)
import Data.Map (Map)
import Data.String (IsString(fromString))
import GHC.Generics (Generic)
import Network.Socket (PortNumber)
import OM.Fork (Responder)
import OM.Legion.MsgChan (Peer(unPeer), ClusterName, MessageId,
  PeerMessage, close, enqueueMsg, newMsgChan, stream)
import OM.Show (showt)
import OM.Socket (AddressDescription(AddressDescription), openEgress)
import System.Clock (TimeSpec)
import qualified Data.Map as Map


{- | A handle on the connection to a peer. -}
newtype Connection e = Connection
  { _unConnection
      :: forall m.
         ( MonadIO m
         , MonadLogger m
         , MonadState (RuntimeState e) m
         )
      => PeerMessage e
      -> m Bool
  }


{- | Create a connection to a peer. -}
createConnection
  :: forall m e.
     ( EventConstraints e
     , MonadCatch m
     , MonadLoggerIO m
     , MonadState (RuntimeState e) m
     )
  => Peer
  -> m (Connection e)
createConnection peer = do
    logInfo $ "Creating connection to: " <> showt peer
    RuntimeState {rsSelf} <- get
    msgChan <- newMsgChan
    logging <- askLoggerIO
    liftIO . void . async . (`runLoggingT` logging) $
      let
        addy :: AddressDescription
        addy =
          AddressDescription
            (
              fromString (unPeer peer)
              <> ":" <> showt peerMessagePort
            )
      in
        finally 
          (
            (tryAny . runConduit) (
              stream rsSelf msgChan
              .| logMessageSend
              .| openEgress addy
            ) >>= \case
              Left err ->
                logInfo $ "Disconnecting because of error: " <> showt err
              Right () -> logInfo "Disconnecting because source dried up."
          )
          (close msgChan)
      
    let
      conn :: Connection e
      conn = Connection (enqueueMsg msgChan)
    modify
      (\state -> state {
        rsConnections = Map.insert peer conn (rsConnections state)
      })
    pure conn
  where
    logMessageSend
      :: forall w.
         (MonadLogger w)
      => ConduitT (Peer, PeerMessage e) (Peer, PeerMessage e) w ()
    logMessageSend =
      awaitForever
        (\msg -> do
          logDebug
            $ "Sending Message to Peer (peer, msg): "
            <> showt (peer, msg)
          yield msg
        )


{- |
  Shorthand for all the constraints needed for the event type. Mainly
  used so that documentation renders better.
-}
type EventConstraints e =
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
  )


{- | The Legionary runtime state. -}
data RuntimeState e = RuntimeState
  {         rsSelf :: Peer
  , rsClusterState :: EventFold ClusterName Peer e
  ,  rsConnections :: Map Peer (Connection e)
  ,      rsWaiting :: Map (EventId Peer) (Responder (Output e))
  ,        rsCalls :: Map MessageId (Responder ByteString)
  ,   rsBroadcalls :: Map
                        MessageId
                        (
                          Map Peer (Maybe ByteString),
                          Responder (Map Peer (Maybe ByteString)),
                          TimeSpec
                        )
  ,       rsNextId :: MessageId
  ,       rsNotify :: EventFold ClusterName Peer e -> IO ()
  ,        rsJoins :: Map
                        (EventId Peer)
                        (Responder (JoinResponse e))
                      {- ^
                        The infimum of the eventfold we send to a
                        new participant must have moved past the
                        participation event itself. In other words,
                        the join must be totally consistent across the
                        cluster. The reason is that we can't make the
                        new participant responsible for applying events
                        that occur before it joined the cluster, because
                        it has no way to ensure that it can collect all
                        such events.  Therefore, this field tracks the
                        outstanding joins until they become consistent.
                      -}
  ,    rsDivergent :: Map Peer (EventId Peer, TimeSpec)
  }


{- | The response to a JoinRequest message -}
newtype JoinResponse e
  = JoinOk (EventFold ClusterName Peer e)
  deriving stock (Generic)
deriving stock instance (EventConstraints e) => Show (JoinResponse e)
instance (EventConstraints e) => Binary (JoinResponse e)


{- | The peer message port. -}
peerMessagePort :: PortNumber
peerMessagePort = 5288


{- | Disconnect the connection to a peer. -}
disconnect
  :: ( MonadLogger m
     , MonadState (RuntimeState e) m
     )
  => Peer
  -> m ()
disconnect peer = do
  logInfo $ "Disconnecting: " <> showt peer
  modify (\state@RuntimeState {rsConnections} -> state {
    rsConnections = Map.delete peer rsConnections
  })


{- | Send a peer message, creating a new connection if need be. -}
sendPeer
  :: forall m e.
     ( EventConstraints e
     , MonadCatch m
     , MonadLoggerIO m
     , MonadState (RuntimeState e) m
     )
  => PeerMessage e
  -> Peer
  -> m ()
sendPeer msg peer = do
    RuntimeState {rsConnections} <- get
    case Map.lookup peer rsConnections of
      Nothing -> do
        conn <- createConnection peer
        sendTheMessage conn
      Just conn ->
        sendTheMessage conn
  where
    sendTheMessage :: Connection e -> m ()
    sendTheMessage (Connection conn) =
      conn msg >>= \case
        True -> pure ()
        False -> disconnect peer


