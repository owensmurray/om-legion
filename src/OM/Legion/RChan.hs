{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

{-| Description: A specialized channel for incoming runtime messages.  -}
module OM.Legion.RChan (
  RChan,
  RuntimeMessage(..),
  JoinRequest(..),
  readRChan,
  newRChan,
) where

import Control.Concurrent (MVar, newEmptyMVar, putMVar, takeMVar)
import Control.Monad.IO.Class (MonadIO(liftIO))
import Data.Binary (Binary)
import Data.ByteString.Lazy (ByteString)
import Data.CRDT.EventFold (Event(Output, State), Diff, EventFold,
  EventId)
import Data.Map (Map)
import Data.Time (DiffTime)
import GHC.Generics (Generic)
import OM.Fork (Actor(Msg, actorChan), Responder)
import OM.Legion.Connection (JoinResponse)
import OM.Legion.MsgChan (ClusterName, MessageId, Peer)
import Prelude ((.), (<$>), Maybe, Show)

{- | The type of the runtime message channel. -}
newtype RChan e = RChan {
    unRChan :: MVar (RuntimeMessage e)
  }
instance Actor (RChan e) where
  type Msg (RChan e) = RuntimeMessage e
  actorChan = putMVar . unRChan


readRChan
  :: (MonadIO m)
  => RChan e
  -> m (RuntimeMessage e)
readRChan =
  liftIO . takeMVar . unRChan


newRChan :: (MonadIO m) => m (RChan e)
newRChan =
  RChan <$> liftIO newEmptyMVar


{- | The types of messages that can be sent to the runtime. -}
data RuntimeMessage e
  = ApplyFast e (Responder (Output e))
  | ApplyConsistent e (Responder (Output e))
  | Eject Peer (Responder ())
  | Merge (Diff ClusterName Peer e)
  | FullMerge (EventFold ClusterName Peer e)
  | Outputs (Map (EventId Peer) (Output e))
  | Join JoinRequest (Responder (JoinResponse e))
  | ReadState (Responder (EventFold ClusterName Peer e))
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
  | GetStats (Responder (EventFold ClusterName Peer e))
deriving stock instance
    ( Show e
    , Show (Output e)
    , Show (State e)
    )
  =>
    Show (RuntimeMessage e)


{- | This is the type of a join request message. -}
newtype JoinRequest = JoinRequest Peer
  deriving stock (Generic, Show)
instance Binary JoinRequest


