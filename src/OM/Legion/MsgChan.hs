{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE UndecidableInstances #-}

{- | Description: A specialized channel for outgoing peer messages. -}
module OM.Legion.MsgChan (
  MsgChan,
  Peer(..),
  ClusterName(..),
  MessageId(..),
  PeerMessage(..),
  close,
  enqueueMsg,
  newMsgChan,
  stream,
) where

import Control.Concurrent.STM (TVar, atomically, newTVarIO, readTVar,
  retry, writeTVar)
import Control.Monad (join)
import Control.Monad.IO.Class (MonadIO(liftIO))
import Data.Aeson (FromJSON, FromJSONKey, ToJSON, ToJSONKey)
import Data.Binary (Binary, Word64)
import Data.ByteString.Lazy (ByteString)
import Data.CRDT.EventFold (Event(Output, State), Diff, EventFold)
import Data.Conduit (ConduitT, yield)
import Data.Foldable (traverse_)
import Data.String (IsString)
import Data.Text (Text)
import Data.UUID (UUID)
import GHC.Generics (Generic)
import Web.HttpApiData (FromHttpApiData, ToHttpApiData)


{- | A specialized channel for outgoing peer messages.  -}
newtype MsgChan e = MsgChan
  { _unMsgChan :: TVar (Maybe [PeerMessage e])
  }


{- | Create a new 'MsgChan'. -}
newMsgChan :: (MonadIO m) => m (MsgChan e)
newMsgChan = liftIO $ MsgChan <$> newTVarIO (Just [])


{- | Close a MsgChan. -}
close :: (MonadIO m) => MsgChan e -> m ()
close (MsgChan chan) =
  (liftIO . atomically) (writeTVar chan Nothing)


stream
  :: MonadIO m
  => Peer
  -> MsgChan e
  -> ConduitT void (Peer, PeerMessage e) m ()
stream self (MsgChan chan) =
  (join . liftIO . atomically) $
    readTVar chan >>= \case
      Nothing ->
        {- The stream is closed, so stop.  -}
        pure (pure ())
      Just [] ->
        {- The stream is empty, so wait. -}
        retry
      Just messages -> do
        {-
          The stream is not empty, consume the messages and set the
          stream to empty.
        -}
        writeTVar chan (Just []) 
        pure $ do
          traverse_ (yield . (self,)) messages
          stream self (MsgChan chan)


{- | Enqueue a message to be sent. Return 'False' if the 'MsgChan' is closed.  -}
enqueueMsg :: (MonadIO m) => MsgChan e -> PeerMessage e -> m Bool
enqueueMsg (MsgChan chan) msg =
  join . liftIO . atomically $
    readTVar chan >>= \case
      Nothing -> pure (pure False)
      Just msgs -> do
        writeTVar chan (
            Just $ case msg of
              PMMerge _ ->
                msg : filter
                        (\case {PMMerge _ -> False; _ -> True})
                        msgs
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
        pure (pure True)


{- | The types of messages that can be sent from one peer to another. -}
data PeerMessage e
  = PMMerge (Diff ClusterName Peer e)
    {- ^ Send a powerstate merge. -}
  | PMFullMerge (EventFold ClusterName Peer e)
    {- ^ Send a full merge. -}
  | PMCall Peer MessageId ByteString
    {- ^ Send a user call message from one peer to another. -}
  | PMCast ByteString
    {- ^ Send a user cast message from one peer to another. -}
  | PMCallResponse Peer MessageId ByteString
    {- ^ Send a response to a user call message. -}
  deriving stock (Generic)
deriving stock instance
    ( Show e
    , Show (Output e)
    , Show (State e)
    )
  =>
    Show (PeerMessage e)
instance (Binary e, Binary (Output e), Binary (State e)) => Binary (PeerMessage e)


{- | The identification of a node within the legion cluster. -}
newtype Peer = Peer
  { unPeer :: Text
  }
  deriving newtype (
    Eq, Ord, Show, ToJSON, Binary, ToJSONKey, FromJSON, FromJSONKey,
    FromHttpApiData, ToHttpApiData
  )


{- | The name of a cluster. -}
newtype ClusterName = ClusterName
  { unClusterName :: Text
  }
  deriving newtype (
    IsString, Show, Binary, Eq, ToJSON, FromJSON, Ord, ToHttpApiData,
    FromHttpApiData
  )


{- | Message Identifier. -}
data MessageId = M UUID Word64 deriving stock (Generic, Show, Eq, Ord)
instance Binary MessageId


