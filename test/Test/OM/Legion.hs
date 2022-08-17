{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

module Test.OM.Legion (
  S(..),
  Request(..),
  Response(..),
  Op(..),
  k8sConfig,
  Client,
  send,
  makeClient,
  makeClientLocal,
) where


import Control.Monad.IO.Class (MonadIO(liftIO))
import Control.Monad.Logger.CallStack (MonadLoggerIO, logInfo)
import Data.Aeson (KeyValue((.=)), ToJSON, object)
import Data.Binary (Binary)
import Data.CRDT.EventFold (Event(Output, State, apply),
  EventResult(Pure), EventFold)
import Data.Default.Class (Default(def))
import Data.Text (Text)
import GHC.Generics (Generic)
import Language.Haskell.TH.Syntax (addDependentFile)
import Numeric.Natural (Natural)
import OM.Kubernetes (Namespace(Namespace))
import OM.Legion (ClusterName, Peer, Stats)
import OM.Show (showt)
import OM.Socket.Server (AddressDescription(AddressDescription),
  connectServer)
import qualified Text.Mustache as Mustache (Template, substitute)
import qualified Text.Mustache.Compile as Mustache (embedSingleTemplate)


newtype S = S Int
  deriving newtype (Enum, Binary, Show, ToJSON, Eq, Num)
instance Default S where
  def = 0


data Request
  = OpReq Op
  | ReadState
  | ReadStats
  deriving stock (Generic, Show, Read)
  deriving anyclass (Binary)


data Response
  = OpR (Output Op)
  | ReadStateR (EventFold ClusterName Peer Op)
  | ReadStatsR Stats
  deriving stock (Generic, Show, Eq)
  deriving anyclass (Binary, ToJSON)


data Op
  = Inc
  | Dec
  | Get
  deriving stock (Eq, Generic, Show, Read)
  deriving anyclass (Binary, ToJSON)
instance Event Peer Op where
  type Output Op = (S, S)
  type State Op = S

  apply op val =
    let
      newVal :: S
      newVal =
        case op of
          Inc -> succ val
          Dec -> pred val
          Get -> val
    in
      Pure (val, newVal) newVal


k8sConfig
  :: String {- ^ cluster name.  -}
  -> String {- ^ Docker image name.  -}
  -> Text {- ^ k8s config.  -}
k8sConfig name image =
    Mustache.substitute
      template
      (object ["name" .= name, "image" .= image])
  where
    template :: Mustache.Template
    template =
      $(do
        addDependentFile "test/k8s/k8s.mustache"
        Mustache.embedSingleTemplate "test/k8s/k8s.mustache"
      )


newtype Client = Client
  { unClient :: Request -> IO Response
  }


send :: (MonadIO m) => Client -> Request -> m Response
send client = liftIO . unClient client


makeClient :: (MonadLoggerIO m) => Namespace -> Natural -> m Client
makeClient (Namespace namespace) ord = do
  let
    targetPeer =
      namespace <> "-" <> showt ord
      <> "." <> namespace
      <> "." <> namespace
      <> ".svc.cluster.local"
  logInfo $ "Target: " <> showt targetPeer
  Client <$>
    connectServer
      (AddressDescription (targetPeer <> ":9999"))
      Nothing


makeClientLocal :: (MonadLoggerIO m) => m Client
makeClientLocal= do
  Client <$>
    connectServer
      (AddressDescription ("localhost:9999"))
      Nothing


