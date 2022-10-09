{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

module Main (main) where


import Conduit (MonadTrans(lift), (.|), awaitForever, runConduit)
import Control.Concurrent (myThreadId, throwTo)
import Control.Lens (view)
import Control.Monad (void)
import Control.Monad.IO.Class (MonadIO(liftIO))
import Control.Monad.Logger.CallStack (LoggingT(runLoggingT), MonadLogger,
  logDebug)
import Data.Aeson.Lens (AsPrimitive(_String), AsValue(_Array), key)
import Data.String (IsString(fromString))
import Network.HostName (HostName, getHostName)
import OM.Fork (race, runRace, wait)
import OM.Kubernetes (Pod(unPod), newK8s, queryPods)
import OM.Legion (Peer(Peer), StartupMode(JoinCluster, NewCluster),
  Runtime, applyConsistent, eject, forkLegionary, getSelf, getStats,
  readState)
import OM.Logging (fdLogging, parseLevel, stdoutLogging, teeLogging,
  withStandardFormat)
import OM.Show (showt)
import OM.Socket (openServer)
import System.Environment (getEnv)
import System.Exit (ExitCode(ExitFailure))
import System.IO (IOMode(WriteMode), withFile)
import Test.OM.Legion (Request(OpReq, ReadState, ReadStats), Response(OpR,
  ReadStateR, ReadStatsR), Op)
import qualified Data.Text as Text
import qualified Data.Vector as Vector
import qualified System.Posix.Signals as Signal


main :: IO ()
main = do
    withFile "log" WriteMode $ \logFD ->
      runRace $ do
        logLevel <- (parseLevel . fromString) <$> getEnv "LOG_LEVEL"
        let
          logging =
            withStandardFormat
              logLevel
              (teeLogging stdoutLogging (fdLogging logFD))
        flip runLoggingT logging $ do
          startupMode <- getStartupMode
          runtime <-
            forkLegionary
              pure
              (\_ -> pure ())
              (\_ _ -> pure ())
              500_000
              startupMode

          liftIO . void $ do
            mainThread <- myThreadId
            Signal.installHandler
              Signal.sigTERM
              (Signal.Catch (do
                eject runtime (getSelf runtime)
                throwTo
                  mainThread
                  (ExitFailure (fromIntegral Signal.sigTERM + 128))
              ))
              Nothing

          race "service endpoint" $
            runConduit
              (
                pure ()
                .| openServer "0.0.0.0:9999" Nothing
                .| awaitForever (lift . void . handle runtime)
              )
          wait
  where
    handle :: (MonadIO m) => Runtime Op -> (Request, Response -> m b) -> m b
    handle runtime (req, respond) =
      case req of
        OpReq op -> respond . OpR =<< applyConsistent runtime op
        ReadState -> respond . ReadStateR =<< readState runtime
        ReadStats -> respond . ReadStatsR =<< getStats runtime

    getStartupMode
      :: ( MonadIO m
         , MonadLogger m
         )
      => m (StartupMode Op)
    getStartupMode = do
        domain <- liftIO $ getEnv "DOMAIN"
        hostName <- liftIO $ getHostName
        let self = Peer (fromString (hostName <> "." <> domain))
        nsString <- liftIO $ getEnv "NAMESPACE"
        let
          clusterName :: (IsString a) => a
          clusterName = fromString nsString
        getExistingNodes clusterName >>= \case
          [] -> pure $ NewCluster self clusterName
          node:_ ->
            let
              joinPeer :: Peer
              joinPeer = Peer . fromString $ node <> "." <> domain
            in
              pure $
                JoinCluster
                  self
                  clusterName
                  joinPeer
      where
        getExistingNodes
          :: ( MonadIO m
             , MonadLogger m
             )
          => (forall ns. IsString ns => ns)
          -> m [HostName]
        getExistingNodes namespace = do
            k8s <- newK8s
            logDebug "Querying pods"
            pods <- queryPods k8s namespace [("app", namespace)]
            logDebug $ "Got pods: " <> showt pods
            let
              readyPods :: [Pod]
              readyPods = filter isReady pods
            logDebug $ "Ready pods: " <> showt readyPods
            pure $ getName <$> readyPods
          where
            isReady :: Pod -> Bool
            isReady =
              not
              . Vector.null
              . Vector.filter
                  (\condition ->
                    view (key "status" . _String) condition == "True"
                    && view (key "type" . _String) condition == "Ready"
                  )
              . view (key "status" . key "conditions" . _Array)
              . unPod

            getName :: Pod -> HostName
            getName =
              Text.unpack
              . view (key "metadata" . key "name" . _String)
              . unPod


