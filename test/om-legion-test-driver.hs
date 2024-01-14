{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

{- | Description: Run an om-legion tests. -}
module Main (
  main,
) where


import Control.Concurrent (threadDelay)
import Control.Lens (Identity, view)
import Control.Monad (unless)
import Control.Monad.Logger.CallStack (LogLevel(LevelDebug, LevelError,
  LevelInfo, LevelOther, LevelWarn), LoggingT(runLoggingT), MonadLogger,
  MonadLoggerIO, logDebug, logError, logInfo)
import Data.Aeson (Value, eitherDecode)
import Data.Aeson.Lens (AsValue(_Array, _String), key)
import Data.Foldable (for_)
import Data.String (IsString(fromString))
import OM.Fork (Race, runRace)
import OM.Logging (standardLogging)
import OM.Show (showt)
import Prelude (Applicative((<*>), pure), Bool(False, True), Either(Left,
  Right), Eq((==)), Foldable(foldl, null), Maybe(Just, Nothing),
  Monad((>>=)), MonadFail(fail), Num((-)), Semigroup((<>)), Show(show),
  ($), (&&), (.), (<$>), IO, Int, String, flip, id, not)
import System.Console.GetOpt (ArgDescr(OptArg, ReqArg), ArgOrder(Permute),
  OptDescr(Option), getOpt, usageInfo)
import System.Environment (getArgs)
import System.Exit (ExitCode(ExitFailure, ExitSuccess))
import Test.Hspec (Spec, describe, it, shouldReturn)
import Test.Hspec.Runner (defaultConfig, evaluateSummary, runSpec)
import Test.OM.Legion (Op(Inc), Request(OpReq, ReadState, ReadStats),
  k8sConfig)
import UnliftIO (MonadIO(liftIO), MonadUnliftIO, SomeException, finally,
  try)
import UnliftIO.Process (proc, readProcess, readProcessWithExitCode,
  showCommandForUser, waitForProcess, withCreateProcess)
import qualified Data.Text as Text
import qualified Data.Vector as Vector


main :: IO ()
main = do
  config <- getConfig
  let
    logging = standardLogging (logLevel config)
    it_ :: String -> (Race => LoggingT IO ()) -> Spec
    it_ doesWhat action =
      it doesWhat
      . flip shouldReturn ()
      . flip runLoggingT logging
      $ runRace
      $ flip finally (terminateCluster config)
      $ action
  runTests $
    describe "om-legion" $ do
      it_ "scales up" $ do
        launchCluster config
        runOps config 0 [OpReq Inc, OpReq Inc, OpReq Inc]
        resizeCluster (namespace config) 3
        runOps config 1 [OpReq Inc, OpReq Inc, OpReq Inc]
        runOps config 2 [OpReq Inc, OpReq Inc, OpReq Inc]
        for_ [0..2] $ \n ->
          runOps config n [ ReadState, ReadStats ]
        checkStable config 3 9

      it_ "scales down" $ do
        launchCluster config
        resizeCluster (namespace config) 3
        for_ [0..2] $ \n ->
          runOps config n [OpReq Inc, OpReq Inc, OpReq Inc]
        resizeCluster (namespace config) 1
        runOps config 0 [ ReadState, ReadStats ]
        checkStable config 1 9

      it_ "recovers from pod deletion" $ do
        launchCluster config
        resizeCluster (namespace config) 3
        runOps config 2 [OpReq Inc, OpReq Inc, OpReq Inc, ReadStats]
        runOps config 2 [ReadState]
        cmd "kubectl" ["-n", namespace config, "delete", "pod", "test-2"]
        runOps config 0 [OpReq Inc, OpReq Inc, OpReq Inc, ReadStats]
        waitForStable config 3 50 6


runOps
  :: ( MonadFail m
     , MonadLogger m
     , MonadUnliftIO m
     )
  => Config Identity
  -> Int
  -> [Request]
  -> m ()
runOps config n ops = do
  cmd
    "kubectl"
    (
      [ "-n"
      , (namespace config)
      , "run"
      , "test-driver"
      , "--image=us.gcr.io/friendlee/om-legion-test:latest"
      , "-i"
      , "--rm"
      , "--restart=Never"
      , "--env=NAMESPACE=" <> namespace config
      , "--env=LOG_LEVEL=DEBUG"
      , "--serviceaccount=" <> namespace config
      , "--wait=true"
      , "--"
      , "/bin/om-legion-test-run"
      , show n
      ] <> (show <$> ops)
    )


resizeCluster
  :: ( MonadFail m
     , MonadLogger m
     , MonadUnliftIO m
     )
  => String
  -> Int
  -> m ()
resizeCluster namespace n = do
  cmd
    "kubectl"
    [
      "-n",
      namespace,
      "patch",
      "statefulset",
      namespace,
      "-p",
      "{\"spec\": {\"replicas\": " <> showt n <> "}}"
    ]
  for_ [0..n-1] (waitForReady namespace)


waitForReady
  :: ( MonadFail m
     , MonadLogger m
     , MonadUnliftIO m
     )
  => String
  -> Int
  -> m ()
waitForReady namespace n = do
    cmd "kubectl" ["-n", namespace, "get", "pods"]
    json <-
      readProcess
        "kubectl"
        [ "-n"
        , namespace
        , "get"
        , "pod"
        , namespace <> "-" <> show n
        , "-o"
        , "json"
        ]
        ""
    case eitherDecode (fromString json) of
      Left err -> fail $ "Can't check ready: " <> show (n, err)
      Right val ->
        case isReady val of
          True -> pure ()
          False -> do
            logInfo $ "Not yet ready: " <> showt n
            liftIO $ threadDelay 1_000_000
            waitForReady namespace n
  where
    isReady :: Value -> Bool
    isReady =
      not
      . Vector.null
      . Vector.filter
          (\condition ->
            view (key "status" . _String) condition == "True"
            && view (key "type" . _String) condition == "Ready"
          )
      . view (key "status" . key "conditions" . _Array)


getConfig
  :: ( MonadFail m
     , MonadIO m
     )
  => m (Config Identity)
getConfig = do
    let logging = standardLogging LevelDebug
    flip runLoggingT logging $ do
      args <- liftIO $ getArgs
      case getOpt Permute options args of
        (o, [], []) ->
          let
            config = foldl (.) id o (Config Nothing Nothing LevelInfo)
          in
            case validate config of
              Nothing -> do
                logError ("Invalid config: " <> showt config)
                usage
              Just cfg -> pure cfg
        (_, _, err) -> do
          logError ("opt-result: " <> showt err)
          usage
  where
    usage :: (MonadLogger m, MonadFail m) => m a
    usage = do
      logError $ fromString (usageInfo "Usage:" options)
      fail "Invalid usage."

    options :: [OptDescr (Config Maybe -> Config Maybe)]
    options =
      [ Option
          "n"
          ["namespace"]
          (ReqArg (\val cfg -> cfg {namespace = Just val}) "<namespace>")
          "The k8s namespace to deploy to."
      , Option
          "i"
          ["image"]
          (ReqArg (\val cfg -> cfg {image = Just val}) "<docker-image>")
          "The docker image to deploy."
      , Option
          ""
          ["log-level"]
          (
            OptArg
              (\mLevel cfg ->
                case mLevel of
                  Nothing      -> cfg {logLevel = LevelInfo}
                  Just "DEBUG" -> cfg {logLevel = LevelDebug}
                  Just "INFO"  -> cfg {logLevel = LevelInfo}
                  Just "WARN"  -> cfg {logLevel = LevelWarn}
                  Just "ERROR" -> cfg {logLevel = LevelError}
                  Just other   -> cfg {logLevel = LevelOther (fromString other)}
              )
              "<log-level>"
          )
          "The logging level to choose. [DEBUG, INFO, WARN, ERROR]"
      ]


runTests :: Spec -> IO ()
runTests spec =
  runSpec spec defaultConfig >>= evaluateSummary


terminateCluster
  :: forall m.
     ( MonadFail m
     , MonadLoggerIO m
     , MonadUnliftIO m
     )
  => Config Identity
  -> m ()
terminateCluster Config {namespace} =
    finally
      (do
        logDumpedState 0
        logDumpedState 1
        logDumpedState 2
      ) (
        cmd
          "kubectl"
          [ "delete"
          , "namespace"
          , namespace
          ]
      )
  where
    logDumpedState :: Int -> m ()
    logDumpedState n = do
      (_, out, _) <-
        readProcessWithExitCode
          "bash"
          [
            "-c",
            "kubectl exec -i -n "
            <> show namespace
            <> " "
            <> show namespace
            <> "-"
            <> show n
            <> " bash -- -c 'cat log | gzip' | gzip -d"
          ]
          ""
      logInfo $ "logs " <> showt n <> ": " <> showt out


launchCluster
  :: ( MonadFail m
     , MonadLogger m
     , MonadUnliftIO m
     )
  => Config Identity
  -> m ()
launchCluster Config { namespace, image } = do
  readProcessWithExitCode
      "kubectl"
      [
        "apply",
        "-f",
        "-"
      ]
      (Text.unpack (k8sConfig namespace image))
    >>= \case
      (ExitSuccess, out, err) -> do
        logInfo (fromString out)
        unless (null err) $ logError (fromString err)
      err -> fail ("Got apply error: " <> show err)
  waitForReady namespace 0


cmd
  :: ( MonadFail m
     , MonadLogger m
     , MonadUnliftIO m
     )
  => String
  -> [String]
  -> m ()
cmd prog args = do
  withCreateProcess
    (proc prog args)
    (\_ _ _ handle ->
      waitForProcess handle >>= \case
        result@(ExitFailure _) ->
          fail
            $ "Failed:\n"
            <> showCommandForUser prog args
            <> "\nwith: " <> show result
        result@ExitSuccess ->
          logDebug ("got: " <> showt result)
   )


{- | Check that n nodes are reporting stable stats. -}
checkStable
  :: ( MonadFail m
     , MonadLogger m
     , MonadUnliftIO m
     )
  => Config Identity
  -> Int
  -> Int
  -> m ()
checkStable config n v = do
  cmd
    "kubectl"
    [ "-n"
    , (namespace config)
    , "run"
    , "test-driver"
    , "--image=us.gcr.io/friendlee/om-legion-test:latest"
    , "-i"
    , "--rm"
    , "--restart=Never"
    , "--env=NAMESPACE=" <> namespace config
    , "--env=LOG_LEVEL=DEBUG"
    , "--serviceaccount=" <> namespace config
    , "--wait=true"
    , "--"
    , "/bin/om-legion-test-stable"
    , show n
    , show v
    ]


waitForStable
  :: ( MonadFail m
     , MonadLogger m
     , MonadUnliftIO m
     )
  => Config Identity
  -> Int
  -> Int
  -> Int
  -> m ()
waitForStable config n v tries =
  case tries of
    0 -> fail "The cluster doesn't seem like it's going to stabilize."
    _ -> do
      runOps config 0 [ReadState, ReadStats]
      try (checkStable config n v) >>= \case
        Left (_err :: SomeException) -> do
          logInfo $ "Not yet stable."
          waitForStable config n v (tries - 1)
        Right () -> pure ()


data Config f = Config
  { namespace :: F f String
  ,     image :: F f String
  ,  logLevel :: LogLevel
  }
deriving stock instance Show (Config Maybe)


type family F a b where
  F Identity a = a
  F f a = f a


validate :: Config Maybe -> Maybe (Config Identity)
validate (Config namespace image logLevel) =
  Config
    <$> namespace
    <*> image
    <*> pure logLevel


