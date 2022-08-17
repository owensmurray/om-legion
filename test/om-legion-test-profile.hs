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


import Control.Lens (Identity)
import Control.Monad.Logger.CallStack (LogLevel(LevelDebug, LevelError,
  LevelInfo, LevelOther, LevelWarn), LoggingT(runLoggingT), MonadLogger,
  logDebug, logError)
import Data.String (IsString(fromString))
import OM.Logging (standardLogging)
import OM.Show (showt)
import Safe (readMay)
import System.Console.GetOpt (ArgDescr(OptArg, ReqArg), ArgOrder(Permute),
  OptDescr(Option), getOpt, usageInfo)
import System.Environment (getArgs)
import System.Exit (ExitCode(ExitFailure, ExitSuccess))
import UnliftIO (MonadIO(liftIO), MonadUnliftIO, forConcurrently_)
import UnliftIO.Process (proc, showCommandForUser, waitForProcess,
  withCreateProcess)


main :: IO ()
main = do
  
  Config
      { namespace
      , logLevel
      , numNodes
      , threads
      , numOps
      }
    <- getConfig
  flip runLoggingT (standardLogging logLevel) $
    forConcurrently_ [0..numNodes - 1] $ \node ->
      runIncs namespace node threads numOps


runIncs
  :: ( MonadFail m
     , MonadLogger m
     , MonadUnliftIO m
     )
  => String
  -> Int
  -> Int
  -> Int
  -> m ()
runIncs namespace node threads numOps = do
  cmd
    "kubectl"
    [ "-n"
    , namespace
    , "exec"
    , "-i"
    , namespace <> "-" <> show node
    , "--"
    , "/bin/om-legion-test-inc"
    , show numOps
    , show threads
    ]


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
            config = foldl (.) id o (Config Nothing LevelInfo Nothing Nothing Nothing)
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
          ""
          ["num-nodes"]
          (ReqArg (\val cfg -> cfg {numNodes = readMay val}) "<num-nodes>")
          "The number of nodes in the cluster."
      , Option
          ""
          ["num-ops"]
          (ReqArg (\val cfg -> cfg {numOps = readMay val}) "<num-ops>")
          "The number of increments to perform on each node, in each thread."
      , Option
          ""
          ["num-threads"]
          (ReqArg (\val cfg -> cfg {threads = readMay val}) "<num-threads>")
          "The number of request threads each node should be hit with."
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


data Config f = Config
  { namespace :: F f String
  ,  logLevel :: LogLevel
  ,  numNodes :: F f Int
  ,    numOps :: F f Int
  ,   threads :: F f Int
  }
deriving stock instance Show (Config Maybe)


type family F a b where
  F Identity a = a
  F f a = f a


validate :: Config Maybe -> Maybe (Config Identity)
validate (Config namespace logLevel numNodes numOps threads) =
  Config
    <$> namespace
    <*> pure logLevel
    <*> numNodes
    <*> numOps
    <*> threads


