{-# LANGUAGE TemplateHaskell #-}

{- |
  This module holds `forkC`, because we use it in at  least two other modules.
-}
module OM.Legion.Fork (
  forkC,
  ForkM(..),
) where

import Control.Concurrent (forkIO)
import Control.Exception.Safe (SomeException, try, MonadCatch)
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO, MonadIO)
import Control.Monad.Logger (logError, askLoggerIO, runLoggingT,
  MonadLoggerIO, LoggingT, MonadLoggerIO)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Reader (ReaderT, runReaderT, ask)
import Data.Text (pack)
import System.Exit (ExitCode(ExitFailure))
import System.IO (hPutStrLn, stderr)
import System.Posix.Process (exitImmediately)

{- |
  Forks a critical thread. "Critical" in this case means that if the thread
  crashes for whatever reason, then the program cannot continue correctly, so
  we should crash the program instead of running in some kind of zombie broken
  state.
-}
forkC :: (ForkM m, MonadCatch m, MonadLoggerIO m)
  => String
    -- ^ The name of the critical thread, used for logging.
  -> m ()
    -- ^ The IO to execute.
  -> m ()
forkC name action =
  forkM $ do
    result <- try action
    case result of
      Left err -> do
        let msg =
              "Exception caught in critical thread " ++ show name
              ++ ". We are crashing the entire program because we can't "
              ++ "continue without this thread. The error was: "
              ++ show (err :: SomeException)
        {- write the message to every place we can think of. -}
        $(logError) . pack $ msg
        liftIO (putStrLn msg)
        liftIO (hPutStrLn stderr msg)
        liftIO (exitImmediately (ExitFailure 1))
      Right v -> return v


{- |
  Class of monads that can be forked. I'm sure there is a better solution for
  this, maybe using MonadBaseControl or something. This needs looking into.
-}
class (Monad m) => ForkM m where
  forkM :: m () -> m ()

instance ForkM IO where
  forkM = void . forkIO

instance (ForkM m, MonadIO m) => ForkM (LoggingT m) where
  forkM action = do
    logging <- askLoggerIO
    lift . forkM $ runLoggingT action logging

instance (ForkM m) => ForkM (ReaderT a m) where
  forkM action = lift . forkM . runReaderT action =<< ask


