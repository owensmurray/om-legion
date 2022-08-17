{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

{- |
  Description: a program that runs some operations against a legion
  test node.
-}
module Main (main) where

import Control.Concurrent.Async (mapConcurrently_)
import Control.Monad.Logger.CallStack (LoggingT(runLoggingT), logInfo)
import Data.String (IsString(fromString))
import OM.Logging (parseLevel, standardLogging)
import OM.Show (showj)
import System.Environment (getArgs, getEnv)
import Test.OM.Legion (Op(Inc), Request(OpReq), makeClientLocal, send)


main :: IO ()
main = do
  level <- parseLevel . fromString <$> getEnv "LOG_LEVEL" 
  [numIncrements, threads] <- getArgs
  let logging = standardLogging level
  mapConcurrently_ id . replicate (read threads) $ do
    flip runLoggingT logging $ do
      client <- makeClientLocal
      logInfo "Sending sequence."
      sequence_ . replicate (read numIncrements) $ do
        logInfo "Sending req"
        send client (OpReq Inc) >>= logInfo . ("Recieved: " <>) . showj
      logInfo "Sequence sent."



