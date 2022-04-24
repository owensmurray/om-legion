{-# LANGUAGE NumericUnderscores #-}

{- | Description: "Scale down" test driver. -}
module Main (main) where

import Control.Monad (when)
import Control.Monad.Logger.CallStack (LoggingT(runLoggingT))
import Data.String (IsString(fromString))
import OM.Legion (Stats(Stats))
import OM.Logging (parseLevel, standardLogging)
import System.Environment (getEnv)
import Test.OM.Legion (Request(ReadStats), Response(ReadStatsR),
  makeClient, send)


main :: IO ()
main = do
  namespace <- fromString <$> getEnv "NAMESPACE"
  level <- parseLevel . fromString <$> getEnv "LOG_LEVEL" 
  let logging = standardLogging level
  flip runLoggingT logging $ do
    actual <-
      sequence
        [ do
            client <- makeClient namespace ord
            send client ReadStats
        | ord <- [0..0]
        ]

    let
      expected :: [Response]
      expected = replicate 1 (ReadStatsR (Stats mempty))
    when (actual /= expected) $
      fail
        $ "(actual /= expected): "
        <> show (actual, expected)


