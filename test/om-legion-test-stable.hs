{-# LANGUAGE NumericUnderscores #-}

{-# OPTIONS_GHC -Wno-incomplete-uni-patterns #-}

{- | Description: "Scale up" test driver. -}
module Main (main) where

import Control.Monad (unless, when)
import Control.Monad.Logger.CallStack (LoggingT(runLoggingT))
import Data.CRDT.EventFold (infimumValue, projectedValue)
import Data.String (IsString(fromString))
import OM.Legion (Stats(Stats))
import OM.Logging (parseLevel, standardLogging)
import System.Environment (getArgs, getEnv)
import Test.OM.Legion (Request(ReadState, ReadStats), Response(ReadStateR,
  ReadStatsR), S(S), makeClient, send)


main :: IO ()
main = do
  [n, val] <- fmap read <$> getArgs
  namespace <- fromString <$> getEnv "NAMESPACE"
  level <- parseLevel . fromString <$> getEnv "LOG_LEVEL" 
  let logging = standardLogging level
  flip runLoggingT logging $ do
    actual <-
      sequence
        [ do
            client <- makeClient namespace ord
            (,)
              <$> send client ReadStats
              <*> send client ReadState
        | ord <- [0..fromIntegral n - 1]
        ]

    let
      actualStats :: [Response]
      actualStats = fst <$> actual
      expectedStats :: [Response]
      expectedStats = replicate n (ReadStatsR (Stats mempty))
      state:states = snd <$> actual
    when (actualStats /= expectedStats) $
      fail
        $ "(actualStats /= expectedStats): "
        <> show (actualStats, expectedStats)
    unless (all (state ==) states) $
      fail
        $ "Not all states the same: "
        <> show actual

    let
      ReadStateR ef = state
      expectedVals = (S val, S val)
      actualVals = (projectedValue ef, infimumValue ef)

    unless (actualVals == expectedVals) $
      fail
        $ "(actualVals /= expectedVals): "
        <> show (actualVals, expectedVals)


