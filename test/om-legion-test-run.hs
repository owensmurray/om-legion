{-# LANGUAGE NumericUnderscores #-}

{- |
  Description: a program that runs some operations against a legion
  test node.
-}
module Main (main) where

import Control.Monad.Logger.CallStack (LoggingT(runLoggingT))
import Data.Maybe (maybeToList)
import Data.String (IsString(fromString))
import Numeric.Natural (Natural)
import OM.Logging (parseLevel, standardLogging)
import OM.Show (showj)
import Safe (readMay)
import System.Environment (getArgs, getEnv)
import Test.OM.Legion (Request, makeClient, send)


main :: IO ()
main = do
  namespace <- fromString <$> getEnv "NAMESPACE"
  level <- parseLevel . fromString <$> getEnv "LOG_LEVEL" 
  (ord, reqs) <- parseArgs =<< getArgs
  let logging = standardLogging level
  client <- flip runLoggingT logging $ makeClient namespace ord
  sequence_
    [ send client req >>= putStrLn . showj
    | req <- reqs
    ]
  
parseArgs :: (MonadFail m) => [String] -> m (Natural, [Request])
parseArgs args =
  case args of
    [] -> fail "Needs some arguments."
    n:ops ->
      case readMay n of
        Nothing -> fail "Bad first argment."
        Just ord ->
          pure $ (ord, readMay <$> ops >>= maybeToList)


