{-# OPTIONS_GHC -Wno-deprecations #-}
{- | This module contains some handy conduit abstractions. -}
module OM.Legion.Conduit (
  chanToSource,
  chanToSink,
) where


import Control.Concurrent.Chan (Chan, writeChan, readChan)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Conduit (Source, Sink, yield, awaitForever)


{- |
  Convert a channel into a Source.
-}
chanToSource :: (MonadIO io) => Chan a -> Source io a
chanToSource chan = do
  {-
    Don't use 'Control.Monad.forever' here. For some reason that is unclear to
    me, use of 'forever' creates a space leak, despite the comments in the
    'forever' source code.
    
    The code:

    > forever $ yield =<< liftIO (readChan chan)

    will reliably leak several megabytes of memory over the course of 10k
    messages when tested using the 'legion-discovery' project. This was
    discovered by @-hr@ heap profiling, which pointed to 'chanToSource'
    as the retainer. I think it didn't point to 'forever' as the retainer
    because 'forever' is inlined, and thus does not have a cost-centre
    associated with it.
  -}
  yield =<< liftIO (readChan chan)
  chanToSource chan


{- |
 Convert a channel into a Sink.
-}
chanToSink :: (MonadIO io) => Chan a -> Sink a io ()
chanToSink chan = awaitForever (liftIO . writeChan chan)


