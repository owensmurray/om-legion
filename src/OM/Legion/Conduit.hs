{- |
  This module contains some handy conduit abstractions.
-}
module OM.Legion.Conduit (
  chanToSource,
  chanToSink,
  mergeE,
  merge,
) where


import Control.Concurrent (forkIO)
import Control.Concurrent.Chan (Chan, newChan, writeChan, readChan)
import Control.Monad (void)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Conduit (Source, Sink, ($$), ($=), yield, awaitForever)
import qualified Data.Conduit.List as CL (map)


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


{- |
  Merge two sources into one source. This is a concurrency abstraction.
  The resulting source will produce items from either of the input sources
  as they become available. As you would expect from a multi-producer,
  single-consumer concurrency abstraction, the ordering of items produced
  by each source is consistent relative to other items produced by
  that same source, but the interleaving of items from both sources
  is nondeterministic.
-}
mergeE :: (MonadIO io) => Source IO a -> Source IO b -> Source io (Either a b)
mergeE left right = do
  chan <- liftIO newChan
  (liftIO . void . forkIO) (left $= CL.map Left $$ chanToSink chan)
  (liftIO . void . forkIO) (right $= CL.map Right $$ chanToSink chan)
  chanToSource chan


{- |
  Like `mergeE`, but without `Either` in the type signature, because
  both input sources are of the same type.
-}
merge :: (MonadIO io) => Source IO a -> Source IO a -> Source io a
merge left right = mergeE left right $= CL.map unEither
  where
    unEither :: Either a a -> a
    unEither (Left a) = a
    unEither (Right a) = a


