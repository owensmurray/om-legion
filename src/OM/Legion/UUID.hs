
{- | Contains common UUID functionality.  -}
module OM.Legion.UUID (
  getUUID,
) where


import Control.Concurrent (threadDelay)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.UUID (UUID)
import Data.UUID.V1 (nextUUID)


{- | A utility function that makes a UUID, no matter what.  -}
getUUID :: (MonadIO io) => io UUID

getUUID = liftIO nextUUID >>= maybe (wait >> getUUID) return
  where
    wait = liftIO (threadDelay oneMillisecond)
    oneMillisecond = 1000


