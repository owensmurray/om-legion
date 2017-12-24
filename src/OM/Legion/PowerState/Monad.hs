{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{- |
  This module provides a monadic interface for power state manipulation,
  where the monadic context contains the current value of the power state,
  the collection of outputs for events that have reached the infimum,
  and the collection of actions that should be taken to propagate the
  powerstate to all other peers.
-}
module OM.Legion.PowerState.Monad (
  PowerStateT,
  runPowerStateT,

  PropAction(..),

  event,
  merge,
  acknowledge,
  acknowledgeAs,

  getPowerState,

  participate,
  disassociate,
) where

import Control.Exception.Safe (MonadCatch, MonadThrow)
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Logger (MonadLogger, MonadLoggerIO)
import Control.Monad.Trans.Class (MonadTrans, lift)
import Control.Monad.Trans.Except (ExceptT, throwE)
import Control.Monad.Trans.Reader (ReaderT, runReaderT, ask)
import Control.Monad.Trans.State (StateT, runStateT, get, put, modify)
import Control.Monad.Trans.Writer (WriterT, runWriterT, tell)
import Data.Default.Class (Default, def)
import Data.Map (Map)
import OM.Legion.Lift (lift2, lift3, lift4)
import OM.Legion.PowerState (StateId, DifferentOrigins, Event, PowerState)
import qualified OM.Legion.PowerState as PS


{- |
  Monad Transformer that manages the powerstate value, accumulated infimum
  outputs, and the actions necessary for propagation as monadic context.
-}
newtype PowerStateT o s p e r m a = PowerStateT {
    unPowerStateT ::
      StateT (PowerState o s p e r) ( {- Maintain the power state value. -}
      StateT PropAction (             {- Maintain the propagation actions. -}
      ReaderT p (                     {- Provide the 'self' value. -}
      WriterT (Map (StateId p) r)     {- Accumulate the infimum outputs. -}
      m))) a
  }
  deriving (
    Applicative, Functor, Monad, MonadCatch, MonadIO, MonadLogger,
    MonadLoggerIO, MonadThrow
  )
instance (Ord p) => MonadTrans (PowerStateT o s p e r) where
  lift = PowerStateT . lift4


{- | Run a PowerStateT monad.  -}
runPowerStateT :: (Monad m)
  => p {- ^ self -}
  -> PowerState o s p e r
  -> PowerStateT o s p e r m a
  -> m (
        a,
        PropAction,
        PowerState o s p e r,
        Map (StateId p) r
      )
runPowerStateT self ps =
    fmap flatten
    . runWriterT
    . (`runReaderT` self)
    . (`runStateT` def)
    . (`runStateT` ps)
    . unPowerStateT
  where
    {- |
      This just converts the tuple structure of the monad transformation
      stack into the tuple structure we want to expose to the user.
    -}
    flatten (((a, ps2), prop), outputs) = (a, prop, ps2, outputs)


{- | The action that needs to be taken to distribute any new information. -}
data PropAction
  = DoNothing
  | Send
  deriving (Show, Eq)
instance Default PropAction where
  def = DoNothing


{- | Add a user event. Return the projected output of the event. -}
event :: (Monad m, Ord p, Event e r s)
  => e
  -> PowerStateT o s p e r m (r, StateId p)
event e = PowerStateT $ do
  self <- lift2 ask
  (r, sid, ps) <- PS.event self e <$> get
  put ps
  return (r, sid)


{- |
  Monotonically merge the information in two power states.  The resulting
  power state may have a higher infimum value, but it will never
  have a lower one. This function is not total. Only `PowerState`s
  that originated from the same `new` call can be merged. This can
  potentially throw a 'DifferentOrigins' if the origin of @__other__@
  is not the same as the origin of the powerstate in the monadic context.
-}
merge :: (Monad m, Ord p, Eq o, Event e r s)
  => PowerState o s p e r
  -> ExceptT (DifferentOrigins o) (PowerStateT o s p e r m) ()
merge other = do
  ps <- lift (PowerStateT get)
  case PS.mergeEither other ps of
    Left err -> throwE err
    Right (merged, outputs) -> lift . PowerStateT $ do
      lift3 (tell outputs)
      put merged


{- |
  Record the fact that the participant acknowledges the information
  contained in the powerset. The implication is that the participant
  __must__ base all future operations on the result of this function.
-}
acknowledge :: (Monad m, Ord p, Event e r s, Eq e, Eq o)
  => PowerStateT o s p e r m ()
acknowledge = PowerStateT (lift2 ask) >>= acknowledgeAs


{- | Like 'acknowledge', but for an arbigrary participant. -}
acknowledgeAs :: (Monad m, Ord p, Event e r s, Eq e, Eq o)
  => p
  -> PowerStateT o s p e r m ()
acknowledgeAs p = PowerStateT $ do
  ps <- get
  prop <- lift get
  let
    (ps2, outputs) = PS.acknowledge p ps
    prop2 = if ps2 /= ps
      then Send
      else prop
  put ps2
  (lift . put) prop2
  (lift3 . tell) outputs


{- | Return the current value of the power state. -}
getPowerState :: (Monad m, Ord p)
  => PowerStateT o s p e r m (PowerState o s p e r)
getPowerState = PowerStateT get


{- |
  Allow a participant to join in the distributed nature of the power state.
-}
participate :: (Monad m, Ord p) => p -> PowerStateT o s p e r m ()
participate newPeer = PowerStateT $
  modify (PS.participate newPeer)


{- |
  Indicate that a participant is removing itself from participating in
  the distributed power state.
-}
disassociate :: (Monad m, Ord p) => p -> PowerStateT o s p e r m ()
disassociate peer = PowerStateT $
  modify (PS.disassociate peer)


