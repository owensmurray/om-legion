
{- |
  This module contains some utilities for dealing with monad transformer
  stacks.
-}
module OM.Legion.Lift (
  lift2,
  lift3,
  lift4,
  lift5,
) where

import Control.Monad.Trans.Class (MonadTrans, lift)

{- | Lift from two levels down in a monad transformation stack. -}
lift2
  :: (
    MonadTrans a,
    MonadTrans b,
    Monad m,
    Monad (a m)
  )
  => m r
  -> b (a m) r
lift2 = lift . lift


{- | Lift from three levels down in a monad transformation stack. -}
lift3
  :: (
    MonadTrans a,
    MonadTrans b,
    MonadTrans c,
    Monad m,
    Monad (a m),
    Monad (b (a m))
  )
  => m r
  -> c (b (a m)) r
lift3 = lift . lift . lift


{- | Lift from four levels down in a monad transformation stack. -}
lift4
  :: (
    MonadTrans a,
    MonadTrans b,
    MonadTrans c,
    MonadTrans d,
    Monad m,
    Monad (a m),
    Monad (b (a m)),
    Monad (c (b (a m)))
  )
  => m r
  -> d (c (b (a m))) r
lift4 = lift . lift . lift . lift


{- | Lift from five levels down in a monad transformation stack. -}
lift5
  :: (
    MonadTrans a,
    MonadTrans b,
    MonadTrans c,
    MonadTrans d,
    MonadTrans e,
    Monad m,
    Monad (a m),
    Monad (b (a m)),
    Monad (c (b (a m))),
    Monad (d (c (b (a m))))
  )
  => m r
  -> e (d (c (b (a m)))) r
lift5 = lift . lift . lift . lift . lift


