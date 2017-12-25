
{- | Version 2 of the Legion framework. -}
module OM.Legion (
  -- * Starting up the runtime.
  forkLegionary,
  RuntimeSettings(..),
  StartupMode(..),

  -- * Applying state changes.
  applyFast,
  applyConsistent,

  -- * Implementing your distributed data type.
  Event(..),

  -- * Other types.
  ForkM(..),
  Runtime,
) where


import OM.Legion.Fork (ForkM(forkM))
import OM.Legion.PowerState (Event(apply))
import OM.Legion.Runtime (forkLegionary, StartupMode(NewCluster,
   JoinCluster), Runtime, applyFast, applyConsistent)
import OM.Legion.Settings (RuntimeSettings(RuntimeSettings, peerBindAddr,
   joinBindAddr))


