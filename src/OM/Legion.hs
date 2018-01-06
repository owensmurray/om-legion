
{- | Version 2 of the Legion framework. -}
module OM.Legion (
  -- * Starting up the runtime.
  forkLegionary,
  RuntimeSettings(..),
  StartupMode(..),

  -- * Applying state changes.
  applyFast,
  applyConsistent,

  -- * Sending messages around the cluster.
  cast,
  call,
  broadcast,
  broadcall,

  -- * Inspecting the current state.
  readState,
  getSelf,
  PowerState,
  infimumParticipants,
  projParticipants,
  infimumValue,
  projectedValue,

  -- * Implementing your distributed data type.
  Event(..),

  -- * Other types.
  ForkM(..),
  Runtime,
  ClusterId,
  Peer,
) where


import OM.Legion.Fork (ForkM(forkM))
import OM.Legion.PowerState (Event(apply), PowerState, infimumValue,
   infimumParticipants, infimumValue, projectedValue, projParticipants)
import OM.Legion.Runtime (forkLegionary, StartupMode(NewCluster,
   JoinCluster), Runtime, applyFast, applyConsistent, readState,
   ClusterId, Peer, call, cast, broadcall, broadcast, getSelf)
import OM.Legion.Settings (RuntimeSettings(RuntimeSettings, peerBindAddr,
   joinBindAddr, handleUserCall, handleUserCast))


