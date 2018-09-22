
{- | Version 2 of the Legion framework. -}
module OM.Legion (
  -- * Starting up the runtime.
  forkLegionary,
  StartupMode(..),
  getAsync,

  -- * Applying state changes.
  applyFast,
  applyConsistent,
  eject,

  -- * Sending messages around the cluster.
  cast,
  call,
  broadcast,
  broadcall,

  -- * Inspecting the current state.
  readState,
  getSelf,
  getClusterId,

  -- * Other types.
  Runtime,
  ClusterId(..),
  Peer(..),
  joinMessagePort,
) where


import OM.Legion.Runtime (forkLegionary, StartupMode(NewCluster,
  JoinCluster, Recover), Runtime, applyFast, applyConsistent, readState,
  ClusterId(ClusterId, unClusterId), Peer(Peer, unPeer), call, cast,
  broadcall, broadcast, getSelf, eject, getAsync, getClusterId,
  joinMessagePort)


