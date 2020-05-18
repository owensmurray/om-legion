
{- | Version 2 of the Legion framework. -}
module OM.Legion (
  -- * Starting up the runtime.
  forkLegionary,
  StartupMode(..),
  Runtime,

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
  getClusterName,

  -- * Cluster Topology
  Peer,
  ClusterName,
) where


import OM.Legion.Runtime (forkLegionary, StartupMode(NewCluster,
  JoinCluster, Recover), Runtime, applyFast, applyConsistent, readState,
  Peer, call, cast, broadcall, broadcast, getSelf, eject,
  getClusterName, ClusterName)


