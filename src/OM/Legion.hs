
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
  ClusterGoal,
  ClusterEvent,
  parseLegionPeer,
  legionPeer,
) where


import OM.Legion.Management (ClusterEvent, ClusterGoal, Peer)
import OM.Legion.Runtime (StartupMode(JoinCluster, NewCluster, Recover),
  ClusterName, Runtime, applyConsistent, applyFast, broadcall, broadcast,
  call, cast, eject, forkLegionary, getClusterName, getSelf, legionPeer,
  parseLegionPeer, readState)


