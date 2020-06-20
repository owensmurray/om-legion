
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
  ClusterName(..),
  ClusterGoal(..),
  parseLegionPeer,
  legionPeer,

  -- ** Topology Events.
  {- |
    These types allow your custom cluster state to get changed in response
    to om-legion cluster topology events.
  -}
  TopologySensitive(..),
  TopologyEvent(..),
  ClusterEvent,

) where


import OM.Legion.Management (ClusterGoal(ClusterGoal, cgNumNodes),
  TopologyEvent(CommissionComplete, Terminated, UpdateClusterGoal),
  TopologySensitive(allowDecommission, applyTopology), ClusterEvent, Peer)
import OM.Legion.Runtime (ClusterName(ClusterName, unClusterName),
  StartupMode(JoinCluster, NewCluster, Recover), Runtime, applyConsistent,
  applyFast, broadcall, broadcast, call, cast, eject, forkLegionary,
  getClusterName, getSelf, legionPeer, parseLegionPeer, readState)


