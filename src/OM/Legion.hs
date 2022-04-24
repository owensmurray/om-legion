
{- | Version 2 of the Legion framework. -}
module OM.Legion (
  -- * ∙ Starting up the runtime.
  forkLegionary,
  EventConstraints,
  MonadConstraints,
  StartupMode(..),
  Runtime,

  -- * ∙ Applying state changes.
  applyFast,
  applyConsistent,

  -- * ∙ Sending messages around the cluster.
  cast,
  call,
  broadcast,
  broadcall,

  -- * ∙ Inspecting the current state.
  readState,
  getSelf,
  getClusterName,

  -- * ∙ Observability
  getStats,
  Stats(..),

  -- * ∙ Cluster Topology
  eject,
  Peer(..),
  ClusterName(..),

) where


import OM.Legion.Runtime (ClusterName(ClusterName, unClusterName),
  Peer(Peer, unPeer), StartupMode(JoinCluster, NewCluster, Recover),
  Stats(Stats, timeWithoutProgress), EventConstraints, MonadConstraints,
  Runtime, applyConsistent, applyFast, broadcall, broadcast, call, cast,
  eject, forkLegionary, getClusterName, getSelf, getStats, readState)


