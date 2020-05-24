{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

{- |
  Description: This module contains the algorith for cluster
               self-management.
-}
module OM.Legion.Management (
  Peer(..),
  Cluster(..),
  TopologyEvent(..),
  ClusterEvent(..),
  ClusterGoal(..),
  RebalanceOrdinal,
  Action(..),
  TopologySensitive(..),
  userEvent,
  topEvent,
) where


import Data.Aeson (FromJSON, ToJSON, ToJSONKey)
import Data.Binary (Binary)
import Data.Default.Class (Default, def)
import Data.Proxy (Proxy(Proxy))
import Data.Semigroup ((<>))
import Data.Set ((\\), Set, member)
import Data.Word (Word64)
import GHC.Generics (Generic)
import Numeric.Natural (Natural)
import OM.PowerState (EventResult(Pure, SystemError), Event, Output,
  State, apply)
import qualified Data.Set as Set


{- | The cluster state. -}
data Cluster = Cluster {
      cGoal :: ClusterGoal,
    cOnline :: Set Peer,
      cPlan :: [Action],
       cOrd :: RebalanceOrdinal
  }
  deriving (Show, Generic)
instance ToJSON Cluster
instance Binary Cluster
instance Default Cluster where
  def = newCluster


{- | Cluster goal. -}
newtype ClusterGoal = ClusterGoal {
    cgNumNodes :: Int
  }
  deriving newtype (Eq, Num, Show, ToJSON, Binary)


{- | Events that can change the cluster topology. -}
data TopologyEvent
  = CommissionComplete Peer
  | UpdateClusterGoal ClusterGoal
  | Terminated Peer
  deriving (Eq, Show, Generic)
instance Binary TopologyEvent
instance ToJSON TopologyEvent
instance Event TopologyEvent where
  type State TopologyEvent = Cluster
  type Output TopologyEvent = ()
  apply e cluster =
    let
      (o, c) = case e of
        CommissionComplete peer ->
          (
            (),
            case currentPlan cluster of
              Just (_ord, action@(Commission p))
                | peer == p -> actionResult action cluster
              _ -> cluster
          )
        UpdateClusterGoal goal ->
          ((), cluster {cGoal = goal})
        Terminated peer ->
          (
            (),
            case currentPlan cluster of
              Just (_, action@(Decommission p))
                | p == peer -> actionResult action cluster
              _ -> cluster
          )
    in
      Pure
        o
        (
          case cPlan c of
            [] -> c { cPlan = plan (cGoal c) (cOnline c) }
            _ -> c
        )


{- | Smart constructor for a user event. -}
userEvent :: e -> ClusterEvent e
userEvent = ClusterEvent . Right

{- | Smart constructor for a topolobyt event. -}
topEvent :: TopologyEvent -> ClusterEvent e
topEvent = ClusterEvent . Left


newtype ClusterEvent e = ClusterEvent {
    _unClusterEvent :: Either TopologyEvent e
  }
  deriving stock (Show)
  deriving newtype (ToJSON, Binary, Eq)
instance (TopologySensitive e, Event e) => Event (ClusterEvent e) where
  type State (ClusterEvent e) = (State TopologyEvent, State e)
  type Output (ClusterEvent e) = Either (Output TopologyEvent) (Output e)
  apply (ClusterEvent (Left e)) (a, b) =
    case (apply e a, applyTopology (Proxy @e) e b) of
      (Pure o a2, b2) -> Pure (Left o) (a2, b2)
      (SystemError o, _) -> SystemError (Left o)
  apply (ClusterEvent (Right e)) (a, b) =
    case apply e b of
      Pure o b2 -> Pure (Right o) (a, b2)
      SystemError o -> SystemError (Right o)

{- |
  om-legion allows your global cluster state to be sensitive to changes
  in the cluster topology. This type class allows that to happen.
-}
class TopologySensitive e where
  {- | Apply the topology change to your application state. -}
  applyTopology :: proxy e -> TopologyEvent -> State e -> State e

  {- |
    If your application is sensitive to the legion cluster topology,
    it may be the case that you need to take action prior to a peer
    being decommissioned.  For instance other peers may need to take
    over resources managed by the outgoing peer. This method asks
    your application state whether it still needs time before actually
    turning off the indicated peer. It is expected that your application
    will automatically recognize that it needs to take some action by
    inspecting its own state, and the state will eventually change to
    a value that returns 'True' for the indicated peer.
  -}
  allowDecommission :: proxy e -> Peer -> State e -> Bool


{- | The identification of a node within the legion cluster. -}
newtype Peer = Peer {
    _unPeerOrdinal :: Natural
    {- TODO: find a suitable 'Positive' implementation. -}
  }
  deriving newtype (
    Eq, Ord, Show, ToJSON, Binary, ToJSONKey, Enum, Num, Integral,
    Real, FromJSON
  )


{- | Rebalancing Actions. -}
data Action
  = Decommission Peer
  | Commission Peer
  deriving (Show, Generic)
instance ToJSON Action
instance Binary Action


{- |
  A monotonically increasing ordinal that identifies the action
  taken. Used so that nodes don't repeat the same action, because
  otherwise they have no way to tell the difference between identical
  actions. Even though the actions themselves should be idempotent,
  having some provision to avoid a hard loop on the same action is good
  for performance.
-}
newtype RebalanceOrdinal = RebalanceOrdinal {
    _unRebalanceOrdinal :: Word64
  }
  deriving newtype (Show, Enum, ToJSON, Binary, Bounded, Eq, Ord)


{- | Result on the cluster of having performed a rebalance action.  -}
actionResult :: Action -> Cluster -> Cluster
actionResult action c =
  let
    newOnline :: Set Peer
    newOnline = 
      case action of
        Decommission peer -> Set.delete peer (cOnline c)
        Commission peer -> Set.insert peer (cOnline c)
  in
    c {
      cOrd = succ (cOrd c),
      cOnline = newOnline,
      cPlan = plan (cGoal c) newOnline
    }


{- | A new cluster with no peers commissioned. -}
newCluster :: Cluster
newCluster =
  let
    goal :: ClusterGoal
    goal = 0

    online :: Set Peer
    online = mempty
  in
    Cluster {
      cGoal = goal,
      cOnline = online,
      cPlan = plan goal online,
      cOrd = minBound
    }


{- | Get the current cluster action plan. -}
currentPlan :: Cluster -> Maybe (RebalanceOrdinal, Action)
currentPlan cluster =
  case cPlan cluster of
    [] -> Nothing
    a:_ -> Just (cOrd cluster, a)


{- |
  Figure out the next action in the rebalancing plan.

  Steps are:

  1) Commission any new peers.
  2) Decommission any obsolete peers.
-}
plan :: ClusterGoal -> Set Peer -> [Action]
plan goal online = 
    commissionMissing <> decommissionObsolete
  where
    {- | The ideal number of peers online, which is all of them. -}
    ideal :: Set Peer
    ideal = Set.fromList (Peer . fromIntegral <$> [1 .. cgNumNodes goal])

    commissionMissing :: [Action]
    commissionMissing =
      [
        Commission peer
        | peer <- Set.toAscList ideal
        , not (peer `member` online)
      ]
      
    decommissionObsolete :: [Action]
    decommissionObsolete =
      Decommission <$> Set.toAscList (online \\ ideal)


