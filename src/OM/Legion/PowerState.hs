{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{- |
  This module contains the fundamental distributed data object.

  A note on terminology: "divergent" in this context referes to events
  which are not known to have been acknowledged by all participating
  peers.
-}
module OM.Legion.PowerState (
  PowerState,
  Event(..),
  StateId,
  DifferentOrigins(..),

  new,
  event,
  mergeMaybe,
  mergeEither,
  acknowledge,

  participate,
  disassociate,

  projectedValue,
  infimumValue,
  infimumParticipants,
  allParticipants,
  projParticipants,
  divergent,
) where

import Prelude hiding (null)

import Data.Aeson (ToJSON, toJSON, object, (.=), eitherDecode)
import Data.Binary (Binary(put, get))
import Data.Default.Class (Default(def))
import Data.DoubleWord (Word256(Word256), Word128(Word128))
import Data.Map (Map, filterWithKey, unionWith, minViewWithKey, keys,
  toDescList, toAscList, fromAscList)
import Data.Set (Set, union, (\\), null, member)
import Data.Word (Word64)
import GHC.Generics (Generic)
import qualified Data.ByteString.Lazy.Char8 as BSL8
import qualified Data.Map as Map
import qualified Data.Set as Set


{- |
  This represents the set of all possible future values of @s@, in a
  distributed, monotonically increasing environment. The term "power
  state" is chosen to indicate that values of this type represent multiple
  possible values of the underlying user state @s@.
-}
data PowerState o s p e r = PowerState {
     origin :: o,
    infimum :: Infimum s p,
     events :: Map (StateId p) (Delta p e, Set p)
  } deriving (Generic, Show, Eq)
instance
    (Binary o, Binary s, Binary p, Binary e)
  =>
    Binary (PowerState o s p e r)
instance (Show o, Show s, Show p, Show e) => ToJSON (PowerState o s p e r) where
  toJSON PowerState {origin, infimum, events} = object [
      "origin" .= show origin,
      "infimum" .= infimum,
      "events" .= Map.fromList [
          (show sid, (show e, Set.map show ps))
          | (sid, (e, ps)) <- Map.toList events
        ]
    ]


{- |
  `Infimum` is the infimum, or greatest lower bound, of the possible
  values of @s@.
-}
data Infimum s p = Infimum {
         stateId :: StateId p,
    participants :: Set p,
      stateValue :: s
  } deriving (Generic, Show)
instance (Binary s, Binary p) => Binary (Infimum s p)
instance (Eq p) => Eq (Infimum s p) where
  Infimum s1 _ _ == Infimum s2 _ _ = s1 == s2
instance (Ord p) => Ord (Infimum s p) where
  compare (Infimum s1 _ _) (Infimum s2 _ _) = compare s1 s2
instance (Show s, Show p) => ToJSON (Infimum s p) where
  toJSON Infimum {stateId, participants, stateValue} = object [
      "stateId" .= show stateId,
      "participants" .= Set.map show participants,
      "stateValue" .=
        {-
          If this `Show` instance of the state happens to decode as valid
          JSON, then use the decoded json inline instead of quoting it.
        -}
        case eitherDecode (BSL8.pack (show stateValue)) of
          Left _ -> toJSON (show stateValue)
          Right val -> val
    ]


{- |
  `StateId` is a monotonically increasing, totally ordered identification
  value which allows us to lend the attribute of monotonicity to state
  operations which would not naturally be monotonic.
-}
data StateId p
  = BottomSid
  | Sid Word256 p
  deriving (Generic, Eq, Ord, Show)
instance (Binary p) => Binary (StateId p) where
  put = put . toMaybe
    where
      toMaybe :: StateId p -> Maybe (Word64, Word64, Word64, Word64, p)
      toMaybe BottomSid =
        Nothing
      toMaybe (Sid (Word256 (Word128 a b) (Word128 c d)) p) =
        Just (a, b, c, d, p)
  get = do
    theThing <- get
    return $ case theThing of
      Nothing -> BottomSid
      Just (a, b, c, d, p) -> Sid (Word256 (Word128 a b) (Word128 c d)) p
instance Default (StateId p) where
  def = BottomSid


{- |
  This is the exception type for illegal merges. An illegal merge is
  one where the two PowerStates do not share the same origin.
-}
data DifferentOrigins o = DifferentOrigins o o
  deriving (Show)


{- |
  `Delta` is how we represent mutations to the power state.
-}
data Delta p e
  = Join p
  | UnJoin p
  | Event e
  deriving (Generic, Show, Eq)
instance (Binary p, Binary e) => Binary (Delta p e)


{- | The class which allows for event application. -}
class Event e o s | e -> s o where
  {- | Apply an event to a state value. **This function MUST be total!!!** -}
  apply :: e -> s -> (o, s)
{- |
  Some applications don't maintain any particular cluster-wide state, and
  only want to use Legion for the peer management and communications. For
  those applications, this instance is provided as a convenient no-op
  'Event' instance.
-}
instance Event () () () where
  apply () () = ((), ())


{- |
  Construct a new PowerState with the given origin and initial
  participants.
-}
new :: (Default s) => o -> Set p -> PowerState o s p e r
new origin participants =
  PowerState {
      origin,
      infimum = Infimum {
          stateId = def,
          participants,
          stateValue = def
        },
      events = Map.empty
    }


{- |
  Monotonically merge the information in two power states.  The resulting
  power state may have a higher infimum value, but it will never have
  a lower one. Only 'PowerState's that originated from the same 'new'
  call can be merged. If the origins are mismatched, then 'Nothing'
  is returned.
-}
mergeMaybe :: (Eq o, Event e r s, Ord p)
  => PowerState o s p e r
  -> PowerState o s p e r
  -> Maybe (PowerState o s p e r, Map (StateId p) r)
mergeMaybe a b = either (const Nothing) Just (mergeEither a b)


{- |
  Like `mergeMaybe`, but returns a human-decipherable error message of
  exactly what went wrong.
-}
mergeEither :: (Eq o, Event e r s, Ord p)
  => PowerState o s p e r
  -> PowerState o s p e r
  -> Either (DifferentOrigins o) (PowerState o s p e r, Map (StateId p) r)
mergeEither (PowerState o1 i1 d1) (PowerState o2 i2 d2) | o1 == o2 =
    Right . reduce . removeRenegade $ PowerState {
        origin = o1,
        infimum,
        events = removeObsolete (unionWith mergeAcks d1 d2)
      }
  where
    infimum = max i1 i2

    {- |
      Obsolete events are events that are already included in the latest
      infimum.
    -}
    removeObsolete = filterWithKey (\k _ -> k > stateId infimum)

    {- |
      Renegade events are events that originate from a non-participating
      peer.  This might happen in a network partition situation, where
      the cluster ejected a peer that later reappears on the network,
      broadcasting updates.

      In reality, this will probably always be a no-op because the
      message dispatcher in the main state machine will immediately
      drop messages that originate from unknown peers (where "unknown"
      includes peers that have been ejected), so it is unlikely that any
      renegade merge requests will make it this far, but you can never
      be too paranoid I guess.
    -}
    removeRenegade ps =
        ps {
            events =
              fromAscList
              . filter nonRenegade
              . toAscList
              . events
              $ ps
          }
      where
        nonRenegade (BottomSid, _) = True
        nonRenegade (Sid _ p, _) = p `member` peers
        peers = allParticipants ps

    mergeAcks (e, s1) (_, s2) = (e, s1 `union` s2)

mergeEither PowerState {origin = o1} PowerState {origin = o2} =
  Left (DifferentOrigins o1 o2)


{- |
  Record the fact that the participant acknowledges the information
  contained in the powerset. The implication is that the participant
  __must__ base all future operations on the result of this function.
-}
acknowledge :: (Event e r s, Ord p)
  => p
  -> PowerState o s p e r
  -> (PowerState o s p e r, Map (StateId p) r)
acknowledge p ps@PowerState {events} =
    reduce ps {events = fmap ackOne events}
  where
    ackOne (e, acks) = (e, Set.insert p acks)


{- |
  Allow a participant to join in the distributed nature of the power state.
-}
participate :: (Ord p)
  => p
  -> PowerState o s p e r
  -> PowerState o s p e r
participate p ps@PowerState {events} = ps {
    events = Map.insert (nextId p ps) (Join p, Set.empty) events
  }


{- |
  Indicate that a participant is removing itself from participating in
  the distributed power state.
-}
disassociate :: (Ord p)
  => p
  -> PowerState o s p e r
  -> PowerState o s p e r
disassociate p ps@PowerState {events} = ps {
    events = Map.insert (nextId p ps) (UnJoin p, Set.empty) events
  }


{- |
  Introduce a change to the PowerState on behalf of the participant.
  Return the new powerstate along with the projected output of the event.
-}
event :: (Ord p, Event e r s)
  => p
  -> e
  -> PowerState o s p e r
  -> (r, StateId p, PowerState o s p e r)
event p e ps@PowerState {events} =
  let
    sid = nextId p ps
  in
    (
      fst (apply e (projectedValue ps)),
      sid,
      ps {
          events = Map.insert sid (Event e, Set.empty) events
        }
    )


{- |
  Return the current projected value of the power state.
-}
projectedValue :: (Event e r s) => PowerState o s p e r -> s
projectedValue PowerState {infimum = Infimum {stateValue}, events} =
    foldr (\ e s -> snd (apply e s)) stateValue changes
  where
    changes = foldr getDeltas [] (toDescList events)
    getDeltas (_, (Event e, _)) acc = e:acc
    getDeltas _ acc = acc


{- |
  Return the current infimum value of the power state.
-}
infimumValue :: PowerState o s p e r -> s
infimumValue PowerState {infimum = Infimum {stateValue}} = stateValue


{- |
  Gets the known participants at the infimum.
-}
infimumParticipants :: PowerState o s p e r -> Set p
infimumParticipants PowerState {infimum = Infimum {participants}} = participants


{- |
  Get all known participants. This includes participants that are
  projected for removal.
-}
allParticipants :: (Ord p) => PowerState o s p e r -> Set p
allParticipants PowerState {
    infimum = Infimum {participants},
    events
  } =
    foldr updateParticipants participants (toDescList events)
  where
    updateParticipants (_, (Join p, _)) = Set.insert p
    updateParticipants _ = id


{- |
  Get all the projected participants. This does not include participants that
  are projected for removal.
-}
projParticipants :: (Ord p) => PowerState o s p e r -> Set p
projParticipants PowerState {
    infimum = Infimum {participants},
    events
  } =
    foldr updateParticipants participants (toDescList events)
  where
    updateParticipants (_, (Join p, _)) = Set.insert p
    updateParticipants (_, (UnJoin p, _)) = Set.delete p
    updateParticipants _ = id


{- |
  Returns the participants that we think might be diverging. In this
  context, a participant is "diverging" if there is an event that the
  participant has not acknowledged.
-}
divergent :: (Ord p) => PowerState o s p e r -> Set p
divergent PowerState {
    infimum = Infimum {participants},
    events
  } =
    accum participants Set.empty (toAscList events)
  where
    {- |
      `accum` mnemonics:
        j = pro(J)ected participants
        d = (D)iverging participants
        a = peers that have (A)cknowledged an update.
        p = (P)eer that is joining or unjoining
    -}
    accum _ d [] = d

    accum j d ((_, (Join p, a)):moreDeltas) =
      let
        j2 = Set.insert p j
        d2 = (j2 \\ a) `union` d
      in
        accum j2 d2 moreDeltas

    accum j d ((_, (UnJoin p, a)):moreDeltas) =
      let
        j2 = Set.delete p j
        {- A participant must acknowledge its own unjoin. -}
        d2 = (j \\ a) `union` d
      in
        accum j2 d2 moreDeltas

    accum j d ((_, (Event _, a)):moreDeltas) =
      let
        d2 = (j \\ a) `union` d
      in
        accum j d2 moreDeltas


{- |
  Return all divergent events, along with the set of peers for which we
  are expecting an acknowledgement of the event.
-}
_divergences :: (Ord p) => PowerState o s p e r -> Map (StateId p) (e, Set p)
_divergences PowerState {events, infimum} =
    go (participants infimum) (Map.toAscList events)
  where
    go :: (Ord p)
      => Set p
      -> [(StateId p, (Delta p e, Set p))]
      -> Map (StateId p) (e, Set p)
    go _ [] = Map.empty
    go ps ((sid, (Event e, p)):moreEvents) =
      Map.insert sid (e, ps \\ p) (go ps moreEvents)
    go ps ((_, (Join p, _)):moreEvents) = go (Set.insert p ps) moreEvents
    go ps ((_, (UnJoin p, _)):moreEvents) = go (Set.delete p ps) moreEvents


{- |
  This helper function is responsible for figuring out if the power state
  has enough information to derive a new infimum value. In other words,
  this is where garbage collection happens.
-}
reduce :: (Event e r s, Ord p)
  => PowerState o s p e r
  -> (PowerState o s p e r, Map (StateId p) r)
reduce ps@PowerState {
    infimum = infimum@Infimum {participants, stateValue},
    events
  } =
    case minViewWithKey events of
      Nothing -> (ps, Map.empty)
      Just ((sid, (update, acks)), newDeltas) ->
        if not . null $ participants \\ acks
          then (ps, Map.empty)
          else case update of
            Join p -> reduce ps {
                infimum = infimum {
                    stateId = sid,
                    participants = Set.insert p participants
                  },
                events = newDeltas
              }
            UnJoin p -> reduce ps {
                infimum = infimum {
                    stateId = sid,
                    participants = Set.delete p participants
                  },
                events = newDeltas
              }
            Event e ->
              let
                (output, newState) = apply e stateValue
                (ps2, outputs) = reduce ps {
                    infimum = infimum {
                        stateId = sid,
                        stateValue = newState
                      },
                    events = newDeltas
                  }
              in (ps2, Map.insert sid output outputs)


{- |
  A utility function that constructs the next `StateId` on behalf of
  a participant.
-}
nextId :: (Ord p) => p -> PowerState o s p e r -> StateId p
nextId p PowerState {infimum = Infimum {stateId}, events} =
  case maximum (stateId:keys events) of
    BottomSid -> Sid 0 p
    Sid ord _ -> Sid (succ ord) p


