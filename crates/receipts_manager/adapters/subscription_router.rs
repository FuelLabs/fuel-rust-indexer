//! Routes preconfirmation and finalized-block events from multiple subscription
//! sources into a single stream using first-arrival semantics.
//!
//! Preconfirmations and finalized blocks both arrive in monotonically
//! non-decreasing height order, so the router only needs one slot per stream:
//!
//! - The current preconf slot binds preconfs for its height to a single source.
//!   A higher height advances the slot (and rebinds to whichever source got
//!   there first); a lower or stale height is dropped.
//! - The last-sealed slot records the first finalized block seen. Subsequent
//!   blocks at the same height from another source are compared by id — a
//!   mismatch produces a warning. Older heights are dropped unconditionally.

use fuel_core_types::{
    blockchain::primitives::BlockId,
    fuel_types::BlockHeight,
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct SourceId(pub usize);

impl std::fmt::Display for SourceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "source#{}", self.0)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreconfDecision {
    Forward,
    DropStale,
    DropAlreadyOwned { owner: SourceId },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BlockDecision {
    Forward,
    DropStale,
    DropDuplicate,
    DropMismatch {
        first_source: SourceId,
        first_id: BlockId,
    },
}

#[derive(Clone, Copy, Debug)]
struct PreconfOwner {
    height: BlockHeight,
    source: SourceId,
}

#[derive(Clone, Copy, Debug)]
struct SealedBlock {
    height: BlockHeight,
    id: BlockId,
    source: SourceId,
}

#[derive(Default)]
pub struct RouterState {
    preconf: Option<PreconfOwner>,
    sealed: Option<SealedBlock>,
}

impl RouterState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn admit_preconf(
        &mut self,
        source: SourceId,
        height: BlockHeight,
    ) -> PreconfDecision {
        if matches!(self.sealed, Some(s) if height <= s.height) {
            return PreconfDecision::DropStale;
        }

        match self.preconf {
            Some(current) if height < current.height => PreconfDecision::DropStale,
            Some(current) if height == current.height && current.source != source => {
                PreconfDecision::DropAlreadyOwned {
                    owner: current.source,
                }
            }
            Some(current) if height == current.height => PreconfDecision::Forward,
            _ => {
                self.preconf = Some(PreconfOwner { height, source });
                PreconfDecision::Forward
            }
        }
    }

    pub fn admit_block(
        &mut self,
        source: SourceId,
        height: BlockHeight,
        id: BlockId,
    ) -> BlockDecision {
        match self.sealed {
            Some(current) if height < current.height => BlockDecision::DropStale,
            Some(current) if height == current.height => {
                if current.id == id {
                    BlockDecision::DropDuplicate
                } else {
                    BlockDecision::DropMismatch {
                        first_source: current.source,
                        first_id: current.id,
                    }
                }
            }
            _ => {
                self.sealed = Some(SealedBlock { height, id, source });
                if matches!(self.preconf, Some(p) if p.height <= height) {
                    self.preconf = None;
                }
                BlockDecision::Forward
            }
        }
    }

    #[cfg(test)]
    pub fn preconf_owner(&self, height: BlockHeight) -> Option<SourceId> {
        self.preconf
            .filter(|p| p.height == height)
            .map(|p| p.source)
    }

    #[cfg(test)]
    pub fn latest_sealed_height(&self) -> Option<BlockHeight> {
        self.sealed.map(|s| s.height)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(height: u32) -> BlockHeight {
        BlockHeight::new(height)
    }

    fn id(byte: u8) -> BlockId {
        let mut bytes = [0u8; 32];
        bytes[0] = byte;
        BlockId::from(fuel_core_types::fuel_tx::Bytes32::new(bytes))
    }

    #[test]
    fn preconf_first_wins_per_height() {
        let mut state = RouterState::new();
        let s0 = SourceId(0);
        let s1 = SourceId(1);

        assert_eq!(state.admit_preconf(s0, h(10)), PreconfDecision::Forward);
        // Same source, same height: still forwards (subsequent tx indices).
        assert_eq!(state.admit_preconf(s0, h(10)), PreconfDecision::Forward);
        // Other source at same height: dropped.
        assert_eq!(
            state.admit_preconf(s1, h(10)),
            PreconfDecision::DropAlreadyOwned { owner: s0 }
        );
    }

    #[test]
    fn preconf_advancing_height_rebinds_ownership() {
        let mut state = RouterState::new();
        let s0 = SourceId(0);
        let s1 = SourceId(1);

        assert_eq!(state.admit_preconf(s0, h(10)), PreconfDecision::Forward);
        // A different source is allowed to take over H+1 once it arrives first.
        assert_eq!(state.admit_preconf(s1, h(11)), PreconfDecision::Forward);
        // Owner for H+1 is now s1.
        assert_eq!(state.preconf_owner(h(11)), Some(s1));
        // Late preconf for H=10 is dropped as stale.
        assert_eq!(state.admit_preconf(s0, h(10)), PreconfDecision::DropStale);
    }

    #[test]
    fn block_first_wins_and_evicts_preconfs_at_or_below() {
        let mut state = RouterState::new();
        let s0 = SourceId(0);
        let s1 = SourceId(1);

        state.admit_preconf(s0, h(10));

        assert_eq!(
            state.admit_block(s0, h(10), id(0xAA)),
            BlockDecision::Forward
        );

        // Preconfs <= 10 now dropped as stale.
        assert_eq!(state.admit_preconf(s1, h(10)), PreconfDecision::DropStale);
        assert_eq!(state.admit_preconf(s1, h(9)), PreconfDecision::DropStale);
        // Future heights still allowed.
        assert_eq!(state.admit_preconf(s1, h(11)), PreconfDecision::Forward);
    }

    #[test]
    fn block_mismatch_from_other_source_is_reported() {
        let mut state = RouterState::new();
        let s0 = SourceId(0);
        let s1 = SourceId(1);

        assert_eq!(
            state.admit_block(s0, h(10), id(0xAA)),
            BlockDecision::Forward
        );

        match state.admit_block(s1, h(10), id(0xBB)) {
            BlockDecision::DropMismatch {
                first_source,
                first_id,
            } => {
                assert_eq!(first_source, s0);
                assert_eq!(first_id, id(0xAA));
            }
            other => panic!("expected DropMismatch, got {other:?}"),
        }
    }

    #[test]
    fn block_duplicate_same_id_is_silent_drop() {
        let mut state = RouterState::new();
        let s0 = SourceId(0);
        let s1 = SourceId(1);

        assert_eq!(
            state.admit_block(s0, h(10), id(0xAA)),
            BlockDecision::Forward
        );
        assert_eq!(
            state.admit_block(s1, h(10), id(0xAA)),
            BlockDecision::DropDuplicate
        );
    }

    #[test]
    fn block_below_latest_sealed_is_dropped() {
        let mut state = RouterState::new();
        let s0 = SourceId(0);
        let s1 = SourceId(1);

        state.admit_block(s0, h(11), id(0xAA));
        assert_eq!(
            state.admit_block(s1, h(10), id(0xBB)),
            BlockDecision::DropStale
        );
    }

    #[test]
    fn preconf_below_current_slot_but_above_sealed_is_stale() {
        let mut state = RouterState::new();
        let s0 = SourceId(0);
        let s1 = SourceId(1);

        state.admit_block(s0, h(5), id(5));
        assert_eq!(state.admit_preconf(s0, h(11)), PreconfDecision::Forward);
        // Height 10 is above sealed (5) but below current preconf (11): stale.
        assert_eq!(state.admit_preconf(s1, h(10)), PreconfDecision::DropStale);
    }

    #[test]
    fn block_preserves_preconf_slot_when_preconf_is_ahead() {
        let mut state = RouterState::new();
        let s0 = SourceId(0);
        let s1 = SourceId(1);

        assert_eq!(state.admit_preconf(s0, h(12)), PreconfDecision::Forward);
        assert_eq!(
            state.admit_block(s1, h(10), id(0xAA)),
            BlockDecision::Forward
        );
        // Preconf slot at 12 must not be cleared by a block at 10.
        assert_eq!(state.preconf_owner(h(12)), Some(s0));
        // And other sources still can't take over 12.
        assert_eq!(
            state.admit_preconf(s1, h(12)),
            PreconfDecision::DropAlreadyOwned { owner: s0 }
        );
    }

    #[test]
    fn same_source_different_ids_at_same_height_is_mismatch() {
        let mut state = RouterState::new();
        let s0 = SourceId(0);

        assert_eq!(
            state.admit_block(s0, h(10), id(0xAA)),
            BlockDecision::Forward
        );
        match state.admit_block(s0, h(10), id(0xBB)) {
            BlockDecision::DropMismatch {
                first_source,
                first_id,
            } => {
                assert_eq!(first_source, s0);
                assert_eq!(first_id, id(0xAA));
            }
            other => panic!("expected DropMismatch, got {other:?}"),
        }
    }

    #[test]
    fn preconf_at_sealed_plus_one_forwards() {
        let mut state = RouterState::new();
        let s0 = SourceId(0);
        let s1 = SourceId(1);

        assert_eq!(
            state.admit_block(s0, h(10), id(0xAA)),
            BlockDecision::Forward
        );
        // Exactly sealed + 1 must be admitted.
        assert_eq!(state.admit_preconf(s1, h(11)), PreconfDecision::Forward);
        assert_eq!(state.preconf_owner(h(11)), Some(s1));
    }

    #[test]
    fn block_advances_sealed_slot() {
        let mut state = RouterState::new();
        let s0 = SourceId(0);

        state.admit_block(s0, h(5), id(5));
        assert_eq!(state.latest_sealed_height(), Some(h(5)));
        state.admit_block(s0, h(10), id(10));
        assert_eq!(state.latest_sealed_height(), Some(h(10)));
    }
}
