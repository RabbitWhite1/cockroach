package kvserver

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/rac2"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/crlib/crtime"
)

func saveApp(syn *Replica, app raft.StorageAppend, raftEvent rac2.RaftEvent, stats *handleRaftReadyStats) logstore.RaftState {
	r := syn
	ctx := r.raftCtx
	var err error

	// TODO(pavelkalinnikov): find a way to move it to storeEntries.
	if app.Commit != 0 && !r.IsInitialized() {
		log.Dev.Fatalf(ctx, "setting non-zero HardState.Commit on uninitialized replica %s", r)
	}
	// TODO(pav-kv): make this branch unconditional.
	if r.IsInitialized() && r.store.cfg.KVAdmissionController != nil {
		// Enqueue raft log entries into admission queues. This is
		// non-blocking; actual admission happens asynchronously.
		r.flowControlV2.AdmitRaftEntriesRaftMuLocked(ctx, raftEvent)
	}

	r.mu.raftTracer.MaybeTraceAppend(app)
	var state logstore.RaftState
	if state, err = r.asLogStorage().appendRaftMuLocked(ctx, app, &stats.append); err != nil {
		log.Dev.Fatalf(ctx, "while storing log entries: %s", err)
	}

	return state
}

func saveSnap(syn *Replica, app raft.StorageAppend, inSnap IncomingSnapshot, stats *handleRaftReadyStats) logstore.RaftState {
	r := syn
	ctx := r.raftCtx

	// (*syn).SaveSnap(snap)
	if inSnap.Desc == nil {
		// If we didn't expect Raft to have a snapshot but it has one
		// regardless, that is unexpected and indicates a programming
		// error.
		log.Dev.Fatalf(ctx, "have inSnap=nil, but raft has a snapshot %s", raft.DescribeSnapshot(*app.Snapshot))
		// return stats, errors.AssertionFailedf(
		// 	"have inSnap=nil, but raft has a snapshot %s",
		// 	raft.DescribeSnapshot(*app.Snapshot),
		// )
	}

	snapUUID, err := uuid.FromBytes(app.Snapshot.Data)
	if err != nil {
		log.Dev.Fatalf(ctx, "invalid snapshot id: %s", err)
		// return stats, errors.Wrap(err, "invalid snapshot id")
	}
	if inSnap.SnapUUID == (uuid.UUID{}) {
		log.Dev.Fatalf(ctx, "programming error: a snapshot application was attempted outside of the streaming snapshot codepath")
	}
	if snapUUID != inSnap.SnapUUID {
		log.Dev.Fatalf(ctx, "incoming snapshot id doesn't match raft snapshot id: %s != %s", snapUUID, inSnap.SnapUUID)
	}

	snap := *app.Snapshot
	if len(app.Entries) != 0 {
		log.Dev.Fatalf(ctx, "found Entries in MsgStorageAppend with non-empty Snapshot")
	}

	// Applying this snapshot may require us to subsume one or more of our right
	// neighbors. This occurs if this replica is informed about the merges via a
	// Raft snapshot instead of a MsgApp containing the merge commits, e.g.,
	// because it went offline before the merge commits applied and did not come
	// back online until after the merge commits were truncated away.
	subsumedRepls, releaseMergeLock := r.maybeAcquireSnapshotMergeLock(ctx, inSnap)
	defer releaseMergeLock()

	stats.tSnapBegin = crtime.NowMono()
	if err := r.applySnapshotRaftMuLocked(ctx, inSnap, snap, app.HardState, subsumedRepls); err != nil {
		log.Dev.Fatalf(ctx, "while applying snapshot: %s", err)
		// return stats, errors.Wrap(err, "while applying snapshot")
	}
	for _, msg := range app.Responses {
		// The caller would like to see the MsgAppResp that usually results from
		// applying the snapshot synchronously, so fish it out.
		if msg.To == raftpb.PeerID(inSnap.FromReplica.ReplicaID) &&
			msg.Type == raftpb.MsgAppResp &&
			!msg.Reject &&
			msg.Index == snap.Metadata.Index {

			inSnap.msgAppRespCh <- msg
			break
		}
	}
	stats.tSnapEnd = crtime.NowMono()
	stats.snap.applied = true

	// The raft log state was updated in applySnapshotRaftMuLocked, but we also want to
	// reflect these changes in the state variable here.
	// TODO(pav-kv): this is unnecessary. We only do it because there is an
	// unconditional storing of this state below. Avoid doing it twice.
	state := r.asLogStorage().stateRaftMuLocked()

	// We refresh pending commands after applying a snapshot because this
	// replica may have been temporarily partitioned from the Raft group and
	// missed leadership changes that occurred. Suppose node A is the leader,
	// and then node C gets partitioned away from the others. Leadership passes
	// back and forth between A and B during the partition, but when the
	// partition is healed node A is leader again.
	// if !r.store.TestingKnobs().DisableRefreshReasonSnapshotApplied &&
	// 	refreshReason == noReason {
	// 	refreshReason = reasonSnapshotApplied
	// }

	cb := (*replicaSyncCallback)(r)
	cb.OnSnapSync(ctx, app.Ack())

	return state
}

func (s *RaftSync) save(states Sync_Protocol_States) (raft.Notify, []bool) {
	var state logstore.RaftState
	saved_states := s.saved_states.Load()

	if states.StorageAppend.Snapshot != nil {
		state = saveSnap(s.storage, states.StorageAppend, states.InSnap, states.Stats)
	} else {
		state = saveApp(s.storage, states.StorageAppend, states.RaftEvent, states.Stats)
	}

	new_states, send_notify := step(*saved_states, states)
	s.saved_states.Store(new_states)

	new_term, new_index := (*new_states).GetLastEntryTermIndex()
	// `new_index` and `new_term` may be 0 if first time here. (There could be some initialization, but I need to figure out in the future.)
	if (new_index != 0 && uint64(state.LastIndex) != new_index) || (new_term != 0 && uint64(state.LastTerm) != new_term) {
		fmt.Printf("raft log state mismatch: %+v, last entry term/index: %d/%d\n", state, new_term, new_index)
		panic("!!raft")
	}
	return raft.Notify{Term: states.StorageAppend.Term, LastIndex: uint64(state.LastIndex), LastTerm: uint64(state.LastTerm), Snapshot: states.StorageAppend.Snapshot, ByteSize: state.ByteSize}, send_notify
}

func step(old Sync_Protocol_States, new Sync_Protocol_States) (*Sync_Protocol_States, []bool) {
	if new.StorageAppend.Term != 0 {
		old.StorageAppend.Term = new.StorageAppend.Term
	}
	if new.StorageAppend.Commit != 0 {
		old.StorageAppend.Commit = new.StorageAppend.Commit
	}
	if new.StorageAppend.Vote != 0 {
		old.StorageAppend.Vote = new.StorageAppend.Vote
	}
	if new.StorageAppend.Snapshot != nil && !IsEmptySnap(*new.StorageAppend.Snapshot) {
		old.StorageAppend.Snapshot = new.StorageAppend.Snapshot
	}
	if len(new.StorageAppend.Entries) > 0 {
		// FIXME: when do we GC?
		old.StorageAppend.Entries = append(old.StorageAppend.Entries, new.StorageAppend.Entries...)
	}
	// For cockroach, we must notify anyways, because we always block until the saving done for some following updates.
	return &old, []bool{true}
}

func can_send(saved_states Sync_Protocol_States, m interface{}) bool {
	if msg, ok := m.(raftpb.Message); ok {
		// fmt.Printf("sync thread: can send check message is: %+v, saved_states is: %+v\n", msg, saved_states)
		if msg.Type == raftpb.MsgPreVote || msg.Type == raftpb.MsgPreVoteResp || msg.Type == raftpb.MsgHeartbeat || msg.Type == raftpb.MsgHeartbeatResp || msg.Type == raftpb.MsgVote {
			return true
		}
		if msg.Type == raftpb.MsgApp {
			return true
			// _, last_index := saved_states.GetLastEntryTermIndex()
			// if msg.Term == saved_states.Term && last_index >= msg.Commit {
			// 	return true
			// }
		}
		if msg.Type == raftpb.MsgAppResp {
			// fmt.Printf("sync thread: can send check message is: %+v, saved_states is: %+v\n", msg, saved_states)
			if msg.Reject {
				return true
			}
			_, last_index := saved_states.GetLastEntryTermIndex()
			if msg.Term == saved_states.StorageAppend.Term && msg.Index <= last_index {
				return true
			}
		}
		if msg.Type == raftpb.MsgPreVote || msg.Type == raftpb.MsgPreVoteResp {
			return true
		}
		if msg.Type == raftpb.MsgVoteResp {
			if msg.Reject {
				return true
			}
			if msg.Term == saved_states.StorageAppend.Term && msg.To == saved_states.StorageAppend.Vote {
				return true
			}
		}
		if msg.Type == raftpb.MsgProp {
			return true
		}
	}
	return false
}

func isProtocolStatesEqual(a, b Sync_Protocol_States) bool {
	last_term_a, last_index_a := a.GetLastEntryTermIndex()
	last_term_b, last_index_b := b.GetLastEntryTermIndex()
	return a.StorageAppend.Term == b.StorageAppend.Term && a.StorageAppend.Vote == b.StorageAppend.Vote && a.StorageAppend.Commit == b.StorageAppend.Commit && last_term_a == last_term_b && last_index_a == last_index_b
}

func isSnapshotEqual(a, b raftpb.Snapshot) bool {
	return (a.Metadata.Term == b.Metadata.Term && a.Metadata.Index == b.Metadata.Index) || IsEmptySnap(b)
}
