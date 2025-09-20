package kvserver

import (
	fmt "fmt"
	math "math"
	"os"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/rac2"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
)

const RAFTSYNC_UNKNOWN = math.MaxUint64

type Sync_Protocol_States struct {
	// Term     uint64
	// Commit   uint64
	// Vote     uint64
	// Entries  []raftpb.Entry
	// Snapshot raftpb.Snapshot

	StorageAppend raft.StorageAppend

	// InSnap is required because CockroachDB stores snapshot real data in it, not in raftpb.Snapshot.
	InSnap IncomingSnapshot

	RaftEvent rac2.RaftEvent

	Stats *handleRaftReadyStats
}

type RaftSync struct {
	saved_states atomic.Pointer[Sync_Protocol_States]
	storage      *Replica
	// states       *raft.Raft
	// msgs         <-chan interface{}
	StatesCh     <-chan Sync_Protocol_States
	writeEnd     []chan raft.Notify
	allcounter   atomic.Uint64
	savedcounter atomic.Uint64
}

// IsEmptySnap returns true if the given Snapshot is empty.
func IsEmptySnap(sp raftpb.Snapshot) bool {
	return sp.Metadata.Index == 0
}

func (s *Sync_Protocol_States) GetLastEntryTermIndex() (uint64, uint64) {
	Entries := s.StorageAppend.Entries
	if len(Entries) == 0 {
		return 0, 0
	}
	return Entries[len(Entries)-1].Term, Entries[len(Entries)-1].Index
}

func (s *RaftSync) Init(st *Replica, statesCh <-chan Sync_Protocol_States, writeEnd []chan raft.Notify) {
	s.storage = st
	s.StatesCh = statesCh
	s.writeEnd = writeEnd
	s.allcounter.Store(math.MaxUint64)
	s.savedcounter.Store(0)
	saved_states := Sync_Protocol_States{}
	s.saved_states.Store(&saved_states)
}

func (s *RaftSync) AddCounter() {
	s.allcounter.Add(1)
}

func (s *RaftSync) TrySend(msg interface{}, counter uint64) (bool, uint64) {
	if counter == RAFTSYNC_UNKNOWN {
		counter = s.allcounter.Load()
	}
	states := s.saved_states.Load()
	if can_send(*states, msg) {
		return true, 0
	}
	saved_counter := s.savedcounter.Load()
	if saved_counter >= counter {
		return true, 0
	}
	return false, counter
}

func (s *RaftSync) Send(msg interface{}, counter uint64) {
	if counter == RAFTSYNC_UNKNOWN {
		counter = s.allcounter.Load()
	}
	for {
		states := s.saved_states.Load()
		if can_send(*states, msg) {
			return
		}
		saved_counter := s.savedcounter.Load()
		if saved_counter >= counter {
			return
		}
	}
}

func (s *RaftSync) Run() {
	for {
		select {
		case states := <-s.StatesCh:
			if s.allcounter.Load() == math.MaxUint64 {
				s.allcounter.Store(1)
			} else {
				s.allcounter.Add(1)
			}
			notify, send := s.save(states)
			s.savedcounter.Add(1)
			if send[0] {
				select {
				case <-s.writeEnd[0]:
				case s.writeEnd[0] <- notify:
				case <-time.After(3 * time.Second):
					fmt.Printf("snap write end is blocked\n")
					os.Exit(-1)
				}
			}
		}
	}
}
