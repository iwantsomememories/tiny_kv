// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

var ErrExceedLastIndex = errors.New("requested index exceeds the lastIndex of raftlog")

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil {
		panic("storage must not be nil.")
	}

	log := &RaftLog{storage: storage}

	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	log.stabled = lastIndex

	log.committed = firstIndex - 1
	log.applied = firstIndex - 1

	return log
}

// maybeAppend returns (0, false) if the entries cannot be appended. Otherwise,
// it returns (last index of new entries, true).
func (l *RaftLog) maybeAppend(index, logTerm, committed uint64, ents ...pb.Entry) (uint64, bool) {
	if l.matchTerm(index, logTerm) {
		lastnewi := index + uint64(len(ents))

		ci := l.findConflict(ents)
		switch {
		case ci == 0:
		case ci <= l.committed:
			panic(fmt.Sprintf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed))
		default:
			offset := index + 1
			l.append(ents[ci-offset:]...)
		}
		l.commitTo(min(lastnewi, committed))
		return lastnewi, true
	} else {
		return 0, false
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

func (l *RaftLog) maybeCommit(maxIndex, term uint64) bool {
	if maxIndex > l.committed && l.zeroTermOnErrCompacted(l.Term(maxIndex)) == term {
		l.commitTo(maxIndex)
		return true
	}
	return false
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	ents, err := l.slice(l.FirstIndex(), l.LastIndex()+1)
	if err == nil {
		if ents[0].Term == 0 && ents[0].Data == nil { // remove dummy entries
			ents = ents[1:]
		}
		return ents
	}

	if err == ErrCompacted { // try again if there was a racing compaction
		return l.allEntries()
	}
	panic(err)
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}
	return l.entries
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if l.committed == l.applied {
		return nil
	}

	entries, err := l.slice(l.applied+1, l.committed+1)
	if err != nil {
		panic(err)
	}
	return entries
}

func (l *RaftLog) FirstIndex() uint64 {
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index + 1
	}

	index, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	return index
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) != 0 {
		return l.entries[len(l.entries)-1].Index
	}

	lastIndex, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}

	return lastIndex
}

func (l *RaftLog) LastTerm() uint64 {
	t, err := l.Term(l.LastIndex())
	if err != nil {
		panic(fmt.Sprintf("unexpected error when getting the last term (%v)", err))
	}

	return t
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	dummyIndex := l.FirstIndex() - 1
	if i < dummyIndex || i > l.LastIndex() {
		return 0, nil
	}

	if i > l.stabled {
		return l.entries[i-l.stabled-1].Term, nil
	}

	t, err := l.storage.Term(i)
	if err == nil {
		return t, nil
	}

	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}
	panic(err)
}

func (l *RaftLog) append(ents ...pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.LastIndex()
	}

	if offset := ents[0].Index; offset <= l.committed {
		panic(fmt.Sprintf("offset(%d) is less than or equal to [committed(%d)]", offset, l.committed))
	}

	l.truncateAndAppend(ents)
	return l.LastIndex()
}

func (l *RaftLog) truncateAndAppend(ents []pb.Entry) {
	after := ents[0].Index
	nextIndex := l.stabled + uint64(len(l.entries)+1)

	if after == nextIndex {
		l.entries = append(l.entries, ents...)
	} else if after <= l.stabled+1 {
		DPrintf("replace the unstable entries from index %d", after)
		l.entries = ents
		l.stabled = after - 1
	} else if after <= nextIndex {
		DPrintf("truncate the unstable entries before index %d", after)
		l.entries = append([]pb.Entry{}, l.entries[:after-l.stabled-1]...)
		l.entries = append(l.entries, ents...)
	} else {
		DPrintf("warning -- append Entries(after=%x) when nextIndex=%x", after, nextIndex)
	}
}

func (l *RaftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}

	if i > l.committed || i < l.applied {
		panic(fmt.Sprintf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed))
	}
	l.applied = i
}

func (l *RaftLog) commitTo(tocommit uint64) {
	if tocommit > l.committed {
		if l.LastIndex() < tocommit {
			panic(fmt.Sprintf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, l.LastIndex()))
		}
		l.committed = tocommit
		DPrintf("commit to %d", tocommit)
	}
}

func (l *RaftLog) entriesAfter(start uint64) ([]pb.Entry, error) {
	if start > l.LastIndex() {
		return []pb.Entry{}, nil
	}

	return l.slice(start, l.LastIndex()+1)
}

// slice returns a slice of log entries from lo through hi - 1, inclusive.
func (l *RaftLog) slice(lo, hi uint64) ([]pb.Entry, error) {
	if lo > hi {
		panic(fmt.Sprintf("invalid slice %d > %d", lo, hi))
	}

	fi := l.FirstIndex()
	li := l.LastIndex()
	if lo < fi {
		return nil, ErrCompacted
	}

	if hi > li+1 {
		return nil, ErrExceedLastIndex
	}

	var ents []pb.Entry
	if lo <= l.stabled {
		storedEnts, err := l.storage.Entries(lo, min(hi, l.stabled+1))
		if err == ErrCompacted {
			return nil, err
		} else if err == ErrUnavailable {
			panic(fmt.Sprintf("entries[%d:%d) is unavailable from storage", lo, min(hi, l.stabled+1)))
		} else if err != nil {
			panic(err)
		}

		ents = storedEnts
	}

	if hi > l.stabled+1 {
		unstableEnts := l.entries[max(lo-l.stabled-1, 0) : hi-l.stabled-1]
		if len(ents) > 0 {
			ents = append(ents, unstableEnts...)
		} else {
			ents = unstableEnts
		}
	}

	return ents, nil
}

func (l *RaftLog) isUpToDate(index, term uint64) bool {
	return term > l.LastTerm() || (term == l.LastTerm() && index >= l.LastIndex())
}

func (l *RaftLog) zeroTermOnErrCompacted(term uint64, err error) uint64 {
	if err == nil {
		return term
	}

	if err == ErrCompacted {
		return 0
	}
	panic(err)
}

func (l *RaftLog) matchTerm(index, term uint64) bool {
	t, err := l.Term(index)
	if err != nil {
		return false
	}

	return t == term
}

func (l *RaftLog) findConflict(ents []pb.Entry) uint64 {
	for _, en := range ents {
		if !l.matchTerm(en.Index, en.Term) {
			if en.Index < l.LastIndex() {
				DPrintf("found conflict at index %d [existing term: %d, conflicting term: %d]",
					en.Index, l.zeroTermOnErrCompacted(l.Term(en.Index)), en.Term)
			}

			return en.Index
		}
	}

	return 0
}
