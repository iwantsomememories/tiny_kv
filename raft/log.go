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
	"fmt"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

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
	// the index of the first entry, i.e., the index of the dummy entry.
	offset uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil {
		panic("storage must not be nil.")
	}

	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	// start with dummy entry
	log := &RaftLog{storage: storage, entries: make([]pb.Entry, 1)}
	log.entries[0].Index = firstIndex - 1
	dummyTerm, err := storage.Term(firstIndex - 1)
	if err != nil {
		panic(err)
	}
	log.entries[0].Term = dummyTerm
	log.offset = log.entries[0].Index

	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	stableEntries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}

	for _, ent := range stableEntries {
		log.entries = append(log.entries, ent)
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
	if maxIndex > l.committed && l.zeroTermOnErr(l.Term(maxIndex)) == term {
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
	if len(l.entries) == 1 { // only contain dummy entry
		return make([]pb.Entry, 0)
	}

	return l.entries[1:]
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if l.stabled == l.LastIndex() {
		return make([]pb.Entry, 0)
	}

	return l.entries[l.stabled-l.offset+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	off := max(l.applied+1, l.FirstIndex())

	if l.committed+1 > off {
		entries, err := l.slice(off, l.committed+1)
		if err != nil {
			panic(fmt.Sprintf("unexpected error when getting unapplied entries (%v)", err))
		}

		return entries
	}

	return nil
}

func (l *RaftLog) snapshot() (pb.Snapshot, error) {
	if l.pendingSnapshot != nil {
		return *l.pendingSnapshot, nil
	}

	return l.storage.Snapshot()
}

func (l *RaftLog) restore(snapshot *pb.Snapshot) {
	l.committed = snapshot.Metadata.Index

	l.entries = make([]pb.Entry, 0)
	l.entries = append(l.entries, pb.Entry{Term: snapshot.Metadata.Term, Index: snapshot.Metadata.Index})
	l.offset = snapshot.Metadata.Index
	l.stabled = snapshot.Metadata.Index
	l.pendingSnapshot = snapshot
}

func (l *RaftLog) FirstIndex() uint64 {
	return l.offset + 1
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return l.offset + uint64(len(l.entries)) - 1
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
	if i < dummyIndex {
		return 0, ErrCompacted
	}

	if i > l.LastIndex() {
		return 0, ErrUnavailable
	}

	return l.entries[i-l.offset].Term, nil
}

func (l *RaftLog) append(ents ...pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.LastIndex()
	}

	if after := ents[0].Index; after <= l.committed {
		panic(fmt.Sprintf("after(%d) is less than or equal to [committed(%d)]", after, l.committed))
	}

	l.truncateAndAppend(ents)
	return l.LastIndex()
}

func (l *RaftLog) truncateAndAppend(ents []pb.Entry) {
	after := ents[0].Index
	firstIndex := l.FirstIndex()
	if after < firstIndex {
		panic(fmt.Sprintf("truncateAndAppend(after=%x) when firstIndex=%x", after, firstIndex))
	}

	nextIndex := l.LastIndex() + 1
	if after == nextIndex {
		l.entries = append(l.entries, ents...)
	} else if after < nextIndex {
		l.entries = append([]pb.Entry{}, l.entries[:after-l.offset]...)
		l.entries = append(l.entries, ents...)

		if after <= l.stabled {
			l.stabled = after - 1
		}
	} else {
		panic(fmt.Sprintf("truncateAndAppend(after=%x) when nextIndex=%x", after, nextIndex))
	}
}

func (l *RaftLog) appliedTo(toapply uint64) {
	if toapply > l.committed || toapply < l.applied {
		panic(fmt.Sprintf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", toapply, l.applied, l.committed))
	}
	l.applied = toapply
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

func (l *RaftLog) stableTo(tostable, term uint64) {
	gt, err := l.Term(tostable)
	if err != nil {
		return
	}

	if gt == term && tostable >= l.stabled {
		l.stabled = tostable
	}
}

func (l *RaftLog) stableSnapTo(index uint64) {
	if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index == index {
		l.pendingSnapshot = nil
	}
}

func (l *RaftLog) entriesAfter(start uint64) ([]pb.Entry, error) {
	if start > l.LastIndex() {
		return []pb.Entry{}, ErrUnavailable
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
		return nil, ErrUnavailable
	}

	if lo == hi {
		return make([]pb.Entry, 0), nil
	}

	return l.entries[lo-l.offset : hi-l.offset], nil
}

func (l *RaftLog) isUpToDate(index, term uint64) bool {
	return term > l.LastTerm() || (term == l.LastTerm() && index >= l.LastIndex())
}

func (l *RaftLog) zeroTermOnErr(term uint64, err error) uint64 {
	if err == nil {
		return term
	}

	return 0
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
					en.Index, l.zeroTermOnErr(l.Term(en.Index)), en.Term)
			}

			return en.Index
		}
	}

	return 0
}

func (l *RaftLog) uncommittedEnts() ([]pb.Entry, error) {
	if l.committed < l.LastIndex() {
		ents, err := l.entriesAfter(l.committed + 1)
		if err != nil {
			return make([]pb.Entry, 0), err
		}

		return ents, nil
	} else {
		return make([]pb.Entry, 0), nil
	}
}
