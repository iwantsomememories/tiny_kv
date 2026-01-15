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
	"math/rand"
	"sort"
	"strings"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

func (p *Progress) maybeUpdate(n uint64) bool {
	var updated bool
	if n > p.Match {
		p.Match = n
		updated = true
	}

	if n+1 > p.Next {
		p.Next = n + 1
	}

	return updated
}

func (p *Progress) maybeDecrto(rejected, last uint64) bool {
	if rejected != p.Next-1 {
		return false
	}

	if p.Next = min(rejected, last+1); p.Next < 1 {
		p.Next = 1
	}
	return true
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int

	randomizedElectionTimeout int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	step stepFunc
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).

	raftLog := newLog(c.Storage)
	hstate, cstate, err := c.Storage.InitialState()
	if err != nil {
		panic(err.Error())
	}

	peers := c.peers
	if len(cstate.Nodes) > 0 {
		if len(peers) > 0 {
			panic("cannot specify both Config.peers when restarting raft.")
		}

		peers = cstate.Nodes
	}

	r := &Raft{
		id:               c.ID,
		RaftLog:          raftLog,
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		msgs:             make([]pb.Message, 0),
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}

	for _, p := range peers {
		r.Prs[p] = &Progress{Next: 1, Match: 0}
	}

	if !IsEmptyHardState(hstate) {
		r.loadHardState(hstate)
	}

	if c.Applied > 0 {
		r.RaftLog.appliedTo(c.Applied)
	}

	r.becomeFollower(r.Term, None)

	var peersStrs []string
	for p := range r.Prs {
		peersStrs = append(peersStrs, fmt.Sprintf("%x", p))
	}

	DPrintf("Raft %x start with [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
		r.id, strings.Join(peersStrs, ","), r.Term, r.RaftLog.committed, r.RaftLog.applied, r.RaftLog.LastIndex(), r.RaftLog.LastTerm())

	return r
}

func (r *Raft) quorum() int { return len(r.Prs)/2 + 1 }

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	pr := r.Prs[to]
	m := pb.Message{}
	m.To = to

	term, errt := r.RaftLog.Term(pr.Next - 1)
	entries, erre := r.RaftLog.entriesAfter(pr.Next)

	if errt != nil || erre != nil {
		// todo
	} else {
		m.MsgType = pb.MessageType_MsgAppend
		m.Index = pr.Next - 1
		m.LogTerm = term
		m.Commit = r.RaftLog.committed
		if len(entries) > 0 {
			var ents []*pb.Entry
			for _, entry := range entries {
				ents = append(ents, &entry)
			}
			m.Entries = ents
		}

	}
	r.send(m)
	return true
}

func (r *Raft) bcastAppend() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	commit := min(r.RaftLog.committed, r.Prs[to].Match)
	r.send(pb.Message{To: r.id, MsgType: pb.MessageType_MsgHeartbeat, Commit: commit})
}

func (r *Raft) bcastHeartbeat() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendHeartbeat(id)
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).

	switch r.State {
	case StateLeader:
		r.tickHeartbeat()
	default:
		r.tickElection()
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(term)
	r.State = StateFollower
	r.step = stepFollower
	r.Lead = lead

	DPrintf("Raft %x became follower at term %d.\n", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.reset(r.Term + 1)
	r.State = StateCandidate
	r.step = stepCandidate
	r.Vote = r.id
	r.votes[r.id] = true

	DPrintf("Raft %x became candidate at term %d.\n", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.reset(r.Term)
	r.State = StateLeader
	r.step = stepLeader
	r.Lead = r.id

	ents, err := r.RaftLog.entriesAfter(r.RaftLog.committed + 1)
	if err != nil {
		panic(fmt.Sprintf("unexpected error getting uncommitted entries (%v)", err))
	}

	pcents := pendingConfChangeEntries(ents)
	if len(pcents) > 1 {
		panic("unexpected multiple uncommitted config entry")
	}
	if len(pcents) == 1 {
		r.PendingConfIndex = pcents[0].Index
	}

	r.appendEntry(pb.Entry{Data: nil, EntryType: pb.EntryType_EntryNormal})
	DPrintf("Raft %x became leader at term %d.\n", r.id, r.Term)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	DPrintf("Raft %x receive Msg{From: %x, Type: %s} at Term %d as %s.\n", r.id, m.From, m.MsgType.String(), r.Term, r.State)

	switch {
	case m.Term == 0:
		// local message
	case m.Term > r.Term:
		lead := m.From
		if m.MsgType == pb.MessageType_MsgRequestVote {
			// todo
			lead = None
		}
		r.becomeFollower(r.Term, lead)
	case m.Term < r.Term:
		return nil
	}

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if r.State != StateLeader {
			ents := r.RaftLog.nextEnts()
			if pcents := pendingConfChangeEntries(ents); len(pcents) != 0 {
				DPrintf("Raft %x cannot campaign at term %d since there are still %d pending configuration changes to apply.\n", r.id, r.Term, len(pcents))
				return nil
			}
			DPrintf("Raft %x is starting a new election at term %d.\n", r.id, r.Term)
			r.campaign()
		} else {
			DPrintf("Raft %x ignoring MsgHup because already leader.\n", r.id)
		}
	case pb.MessageType_MsgRequestVote:
		if (r.Vote == None || r.Vote == m.From) && r.RaftLog.isUpToDate(m.Index, m.Term) {
			DPrintf("Raft %x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
				r.id, r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term)

			r.send(pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From})
			r.electionElapsed = 0
			r.Vote = m.From
		} else {
			DPrintf("Raft %x [logterm: %d, index: %d, vote: %x] rejected %s for %x [logterm: %d, index: %d] at term %d", r.id, r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term)

			r.send(pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From, Reject: true})
		}
	default:
		r.step(r, m)
	}

	return nil
}

type stepFunc func(r *Raft, m pb.Message)

func stepLeader(r *Raft, m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
		return
	case pb.MessageType_MsgPropose:
		// todo

		return
	}

	pr, exist := r.Prs[m.From]
	if !exist {
		DPrintf("Raft %x no progress available for %x.\n", r.id, m.From)
		return
	}
	switch m.MsgType {
	case pb.MessageType_MsgAppendResponse:
		if m.Reject {
			DPrintf("Raft %x received msgApp rejection(lastindex: %d) from %x for index %d",
				r.id, m.RejectHint, m.From, m.Index)
			if pr.maybeDecrto(m.Index, m.RejectHint) {
				DPrintf("Raft %x decreased progress of %x to [%+v].\n", r.id, m.From, pr)
				// try to send append again.
				r.sendAppend(m.From)
			}
		} else {
			if pr.maybeUpdate(m.Index) {
				if r.maybeCommit() {
					// if commit, then broadcast AppendEntries
					r.bcastAppend()
				}

				if m.From == r.leadTransferee && pr.Match == r.RaftLog.LastIndex() {
					// todo
				}
			}
		}
	case pb.MessageType_MsgHeartbeatResponse:
		if pr.Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	case pb.MessageType_MsgTransferLeader:
		// todo
	}
}

func stepCandidate(r *Raft, m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		DPrintf("Raft %x no leader at term %d; dropping proposal", r.id, r.Term)
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case pb.MessageType_MsgRequestVoteResponse:
		gr := r.poll(m.From, m.MsgType, !m.Reject)
		DPrintf("%x [quorum:%d] has received %d %s votes and %d vote rejections", r.id, r.quorum(), gr, m.MsgType, len(r.votes)-gr)
		switch r.quorum() {
		case gr:
			r.becomeLeader()
			r.bcastAppend()
		case len(r.votes) - gr:
			r.becomeFollower(r.Term, None)
		}
	case pb.MessageType_MsgTimeoutNow:
		DPrintf("Raft %x [term %d state %v] ignored MsgTimeoutNow from %x", r.id, r.Term, r.State, m.From)
	}
}

func stepFollower(r *Raft, m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		if r.Lead == None {
			DPrintf("Raft %x no leader at term %d; dropping proposal", r.id, r.Term)
			return
		}
		m.To = r.Lead
		r.send(m)
	case pb.MessageType_MsgAppend:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleSnapshot(m)
	case pb.MessageType_MsgTimeoutNow:
		// todo
	case pb.MessageType_MsgTransferLeader:
		if r.Lead == None {
			DPrintf("%x no leader at term %d; dropping leader transfer msg", r.id, r.Term)
			return
		}
		m.To = r.Lead
		r.send(m)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Index < r.RaftLog.committed {
		r.send(pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, Index: r.RaftLog.committed})
		return
	}

	DPrintf("Raft %x received msgApp [logterm: %d, index: %d] from %x.\n", r.id, m.LogTerm, m.Index, m.From)
	ents := make([]pb.Entry, 0, len(m.Entries))
	for _, e := range m.Entries {
		ents = append(ents, *e)
	}
	if mlastIndex, ok := r.RaftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, ents...); ok {
		r.send(pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, Index: mlastIndex})
	} else {
		DPrintf("Raft %x [logterm: %d, index: %d] rejected msgApp [logterm: %d, index: %d] from %x",
			r.id, r.RaftLog.zeroTermOnErrCompacted(r.RaftLog.Term(m.Index)), m.Index, m.LogTerm, m.Index, m.From)
		r.send(pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, Index: m.Index, Reject: true, RejectHint: r.RaftLog.LastIndex()})
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.RaftLog.commitTo(m.Commit)
	r.send(pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse, To: m.From})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.promotable() && r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		r.send(pb.Message{MsgType: pb.MessageType_MsgHup})
	}
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++

	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		if r.State == StateLeader && r.leadTransferee != None {
			r.abortLeaderTransfer()
		}
	}

	if r.State != StateLeader {
		return
	}

	r.heartbeatElapsed = 0
	r.send(pb.Message{MsgType: pb.MessageType_MsgBeat})
}

func (r *Raft) appendEntry(ents ...pb.Entry) {
	li := r.RaftLog.LastIndex()
	for i := range ents {
		ents[i].Term = r.Term
		ents[i].Index = li + 1 + uint64(i)
	}

	r.RaftLog.append(ents...)
	r.Prs[r.id].maybeUpdate(r.RaftLog.LastIndex())
	r.maybeCommit()
}

func (r *Raft) maybeCommit() bool {
	mis := make(uint64Slice, 0, len(r.Prs))
	for _, pr := range r.Prs {
		mis = append(mis, pr.Match)
	}

	sort.Sort(sort.Reverse(mis))

	mci := mis[r.quorum()-1]
	return r.RaftLog.maybeCommit(mci, r.Term)
}

func (r *Raft) campaign() {
	term := r.Term
	msgType := pb.MessageType_MsgRequestVote
	r.becomeCandidate()

	if r.quorum() == r.poll(r.id, pb.MessageType_MsgRequestVoteResponse, true) {
		r.becomeLeader()
		return
	}

	for id := range r.Prs {
		if id == r.id {
			continue
		}

		DPrintf("Raft %x [logterm: %d, index: %d] sent %s request to %x at term %d",
			r.id, r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), msgType.String(), id, r.Term)

		msg := pb.Message{MsgType: pb.MessageType_MsgRequestVote, To: id, Term: term, LogTerm: r.RaftLog.LastTerm(), Index: r.RaftLog.LastIndex()}
		r.send(msg)
	}
}

func (r *Raft) send(m pb.Message) {
	m.From = r.id
	if m.MsgType == pb.MessageType_MsgRequestVote {
		if m.Term == 0 {
			panic(fmt.Sprintf("term should be set when sending %s", m.MsgType))
		}
	} else {
		if m.Term != 0 {
			panic(fmt.Sprintf("term should not be set when sending %s (was %d)", m.MsgType, m.Term))
		}

		if m.MsgType != pb.MessageType_MsgPropose {
			m.Term = r.Term
		}
	}
	r.msgs = append(r.msgs, m)
}

func (r *Raft) loadHardState(state pb.HardState) {
	if state.Commit < r.RaftLog.committed || state.Commit > r.RaftLog.LastIndex() {
		panic(fmt.Sprintf("Raft %x state.commit %d is out of range [%d, %d]", r.id, state.Commit, r.RaftLog.committed, r.RaftLog.LastIndex()))
	}
	r.RaftLog.committed = state.Commit
	r.Term = state.Term
	r.Vote = state.Vote
}

func (r *Raft) reset(term uint64) {
	if term != r.Term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()

	r.abortLeaderTransfer()

	r.votes = make(map[uint64]bool)
	for id := range r.Prs {
		r.Prs[id] = &Progress{Next: r.RaftLog.LastIndex() + 1, Match: 0}
	}
}

func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

func (r *Raft) abortLeaderTransfer() {
	r.leadTransferee = None
}

func (r *Raft) promotable() bool {
	_, exist := r.Prs[r.id]
	return exist
}

func (r *Raft) poll(id uint64, t pb.MessageType, v bool) (granted int) {
	if v {
		DPrintf("%x received %s from %x at term %d", r.id, t, id, r.Term)
	} else {
		DPrintf("%x received %s rejection from %x at term %d", r.id, t, id, r.Term)
	}

	if _, exist := r.votes[id]; !exist {
		r.votes[id] = v
	}

	for _, vv := range r.votes {
		if vv {
			granted++
		}
	}

	return
}

func pendingConfChangeEntries(ents []pb.Entry) (entries []pb.Entry) {
	for i := range ents {
		if ents[i].EntryType == pb.EntryType_EntryConfChange {
			entries = append(entries, ents[i])
		}
	}
	return
}
