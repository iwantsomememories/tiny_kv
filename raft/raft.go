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
	"os"
	"sort"
	"strings"

	"github.com/pingcap-incubator/tinykv/log"
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

	// RecentActive is true if the progress is recently active. Receiving any messages
	// from the corresponding follower indicates the progress is active.
	// RecentActive can be reset to false after an election timeout.
	RecentActive bool
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
	if rejected != p.Next-1 { // outdated response
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

	logger *log.Logger
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
			log.Panicf("cannot specify both Config.peers when restarting raft.")
		}

		peers = cstate.Nodes
	}

	r := &Raft{
		id:               c.ID,
		RaftLog:          raftLog,
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		logger:           log.NewLogger(os.Stderr, fmt.Sprintf("Raft %x: ", c.ID)),
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

	r.logger.Debugf("start with [peers: [%s], term: %x, commit: %x, applied: %x, lastindex: %x, lastterm: %x]",
		strings.Join(peersStrs, ","), r.Term, r.RaftLog.committed, r.RaftLog.applied, r.RaftLog.LastIndex(), r.RaftLog.LastTerm())

	return r
}

func (r *Raft) quorum() int { return len(r.Prs)/2 + 1 }

func (r *Raft) softState() *SoftState { return &SoftState{Lead: r.Lead, RaftState: r.State} }

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{Term: r.Term, Vote: r.Vote, Commit: r.RaftLog.committed}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	pr := r.Prs[to]
	m := pb.Message{}
	m.To = to
	m.MsgType = pb.MessageType_MsgAppend

	term, errt := r.RaftLog.Term(pr.Next - 1)
	entries, erre := r.RaftLog.entriesAfter(pr.Next)

	if errt != nil || (erre != nil && erre != ErrUnavailable) {
		// todo

	} else {
		m.Index = pr.Next - 1
		m.LogTerm = term
		m.Commit = r.RaftLog.committed
		if len(entries) > 0 {
			var ents []*pb.Entry
			for _, entry := range entries {
				ents = append(ents, &pb.Entry{EntryType: entry.EntryType, Term: entry.Term, Index: entry.Index, Data: entry.Data})
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
	r.send(pb.Message{To: to, MsgType: pb.MessageType_MsgHeartbeat, Commit: commit})
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
	r.Lead = lead

	r.logger.Debugf("became follower at term %x.\n", r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.reset(r.Term + 1)
	r.State = StateCandidate
	r.Vote = r.id

	r.logger.Debugf("became candidate at term %x.\n", r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.reset(r.Term)
	r.State = StateLeader
	r.Lead = r.id

	ents, err := r.RaftLog.uncommittedEnts()
	if err != nil {
		r.logger.Panicf("unexpected error getting uncommitted entries (%v)", err)
	}

	pcents := pendingConfChangeEntries(ents)
	if len(pcents) > 1 {
		r.logger.Panicf("unexpected multiple uncommitted config entry")
	}
	if len(pcents) == 1 {
		r.PendingConfIndex = pcents[0].Index
	}

	r.appendEntry(pb.Entry{Data: nil, EntryType: pb.EntryType_EntryNormal})
	r.logger.Debugf("became leader at term %x.\n", r.Term)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	r.logger.Debugf("[state: %s, term: %x] received Msg{From: %x, Type: %s}.\n", r.State, r.Term, m.From, m.MsgType.String())

	switch {
	case m.Term == 0:
		// local message
	case m.Term > r.Term:
		lead := m.From
		if m.MsgType == pb.MessageType_MsgRequestVote {
			// todo
			lead = None
		}
		r.becomeFollower(m.Term, lead)
	case m.Term < r.Term:
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat {
			r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse})
		}
		return nil
	}

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if r.State != StateLeader {
			ents := r.RaftLog.nextEnts()
			if pents := pendingConfChangeEntries(ents); len(pents) != 0 {
				r.logger.Warningf("cannot campaign at term %x since there are still %x pending configuration changes to apply.\n", r.Term, len(pents))
				return nil
			}
			r.logger.Debugf("start a new election at term %x.\n", r.Term)
			r.campaign()
		} else {
			r.logger.Warningf("ignoring MsgHup because already leader.\n")
		}
	case pb.MessageType_MsgRequestVote:
		if (r.Vote == None || r.Vote == m.From) && r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
			r.logger.Debugf("[logterm: %x, index: %x, vote: %x] cast %s for %x [logterm: %x, index: %x] at term %x", r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term)

			r.send(pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From})
			r.electionElapsed = 0
			r.Vote = m.From
		} else {
			r.logger.Debugf("[logterm: %x, index: %x, vote: %x] rejected %s for %x [logterm: %x, index: %x] at term %x", r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term)

			r.send(pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From, Reject: true})
		}
	default:
		switch r.State {
		case StateFollower:
			return stepFollower(r, m)
		case StateCandidate:
			return stepCandidate(r, m)
		case StateLeader:
			return stepLeader(r, m)
		}
	}

	return nil
}

func stepLeader(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
		return nil
	case pb.MessageType_MsgCheckQuorum:
		if !r.checkQuorumActive() {
			r.logger.Warningf("stepped down to follower since quorum is not active.\n")
			r.becomeFollower(r.Term, None)
		}
		return nil
	case pb.MessageType_MsgPropose:
		if len(m.Entries) == 0 {
			return ErrProposalDropped
		}

		if _, exist := r.Prs[r.id]; !exist {
			return ErrProposalDropped
		}

		if r.leadTransferee != None {
			r.logger.Warningf("[term: %x] transfer leadership to %x is in progress; dropping proposal", r.Term, r.leadTransferee)
			return ErrProposalDropped
		}

		ents := []pb.Entry{}
		for _, e := range m.Entries {
			if e.EntryType == pb.EntryType_EntryConfChange {
				// todo
				if r.PendingConfIndex != 0 {
					r.logger.Warningf("propose conf %s ignored since pending unapplied configuration", e.String())
					e.EntryType = pb.EntryType_EntryNormal
				} else {
					r.PendingConfIndex = e.Index
				}
			}
			ents = append(ents, pb.Entry{Index: e.Index, Term: e.Term, EntryType: e.EntryType, Data: e.Data})
		}

		r.appendEntry(ents...)
		r.bcastAppend()
		return nil
	}

	pr, exist := r.Prs[m.From]
	if !exist {
		r.logger.Warningf("no progress available for %x.\n", m.From)
		return nil
	}
	switch m.MsgType {
	case pb.MessageType_MsgAppendResponse:
		pr.RecentActive = true

		if m.Reject {
			r.logger.Debugf("received msgApp rejection(lastindex: %x) from %x for index %x", m.RejectHint, m.From, m.Index)
			if pr.maybeDecrto(m.Index, m.RejectHint) {
				r.logger.Debugf("decreased progress of %x to [%+v].\n", m.From, pr)
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
		pr.RecentActive = true

		if pr.Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	case pb.MessageType_MsgTransferLeader:
		// todo
	}

	return nil
}

func stepCandidate(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		r.logger.Debugf("no leader at term %x; dropping proposal", r.Term)
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
		r.logger.Debugf("[quorum:%d] has received %d %s votes and %d vote rejections", r.quorum(), gr, m.MsgType, len(r.votes)-gr)
		switch r.quorum() {
		case gr:
			r.becomeLeader()
			r.bcastAppend()
		case len(r.votes) - gr:
			r.becomeFollower(r.Term, None)
		}
	case pb.MessageType_MsgTimeoutNow:
		r.logger.Debugf("[term: %x, state: %v] ignored MsgTimeoutNow from %x", r.Term, r.State, m.From)
	}

	return nil
}

func stepFollower(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		if r.Lead == None {
			r.logger.Warningf("no leader at term %x; dropping proposal", r.Term)
			return ErrProposalDropped
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
			r.logger.Warningf("no leader at term %x; dropping leader transfer msg", r.Term)
			return ErrProposalDropped
		}
		m.To = r.Lead
		r.send(m)
	}

	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Index < r.RaftLog.committed {
		r.send(pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, Index: r.RaftLog.committed})
		return
	}

	r.logger.Debugf("received msgApp [logterm: %x, index: %x, entries: %+v] from %x.\n", m.LogTerm, m.Index, m.Entries, m.From)
	ents := make([]pb.Entry, 0, len(m.Entries))
	for _, e := range m.Entries {
		ents = append(ents, *e)
	}
	if mlastIndex, ok := r.RaftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, ents...); ok {
		r.send(pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, Index: mlastIndex})
	} else {
		r.logger.Debugf("[logterm: %x, index: %x] rejected msgApp [logterm: %x, index: %x] from %x.\n", r.RaftLog.zeroTermOnErr(r.RaftLog.Term(m.Index)), m.Index, m.LogTerm, m.Index, m.From)
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
	if r.promotable() && r.pastElectionTimeout() {
		r.electionElapsed = 0
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup})
	}
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++

	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgCheckQuorum})
		if r.State == StateLeader && r.leadTransferee != None {
			r.abortLeaderTransfer()
		}
	}

	if r.State != StateLeader {
		return
	}

	r.heartbeatElapsed = 0
	r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgBeat})
}

func (r *Raft) appendEntry(ents ...pb.Entry) {
	li := r.RaftLog.LastIndex()
	for i := range ents {
		ents[i].Term = r.Term
		ents[i].Index = li + 1 + uint64(i)
	}

	r.RaftLog.append(ents...)
	r.logger.Debugf("append new ents{%+v} in its log.\n", ents)
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
	r.becomeCandidate()
	term := r.Term
	msgType := pb.MessageType_MsgRequestVote

	if r.quorum() == r.poll(r.id, pb.MessageType_MsgRequestVoteResponse, true) {
		r.becomeLeader()
		return
	}

	for id := range r.Prs {
		if id == r.id {
			continue
		}

		r.logger.Debugf("[logterm: %x, index: %x] sent %s request to %x at term %x.\n", r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), msgType.String(), id, r.Term)

		msg := pb.Message{MsgType: msgType, To: id, Term: term, LogTerm: r.RaftLog.LastTerm(), Index: r.RaftLog.LastIndex()}
		r.send(msg)
	}
}

func (r *Raft) send(m pb.Message) {
	m.From = r.id
	if m.MsgType == pb.MessageType_MsgRequestVote {
		if m.Term == 0 {
			r.logger.Panicf("term should be set when sending %s", m.MsgType)
		}
	} else {
		if m.Term != 0 {
			r.logger.Panicf("term should not be set when sending %s (was %x)", m.MsgType, m.Term)
		}

		if m.MsgType != pb.MessageType_MsgPropose {
			m.Term = r.Term
		}
	}
	r.msgs = append(r.msgs, m)
}

func (r *Raft) loadHardState(state pb.HardState) {
	if state.Commit < r.RaftLog.committed || state.Commit > r.RaftLog.LastIndex() {
		r.logger.Panicf("Raft %x state.commit %x is out of range [%x, %x]", r.id, state.Commit, r.RaftLog.committed, r.RaftLog.LastIndex())
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
		r.logger.Debugf("received %s from %x at term %x.\n", t, id, r.Term)
	} else {
		r.logger.Debugf("received %s rejection from %x at term %x.\n", t, id, r.Term)
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

func (r *Raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomizedElectionTimeout
}

func (r *Raft) checkQuorumActive() bool {
	var act int

	for id := range r.Prs {
		if id == r.id {
			act++
			continue
		}

		if r.Prs[id].RecentActive {
			act++
		}

		r.Prs[id].RecentActive = false
	}

	return act >= r.quorum()
}

func pendingConfChangeEntries(ents []pb.Entry) (pents []pb.Entry) {
	for i := range ents {
		if ents[i].EntryType == pb.EntryType_EntryConfChange {
			pents = append(pents, ents[i])
		}
	}
	return
}
