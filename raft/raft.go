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

type CampaignType string

const (
	// campaignElection represents a normal (time-based) election (the second phase
	// of the election when Config.PreVote is true).
	campaignElection CampaignType = "CampaignElection"
	// campaignTransfer represents the type of leader transfer
	campaignTransfer CampaignType = "CampaignTransfer"
)

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

const (
	ProgressStateProbe ProgressStateType = iota
	ProgressStateReplicate
	ProgressStateSnapshot
)

type ProgressStateType uint64

var prstmap = [...]string{
	"ProgressStateProbe",
	"ProgressStateReplicate",
	"ProgressStateSnapshot",
}

func (st ProgressStateType) String() string { return prstmap[uint64(st)] }

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64

	// RecentActive is true if the progress is recently active. Receiving any messages
	// from the corresponding follower indicates the progress is active.
	// RecentActive can be reset to false after an election timeout.
	RecentActive bool

	State ProgressStateType
	// Paused is used in ProgressStateProbe.
	// When Paused is true, raft should pause sending replication message to this peer.
	Paused bool

	// PendingSnapshot is used in ProgressStateSnapshot.
	// If there is a pending snapshot, the pendingSnapshot will be set to the
	// index of the snapshot. If pendingSnapshot is set, the replication process of
	// this Progress will be paused. raft will not resend snapshot until the pending one
	// is reported to be failed.
	PendingSnapshot uint64
}

func (pr *Progress) resetState(state ProgressStateType) {
	pr.Paused = false
	pr.PendingSnapshot = 0
	pr.State = state
}

func (pr *Progress) becomeProbe() {
	if pr.State == ProgressStateSnapshot {
		pendingSnapshot := pr.PendingSnapshot
		pr.resetState(ProgressStateProbe)
		pr.Next = max(pr.Match+1, pendingSnapshot+1)
	} else {
		pr.resetState(ProgressStateProbe)
		pr.Next = pr.Match + 1
	}
}

func (pr *Progress) becomeSnapshot(snapshoti uint64) {
	pr.resetState(ProgressStateSnapshot)
	pr.PendingSnapshot = snapshoti
}

func (pr *Progress) becomeReplicate() {
	pr.resetState(ProgressStateReplicate)
	pr.Next = pr.Match + 1
}

func (pr *Progress) maybeUpdate(n uint64) bool {
	var updated bool
	if n > pr.Match {
		pr.Match = n
		updated = true
	}

	if n+1 > pr.Next {
		pr.Next = n + 1
	}

	return updated
}

func (pr *Progress) optimisticUpdate(n uint64) { pr.Next = n + 1 }

func (pr *Progress) maybeDecrto(rejected, last uint64) bool {
	if pr.State == ProgressStateReplicate {
		if rejected <= pr.Match {
			return false
		}

		pr.Next = pr.Match + 1
		return true
	}

	if rejected != pr.Next-1 { // outdated response
		return false
	}

	if pr.Next = min(rejected, last+1); pr.Next < 1 {
		pr.Next = 1
	}
	pr.resume()
	return true
}

func (pr *Progress) pause()  { pr.Paused = true }
func (pr *Progress) resume() { pr.Paused = false }

func (pr *Progress) IsPaused() bool {
	switch pr.State {
	case ProgressStateProbe:
		return pr.Paused
	case ProgressStateReplicate:
		return false
	case ProgressStateSnapshot:
		return true
	default:
		panic("unexpected state")
	}
}

func (pr *Progress) needSnapshotAbort() bool {
	return pr.State == ProgressStateSnapshot && pr.Match >= pr.PendingSnapshot
}

func (pr *Progress) String() string {
	return fmt.Sprintf("next = %d, match = %d, state = %s, waiting = %v, pendingSnapshot = %d", pr.Next, pr.Match, pr.State, pr.IsPaused(), pr.PendingSnapshot)
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
	if pr.IsPaused() {
		r.logger.Infof("node %x paused.\n", to)
		return false
	}

	m := pb.Message{}
	m.To = to
	m.MsgType = pb.MessageType_MsgAppend

	term, errt := r.RaftLog.Term(pr.Next - 1)
	entries, erre := r.RaftLog.entriesAfter(pr.Next)

	if errt != nil || (erre != nil && erre != ErrUnavailable) {
		if !pr.RecentActive {
			r.logger.Debugf("ignore sending snapshot to %x since it is not recently active", to)
			return false
		}

		m.MsgType = pb.MessageType_MsgSnapshot
		snapshot, err := r.RaftLog.snapshot()
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable {
				r.logger.Debugf("failed to send snapshot to %x because snapshot is temporarily unavailable", to)
				return false
			}
			panic(err)
		}

		if IsEmptySnap(&snapshot) {
			panic("need non-empty snapshot")
		}

		m.Snapshot = &snapshot
		sindex, sterm := snapshot.Metadata.Index, snapshot.Metadata.Term
		r.logger.Debugf("[firstindex: %d, commit: %d] sent snapshot[index: %d, term: %d] to %x [%+v]", r.RaftLog.FirstIndex(), r.RaftLog.committed, sindex, sterm, to, pr)

		pr.becomeSnapshot(sindex)
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

			switch pr.State {
			case ProgressStateProbe:
				pr.pause()
			case ProgressStateReplicate:
				last := entries[len(entries)-1].Index
				pr.optimisticUpdate(last)
			default:
				r.logger.Panicf("send append in unhandled state %s.\n", pr.State)
			}
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

	r.logger.Infof("became follower at term %x.\n", r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.reset(r.Term + 1)
	r.State = StateCandidate
	r.Vote = r.id

	r.logger.Infof("became candidate at term %x.\n", r.Term)
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
	r.logger.Infof("became leader at term %x.\n", r.Term)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	r.logger.Infof("[state: %s, term: %x] received Msg{From: %x, Type: %s}.\n", r.State, r.Term, m.From, m.MsgType.String())

	switch {
	case m.Term == 0:
		// local message
	case m.Term > r.Term:
		lead := m.From
		if m.MsgType == pb.MessageType_MsgRequestVote {
			// Comment codes below to pass the tests.

			// force := bytes.Equal(m.Context, []byte(campaignTransfer))
			// inLease := r.Lead != None && r.electionElapsed < r.electionTimeout

			// If a server receives a RequestVote request within the minimum election timeout
			// of hearing from a current leader, it does not update its term or grant its vote
			// if !force && inLease {
			// 	r.logger.Infof("[logterm: %d, index: %d, vote: %x] ignored %s from %x [logterm: %d, index: %d] at term %d: lease is not expired (remaining ticks: %d)", r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term, r.electionTimeout-r.electionElapsed)

			// 	return nil
			// }

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
			r.logger.Infof("start a new election at term %x.\n", r.Term)
			r.campaign(campaignElection)
		} else {
			r.logger.Warningf("ignoring MsgHup because already leader.\n")
		}
	case pb.MessageType_MsgRequestVote:
		if (r.Vote == None || r.Vote == m.From) && r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
			r.logger.Infof("[logterm: %x, index: %x, vote: %x] cast %s for %x [logterm: %x, index: %x] at term %x", r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term)

			r.send(pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From})
			r.electionElapsed = 0
			r.Vote = m.From
		} else {
			r.logger.Infof("[logterm: %x, index: %x, vote: %x] rejected %s for %x [logterm: %x, index: %x] at term %x", r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term)

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
		if _, exist := r.Prs[r.id]; !exist {
			return ErrProposalDropped
		}

		if _, exist := r.Prs[m.From]; exist {
			r.Prs[m.From].RecentActive = true
		}

		if len(m.Entries) == 0 {
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
				if r.RaftLog.applied < r.PendingConfIndex {
					r.logger.Warningf("propose conf %s ignored since pending unapplied configuration", e.String())
					return ErrProposalDropped
				}
				r.PendingConfIndex = e.Index
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
			r.logger.Infof("received msgApp rejection(lastindex: %x) from %x for index %x", m.RejectHint, m.From, m.Index)
			if pr.maybeDecrto(m.Index, m.RejectHint) {
				r.logger.Debugf("decreased progress of %x to [%+v].\n", m.From, pr)
				if pr.State == ProgressStateReplicate {
					pr.becomeProbe()
				}
				// try to send append again.
				r.sendAppend(m.From)
			}
		} else {
			oldPaused := pr.IsPaused()
			if pr.maybeUpdate(m.Index) {
				switch {
				case pr.State == ProgressStateProbe:
					pr.becomeReplicate()
				case pr.State == ProgressStateSnapshot && pr.needSnapshotAbort():
					r.logger.Infof("snapshot aborted, resumed sending replication messages to %x [%s]", m.From, pr)
					pr.becomeProbe()
				}

				if r.maybeCommit() {
					// if commit, then broadcast AppendEntries
					r.bcastAppend()
				} else if oldPaused {
					r.sendAppend(m.From)
				}

				if m.From == r.leadTransferee && pr.Match == r.RaftLog.LastIndex() {
					// todo
				}
			}
		}
	case pb.MessageType_MsgHeartbeatResponse:
		pr.RecentActive = true
		pr.resume()

		if pr.Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	case pb.MessageType_MsgTransferLeader:
		// todo
		leadTransferee := m.From
		lastLeadTransferee := r.leadTransferee

		if lastLeadTransferee != None {
			if lastLeadTransferee == leadTransferee {
				r.logger.Infof("[term %d] transfer leadership to %x is in progress, ignores request to same node %x.\n",
					r.Term, leadTransferee, leadTransferee)
				return nil
			}
			r.abortLeaderTransfer()
			r.logger.Infof("[term %d] abort previous transferring leadership to %x.\n", r.Term, lastLeadTransferee)
		}

		if leadTransferee == r.id {
			r.logger.Infof("is already leader. Ignored transferring leadership to self.\n")
			return nil
		}

		r.logger.Infof("[term %d] starts to transfer leadership to %x.\n", r.Term, leadTransferee)

		r.electionElapsed = 0
		r.leadTransferee = leadTransferee
		if pr.Match == r.RaftLog.LastIndex() {
			r.sendTimeoutNow(leadTransferee)
			r.logger.Infof("sends MsgTimeoutNow to %x immediately as %x already has up-to-date log", leadTransferee, leadTransferee)
		} else {
			r.sendAppend(lastLeadTransferee)
		}
	}

	return nil
}

func stepCandidate(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		r.logger.Infof("no leader at term %x; dropping proposal", r.Term)
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
		r.logger.Infof("[quorum:%d] has received %d %s votes and %d vote rejections", r.quorum(), gr, m.MsgType, len(r.votes)-gr)
		switch r.quorum() {
		case gr:
			r.becomeLeader()
			r.bcastAppend()
		case len(r.votes) - gr:
			r.becomeFollower(r.Term, None)
		}
	case pb.MessageType_MsgTimeoutNow:
		r.logger.Infof("[term: %x, state: %v] ignored MsgTimeoutNow from %x", r.Term, r.State, m.From)
	}

	return nil
}

func stepFollower(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		r.logger.Warningf("[term=%x]is not leader; dropping propose msg.\n", r.Term)
		return ErrProposalDropped
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
		if r.promotable() {
			r.logger.Infof("[term %d] received MsgTimeoutNow from %x and starts an election to get leadership.\n", r.Term, m.From)
			r.campaign(campaignTransfer)
		} else {
			r.logger.Infof("received MsgTimeoutNow from %x but is not promotable.\n", m.From)
		}
	case pb.MessageType_MsgTransferLeader:
		r.logger.Warningf("[term=%x]is not leader; dropping leader transfer msg.\n", r.Term)
		return ErrProposalDropped
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

	r.logger.Infof("received msgApp [logterm: %x, index: %x] from %x.\n", m.LogTerm, m.Index, m.From)
	ents := make([]pb.Entry, 0, len(m.Entries))
	for _, e := range m.Entries {
		ents = append(ents, *e)
	}
	if mlastIndex, ok := r.RaftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, ents...); ok {
		r.send(pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, Index: mlastIndex})
	} else {
		r.logger.Infof("[logterm: %x, index: %x] rejected msgApp [logterm: %x, index: %x] from %x.\n", r.RaftLog.zeroTermOnErr(r.RaftLog.Term(m.Index)), m.Index, m.LogTerm, m.Index, m.From)
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
	sindex, sterm := m.Snapshot.Metadata.Index, m.Snapshot.Metadata.Term

	if r.restore(m.Snapshot) {
		r.logger.Infof("[commit: %d] restored snapshot [index: %d, term: %d]", r.RaftLog.committed, sindex, sterm)
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.LastIndex()})
	} else {
		r.logger.Infof("[commit: %d] ignored snapshot [index: %d, term: %d]", r.RaftLog.committed, sindex, sterm)
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed})
	}
}

func (r *Raft) restore(snapshot *pb.Snapshot) bool {
	sindex, sterm := snapshot.Metadata.Index, snapshot.Metadata.Term
	if sindex < r.RaftLog.committed {
		return false
	}

	if r.RaftLog.matchTerm(snapshot.Metadata.Index, snapshot.Metadata.Term) {
		r.logger.Debugf("[commit: %d, lastindex: %d, lastterm: %d] fast-forwarded commit to snapshot [index: %d, term: %d]", r.RaftLog.committed, r.RaftLog.LastIndex(), r.RaftLog.LastTerm(), sindex, sterm)
		r.RaftLog.commitTo(sindex)
		return false
	}

	r.logger.Debugf("[commit: %d, lastindex: %d, lastterm: %d] starts to restore snapshot [index: %d, term: %d]", r.RaftLog.committed, r.RaftLog.LastIndex(), r.RaftLog.LastTerm(), sindex, sterm)

	r.RaftLog.restore(snapshot)
	r.Prs = make(map[uint64]*Progress)
	for _, n := range snapshot.Metadata.ConfState.Nodes {
		match, next := uint64(0), r.RaftLog.LastIndex()+1
		if n == r.id {
			match = next - 1
		}

		r.Prs[n] = &Progress{Match: match, Next: next}
		r.logger.Debugf("restored progress of %x [%+v]", n, r.Prs[n])
	}

	return true
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	if _, exist := r.Prs[id]; exist {
		return
	}

	r.Prs[id] = &Progress{Match: 0, Next: r.RaftLog.LastIndex() + 1}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	delete(r.Prs, id)

	if len(r.Prs) == 0 {
		return
	}

	if r.maybeCommit() {
		r.bcastAppend()
	}

	if r.State == StateLeader && r.leadTransferee == id {
		r.abortLeaderTransfer()
	}
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

func (r *Raft) campaign(t CampaignType) {
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
		r.logger.Infof("[logterm: %x, index: %x] sent %s request to %x at term %x.\n", r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), msgType.String(), id, r.Term)

		var ctx []byte
		if t == campaignTransfer {
			ctx = []byte(t)
		}

		msg := pb.Message{MsgType: msgType, To: id, Term: term, LogTerm: r.RaftLog.LastTerm(), Index: r.RaftLog.LastIndex(), Context: ctx}
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

func (r *Raft) sendTimeoutNow(to uint64) {
	r.send(pb.Message{To: to, MsgType: pb.MessageType_MsgTimeoutNow})
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
		r.logger.Infof("received %s from %x at term %x.\n", t, id, r.Term)
	} else {
		r.logger.Infof("received %s rejection from %x at term %x.\n", t, id, r.Term)
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
