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

	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
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
	votes     map[uint64]bool
	voteCnt   uint64
	denialCnt uint64

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout       int
	randomElectionTimeout int
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
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	prs := make(map[uint64]*Progress)
	lastIndex, _ := c.Storage.LastIndex()
	if len(c.peers) == 0 {
		c.peers = confState.Nodes
	}
	for _, peer := range c.peers {
		if c.ID == peer {
			prs[peer] = &Progress{Match: 0, Next: lastIndex + 1}
		} else {
			prs[peer] = &Progress{Match: lastIndex, Next: lastIndex + 1}
		}
	}
	raft := &Raft{
		id:      c.ID,
		Term:    hardState.Term,
		Vote:    hardState.Vote,
		Prs:     prs,
		RaftLog: newLog(c.Storage),
		State:   StateFollower,
		votes:   make(map[uint64]bool),
		// voteCnt: 0,
		// denialCnt: 0,
		msgs:             make([]pb.Message, 0),
		Lead:             hardState.Vote,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		// randomElectionTimeout: c.ElectionTick+,
		// heartbeatElapsed: 0,
		// electionElapsed: 0,
		// leadTransferee: 0,
		// PendingConfIndex: 0,
	}
	raft.becomeFollower(raft.Term, raft.Lead) // don't change persist Lead
	return raft
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Commit:  min(r.Prs[to].Match, r.RaftLog.committed), // may not sync commited log
		Term:    r.Term,
	})
}

func (r *Raft) sendVoteRequest(to uint64) {
	// get log index and term to check uptodate or not
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: lastTerm,
		Index:   lastIndex,
	})
}

func (r *Raft) abortLeaderTransferee() {
	r.leadTransferee = None
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower, StateCandidate:
		r.electionElapsed += 1
		if r.electionElapsed >= r.randomElectionTimeout {
			r.electionElapsed = 0
			// r.becomeCandidate()
			if err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, From: r.id}); err != nil {
				panic(err)
			}
		}
	case StateLeader:
		r.heartbeatElapsed += 1
		r.electionElapsed += 1
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			// r.boradcastHeartbeat()
			if err := r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat, From: r.id}); err != nil {
				panic(err)
			}
		}
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0
			if r.State == StateLeader && r.leadTransferee != None {
				r.abortLeaderTransferee()
			}
		}
	}
}

func (r *Raft) resetTick() {
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.State = StateFollower
	r.resetTick()
	r.Term = term
	r.Lead = lead
	r.Vote = lead
	r.leadTransferee = None
	r.PendingConfIndex = None
	// r.leadTransferee = None // TODO@wy: to test
	// Your Code Here (2A).
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.resetTick()
	r.Term += 1
	r.Vote = r.id
	r.State = StateCandidate
	r.voteCnt = 1
	r.denialCnt = 0
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.leadTransferee = None
	r.PendingConfIndex = None
	// for peer := range r.Prs {
	// 	r.sendVoteRequest(peer)
	// }
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	log.Debugf("%+v become leader\n", r.id)
	r.resetTick()
	r.Lead = r.id
	r.State = StateLeader
	r.leadTransferee = None
	r.PendingConfIndex = None

	lastIndex, _ := r.RaftLog.storage.LastIndex() // need done before append, because appendEntries change leader's match and next
	for peer, p := range r.Prs {                  // init prs's state
		if peer == r.id {
			p.Match = lastIndex
		}
		// else {
		// 	p.Match = 0
		// }
		p.Next = lastIndex + 1
	}
	r.appendEntries(&pb.Entry{ // append cause leader's  progress change
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     r.RaftLog.LastIndex() + 1,
		Data:      nil,
	})

	r.logSync()
}

func (r *Raft) logSync() {
	r.boradcastAppend()
	r.maybeCommit()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if _, ok := r.Prs[r.id]; !ok && !util.IsInitialMsg(&m) { // Note@wy initialMsg don't need to promise r.id in prs
		return nil
	}
	// if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgAppendResponse || m.MsgType == pb.MessageType_MsgHup {
	log.Debugf(" id: %+v   step %+v", r.id, m)
	// }

	// Firstly, compare the term of message with raft.term
	switch {
	case m.Term == 0:
		// local message
	case m.Term > r.Term:
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgSnapshot { // only for message recieve from leader
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	case m.Term < r.Term:
		return nil
	}
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.Hup()
	case pb.MessageType_MsgRequestVote:
		// We can vote if this is a repeat of a vote we've already cast...
		canVote := (r.Vote == m.From) ||
			// ...we haven't voted and we don't think there's a leader yet in this term...
			(r.Vote == None && r.Lead == None) ||
			// vote and lager term
			(m.Term > r.Term)
		// log.Printf("id:%+v entries:%+v canvote:%+v", r.id, r.RaftLog.entries, canVote)
		if canVote && r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Reject:  false,
			})
			// r.becomeFollower(m.Term, m.From) // need to reset state and tick
			r.electionElapsed = 0
			r.Vote = m.From // only need to change vote but not lead
		} else {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Reject:  true,
			})
		}
	default:
		switch r.State {
		case StateFollower:
			err := r.stepFollower(m)
			if err != nil {
				return err
			}
		case StateCandidate:
			err := r.stepCandidate(m)
			if err != nil {
				return err
			}
		case StateLeader:
			err := r.stepLeader(m)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Raft) Hup() {
	if r.State == StateLeader {
		return
	}
	r.campaign()
}

func (r *Raft) campaign() {
	r.becomeCandidate()
	if len(r.Prs) == 1 {
		r.becomeLeader()
	}
	for peer := range r.Prs {
		if peer != r.id {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				From:    r.id,
				To:      peer,
				Term:    r.Term,
				Index:   r.RaftLog.LastIndex(),
				LogTerm: r.RaftLog.LastTerm(),
			})
		}
	}
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		if r.Lead == None {
			m.To = r.Lead // transmit to leader
			r.msgs = append(r.msgs, m)
		}
	case pb.MessageType_MsgHeartbeat:
		r.electionElapsed = 0
		r.Lead = m.From //  have a new lead
		r.handleHeartbeat(m)
	// case pb.MessageType_MsgRequestVote:
	case pb.MessageType_MsgAppend:
		// log.Printf("test1")
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleAppendEntries(m)
	case pb.MessageType_MsgSnapshot:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		if r.Lead == None {
			log.Infof("%x no leader at term %d; dropping leader transfer to %+v msg", r.id, r.Term, m.From)
			return nil
		}
		m.To = r.Lead
		r.msgs = append(r.msgs, m)
	case pb.MessageType_MsgTimeoutNow:
		r.campaign()
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgRequestVoteResponse:
		if _, ok := r.votes[m.From]; ok {
			return nil
		}
		r.votes[m.From] = !m.Reject
		if m.Reject {
			r.denialCnt += 1
			if r.denialCnt >= uint64(len(r.Prs)/2+1) {
				r.becomeFollower(r.Term, None) // reuse r.Term , m.From may not be leader
			}
		} else {
			r.voteCnt += 1
			if r.voteCnt >= uint64(len(r.Prs)/2+1) {
				r.becomeLeader()
			}
		}
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From) // already have a leader , exit election
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	}

	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		// log.Info("%+vsend propose %+v\n", r.id, m.Entries)
		if r.leadTransferee != None {
			log.Debugf("%x [term %d] transfer leadership to %x is in progress; dropping proposal", r.id, r.Term, r.leadTransferee)
			return ErrProposalDropped
		}
		if len(m.Entries) == 0 {
			return errors.New("Empty propose message.")
		}
		if !r.appendEntries(m.Entries...) {
			return ErrProposalDropped
		}
		r.boradcastAppend()
	case pb.MessageType_MsgBeat:
		r.boradcastHeartbeat()
		return nil
	// case pb.mes

	case pb.MessageType_MsgHeartbeatResponse:

		if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}

	case pb.MessageType_MsgAppendResponse:
		pr := r.Prs[m.From]
		if m.Reject {
			nextProbeIndex := m.Index
			if m.LogTerm > 0 {
				nextProbeIndex = r.RaftLog.findConflictByTerm(nextProbeIndex, m.LogTerm)
			}

			if nextProbeIndex == m.Index && m.LogTerm == r.RaftLog.zeroTermOnErrCompacted(r.RaftLog.Term(nextProbeIndex)) { // may match this time
				pr.Match = nextProbeIndex
				pr.Next = pr.Match + 1
				r.sendAppend(m.From)
			} else if nextProbeIndex+1 < pr.Next {
				pr.Next = max(1, min(nextProbeIndex+1, m.Index)) // use nextProbeIndex+1, because nextProbeIndex may match
				r.sendAppend(m.From)
			}
		} else {
			if r.Prs[m.From].Match < m.Index {
				r.Prs[m.From].Match = max(r.Prs[m.From].Match, m.Index)
				r.Prs[m.From].Next = max(r.Prs[m.From].Next, r.Prs[m.From].Match+1)
				pr := r.Prs[m.From]
				if r.maybeCommit() {
					r.boradcastAppend()
				} else if pr.Match < r.RaftLog.LastIndex() { // still need to send to rest entries
					// TODO@wy add more conditional to handle send or not , need in TestLeaderCommitEntry2AB
					// need to update committed of m.From if need
					r.sendAppend(m.From)
				}
			}
			if r.Prs[m.From].Match == r.RaftLog.LastIndex() && r.leadTransferee == m.From {
				r.sendTimeout(r.leadTransferee)
			}
		}
	case pb.MessageType_MsgTransferLeader:
		newTransferLeader := m.From
		if _, ok := r.Prs[newTransferLeader]; !ok {
			log.Debugf("Peer don't exit in %+v for leader transfering to %+v\n", r.id, newTransferLeader)
			return nil // Todo@wy some error ?
		}
		lastTransferLeader := r.leadTransferee
		if lastTransferLeader != None {
			if newTransferLeader == lastTransferLeader {
				log.Debugf("%x [term %d] transfer leadership to %x is in progress, ignores request to same node %x",
					r.id, r.Term, newTransferLeader, lastTransferLeader)
				return nil
			}
			r.abortLeaderTransferee()
		}
		if newTransferLeader == r.id {
			return nil
		}
		r.leadTransferee = newTransferLeader
		r.electionElapsed = 0
		if r.Prs[newTransferLeader].Match == r.RaftLog.LastIndex() {
			r.sendTimeout(newTransferLeader)
		} else {
			r.sendAppend(newTransferLeader)
		}
	}
	return nil
}

func (r *Raft) boradcastHeartbeat() {
	for peer := range r.Prs {
		if peer != r.id {
			r.sendHeartbeat(peer)
		}
	}
}

func (r *Raft) sendTimeout(to uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	})
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	r.resetTick()                      // Note@wy need to reset tick , fix big bug
	if m.Index < r.RaftLog.committed { // already commit, return directly
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Index:   r.RaftLog.committed,
		})
		return
	}
	// ents := make([]pb.Entry, 0)
	// for i, ent := range m.Entries {
	// 	ents[i] = *ent
	// }
	if mlastIndex, ok := r.RaftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Index:   mlastIndex,
			Reject:  false,
		})
	} else {
		hintIndex := min(m.Index, r.RaftLog.LastIndex())
		log.Debugf("%+v recevie from %+v hint %+v", r.id, m.From, hintIndex)
		hintIndex = r.RaftLog.findConflictByTerm(hintIndex, m.LogTerm)
		hintTerm, err := r.RaftLog.Term(hintIndex)
		if err != nil {
			panic(fmt.Sprintf("term(%d) must be valid, but got %v", hintIndex, err))
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
			Index:   hintIndex,
			LogTerm: hintTerm,
		})
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	pr := r.Prs[to]
	nexti := pr.Next
	// if pr.Next != pr.Match+1 { // if not match, need to match log and append log entry
	// 	nexti = pr.Match + 1
	// 	if pr.Match == 0 {
	// 		// first time to append, because initStoreLogTerm=5 in tinykv
	// 		log.Infof("to=%v %v %v %v", to, pr.Next, r.RaftLog.LastIndex(), ents)
	// 		// if len(ents) != 0 {
	// 		r.msgs = append(r.msgs, pb.Message{
	// 			MsgType: pb.MessageType_MsgAppend,
	// 			From:    r.id,
	// 			To:      to,
	// 			Term:    r.Term,
	// 			Entries: make([]*pb.Entry,0),
	// 			Index:   r.RaftLog.storage.FirstIndex(),
	// 			LogTerm: nextTerm,
	// 			Commit:  r.RaftLog.committed, // update peer's committed
	// 		})
	// 		return true
	// 	}
	// }
	nexti = max(nexti, 1)                       // Todo@wy if nexti is 0 append all
	nextTerm, errt := r.RaftLog.Term(nexti - 1) //Todo@wy handle -1
	ents, erre := r.RaftLog.getEntries(nexti, r.RaftLog.LastIndex()+1)
	if errt != nil || erre != nil {
		log.Debugf("error find term or entry")
		// send snapshot
		return r.sendSnapshot(to)
	}

	// log.Debugf("leader: %+v to=%v pr:%+v logs:%+v %v nexti%v logterm%v\n", r.id, to, pr, r.RaftLog.entries, ents, nexti-1, nextTerm)
	// if len(ents) != 0 {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Entries: ents,
		Index:   nexti - 1,
		LogTerm: nextTerm,
		Commit:  r.RaftLog.committed, // update peer's committed
	})
	// }

	return true
}

func (r *Raft) boradcastAppend() {
	for peer := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
}

func (r *Raft) appendEntries(ents ...*pb.Entry) bool {
	// n := len(ents)
	off := r.RaftLog.LastIndex()

	for i, ent := range ents { // complete the index and term
		if ent.EntryType == pb.EntryType_EntryConfChange {
			if len(ents) != 1 {
				log.Fatalf("Propose confchang, but the length of entry is %+v\n", len(ents))
			}
			if r.PendingConfIndex > r.RaftLog.applied {
				return false
			} else {
				r.PendingConfIndex = off + 1
			}
		}
		ents[i].Term = r.Term
		ents[i].Index = (off) + 1
		// log.Debugf("term %v, index %v", ents[i].Term, ents[i].Index)
		off += 1
	}
	// Todo@wy some entry Index and term already init by peer_msg_handle
	r.RaftLog.appendEntries(ents...)
	// for peer := range r.Prs {
	// 	r.Prs[peer].Next = off // update peer's next to check func sendappend's type(log match or log append)
	// }
	r.Prs[r.id].Match = off
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	r.maybeCommit() // for only one peer
	return true
}

// use median of match to find committed Index
func (r *Raft) maybeCommit() bool {
	if len(r.Prs) == 0 {
		return false
	}
	matchs := make([]uint64, 0, len(r.Prs))
	for peer := range r.Prs {
		matchs = append(matchs, r.Prs[peer].Match)
	}
	sort.Slice(matchs, func(i, j int) bool { return matchs[i] < matchs[j] })
	committedIndex := matchs[(len(r.Prs)-1)/2] // len(r.Prs) maybe even
	// log.Printf("%+v %+v\n", matchs, committedIndex)
	return r.RaftLog.maybeCommit(committedIndex, r.Term)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.resetTick()                // Note@wy need to reset tick , fix big bug
	r.RaftLog.commitTo(m.Commit) // Todo@wy update committed , but how to promise log snyc already finished?
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
	})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	snap := m.Snapshot
	if r.restore(snap) {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Index:   r.RaftLog.LastIndex(),
		})
	} else {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Index:   r.RaftLog.committed,
		})
	}
}

func (r *Raft) restore(s *pb.Snapshot) bool {

	if s.Metadata.Index <= r.RaftLog.committed {
		return false
	}
	if r.RaftLog.matchTerm(s.Metadata.Index, s.Metadata.Term) {
		log.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] fast-forwarded commit to snapshot [index: %d, term: %d]",
			r.id, r.RaftLog.committed, r.RaftLog.LastIndex(), r.RaftLog.LastTerm(), s.Metadata.Index, s.Metadata.Term)

		r.RaftLog.commitTo(s.Metadata.Index)
		return false
	}
	r.RaftLog.commitTo(s.Metadata.Index)
	r.RaftLog.pendingSnapshot = s
	for _, peer := range s.Metadata.ConfState.Nodes { // Note@wy update nodes from snapshot
		if _, ok := r.Prs[peer]; !ok {
			r.Prs[peer] = &Progress{}
		}
	}
	return true
}

func (r *Raft) sendSnapshot(to uint64) bool {
	snap, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		if err == ErrSnapshotTemporarilyUnavailable {
			log.Debugf("%x failed to send snapshot to %x because snapshot is temporarily unavailable", r.id, to)
			return false
		}
		panic(err)
	}
	if IsEmptySnap(&snap) {
		// panic("need non-empty snapshot")
		// return false

	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		To:       to,
		From:     r.id,
		Term:     r.Term,
		Commit:   r.RaftLog.committed,
		Snapshot: &snap,
	})
	return true
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; !ok {
		r.Prs[id] = &Progress{
			Match: 0,
			Next:  0,
		}
	}
	r.PendingConfIndex = None
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; ok {
		delete(r.Prs, id)
		if r.State == StateLeader {
			if r.maybeCommit() {
				r.boradcastAppend()
			}
		}
		if r.State == StateLeader && r.leadTransferee == id {
			r.abortLeaderTransferee()
		}
	}
	r.PendingConfIndex = None
}

func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) advance(rd Ready) {
	if newApplied := rd.appiledCursor(); newApplied > r.RaftLog.applied {
		r.RaftLog.applyTo(newApplied)
	}
	if n := len(rd.Entries); n > 0 {
		if r.RaftLog.stabled+1 != rd.Entries[0].Index {
			log.Errorf("Log is not sequential, lastindex %+v ent.index %+v\n", r.RaftLog.stabled, rd.Entries[0].Index)
		}
		r.RaftLog.stableTo(rd.Entries[n-1].Index)
		// r.RaftLog.checkLogSequential()
	}
}
