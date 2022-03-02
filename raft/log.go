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
	"log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
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
	// need some time to apply committed log to state machine
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.  Including unstabled entry to be handled by leader, which means just recieve from client.
	// for follower, unstable entry means entry recieve from leader and wait for  storge to memorystorage.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// in etcd Unstable include entries & pendingSnapshot
	// once time either entries or pendingSnapshot exit

	// Your Data Here (2A).
	// the start index of entries
	offset uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil {
		log.Panic("storage must not be nil")
	}
	hardState, _, err := storage.InitialState()
	if err != nil {
		panic(err)
	}
	hi, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	lo, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	ents := make([]pb.Entry, 0)
	if lo <= hi {
		ents, err = storage.Entries(lo, hi+1)
		if err != nil {
			panic(err)
		}
	}

	return &RaftLog{
		storage:         storage,
		committed:       hardState.Commit,
		applied:         lo - 1,
		stabled:         hi,
		entries:         ents,
		pendingSnapshot: nil,
		offset:          max(lo, 1),
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if l.offset+uint64(len(l.entries))-1 > l.stabled {
		return l.entries[l.stabled-l.offset+1:]
	}
	return make([]pb.Entry, 0)
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) { // Todo@wy where to limit commit entry whose term is not equal to raft term.
	// Your Code Here (2A).
	off := l.applied + 1
	if !IsEmptySnap(l.pendingSnapshot) {
		off = max(off, l.pendingSnapshot.Metadata.Index+1)
	}
	if l.committed >= off {
		return l.entries[off-l.offset : min(uint64(len(l.entries)+1), l.committed-l.offset+1)]
	}
	return make([]pb.Entry, 0)
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.offset + uint64(len(l.entries)) - 1
	}
	if !IsEmptySnap(l.pendingSnapshot) {
		// if l.pendingSnapshot!=nil {
		return l.pendingSnapshot.Metadata.Index
	}
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	return i
}

func (l *RaftLog) LastTerm() uint64 {
	lastTerm, err := l.Term(l.LastIndex())
	if err != nil {
		fmt.Printf("log.Logger: %v\n", err) // may no term
	}
	// fmt.Printf("log.Logger: %v %v\n", lastTerm, l.LastIndex())
	return lastTerm
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) { //Todo@wy handle -1
	// Your Code Here (2A).
	if len := len(l.entries); len > 0 && i >= l.offset {
		if i < l.offset+uint64(len) {
			return l.entries[i-l.offset].Term, nil
		} else {
			return 0, ErrUnavailable
		}
	}
	term, err := l.storage.Term(i)
	if err == ErrUnavailable && !IsEmptySnap(l.pendingSnapshot) {
		if i == l.pendingSnapshot.GetMetadata().Index {
			return l.pendingSnapshot.Metadata.Term, nil
		} else if i < l.pendingSnapshot.GetMetadata().Index {
			return term, ErrCompacted
		}
	}
	return term, err
}

// vote promise log up-to-date
func (l *RaftLog) isUpToDate(index uint64, logTerm uint64) bool {
	if logTerm > l.LastTerm() || (logTerm == l.LastTerm() && index >= l.LastIndex()) {
		return true
	}
	return false
}

func (l *RaftLog) findConflictByTerm(index uint64, term uint64) uint64 {
	if li := l.LastIndex(); li < index {
		return li
	} else {
		for { // find a last entry i, which i<=index &&  term(i)<=term
			logTerm, err := l.Term(index)
			if logTerm <= term || err != nil {
				break
			}
			index -= 1
		}
		return index
	}

}

// handle append message to check append
func (l *RaftLog) maybeAppend(index, logTerm, committed uint64, ents ...*pb.Entry) (lastnewi uint64, ok bool) {
	// ti, _ := l.Term(index)
	// log.Printf("%v %v\n", ti, logTerm)
	if l.zeroTermOnErrCompacted(l.Term(index)) == logTerm {
		if len(ents) == 0 { // need to commmit
			l.commitTo(min(committed, index)) // don't not use lastIndex, it may not sync with leader
			return index, true
		}
		preStabled := l.stabled
		modifyStabled := false
		// l.applied = min(l.applied, ents[0].Index-1)
		l.stabled = min(l.stabled, ents[0].Index-1)
		// TODO@wy may not true , I don't think TestFollowerAppendEntries2AB make sense.
		// entries may occur situation like pre...| now... | pre... and may need to modify the value of stabled
		for _, ent := range ents {
			lastIndex := l.LastIndex()
			if ent.Index <= lastIndex {
				if l.zeroTermOnErrCompacted(l.Term(ent.Index)) == ent.Term {
					l.stabled = max(l.committed, ent.Index)
					// l.applied = ent.Index
				} else {
					l.entries[ent.Index-l.offset] = *ent
					if (ent.Index+1 <= lastIndex) && l.zeroTermOnErrCompacted(l.Term(ent.Index+1)) <= ent.Term { // add equal for term to pass sageType_MsgAppend2AB
						l.entries = l.entries[:ent.Index-l.offset+1]
					}
					modifyStabled = true
				}
			} else {
				l.entries = append(l.entries, *ent)
				modifyStabled = true
			}
		}
		if !modifyStabled {
			l.stabled = preStabled
		}
		// l.appendEntries(ents...)
		// log.Printf("finsh:%+v\n", l.entries)
		l.commitTo(min(committed, ents[len(ents)-1].Index))
		return ents[len(ents)-1].Index, true
	}
	return 0, false
}

func (l *RaftLog) appendEntries(ents ...*pb.Entry) uint64 {
	// l.entries = append(l.entries, ents...)
	for _, ent := range ents {
		l.entries = append(l.entries, *ent)
	}
	return l.LastIndex()
}

func (l *RaftLog) getEntries(lo uint64, hi uint64) ([]*pb.Entry, error) {
	if lo >= l.offset && hi <= l.offset+uint64(len(l.entries)) {
		ents := make([]*pb.Entry, 0)
		for ; lo < hi; lo += 1 {
			ents = append(ents, &l.entries[lo-l.offset])
		}
		return ents, nil
	}
	return nil, ErrUnavailable
}

func (l *RaftLog) maybeCommit(committedIndex uint64, term uint64) bool {
	if l.committed >= committedIndex {
		return false
	}
	if l.zeroTermOnErrCompacted(l.Term(committedIndex)) != term {
		return false
	}
	l.commitTo(committedIndex)
	return true
}

func (l *RaftLog) commitTo(committed uint64) {
	if l.committed < committed {
		if l.LastIndex() < committed {
			log.Printf("Committed position %+v is out of range of entry.\n", committed)
		}
		l.committed = committed
	}
	// how to update in stabled storage ? Todo@wy
}

func (l *RaftLog) applyTo(applied uint64) {
	if applied == 0 {
		return // filter nil?
	}
	if l.committed < applied || l.applied > applied {
		log.Printf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", applied, l.applied, l.committed)
	}
	l.applied = applied
}

func (l *RaftLog) stableTo(i uint64) { // Todo@wy handle exception
	l.stabled = i
}

func (l *RaftLog) zeroTermOnErrCompacted(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	if err == ErrCompacted {
		return 0
	}
	// log.Printf("unexpected error (%v)", err)
	return 0
}
