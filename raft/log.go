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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries

// 我淘他猴子，别被这张图蒙骗了, stabled和commited没有大小关系
// stabled仅仅指示哪些日志被持久化了，
// 在Raft状态机中，如果有持久化的日志被删除了，那么stabled会回退！！
// 所以当Ready收到需要持久化的日志时，删除Index重合的，不重合的直接添加

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
	dummyIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil {
		log.Panicf("storage must not be nil")
	}
	log := &RaftLog{
		storage: storage,
	}

	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()

	entries, _ := storage.Entries(firstIndex, lastIndex+1)
	entries = append(make([]pb.Entry, 1), entries...)
	entries[0].Index = firstIndex - 1
	log.entries = entries

	hardState, _, _ := storage.InitialState()
	log.committed = hardState.Commit
	log.applied = firstIndex - 1
	log.stabled = lastIndex
	log.pendingSnapshot = nil
	return log
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries[1:]
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 1 {
		dummyIndex := l.FirstIndex() - 1
		if l.stabled <= dummyIndex {
			return l.entries[1:]
		}
		if l.stabled-dummyIndex >= uint64(len(l.entries)-1) {
			return make([]pb.Entry, 0)
		}
		return l.entries[l.stabled-dummyIndex+1:]
	}
	return make([]pb.Entry, 0)
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	dummyIndex := l.FirstIndex() - 1
	appliedIndex := l.applied
	commitedIndex := l.committed
	if len(l.entries) > 1 {
		if appliedIndex >= dummyIndex && appliedIndex < commitedIndex && commitedIndex <= l.LastIndex() {
			return l.entries[appliedIndex-dummyIndex + 1 : commitedIndex-dummyIndex + 1]
		}
	}
	return make([]pb.Entry, 0)
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return l.entries[0].Index + uint64(len(l.entries)) - 1
}

func (l *RaftLog) FirstIndex() uint64 {
	return l.entries[0].Index + 1
}

func (l *RaftLog) Empty() bool {
	return l.LastIndex() >= l.FirstIndex()
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	offset := l.entries[0].Index
	if i < offset {
		return 0, ErrCompacted
	}
	if int(i-offset) >= len(l.entries) {
		return 0, ErrUnavailable
	}
	return l.entries[i-offset].Term, nil
}

func (l *RaftLog) maybeCommit(maxIndex, term uint64) bool {
	maxTerm, _ := l.Term(maxIndex)
	if maxIndex > l.committed && maxTerm == term {
		l.committed = maxIndex
		return true
	}
	return false
}
