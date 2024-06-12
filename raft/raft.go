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
	"math"
	"math/rand"
	"sort"

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
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// real election timeout, calculate by 'electionTimeout' and rand.Int()
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
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	var r *Raft = &Raft{
		id:                        c.ID,
		Term:                      0,
		Vote:                      None,
		RaftLog:                   nil,
		Prs:                       make(map[uint64]*Progress),
		State:                     StateFollower,
		votes:                     make(map[uint64]bool),
		msgs:                      make([]pb.Message, 0),
		Lead:                      None,
		heartbeatTimeout:          c.HeartbeatTick,
		electionTimeout:           c.ElectionTick,
		randomizedElectionTimeout: c.ElectionTick + rand.Intn(c.ElectionTick),
		heartbeatElapsed:          0,
		electionElapsed:           0,
		leadTransferee:            0,
		PendingConfIndex:          0,
	}

	r.RaftLog = newLog(c.Storage)
	hardState, confState, _ := c.Storage.InitialState()
	r.Term = hardState.Term
	r.Vote = hardState.Vote

	if c.peers == nil {
		c.peers = confState.Nodes
	}
	for _, peer := range c.peers {
		r.Prs[uint64(peer)] = &Progress{0, 1}
	}
	return r
}

func (r *Raft) sendRequestVote(to uint64) {

	if r.State != StateCandidate {
		panic(fmt.Sprintf("State %v want to send RequestVotes", r.State))
	}

	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastLogIndex)

	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: lastLogTerm,
		Index:   lastLogIndex,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the  given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).

	if r.State != StateLeader {
		panic(fmt.Sprintf("State %v want to send AppendEntries", r.State))
	}

	nextIndex := r.Prs[to].Next
	lastIndex := r.RaftLog.LastIndex()

	if nextIndex > lastIndex+1 {
		panic("incorrect relationship between nextIndex and lastIndex")
	}
	prevLogIndex := nextIndex - 1
	prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)

	firstIndex := r.RaftLog.entries[0].Index
	var entries []*pb.Entry
	for i := nextIndex; i <= r.RaftLog.LastIndex(); i++ {
		entries = append(entries, &r.RaftLog.entries[i-firstIndex])
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return true

}

func (r *Raft) sendAppendResponse(to uint64, reject bool, index uint64) bool {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		Index:   index,
	}
	r.msgs = append(r.msgs, msg)
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).

	// 发送心跳时，将Commit阉割，同时Follower无需计算 “index of last new entry”
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  min(r.RaftLog.committed, r.Prs[to].Match),
	}
	r.msgs = append(r.msgs, msg)
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeatResponse(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
}

// func (r *Raft) send(msg pb.Message) {
// 	r.msgs = append(r.msgs, msg)
// }

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).

	// Add elapsed time
	if r.State == StateFollower || r.State == StateCandidate {
		r.electionElapsed++
	} else {
		r.heartbeatElapsed++
	}

	if r.electionElapsed >= r.randomizedElectionTimeout {
		r._start_election()
	}
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r._send_heartbeat_to_all()
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	// clear the origin state firstly
	r._reset(term)
	r.Lead = lead
	r.State = StateFollower
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).

	// must be Follower/Candidate->Candidate
	if StateLeader == r.State {
		panic("State Error in becomeCandidate")
	}
	r.State = StateCandidate

	r._reset(r.Term + 1)

	r.Vote = r.id
	r.votes[r.id] = true
	if len(r.Prs) == 1 {
		r.becomeLeader()
	}

}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	// if r.State != StateCandidate {
	// 	panic("State can not convert to Leader")
	// }

	r._reset(r.Term)

	r.State = StateLeader
	n := r.RaftLog.LastIndex() + 1
	for peer := range r.Prs {
		r.Prs[peer] = &Progress{0, n}
	}
	// r._send_heartbeat_to_all()

	// no-op entry
	no_op := pb.Entry{
		Term:  r.Term,
		Index: n,
	}
	r.RaftLog.entries = append(r.RaftLog.entries, no_op)

	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	r._send_append_to_all()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).

	log.Debugf("%v: %v Step a message %+v", r.State, r.id, m)

	if m.Term == 0 {
		// 本地消息的term为0
		/*
			1. MessageType_MsgHup: 用于发起选举
			2. MessageType_MsgBeat: 用于发送心跳
		*/
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			if r.State != StateLeader {
				// 由tick函数发起，tick与Step是串行关系，不会并行，无需加锁
				if r.State == StateLeader {
					panic(" receive MsgHup while State is Leader ")
				}
				r._start_election()
			}
		case pb.MessageType_MsgBeat:
			if r.State == StateLeader {
				if r.State != StateLeader {
					panic(" receive MsgBeat while State is not Leader ")
				}
				r._send_heartbeat_to_all()
				r.heartbeatElapsed = 0
			}
		case pb.MessageType_MsgPropose:
			// 提交日志
			if r.State == StateLeader {
				r._appendEntry(m.Entries)
				r._send_append_to_all()
			}
		}
		return nil
	}

	// 这里已经判断了任期大于和小于当前任期的情况
	// 所以不需要在handleXXXX中处理类似的情况
	if m.Term > r.Term {
		lead := None
		// 如果是心跳或者AppendEntries，则可以判定当前任期的leader
		// 如果不是这两种RPC，则无法判定leader
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat {
			lead = m.From
		}
		r.becomeFollower(m.Term, lead)
	}

	var err error = nil
	switch r.State {
	case StateFollower:
		// Follower
		err = r._follower_step(m)
	case StateCandidate:
		// Candidate
		err = r._candidate_step(m)
	case StateLeader:
		// Leader
		err = r._leader_step(m)
	}
	return err
}

// handleRequestVotes handle RequestVotes RPC request
func (r *Raft) handleRequestVotes(m pb.Message) {
	if m.GetTerm() < r.Term {
		r.sendRequestVoteResponse(m.From, true)
		return
	}

	// 此时m.Term == r.Term
	if r.Term != m.Term {
		panic("incorrect term where receive requestvotes request")
	}

	// 是否投票
	/*
		1. 本任期是否投过票，如果已投票是否投给了Candidate
		2. 比较日志在Raft的日志时间线上是否更新
	*/
	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastLogIndex)
	var isVote bool = (r.Vote == 0 || r.Vote == m.From) &&
		(m.LogTerm > lastLogTerm || (m.LogTerm == lastLogTerm && m.Index >= lastLogIndex))
	r.sendRequestVoteResponse(m.From, !isVote)

	if isVote {
		r.Vote = m.From
		// 仅在投票时重置选举计时器
		r.electionElapsed = 0
		r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	}
}

// handleRequestVotes handle RequestVotes RPC response
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if m.GetTerm() < r.Term {
		return
	}

	// 此时m.Term == r.Term
	if r.Term != m.Term {
		panic("incorrect term where receive requestvotes response")
	}

	// 仅candidate需要处理RequestVotes Response
	if r.State != StateCandidate {
		panic("incorrect state where handle requestvotes response")
	}

	r.votes[m.From] = !m.Reject
	agree_count := 0
	for _, v := range r.votes {
		if v {
			agree_count ++
		}
	}
	if agree_count > (len(r.Prs) / 2) {
		// 收到大多数同意
		r.becomeLeader()
	} else if len(r.votes) - agree_count > (len(r.Prs) / 2) {
		// 收到大多数失败
		r.becomeFollower(r.Term, None)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// fmt.Println(m.Term, r.Term)
	if m.Term < r.Term {
		r.sendAppendResponse(m.From, true, m.Index)
		return
	}

	if m.Term > r.Term {
		lead := None
		// 如果是心跳或者AppendEntries，则可以判定当前任期的leader
		// 如果不是这两种RPC，则无法判定leader
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat {
			lead = m.From
		}
		r.becomeFollower(m.Term, lead)
	}

	// 此时m.Term == r.Term
	if r.Term != m.Term {
		panic("incorrect term where receive requestvotes response")
	}

	r.becomeFollower(m.Term, m.From)

	local_term, err := r.RaftLog.Term(m.Index)
	if err != nil || local_term != m.LogTerm {
		r.sendAppendResponse(m.From, true, m.Index)
		return
	}

	// 此时保证在m.Index位置存在m.LogTerm日志
	lastIndex := r.RaftLog.LastIndex()
	snapshotIndex := r.RaftLog.FirstIndex() - 1

	for index, entry := range m.Entries {
		if (lastIndex < entry.Index) || (r.RaftLog.entries[entry.Index-snapshotIndex].Term != entry.Term) {
			// 第一步 删除rf.log[PrevLogIndex + index + 1:]
			// 1. rf.log = rf.log[:args.PrevLogIndex + index + 1]
			// 第二部 加入args.Entries中从index开始的所有entry
			// 2. rf.log = append(rf.log, args.Entries[index:]...)
			// 将以上两步合二为一
			r.RaftLog.entries = r.RaftLog.entries[:entry.Index-snapshotIndex]
			for _, entry := range m.Entries[index:] {
				r.RaftLog.entries = append(r.RaftLog.entries, *entry)
			}
			r.RaftLog.stabled = min(r.RaftLog.stabled, entry.Index-1)
			break
		}
	}

	// 这里应该保证即使收到了过期的AppendEntries RPC也不会对commited的更新有影响
	// 即：保证RPC调用的幂等性
	r._updateCommitIndex(m.Commit, m.Index+uint64(len(m.Entries)))

	r.sendAppendResponse(m.From, false, m.Index+uint64(len(m.Entries)))
}

// handleAppendEntries handle AppendEntries RPC response
func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	// Your Code Here (2A).
	if m.GetTerm() < r.Term {
		return
	}

	// 此时m.Term == r.Term
	if r.Term != m.Term {
		panic("incorrect term where receive requestvotes response")
	}

	// 仅leader需要处理AppendEntries Response
	if r.State != StateLeader {
		panic("incorrect state where handle appendentries response")
	}

	// Leader 要对乱序的AppendEntries Response具有鲁棒性(幂等性)
	if m.Reject {
		if m.Index == r.Prs[m.From].Next-1 {
			r.Prs[m.From].Next = m.Index
			r.sendAppend(m.From)
		}
	} else {
		// 日志同步成功,但是收到的Response可能是乱序的，
		// 所以需要发送Response的Follower在回复中记录成功同步的日志中最大的Index
		newMatchIndex := m.Index
		newNextIndex := m.Index + 1

		if newMatchIndex > r.Prs[m.From].Match {
			r.Prs[m.From] = &Progress{newMatchIndex, newNextIndex}
		}

		isCommit := r._maybeCommit()
		if isCommit {
			r._send_append_to_all()
		}

		if !isCommit && r.RaftLog.LastIndex() >= newNextIndex {
			r.sendAppend(m.From)
		}
	}

}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.GetTerm() < r.Term {
		r.sendHeartbeatResponse(m.From)
		return
	}

	if m.Term > r.Term {
		lead := None
		// 如果是心跳或者AppendEntries，则可以判定当前任期的leader
		// 如果不是这两种RPC，则无法判定leader
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat {
			lead = m.From
		}
		r.becomeFollower(m.Term, lead)
	}

	// 此时m.Term == r.Term
	if r.Term != m.Term {
		panic("incorrect term where receive requestvotes response")
	}

	r.becomeFollower(m.Term, m.From)
	r._updateCommitIndex(m.Commit, math.MaxInt64)
	r.sendHeartbeatResponse(m.From)
}

// handleHeartbeat handle Heartbeat RPC response
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		return
	}

	// 此时m.Term == r.Term
	if r.Term != m.Term {
		panic("incorrect term where receive requestvotes response")
	}

	/*
		ETCD中将Raft实现为一个状态机，消息处理是单线性的，没有其他线程
		这样就有一个问题：在Raft算法中要求Leader对于落后的Follower不能放弃
		要一直发送AppendEntries RPC，但是单线程做不到这种事情。

		这就是说：如果Follower宕机后重启，如果没有新日志提交，Leader无法向Follower传输日志了
		测试用例：TestCommitWithHeartbeat2AB
		要求：    HeartBeat能够触发日志同步过程
	*/
	// 实现参考ETCD

	// 该节点的match节点小于当前最大日志索引，可能已经过期了，尝试添加日志
	if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
	return
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

/*
	the following section are the new function I added.
	the function names start with '_'
*/

func (r *Raft) _start_election() {
	r.becomeCandidate()
	r._send_requestvotes_to_all()
}

func (r *Raft) _send_requestvotes_to_all() {
	for peer := range r.Prs {
		if peer != r.id {
			r.sendRequestVote(peer)
		}
	}
}

func (r *Raft) _send_append_to_all() {
	for peer := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
}

func (r *Raft) _send_heartbeat_to_all() {
	for peer := range r.Prs {
		if peer != r.id {
			r.sendHeartbeat(peer)
		}
	}
}

func (r *Raft) _follower_step(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVotes(m)
	case pb.MessageType_MsgRequestVoteResponse:
		// nothing to do
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		// nothing to do
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		// nothing to do
	default:
		return errors.New("invalid request")
	}
	return nil
}

func (r *Raft) _candidate_step(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVotes(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		// candidate收到appendentries response，response的任期一定小于当前任期
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		// candidate收到heartbeat response，response的任期一定小于当前任期

	default:
		return errors.New("invalid request")
	}
	return nil
}

func (r *Raft) _leader_step(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVotes(m)
	case pb.MessageType_MsgRequestVoteResponse:
		// do nothing
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	default:
		return errors.New("invalid request")
	}
	return nil
}

// 重置状态信息、选举计时器
func (r *Raft) _reset(term uint64) {
	if term < r.Term {
		panic(" error term when execute 'reset' ")
	}

	if term != r.Term {
		r.Vote = None
		r.Term = term
	}
	r.Lead = None

	r.electionElapsed = 0
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.heartbeatElapsed = 0

	r.votes = map[uint64]bool{}
}

func (r *Raft) _appendEntry(es []*pb.Entry) {
	lastIndex := r.RaftLog.LastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = lastIndex + 1 + uint64(i)
		r.RaftLog.entries = append(r.RaftLog.entries, *es[i])
	}

	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1

	// 应对集群中仅有一个节点的情况
	if len(r.Prs) == 1 {
		r._maybeCommit()
	}

}

/*
问题：
在心跳HeartBeat中，Entries字段为空列表，prevLogTerm和prevLogIndex为空，
是无法计算“index of last new entry”的；且单单依靠LeaderCommit是无法更新
Follower的CommitIndex的(因为：收到心跳后，Follower无法判断自己日志和Leader
日志的一致性，从而无法仅通过LeaderCommit更新自己的CommitIndex)。

解决：
有关CommitIndex的更新过程，参考了ETCD的处理，与原生Raft论文在心跳的处理上存在不一致
ETCD将心跳HeartBeat和AppendEntries分开：
●   在发送AppendEntries时，LeaderCommit字段为真实的LeaderCommit，
	Follower按照原Raft算法更新本地CommitIndex。
●   在发送HeartBeat时，LeaderCommit字段被设置为min(LearderCommit, matchIndex[FollowerID])。
	由于matchIndex的参与，保证了Follower和Leader在matchIndex[FollowerID]之前的日志是完全一致的，
	所以此时Follower直接将CommitIndex=LeaderCommit就好！！

具体实现：
●	当lastNewLogIndex为无穷大时，表明是handleHeartBeat调用，
	Leader发送的leaderCommit是min(leaderCommit, matchIndex[FollowerID]),
	此时直接用leaderCommit更新就好

	当lastNewLogIndex为其他时，表明是handleApend调用，
●	Leader发送的leaderCommit为真实的leaderCommit，按Raft论文逻辑进行
*/
func (r *Raft) _updateCommitIndex(leaderCommit, lastNewLogIndex uint64) {
	if leaderCommit > r.RaftLog.committed {
		// 如果收到过期的AppendEntries RPC可能导致committed回退
		r.RaftLog.committed = max(r.RaftLog.committed, min(leaderCommit, lastNewLogIndex))
	}
}

func (r *Raft) _maybeCommit() bool {
	if r.State != StateLeader {
		panic("only Leader can decide to commit")
	}

	// 以下代码参考ETCD
	// 我自己的实现使用二分, L = r.RaftLog.committed, R = r.RaftLog.LastIndex()
	// 复杂度都是NlogN
	mis := make(uint64Slice, 0, len(r.Prs))
	for id := range r.Prs {
		mis = append(mis, r.Prs[id].Match)
	}
	// 逆序排列
	sort.Sort(sort.Reverse(mis))
	// 中位数
	mci := mis[len(r.Prs)/2]
	// raft日志尝试commit
	return r.RaftLog.maybeCommit(mci, r.Term)
}
