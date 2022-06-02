package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft  server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

//
// A Go object implementing a single Raft peer.
//

// func (rf *Raft) TimerInvalid() {
// 	rf.IsTimerVaild = false
// }

func (rf *Raft) AppendEntries(request *AppendEntriesRequest, response *AppendEntriesResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer DPrintf(0, "{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing AppendEntriesRequest %v and reply AppendEntriesResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), request, response)

	if request.Term < rf.currentTerm {
		response.Term, response.Success = rf.currentTerm, false
		return
	}

	if request.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = request.Term, -1
	}

	rf.ChangeState(StateFollower)
	rf.electionTimer.Reset(RandomizedElectionTimeout())

	if request.PrevLogIndex < rf.getFirstLog().Index { //!used for log compaction condition,do you know
		response.Term, response.Success = 0, false
		DPrintf(0, "{Node %v} receives unexpected AppendEntriesRequest %v from {Node %v} because prevLogIndex %v < firstLogIndex %v", rf.me, request, request.LeaderId, request.PrevLogIndex, rf.getFirstLog().Index)
		return
	}

	if !rf.matchLog(request.PrevLogTerm, request.PrevLogIndex) {
		response.Term, response.Success = rf.currentTerm, false
		lastIndex := rf.getLastLog().Index
		if lastIndex < request.PrevLogIndex {
			response.ConflictTerm, response.ConflictIndex = -1, lastIndex+1
		} else {
			firstIndex := rf.getFirstLog().Index
			response.ConflictTerm = rf.logs[request.PrevLogIndex-firstIndex].Term
			index := request.PrevLogIndex - 1
			for index >= firstIndex && rf.logs[index-firstIndex].Term == response.ConflictTerm {
				index--
			}
			response.ConflictIndex = index
		}
		return
	}

	firstIndex := rf.getFirstLog().Index
	for index, entry := range request.Entries {
		if entry.Index-firstIndex >= len(rf.logs) || rf.logs[entry.Index-firstIndex].Term != entry.Term {
			rf.logs = shrinkEntriesArray(append(rf.logs[:entry.Index-firstIndex], request.Entries[index:]...))
			break
		}
	}

	rf.advanceCommitIndexForFollower(request.LeaderCommit)

	response.Term, response.Success = rf.currentTerm, true
}

func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.RLock()
	if rf.state != StateLeader {
		rf.mu.RUnlock()
		return
	}
	prevLogIndex := rf.nextIndex[peer] - 1
	if prevLogIndex < rf.getFirstLog().Index {
		// only snapshot can catch up
		request := rf.genInstallSnapshotRequest()
		rf.mu.RUnlock()
		response := new(InstallSnapshotResponse)
		if rf.sendInstallSnapshot(peer, request, response) {
			rf.mu.Lock()
			rf.handleInstallSnapshotResponse(peer, request, response)
			rf.mu.Unlock()
		}
	} else {
		// just entries can catch up
		request := rf.genAppendEntriesRequest(prevLogIndex)
		rf.mu.RUnlock()
		response := new(AppendEntriesResponse)
		if rf.sendAppendEntries(peer, request, response) {
			rf.mu.Lock()
			rf.handleAppendEntriesResponse(peer, request, response)
			rf.mu.Unlock()
		}
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	var term int
	var isleader bool
	term = rf.currentTerm
	isleader = rf.state == StateLeader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//

func (rf *Raft) persist() {
	rf.raftPersister.persist(rf, rf.persister)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	rf.raftPersister.loadState(rf, data, 0)
	rf.raftPersister.loadLog(rf, data, statePersistOffset)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	//!how to do it?

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//

func (rf *Raft) ToFollowerCheck(term int) {
	if term > rf.CurrentTerm {
		DPrintf(0, 0, "%d ToFollowerCheck %d", rf.me, term)
		rf.CurrentTerm = term
		rf.VotedFor = -1
		rf.Stage = StateFollower
		rf.VoteCount = 0
		rf.persist()
	}
}

//
// example RequestVote RPC handler.
//

func (rf *Raft) RequestVote(request *RequestVoteRequest, response *RequestVoteResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer DPrintf(0, "{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing requestVoteRequest %v and reply requestVoteResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), request, response)

	if request.Term < rf.currentTerm || (request.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != request.CandidateId) {
		response.Term, response.VoteGranted = rf.currentTerm, false
		return
	}
	if request.Term > rf.currentTerm {
		rf.ChangeState(StateFollower)
		rf.currentTerm, rf.votedFor = request.Term, -1
	}
	if !rf.isLogUpToDate(request.LastLogTerm, request.LastLogIndex) {
		response.Term, response.VoteGranted = rf.currentTerm, false
		return
	}
	rf.votedFor = request.CandidateId
	rf.electionTimer.Reset(RandomizedElectionTimeout())
	response.Term, response.VoteGranted = rf.currentTerm, true
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesRequest, reply *AppendEntriesResponse) bool {
	ok := rf.peers[server].Call("Raft.Appen", args, reply)
	return ok
	//TODO:revise here
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) AppendLogEntry(command interface{}) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.Stage != StateLeader {
		return
	}
	rf.Log = append(rf.Log, LogEntry{Term: rf.CurrentTerm, Command: command, Index: len(rf.Log)})
	DPrintf(0, 0, "rf.me %v after appending cmd %v with len(rf.log) %v", rf.me, command, len(rf.Log))
	rf.NextIndex[rf.me] = len(rf.Log) //TODO:leader's is right
	rf.MatchIndex[rf.me] = len(rf.Log) - 1
	// rf.ResetElectionTimer()
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != StateLeader {
		return -1, -1, false
	}
	newLog := rf.appendNewEntry(command)
	DPrintf("{Node %v} receives a new command[%v] to replicate in term %v", rf.me, newLog, rf.currentTerm)
	rf.BroadcastHeartbeat(false)
	return newLog.Index, newLog.Term, true
}


func (rf *Raft) BroadcastHeartbeat(isHeartBeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartBeat {
			// need sending at once to maintain leadership
			go rf.replicateOneRound(peer)
		} else {
			// just signal replicator goroutine to send entries in batch
			rf.replicatorCond[peer].Signal()
		}
	}
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for rf.killed() == false {
		// if there is no need to replicate entries for this peer, just release CPU and wait other goroutine's signal if service adds new Command
		// if this peer needs replicating entries, this goroutine will call replicateOneRound(peer) multiple times until this peer catches up, and then wait
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}
		// maybe a pipeline mechanism is better to trade-off the memory usage and catch up time
		rf.replicateOneRound(peer)
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == StateLeader && rf.matchIndex[peer] < rf.getLastLog().Index
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func random(min, max int) int {
	return rand.Intn(max-min) + min
}

func (rf *Raft) genRequestVoteRequest() *RequestVoteArgs {
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLog().Index,
		LastLogTerm:  rf.getLastLog().Term,
	}
	return args
}

func (rf *Raft) StartElection() {
	request := rf.genRequestVoteRequest()
	DPrintf(0, "{Node %v} starts election with RequestVoteRequest %v", rf.me, request)
	// use Closure
	grantedVotes := 1
	rf.votedFor = rf.me
	rf.persist()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			response := new(RequestVoteReply)
			if rf.sendRequestVote(peer, request, response) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf(0, "{Node %v} receives RequestVoteResponse %v from {Node %v} after sending RequestVoteRequest %v in term %v", rf.me, response, peer, request, rf.currentTerm)
				if rf.currentTerm == request.Term && rf.state == StateCandidate {
					if response.VoteGranted {
						grantedVotes += 1
						if grantedVotes > len(rf.peers)/2 {
							DPrintf(0, "Node %v receives majority votes in term %v", rf.me, rf.currentTerm)
							rf.ChangeState(StateLeader)
							rf.BroadcastHeartbeat(true)
						}
					} else if response.Term > rf.currentTerm {
						DPrintf(0, "{Node %v} finds a new leader {Node %v} with term %v and steps down in term %v", rf.me, peer, response.Term, rf.currentTerm)
						rf.ChangeState(StateFollower)
						rf.currentTerm, rf.votedFor = response.Term, -1
						rf.persist()
					}
				}
			}
		}(peer)
	}
}
func (rf *Raft) ResetElectionTimer() {
	timeout := time.Millisecond * time.Duration(random(ElectionTimeoutMin, ElectionTimeoutMax))
	rf.DeadLine = time.Now().Add(timeout)
}

func (rf *Raft) IsTimeout() bool {
	return time.Now().After(rf.DeadLine)
}

func (rf *Raft) SendAppendEntriesToServer(server int) {
	sending_entries := []LogEntry{}
	target_nextindex := 0
	rf.mu.Lock()
	target_nextindex = rf.NextIndex[server]
	sending_entries = rf.Log[target_nextindex:]
	// }
	rf.mu.Unlock()
	reply := &AppendEntriesResponse{Reach: false}
	// if server == (rf.me+1)%len(rf.peers) && len(sending_entries) > 0 {
	// 	DPrintf(0,0, "%d send to disconnected server %d,nextindex:%d,len:%d,entries:%v", rf.me, server, target_nextindex, len(sending_entries), sending_entries)
	// }
	if rf.Stage != StateLeader {
		return
	}
	go rf.sendAppendEntries(server, &AppendEntriesRequest{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: target_nextindex - 1,
		PrevLogTerm:  rf.GetLogEnrtyTerm(target_nextindex - 1),
		Entries:      sending_entries,
		LeaderCommit: rf.CommitIndex,
	}, reply)
}

func (rf *Raft) BroadcastAppendEntries() {

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.SendAppendEntriesToServer(i)
	}
}

func (rf *Raft) HeartBeat() {
	for {
		if rf.killed() || rf.Stage != StateLeader {
			return
		}
		rf.BroadcastAppendEntries()
		time.Sleep(time.Millisecond * HeartBeatInterval)
	}
}

func (rf *Raft) DealVoteReply() {
	for len(rf.VoteReplyCh) > 0 {
		<-rf.VoteReplyCh //!first we should empty channel
	}
	go func() {
		for {
			if rf.killed() || rf.Stage != StateCandidate { //!this is the only return condition,do you know
				return
			}
			if len(rf.VoteReplyCh) == 0 {
				continue
			}
			for len(rf.VoteReplyCh) > 0 {
				reply := <-rf.VoteReplyCh
				if reply.Term != rf.CurrentTerm {
					continue
				}
				if reply.VoteGranted {
					rf.mu.Lock()
					rf.VoteCount++
					rf.mu.Unlock()
				}
			}
			time.Sleep(time.Millisecond * 10)
		}
	}()
}

func (rf *Raft) DealAppendEntriesReply() {
	for len(rf.AppendEntriesRpcCh) > 0 {
		<-rf.AppendEntriesRpcCh //!first we should empty channel
	}
	go func() {
		for {
			if rf.killed() || rf.Stage != StateLeader {
				return
			}
			if len(rf.AppendEntriesRpcCh) == 0 {
				continue
			}
			for len(rf.AppendEntriesRpcCh) > 0 {
				rpc_enrty := <-rf.AppendEntriesRpcCh
				args := rpc_enrty.Args
				reply := rpc_enrty.Reply
				serverId := rpc_enrty.ServerId
				if args.Term != reply.Term || !reply.Reach || reply.Term != rf.CurrentTerm {
					continue
				}
				if reply.Success {
					rf.mu.Lock()
					rf.MatchIndex[serverId] = args.PrevLogIndex + len(args.Entries)
					rf.NextIndex[serverId] = rf.MatchIndex[serverId] + 1
					rf.mu.Unlock()
				} else {
					rf.mu.Lock()
					if rf.NextIndex[serverId] > 1 && reply.Reach {
						if reply.ConflictTerm == -1 {
							rf.NextIndex[serverId] = reply.ConflictIndex
						} else if rf.GetLogEnrtyTerm(reply.ConflictIndex) != reply.ConflictTerm {
							rf.NextIndex[serverId] = reply.ConflictIndex
						} else if rf.GetLogEnrtyTerm(reply.ConflictIndex) == reply.ConflictTerm {
							lastIndexWithTerm := reply.ConflictIndex
							for ; lastIndexWithTerm < len(rf.Log); lastIndexWithTerm++ {
								if rf.GetLogEnrtyTerm(lastIndexWithTerm) != reply.ConflictTerm {
									break
								}
							}
							rf.NextIndex[serverId] = lastIndexWithTerm
						}
					}
					rf.mu.Unlock()
				}
				if len(args.Entries) > 0 {
					DPrintf(0, 1, "%d send entries to %d with Term %v begin index %v len %v and entries %v", rf.me, serverId, rf.CurrentTerm, args.PrevLogIndex+1, len(args.Entries), args.Entries)
					DPrintf(0, 1, "%d deal append entries reply from %d with Success %v and len %v ", rf.me, serverId, reply.Success, len(args.Entries))
					DPrintf(0, 1, "MatchIndex %v NextIndex %v", rf.MatchIndex, rf.NextIndex)
				}
			}
			time.Sleep(time.Millisecond * 10)
		}
	}()
}
func (rf *Raft) LeaderUpdateCommitIndex() {
	for {
		if rf.killed() || rf.Stage != StateLeader {
			return
		}
		rf.mu.Lock()
		N := len(rf.Log) - 1
		for N > rf.CommitIndex {
			count := 1
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				if rf.MatchIndex[i] >= N {
					count++
				}
			}
			if count > len(rf.peers)/2 && rf.Log[N].Term == rf.CurrentTerm {
				rf.CommitIndex = N
				break
			}
			N--
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 100)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C: //!this method is very good,do you know
			rf.mu.Lock()
			rf.ChangeState(StateCandidate)
			rf.currentTerm += 1
			rf.StartElection()
			rf.electionTimer.Reset(RandomizedElectionTimeout())
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == StateLeader {
				rf.BroadcastHeartbeat(true)
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			}
			rf.mu.Unlock()
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

// CurrentTerm int //!first initialize to -1
// 	VotedFor    int
// 	Log         []LogEntry
// 	CommitIndex int
// 	LastApplied int
// 	NextIndex  []int
// 	MatchIndex []int

// 	Isleader bool
func (rf *Raft) InitNextAndMatchIndex() {
	for i := 0; i < len(rf.peers); i++ {
		rf.NextIndex[i] = len(rf.Log)
		rf.MatchIndex[i] = 0
	}
}
func makeRaftPersister() *RaftPersister {
	return &RaftPersister{}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		applyCh:        applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
		state:          StateFollower,
		currentTerm:    0,
		votedFor:       -1,
		logs:           make([]LogEntry, 1),
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()),
		electionTimer:  time.NewTimer(RandomizedElectionTimeout()),
	}

	rf.readPersist(persister.ReadRaftState())
	rf.applyCond = sync.NewCond(&rf.mu)
	lastLog := rf.getLastLog()
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{}) //!just think as a lock
			// start replicator goroutine to replicate entries in batch
			go rf.replicator(i)
		}
	}
	// start ticker goroutine to start elections
	go rf.ticker()
	// start applier goroutine to push committed logs into applyCh exactly once
	go rf.applier()

	return rf
}
