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
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

type AppendEntries struct {
	Term    int
	Command interface{}
}

type AppendEntriesRpc struct {
	ServerId int
	Args     *RequestAppendEntriesArgs
	Reply    *RequestAppendEntriesReply
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu                 sync.Mutex          // Lock to protect shared access to this peer's state
	peers              []*labrpc.ClientEnd // RPC end points of all peers
	persister          *Persister          // Object to hold this peer's persisted state
	me                 int                 // this peer's index into peers[]
	dead               int32               // set by Kill()
	applyCh            chan ApplyMsg
	VoteReplyCh        chan *RequestVoteReply
	AppendEntriesRpcCh chan AppendEntriesRpc
	VoteCount          int
	Stage              int
	//Persistent state
	CurrentTerm int //!first initialize to -1
	VotedFor    int
	Log         []LogEntry
	//Volatile state
	CommitIndex int
	LastApplied int
	//Volatile state on leaders
	NextIndex  []int
	MatchIndex []int

	DeadLine time.Time
	// Your data here (2A, 2B).
}

type RequestAppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type RequestAppendEntriesReply struct {
	// Your data here (2A).\
	Term    int
	Success bool
	Reach   bool
}

// func (rf *Raft) TimerInvalid() {
// 	rf.IsTimerVaild = false
// }

func (rf *Raft) RequestAppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	reply.Reach = true
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.ToFollowerCheck(args.Term)
	reply.Term = rf.CurrentTerm
	if len(args.Entries) > 0 && args.PrevLogIndex < len(rf.Log) {
		DPrintf(0, "Server %d receive entries with rf.CurrentTerm %v len(rf.Log) %v rf.GetLogEnrtyTerm(args.PrevLogIndex) %v and args %v", rf.me, rf.CurrentTerm, len(rf.Log), rf.GetLogEnrtyTerm(args.PrevLogIndex), args)
	}
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		return
	} else {
		if args.PrevLogIndex >= len(rf.Log) {
			reply.Success = false
			return
		} else {
			if rf.GetLogEnrtyTerm(args.PrevLogIndex) != args.PrevLogTerm { //TODO:error for GetLogEnrtyTerm but it's strange
				reply.Success = false
				return
			} else {
				DPrintf(1, "server %d receive append entries from leader %d", rf.me, args.LeaderId)
				rf.ResetElectionTimer()
				reply.Success = true
				startindex := args.PrevLogIndex + 1
				index := startindex
				for ; index-args.PrevLogIndex-1 < len(args.Entries) && index < len(rf.Log); index++ {
					if rf.Log[index].Term != args.Entries[index-args.PrevLogIndex-1].Term {
						rf.Log = rf.Log[:index]
						break
					}
				}
				rf.Log = append(rf.Log, args.Entries[index-args.PrevLogIndex-1:]...)
				if args.LeaderCommit > rf.CommitIndex {
					rf.CommitIndex = min(args.LeaderCommit, len(rf.Log)-1)
				}
			}
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	var term int
	var isleader bool
	term = rf.CurrentTerm
	isleader = rf.Stage == LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) ToFollowerCheck(term int) {
	if term > rf.CurrentTerm {
		DPrintf(0, "%d ToFollowerCheck %d", rf.me, term)
		rf.CurrentTerm = term
		rf.VotedFor = -1
		rf.Stage = FOLLOWER
		rf.VoteCount = 0
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.ToFollowerCheck(args.Term)
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false
	DPrintf(0, "rf.me %v rf.VotedFor %v rf.CurrentTerm %v len(rf.Log) %v args.Term %v args.CandidateId %v args.LastLogTerm %v args.LastLogIndex %v",
		rf.me, rf.VotedFor, rf.CurrentTerm, len(rf.Log), args.Term, args.CandidateId, args.LastLogTerm, args.LastLogIndex)
	if args.Term < rf.CurrentTerm {
		return
	}
	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
		if args.LastLogTerm > rf.GetLogEnrtyTerm(len(rf.Log)-1) || (args.LastLogTerm == rf.GetLogEnrtyTerm(len(rf.Log)-1) && args.LastLogIndex >= len(rf.Log)-1) {
			rf.VotedFor = args.CandidateId
			reply.VoteGranted = true
			DPrintf(0, "%d vote for %d, %t", rf.me, args.CandidateId, reply.VoteGranted)
			rf.ResetElectionTimer()
		}
	}
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
	rf.mu.Lock()
	rf.ToFollowerCheck(reply.Term)
	rf.mu.Unlock()
	rf.VoteReplyCh <- reply
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	rf.mu.Lock()
	rf.ToFollowerCheck(reply.Term)
	rf.mu.Unlock()
	rf.AppendEntriesRpcCh <- AppendEntriesRpc{Reply: reply, Args: args, ServerId: server}
	return ok
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
	if rf.Stage != LEADER {
		return
	}
	rf.Log = append(rf.Log, LogEntry{Term: rf.CurrentTerm, Command: command, Index: len(rf.Log)})
	rf.NextIndex[rf.me] = len(rf.Log) //TODO:leader's is right
	rf.MatchIndex[rf.me] = len(rf.Log) - 1
	rf.ResetElectionTimer()
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	//TODO:for lab 2B
	rf.mu.Lock()
	index := len(rf.Log)
	term := rf.CurrentTerm
	isLeader := (rf.Stage == LEADER)
	rf.mu.Unlock()
	if isLeader {
		go rf.AppendLogEntry(command)
	}
	return index, term, isLeader
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

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

func (rf *Raft) GetLogEnrtyTerm(index int) int {
	if index >= len(rf.Log) {
		panic("index out of range,do you know")
	} else if len(rf.Log) == LogEmptyLen || index <= LogEmptyIndex {
		return rf.CurrentTerm
	}
	return rf.Log[index].Term
}

func (rf *Raft) StartElection() {
	DPrintf(0, "%d start election", rf.me)
	rf.mu.Lock()
	rf.VoteCount = 1
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.ResetElectionTimer()
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := &RequestVoteReply{}
			rf.sendRequestVote(server, &RequestVoteArgs{
				Term:         rf.CurrentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.Log) - 1,
				LastLogTerm:  rf.GetLogEnrtyTerm(len(rf.Log) - 1),
			}, reply)
		}(i)
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
	reply := &RequestAppendEntriesReply{Reach: false}
	// if server == (rf.me+1)%len(rf.peers) && len(sending_entries) > 0 {
	// 	DPrintf(0, "%d send to disconnected server %d,nextindex:%d,len:%d,entries:%v", rf.me, server, target_nextindex, len(sending_entries), sending_entries)
	// }
	go rf.sendAppendEntries(server, &RequestAppendEntriesArgs{
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
		if rf.killed() || rf.Stage != LEADER {
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
			if rf.killed() || rf.Stage != CANDIDATE { //!this is the only return condition,do you know
				return
			}
			if len(rf.VoteReplyCh) == 0 {
				continue
			}
			reply := <-rf.VoteReplyCh
			if reply.VoteGranted {
				rf.mu.Lock()
				rf.VoteCount++
				rf.mu.Unlock()
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
			if rf.killed() || rf.Stage != LEADER {
				return
			}
			if len(rf.AppendEntriesRpcCh) == 0 {
				continue
			}
			rpc_enrty := <-rf.AppendEntriesRpcCh
			args := rpc_enrty.Args
			reply := rpc_enrty.Reply
			serverId := rpc_enrty.ServerId
			if reply.Success {
				rf.mu.Lock()
				rf.MatchIndex[serverId] = args.PrevLogIndex + len(args.Entries)
				rf.NextIndex[serverId] = rf.MatchIndex[serverId] + 1
				rf.mu.Unlock()
			} else {
				rf.mu.Lock()
				if rf.NextIndex[serverId] > 1 && reply.Reach { //!wrong here
					rf.NextIndex[serverId]--
				}
				rf.mu.Unlock()
			}
			if len(args.Entries) > 0 {
				DPrintf(1, "%d send entries to %d with Term %v begin index %v len %v and entries %v", rf.me, serverId, rf.CurrentTerm, args.PrevLogIndex+1, len(args.Entries), args.Entries)
				DPrintf(1, "%d deal append entries reply from %d with Success %v and len %v ", rf.me, serverId, reply.Success, len(args.Entries))
				DPrintf(1, "MatchIndex %v NextIndex %v", rf.MatchIndex, rf.NextIndex)
			}
			time.Sleep(time.Millisecond * 10)
		}
	}()
}
func (rf *Raft) LeaderUpdateCommitIndex() {
	for {
		if rf.killed() || rf.Stage != LEADER {
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
	rf.ResetElectionTimer()

	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		rf.mu.Lock()
		for rf.CommitIndex > rf.LastApplied { //!apply all logs
			rf.LastApplied++
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.Log[rf.LastApplied].Command,
				CommandIndex: rf.LastApplied,
			}
		}
		rf.mu.Unlock()
		switch rf.Stage {
		case FOLLOWER:
			if rf.IsTimeout() {
				rf.Stage = CANDIDATE
				rf.DealVoteReply()
				rf.StartElection()
			}
		case CANDIDATE:
			if rf.VoteCount > len(rf.peers)/2 {
				DPrintf(0, "%d become leader", rf.me)
				rf.mu.Lock()
				rf.Stage = LEADER
				rf.InitNextAndMatchIndex()
				rf.mu.Unlock()
				go rf.DealAppendEntriesReply()
				go rf.HeartBeat()
				go rf.LeaderUpdateCommitIndex()
			}
			if rf.IsTimeout() {
				rf.StartElection()
			}
		case LEADER:

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
		rf.NextIndex[i] = len(rf.Log) //!>=0
		rf.MatchIndex[i] = 0
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	//make(chan Task, int(math.Max(float64(len(files)), float64(nReduce))))
	rf.AppendEntriesRpcCh = make(chan AppendEntriesRpc, len(rf.peers))
	rf.VoteReplyCh = make(chan *RequestVoteReply, len(rf.peers))
	// Your initialization code here (2A, 2B, 2C).
	rf.CurrentTerm = 0
	rf.Stage = FOLLOWER
	rf.VotedFor = -1

	rf.Log = append(rf.Log, LogEntry{Term: 0, Command: nil, Index: 0}) //ADD dummpy log
	//Log
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.NextIndex = make([]int, len(peers)) //TODO:this one is on leader
	rf.MatchIndex = make([]int, len(peers))
	rf.InitNextAndMatchIndex()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
