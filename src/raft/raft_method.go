package raft

func (rf *Raft) getFirstLog() *LogEntry {
	//TODO:you should know what is first log
	return &rf.logs[1]
}

func (rf *Raft) getLastLog() *LogEntry {
	return &rf.logs[len(rf.logs)-1]
}

func (rf *Raft) ChangeState(state int) {
	rf.state = state
}

func (rf *Raft) isLogUpToDate(request_term int, request_index int) bool {
	up_to_date_flag := false
	LastLogTerm := rf.getLastLog().Term
	if request_term > LastLogTerm {
		up_to_date_flag = true
	} else if request_term == LastLogTerm {
		if request_index >= rf.getLastLog().Index {
			up_to_date_flag = true
		}
	}
	return up_to_date_flag
}
func (rf *Raft) matchLog(request_term int, request_index int) bool {
	match_flag := true
	firstIndex := rf.getFirstLog().Index
	if request_index > rf.getLastLog().Index {
		match_flag = false
	} else {
		if rf.logs[request_index-firstIndex].Term != request_term {
			match_flag = false
		}
	}
	return match_flag
}

func (rf *Raft) advanceCommitIndexForFollower(leader_commit int) {
	if leader_commit > rf.commitIndex {
		rf.commitIndex = min(leader_commit, rf.getLastLog().Index)
	}
}
