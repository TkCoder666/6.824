package raft

import (
	"time"
)

func RandomizedElectionTimeout() time.Duration {
	return time.Millisecond * time.Duration(random(ElectionTimeoutMin, ElectionTimeoutMax))
}

func StableHeartbeatTimeout() time.Duration {
	return time.Microsecond * HeartBeatInterval
}

func shrinkEntriesArray(logs []LogEntry) []LogEntry {
	return append([]LogEntry{}, logs...)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
