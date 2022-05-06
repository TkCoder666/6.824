package raft

import "log"

// Debugging
const debug_level = -10

func DPrintf(verbose int, format string, a ...interface{}) (n int, err error) {
	if debug_level >= verbose {
		log.Printf(format, a...)
	}
	return
}

//Const values
const (
	HeartBeatInterval  = 200
	ElectionTimeoutMin = 250
	ElectionTimeoutMax = 450
	LogEmptyLen        = 1
	LogEmptyIndex      = 0
)
