package raft

import "log"

// Debugging
const debug_level = -1

func DPrintf(verbose int, format string, a ...interface{}) (n int, err error) {
	if debug_level >= verbose {
		log.Printf(format, a...)
	}
	return
}
