package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func Contains(list []int, a int) bool {
	for _, v := range list {
		if v == a {
			return true
		}
	}
	return false
}
