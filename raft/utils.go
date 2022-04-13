package raft

import (
	"log"
	"math/rand"
	"time"
)

func min(x, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}

func assert(condition bool, format string, args ...any) {
	if !condition {
		log.Fatalf(format, args)
	}
}

func randomizedElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(250)+250) * time.Millisecond
}

func randomizedBroadcastPeriod() time.Duration {
	// should be less than election timeout
	return time.Duration(rand.Intn(150)+150) * time.Millisecond
}
