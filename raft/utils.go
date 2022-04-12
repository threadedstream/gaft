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
	return time.Duration(rand.Intn(150)+150) * time.Millisecond
}
