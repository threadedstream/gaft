package raft

import (
	"math/rand"
	"time"
)

func randomizedElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(250)+250) * time.Millisecond
}

func randomizedBroadcastPeriod() time.Duration {
	// should be less than election timeout
	return time.Duration(rand.Intn(150)+150) * time.Millisecond
}
