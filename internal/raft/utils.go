package raft

import (
	"math/rand"
	"time"
)

func randomizedElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(10)+20) * time.Second
}

func randomizedBroadcastPeriod() time.Duration {
	// should be less than election timeout
	return time.Duration(rand.Intn(5)+10) * time.Second
}
