package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"
)

func (s *Server) log(format string, args ...any) {
	currState := s.stringState()
	extendedFmt := fmt.Sprintf("[%s(%d)]: %s", currState, s.id, format)
	log.Printf(extendedFmt, args)
}

func (s *Server) monitorElectionTimer() {
	timeoutDuration := randomizedElectionTimeout()
	termStarted := s.currentTerm

	for {
		time.Sleep(10 * time.Millisecond)

		if s.state != Follower && s.state != Candidate {
			s.log("neither follower nor candidate")
			return
		}

		if termStarted != s.currentTerm {
			s.log("termStarted does not match a current term")
			return
		}

		if elapsed := time.Since(s.electionResetEvent); elapsed >= timeoutDuration {
			s.log("timed out, about to start an election")
			s.startElection()
			return
		}
	}
}

func randomizedElectionTimeout() time.Duration {
	if rand.Intn(5) == 2 {
		return time.Duration(150) * time.Millisecond
	}
	return time.Duration(300) * time.Millisecond
}

func (s *Server) startElection() {
	s.state = Candidate
	s.currentTerm++
	s.votedFor = s.id
	s.electionResetEvent = time.Now()

	requestVoteFunc := func(peer int) {

	}
	for _, peer := range s.peers {

	}
}

func (s *Server) lastLogIndexAndTerm() (int, int) {
	if len(s.logEntries) > 0 {
		lastIndex := len(s.logEntries) - 1
		return lastIndex, s.logEntries[lastIndex].Term
	}
	return -1, -1
}

func (s *Server) transitionToFollower(term int) {
	s.log("become a follower with term %d", term)
	s.state = Follower
	s.votedFor = -1
	s.currentTerm = term
	s.electionResetEvent = time.Now()

	go s.monitorElectionTimer()
}

func main() {

}
