package main

import (
	"math/rand"
	"time"
	"fmt"
)

type state int

const (
	Leader state = iota
	Follower
	Candidate
)

type LogEntry struct {
	Term    int
	Command any
}

type Server struct {
	state
	// server's id 
	id int
	// latest term server has seen (initialized to 0 on first boot, increases monotonically)
	currentTerm int
	// latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor int
	// log entries; each entry contains command
	// for state machine, and term when entry
	// was received by leader (first index is 1)
	logEntries []LogEntry
	// volatile state on servers
	commitIndex int
	lastApplied int
	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	electionTimeout time.Duration
	electionResetEvent time.Time
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictTerm int
	ConflictIndex int
}

type AppendEntriesArguments struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

func NewServer() *Server {
	server := new(Server)
	server.state = Follower
	server.currentTerm = 0
	server.votedFor = -1

	server.commitIndex = 0
	server.lastApplied = 0

	return server
}

func (s *Server) stringState() string{
	switch s.state {
	case Leader:
		return "leader"
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	default:
		return "undetermined"
	}
}

func (s *Server) log(format string, args ... any) {
	currState := s.stringState()
	extendedFmt := fmt.Sprintf("[%s(%d)]: %s", currState, s.id, format)
	fmt.Printf(extendedFmt, args)
}

func min(x, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
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

}

// AppendEntries RPC
//Arguments:
// term: leader’s term
// leaderId: 	so follower can redirect clients
// prevLogIndex:  index of log entry immediately preceding new ones
// prevLogTerm:   term of prevLogIndex entry
// entries[]: 	 log entries to store (empty for heartbeat; may send more than one for efficiency)
// leaderCommit: leader’s commitIndex
//Results:
// term: currentTerm, for leader to update itself
// success: true if follower contained entry matching prevLogIndex and prevLogTerm
//Receiver implementation:
// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index
// but different terms), delete the existing entry and all that
// follow it (§5.3)
// 5. If leaderCommit > commitIndex, set commitIndex =
// 4. Append any new entries not already in the log
// min(leaderCommit, index of last new entry)

func (s *Server) AppendEntries(args AppendEntriesArguments, result *AppendEntriesReply) error {
	result.Success = false

	if s.currentTerm < args.Term {
		s.transitionToFollower(args.Term)
	}

	if s.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
		return nil
	}

	if args.PrevLogIndex == -1 ||
		args.PrevLogIndex < len(s.logEntries) {

		result.Success = true

		logInsertIdx := args.PrevLogIndex - 1
		newEntriesIdx := 0

		for {
			if logInsertIdx > len(s.logEntries) || newEntriesIdx > len(args.Entries) {
				break
			}

			if s.logEntries[logInsertIdx].Term != args.Entries[newEntriesIdx].Term {
				break
			}
			logInsertIdx++
			newEntriesIdx++
		}

		if newEntriesIdx < len(args.Entries) {
			s.logEntries = append(s.logEntries[:logInsertIdx], args.Entries[newEntriesIdx:]...)
		}
		
		if args.LeaderCommit > s.commitIndex {
			s.commitIndex = min(args.LeaderCommit, len(s.logEntries) - 1)
		}
	} else {
		if args.PrevLogIndex >= len(s.logEntries) {
			result.ConflictIndex = len(s.logEntries)
			result.ConflictTerm = -1
		} else {
			result.ConflictTerm = s.logEntries[s.PrevLogIndex].Term
			var i int
			for i = s.PrevLogIndex - 1; i >= 0; i-- {
				if s.logEntries[i].Term != result.ConflictTerm {
					break
				}
			}
			result.ConflictIndex = i + 1
		}
	}

	return nil
}

func (s *Server) transitionToFollower(term int) {
	s.log("become a follower with term %d", term)
	s.state = Follower
	s.votedFor = -1
	s.currentTerm = term
	s.electionResetEvent = time.Now()

	go monitorElectionTimer()
}

// RequestVote RPC
//Arguments:
// term: candidate’s term
// candidateId: candidate requesting vote
// lastLogIndex: index of candidate’s last log entry (§5.4)
// lastLogTerm:  term of candidate’s last log entry (§5.4)
//Results:
// term: currentTerm, for candidate to update itself
// voteGranted: true means candidate received vote
//Receiver implementation:
// 1. Reply false if term < currentTerm (§5.1)
// 2. If votedFor is null or candidateId, and candidate’s log is at
// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)

func main() {

}
