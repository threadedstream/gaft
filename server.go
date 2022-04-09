package main

import (
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// arguments and replies for rpc calls
type (
	AppendEntriesArguments struct {
		Term         int
		LeaderId     int
		PrevLogIndex int
		PrevLogTerm  int
		LeaderCommit int
		Entries      []LogEntry
	}

	AppendEntriesReply struct {
		Term          int
		ConflictTerm  int
		ConflictIndex int
		Success       bool
	}

	RequestVoteArguments struct {
		Term         int
		CandidateId  int
		LastLogIndex int
		LastLogTerm  int
	}

	RequestVoteReply struct {
		Term        int
		VoteGranted bool
	}
)

type state int

const (
	Leader state = iota
	Follower
	Candidate
	Dead
)

type LogEntry struct {
	Term    int
	Command any
}

// Server represents a single unit in cluster of servers
type Server struct {
	state
	// server's id
	id int
	// other servers in a cluster
	peers []int
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

	// election timer
	electionResetEvent time.Time

	// rpc related stuff
	rpcServer   *rpc.Server
	listener    net.Listener
	peerClients map[int]*rpc.Client

	// concurrency related
	wg *sync.WaitGroup
	mu sync.Mutex
}

func NewServer(id int, peers []int) *Server {
	// raft related server data
	server := new(Server)
	server.state = Follower
	server.currentTerm = 0
	server.votedFor = -1
	server.id = id
	server.peers = peers

	server.commitIndex = 0
	server.lastApplied = 0
	server.electionResetEvent = time.Now()

	// rpc related server data
	server.rpcServer = rpc.NewServer()
	server.rpcServer.RegisterName("gaft", server)

	var err error
	if server.listener, err = net.Listen("tcp", ":0"); err != nil {
		// TODO(threadedstream): should we panic right away, wouldn't logging suffice?
		panic(err)
	}

	server.log("setup a listener for server %d", server.id)
	return server
}

func (s *Server) Start() {
	go func() {
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				log.Fatal("listener accept error: ", err)
			}
			s.wg.Add(1)
			go func(conn net.Conn) {
				defer s.wg.Done()
				s.rpcServer.ServeConn(conn)
			}(conn)
		}
	}()
}

func (s *Server) Shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = Dead
	s.listener.Close()
	s.wg.Wait()
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

		// resetting election timer, as we've got a heartbeat from a leader
		s.electionResetEvent = time.Now()

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
			s.commitIndex = min(args.LeaderCommit, len(s.logEntries)-1)
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
func (s *Server) RequestVote(args RequestVoteArguments, result *RequestVoteReply) error {
	result.VoteGranted = false

	if args.Term > s.currentTerm {
		s.log("transition to follower")
		s.transitionToFollower(args.Term)
	}

	lastLogIndex, lastLogTerm := s.lastLogIndexAndTerm()

	// kind of weird, but I enjoy writing it that way
	termsEqual := args.Term == s.currentTerm
	votedForIsNullOrCandidateId := s.votedFor == -1 || s.votedFor == args.CandidateId
	candLastLogTermGtServers := args.LastLogTerm > lastLogTerm
	lastLogTermsEqualAndCandLastIndexGteServers := args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex

	if (candLastLogTermGtServers || lastLogTermsEqualAndCandLastIndexGteServers) &&
		(termsEqual && votedForIsNullOrCandidateId) {

		// at this point, vote is allowed to be granted to the candidate
		result.VoteGranted = true
		s.votedFor = args.CandidateId
		s.electionResetEvent = time.Now()
	}

	result.Term = s.currentTerm

	return nil
}

func (s *Server) stringState() string {
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
