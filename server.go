package main

import (
	"log"
	"net"
	"net/rpc"
	"sync"
	"math/rand"
	"time"
	"fmt"
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

	quit <-chan bool
}

func NewServer(id int, peers []int, wg *sync.WaitGroup) *Server {
	// raft related server data
	server := new(Server)
	server.state = Follower
	server.currentTerm = 0
	server.votedFor = -1
	server.id = id
	server.peers = peers
	server.wg = wg

	server.commitIndex = 0
	server.lastApplied = 0
	server.electionResetEvent = time.Now()

	// rpc related server data
	server.rpcServer = rpc.NewServer()
	server.rpcServer.RegisterName("gaft", server)
	server.peerClients = make(map[int]*rpc.Client)

	var err error
	addr := fmt.Sprintf(":%d", id)
	if server.listener, err = net.Listen("tcp", addr); err != nil {
		panic(err)
	}


	server.log("setup a listener for server %d", server.id)
	server.start()
	go server.monitorElectionTimer()
	return server
}

func (s *Server) Accept() chan net.Conn { 
	connChan := make(chan net.Conn)
	conn, err := s.listener.Accept()
	if err != nil {
		log.Fatal("listener accept error: ", err)
	}
	connChan <- conn
	return connChan
}

func (s *Server) start() {
	s.wg.Add(1)
	go func() {
		for {
			select {
			case <-s.quit:
				return
			case conn := <-s.Accept():
				s.wg.Add(1)
				go func(conn net.Conn) {
					defer s.wg.Done()
					s.rpcServer.ServeConn(conn)
				}(conn)

			}
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
		s.log("term out of date, transition to follower")
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
			result.ConflictTerm = s.logEntries[args.PrevLogIndex].Term
			var i int
			for i = args.PrevLogIndex - 1; i >= 0; i-- {
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

func (s *Server) monitorElectionTimer() {
	timeoutDuration := randomizedElectionTimeout()
	termStarted := s.currentTerm

	for {
		time.Sleep(10 * time.Millisecond)

		if s.commitIndex > s.lastApplied {
			s.lastApplied++
		}

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
	votesReceived := 1
	
	requestVoteFunc := func(peer int, votesReceived *int) {
		defer s.wg.Done()
		// query peerClients to avoid superfluous allocation of the client
		if _, ok := s.peerClients[peer]; !ok {
			addr := fmt.Sprintf(":%d", peer)
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				panic(err)
			}
			s.peerClients[peer] = client
		}
		client := s.peerClients[peer]
		defer s.peerClients[peer].Close()
		// write out a logic for sending rpc to a specific client
		lastLogIndex, lastLogTerm := s.lastLogIndexAndTerm()
		
		request := RequestVoteArguments{
			Term: s.currentTerm,
			CandidateId: s.id,
			LastLogIndex: lastLogIndex,
			LastLogTerm: lastLogTerm,
		}

		var reply RequestVoteReply
		client.Call("gaft.RequestVote", request, &reply)
		s.currentTerm = reply.Term
		if reply.voteGranted {
			*votesReceived++
		}
	}

	s.wg.Add(len(s.peers))
	for _, peer := range s.peers {
		go requestVoteFunc(peer, &votesReceived)
	}

	go monitorElectionTimer()
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

func (s *Server) log(format string, args ...any) {
	currState := s.stringState()
	extendedFmt := fmt.Sprintf("[%s(%d)]: %s", currState, s.id, format)
	log.Printf(extendedFmt, args...)
}
