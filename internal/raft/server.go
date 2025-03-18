package raft

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

const (
	rpcBase                = "Gaft"
	appendEntriesEndpoint  = "Gaft.AppendEntries"
	requestVoteEndpoint    = "Gaft.RequestVote"
	identifyLeaderEndpoint = "Gaft.IdentifyLeader"
	issueCommandEndpoint   = "Gaft.IssueCommand"
)

const (
	rpcCallTimeout = time.Second * 15
	// maximum number of attempts to reach out a client
	maxCallAttempts = 3
)

// initialized in NewRaftServer()
var quorum = -1

type state int

const (
	Leader state = iota
	Follower
	Candidate
	Dead
)

type LogEntry struct {
	Term        int
	ClientId    int
	SequenceNum int
	Command     any
}

type Printable interface {
	String() string
}

// Server represents a single unit in cluster of raft servers
type Server struct {
	Printable
	state
	// Server's id
	id int
	// leader's id
	leaderId int
	// other RaftServers in a cluster
	peers []int
	// latest term Server has seen (initialized to 0 on first boot, increases monotonically)
	currentTerm atomic.Int64
	// latest term Server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor int
	// log entries; each entry contains command
	// for state machine, and term when entry
	// was received by leader (first index is 1)
	logEntries []LogEntry
	// volatile state on RaftServers
	commitIndex atomic.Int64
	lastApplied atomic.Int64
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

	// finite-state machine executing commands
	fsm *FSM
	// map from clientId to the result of previously executed command
	fsmResults map[int]any
}

func NewServer(id int, peers []int, wg *sync.WaitGroup) *Server {
	// raft related Server data
	server := new(Server)
	server.state = Follower
	server.currentTerm.Store(0)
	server.votedFor = -1
	server.id = id
	server.peers = peers
	server.wg = wg

	server.electionResetEvent = time.Now()

	server.nextIndex = make([]int, len(server.peers))
	server.matchIndex = make([]int, len(server.peers))

	server.fsm = NewFSM(3 * time.Minute)
	server.fsmResults = make(map[int]any)
	// rpc related Server data
	server.rpcServer = rpc.NewServer()
	if err := server.rpcServer.RegisterName(rpcBase, server); err != nil {
		log.Fatal(err)
	}

	server.peerClients = make(map[int]*rpc.Client)

	// used to determine what number of servers is considered
	// as a majority
	quorum = len(peers)/2 + 1

	// for randomized election timeouts
	rand.Seed(time.Now().UnixNano())

	var err error
	addr := fmt.Sprintf(":%d", id)
	if server.listener, err = net.Listen("tcp", addr); err != nil {
		panic(err)
	}

	server.log("setup a listener for Server %d", server.id)
	server.start()
	go server.monitorElectionTimer()
	return server
}

func (s *Server) Accept() chan net.Conn {
	connChan := make(chan net.Conn, 1)
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

func (s *Server) shutdownClientConnections() {
	for _, client := range s.peerClients {
		client.Close()
	}
}

func (s *Server) Shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = Dead
	s.shutdownClientConnections()
	s.listener.Close()
	s.wg.Wait()
}

// AppendEntries RPC
// Arguments:
// term: leader’s term
// leaderId: 	so follower can redirect clients
// prevLogIndex:  index of log entry immediately preceding new ones
// prevLogTerm:   term of prevLogIndex entry
// entries[]: 	 log entries to store (empty for heartbeat; may send more than one for efficiency)
// leaderCommit: leader’s commitIndex
// Results:
// term: currentTerm, for leader to update itself
// success: true if follower contained entry matching prevLogIndex and prevLogTerm
// Receiver implementation:
// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index
// but different terms), delete the existing entry and all that
// follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
func (s *Server) AppendEntries(args AppendEntriesArguments, result *AppendEntriesReply) error {
	result.Success = false

	if args.Term < int(s.currentTerm.Load()) {
		return errors.New("stale term")
	}

	if int(s.currentTerm.Load()) <= args.Term && s.state != Follower {
		s.transitionToFollower(args.Term)
	}

	// resetting election timer, as we've got a heartbeat from a leader
	s.electionResetEvent = time.Now()

	if len(s.logEntries) > 0 && s.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
		return errors.New("different terms at prevLogIndex")
	}

	if args.PrevLogIndex == -1 ||
		args.PrevLogIndex < len(s.logEntries) {

		result.Success = true

		logInsertIdx := args.PrevLogIndex + 1
		newEntriesIdx := 0

		for {
			if logInsertIdx >= len(s.logEntries) || newEntriesIdx >= len(args.Entries) {
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

		if args.LeaderCommit > int(s.commitIndex.Load()) {
			s.commitIndex.Store(int64(min(args.LeaderCommit, len(s.logEntries)-1)))
		}
		s.leaderId = args.LeaderId
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
// Arguments:
// term: candidate’s term
// candidateId: candidate requesting vote
// lastLogIndex: index of candidate’s last log entry (§5.4)
// lastLogTerm:  term of candidate’s last log entry (§5.4)
// Results:
// term: currentTerm, for candidate to update itself
// voteGranted: true means candidate received vote
// Receiver implementation:
// 1. Reply false if term < currentTerm (§5.1)
// 2. If votedFor is null or candidateId, and candidate’s log is at
// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
func (s *Server) RequestVote(args RequestVoteArguments, result *RequestVoteReply) error {
	result.VoteGranted = false

	if args.Term > int(s.currentTerm.Load()) && s.state != Follower {
		s.transitionToFollower(args.Term)
	}

	lastLogIndex, lastLogTerm := s.lastLogIndexAndTerm()

	// kind of weird, but I enjoy writing it that way
	termsEqual := args.Term == int(s.currentTerm.Load())
	votedForIsNullOrCandidateId := s.votedFor == -1 || s.votedFor == args.CandidateId
	candLastLogTermGtRaftServers := args.LastLogTerm > lastLogTerm
	lastLogTermsEqualAndCandLastIndexGteRaftServers := args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex

	if (candLastLogTermGtRaftServers || lastLogTermsEqualAndCandLastIndexGteRaftServers) &&
		(termsEqual && votedForIsNullOrCandidateId) {

		// at this point, vote is allowed to be granted to the candidate
		result.VoteGranted = true
		s.votedFor = args.CandidateId
		// reset election timer
	}

	result.Term = int(s.currentTerm.Load())

	return nil
}

func (s *Server) IdentifyLeader(_ struct{}, reply *IdentifyLeaderReply) error {
	if s.state != Leader {
		reply.Id = -1
		return nil
	}
	reply.Id = s.id
	return nil
}

func (s *Server) String() string {
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

	for {
		time.Sleep(10 * time.Millisecond)

		if s.commitIndex.Load() > s.lastApplied.Load() {
			s.lastApplied.Add(1)
		}

		if s.state != Follower && s.state != Candidate {
			s.log("neither follower nor candidate")
			return
		}

		if elapsed := time.Since(s.electionResetEvent); elapsed >= timeoutDuration {
			s.log("timed out, about to start an election")
			s.startElection()
			return
		}
	}
}

func (s *Server) commit() {
	for int(s.commitIndex.Load()) < len(s.logEntries) {
		s.commitIndex.Add(1)
	}
}

func (s *Server) replicate() int {
	votedPositively := 0
	wg := sync.WaitGroup{}
	for _, peer := range s.peers {
		go func(peer int) {
			reply := s.sendHeartbeat(peer, true, &wg)
			if reply.Success {
				votedPositively++
			}
		}(peer)
	}

	wg.Wait()

	return votedPositively
}

func (s *Server) startElection() {
	s.state = Candidate
	s.currentTerm.Add(1)
	s.votedFor = s.id
	s.electionResetEvent = time.Now()
	votesReceived := 1

	requestVoteFunc := func(peer int, votesReceived *int, wg *sync.WaitGroup) {
		defer wg.Done()
		// query peerClients to avoid superfluous allocation of the client
		if _, ok := s.peerClients[peer]; !ok {
			s.mu.Lock()
			addr := fmt.Sprintf(":%d", peer)
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				panic(err)
			}
			s.peerClients[peer] = client
			s.mu.Unlock()
		}
		client := s.peerClients[peer]
		// write out a logic for sending rpc to a specific client
		lastLogIndex, lastLogTerm := s.lastLogIndexAndTerm()

		args := RequestVoteArguments{
			Term:         int(s.currentTerm.Load()),
			CandidateId:  s.id,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}

		var reply RequestVoteReply
	reachOutLoop:
		for {
			select {
			case err := <-channedRpcCall(requestVoteEndpoint, client, args, &reply):
				if err != nil {
					s.log("%s ended up with error %v", requestVoteEndpoint, err)
					return
				}
				break reachOutLoop
			case <-time.After(rpcCallTimeout):
				// redo the request
				continue reachOutLoop
			}
		}

		s.currentTerm.Store(int64(reply.Term))
		if reply.VoteGranted {
			*votesReceived++
		}
	}

	wg := sync.WaitGroup{}

	wg.Add(len(s.peers))
	for _, peer := range s.peers {
		go requestVoteFunc(peer, &votesReceived, &wg)
	}
	wg.Wait()

	if votesReceived >= quorum {
		s.log("received majority of votes, transition to a leader")
		// become a leader
		s.transitionToLeader()
		return
	}

	go s.monitorElectionTimer()
}

func (s *Server) lastLogIndexAndTerm() (int, int) {
	if len(s.logEntries) > 0 {
		lastIndex := len(s.logEntries) - 1
		return lastIndex, s.logEntries[lastIndex].Term
	}
	return -1, -1
}

func (s *Server) scheduleHeartbeats(every time.Duration) {
	if s.state != Leader {
		// don't even consider continuing doing anything else, as
		// we are not dealing with a leader
		return
	}

	heartbeatTimeout := time.NewTicker(every)
	for s.state == Leader {
		select {
		case <-heartbeatTimeout.C:
			// send heartbeat to each peer
			for _, peer := range s.peers {
				go s.sendHeartbeat(peer, false, nil)
			}
			heartbeatTimeout.Reset(every)
		}
	}
}

func (s *Server) sendHeartbeat(peer int, withLogEntries bool, wg *sync.WaitGroup) AppendEntriesReply {
	if wg != nil {
		defer wg.Done()
	}
	if _, ok := s.peerClients[peer]; !ok {
		s.mu.Lock()
		addr := fmt.Sprintf(":%d", peer)
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			panic(err)
		}
		s.peerClients[peer] = client
		s.mu.Unlock()
	}
	client := s.peerClients[peer]
	var args AppendEntriesArguments
	if withLogEntries {
		var prevLogTerm, prevLogIndex int
		prevLogIndex = int(s.commitIndex.Load() - 1)
		if prevLogIndex >= 0 && prevLogIndex < len(s.logEntries) {
			prevLogTerm = s.logEntries[prevLogIndex].Term
		}
		args = AppendEntriesArguments{
			Term:         int(s.currentTerm.Load()),
			LeaderId:     s.id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: int(s.commitIndex.Load()),
			Entries:      s.logEntries,
		}
	} else {
		args = AppendEntriesArguments{
			Term:     int(s.currentTerm.Load()),
			LeaderId: s.id,
		}
	}
	var reply AppendEntriesReply
reachOutLoop:
	for {
		select {
		case err := <-channedRpcCall(appendEntriesEndpoint, client, args, &reply):
			if err != nil {
				s.log("%s ended up with error %v", appendEntriesEndpoint, err)
				break reachOutLoop
			}
		case <-time.After(rpcCallTimeout):
			// redo request
			continue reachOutLoop
		}
	}
	return reply
}

func (s *Server) transitionToLeader() {
	s.state = Leader
	// send out empty AppendEntries rpcs (heartbeats) to others in the cluster,
	// so to maintain leader's authority

	// for each Server, index of the next log to send to that Server
	// (initialized to leader last log index + 1)
	lastIndex, _ := s.lastLogIndexAndTerm()
	for idx, _ := range s.peers {
		s.nextIndex[idx] = lastIndex + 1
	}

	for _, peer := range s.peers {
		go s.sendHeartbeat(peer, false, nil)
	}

	go s.scheduleHeartbeats(randomizedBroadcastPeriod())
}

func (s *Server) transitionToFollower(term int) {
	s.log("become a follower with term %d", term)
	s.state = Follower
	s.votedFor = -1
	s.currentTerm.Store(int64(term))
	s.electionResetEvent = time.Now()

	go s.monitorElectionTimer()
}

func (s *Server) log(format string, args ...any) {
	currState := s.String()
	extendedFmt := fmt.Sprintf("[%s(%d)]: %s", currState, s.id, format)
	log.Printf(extendedFmt, args...)
}
