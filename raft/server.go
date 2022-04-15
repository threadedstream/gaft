package raft

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
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
		Success       bool
		ConflictTerm  int
		ConflictIndex int
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

	IssueCommandArguments struct {
		Command any
	}

	IssueCommandReply struct {
		Result any
	}

	IdentifyLeaderArguments struct {
	}

	IdentifyLeaderReply struct {
		Id int
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
	Term        int
	ClientId    int
	SequenceNum int
	Command     any
}

type Printable interface {
	String() string
}

// RaftServer represents a single unit in cluster of RaftServers
type RaftServer struct {
	Printable
	state
	// RaftServer's id
	id int
	// leader's id
	leaderId int
	// other RaftServers in a cluster
	peers []int
	// latest term RaftServer has seen (initialized to 0 on first boot, increases monotonically)
	currentTerm int
	// latest term RaftServer has seen (initialized to 0 on first boot, increases monotonically)
	votedFor int
	// log entries; each entry contains command
	// for state machine, and term when entry
	// was received by leader (first index is 1)
	logEntries []LogEntry
	// volatile state on RaftServers
	commitIndex int
	lastApplied int
	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// election timer
	electionResetEvent time.Time

	// rpc related stuff
	rpcRaftServer *rpc.Server
	listener      net.Listener
	peerClients   map[int]*rpc.Client

	// concurrency related
	wg *sync.WaitGroup
	mu sync.Mutex

	quit <-chan bool

	// finite-state machine executing commands
	fsm *RaftFSM
	// map from clientId to the result of previously executed command
	fsmResults map[int]any
}

func NewRaftServer(id int, peers []int, wg *sync.WaitGroup) *RaftServer {
	// raft related RaftServer data
	server := new(RaftServer)
	server.state = Follower
	server.currentTerm = 0
	server.votedFor = -1
	server.id = id
	server.peers = peers
	server.wg = wg

	server.commitIndex = 0
	server.lastApplied = 0
	server.electionResetEvent = time.Now()

	server.nextIndex = make([]int, len(server.peers))
	server.matchIndex = make([]int, len(server.peers))

	server.fsm = NewRaftFSM(3 * time.Minute)
	server.fsmResults = make(map[int]any)
	// rpc related RaftServer data
	server.rpcRaftServer = rpc.NewServer()
	if err := server.rpcRaftServer.RegisterName(rpcBase, server); err != nil {
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

	server.log("setup a listener for RaftServer %d", server.id)
	server.start()
	go server.monitorElectionTimer()
	return server
}

func (s *RaftServer) Accept() chan net.Conn {
	connChan := make(chan net.Conn, 1)
	conn, err := s.listener.Accept()
	if err != nil {
		log.Fatal("listener accept error: ", err)
	}
	connChan <- conn
	return connChan
}

func (s *RaftServer) start() {
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
					s.rpcRaftServer.ServeConn(conn)
				}(conn)
			}
		}
	}()
}

func (s *RaftServer) shutdownClientConnections() {
	for _, client := range s.peerClients {
		client.Close()
	}
}

func (s *RaftServer) Shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = Dead
	s.shutdownClientConnections()
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
func (s *RaftServer) AppendEntries(args AppendEntriesArguments, result *AppendEntriesReply) error {
	result.Success = false

	if s.currentTerm < args.Term && s.state != Follower {
		s.transitionToFollower(args.Term)
	}

	if len(s.logEntries) > 0 && s.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
		return nil
	}

	if args.PrevLogIndex == -1 ||
		args.PrevLogIndex < len(s.logEntries) {

		result.Success = true

		// resetting election timer, as we've got a heartbeat from a leader
		s.electionResetEvent = time.Now()

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

		if args.LeaderCommit > s.commitIndex {
			s.commitIndex = min(args.LeaderCommit, len(s.logEntries)-1)
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
func (s *RaftServer) RequestVote(args RequestVoteArguments, result *RequestVoteReply) error {
	result.VoteGranted = false

	s.electionResetEvent = time.Now()
	go s.monitorElectionTimer()

	if args.Term > s.currentTerm && s.state != Follower {
		s.transitionToFollower(args.Term)
	}

	lastLogIndex, lastLogTerm := s.lastLogIndexAndTerm()

	// kind of weird, but I enjoy writing it that way
	termsEqual := args.Term == s.currentTerm
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

	result.Term = s.currentTerm

	return nil
}

func (s *RaftServer) IdentifyLeader(args IdentifyLeaderArguments, reply *IdentifyLeaderReply) error {
	if s.state != Leader {
		reply.Id = -1
		return nil
	}
	reply.Id = s.id
	return nil
}

func (s *RaftServer) IssueCommand(args IssueCommandArguments, reply *IssueCommandReply) error {
	if s.state != Leader {
		if _, ok := s.peerClients[s.leaderId]; !ok {
			s.mu.Lock()
			addr := fmt.Sprintf(":%d", s.leaderId)
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				panic(err)
			}
			s.peerClients[s.leaderId] = client
			s.mu.Unlock()
		}
		client := s.peerClients[s.leaderId]
	issueCommandRequest:
		for {
			select {
			case err := <-channedRpcCall(issueCommandEndpoint, client, args, reply):
				if err != nil {
					// should we indefinitely retry a request
					s.log("%s ended up with error %v", issueCommandEndpoint, err)
					return nil
				}
				break issueCommandRequest
			case <-time.After(rpcCallTimeout):
				// retry the request
				continue issueCommandRequest
			}
		}

		//// delegate it to leader
		//delegateToLeader := func(args IssueCommandArguments, reply *IssueCommandReply) {
		//	// first, identify a leader
		//	identifyLeader := func(peer int, leaderId chan<- int) {
		//		if _, ok := s.peerClients[peer]; !ok {
		//			s.mu.Lock()
		//			addr := fmt.Sprintf(":%d", peer)
		//			client, err := rpc.Dial("tcp", addr)
		//			if err != nil {
		//				panic(err)
		//			}
		//			s.peerClients[peer] = client
		//			s.mu.Unlock()
		//		}
		//		client := s.peerClients[peer]
		//		var identifyArgs IdentifyLeaderArguments
		//		var identifyReply IdentifyLeaderReply
		//	identifyLeaderRequest:
		//		for {
		//			select {
		//			case err := <-channedRpcCall(identifyLeaderEndpoint, client, identifyArgs, &identifyReply):
		//				if err != nil {
		//					s.log("%s ended up with error %v", identifyLeaderEndpoint, err)
		//					return
		//				}
		//				break identifyLeaderRequest
		//			case <-time.After(rpcCallTimeout):
		//				continue identifyLeaderRequest
		//			}
		//		}
		//		leaderId <- identifyReply.Id
		//	}
		//
		//	leaderId := make(chan int)
		//	for _, peer := range s.peers {
		//		go identifyLeader(peer, leaderId)
		//	}
		//	id := <-leaderId
		//	// at this point, we've successfully got a leader's port and
		//	// may easily query the map for a leader's client instance
		//	client := s.peerClients[id]
		//issueCommandEndpoint:
		//	for {
		//		select {
		//		case err := <-channedRpcCall(issueCommandEndpoint, client, args, reply):
		//			if err != nil {
		//				s.log("%s ended up with error %v", issueCommandEndpoint, err)
		//				return
		//			}
		//			break issueCommandEndpoint
		//		case <-time.After(rpcCallTimeout):
		//			// retry the request
		//			continue issueCommandEndpoint
		//		}
		//	}
		//}
		//delegateToLeader(args, reply)
		//return nil
	}

	s.logEntries = append(s.logEntries, LogEntry{
		Term:    s.currentTerm,
		Command: args.Command,
	})

	//appendEntriesArgs := AppendEntriesArguments{
	//	Term:         s.currentTerm,
	//	LeaderId:     s.id,
	//	PrevLogIndex: s.commitIndex - 1
	//	PrevLogTerm:
	//	LeaderCommit: s.commitIndex,
	//	Entries:      s.logEntries,
	//}
	//
	//// first, replicate log entries across all RaftServers
	//appendEntries := func(peer int, wg *sync.WaitGroup) {
	//	if _, ok := s.peerClients[peer]; !ok {
	//		s.mu.Lock()
	//		addr := fmt.Sprintf(":%d", peer)
	//		client, err := rpc.Dial("tcp", addr)
	//		if err != nil {
	//			panic(err)
	//		}
	//		s.peerClients[peer] = client
	//		s.mu.Unlock()
	//	}
	//	client := s.peerClients[peer]
	//
	//	for {
	//		select {
	//		case <-channedRpcCall(appendEntriesEndpoint, client, args):
	//
	//		}
	//	}
	//}
	return nil
}

func (s *RaftServer) String() string {
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

func (s *RaftServer) monitorElectionTimer() {
	timeoutDuration := randomizedElectionTimeout()
	//termStarted := s.currentTerm

	for {
		time.Sleep(10 * time.Millisecond)

		if s.commitIndex > s.lastApplied {
			s.lastApplied++
		}

		if s.state != Follower && s.state != Candidate {
			s.log("neither follower nor candidate")
			return
		}

		//if termStarted != s.currentTerm {
		//	s.log("termStarted does not match a current term")
		//	return
		//}

		if elapsed := time.Since(s.electionResetEvent); elapsed >= timeoutDuration {
			s.log("timed out, about to start an election")
			s.startElection()
			return
		}
	}
}

func (s *RaftServer) commit() {
	for s.commitIndex < len(s.logEntries) {
		s.commitIndex++
	}
}

func (s *RaftServer) replicate() int {
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

func (s *RaftServer) startElection() {
	s.state = Candidate
	s.currentTerm++
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
			Term:         s.currentTerm,
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

		s.currentTerm = reply.Term
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

func (s *RaftServer) lastLogIndexAndTerm() (int, int) {
	if len(s.logEntries) > 0 {
		lastIndex := len(s.logEntries) - 1
		return lastIndex, s.logEntries[lastIndex].Term
	}
	return -1, -1
}

func (s *RaftServer) scheduleHeartbeats(every time.Duration) {
	if s.state != Leader {
		// don't even consider continuing doing anything else, as
		// we don't deal with a leader
		return
	}

	heartbeatTimeout := time.NewTicker(every)
heartbeatLoop:
	for {
		select {
		case <-heartbeatTimeout.C:
			// send heartbeat to each peer
			for _, peer := range s.peers {
				go s.sendHeartbeat(peer, false, nil)
			}
			heartbeatTimeout.Reset(every)
		}
		if s.state != Leader {
			break heartbeatLoop
		}
	}
}

func (s *RaftServer) sendHeartbeat(peer int, withLogEntries bool, wg *sync.WaitGroup) AppendEntriesReply {
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
		prevLogIndex = s.commitIndex - 1
		if prevLogIndex >= 0 && prevLogIndex < len(s.logEntries) {
			prevLogTerm = s.logEntries[prevLogIndex].Term
		}
		args = AppendEntriesArguments{
			Term:         s.currentTerm,
			LeaderId:     s.id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: s.commitIndex,
			Entries:      s.logEntries,
		}
	} else {
		args = AppendEntriesArguments{
			Term:     s.currentTerm,
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

func (s *RaftServer) transitionToLeader() {
	s.state = Leader
	// send out empty AppendEntries rpcs (heartbeats) to others in the cluster,
	// so to maintain leader's authority

	// for each RaftServer, index of the next log to send to that RaftServer
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

func (s *RaftServer) transitionToFollower(term int) {
	s.log("become a follower with term %d", term)
	s.state = Follower
	s.votedFor = -1
	s.currentTerm = term
	s.electionResetEvent = time.Now()

	go s.monitorElectionTimer()
}

func (s *RaftServer) log(format string, args ...any) {
	currState := s.String()
	extendedFmt := fmt.Sprintf("[%s(%d)]: %s", currState, s.id, format)
	log.Printf(extendedFmt, args...)
}
