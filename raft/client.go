package raft

type ClientRequestStatus = int

const (
	OK ClientRequestStatus = iota
	NotLeader
	SessionExpired
)

type (
	ClientRequestArguments struct {
		ClientId    int
		SequenceNum int
		Command     any
	}

	ClientRequestReply struct {
		Status     ClientRequestStatus
		Response   any // should turn into some type later
		LeaderHint int // the port of a leader's server, if available
	}

	RegisterClientReply struct {
		Status     ClientRequestStatus
		ClientId   int
		LeaderHint int
	}

	ClientQueryArguments struct {
		Query any
	}

	ClientQueryReply struct {
		Status     ClientRequestStatus
		Response   any
		LeaderHint int
	}
)

// RaftClient represents a client making requests to the RaftServer
type RaftClient struct {
	clientId int
}

//ClientRequest RPC
//Arguments:
// 	clientId:	 	client invoking request
// 	sequenceNum:		to eliminate duplicates
// 	command:			request for state machine, may affect state
//Results:
//	status: 	OK if state machine applied command
// 	response: 	state machine output, if successful
//  leaderHint: address of recent leader
//Receiver implementation:
// 	1. Reply NOT_LEADER if not leader, providing hint when available
//  2. Append command to log, replicate and commit it
//  3. Reply SESSION_EXPIRED if no record of clientId or if response
// 	   for client's sequenceNum already discarded
//  4. If sequenceNum already processed from client, reply OK
// 	   with stored response
//  5. Apply command in log order
//  6. Save state machine output with sequenceNum for client, discard any
// 	   prior response for client
//  7. Reply OK with state machine output
func (s *RaftServer) ClientRequest(args ClientRequestArguments, reply *ClientRequestReply) error {
	if s.state != Leader {
		reply.Status = NotLeader
		reply.LeaderHint = s.leaderId
		return nil
	}
	reply.Status = OK
	s.logEntries = append(s.logEntries, LogEntry{
		Term:    s.currentTerm,
		Command: args.Command,
	})

	if _, ok := s.fsm.sessions[args.ClientId]; !ok {
		// no session entry associated with a given client
		reply.Status = SessionExpired
		return nil
	}

	credentials := pair{clientId: args.ClientId, sequenceNum: args.SequenceNum}
	// reply OK if already processed
	if _, ok := s.fsm.fsmResults[credentials]; ok {
		reply.Response = s.fsm.fsmResults[credentials]
		return nil
	}

	// apply commands in log order

	// save state machine output of a client's command with
	// sequenceNum, discard any prior response for client
	return nil
}

func (s *RaftServer) RegisterClient(_ struct{}, reply *RegisterClientReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != Leader {
		reply.Status = NotLeader
		reply.LeaderHint = s.leaderId
		return nil
	}
	command := "register client"
	s.logEntries = append(s.logEntries, LogEntry{
		Term:    s.currentTerm,
		Command: command,
	})
	// apply command in log order
	newClientId := len(s.logEntries) - 1
	result := s.fsm.Apply(newClientId, -1, s.logEntries)
	if result != FsmOk {
		reply.Status = result
		return nil
	}
	s.fsmResults[newClientId] = result
	reply.Status = OK
	reply.ClientId = newClientId
	return nil
}

func (s *RaftServer) ClientQuery(args ClientQueryArguments, reply *ClientQueryReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != Leader {
		reply.Status = NotLeader
		reply.LeaderHint = s.leaderId
		return nil
	}
	// Wait until last committed entry is from this leader's term
	<-s.waitUntilCommittedEntryFromTerm()
	readIndex := s.commitIndex
	serversReplied := 0

	feedbackHeartbeat := func(peer int) {
		reply := s.sendHeartbeat(peer)
		if reply.Success {
			serversReplied++
		}
	}

	for _, peer := range s.peers {
		go feedbackHeartbeat(peer)
	}

	for serversReplied < quorum {
		// wait
	}

	return nil
}

func (s *RaftServer) waitUntilCommittedEntryFromTerm() chan struct{} {
	out := make(chan struct{}, 1)
	for s.logEntries[s.commitIndex].Term != s.currentTerm {

	}

	out <- struct{}{}
}
