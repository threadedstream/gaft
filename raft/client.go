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
)

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
func (s *Server) ClientRequest(args ClientRequestArguments, reply *ClientRequestReply) error {
	if s.state != Leader {
		reply.Status = NotLeader
		return nil
	}
	s.logEntries = append(s.logEntries, LogEntry{
		Term:    s.currentTerm,
		Command: args.Command,
	})

	// check if response for client's sequenceNum is already
	// discarded
	if args.ClientId == 0 {

	}
}
