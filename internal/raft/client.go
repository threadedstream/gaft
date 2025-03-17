package raft

import (
	"sync/atomic"
	"time"
)

type ClientRequestStatus = int

const (
	OK ClientRequestStatus = iota
	NotLeader
	SessionExpired
)

// Client represents a client making requests to the RaftServer
type Client struct {
	ID int
}

// ClientRequest RPC
// Arguments:
//
//	clientId:	 	client invoking request
//	sequenceNum:		to eliminate duplicates
//	command:			request for state machine, may affect state
//
// Results:
//
//		status: 	OK if state machine applied command
//		response: 	state machine output, if successful
//	 leaderHint: address of recent leader
//
// Receiver implementation:
//  7. Reply OK with state machine output
func (s *Server) ClientRequest(args ClientRequestArguments, reply *ClientRequestReply) error {
	// 	1. Reply NOT_LEADER if not leader, providing hint when available
	if s.state != Leader {
		reply.Status = NotLeader
		reply.LeaderHint = s.leaderId
		return nil
	}
	reply.Status = OK

	//  2. Append command to log, replicate and commit it
	s.logEntries = append(s.logEntries, LogEntry{
		Term:        int(s.currentTerm.Load()),
		Command:     args.Command,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	})

replicationStep:
	votes := s.replicate()
	if votes < quorum {
		goto replicationStep
	}

	s.commit()

	//  3. Reply SESSION_EXPIRED if no record of clientId or if response
	// 	   for client's sequenceNum already discarded
	credentials := commandheader{
		clientId:    args.ClientId,
		sequenceNum: args.SequenceNum,
	}
	_, noClientRecord := s.fsm.sessions[args.ClientId]
	_, sequenceNumPresent := s.fsm.fsmResults[credentials]
	if noClientRecord && !sequenceNumPresent {
		// no session entry associated with a given client
		reply.Status = SessionExpired
		return nil
	}

	//  4. If sequenceNum already processed from client, reply OK
	// 	   with stored response
	if sequenceNumPresent {
		reply.Response = s.fsm.fsmResults[credentials].result
		reply.Status = OK
		return nil
	}

	//  5. Apply command in log order
	newLogIdx := s.commitIndex + 1
	for newLogIdx < len(s.logEntries) {
		entry := s.logEntries[newLogIdx]
		s.fsm.Apply(entry.ClientId, entry.SequenceNum, newLogIdx, entry.Command)
	}

	//  6. Save state machine output with sequenceNum for client, discard any
	// prior response for client
	s.fsm.sweepPriorResults(args.ClientId, args.SequenceNum)

	reply.Status = OK
	reply.Response = s.fsm.fsmResults[commandheader{clientId: args.ClientId, sequenceNum: args.SequenceNum}]
	// save state machine output of a client's command with
	// sequenceNum, discard any prior response for client
	return nil
}

func (s *Server) RegisterClient(_ struct{}, reply *RegisterClientReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != Leader {
		reply.Status = NotLeader
		reply.LeaderHint = s.leaderId
		return nil
	}
	command := "register client"
	s.logEntries = append(s.logEntries, LogEntry{
		Term:    int(s.currentTerm.Load()),
		Command: command,
	})
	// apply command in log order
	n := len(s.logEntries) - 1
	newClientId := n
	result := s.fsm.Apply(newClientId, -1, n, s.logEntries)
	if result != FsmOk {
		reply.Status = result
		return nil
	}
	s.fsmResults[newClientId] = result
	reply.Status = OK
	reply.ClientId = newClientId
	return nil
}

func (s *Server) ClientQuery(_ ClientQueryArguments, reply *ClientQueryReply) error {
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

	var serversReplied atomic.Int32

	feedbackHeartbeat := func(peer int) {
		reply := s.sendHeartbeat(peer, false, nil)
		if reply.Success {
			serversReplied.Add(1)
		}
	}

	for _, peer := range s.peers {
		go feedbackHeartbeat(peer)
	}

	for int(serversReplied.Load()) < quorum {
		// wait
	}

	<-s.fsm.waitTill(readIndex)

	reply.Status = OK

	return nil
}

func (s *Server) waitUntilCommittedEntryFromTerm() chan struct{} {
	out := make(chan struct{}, 1)
	for s.logEntries[s.commitIndex].Term != int(s.currentTerm.Load()) {
		time.Sleep(time.Millisecond * 100)
	}

	out <- struct{}{}
	return out
}
