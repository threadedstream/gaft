package raft

import (
	"fmt"
	"net/rpc"
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

	IdentifyLeaderReply struct {
		Id int
	}
)

// useful for handling timeouts
func channedRpcCall(endpoint string, client *rpc.Client, args any, reply any) chan error {
	chanErr := make(chan error, 1)
	switch endpoint {
	case appendEntriesEndpoint:
		err := client.Call(appendEntriesEndpoint, args.(AppendEntriesArguments), reply.(*AppendEntriesReply))
		chanErr <- err
		return chanErr
	case requestVoteEndpoint:
		err := client.Call(requestVoteEndpoint, args.(RequestVoteArguments), reply.(*RequestVoteReply))
		chanErr <- err
		return chanErr
	case identifyLeaderEndpoint:
		err := client.Call(identifyLeaderEndpoint, args.(struct{}), reply.(*IdentifyLeaderReply))
		chanErr <- err
		return chanErr
	case issueCommandEndpoint:
		err := client.Call(issueCommandEndpoint, args.(IssueCommandArguments), reply.(*IssueCommandReply))
		chanErr <- err
		return chanErr
	default:
		// unknown endpoint
		panic(fmt.Errorf("unknown endpoint %s", endpoint))
	}
}
