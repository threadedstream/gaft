package raft

import (
	"fmt"
	"net/rpc"
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
		err := client.Call(identifyLeaderEndpoint, args.(IdentifyLeaderArguments), reply.(*IdentifyLeaderReply))
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
