package raft

import "net/rpc"

// useful for handling timeouts
func channedAppendEntries(client *rpc.Client, args AppendEntriesArguments, reply *AppendEntriesReply) chan error {
	chanErr := make(chan error)
	err := client.Call(appendEntriesEndpoint, args, reply)
	chanErr <- err
	return chanErr
}

func channedRequestVote(client *rpc.Client, args RequestVoteArguments, reply *RequestVoteReply) chan error {
	chanErr := make(chan error, 1)
	err := client.Call(requestVoteEndpoint, args, reply)
	chanErr <- err
	return chanErr
}
