package raft

import (
	"sync"
	"time"
)

type ExecutionResult = int

// TODO(threadedstream): provide more descriptive names
const (
	FsmOk ExecutionResult = iota
	FsmInternalError
	FsmUnknownCommand
)

type SweepPhase = int

const (
	SweepTerminated SweepPhase = iota
	SweepInProgress
)

type FSM interface {
	Apply(int, int, any) any
}

type pair struct {
	clientId    int
	sequenceNum int
}

type commandheader struct {
	clientId    int
	sequenceNum int
}

type commandresult struct {
	result        any
	executionTime any
}

type RaftFSM struct {
	FSM
	// sessions track the latest serial number processed for a client
	sessions map[int]int
	// fsmResults stores results of clients' requests
	// it maps sequenceNum
	// TODO(threadedstream): map might not be the best choice for this
	// particular scenario, as each time client's command gets successfully
	// executed, all prior results have to be discarded.
	// We need a data structure that is guaranteed to rapidly find
	// all entries associated with some key. Something hints me that
	// the set is indeed a worthy candidate, however some research is yet to
	// be done.
	fsmResults    map[commandheader]commandresult
	mu            sync.Mutex
	sweepInterval time.Duration
	sweepPhase    SweepPhase
}

func NewRaftFSM(sweepInterval time.Duration) *RaftFSM {
	fsm := new(RaftFSM)
	fsm.sessions = make(map[int]int)
	fsm.fsmResults = make(map[commandheader]commandresult)
	fsm.sweepInterval = sweepInterval
	go fsm.scheduleSweeping()
	return fsm
}

func (fsm *RaftFSM) Apply(clientId, sequenceNum int, command any) ExecutionResult {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	// assume for now that command is of type string
	var result string
	switch command {
	case "make a sandwich":
		result = "sandwich is made"
	case "register client":
		result = "client has been successfully registered"
	default:
		// unknown command
		return FsmUnknownCommand
	}

	fsm.sessions[clientId] = sequenceNum

	header := commandheader{
		clientId:    clientId,
		sequenceNum: sequenceNum,
	}

	res := commandresult{
		result:        result,
		executionTime: time.Now(),
	}

	fsm.fsmResults[header] = res
	return FsmOk
}

func (fsm *RaftFSM) scheduleSweeping() {
	ticker := time.NewTicker(fsm.sweepInterval)
	for {
		select {
		case <-ticker.C:
			// check if previous sweep has been terminated
			if fsm.sweepPhase == SweepTerminated {
				go fsm.sweep()
			}
			ticker.Reset(fsm.sweepInterval)
		}
	}
}

func (fsm *RaftFSM) sweep() {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	fsm.sweepPhase = SweepInProgress
	for clientId, lastSeqNum := range fsm.sessions {
		fsm.sweepPriorResults(clientId, lastSeqNum)
	}
}

func (fsm *RaftFSM) sweepPriorResults(clientId int, lastSeqNum int) {
	for header, _ := range fsm.fsmResults {
		if header.clientId == clientId && header.sequenceNum != lastSeqNum {
			delete(fsm.fsmResults, header)
		}
	}
}
