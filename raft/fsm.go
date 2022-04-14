package raft

type ExecutionResult = int

// TODO(threadedstream): provide more descriptive names
const (
	FsmOk ExecutionResult = iota
	FsmInternalError
	FsmUnknownCommand
)

type FSM interface {
	Apply(int, int, any) any
}

type pair struct {
	clientId    int
	sequenceNum int
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
	fsmResults map[pair]any
}

func NewRaftFSM() *RaftFSM {
	fsm := new(RaftFSM)
	return fsm
}

func (fsm *RaftFSM) Apply(clientId, sequenceNum int, command any) ExecutionResult {
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

	fsm.fsmResults[pair{clientId: clientId, sequenceNum: sequenceNum}] = result
	return FsmOk
}
