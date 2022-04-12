package main

import (
	"net/rpc"
	"raft/raft"
	"sync"
)

const N = 5

func allBut(all []int, but int) []int {
	var result []int
	for _, single := range all {
		if single == but {
			continue
		}
		result = append(result, single)
	}

	return result
}

func allocatePorts(n int) []int {
	var ports []int
	start := 35000
	// 35000-45000
	for port := start; port < start+n; port++ {
		ports = append(ports, port)
	}

	return ports
}

func IssueCommand(command any) {
	client, err := rpc.Dial("tcp", ":35000")
	if err != nil {
		panic(err)
	}
}

func main() {
	wg := sync.WaitGroup{}
	var servers [N]*raft.Server
	ports := allocatePorts(N)
	// available ports
	for i, port := range ports {
		servers[i] = raft.NewServer(port, allBut(ports, port), &wg)
	}

	wg.Wait()
}
