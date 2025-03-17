package main

import (
	"raft/internal/raft"
	"sync"

	"github.com/travisjeffery/go-dynaport"
)

const N = 5

func allBut(ports []int, port int) []int {
	result := make([]int, 0, len(ports)-1)
	for _, p := range ports {
		if p != port {
			result = append(result, p)
		}
	}
	return result
}

func main() {
	wg := sync.WaitGroup{}
	var servers [N]*raft.Server
	ports := dynaport.Get(N)
	// available ports
	for i, port := range ports {
		servers[i] = raft.NewServer(port, allBut(ports, port), &wg)
	}

	wg.Wait()
}
