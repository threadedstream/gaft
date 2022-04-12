package main

import (
	"sync"
)

const N = 5

func allBut(all []int, but int) []int{
	var result []int
	for _, single := range all {
		if single == but {
			continue
		}
		result = append(result, single)
	}

	return result
}

func allocatePorts(n int) []int{ 
	var ports []int
	start := 35000
	// 35000-45000
	for port := start; port < start + n; port++ {
		ports = append(ports, port)
	}	
	
	return ports
}

func main() {
	wg := sync.WaitGroup{}
	var servers [N]*Server
	ports := allocatePorts(N)
	// available ports
	for i, port := range ports {
		servers[i] = NewServer(port, allBut(ports, port), &wg)
	}

	wg.Wait()
}

