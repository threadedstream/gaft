package main

import "log"

func min(x, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}

func assert(condition bool, format string, args ...any) {
	if !condition {
		log.Fatalf(format, args)
	}
}
