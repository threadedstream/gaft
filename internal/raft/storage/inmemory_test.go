package storage

import (
	"fmt"
	"testing"
)

func TestSet(t *testing.T) {
	s := NewInMemory()
	if err := s.Set(1, "make a sandwich"); err != nil {
		t.Fatal(err)
	}
}

func TestGet(t *testing.T) {
	s := NewInMemory()
	command := "make a sandwich"
	if err := s.Set(1, command); err != nil {
		t.Fatal(err)
	}
	var result any
	var err error
	if result, err = s.Get(1); err != nil {
		t.Fatal(err)
	}

	resultStr := fmt.Sprintf("%v", result)
	if resultStr != command {
		t.Fatalf("expected %s, but got %s", command, resultStr)
	}
}

func TestStress(t *testing.T) {
	s := NewInMemory()
	commandFmt := "make a %d-th sandwich"
	var result any
	var err error
	for i := 0; i < 100000; i++ {
		command := fmt.Sprintf(commandFmt, i)
		if err = s.Set(1, command); err != nil {
			t.Fatal(err)
		}

		// Try tinkering with a Go compiler to allow
		// implicit casts from any to string
		if result, err = s.Get(1); err != nil {
			t.Fatal(err)
		}

		if result != command {
			t.Fatalf("expected %s, but got %s", command, result)
		}
	}
}
