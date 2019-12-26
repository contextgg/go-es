package es

import "testing"

// Event1 for testing
type Event1 struct{}

func TestMatchAnyEventOf(t *testing.T) {
	matcher := MatchAnyEventOf(&Event1{})

	if matcher == nil {
		t.Error("Not a match")
	}
}
