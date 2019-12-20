package es

import (
	"fmt"
	"testing"
)

func TestCommandNameMatch(t *testing.T) {
	data := []struct {
		key  string
		name string
		out  bool
	}{
		{"abc", "abc", true},
		{"TryLoginCommand", "TryLoginCommand", true},
		{"TryLoginCommand", "TryLogin", true},
		{"TryLoginCommand", "tryLogin", true},
		{"TryLoginCommand", "trylogin", true},
	}

	for _, tt := range data {
		t.Run(fmt.Sprintf("%s-%s", tt.key, tt.name), func(t *testing.T) {
			out := isCommandMatch(tt.key, tt.name)
			if out != tt.out {
				t.Errorf("got %v, want %v", out, tt.out)
			}
		})
	}
}
