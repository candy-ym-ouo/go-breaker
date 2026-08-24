package breaker

import "testing"

func TestStateMappings(t *testing.T) {
	cases := []struct {
		state State
		name  string
		color string
	}{
		{StateClosed, "closed", "green"},
		{StateOpen, "open", "red"},
		{StateHalfOpen, "half_open", "yellow"},
	}
	for _, tc := range cases {
		if tc.state.String() != tc.name || tc.state.Color() != tc.color {
			t.Fatalf("unexpected mapping for %v", tc.state)
		}
		parsed, err := ParseState(tc.name)
		if err != nil || parsed != tc.state {
			t.Fatalf("parse %s: %v, %v", tc.name, parsed, err)
		}
	}
}

func TestConfigValidation(t *testing.T) {
	config := DefaultConfig()
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
	config.ErrorThreshold = 1.1
	if err := config.Validate(); err == nil {
		t.Fatal("expected invalid threshold")
	}
}
