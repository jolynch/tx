package utils

import "testing"

func TestIsHostPort(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{"127.0.0.1:3453", true},
		{"localhost:80", true},
		{"[::1]:443", true},
		{"0.0.0.0:0", true},
		{"", false},
		{"127.0.0.1", false},
		{"localhost", false},
		{":3453", false},
		{"127.0.0.1:", false},
		{"/tmp/foo", false},
		{"--flag", false},
	}
	for _, tc := range tests {
		if got := IsHostPort(tc.in); got != tc.want {
			t.Errorf("IsHostPort(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestValidateHostPort(t *testing.T) {
	ok := []string{"127.0.0.1:3453", "[::1]:443", "host.example:1"}
	for _, s := range ok {
		if err := ValidateHostPort(s); err != nil {
			t.Errorf("ValidateHostPort(%q) unexpected error: %v", s, err)
		}
	}
	bad := []string{"", "   ", "-l", "/tmp/x", "host", "host:"}
	for _, s := range bad {
		if err := ValidateHostPort(s); err == nil {
			t.Errorf("ValidateHostPort(%q) expected error, got nil", s)
		}
	}
}
