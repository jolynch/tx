package utils

import (
	"errors"
	"net"
	"strings"
)

// IsHostPort reports whether s parses as a non-empty host:port pair.
func IsHostPort(s string) bool {
	host, port, err := net.SplitHostPort(s)
	if err != nil {
		return false
	}
	return strings.TrimSpace(host) != "" && strings.TrimSpace(port) != ""
}

// ValidateHostPort returns an error if s is not a valid host:port address.
// A leading dash is rejected so flag-like tokens do not slip through as
// addresses when positional parsing is ambiguous.
func ValidateHostPort(s string) error {
	errInvalid := errors.New("expected host:port address, for example 127.0.0.1:3453")
	if strings.TrimSpace(s) == "" {
		return errInvalid
	}
	if strings.HasPrefix(s, "-") {
		return errInvalid
	}
	if !IsHostPort(s) {
		return errInvalid
	}
	return nil
}
