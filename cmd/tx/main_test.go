package main

import (
	"slices"
	"strings"
	"testing"
)

func TestParsePercentFlag(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    int
		wantErr bool
	}{
		{name: "default", raw: "25%", want: 25},
		{name: "min", raw: "1%", want: 1},
		{name: "max", raw: "100%", want: 100},
		{name: "missing percent", raw: "25", wantErr: true},
		{name: "decimal", raw: "25.0%", wantErr: true},
		{name: "zero", raw: "0%", wantErr: true},
		{name: "too large", raw: "101%", wantErr: true},
		{name: "negative", raw: "-1%", wantErr: true},
		{name: "empty", raw: "", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parsePercentFlag(tc.raw, "--gentle-cpu")
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", tc.raw)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("unexpected value: got=%d want=%d", got, tc.want)
			}
		})
	}
}

func TestSplitSendCommand(t *testing.T) {
	tests := []struct {
		name            string
		args            []string
		wantAddr        string
		wantCmd         string
		wantRest        []string
		wantErrContains string
	}{
		{
			name:     "default addr serve",
			args:     []string{"serve"},
			wantAddr: defaultFileListener,
			wantCmd:  "serve",
		},
		{
			name:     "default addr serve with chroot",
			args:     []string{"serve", "/tmp"},
			wantAddr: defaultFileListener,
			wantCmd:  "serve",
			wantRest: []string{"/tmp"},
		},
		{
			name:     "explicit addr serve",
			args:     []string{"127.0.0.1:4000", "serve"},
			wantAddr: "127.0.0.1:4000",
			wantCmd:  "serve",
		},
		{
			name:     "explicit addr serve with remaining args",
			args:     []string{"127.0.0.1:4000", "serve", "/tmp"},
			wantAddr: "127.0.0.1:4000",
			wantCmd:  "serve",
			wantRest: []string{"/tmp"},
		},
		{
			name:            "invalid addr token",
			args:            []string{"bogus-flag"},
			wantErrContains: "invalid server-addr",
		},
		{
			name:            "missing command after addr",
			args:            []string{"127.0.0.1:4000"},
			wantErrContains: errMissingSendCommand.Error(),
		},
		{
			name:            "empty args",
			args:            nil,
			wantErrContains: errMissingSendCommand.Error(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotAddr, gotCmd, gotRest, err := splitSendCommand(tc.args)
			if tc.wantErrContains != "" {
				if err == nil {
					t.Fatalf("expected error containing %q", tc.wantErrContains)
				}
				if !strings.Contains(err.Error(), tc.wantErrContains) {
					t.Fatalf("expected error containing %q, got %q", tc.wantErrContains, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if gotAddr != tc.wantAddr {
				t.Fatalf("addr: got %q want %q", gotAddr, tc.wantAddr)
			}
			if gotCmd != tc.wantCmd {
				t.Fatalf("cmd: got %q want %q", gotCmd, tc.wantCmd)
			}
			if !slices.Equal(gotRest, tc.wantRest) {
				t.Fatalf("rest: got %v want %v", gotRest, tc.wantRest)
			}
		})
	}
}
