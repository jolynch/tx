package utils

import (
	"testing"
)

func naiveCommonPrefixLen(a string, b string) int {
	commonLen := len(a)
	if len(b) < commonLen {
		commonLen = len(b)
	}
	for i := 0; i < commonLen; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return commonLen
}

func FuzzCommonPrefixLen(f *testing.F) {
	f.Add("", "")
	f.Add("a", "")
	f.Add("", "b")
	f.Add("abc", "abc")
	f.Add("abc", "abd")
	f.Add("prefix-123", "prefix-xyz")
	f.Add("hello world", "hello worlD")
	f.Add("same", "same")
	f.Add("short", "shorter")
	f.Add("longer", "long")

	f.Fuzz(func(t *testing.T, a string, b string) {
		got := CommonPrefixLen(a, b)
		want := naiveCommonPrefixLen(a, b)
		if got != want {
			t.Fatalf("CommonPrefixLen(%q, %q) = %d, want %d", a, b, got, want)
		}
	})
}

func TestPathWithinRoot(t *testing.T) {
	tests := []struct {
		root, p string
		want    bool
	}{
		{"/srv/data", "/srv/data", true},
		{"/srv/data/", "/srv/data/sub/f", true},
		{"/srv/data", "/srv/data/a/../b", true},
		{"/srv/data", "/srv/dataX", false}, // sibling sharing the root's prefix
		{"/srv/data", "/srv/dataX/f", false},
		{"/srv/data", "/srv", false},
		{"/srv/data", "/srv/data/../other", false},
		{"/srv/data", "/srv/data/..foo", true}, // "..foo" is a name, not a parent
		{"/", "/etc", true},
		{"/srv/data", "relative", false},
	}
	for _, tc := range tests {
		if got := PathWithinRoot(tc.root, tc.p); got != tc.want {
			t.Errorf("PathWithinRoot(%q, %q) = %v, want %v", tc.root, tc.p, got, tc.want)
		}
	}
}
