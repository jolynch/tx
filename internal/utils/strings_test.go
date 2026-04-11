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

