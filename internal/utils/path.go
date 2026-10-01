package utils

import (
	"path/filepath"
	"strings"
)

// PathWithinRoot reports whether p, once cleaned, is root itself or lies
// beneath it. The check is lexical: symlinks are not resolved.
func PathWithinRoot(root string, p string) bool {
	root = filepath.Clean(root)
	p = filepath.Clean(p)
	rel, err := filepath.Rel(root, p)
	if err != nil {
		return false
	}
	if rel == "." {
		return true
	}
	if strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." {
		return false
	}
	return !filepath.IsAbs(rel)
}
