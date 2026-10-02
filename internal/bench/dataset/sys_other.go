//go:build !linux

package dataset

import "os"

func fdatasync(f *os.File) error { return f.Sync() }
