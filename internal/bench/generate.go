package main

import (
	"bufio"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/jolynch/tx/internal/filexfer/encoding"
)

type genSpec struct {
	outdir string
	count  int
	size   int64
}

func parseGenSpec(s string) (genSpec, error) {
	at := strings.LastIndex(s, "@")
	if at < 0 {
		return genSpec{}, fmt.Errorf("invalid spec %q: missing '@'", s)
	}
	lhs, sizeStr := s[:at], s[at+1:]
	colon := strings.LastIndex(lhs, ":")
	if colon < 0 {
		return genSpec{}, fmt.Errorf("invalid spec %q: missing ':'", s)
	}
	outdir, countStr := lhs[:colon], lhs[colon+1:]
	if outdir == "" {
		return genSpec{}, fmt.Errorf("invalid spec %q: empty outdir", s)
	}
	count, err := strconv.Atoi(countStr)
	if err != nil || count <= 0 {
		return genSpec{}, fmt.Errorf("invalid count %q in spec %q", countStr, s)
	}
	size, err := encoding.ParseByteSize(sizeStr)
	if err != nil {
		return genSpec{}, fmt.Errorf("invalid size %q in spec %q: %w", sizeStr, s, err)
	}
	return genSpec{outdir: outdir, count: count, size: size}, nil
}

func runGenerate(args []string) error {
	if len(args) == 0 {
		return errors.New("requires at least one spec of form <outdir>:<count>@<size>")
	}
	specs := make([]genSpec, 0, len(args))
	for _, a := range args {
		spec, err := parseGenSpec(a)
		if err != nil {
			return err
		}
		specs = append(specs, spec)
	}
	for _, spec := range specs {
		if err := generateSpec(spec); err != nil {
			return err
		}
	}
	return nil
}

func generateSpec(s genSpec) error {
	if err := os.MkdirAll(s.outdir, 0o755); err != nil {
		return err
	}
	if parent := filepath.Dir(s.outdir); parent != "." && parent != "" {
		_ = os.MkdirAll(filepath.Join(parent, "dst"), 0o755)
	}
	pad := len(strconv.Itoa(s.count))
	rng := rand.New(rand.NewSource(1))
	buf := make([]byte, 1<<20)
	for i := 0; i < s.count; i++ {
		name := fmt.Sprintf("file-%0*d.bin", pad, i)
		path := filepath.Join(s.outdir, name)
		if err := writeRandom(path, s.size, buf, rng); err != nil {
			return fmt.Errorf("write %s: %w", path, err)
		}
	}
	fmt.Printf("generated %d files of %s in %s\n",
		s.count, encoding.HumanBytes(s.size), s.outdir)
	return nil
}

func writeRandom(path string, size int64, buf []byte, rng *rand.Rand) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	remaining := size
	for remaining > 0 {
		chunk := int64(len(buf))
		if remaining < chunk {
			chunk = remaining
		}
		_, _ = rng.Read(buf[:chunk])
		if _, err := w.Write(buf[:chunk]); err != nil {
			f.Close()
			return err
		}
		remaining -= chunk
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
