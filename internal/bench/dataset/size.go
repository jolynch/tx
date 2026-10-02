package dataset

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"

	"github.com/jolynch/tx/internal/filexfer/encoding"
)

// SizeSpec is a parsed --size or --fill value.
type SizeSpec struct {
	Raw string
	// Exactly one of the following describes the request.
	Bytes   int64 // absolute bytes
	MemPct  int   // percent of MemTotal; may exceed 100
	DiskPct int   // percent of the BENCH_DIR filesystem capacity
	FillPct int   // --fill: size so the filesystem ends up this full
}

// ParseSize parses the --size grammar: BYTES, N%mem, or N%disk.
func ParseSize(raw string) (SizeSpec, error) {
	raw = strings.TrimSpace(raw)
	spec := SizeSpec{Raw: raw}
	switch {
	case strings.HasSuffix(raw, "%mem"):
		n, err := parsePct(strings.TrimSuffix(raw, "%mem"), 1<<20)
		if err != nil {
			return SizeSpec{}, fmt.Errorf("--size %q: %w", raw, err)
		}
		spec.MemPct = n
	case strings.HasSuffix(raw, "%disk"):
		n, err := parsePct(strings.TrimSuffix(raw, "%disk"), 100)
		if err != nil {
			return SizeSpec{}, fmt.Errorf("--size %q: %w", raw, err)
		}
		spec.DiskPct = n
	default:
		n, err := encoding.ParseByteSize(raw)
		if err != nil {
			return SizeSpec{}, fmt.Errorf("--size %q: %w", raw, err)
		}
		if n <= 0 {
			return SizeSpec{}, fmt.Errorf("--size %q must be > 0", raw)
		}
		spec.Bytes = n
	}
	return spec, nil
}

// ParseFill parses --fill N%.
func ParseFill(raw string) (SizeSpec, error) {
	raw = strings.TrimSpace(raw)
	if !strings.HasSuffix(raw, "%") {
		return SizeSpec{}, fmt.Errorf("--fill %q must be a percent like 50%%", raw)
	}
	n, err := parsePct(strings.TrimSuffix(raw, "%"), 100)
	if err != nil {
		return SizeSpec{}, fmt.Errorf("--fill %q: %w", raw, err)
	}
	return SizeSpec{Raw: "fill=" + raw, FillPct: n}, nil
}

func parsePct(raw string, maxPct int) (int, error) {
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || n <= 0 || n > maxPct {
		return 0, fmt.Errorf("percent must be 1..%d", maxPct)
	}
	return n, nil
}

// Resolve turns the spec into a byte total for a dataset under dir.
// reclaimable is the size of an existing dataset that would be replaced; a
// --fill counts it as free space.
func (s SizeSpec) Resolve(dir string, reclaimable int64) (int64, error) {
	switch {
	case s.Bytes > 0:
		return s.Bytes, nil
	case s.MemPct > 0:
		mem, err := ReadMemInfo()
		if err != nil {
			return 0, err
		}
		return mem.Total / 100 * int64(s.MemPct), nil
	case s.DiskPct > 0:
		fs, err := StatFS(dir)
		if err != nil {
			return 0, err
		}
		return fs.Capacity / 100 * int64(s.DiskPct), nil
	case s.FillPct > 0:
		fs, err := StatFS(dir)
		if err != nil {
			return 0, err
		}
		used := fs.Capacity - fs.Available - reclaimable
		n := fs.Capacity/100*int64(s.FillPct) - used
		if n <= 0 {
			return 0, fmt.Errorf("--fill %d%%: the filesystem is already %s used of %s",
				s.FillPct, encoding.HumanBytes(used), encoding.HumanBytes(fs.Capacity))
		}
		return n, nil
	}
	return 0, fmt.Errorf("empty size spec")
}

// MemInfo holds the /proc/meminfo values tx-bench reports and sizes against.
type MemInfo struct {
	Total     int64
	Available int64
}

// ReadMemInfo reads MemTotal and MemAvailable.
func ReadMemInfo() (MemInfo, error) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return MemInfo{}, fmt.Errorf("read memory size: %w", err)
	}
	defer f.Close()
	var mi MemInfo
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 2 {
			continue
		}
		kib, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			continue
		}
		switch fields[0] {
		case "MemTotal:":
			mi.Total = kib * 1024
		case "MemAvailable:":
			mi.Available = kib * 1024
		}
	}
	if mi.Total == 0 {
		return MemInfo{}, fmt.Errorf("MemTotal missing from /proc/meminfo")
	}
	return mi, nil
}

// FSInfo describes the filesystem holding a path.
type FSInfo struct {
	Capacity  int64
	Available int64
	Type      string
	Device    string
}

// IgnoresFadvise reports filesystems whose cache does not honor
// posix_fadvise, so --cache-warm cannot be enforced on them.
func (f FSInfo) IgnoresFadvise() bool { return f.Type == "zfs" }

// StatFS reports capacity, free space, type, and device for path's
// filesystem.
func StatFS(path string) (FSInfo, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return FSInfo{}, fmt.Errorf("statfs %s: %w", path, err)
	}
	info := FSInfo{
		Capacity:  int64(st.Blocks) * st.Bsize,
		Available: int64(st.Bavail) * st.Bsize,
	}
	info.Type, info.Device = mountOf(path)
	return info, nil
}

// mountOf finds the longest /proc/mounts mount point containing path.
func mountOf(path string) (fsType, device string) {
	data, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return "", ""
	}
	best := -1
	for _, line := range strings.Split(string(data), "\n") {
		f := strings.Fields(line)
		if len(f) < 3 {
			continue
		}
		mnt := f[1]
		if mnt == path || mnt == "/" || strings.HasPrefix(path, strings.TrimSuffix(mnt, "/")+"/") {
			if len(mnt) > best {
				best = len(mnt)
				device, fsType = f[0], f[2]
			}
		}
	}
	return fsType, device
}
