package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
)

type benchResult struct {
	name        string
	iters       int64
	nsPerOp     float64
	bytesPerOp  int64
	allocsPerOp int64
	mibPerSec   float64
	extra       map[string]float64
}

func runReport(args []string) error {
	path := "bench/results/latest.txt"
	if len(args) > 0 {
		path = args[0]
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	results, err := parseBenchOutput(f)
	if err != nil {
		return err
	}
	if len(results) == 0 {
		return fmt.Errorf("no benchmark lines found in %s", path)
	}
	return writeTable(os.Stdout, results)
}

func parseBenchOutput(r io.Reader) ([]benchResult, error) {
	var out []benchResult
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 1<<20)
	for scanner.Scan() {
		line := strings.TrimRight(scanner.Text(), "\r\n")
		if !strings.HasPrefix(line, "Benchmark") {
			continue
		}
		if res, ok := parseBenchLine(line); ok {
			out = append(out, res)
		}
	}
	return out, scanner.Err()
}

func parseBenchLine(line string) (benchResult, bool) {
	fields := strings.Fields(line)
	if len(fields) < 3 {
		return benchResult{}, false
	}
	name := fields[0]
	if i := strings.LastIndex(name, "-"); i > 0 {
		if _, err := strconv.Atoi(name[i+1:]); err == nil {
			name = name[:i]
		}
	}
	iters, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return benchResult{}, false
	}
	res := benchResult{name: name, iters: iters, extra: map[string]float64{}}
	for i := 2; i+1 < len(fields); i += 2 {
		val, err := strconv.ParseFloat(fields[i], 64)
		if err != nil {
			continue
		}
		switch fields[i+1] {
		case "ns/op":
			res.nsPerOp = val
		case "B/op":
			res.bytesPerOp = int64(val)
		case "allocs/op":
			res.allocsPerOp = int64(val)
		case "MB/s", "MiB/s":
			res.mibPerSec = val
		default:
			res.extra[fields[i+1]] = val
		}
	}
	return res, true
}

func writeTable(w io.Writer, results []benchResult) error {
	sort.SliceStable(results, func(i, j int) bool {
		return results[i].name < results[j].name
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "BENCHMARK\tITERS\tNS/OP\tMiB/s\tB/OP\tALLOCS/OP\tEXTRA")
	for _, r := range results {
		extra := ""
		if len(r.extra) > 0 {
			parts := make([]string, 0, len(r.extra))
			for k, v := range r.extra {
				parts = append(parts, fmt.Sprintf("%s=%.2f", k, v))
			}
			sort.Strings(parts)
			extra = strings.Join(parts, " ")
		}
		fmt.Fprintf(tw, "%s\t%d\t%.1f\t%.1f\t%d\t%d\t%s\n",
			r.name, r.iters, r.nsPerOp, r.mibPerSec,
			r.bytesPerOp, r.allocsPerOp, extra)
	}
	return tw.Flush()
}
