package harness

import (
	"regexp"
	"strings"
)

// txFlag is one occurrence of a flag in TX_ARGS.
type txFlag struct {
	name  string // without dashes
	value string
	has   bool // a value was present (inline or the next token)
}

// passThrough is the classified TX_ARGS of one forked tx command. Arguments
// are kept verbatim; tx-bench only reads the few flags that change what it
// does and rejects the ones it manages itself.
type passThrough struct {
	args  []string
	flags []txFlag
}

var numberLike = regexp.MustCompile(`^-[0-9.]`)

// parsePassThrough scans args in every form Go's flag package accepts:
// -x v, --x v, -x=v, --x=v. TX_ARGS carry no positionals (tx-bench supplies
// them), so a token that does not look like a flag is the value of the flag
// before it; a lone number such as -1 is a value too.
func parsePassThrough(args []string) passThrough {
	pt := passThrough{args: append([]string(nil), args...)}
	for i := 0; i < len(args); i++ {
		a := args[i]
		if !strings.HasPrefix(a, "-") || a == "-" || a == "--" || numberLike.MatchString(a) {
			continue
		}
		name := strings.TrimLeft(a, "-")
		f := txFlag{name: name}
		if n, v, ok := strings.Cut(name, "="); ok {
			f.name, f.value, f.has = n, v, true
		} else if i+1 < len(args) && (!strings.HasPrefix(args[i+1], "-") || numberLike.MatchString(args[i+1]) || args[i+1] == "-") {
			f.value, f.has = args[i+1], true
			i++
		}
		pt.flags = append(pt.flags, f)
	}
	return pt
}

// values returns every value given for any of names.
func (pt passThrough) values(names ...string) []string {
	var out []string
	for _, f := range pt.flags {
		for _, n := range names {
			if f.name == n {
				out = append(out, f.value)
			}
		}
	}
	return out
}

// has reports whether any of names appears.
func (pt passThrough) has(names ...string) bool {
	for _, f := range pt.flags {
		for _, n := range names {
			if f.name == n {
				return true
			}
		}
	}
	return false
}

// boolSet reports whether a boolean flag is on: present without a value, or
// with a true value.
func (pt passThrough) boolSet(name string) bool {
	on := false
	for _, f := range pt.flags {
		if f.name != name {
			continue
		}
		switch strings.ToLower(f.value) {
		case "", "true", "1", "t":
			on = true
		case "false", "0", "f":
			on = false
		default:
			// "--skip-write somevalue" cannot happen for a bool flag in
			// tx's parser; treat it as set.
			on = true
		}
	}
	return on
}

// managedFlag maps a tx flag tx-bench sets itself to the tx-bench option
// that controls it.
type managedFlag struct {
	names  []string
	option string
}

// checkManaged rejects TX_ARGS that set a flag tx-bench manages. Progress
// targets are managed only while tracing; otherwise they pass through.
func (pt passThrough) checkManaged(managed []managedFlag) error {
	for _, m := range managed {
		if pt.has(m.names...) {
			return usageErrorf("-%s is set by tx-bench; use tx-bench's %s instead", m.names[len(m.names)-1], m.option)
		}
	}
	return nil
}

func sendManaged(tracing bool) []managedFlag {
	m := []managedFlag{
		{[]string{"listen"}, "-l/--listen"},
		{[]string{"exit-with"}, "nothing (tx-bench always sets --exit-with stdin, tying tx to its own lifetime)"},
		{[]string{"stats"}, "-o/--metrics (tx --stats is always on)"},
		{[]string{"trace"}, "--go-trace"},
	}
	if tracing {
		m = append(m, managedFlag{[]string{"p", "progress-path"}, "--trace"}, managedFlag{[]string{"f", "progress-format"}, "--trace"})
	}
	return m
}

func recvManaged(tracing bool) []managedFlag {
	m := []managedFlag{
		{[]string{"stats"}, "-o/--metrics (tx --stats is always on)"},
		{[]string{"trace"}, "--go-trace"},
	}
	if tracing {
		m = append(m, managedFlag{[]string{"p", "progress-path"}, "--trace"}, managedFlag{[]string{"f", "progress-format"}, "--trace"})
	}
	return m
}

// fetchArgs are the TX_ARGS that also apply to the small tx recv get
// fetches of sender state: --encrypt, -k, and -t.
func (pt passThrough) fetchArgs() []string {
	var out []string
	for _, f := range pt.flags {
		switch f.name {
		case "encrypt", "k", "keys", "t", "auth-token":
			out = append(out, "--"+f.name+"="+f.value)
		}
	}
	return out
}

// label describes a few behavior-relevant pass-through settings for the
// report header.
func (pt passThrough) setting(name, def string, aliases ...string) string {
	vals := pt.values(append([]string{name}, aliases...)...)
	if len(vals) == 0 {
		return def
	}
	return vals[len(vals)-1]
}
