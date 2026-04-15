package cliflags

import (
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/jolynch/tx/internal/filexfer"
)

// StringSliceFlag implements flag.Value for a repeatable string flag.
type StringSliceFlag struct {
	Values *[]string
}

func (f *StringSliceFlag) String() string {
	if f.Values == nil || len(*f.Values) == 0 {
		return ""
	}
	return strings.Join(*f.Values, ", ")
}

func (f *StringSliceFlag) Set(val string) error {
	*f.Values = append(*f.Values, val)
	return nil
}

// ResolveProgressTargets pairs progress paths with formats.
//   - 0 formats: all paths default to json
//   - 1 format: all paths share that format
//   - N formats (== N paths): paired positionally
//   - Mismatched counts: error
func ResolveProgressTargets(paths, formats []string) ([]filexfer.ProgressTarget, error) {
	if len(paths) == 0 {
		return nil, nil
	}
	switch len(formats) {
	case 0:
	case 1:
	default:
		if len(formats) != len(paths) {
			return nil, fmt.Errorf("--progress-format count (%d) must be 1 or match --progress-path count (%d)", len(formats), len(paths))
		}
	}
	targets := make([]filexfer.ProgressTarget, len(paths))
	for i, p := range paths {
		if p == "" {
			return nil, fmt.Errorf("--progress-path must not be empty")
		}
		f := filexfer.ProgressFormatJSON
		if len(formats) == 1 {
			f = filexfer.ProgressFormat(formats[0])
		} else if i < len(formats) {
			f = filexfer.ProgressFormat(formats[i])
		}
		switch f {
		case filexfer.ProgressFormatJSON, filexfer.ProgressFormatInt:
		default:
			return nil, fmt.Errorf("unsupported --progress-format %q (supported: json, int)", f)
		}
		targets[i] = filexfer.ProgressTarget{Path: p, Format: f}
	}
	return targets, nil
}

// flagEntry records one option for combined help rendering.
type flagEntry struct {
	short string // "s", "v" — empty for long-only flags
	long  string // "source-directory", "verbose" — empty for short-only flags
	typ   string // "string", "int" — empty for bool flags
	usage string
	def   string
}

// Flags wraps flag.FlagSet and tracks short/long pairs so they can be
// printed combined ("-s, --source-directory string   description") rather
// than as two separate lines.
type Flags struct {
	fs    *flag.FlagSet
	pairs []flagEntry
}

const helpWrapWidth = 88

func New(name string) *Flags {
	return &Flags{fs: flag.NewFlagSet(name, flag.ContinueOnError)}
}

func (c *Flags) SetOutput(w io.Writer)     { c.fs.SetOutput(w) }
func (c *Flags) Parse(args []string) error { return c.fs.Parse(args) }
func (c *Flags) Arg(i int) string          { return c.fs.Arg(i) }
func (c *Flags) Args() []string            { return c.fs.Args() }
func (c *Flags) NArg() int                 { return c.fs.NArg() }
func (c *Flags) Visit(fn func(*flag.Flag)) { c.fs.Visit(fn) }
func (c *Flags) FlagSet() *flag.FlagSet    { return c.fs }

// StringVar registers a string flag. Pass short="" for long-only, long="" for short-only.
func (c *Flags) StringVar(p *string, short, long, defVal, usage string) {
	if short != "" {
		c.fs.StringVar(p, short, defVal, usage)
	}
	if long != "" {
		c.fs.StringVar(p, long, defVal, usage)
	}
	c.pairs = append(c.pairs, flagEntry{short: short, long: long, typ: "string", usage: usage, def: strconv.Quote(defVal)})
}

// BoolVar registers a bool flag. Pass short="" for long-only, long="" for short-only.
func (c *Flags) BoolVar(p *bool, short, long string, defVal bool, usage string) {
	if short != "" {
		c.fs.BoolVar(p, short, defVal, usage)
	}
	if long != "" {
		c.fs.BoolVar(p, long, defVal, usage)
	}
	c.pairs = append(c.pairs, flagEntry{short: short, long: long, typ: "", usage: usage, def: strconv.FormatBool(defVal)})
}

// IntVar registers an int flag. Pass short="" for long-only, long="" for short-only.
func (c *Flags) IntVar(p *int, short, long string, defVal int, usage string) {
	if short != "" {
		c.fs.IntVar(p, short, defVal, usage)
	}
	if long != "" {
		c.fs.IntVar(p, long, defVal, usage)
	}
	c.pairs = append(c.pairs, flagEntry{short: short, long: long, typ: "int", usage: usage, def: strconv.Itoa(defVal)})
}

// StringSliceVar registers a repeatable string flag. Each occurrence appends
// to the slice. Pass short="" for long-only, long="" for short-only.
func (c *Flags) StringSliceVar(p *[]string, short, long, usage string) {
	f := &StringSliceFlag{Values: p}
	if short != "" {
		c.fs.Var(f, short, usage)
	}
	if long != "" {
		c.fs.Var(f, long, usage)
	}
	c.pairs = append(c.pairs, flagEntry{short: short, long: long, typ: "string", usage: usage, def: `""`})
}

// leftCol returns the formatted left column for a flag entry:
//
//	"-s, --source-directory string"   (short + long)
//	"    --manifest string"           (long-only)
//	"-o string"                       (short-only)
//	"-v, --verbose"                   (bool, short + long)
func (e flagEntry) leftCol() string {
	var b strings.Builder
	switch {
	case e.short != "" && e.long != "":
		b.WriteString("-")
		b.WriteString(e.short)
		b.WriteString(", --")
		b.WriteString(e.long)
	case e.long != "":
		b.WriteString("    --")
		b.WriteString(e.long)
	default:
		b.WriteString("-")
		b.WriteString(e.short)
	}
	if e.typ != "" {
		b.WriteString(" ")
		b.WriteString(e.typ)
	}
	return b.String()
}

// PrintDefaults writes age-style combined option help to w.
func (c *Flags) PrintDefaults(w io.Writer) {
	if len(c.pairs) == 0 {
		return
	}
	maxW := 0
	for _, e := range c.pairs {
		if n := len(e.leftCol()); n > maxW {
			maxW = n
		}
	}
	fmt.Fprintln(w, "Options:")
	for _, e := range c.pairs {
		usage := e.usage
		if !strings.Contains(strings.ToLower(usage), "default") {
			usage = fmt.Sprintf("%s (default %s)", usage, e.def)
		}
		prefix := fmt.Sprintf("  %-*s  ", maxW, e.leftCol())
		lines := wrapHelpText(usage, helpWrapWidth-len(prefix))
		if len(lines) == 0 {
			fmt.Fprintln(w, prefix)
			continue
		}
		fmt.Fprintf(w, "%s%s\n", prefix, lines[0])
		indent := strings.Repeat(" ", len(prefix))
		for _, line := range lines[1:] {
			fmt.Fprintf(w, "%s%s\n", indent, line)
		}
	}
}

func wrapHelpText(text string, width int) []string {
	if text == "" {
		return nil
	}
	if width <= 0 {
		return []string{text}
	}

	words := strings.Fields(text)
	if len(words) == 0 {
		return nil
	}

	lines := make([]string, 0, 4)
	current := words[0]
	for _, word := range words[1:] {
		if len(current)+1+len(word) <= width {
			current += " " + word
			continue
		}
		lines = append(lines, current)
		current = word
	}
	lines = append(lines, current)
	return lines
}
