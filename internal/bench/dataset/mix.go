// Package dataset builds, imports, and verifies tx-bench datasets: the
// deterministic generated trees and user-supplied trees that tx-bench serves,
// the files.tsv oracle that lists them, and the page-cache state prep sets
// up. See docs/bench/DATASET.md.
package dataset

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/jolynch/tx/internal/filexfer/encoding"
)

// SilesiaNames lists the Silesia corpus files in their canonical order, the
// order a bare "silesia" source concatenates them in.
var SilesiaNames = []string{
	"dickens", "mozilla", "mr", "nci", "ooffice", "osdb",
	"reymont", "samba", "sao", "webster", "xml", "x-ray",
}

// Profiles maps each --profile name to the mix it stands for.
var Profiles = map[string]string{
	"mixed":        "rand=40%@1GiB,silesia:osdb+dickens+nci+xml=30%@16MiB,rand=30%@4KiB..256KiB",
	"small":        "rand=100%@4KiB..64KiB",
	"large":        "rand=100%@2GiB",
	"random":       "rand=100%@16MiB",
	"compressible": "silesia=100%@16MiB",
}

// ProfileNames is the --profile values in help order.
var ProfileNames = []string{"mixed", "small", "large", "random", "compressible"}

const (
	DefaultProfile = "mixed"
	DefaultSize    = "10GiB"
	DefaultSeed    = 1
)

const (
	SourceRand    = "rand"
	SourceSilesia = "silesia"
)

// Source is where a part's bytes come from.
type Source struct {
	Kind string
	// Names are the Silesia corpus files in the order given; empty means
	// every file in SilesiaNames order.
	Names []string
}

// CorpusNames returns the Silesia files a silesia source concatenates.
func (s Source) CorpusNames() []string {
	if s.Kind != SourceSilesia {
		return nil
	}
	if len(s.Names) == 0 {
		return SilesiaNames
	}
	return s.Names
}

func (s Source) String() string {
	if len(s.Names) == 0 {
		return s.Kind
	}
	return s.Kind + ":" + strings.Join(s.Names, "+")
}

// Part is one comma-separated element of a mix.
type Part struct {
	Source   Source
	SharePct int
	// Lo and Hi bound file sizes. Lo == Hi is a fixed size; otherwise sizes
	// are drawn log-uniformly from [Lo, Hi].
	Lo, Hi int64
}

// Fixed reports whether the part uses a single file size.
func (p Part) Fixed() bool { return p.Lo == p.Hi }

// Sizes renders the SIZES clause.
func (p Part) Sizes() string {
	if p.Fixed() {
		return FormatSize(p.Lo)
	}
	return FormatSize(p.Lo) + ".." + FormatSize(p.Hi)
}

func (p Part) String() string {
	return fmt.Sprintf("%s=%d%%@%s", p.Source, p.SharePct, p.Sizes())
}

// Mix is an ordered list of parts whose shares sum to 100%.
type Mix []Part

// String renders the canonical form of the mix. Two mixes that parse to the
// same parts render identically, whatever units they were written in.
func (m Mix) String() string {
	parts := make([]string, len(m))
	for i, p := range m {
		parts[i] = p.String()
	}
	return strings.Join(parts, ",")
}

// ResolveMix picks the mix for a profile name or an explicit mix; an explicit
// mix overrides the profile, and neither selects DefaultProfile.
func ResolveMix(profile, mix string) (Mix, error) {
	if strings.TrimSpace(mix) != "" {
		return ParseMix(mix)
	}
	if profile == "" {
		profile = DefaultProfile
	}
	raw, ok := Profiles[profile]
	if !ok {
		return nil, fmt.Errorf("unknown --profile %q (supported: %s)", profile, strings.Join(ProfileNames, ", "))
	}
	return ParseMix(raw)
}

// ParseMix parses the MIX grammar:
//
//	MIX    := PART ("," PART)*
//	PART   := SOURCE "=" SHARE "@" SIZES
//	SOURCE := "rand" | "silesia" [":" NAME ("+" NAME)*]
//	SHARE  := INT "%"
//	SIZES  := BYTES | BYTES ".." BYTES
func ParseMix(raw string) (Mix, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("empty mix")
	}
	var mix Mix
	total := 0
	for _, elem := range strings.Split(raw, ",") {
		p, err := parsePart(strings.TrimSpace(elem))
		if err != nil {
			return nil, fmt.Errorf("mix part %q: %w", elem, err)
		}
		total += p.SharePct
		mix = append(mix, p)
	}
	if total != 100 {
		return nil, fmt.Errorf("mix shares sum to %d%%, must be 100%%", total)
	}
	return mix, nil
}

func parsePart(raw string) (Part, error) {
	srcRaw, rest, ok := strings.Cut(raw, "=")
	if !ok {
		return Part{}, fmt.Errorf("missing '='")
	}
	shareRaw, sizesRaw, ok := strings.Cut(rest, "@")
	if !ok {
		return Part{}, fmt.Errorf("missing '@'")
	}
	src, err := parseSource(strings.TrimSpace(srcRaw))
	if err != nil {
		return Part{}, err
	}
	shareRaw = strings.TrimSpace(shareRaw)
	if !strings.HasSuffix(shareRaw, "%") {
		return Part{}, fmt.Errorf("share %q must be a percent like 40%%", shareRaw)
	}
	share, err := strconv.Atoi(strings.TrimSuffix(shareRaw, "%"))
	if err != nil || share <= 0 || share > 100 {
		return Part{}, fmt.Errorf("share %q must be 1%%..100%%", shareRaw)
	}
	p := Part{Source: src, SharePct: share}
	loRaw, hiRaw, isRange := strings.Cut(strings.TrimSpace(sizesRaw), "..")
	if p.Lo, err = parsePositiveSize(loRaw); err != nil {
		return Part{}, err
	}
	p.Hi = p.Lo
	if isRange {
		if p.Hi, err = parsePositiveSize(hiRaw); err != nil {
			return Part{}, err
		}
		if p.Hi < p.Lo {
			return Part{}, fmt.Errorf("size range %s has hi < lo", sizesRaw)
		}
	}
	return p, nil
}

func parseSource(raw string) (Source, error) {
	kind, namesRaw, hasNames := strings.Cut(raw, ":")
	switch kind {
	case SourceRand:
		if hasNames {
			return Source{}, fmt.Errorf("rand takes no corpus names")
		}
		return Source{Kind: SourceRand}, nil
	case SourceSilesia:
		if !hasNames {
			return Source{Kind: SourceSilesia}, nil
		}
		names := strings.Split(namesRaw, "+")
		for _, n := range names {
			if !slices.Contains(SilesiaNames, n) {
				return Source{}, fmt.Errorf("unknown silesia file %q (known: %s)", n, strings.Join(SilesiaNames, ", "))
			}
		}
		return Source{Kind: SourceSilesia, Names: names}, nil
	default:
		return Source{}, fmt.Errorf("unknown source %q (supported: rand, silesia[:a+b])", kind)
	}
}

func parsePositiveSize(raw string) (int64, error) {
	n, err := encoding.ParseByteSize(strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("size %q: %w", raw, err)
	}
	if n <= 0 {
		return 0, fmt.Errorf("size %q must be > 0", raw)
	}
	return n, nil
}

// FormatSize renders n in the largest binary unit that divides it exactly,
// so the result parses back to n: 1073741824 is "1GiB", 1000 is "1000B".
func FormatSize(n int64) string {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB"}
	i := 0
	for i < len(units)-1 && n != 0 && n%1024 == 0 {
		n /= 1024
		i++
	}
	return strconv.FormatInt(n, 10) + units[i]
}
