package filexfer

import (
	"fmt"
	"strings"
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
func ResolveProgressTargets(paths, formats []string) ([]ProgressTarget, error) {
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
	targets := make([]ProgressTarget, len(paths))
	for i, p := range paths {
		if p == "" {
			return nil, fmt.Errorf("--progress-path must not be empty")
		}
		f := ProgressFormatJSON
		if len(formats) == 1 {
			f = ProgressFormat(formats[0])
		} else if i < len(formats) {
			f = ProgressFormat(formats[i])
		}
		switch f {
		case ProgressFormatJSON, ProgressFormatInt:
		default:
			return nil, fmt.Errorf("unsupported --progress-format %q (supported: json, int)", f)
		}
		targets[i] = ProgressTarget{Path: p, Format: f}
	}
	return targets, nil
}
