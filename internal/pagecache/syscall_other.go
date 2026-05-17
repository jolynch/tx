//go:build !linux

package pagecache

const touchSupported = false

var (
	loadResidencyFn = loadResidency
	touchPagesFn    = touchPages
)

func loadResidency(path string) ([]byte, int, error) {
	return nil, 0, ErrUnsupported
}

func touchPages(path string, bits []byte, numPages int) error {
	return ErrUnsupported
}

// systemMemoryBytes is unsupported on this platform. SystemPageBudget
// returns -1 (treat as unlimited).
func systemMemoryBytes() (int64, error) {
	return 0, ErrUnsupported
}
