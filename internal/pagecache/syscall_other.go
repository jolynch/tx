//go:build !linux

package pagecache

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
