package faults

import (
	"context"
	"io"
)

// Site names where a file is opened, read, or written.
type Site uint8

const (
	SiteSendOpen Site = iota + 1
	SiteSendRead
	SiteCXSUMOpen
	SiteCXSUMRead
	SiteVerifyRead
	SiteOutputWrite
	siteCount
)

// OpenErr returns the error to report instead of opening a file at site, or
// nil to open it. No open fault kinds exist yet, so it always returns nil.
func (i *Injector) OpenErr(site Site) error {
	if i == nil {
		return nil
	}
	i.counts.opens[site].Add(1)
	return nil
}

// WrapReaderAt wraps a file's reader at site. A nil injector returns r.
func (i *Injector) WrapReaderAt(ctx context.Context, site Site, r io.ReaderAt) io.ReaderAt {
	if i == nil || r == nil {
		return r
	}
	i.counts.reads[site].Add(1)
	return &readerAt{r: r}
}

type readerAt struct {
	r io.ReaderAt
}

func (r *readerAt) ReadAt(p []byte, off int64) (int, error) {
	return r.r.ReadAt(p, off)
}

// WrapWriter wraps a file's writer at site. A nil injector returns w.
func (i *Injector) WrapWriter(site Site, w io.WriteCloser) io.WriteCloser {
	if i == nil || w == nil {
		return w
	}
	i.counts.writes[site].Add(1)
	return &writer{w: w}
}

type writer struct {
	w io.WriteCloser
}

func (w *writer) Write(p []byte) (int, error) { return w.w.Write(p) }

func (w *writer) Close() error { return w.w.Close() }
