package report

import (
	"fmt"
	"io"
	"strings"
)

// Col is one fixed-width table column. Numbers are right-aligned; text
// columns set Left.
type Col struct {
	Name  string
	Width int
	Left  bool
}

// Table writes fixed-width rows, so streamed rows line up with a header
// printed long before them. Cells past the last column are free-form
// metadata, appended after two spaces.
type Table struct {
	W      io.Writer
	Indent string
	Cols   []Col
}

// Header writes the column names.
func (t Table) Header() {
	names := make([]string, len(t.Cols))
	for i, c := range t.Cols {
		names[i] = c.Name
	}
	t.Row(names...)
}

// Row writes one row. Empty cells print as "-"; a cell wider than its
// column pushes the rest of the row right rather than being cut.
func (t Table) Row(cells ...string) {
	var b strings.Builder
	b.WriteString(t.Indent)
	for i, c := range t.Cols {
		cell := "-"
		if i < len(cells) && cells[i] != "" {
			cell = cells[i]
		}
		if i > 0 {
			b.WriteString("  ")
		}
		if c.Left && i == len(t.Cols)-1 && len(cells) <= len(t.Cols) {
			b.WriteString(cell) // no trailing padding at the end of a line
		} else if c.Left {
			fmt.Fprintf(&b, "%-*s", c.Width, cell)
		} else {
			fmt.Fprintf(&b, "%*s", c.Width, cell)
		}
	}
	if len(cells) > len(t.Cols) {
		for _, extra := range cells[len(t.Cols):] {
			if extra != "" {
				b.WriteString("  ")
				b.WriteString(extra)
			}
		}
	}
	fmt.Fprintln(t.W, strings.TrimRight(b.String(), " "))
}
