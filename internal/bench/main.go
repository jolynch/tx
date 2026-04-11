package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "generate":
		if err := runGenerate(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "generate:", err)
			os.Exit(1)
		}
	case "report":
		if err := runReport(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "report:", err)
			os.Exit(1)
		}
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: bench <command> [args]

commands:
  generate [-source rand|silesia:<csv>] <spec> [<spec>...]
      Generate benchmark files. Spec form: <outdir>:<count>@<size>
      Example: generate bench/data/src:100@10MiB
      Example: generate -source silesia:osdb,nci bench/data/src:10@100MiB

  report [path]
      Parse raw go test -bench output and print a summary table.
      Default path: bench/results/latest.txt`)
}
