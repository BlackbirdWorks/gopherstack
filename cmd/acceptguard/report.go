package main

import (
	"encoding/json"
	"fmt"
	"os"
)

func writeJSON(path string, findings []finding) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")

	return enc.Encode(findings)
}

func printReport(findings []finding) {
	var confident, review []finding

	for _, f := range findings {
		if f.Confident {
			confident = append(confident, f)
		} else {
			review = append(review, f)
		}
	}

	fmt.Fprintf(
		os.Stdout,
		"# %d findings: %d confident, %d needs review\n\n",
		len(findings),
		len(confident),
		len(review),
	)

	if len(confident) > 0 {
		fmt.Fprintln(os.Stdout, "## CONFIDENT")

		for _, f := range confident {
			printFinding(f)
		}

		fmt.Fprintln(os.Stdout)
	}

	if len(review) > 0 {
		fmt.Fprintln(os.Stdout, "## NEEDS REVIEW")

		for _, f := range review {
			printFinding(f)
		}
	}
}

func printFinding(f finding) {
	switch f.Kind {
	case kindInvented:
		fmt.Fprintf(
			os.Stdout,
			"%s:%d  %s.%s: field %q (read in %s) matches no member of ANY real Input in this service's SDK module\n",
			f.File, f.Line, f.Op, f.Struct, f.Field, f.Func,
		)
	case kindFallback:
		fmt.Fprintf(
			os.Stdout,
			"%s:%d  %s.%s: field %q (read in %s) matches no real member, but is only read as a "+
				"zero-guarded fallback alias for one that is -- likely deliberate, not a bug\n",
			f.File, f.Line, f.Op, f.Struct, f.Field, f.Func,
		)
	default:
		fmt.Fprintf(
			os.Stdout,
			"%s:%d  %s.%s: field %q (read in %s) is not on %sInput but IS a real member of a different operation's Input\n",
			f.File,
			f.Line,
			f.Op,
			f.Struct,
			f.Field,
			f.Func,
			f.Op,
		)
	}
}
