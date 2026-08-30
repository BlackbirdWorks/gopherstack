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
	if f.Kind == kindReuse {
		fmt.Fprintf(
			os.Stdout,
			"%s:%d  key=%q(%s) reused for key=%q(%s) at line %d -- different enums, different members\n",
			f.File, f.Line, f.Key, f.Enum, f.OtherKey, f.OtherEnum, f.OtherLine,
		)

		return
	}

	if f.Kind == kindAmbiguousKey {
		fmt.Fprintf(
			os.Stdout,
			"%s:%d  key=%q value=%q is not a member of every candidate enum for this key: %s\n",
			f.File, f.Line, f.Key, f.Value, f.Enum,
		)

		return
	}

	if f.Kind == kindPhantomField {
		fmt.Fprintf(
			os.Stdout,
			"%s:%d  key=%q value=%q assigned on %s, but the real wire type has no such field -- dead or fabricated\n",
			f.File, f.Line, f.Key, f.Value, f.Enum,
		)

		return
	}

	fmt.Fprintf(
		os.Stdout,
		"%s:%d  key=%q value=%q is not a member of %s\n",
		f.File, f.Line, f.Key, f.Value, f.Enum,
	)
}
