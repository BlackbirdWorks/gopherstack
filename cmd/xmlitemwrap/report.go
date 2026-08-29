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
	label := "double-wrap <item><item>...</item></item>"
	if f.Variant == variantNamedChild {
		label = fmt.Sprintf("named-child <item><%s>...</%s></item>", f.Elem, f.Elem)
	}

	fmt.Fprintf(os.Stdout, "%s:%d  %s  %s\n", f.File, f.Line, f.Path, label)
}
