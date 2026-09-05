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

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")

	if encErr := enc.Encode(findings); encErr != nil {
		_ = f.Close()

		return encErr
	}

	if closeErr := f.Close(); closeErr != nil {
		return fmt.Errorf("close %s: %w", path, closeErr)
	}

	return nil
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
	if f.Kind == kindConfident {
		fmt.Fprintf(
			os.Stdout,
			"%s:%d  %s: field %q is plain, guarded by a zero-check, but the real SDK member %s.%s is %s\n",
			f.File, f.Line, f.Op, f.Field, f.Op+"Input", f.SDKField, f.SDKType,
		)

		return
	}

	fmt.Fprintf(
		os.Stdout,
		"%s:%d  %s: field %q is plain but the real SDK member %s.%s is %s (no zero-guard found)\n",
		f.File, f.Line, f.Op, f.Field, f.Op+"Input", f.SDKField, f.SDKType,
	)
}
