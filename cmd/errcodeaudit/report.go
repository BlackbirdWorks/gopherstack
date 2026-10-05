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
	var confident, review, notCode, recorded []finding

	for _, f := range findings {
		switch {
		case f.Recorded:
			recorded = append(recorded, f)
		case f.Kind != "":
			notCode = append(notCode, f)
		case f.Confident:
			confident = append(confident, f)
		default:
			review = append(review, f)
		}
	}

	fmt.Fprintf(
		os.Stdout,
		"# %d findings: %d confident, %d needs review, %d not an error code, %d recorded in PARITY.md\n\n",
		len(findings), len(confident), len(review), len(notCode), len(recorded),
	)

	printSection("CONFIDENT", confident)
	printSection("NEEDS REVIEW", review)
	printSection("NOT AN ERROR CODE", notCode)
	printSection("RECORDED IN PARITY.md", recorded)
}

func printSection(title string, fs []finding) {
	if len(fs) == 0 {
		return
	}

	fmt.Fprintln(os.Stdout, "## "+title)

	for _, f := range fs {
		printFinding(f)
	}

	fmt.Fprintln(os.Stdout)
}

func printFinding(f finding) {
	kind := ""
	if f.Kind != "" {
		kind = " <" + f.Kind + ">"
	}

	fmt.Fprintf(os.Stdout, "%s:%d  %s  [%s]%s  %s\n", f.File, f.Line, f.Code, f.Mechanism, kind, f.Reason)
}
