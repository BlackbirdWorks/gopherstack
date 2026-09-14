package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

func printReport(w io.Writer, findings []finding) {
	byRule := map[string]int{}

	for _, f := range findings {
		severity := "ADVISORY"
		if f.Rule == ruleMissingItemsStillOpen {
			severity = "FAIL"
		}

		fmt.Fprintf(w, "[%s][%s] %s (%s:%d)\n  %s\n", severity, f.Rule, f.Service, f.Path, f.Line, f.Text)

		if f.Extra != "" {
			fmt.Fprintf(w, "  %s\n", f.Extra)
		}

		byRule[f.Rule]++
	}

	total := len(findings)
	missing := byRule[ruleMissingItemsStillOpen]
	resolved := byRule[ruleResolvedElsewhere]
	undisclosed := byRule[ruleUndisclosedOpenItem]

	fmt.Fprintf(w, "\ntotal: %d finding(s) -- missing-items-still-open (FAIL): %d, ", total, missing)
	fmt.Fprintf(w, "resolved-elsewhere (advisory): %d, undisclosed-open-item (advisory): %d\n", resolved, undisclosed)
}

func writeJSON(path string, findings []finding) error {
	data, err := json.MarshalIndent(findings, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0o600)
}
