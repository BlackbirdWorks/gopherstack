package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

const jsonFileMode = 0o644

// jsonResult is the -json output shape.
type jsonResult struct {
	Service string   `json:"service"`
	Path    string   `json:"path"`
	DocSlug string   `json:"docSlug,omitempty"`
	Issues  []string `json:"issues,omitempty"`
}

func writeJSON(path string, results []result) error {
	out := make([]jsonResult, 0, len(results))
	for _, r := range results {
		out = append(out, jsonResult{
			Service: r.service,
			Path:    r.path,
			DocSlug: r.docSlug,
			Issues:  r.issues,
		})
	}

	data, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal results: %w", err)
	}

	if writeErr := os.WriteFile(path, data, jsonFileMode); writeErr != nil {
		return fmt.Errorf("write %s: %w", path, writeErr)
	}

	return nil
}

// printReport writes one line per issue found, then a summary line. A clean
// run prints only the summary.
func printReport(w io.Writer, results []result) {
	totalIssues, badManifests := 0, 0
	for _, r := range results {
		if len(r.issues) == 0 {
			continue
		}

		badManifests++
		for _, issue := range r.issues {
			fmt.Fprintf(w, "%s: %s\n", r.path, issue)
			totalIssues++
		}
	}

	if totalIssues == 0 {
		fmt.Fprintf(w, "parityfmtcheck: %d manifests checked, front-matter checks out clean\n", len(results))

		return
	}

	fmt.Fprintf(w, "parityfmtcheck: %d issue(s) across %d manifest(s) (see above)\n", totalIssues, badManifests)
}
