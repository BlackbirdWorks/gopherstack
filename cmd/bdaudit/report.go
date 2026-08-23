package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)

type report struct {
	Range          string           `json:"range"`
	NotClosed      []trailerFinding `json:"notClosed"`
	Typos          []trailerFinding `json:"typos"`
	Suspicion      []suspicionRow   `json:"suspicion,omitempty"`
	CommitsScanned int              `json:"commitsScanned"`
}

func writeJSONReport(path string, rep report) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")

	if encErr := enc.Encode(rep); encErr != nil {
		return fmt.Errorf("encode json: %w", encErr)
	}

	return nil
}

func printReport(w io.Writer, rep report) {
	fmt.Fprintf(w, "bdaudit: %d commits scanned in range %q\n\n", rep.CommitsScanned, rep.Range)

	printTrailerSection(w, "TRAILER MISMATCH (commit says closed, bd disagrees)", rep.NotClosed, true)
	printTrailerSection(w, "TYPO'D TRAILER ID (id does not exist in bd)", rep.Typos, false)
	printSuspicionSection(w, rep.Suspicion)
}

func printTrailerSection(w io.Writer, header string, findings []trailerFinding, showStatus bool) {
	fmt.Fprintf(w, "== %s (%d) ==\n", header, len(findings))
	if len(findings) == 0 {
		fmt.Fprintln(w, "(none)")
		fmt.Fprintln(w)

		return
	}

	for _, f := range findings {
		commits := strings.Join(f.Commits, ", ")
		if showStatus {
			fmt.Fprintf(w, "  %-20s status=%-12s commits: %s\n", f.IssueID, f.Status, commits)
		} else {
			fmt.Fprintf(w, "  %-20s commits: %s\n", f.IssueID, commits)
		}
	}
	fmt.Fprintln(w)
}

func printSuspicionSection(w io.Writer, rows []suspicionRow) {
	fmt.Fprintf(w, "== SUSPICION LIST (%d) -- heuristic, human review required, nothing auto-closed ==\n", len(rows))
	if len(rows) == 0 {
		fmt.Fprintln(w, "(none)")

		return
	}

	for _, r := range rows {
		fmt.Fprintf(w, "  [%d] %-20s %-28s %s\n", r.Score, r.IssueID, r.Signal, r.Title)
		fmt.Fprintf(w, "        %s\n", r.Detail)
	}
}
