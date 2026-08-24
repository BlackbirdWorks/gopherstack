package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

const disclaimer = `NOTE ON READING THIS REPORT
Every row below is a CANDIDATE, not a verdict -- read the manifest before
touching it. "structured"/"prose" rows found a later, unnegated fixed-marker
for the same named token; "symbol" rows found a backtick-quoted identifier,
named near self-referential language ("the old X function"), that no longer
appears anywhere in the service's own .go source. A real gap that is
genuinely still open, or a claim already amended/cross-referenced in place,
should never survive a human read of the cited lines -- confirm both before
editing the manifest.
`

func printReport(w io.Writer, findings []finding) {
	fmt.Fprintln(w, "staleclaims: open-list/prose claims contradicted elsewhere in the same manifest")
	fmt.Fprintln(w)

	if len(findings) == 0 {
		fmt.Fprintln(w, "no candidates found")

		return
	}

	for _, f := range findings {
		printFinding(w, f)
	}

	fmt.Fprintln(w)
	fmt.Fprint(w, disclaimer)
	fmt.Fprintf(w, "\ntotal candidates: %d\n", len(findings))
}

func printFinding(w io.Writer, f finding) {
	fmt.Fprintf(w, "[%s] %s (%s:%d, field %q)\n", f.Check, f.Service, f.Path, f.ClaimLine, f.Field)
	fmt.Fprintf(w, "  token: %s\n", f.Token)
	fmt.Fprintf(w, "  claim: %s\n", f.ClaimText)

	switch f.Check {
	case "symbol":
		fmt.Fprintf(w, "  %s\n", f.Note)
	default:
		fmt.Fprintf(w, "  fix (line %d): %s\n", f.FixLine, f.FixText)
	}

	fmt.Fprintln(w)
}

func writeJSON(path string, findings []finding) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")

	if encErr := enc.Encode(findings); encErr != nil {
		return fmt.Errorf("encode json: %w", encErr)
	}

	return nil
}
