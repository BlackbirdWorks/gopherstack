// Command paritylint checks the gopherstack-anjf invariant: fix status for
// every services/<svc>/PARITY.md lives in exactly one place, items_still_open:.
// See .claude/memories/parity-principles.md rule 6.
//
// It fails when:
//
//	(a) an open-item entry (items_still_open:, or a not-yet-migrated legacy
//	    gaps:/residual_gaps:) names an op/field a LATER dated part of the
//	    same file already marks fixed (checkResolvedElsewhere)
//	(b) a body paragraph uses this corpus's "not fixed"/"disclosed"/
//	    "deferred" phrasing for a named item absent from items_still_open,
//	    with no evidence anywhere else in the file that it was resolved
//	    (checkUndisclosedOpenItem)
//	(c) items_still_open: is missing entirely (checkMissingItemsStillOpen)
//
// (a) and (b) reuse the same token/marker heuristic cmd/paritymigrate's own
// migration used. All three checks run and are always printed, but ONLY (c)
// -- missing-items-still-open -- affects the exit code. Hand-checking every
// (a) and (b) finding across the real corpus (this migration's own report,
// filed alongside gopherstack-anjf) measured both too imprecise to gate a
// build on: of the 13 (a) candidates found immediately after migration, 8
// were confirmed false positives (a "fixed" citation for an incidental
// comparison token, never the item's actual subject) after hand-reading
// each one -- a ~62% false-positive rate on this corpus. (b) fared far
// worse: 1250 findings, because this corpus's body prose is dense with the
// same "not fixed"/"disclosed" vocabulary describing history, sub-details
// already covered by a differently-worded items_still_open entry, or
// comparisons to other operations. Gating on either would make this check
// impossible to keep green without either weakening the heuristic (hiding
// real hits) or forcing bulk suppressions -- the same failure mode this
// tool exists to prevent, just moved one level up. (c) is 100% mechanical
// with no false positives observed, so it alone gates. (a)/(b) remain
// printed so a human can triage and file follow-ups (cmd/staleclaims
// remains the standing tool for that -- this is not a replacement for it).
//
// Usage:
//
//	go run ./cmd/paritylint                # report to stdout
//	go run ./cmd/paritylint -json out.json # also write full finding detail as JSON
//
// Exit codes: 0 no missing-items-still-open finding, 1 a run error, 2 at
// least one missing-items-still-open finding. (a)/(b) findings are always
// printed but never change the exit code -- see the false-positive rates
// above.
package main

import (
	"flag"
	"fmt"
	"os"
)

const (
	exitClean    = 0
	exitRunError = 1
	exitFindings = 2
)

func main() {
	dir := flag.String("dir", "services", "path to the services directory")
	jsonPath := flag.String("json", "", "also write full finding detail to this path as JSON")
	flag.Parse()

	findings, err := run(*dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "paritylint:", err)
		os.Exit(exitRunError)
	}

	if *jsonPath != "" {
		if writeErr := writeJSON(*jsonPath, findings); writeErr != nil {
			fmt.Fprintln(os.Stderr, "paritylint:", writeErr)
			os.Exit(exitRunError)
		}
	}

	printReport(os.Stdout, findings)

	if gatingCount(findings) > 0 {
		os.Exit(exitFindings)
	}

	os.Exit(exitClean)
}

// gatingCount counts only findings that fail the build -- see main's
// package doc comment for why (a)/(b) are advisory-only.
func gatingCount(findings []finding) int {
	n := 0

	for _, f := range findings {
		if f.Rule == ruleMissingItemsStillOpen {
			n++
		}
	}

	return n
}

func run(dir string) ([]finding, error) {
	manifests, err := discoverManifests(dir)
	if err != nil {
		return nil, err
	}

	var findings []finding

	for _, m := range manifests {
		findings = append(findings, checkMissingItemsStillOpen(m)...)
		findings = append(findings, checkResolvedElsewhere(m)...)
		findings = append(findings, checkUndisclosedOpenItem(m)...)
	}

	return findings, nil
}
