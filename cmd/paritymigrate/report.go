package main

import (
	"fmt"
	"io"
)

// reportExcerptLen bounds how much of an item's text a report row prints
// inline -- long enough to identify the item, short enough to keep the
// report scannable.
const reportExcerptLen = 110

// printReport writes a per-service, per-block dry-run/apply summary. Every
// item is relocated to items_still_open: regardless of verdict (see
// rewrite.go) -- "CANDIDATE-DROP" rows are items classify() judged resolved
// elsewhere, relocated (not deleted) unless -drop-resolved-elsewhere was
// passed, for a human to confirm and remove by hand.
func printReport(w io.Writer, results []serviceResult, applied bool) {
	totalKept, totalCandidateDrop := 0, 0

	mode := "dry run"
	if applied {
		mode = "applied"
	}

	for _, r := range results {
		if r.totalKept()+r.totalDropped()+len(r.existingOpenFindings) == 0 {
			continue
		}

		fmt.Fprintf(w, "\n=== %s (%s) ===\n", r.service, mode)

		for _, b := range r.blocks {
			printBlock(w, b)
		}

		if len(r.existingOpenFindings) > 0 {
			fmt.Fprintf(w, "  [items_still_open: pre-existing entries flagged as resolved elsewhere -- "+
				"NOT auto-removed, file for cleanup]\n")

			for _, ir := range r.existingOpenFindings {
				fmt.Fprintf(w, "    - %s\n", oneLine(ir.item.text, reportExcerptLen))
				for _, f := range ir.c.resolved {
					fmt.Fprintf(w, "      resolved by line %d: %s\n", f.line, f.excerpt)
				}
			}
		}

		totalKept += r.totalKept()
		totalCandidateDrop += r.totalDropped()
	}

	fmt.Fprintf(w, "\ntotal: %d services touched, %d items relocated (%d of them CANDIDATE-DROP: "+
		"resolved elsewhere per classify(), needs a human to confirm and remove)\n",
		countTouched(results), totalKept+totalCandidateDrop, totalCandidateDrop)
}

func countTouched(results []serviceResult) int {
	n := 0
	for _, r := range results {
		if r.totalKept()+r.totalDropped() > 0 {
			n++
		}
	}

	return n
}

func printBlock(w io.Writer, b blockResult) {
	dropped := b.dropped()
	kept := b.kept()

	if len(dropped) == 0 && len(kept) == 0 {
		return
	}

	fmt.Fprintf(w, "  [%s] %d kept, %d candidate-drop\n", b.field, len(kept), len(dropped))

	for _, ir := range dropped {
		fmt.Fprintf(w, "    CANDIDATE-DROP (%s): %s\n", ir.c.verdict, oneLine(ir.item.text, reportExcerptLen))

		for _, f := range ir.c.resolved {
			fmt.Fprintf(w, "      fixed at line %d (%s): %s\n", f.line, f.token, f.excerpt)
		}

		if ir.c.fixedText != "" {
			fmt.Fprintf(w, "      self: %s\n", ir.c.fixedText)
		}
	}

	for _, ir := range kept {
		if ir.c.verdict == verdictStillOpen {
			continue // not ambiguous, no need to spell out in the summary
		}

		fmt.Fprintf(w, "    KEEP (%s): %s\n", ir.c.verdict, oneLine(ir.item.text, reportExcerptLen))

		for _, f := range ir.c.resolved {
			fmt.Fprintf(w, "      partially resolved (%s) at line %d: %s\n", f.token, f.line, f.excerpt)
		}
	}
}

func oneLine(s string, maxLen int) string {
	r := []rune(s)
	if len(r) <= maxLen {
		return s
	}

	return string(r[:maxLen]) + "..."
}
