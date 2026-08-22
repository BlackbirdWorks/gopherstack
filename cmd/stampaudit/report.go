package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

const (
	dateDisplayLayout = "2006-01-02"

	outcomeStrResolved = "resolved"
)

// jsonResult is the -json output shape: result plus its derived,
// human-readable fields, since result's time.Time/enum values don't marshal
// usefully on their own.
type jsonResult struct {
	GapDays            *int   `json:"gapDays,omitempty"`
	SuggestedPasses    *bool  `json:"suggestedMergeBasePassesDateTest,omitempty"`
	SuggestedGapDays   *int   `json:"suggestedMergeBaseGapDays,omitempty"`
	Reachable          *bool  `json:"reachableFromRef,omitempty"`
	CommitDate         string `json:"commitDate,omitempty"`
	AuditDate          string `json:"auditDate,omitempty"`
	Service            string `json:"service"`
	SuggestedMergeBase string `json:"suggestedMergeBase,omitempty"`
	Outcome            string `json:"outcome"`
	CitedValue         string `json:"citedValue,omitempty"`
	Note               string `json:"note,omitempty"`
	Unreachable        bool   `json:"flagUnreachable"`
	StaleDate          bool   `json:"flagStaleDate"`
}

func outcomeString(o shaOutcome) string {
	switch o {
	case outcomeResolved:
		return outcomeStrResolved
	case outcomeMissing:
		return "missing-object"
	case outcomePlaceholder:
		return "placeholder-value"
	case outcomeAbsent:
		return "no-field"
	default:
		return "unknown"
	}
}

func toJSONResult(r result) jsonResult {
	jr := jsonResult{
		Service:     r.service,
		CitedValue:  r.citation.raw,
		Outcome:     outcomeString(r.outcome),
		Unreachable: r.unreachable,
		StaleDate:   r.staleDate,
		Note:        resultNote(r),
	}

	if r.hasCommitDate {
		jr.CommitDate = r.commitDate.Format(dateDisplayLayout)
	}
	if r.hasAuditDate {
		jr.AuditDate = r.auditDate.Format(dateDisplayLayout)
	}
	if r.gapKnown {
		gap := r.gapDays
		jr.GapDays = &gap
	}
	if r.reachabilityKnown {
		reachable := r.reachable
		jr.Reachable = &reachable
	}
	if r.suggestedMergeBase != "" {
		jr.SuggestedMergeBase = r.suggestedMergeBase
		if r.suggestedGapKnown {
			gap := r.suggestedGapDays
			jr.SuggestedGapDays = &gap
			passes := r.suggestedPassesGate
			jr.SuggestedPasses = &passes
		}
	}

	return jr
}

// resultNote is a one-line, per-row explanation kept factual and
// non-verdictive: it names what was observed, not whether it's a problem.
func resultNote(r result) string {
	switch r.outcome {
	case outcomeAbsent:
		return "no last_audit_commit field"
	case outcomePlaceholder:
		return fmt.Sprintf("value %q has no commit-sha prefix -- not checkable by this tool", r.citation.raw)
	case outcomeMissing:
		return fmt.Sprintf("sha %q not found in local repository (never fetched, or pruned)", r.citation.sha)
	case outcomeResolved:
		return resolvedNote(r)
	default:
		return ""
	}
}

func resolvedNote(r result) string {
	if r.citation.suffix != "" {
		return fmt.Sprintf(
			"cited value has trailing %q after the sha; only the sha prefix was checked",
			r.citation.suffix,
		)
	}
	if r.gitErr != "" {
		return "git lookup error: " + r.gitErr
	}
	if !r.hasAuditDate {
		return "last_audit_date missing or unparseable: " + r.auditDateErr
	}

	return ""
}

func writeJSON(path string, results []result) error {
	out := make([]jsonResult, 0, len(results))
	for _, r := range results {
		out = append(out, toJSONResult(r))
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")

	if encErr := enc.Encode(out); encErr != nil {
		return fmt.Errorf("encode json: %w", encErr)
	}

	return nil
}

const disclaimer = `NOTE ON READING THIS REPORT
"unreachable" and "stale-date" below are SIGNALS, not verdicts.

  - unreachable-from-ref is the EXPECTED, structural outcome once an audit
    branch is squash-merged: the recorded sha never lands on the target
    ref. By itself it says nothing about whether the manifest's contents
    are accurate -- it is 100% of citable shas in this repo today, most of
    which were never disputed.
  - stale-date (gap > threshold) is suggestive of a copied/stale stamp,
    but an audit run on a long-lived branch whose HEAD was genuinely old
    produces the identical signal honestly. Treat it as "worth a manual
    look," not "confirmed wrong."

This tool does NOT check whether a cited sha touches the service's own
directory -- that check is deliberately absent (gopherstack-z31a: it
produced four false accusations and zero confirmed findings). Every
confirmed real case so far failed the date test; none was found any other
way. Confirm manually before recording either flag as a defect.
`

func printReport(w io.Writer, results []result, threshold int, ref string, suggest bool) {
	fmt.Fprintf(w, "stampaudit: last_audit_commit provenance vs %s, date threshold %dd\n\n", ref, threshold)

	fmt.Fprintf(w, "%-24s %-11s %-10s %-10s %6s %-10s %-9s %s\n",
		"service", "sha", "commit", "audit", "gap", "reachable", "flags", "note")

	for _, r := range results {
		printRow(w, r, suggest)
	}

	fmt.Fprintln(w)
	fmt.Fprint(w, disclaimer)
	fmt.Fprintln(w)
	printSummary(w, computeSummary(results), threshold)
}

func printRow(w io.Writer, r result, suggest bool) {
	sha := r.citation.sha
	if sha == "" {
		sha = r.citation.raw
	}

	commit := "-"
	if r.hasCommitDate {
		commit = r.commitDate.Format(dateDisplayLayout)
	}

	audit := "-"
	if r.hasAuditDate {
		audit = r.auditDate.Format(dateDisplayLayout)
	}

	gap := "-"
	if r.gapKnown {
		gap = fmt.Sprintf("%dd", r.gapDays)
	}

	reachable := "?"
	if r.reachabilityKnown {
		reachable = "no"
		if r.reachable {
			reachable = "yes"
		}
	}

	var flags string
	switch {
	case r.unreachable && r.staleDate:
		flags = "BOTH"
	case r.unreachable:
		flags = "unreachable"
	case r.staleDate:
		flags = "stale-date"
	case r.outcome == outcomeResolved:
		flags = "clean"
	default:
		flags = outcomeString(r.outcome)
	}

	note := resultNote(r)
	if suggest && r.suggestedMergeBase != "" {
		note = appendSuggestion(note, r)
	}

	fmt.Fprintf(w, "%-24s %-11s %-10s %-10s %6s %-10s %-9s %s\n",
		r.service, sha, commit, audit, gap, reachable, flags, note)
}

func appendSuggestion(note string, r result) string {
	suffix := fmt.Sprintf("suggest merge-base %s w/ %s", r.suggestedMergeBase, r.citation.raw)
	if r.suggestedGapKnown {
		verdict := "passes date test"
		if !r.suggestedPassesGate {
			verdict = "STILL FAILS date test, do not use"
		}
		suffix = fmt.Sprintf("%s (gap %dd, %s)", suffix, r.suggestedGapDays, verdict)
	}
	if note == "" {
		return suffix
	}

	return note + "; " + suffix
}

func printSummary(w io.Writer, s summary, threshold int) {
	fmt.Fprintf(w, "totals: %d manifests audited\n", s.total)
	fmt.Fprintf(w, "  resolved shas:      %d\n", s.resolved)
	fmt.Fprintf(w, "    clean (neither flag):        %d\n", s.clean)
	fmt.Fprintf(w, "    unreachable only:            %d\n", s.unreachable)
	fmt.Fprintf(w, "    stale-date only (>%dd):       %d\n", threshold, s.staleDate)
	fmt.Fprintf(w, "    both unreachable+stale-date: %d\n", s.both)
	fmt.Fprintf(w, "  missing sha (not in local repo): %d\n", s.missing)
	fmt.Fprintf(w, "  placeholder value (not sha-shaped): %d\n", s.placeholder)
	fmt.Fprintf(w, "  no last_audit_commit field: %d\n", s.absent)
}
