package main

import (
	"fmt"
	"strings"
)

const (
	sourceCommit    = "commit"
	sourceParity    = "parity"
	sourceBDComment = "bd_comment"
)

var knownSourceTags = map[string]bool{ //nolint:gochecknoglobals // immutable lookup table
	sourceCommit:    true,
	sourceParity:    true,
	sourceBDComment: true,
}

// Validate checks rows for the three things that make the ledger
// untrustworthy if wrong: a service name with no matching directory, a
// class outside the known set, and a duplicate (service, class) row. It
// also rejects an unknown verdict and a row missing its commit, since a
// verdict with no evidence behind it is exactly the prose problem this
// ledger exists to replace.
//
// knownServices is the set of real services/<dir> basenames -- passed in
// rather than read from disk here, so tests can validate against a small
// fake set without touching the real services/ tree.
//
// Every problem is reported; Validate never skips a bad row to keep
// going, per gopherstack-7q13: a service or class that doesn't check out
// must fail loudly, the same discipline cmd/reqfieldscan's coverage guard
// applies to an implausible number.
func Validate(rows []Row, knownServices map[string]bool) []string {
	var errs []string

	seen := make(map[[2]string]Row, len(rows))

	for i, r := range rows {
		errs = append(errs, validateRow(i, r, knownServices)...)

		key := [2]string{r.Service, r.Class}
		if prev, ok := seen[key]; ok {
			errs = append(errs, fmt.Sprintf(
				"row %d: duplicate row for (service=%s, class=%s) -- also at commit %s (%s), this one at commit %s (%s)",
				i,
				r.Service,
				r.Class,
				prev.Commit,
				prev.Date,
				r.Commit,
				r.Date,
			))

			continue
		}

		seen[key] = r
	}

	return errs
}

func validateRow(i int, r Row, knownServices map[string]bool) []string {
	var errs []string

	if r.Service == "" {
		errs = append(errs, fmt.Sprintf("row %d: empty service", i))
	} else if !knownServices[r.Service] {
		errs = append(errs, fmt.Sprintf("row %d: service %q has no directory under services/", i, r.Service))
	}

	if !isKnownClass(r.Class) {
		errs = append(
			errs,
			fmt.Sprintf("row %d (service=%s): class %q is not one of the known classes", i, r.Service, r.Class),
		)
	}

	if !knownVerdicts[Verdict(r.Verdict)] {
		errs = append(
			errs,
			fmt.Sprintf(
				"row %d (service=%s): verdict %q is not fixed, clean, or inapplicable",
				i,
				r.Service,
				r.Verdict,
			),
		)
	}

	if r.Commit == "" {
		errs = append(errs, fmt.Sprintf("row %d (service=%s): no commit recorded as evidence", i, r.Service))
	}

	if !validSource(r.Source) {
		errs = append(errs, fmt.Sprintf(
			"row %d (service=%s): source %q is not empty or a '+'-joined list of %s/%s/%s with no duplicates",
			i, r.Service, r.Source, sourceCommit, sourceParity, sourceBDComment,
		))
	}

	if Verdict(r.Verdict) == VerdictInapplicable && strings.TrimSpace(r.Reasoning) == "" {
		errs = append(errs, fmt.Sprintf(
			"row %d (service=%s, class=%s): inapplicable verdict has no reasoning recorded",
			i, r.Service, r.Class,
		))
	}

	return errs
}

// validSource reports whether s is empty (a legacy row, implicitly
// commit-subject-derived) or a '+'-joined, duplicate-free list of known
// source tags.
func validSource(s string) bool {
	if s == "" {
		return true
	}

	seen := make(map[string]bool)

	for tag := range strings.SplitSeq(s, "+") {
		if tag == "" || !knownSourceTags[tag] || seen[tag] {
			return false
		}

		seen[tag] = true
	}

	return true
}

// ValidateConflicts checks conflicts for the same structural problems
// Validate checks in rows -- an unknown service, an unknown class, and a
// duplicate entry -- plus one more: a (service, class) pair must never
// appear as both a resolved Row and an open Conflict, since that is a
// direct contradiction about whether the evidence agrees. A Conflict also
// needs a non-empty note; an unexplained conflict is as untrustworthy as
// an unexplained verdict.
func ValidateConflicts(conflicts []Conflict, rows []Row, knownServices map[string]bool) []string {
	var errs []string

	rowKeys := make(map[[2]string]bool, len(rows))
	for _, r := range rows {
		rowKeys[[2]string{r.Service, r.Class}] = true
	}

	seen := make(map[[2]string]bool, len(conflicts))

	for i, c := range conflicts {
		if c.Service == "" {
			errs = append(errs, fmt.Sprintf("conflict %d: empty service", i))
		} else if !knownServices[c.Service] {
			errs = append(errs, fmt.Sprintf("conflict %d: service %q has no directory under services/", i, c.Service))
		}

		if !isKnownClass(c.Class) {
			errs = append(
				errs,
				fmt.Sprintf(
					"conflict %d (service=%s): class %q is not one of the known classes",
					i,
					c.Service,
					c.Class,
				),
			)
		}

		if strings.TrimSpace(c.Note) == "" {
			errs = append(
				errs,
				fmt.Sprintf(
					"conflict %d (service=%s, class=%s): no note recording what the sources disagree about",
					i,
					c.Service,
					c.Class,
				),
			)
		}

		key := [2]string{c.Service, c.Class}
		if seen[key] {
			errs = append(
				errs,
				fmt.Sprintf("conflict %d: duplicate conflict entry for (service=%s, class=%s)", i, c.Service, c.Class),
			)
		}

		seen[key] = true

		if rowKeys[key] {
			errs = append(errs, fmt.Sprintf(
				"conflict %d: (service=%s, class=%s) has both a resolved row and an open conflict -- "+
					"resolve the conflict or remove the row",
				i, c.Service, c.Class,
			))
		}
	}

	return errs
}

func isKnownClass(c string) bool {
	for _, k := range KnownClasses {
		if string(k) == c {
			return true
		}
	}

	return false
}
