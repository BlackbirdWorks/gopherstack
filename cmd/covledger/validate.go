package main

import "fmt"

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
