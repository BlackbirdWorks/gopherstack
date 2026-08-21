package main

import (
	"time"
)

// shaOutcome is what happened when this tool tried to resolve a
// last_audit_commit citation against the local repository.
type shaOutcome int

const (
	outcomeResolved    shaOutcome = iota // a real commit sha the repo still has
	outcomeMissing                       // hex-shaped, but no such object here
	outcomePlaceholder                   // not sha-shaped at all (HEAD, PENDING, ...)
	outcomeAbsent                        // no last_audit_commit field in the manifest
)

const hoursPerDay = 24

// result is one manifest's audit outcome. Every field is reported for its
// own sake -- see printReport -- and the two "defect class" fields
// (unreachable, staleDate) are the ONLY predicates this tool evaluates.
// There is deliberately no directory/path-based check anywhere: see
// gopherstack-z31a, where that heuristic produced four false accusations
// and zero confirmed findings even after agents were explicitly told not
// to use it. Don't add one.
type result struct {
	commitDate            time.Time
	auditDate             time.Time
	suggestedMergeBase    string
	gitErr                string
	suggestedMergeBaseErr string
	service               string
	auditDateErr          string
	citation              citation
	suggestedGapDays      int
	gapDays               int
	outcome               shaOutcome
	reachable             bool
	unreachable           bool
	gapKnown              bool
	staleDate             bool
	reachabilityKnown     bool
	hasAuditDate          bool
	suggestedGapKnown     bool
	suggestedPassesGate   bool
	hasCommitDate         bool
}

const auditDateLayout = "2006-01-02"

// auditOne resolves one manifest's last_audit_commit citation and applies
// the single defect predicate this tool implements: dateGap(sha,
// last_audit_date) > threshold. Nothing here reads services/<svc> or
// diffs the sha against any path.
func auditOne(g gitRunner, ref string, threshold int, m manifest) result {
	r := result{
		service:  m.service,
		citation: classifyCitation(m),
	}
	r.outcome = outcomeForCitation(r.citation)

	if m.rawDateFound {
		if t, err := time.Parse(auditDateLayout, m.rawDate); err == nil {
			r.auditDate = t
			r.hasAuditDate = true
		} else {
			r.auditDateErr = err.Error()
		}
	}

	if r.outcome != outcomeResolved {
		return r
	}

	resolveCommit(&r, g, r.citation.sha)
	resolveReachability(&r, g, ref, r.citation.sha)
	resolveGap(&r, threshold)

	if r.unreachable {
		resolveSuggestion(&r, g, ref, r.citation.sha, threshold)
	}

	return r
}

func outcomeForCitation(c citation) shaOutcome {
	switch c.kind {
	case citationAbsent:
		return outcomeAbsent
	case citationPlaceholder:
		return outcomePlaceholder
	case citationSHA:
		return outcomeResolved
	default:
		return outcomeAbsent
	}
}

func resolveCommit(r *result, g gitRunner, sha string) {
	if !g.commitExists(sha) {
		r.outcome = outcomeMissing

		return
	}

	if t, err := g.commitDate(sha); err == nil {
		r.commitDate = t
		r.hasCommitDate = true
	} else {
		r.gitErr = err.Error()
	}
}

func resolveReachability(r *result, g gitRunner, ref, sha string) {
	if r.outcome != outcomeResolved {
		return
	}

	reachable, err := g.isAncestor(sha, ref)
	if err != nil {
		if r.gitErr == "" {
			r.gitErr = err.Error()
		}

		return
	}

	r.reachabilityKnown = true
	r.reachable = reachable
	r.unreachable = !reachable
}

func resolveGap(r *result, threshold int) {
	if !r.hasCommitDate || !r.hasAuditDate {
		return
	}

	r.gapKnown = true
	r.gapDays = dateGapDays(r.commitDate, r.auditDate)
	r.staleDate = staleDatePredicate(r.gapDays, threshold)
}

// resolveSuggestion computes merge-base(sha, ref) as a candidate
// replacement for an unreachable citation, then validates that candidate
// against the same date predicate before ever surfacing it -- a
// suggestion that would itself fail the date test is reported as such,
// never silently offered as a fix. This tool does not write the
// suggestion anywhere; a human applies it.
func resolveSuggestion(r *result, g gitRunner, ref, sha string, threshold int) {
	base, err := g.mergeBase(sha, ref)
	if err != nil {
		r.suggestedMergeBaseErr = err.Error()

		return
	}

	short, err := g.shortSHA(base)
	if err != nil {
		short = base
	}
	r.suggestedMergeBase = short

	if !r.hasAuditDate {
		return
	}

	baseDate, err := g.commitDate(base)
	if err != nil {
		r.suggestedMergeBaseErr = err.Error()

		return
	}

	r.suggestedGapKnown = true
	r.suggestedGapDays = dateGapDays(baseDate, r.auditDate)
	r.suggestedPassesGate = !staleDatePredicate(r.suggestedGapDays, threshold)
}

// dateGapDays returns last_audit_date minus sha's committer date, in whole
// UTC calendar days. Positive means the commit predates the audit (the
// expected direction); negative means the cited commit postdates its own
// audit date, which is possible under this schema (see auditOne's caller
// for how that's surfaced) but not itself the predicate this tool checks.
func dateGapDays(commitDate, auditDate time.Time) int {
	c := commitDate.UTC()
	a := auditDate.UTC()
	cDay := time.Date(c.Year(), c.Month(), c.Day(), 0, 0, 0, 0, time.UTC)
	aDay := time.Date(a.Year(), a.Month(), a.Day(), 0, 0, 0, 0, time.UTC)

	return int(aDay.Sub(cDay).Hours() / hoursPerDay)
}

// staleDatePredicate is the ONE defect predicate this tool evaluates:
// dateGap(sha, last_audit_date) > threshold. It is suggestive, not proof --
// see printReport's disclaimer -- and it is the only signal wired up
// anywhere in this package.
func staleDatePredicate(gapDays, threshold int) bool {
	return gapDays > threshold
}

// summary is the repo-wide tally requested by gopherstack-z31a: how many
// manifests fall in each class, so the numbers stop being a point-in-time
// claim in the issue notes.
type summary struct {
	total       int
	resolved    int
	missing     int
	placeholder int
	absent      int
	unreachable int
	staleDate   int
	both        int
	clean       int
}

func computeSummary(results []result) summary {
	var s summary
	s.total = len(results)

	for _, r := range results {
		switch r.outcome {
		case outcomeResolved:
			s.resolved++
		case outcomeMissing:
			s.missing++
		case outcomePlaceholder:
			s.placeholder++
		case outcomeAbsent:
			s.absent++
		}

		if r.outcome != outcomeResolved {
			continue
		}

		switch {
		case r.unreachable && r.staleDate:
			s.both++
		case r.unreachable:
			s.unreachable++
		case r.staleDate:
			s.staleDate++
		case r.reachabilityKnown && !r.unreachable && r.gapKnown && !r.staleDate:
			s.clean++
		}
	}

	return s
}
