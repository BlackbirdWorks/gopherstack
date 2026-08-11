package main

import (
	"strconv"
	"strings"
)

// OpStatus is one entry of a PARITY.md ops: block — a single AWS API
// operation together with its four independently-tracked audit dimensions.
type OpStatus struct {
	Name    string
	Wire    string
	Errors  string
	State   string
	Persist string
}

// FamilyStatus is one entry of a PARITY.md families: block — a group of
// operations audited together (used when per-op tracking is impractical).
type FamilyStatus struct {
	Name   string
	Status string
}

// ParityDoc is the parsed content of a single services/<svc>/PARITY.md file.
type ParityDoc struct {
	Service         string
	SDKModule       string
	LastAuditCommit string
	LastAuditDate   string
	Overall         string
	Protocol        string
	LeaksStatus     string
	Ops             []OpStatus
	Families        []FamilyStatus
	Gaps            []string
	StructuralGaps  []string
	Deferred        []string
}

// bucket is the coarse health classification of a single status token (or of
// an op/family derived from its constituent tokens).
type bucket int

const (
	// bucketNeutral covers tokens that carry no signal either way (n/a,
	// empty) — they never move an op's rollup status.
	bucketNeutral bucket = iota
	bucketHealthy
	bucketDeferred
	bucketOther
	bucketPartial
	bucketGap
)

// Display labels for each bucket, as they appear in generated docs (e.g.
// "141 ok, 1 partial, 1 gap"). Named once here since goconst otherwise flags
// the repeated literals across label()/add().
const (
	labelOK       = "ok"
	labelPartial  = "partial"
	labelGap      = "gap"
	labelDeferred = "deferred"
	labelOther    = "other"
)

// Severity ranks, worst-wins, used to pick an op's rollup bucket across its
// wire/errors/state/persist fields. An explicit gap outranks partial, which
// outranks an unrecognized ("other") token, which outranks a deliberately
// deferred field; healthy (and neutral, which is filtered out before
// severity is ever consulted) ranks best.
const (
	severityHealthy = iota
	severityDeferred
	severityOther
	severityPartial
	severityGap
)

// label renders the bucket the way it should appear in generated docs.
// Neutral tokens never reach here in practice (rollup skips them before a
// label is needed), so they fall through to the same label as healthy.
func (b bucket) label() string {
	switch b {
	case bucketPartial:
		return labelPartial
	case bucketGap:
		return labelGap
	case bucketDeferred:
		return labelDeferred
	case bucketOther:
		return labelOther
	case bucketHealthy, bucketNeutral:
		return labelOK
	default:
		return labelOK
	}
}

// severity returns b's rank in the worst-wins comparison rollup uses.
func (b bucket) severity() int {
	switch b {
	case bucketGap:
		return severityGap
	case bucketPartial:
		return severityPartial
	case bucketOther:
		return severityOther
	case bucketDeferred:
		return severityDeferred
	case bucketHealthy, bucketNeutral:
		return severityHealthy
	default:
		return severityHealthy
	}
}

// classifyToken buckets a single raw status token observed in a PARITY.md
// ops:/families: entry. Vocabulary per the corpus: ok, clean, fixed (healthy);
// partial, partial- (partial); gap (gap); deferred (deferred); n, n/a, ""
// (neutral — no signal); anything else is "other".
func classifyToken(tok string) bucket {
	switch strings.ToLower(strings.TrimSpace(tok)) {
	case "ok", "clean", "fixed":
		return bucketHealthy
	case "partial", "partial-":
		return bucketPartial
	case "gap":
		return bucketGap
	case "deferred":
		return bucketDeferred
	case "", "n", "n/a":
		return bucketNeutral
	default:
		return bucketOther
	}
}

// rollup computes the worst-of bucket across a set of raw status tokens,
// ignoring neutral (n/a / unset) ones.
func rollup(tokens ...string) bucket {
	worst := bucketHealthy
	for _, tok := range tokens {
		b := classifyToken(tok)
		if b == bucketNeutral {
			continue
		}

		if b.severity() > worst.severity() {
			worst = b
		}
	}

	return worst
}

// tally counts ops or families by their rollup bucket label.
type tally struct {
	ok, partial, gap, deferred, other int
}

func (t *tally) add(b bucket) {
	switch b.label() {
	case labelPartial:
		t.partial++
	case labelGap:
		t.gap++
	case labelDeferred:
		t.deferred++
	case labelOther:
		t.other++
	default:
		t.ok++
	}
}

func (t *tally) total() int {
	return t.ok + t.partial + t.gap + t.deferred + t.other
}

// String renders a human summary like "141 ok, 1 partial, 1 gap", including
// only the non-zero buckets in a fixed, most-healthy-first order.
func (t *tally) String() string {
	type part struct {
		label string
		n     int
	}

	parts := []part{
		{labelOK, t.ok},
		{labelPartial, t.partial},
		{labelGap, t.gap},
		{labelDeferred, t.deferred},
		{labelOther, t.other},
	}

	rendered := make([]string, 0, len(parts))
	for _, p := range parts {
		if p.n > 0 {
			rendered = append(rendered, strconv.Itoa(p.n)+" "+p.label)
		}
	}

	return strings.Join(rendered, ", ")
}

// opsTally rolls up every op in doc to a tally of its worst-of bucket.
func (d *ParityDoc) opsTally() tally {
	var t tally
	for _, op := range d.Ops {
		t.add(rollup(op.Wire, op.Errors, op.State, op.Persist))
	}

	return t
}

// familiesTally rolls up every family in doc by its single status token.
func (d *ParityDoc) familiesTally() tally {
	var t tally
	for _, f := range d.Families {
		t.add(rollup(f.Status))
	}

	return t
}
