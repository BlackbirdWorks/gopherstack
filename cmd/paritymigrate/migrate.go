package main

// itemResult pairs a parsed item with its classification, for reporting and
// for building the rewrite plan.
type itemResult struct {
	c    classification
	item item
}

// blockResult is one gaps:/residual_gaps: block's migration outcome.
type blockResult struct {
	field   string
	items   []itemResult
	keyLine int
	end     int
}

func (b blockResult) kept() []itemResult {
	var out []itemResult

	for _, ir := range b.items {
		if !ir.c.verdict.drop() {
			out = append(out, ir)
		}
	}

	return out
}

func (b blockResult) dropped() []itemResult {
	var out []itemResult

	for _, ir := range b.items {
		if ir.c.verdict.drop() {
			out = append(out, ir)
		}
	}

	return out
}

// serviceResult is one manifest's full migration plan/report.
type serviceResult struct {
	service string
	path    string
	blocks  []blockResult

	// existingOpenFindings reports rule-(a)-style staleness found in a
	// pre-existing items_still_open: block. This migration never rewrites
	// that block's own content (see manifest.go's destField doc comment on
	// scope) -- these are reported so they can be filed/fixed separately.
	existingOpenFindings []itemResult
}

func (r serviceResult) totalKept() int {
	n := 0
	for _, b := range r.blocks {
		n += len(b.kept())
	}

	return n
}

func (r serviceResult) totalDropped() int {
	n := 0
	for _, b := range r.blocks {
		n += len(b.dropped())
	}

	return n
}

// planMigration classifies every item in m's source blocks (and, read-only,
// its existing destination block) without mutating m.
func planMigration(m manifest) serviceResult {
	res := serviceResult{service: m.service, path: m.path}

	for _, src := range m.sources {
		if src.isInline {
			continue
		}

		br := blockResult{field: src.field, keyLine: src.keyLine, end: src.end}
		for _, it := range src.items {
			br.items = append(br.items, itemResult{item: it, c: classify(m, it)})
		}

		res.blocks = append(res.blocks, br)
	}

	if m.dest != nil && !m.dest.isInline {
		for _, it := range m.dest.items {
			c := classify(m, it)
			if c.verdict.drop() {
				res.existingOpenFindings = append(res.existingOpenFindings, itemResult{item: it, c: c})
			}
		}
	}

	return res
}
