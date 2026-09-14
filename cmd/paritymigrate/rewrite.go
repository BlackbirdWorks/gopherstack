package main

import "strings"

// rewrite builds m's new file content from res: every item's raw lines are
// relocated verbatim into the items_still_open: block (created if absent),
// and a source block drained to zero items has its key line rewritten to
// "field: []". Body text and every comment/blank line are never touched --
// only "- " bullet lines (and their fold-continuation lines) that this
// migration relocates.
//
// dropResolved additionally DELETES (rather than relocates) a
// verdictResolvedElsewhere item instead of moving it. Off by default, and
// deliberately not used by this migration's own applied run: hand-checking
// every "resolved elsewhere" candidate this tool found across the corpus
// turned up real false positives (confirmed live: services/amplify's
// webhookCreateTime item and services/elasticbeanstalk's BranchOrder/
// SupportedTierList item both cite a "fixed" reference for an incidental
// comparison token, not the item's actual subject, while the item's own
// text still discloses the gap as current) at a rate too high to trust
// unattended -- see cmd/paritymigrate's package doc and the migration
// report filed alongside gopherstack-anjf. Relocation-only carries none of
// that risk: it only ever moves text, never judges it.
func rewrite(m manifest, res serviceResult, dropResolved bool) string {
	deleted, movedLines, emptiedKeyLine := planLineEdits(res, dropResolved)

	insertLine, insertHeader := -1, false
	if len(movedLines) > 0 {
		insertLine, insertHeader = destInsertionPoint(m)
	}

	return assembleOutput(m, deleted, movedLines, emptiedKeyLine, insertLine, insertHeader)
}

// planLineEdits decides, per res.blocks, which physical lines to delete
// outright (dropResolved's candidates only), which to relocate into
// items_still_open:, and which drained source blocks need their key line
// rewritten to "field: []".
func planLineEdits(res serviceResult, dropResolved bool) (map[int]bool, []string, map[int]string) {
	deleted := make(map[int]bool) // line index -> delete

	var movedLines []string

	emptiedKeyLine := map[int]string{} // key line index -> new "field: []" text

	for _, br := range res.blocks {
		for _, ir := range br.items {
			if dropResolved && ir.c.verdict.drop() {
				markDeleted(deleted, ir.item)

				continue
			}

			markDeleted(deleted, ir.item)
			movedLines = append(movedLines, ir.item.rawLines...)
		}

		if len(br.items) > 0 {
			emptiedKeyLine[br.keyLine] = br.field + ": []"
		}
	}

	return deleted, movedLines, emptiedKeyLine
}

// assembleOutput walks m.lines once, splicing in movedLines at insertLine,
// skipping every deleted line, and substituting a drained block's key line
// with its "field: []" replacement.
func assembleOutput(
	m manifest, deleted map[int]bool, movedLines []string, emptiedKeyLine map[int]string,
	insertLine int, insertHeader bool,
) string {
	out := make([]string, 0, len(m.lines)+len(movedLines)+1)

	emitMoved := func() {
		if insertHeader {
			out = append(out, destField+":")
		}

		out = append(out, movedLines...)
	}

	for i, line := range m.lines {
		if i == insertLine {
			emitMoved()
		}

		if deleted[i] {
			continue
		}

		if repl, ok := emptiedKeyLine[i]; ok {
			out = append(out, repl)

			continue
		}

		out = append(out, line)
	}

	if insertLine == len(m.lines) {
		emitMoved()
	}

	return strings.Join(out, "\n")
}

func markDeleted(deleted map[int]bool, it item) {
	for i := it.start; i < it.end; i++ {
		deleted[i] = true
	}
}

// destInsertionPoint returns the line index before which relocated items
// should be inserted, and whether a new "items_still_open:" header line must
// be emitted there. An existing items_still_open: block gets new bullets
// appended just before its own end; otherwise the block is created right
// after the last gaps:/residual_gaps: block found (so the diff stays
// localized next to the fields it consolidates). Only called when there is
// at least one item to move.
func destInsertionPoint(m manifest) (int, bool) {
	if m.dest != nil {
		return m.dest.end, false
	}

	return m.sources[len(m.sources)-1].end, true
}
