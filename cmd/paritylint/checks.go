package main

import (
	"strconv"
	"strings"
)

// reportExcerptLen bounds how much of an item's text a finding row prints
// inline -- long enough to identify the item, short enough to stay scannable.
const reportExcerptLen = 140

// Rule names -- see main.go's package doc comment for (a)/(b)/(c) and which
// one gates the exit code.
const (
	ruleMissingItemsStillOpen = "missing-items-still-open"
	ruleResolvedElsewhere     = "resolved-elsewhere"
	ruleUndisclosedOpenItem   = "undisclosed-open-item"
)

// finding is one lint violation.
type finding struct {
	Service string `json:"service"`
	Path    string `json:"path"`
	Rule    string `json:"rule"`
	Text    string `json:"text"`
	Extra   string `json:"extra,omitempty"`
	Line    int    `json:"line"`
}

// checkMissingItemsStillOpen implements gopherstack-anjf option 3(c):
// items_still_open: must always be present, even as an empty list -- it is
// the one place a reader checks current fix status (.claude/memories/
// parity-principles.md rule 6).
func checkMissingItemsStillOpen(m manifest) []finding {
	if m.dest != nil {
		return nil
	}

	return []finding{{
		Service: m.service, Path: m.path, Rule: ruleMissingItemsStillOpen,
		Line: m.frontStart,
		Text: "items_still_open: is absent from front matter",
	}}
}

// checkResolvedElsewhere implements option 3(a): a front-matter open-item
// entry (items_still_open:, or a not-yet-migrated legacy gaps:/
// residual_gaps:) whose named op/field a LATER dated part of the same file
// already marks fixed -- the exact gopherstack-anjf shape. Reuses classify()
// (see classify.go), restricted to its verdictResolvedElsewhere bucket:
// cmd/paritymigrate's own hand-checked migration run found real false
// positives in the broader self-resolved bucket, so this lint only gates on
// the narrower, cross-referenced shape the issue actually describes.
func checkResolvedElsewhere(m manifest) []finding {
	var out []finding

	blocks := append(append([]block{}, m.sources...), destBlockOrNil(m)...)

	for _, b := range blocks {
		if b.isInline {
			continue
		}

		for _, it := range b.items {
			c := classify(m, it)
			if c.verdict != verdictResolvedElsewhere {
				continue
			}

			f := finding{
				Service: m.service, Path: m.path, Rule: ruleResolvedElsewhere,
				Line: it.start + 1, Text: oneLine(it.text, reportExcerptLen),
			}
			if len(c.resolved) > 0 {
				f.Extra = "fixed at line " + strconv.Itoa(c.resolved[0].line) + ": " + c.resolved[0].excerpt
			}

			out = append(out, f)
		}
	}

	return out
}

func destBlockOrNil(m manifest) []block {
	if m.dest == nil {
		return nil
	}

	return []block{*m.dest}
}

// checkUndisclosedOpenItem implements option 3(b): a body paragraph using
// this corpus's documented "not fixed"/"disclosed"/"deferred" phrasing for a
// named token that appears NOWHERE in items_still_open, AND has no later
// fixed-marker anywhere else in the file either (excluding the common case
// of a paragraph narrating a since-resolved historical bug, which is
// correctly absent from the open list precisely because it's resolved).
func checkUndisclosedOpenItem(m manifest) []finding {
	if m.dest == nil || m.dest.isInline {
		return nil // rule (c) already flags this file; nothing to cross-check against
	}

	openText := strings.Join(itemTexts(m.dest.items), "\n")
	bodyText := strings.Join(m.lines[bodyStart(m):], "\n")

	var out []finding

	for _, p := range splitParagraphs(m.lines[bodyStart(m):], bodyStart(m)) {
		if !openClaimMarkerRe.MatchString(p.text) {
			continue
		}

		for _, tok := range extractTokens(p.text) {
			if isTooCommon(m.fullText(), tok) {
				continue
			}

			openIdx, isOpen := firstOpenOccurrence(p.text, tok)
			if !isOpen {
				continue
			}

			if strings.Contains(openText, tok) {
				continue // already disclosed in the canonical list
			}

			if _, fixed := scanForPositiveWithDate(bodyText, tok); fixed {
				continue // historical narrative about a since-resolved item
			}

			out = append(out, finding{
				Service: m.service, Path: m.path, Rule: ruleUndisclosedOpenItem,
				Line: p.startLine, Text: excerpt(p.text, openIdx, len(tok)),
				Extra: tok,
			})
		}
	}

	return out
}

func bodyStart(m manifest) int { return m.frontEnd }

func itemTexts(items []item) []string {
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.text
	}

	return out
}

func oneLine(s string, maxLen int) string {
	r := []rune(s)
	if len(r) <= maxLen {
		return s
	}

	return string(r[:maxLen]) + "..."
}
