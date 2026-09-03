package main

import (
	"regexp"
	"strings"
)

// Tiers, lowest number ranks highest. See the package doc for the
// reasoning behind this order and its validation against known ground
// truth.
const (
	tierDocumentedDefault = 1
	tierCollectionFilter  = 2
	tierSiblingDeclares   = 3
	tierRequired          = 4
	tierNoSignal          = 5
)

// defaultLanguageRe matches an SDK doc comment stating what happens when a
// field is omitted -- "the default value is X", "if not specified, ...",
// "if you omit this...", "defaults to X", "By default, ...". A field with a
// stated default that the emulator never declared cannot possibly honour
// that default: nineteen of this campaign's confirmed bugs came from
// exactly this absence-semantics shape (see gopherstack-uox6's comment
// history), and this tool's own four-field, single-operation ground truth
// (omics' StartRun: RetentionMode, ScratchStorageMode, StorageCapacity,
// StorageType) is entirely this signal.
//
// Deliberately loose: an SDK doc comment states a default in enough
// different phrasings ("The default run storage capacity is 1200 GiB.",
// "By default, ... uses STATIC storage type.", "Default: true") that
// requiring a specific sentence shape missed two of this tool's own four
// ground-truth fields on its first pass (StorageCapacity, StorageType) --
// caught only because the validation step this tool's brief required
// compared the ranked output against known ground truth and found them
// missing from tier 1. A bare "default" match risks pulling in an
// unrelated mention; that costs a human a few seconds of dismissal, which
// is cheaper than silently missing the shape this signal exists for.
var defaultLanguageRe = regexp.MustCompile(
	`(?i)\bdefault\b|if (you )?omit|if not specified|if none (is|are) specified|` +
		`if this (parameter|value|field) is not`,
)

// deprecatedRe matches Go's own convention for a deprecated doc comment.
var deprecatedRe = regexp.MustCompile(`(?i)^deprecated:`)

// collectionOpPrefixes are operation-name prefixes this repo's own
// campaign found concentrate the filter/range/page-size shape: "58 of 64
// Get* families were clean" (per the task brief) is the flip side -- this
// signal is scoped to List/Describe/Search deliberately, not every op.
//
//nolint:gochecknoglobals // read-only lookup table, same pattern as sdkfields.go's dirModuleOverride
var collectionOpPrefixes = []string{"List", "Describe", "Search"}

// collectionFieldHints are field-name substrings (checked against the
// normalized wire name) that mark a field as a filter or page-size
// parameter regardless of which operation it's on -- these are deliberately
// long/specific enough not to false-match an unrelated field name as a
// substring (see collectionRangeHints for the shorter, riskier ones, gated
// on the op actually being a collection op).
//
//nolint:gochecknoglobals // read-only lookup table, same pattern as sdkfields.go's dirModuleOverride
var collectionFieldHints = []string{
	"filter", "maxresults", "maxitems", "pagesize", "nexttoken", "startingtoken",
}

// collectionRangeHints are shorter date/range substrings that DO risk a
// false match against an unrelated field name (e.g. "to" inside
// "StorageType" or "WorkflowBucketOwnerId" -- both matched before this
// list was split and gated, a bug caught by exactly the ground-truth
// validation this tool's brief demanded: neither StorageType nor
// WorkflowBucketOwnerId is a range filter). Gated in
// isCollectionFilterField on the operation actually being a
// List/Describe/Search, which the task brief's own signal description
// scopes this shape to ("concentrates in List/Describe").
//
//nolint:gochecknoglobals // read-only lookup table, same pattern as sdkfields.go's dirModuleOverride
var collectionRangeHints = []string{
	"starttime", "endtime", "startdate", "enddate",
	"createdafter", "createdbefore", "modifiedafter", "modifiedbefore",
}

// triageFinding is one ranked, justified missing-field finding.
type triageFinding struct {
	Op         string   `json:"op"`
	NormWire   string   `json:"normWire"`
	Field      sdkField `json:"field"`
	Signals    []string `json:"signals,omitempty"`
	Tier       int      `json:"tier"`
	Deprecated bool     `json:"deprecated"`
}

// triageOne classifies one missing field for one operation against the
// full per-service field index (built once, see siblingDeclaresElsewhere)
// so the sibling-operation signal can see every other operation's
// resolution.
func triageOne(m missingField, siblingWire map[string]bool) triageFinding {
	f := triageFinding{
		Op: m.Op, Field: m.Field, NormWire: normalizeWireName(m.Field.Name),
		Tier: tierNoSignal,
	}

	if deprecatedRe.MatchString(strings.TrimSpace(m.Field.DocText)) {
		f.Deprecated = true

		return f
	}

	var signals []string

	if defaultLanguageRe.MatchString(m.Field.DocText) {
		signals = append(signals, "documented default")
		f.Tier = min(f.Tier, tierDocumentedDefault)
	}

	if isCollectionFilterField(m.Op, m.Field.Name) {
		signals = append(signals, "filter/range/page-size on a List/Describe/Search op")
		f.Tier = min(f.Tier, tierCollectionFilter)
	}

	if siblingWire[f.NormWire] {
		signals = append(signals, "a sibling operation in this service declares the same field")
		f.Tier = min(f.Tier, tierSiblingDeclares)
	}

	if m.Field.Required {
		signals = append(signals, "required in the SDK")
		f.Tier = min(f.Tier, tierRequired)
	}

	f.Signals = signals

	return f
}

func isCollectionFilterField(op, fieldName string) bool {
	norm := normalizeWireName(fieldName)

	for _, hint := range collectionFieldHints {
		if strings.Contains(norm, hint) {
			return true
		}
	}

	if !isCollectionOp(op) {
		return false
	}

	for _, hint := range collectionRangeHints {
		if strings.Contains(norm, hint) {
			return true
		}
	}

	return false
}

func isCollectionOp(op string) bool {
	for _, p := range collectionOpPrefixes {
		if strings.HasPrefix(op, p) {
			return true
		}
	}

	return false
}

// buildSiblingIndex maps normalized wire name -> declared anywhere among
// the OTHER operations' resolved emulator fields in this service, so
// triageOne's sibling signal can be computed once per service rather than
// once per finding.
func buildSiblingIndex(resolutions map[string]opResolution, excludeOp string) map[string]bool {
	out := map[string]bool{}

	for op, res := range resolutions {
		if op == excludeOp {
			continue
		}

		for wire := range res.Fields {
			out[wire] = true
		}
	}

	return out
}
