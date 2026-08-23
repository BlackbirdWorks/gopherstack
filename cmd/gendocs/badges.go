package main

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Self-hosted badge output. Generated into .badges/ (committed to the repo)
// instead of pointed at shields.io: shields.io has gone down before and
// broken every badge in the README at once, so these are rendered locally
// from data gendocs already parses and never depend on a third party at
// render time. README.md links to them by relative path.
const (
	badgesDir  = ".badges"
	badgeMode  = 0o755
	goModPath  = "go.mod"
	unknownGo  = "unknown"
	badgeLabel = "#555" // shields' standard dark-grey label-section fill.

	// colorInfo is used for badges that convey a plain fact (a count, a
	// version) rather than a pass/fail judgement.
	colorInfo = "#007ec6"
	// colorGood is shields' standard "healthy" green, used for the parity
	// badge since the corpus is overwhelmingly grade A/B.
	colorGood = "#4c1"
	// colorLicense is a slightly softer green, keeping the license badge
	// visually distinct from the parity badge while staying in the same
	// cohesive green/blue family as the rest of the row.
	colorLicense = "#97ca00"
)

// goDirectiveRe matches go.mod's "go 1.26.5" directive line (module-level Go
// version, not a toolchain: line or a require block entry).
var goDirectiveRe = regexp.MustCompile(`^go\s+(\d+\.\d+(?:\.\d+)?)\s*$`)

// operationsBadgeFile is the operations badge's filename under .badges/,
// named so badges_test.go can reference it without tripping goconst.
const operationsBadgeFile = "operations.svg"

// badgeSpec is one badge to render: its filename under .badges/, left-hand
// label, right-hand value, and the value section's fill colour.
type badgeSpec struct {
	File  string
	Label string
	Value string
	Color string
}

// generateBadges computes the five self-hosted badge SVGs from data gendocs
// has already parsed — total PARITY.md ops: entries and the grade
// distribution across docs, plus totalServices (the count of services/ directories,
// including the two removed services that have no PARITY.md) — and writes
// them to .badges/. Idempotent: identical input produces byte-identical SVG
// output, so a second `make docs` run leaves no diff.
func generateBadges(totalServices int, docs []*ParityDoc) error {
	goVersion, err := readGoVersion(goModPath)
	if err != nil {
		return err
	}

	specs := badgeSpecs(totalServices, docs, goVersion)

	if mkErr := os.MkdirAll(badgesDir, badgeMode); mkErr != nil {
		return fmt.Errorf("mkdir %s: %w", badgesDir, mkErr)
	}

	for _, spec := range specs {
		path := filepath.Join(badgesDir, spec.File)
		if writeErr := writeFileAtomic(path, []byte(renderBadge(spec.Label, spec.Value, spec.Color))); writeErr != nil {
			return fmt.Errorf("write %s: %w", path, writeErr)
		}
	}

	return nil
}

// badgeSpecs builds the five badge specs from data gendocs has already
// parsed. Split out from generateBadges so the specs (in particular the
// operations badge's label and value) are unit-testable without touching
// the filesystem.
func badgeSpecs(totalServices int, docs []*ParityDoc, goVersion string) []badgeSpec {
	return []badgeSpec{
		{operationsBadgeFile, "PARITY entries", strconv.Itoa(totalOpEntries(docs)), colorInfo},
		{"services.svg", "AWS services", strconv.Itoa(totalServices), colorInfo},
		{"parity.svg", "parity", gradeDistribution(docs), colorGood},
		{"go.svg", "go", goVersion, colorInfo},
		{"license.svg", "license", "MIT", colorLicense},
	}
}

// totalOpEntries sums len(doc.Ops) across every parsed PARITY.md -- the
// count of ops: block ENTRIES (hand-grouped audit units; one entry can name
// more than one real operation, e.g. "AddPermission/RemovePermission"), not
// a count of real dispatched operations. A service tracked purely by
// families: block contributes 0 here even though it has real operations
// (gopherstack-mgna). For the real per-service operation count, read each
// service's own GetSupportedOperations (cmd/opcensus computes this).
func totalOpEntries(docs []*ParityDoc) int {
	total := 0
	for _, doc := range docs {
		total += len(doc.Ops)
	}

	return total
}

// normalizeGrade collapses a "B+"-style modifier grade into its base letter
// bucket (B+ -> B), so the parity badge shows two clean buckets instead of a
// third one-off bucket for a single outlier grade.
func normalizeGrade(raw string) string {
	return strings.TrimSuffix(strings.TrimSpace(raw), "+")
}

// gradeDistribution renders the parity badge's value, e.g. "133 A · 19 B":
// every doc's overall grade, normalized and tallied, in alphabetical order.
func gradeDistribution(docs []*ParityDoc) string {
	counts := make(map[string]int, len(docs))
	for _, doc := range docs {
		counts[normalizeGrade(doc.Overall)]++
	}

	grades := make([]string, 0, len(counts))
	for g := range counts {
		grades = append(grades, g)
	}
	sort.Strings(grades)

	parts := make([]string, 0, len(grades))
	for _, g := range grades {
		parts = append(parts, fmt.Sprintf("%d %s", counts[g], g))
	}

	return strings.Join(parts, " · ")
}

// readGoVersion extracts the version from go.mod's "go " directive (e.g.
// "1.26.5" from "go 1.26.5"), so the go badge never drifts from the module's
// actual declared version.
func readGoVersion(path string) (string, error) {
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return "", fmt.Errorf("read %s: %w", path, err)
	}

	for line := range strings.SplitSeq(string(data), "\n") {
		if m := goDirectiveRe.FindStringSubmatch(strings.TrimSpace(line)); m != nil {
			return m[1], nil
		}
	}

	return unknownGo, nil
}

// --- SVG rendering -----------------------------------------------------
//
// renderBadge draws a shields.io "flat"-style badge: a rounded rect split
// into a dark-grey label section and a coloured value section, 11px
// Verdana/DejaVu Sans text (with the usual subtle drop-shadow), and a faint
// top-to-bottom gradient for the flat style's characteristic sheen. Section
// widths are sized from an approximated per-character text width so labels
// and values get roughly shields-accurate padding without ever clipping.

const (
	badgeHeight  = 20
	badgeFontSz  = 11
	badgeSidePad = 10.0 // horizontal padding either side of the text within its section.
	badgeRadius  = 3

	// centerDivisor halves a section's width to find its horizontal text
	// centre (named, rather than an inline "/2", to satisfy the mnd linter).
	centerDivisor = 2.0

	// Character-width tiers (px, at 11px), from narrowest to widest. Real
	// Verdana/DejaVu Sans glyph widths vary continuously; collapsing them
	// to a few named tiers keeps charWidths a lookup of identifiers rather
	// than bare numeric literals (also satisfying mnd) while still sizing
	// badge sections close enough to their text to avoid clipping or
	// excess padding.
	widthNarrow = 3.3  // narrow punctuation/letters: . , ' ! | i l j I
	widthSpaced = 4.0  // space, and colon/semicolon (slightly wider than widthNarrow)
	widthMedium = 4.6  // - _ ( ) · t f r J
	widthWide   = 10.5 // extra-wide letters: m w M W

	// avgCharWidth is the fallback for characters not in charWidths and not
	// a digit or upper-case letter (lower-case mixed alphanumerics, at 11px
	// Verdana/DejaVu Sans average roughly 6.0-7.0px).
	avgCharWidth = 6.8

	// digitWidth covers '0'-'9': Verdana/DejaVu Sans digits are tabular
	// (all the same width), noticeably above the mixed-case average.
	digitWidth = 7.0

	// upperWidth is the fallback for capital letters not explicitly listed
	// in charWidths (capitals run wider than the mixed-case average).
	upperWidth = 7.8
)

// charWidthTable hand-tunes the rendered width of characters whose width
// differs noticeably from the mixed-case average. Anything not listed here
// falls back to digitWidth (for 0-9), upperWidth (for A-Z), or
// avgCharWidth (everything else) — see charWidth.
//
// Kept as a function (not a package-level var) per the repo's
// gochecknoglobals convention — build-fresh-per-call is cheap here since
// gendocs runs once as a CLI, not in a hot path (mirrors categoryGroups in
// categories.go).
func charWidthTable() map[rune]float64 {
	return map[rune]float64{
		' ': widthSpaced, ':': widthSpaced, ';': widthSpaced,
		'.': widthNarrow, ',': widthNarrow, '\'': widthNarrow, '!': widthNarrow,
		'|': widthNarrow, 'i': widthNarrow, 'l': widthNarrow, 'j': widthNarrow, 'I': widthNarrow,
		'-': widthMedium, '_': widthMedium, '(': widthMedium, ')': widthMedium, '·': widthMedium,
		't': widthMedium, 'f': widthMedium, 'r': widthMedium, 'J': widthMedium,
		'm': widthWide, 'w': widthWide, 'M': widthWide, 'W': widthWide,
	}
}

func charWidth(r rune) float64 {
	if w, ok := charWidthTable()[r]; ok {
		return w
	}

	switch {
	case r >= '0' && r <= '9':
		return digitWidth
	case r >= 'A' && r <= 'Z':
		return upperWidth
	default:
		return avgCharWidth
	}
}

// textWidth approximates the rendered pixel width of s at badgeFontSz.
func textWidth(s string) float64 {
	var w float64
	for _, r := range s {
		w += charWidth(r)
	}

	return w
}

// escapeXML escapes the handful of characters that matter inside an SVG
// text node or attribute value. Badge labels/values are short generated
// strings (counts, grades, a Go version, "MIT"), never user input, but this
// keeps the output well-formed regardless.
func escapeXML(s string) string {
	replacer := strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		`"`, "&quot;",
	)

	return replacer.Replace(s)
}

// sectionWidth rounds a text's padded section width to a whole pixel so the
// label/value rects and the total badge width always sum consistently
// (rounding the two sections independently, then adding, rather than
// rounding a separately-computed total that could disagree by a pixel).
func sectionWidth(text string) float64 {
	return math.Round(textWidth(text) + 2*badgeSidePad)
}

// renderBadge renders one flat-style badge SVG as a string.
func renderBadge(label, value, valueColor string) string {
	labelW := sectionWidth(label)
	valueW := sectionWidth(value)
	totalW := labelW + valueW
	labelX := labelW / centerDivisor
	valueX := labelW + valueW/centerDivisor

	var b strings.Builder

	fmt.Fprintf(&b,
		"<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"%.0f\" height=\"%d\" role=\"img\" aria-label=\"%s: %s\">\n",
		totalW, badgeHeight, escapeXML(label), escapeXML(value))
	b.WriteString("  <linearGradient id=\"s\" x2=\"0\" y2=\"100%\">\n")
	b.WriteString("    <stop offset=\"0\" stop-color=\"#bbb\" stop-opacity=\".1\"/>\n")
	b.WriteString("    <stop offset=\"1\" stop-opacity=\".1\"/>\n")
	b.WriteString("  </linearGradient>\n")
	fmt.Fprintf(&b, "  <clipPath id=\"r\"><rect width=\"%.0f\" height=\"%d\" rx=\"%d\" fill=\"#fff\"/></clipPath>\n",
		totalW, badgeHeight, badgeRadius)
	b.WriteString("  <g clip-path=\"url(#r)\">\n")
	fmt.Fprintf(&b, "    <rect width=\"%.0f\" height=\"%d\" fill=\"%s\"/>\n", labelW, badgeHeight, badgeLabel)
	fmt.Fprintf(&b,
		"    <rect x=\"%.0f\" width=\"%.0f\" height=\"%d\" fill=\"%s\"/>\n",
		labelW, valueW, badgeHeight, valueColor)
	fmt.Fprintf(&b, "    <rect width=\"%.0f\" height=\"%d\" fill=\"url(#s)\"/>\n", totalW, badgeHeight)
	b.WriteString("  </g>\n")
	fmt.Fprintf(
		&b,
		"  <g fill=\"#fff\" text-anchor=\"middle\" font-family=\"Verdana,Geneva,DejaVu Sans,sans-serif\" font-size=\"%d\">\n",
		badgeFontSz,
	)
	writeBadgeText(&b, labelX, label)
	writeBadgeText(&b, valueX, value)
	b.WriteString("  </g>\n")
	b.WriteString("</svg>\n")

	return b.String()
}

// writeBadgeText appends the shadow+main text pair standard to flat badges:
// a dark, low-opacity copy one pixel below the main text for a subtle drop
// shadow, then the solid white text on top of it.
func writeBadgeText(b *strings.Builder, x float64, text string) {
	escaped := escapeXML(text)
	fmt.Fprintf(b, "    <text x=\"%.1f\" y=\"15\" fill=\"#010101\" fill-opacity=\".3\">%s</text>\n", x, escaped)
	fmt.Fprintf(b, "    <text x=\"%.1f\" y=\"14\">%s</text>\n", x, escaped)
}
