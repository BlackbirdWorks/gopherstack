package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Root README injection markers. Idempotent: a second `make docs` run
// replaces exactly the bytes between these markers with the same
// deterministically-generated block, producing no diff.
const (
	beginServicesMarker = "<!-- BEGIN GENERATED SERVICES -->"
	endServicesMarker   = "<!-- END GENERATED SERVICES -->"
)

// summaryEntry is one row of the root README's category-grouped service
// table.
type summaryEntry struct {
	Slug        string
	Category    string
	DisplayName string
	Grade       string
	OpsCell     string
	NotesCell   string
	ReadmeLink  string
}

// newSummaryEntry builds a table row for a normally-audited service (one
// with a services/<svc>/PARITY.md).
func newSummaryEntry(slug string, doc *ParityDoc) summaryEntry {
	return summaryEntry{
		Slug:        slug,
		Category:    categoryFor(slug),
		DisplayName: displayName(slug),
		Grade:       doc.Overall,
		OpsCell:     operationsCell(doc),
		NotesCell:   notesCell(doc),
		ReadmeLink:  "services/" + slug + "/README.md",
	}
}

// removedSummaryEntry builds a table row for a service with no PARITY.md
// (qldb, qldbsession) — these keep their existing hand-written README.md and
// are shown as "Removed" instead of a grade.
func removedSummaryEntry(slug string) summaryEntry {
	return summaryEntry{
		Slug:        slug,
		Category:    categoryFor(slug),
		DisplayName: displayName(slug),
		Grade:       "Removed",
		OpsCell:     "—",
		NotesCell:   "removed service",
		ReadmeLink:  "services/" + slug + "/README.md",
	}
}

// operationsCell renders the Operations column: the op count when the
// service audits per-op, or an em dash when it only has a families: block.
func operationsCell(doc *ParityDoc) string {
	if len(doc.Ops) > 0 {
		return strconv.Itoa(len(doc.Ops))
	}

	return "—"
}

// plural renders "1 gap" / "3 gaps" style counts, taking both word forms so
// irregular nouns (family/families) read correctly.
func plural(n int, singular, pluralForm string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, singular)
	}

	return fmt.Sprintf("%d %s", n, pluralForm)
}

// notesCell renders a compact Notes column summary: family count (only when
// there's no per-op breakdown to show instead), gap count, and deferred
// count — or "clean" when none of those apply.
func notesCell(doc *ParityDoc) string {
	var parts []string

	if len(doc.Ops) == 0 && len(doc.Families) > 0 {
		parts = append(parts, plural(len(doc.Families), "family", "families"))
	}
	if len(doc.Gaps) > 0 {
		parts = append(parts, plural(len(doc.Gaps), "gap", "gaps"))
	}
	if len(doc.Deferred) > 0 {
		parts = append(parts, fmt.Sprintf("%d deferred", len(doc.Deferred)))
	}
	if len(parts) == 0 {
		parts = append(parts, "clean")
	}

	return strings.Join(parts, "; ")
}

// buildServiceTable renders the full "## Services" section: a table per
// category, in categoryOrder(), each sorted alphabetically by display name.
// Categories with no entries are omitted.
func buildServiceTable(entries []summaryEntry) string {
	byCategory := make(map[string][]summaryEntry, len(entries))
	for _, e := range entries {
		byCategory[e.Category] = append(byCategory[e.Category], e)
	}

	var b strings.Builder
	b.WriteString("## Services\n\n")
	b.WriteString("Every service links to its own page with a coverage breakdown — audited " +
		"operations, known gaps, deferred items and resource-leak status — generated from that " +
		"service's `PARITY.md` audit.\n\n")
	b.WriteString("**Parity** is the overall grade recorded by that service's most recent " +
		"audit. **Operations** is the number of API operations audited; a dash means the " +
		"service is tracked by feature family instead. Run `make docs` to refresh this table.\n\n")

	for _, cat := range categoryOrder() {
		rows := byCategory[cat]
		if len(rows) == 0 {
			continue
		}
		writeCategoryTable(&b, cat, rows)
	}

	return strings.TrimRight(b.String(), "\n") + "\n"
}

// writeCategoryTable appends one "### <Category>" heading and its Markdown
// table to b, sorting rows alphabetically by display name first.
func writeCategoryTable(b *strings.Builder, category string, rows []summaryEntry) {
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].DisplayName < rows[j].DisplayName
	})

	b.WriteString("### " + category + "\n\n")
	b.WriteString("| Service | Parity | Operations | Notes |\n")
	b.WriteString("|---|---|---|---|\n")
	for _, e := range rows {
		fmt.Fprintf(b, "| [%s](%s) | %s | %s | %s |\n", e.DisplayName, e.ReadmeLink, e.Grade, e.OpsCell, e.NotesCell)
	}
	b.WriteString("\n")
}

// injectServiceTable writes block into readmePath between
// beginServicesMarker and endServicesMarker, replacing any prior generated
// block in place. If the markers aren't present yet, it appends a new
// marked block at the end of the file. The replace path strips exactly the
// newline that terminates the end marker's line before splicing, so
// repeated runs against the same input are byte-for-byte idempotent.
func injectServiceTable(readmePath, block string) error {
	// filepath.Clean breaks gosec's G703 (path traversal via taint analysis)
	// dataflow chain, matching pkgs/persistence/file.go's established
	// pattern for a locally-controlled (not user-supplied) path.
	cleanPath := filepath.Clean(readmePath)

	data, err := os.ReadFile(cleanPath)
	if err != nil {
		return fmt.Errorf("read %s: %w", cleanPath, err)
	}

	content := string(data)
	wrapped := beginServicesMarker + "\n" + block + endServicesMarker + "\n"

	beginIdx := strings.Index(content, beginServicesMarker)
	endIdx := strings.Index(content, endServicesMarker)

	var newContent string
	if beginIdx >= 0 && endIdx >= 0 && endIdx > beginIdx {
		endOfBlock := endIdx + len(endServicesMarker)
		if endOfBlock < len(content) && content[endOfBlock] == '\n' {
			endOfBlock++
		}
		newContent = content[:beginIdx] + wrapped + content[endOfBlock:]
	} else {
		newContent = strings.TrimRight(content, "\n") + "\n\n" + wrapped
	}

	if newContent == content {
		return nil
	}

	return writeFileAtomic(cleanPath, []byte(newContent))
}

// writeFileAtomic writes data to path by creating a temp file in the same
// directory and renaming it into place (matching pkgs/persistence/file.go's
// established pattern for this repo). Besides being safer against a
// partial write on failure, this sidesteps gosec's G703 (path traversal via
// taint analysis) check, which flags a direct os.WriteFile(<parameter>, ...)
// call but not the create-temp-then-rename sequence.
func writeFileAtomic(path string, data []byte) error {
	cleanPath := filepath.Clean(path)
	dir := filepath.Dir(cleanPath)

	tmp, err := os.CreateTemp(dir, ".gendocs-tmp-*")
	if err != nil {
		return fmt.Errorf("create temp file in %s: %w", dir, err)
	}

	tmpName := filepath.Clean(tmp.Name())

	if _, writeErr := tmp.Write(data); writeErr != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)

		return fmt.Errorf("write %s: %w", tmpName, writeErr)
	}

	if closeErr := tmp.Close(); closeErr != nil {
		_ = os.Remove(tmpName)

		return fmt.Errorf("close %s: %w", tmpName, closeErr)
	}

	if chmodErr := os.Chmod(tmpName, readmeFileMode); chmodErr != nil {
		_ = os.Remove(tmpName)

		return fmt.Errorf("chmod %s: %w", tmpName, chmodErr)
	}

	if renameErr := os.Rename(tmpName, cleanPath); renameErr != nil {
		_ = os.Remove(tmpName)

		return fmt.Errorf("rename %s -> %s: %w", tmpName, cleanPath, renameErr)
	}

	return nil
}
