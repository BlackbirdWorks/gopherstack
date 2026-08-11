// Command gendocs regenerates per-service documentation from each service's
// PARITY.md audit manifest under services/. For every services/<svc> that has
// a PARITY.md, it writes a friendly services/<svc>/README.md summarizing
// audit coverage; it also builds a category-grouped service table and
// injects it into the root README.md between marker comments.
//
// services/qldb and services/qldbsession have no PARITY.md (they're marked
// REMOVED via hand-written README.md files) and are never touched beyond
// being listed, as "Removed", in the root table.
//
// Usage:
//
//	go run ./cmd/gendocs
//
// Idempotent: running it twice against unchanged PARITY.md files produces no
// further changes.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	servicesDirName = "services"
	parityFileName  = "PARITY.md"
	readmeFileName  = "README.md"
	docsServicesDir = "docs/services"
	rootReadmePath  = "README.md"
	readmeFileMode  = 0o644
)

func main() {
	ctx := context.Background()
	log := logger.NewLogger(slog.LevelInfo)

	if err := run(ctx, log); err != nil {
		log.ErrorContext(ctx, "gendocs failed", "error", err)
		os.Exit(1)
	}
}

// run discovers every services/<svc> directory, generates a README.md for
// each one that has a PARITY.md, and injects the resulting category table
// into the root README.md.
func run(ctx context.Context, log *slog.Logger) error {
	slugs, err := discoverServiceSlugs(servicesDirName)
	if err != nil {
		return err
	}

	entries := make([]summaryEntry, 0, len(slugs))
	docs := make([]*ParityDoc, 0, len(slugs))
	generated := 0

	for _, slug := range slugs {
		entry, doc, wrote, procErr := processService(slug)
		if procErr != nil {
			return procErr
		}

		entries = append(entries, entry)
		if doc != nil {
			docs = append(docs, doc)
		}
		if wrote {
			generated++
		}
	}

	if tableErr := injectServiceTable(rootReadmePath, buildServiceTable(entries)); tableErr != nil {
		return tableErr
	}

	if badgeErr := generateBadges(len(slugs), docs); badgeErr != nil {
		return badgeErr
	}

	logParseWarnings(ctx, log, docs)

	log.InfoContext(ctx, "gendocs complete",
		"services_total", len(entries),
		"readmes_generated", generated,
	)

	return nil
}

// discoverServiceSlugs lists the immediate subdirectories of dir, sorted, as
// service slugs. Stray files directly under services/ (e.g.
// _PARITY_TEMPLATE.md) are skipped since they aren't service directories.
func discoverServiceSlugs(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read dir %s: %w", dir, err)
	}

	slugs := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			slugs = append(slugs, e.Name())
		}
	}
	sort.Strings(slugs)

	return slugs, nil
}

// processService handles a single service slug: if it has a PARITY.md, it
// parses that file, renders and writes services/<svc>/README.md, and returns
// a table row, the parsed doc (so the caller can fold it into the badge
// totals), and wrote=true. If it has no PARITY.md (qldb, qldbsession), it
// returns a "Removed" row with a nil doc, without touching that service's
// existing hand-written README.md.
func processService(slug string) (summaryEntry, *ParityDoc, bool, error) {
	parityPath := filepath.Join(servicesDirName, slug, parityFileName)

	if _, statErr := os.Stat(parityPath); statErr != nil {
		if errors.Is(statErr, os.ErrNotExist) {
			return removedSummaryEntry(slug), nil, false, nil
		}

		return summaryEntry{}, nil, false, fmt.Errorf("stat %s: %w", parityPath, statErr)
	}

	doc, parseErr := ParseParityFile(parityPath)
	if parseErr != nil {
		return summaryEntry{}, nil, false, parseErr
	}

	readmePath := filepath.Join(servicesDirName, slug, readmeFileName)
	content := renderServiceReadme(slug, doc, guideExists(slug))

	if writeErr := writeFileAtomic(readmePath, []byte(content)); writeErr != nil {
		return summaryEntry{}, nil, false, fmt.Errorf("write %s: %w", readmePath, writeErr)
	}

	return newSummaryEntry(slug, doc), doc, true, nil
}

// logParseWarnings surfaces every parsed doc's Warnings via the structured
// logger, at Warn level. These never fail the build: a PARITY.md line that
// looks like an entry but doesn't parse should be visible to whoever next
// touches that file, not silently undercounted (gopherstack-udc7).
func logParseWarnings(ctx context.Context, log *slog.Logger, docs []*ParityDoc) {
	for _, doc := range docs {
		for _, w := range doc.Warnings {
			log.WarnContext(ctx, "PARITY.md entry did not parse", "detail", w)
		}
	}
}

// guideExists reports whether a hand-written docs/services/<svc>.md guide
// exists, so renderServiceReadme knows whether to link to it.
func guideExists(slug string) bool {
	_, err := os.Stat(filepath.Join(docsServicesDir, slug+".md"))

	return err == nil
}
