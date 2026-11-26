// Command gendocs regenerates per-service documentation from each service's
// PARITY.md audit manifest under services/. For every services/<svc> that has
// a PARITY.md, it writes a friendly services/<svc>/README.md summarizing
// audit coverage; it also builds a category-grouped service table and
// injects it into the root README.md between marker comments.
//
// When invoked with -site <dir>, gendocs instead generates the VitePress
// documentation site sources under <dir> without modifying README.md or
// services/*/README.md.
//
// services/qldb and services/qldbsession have no PARITY.md (they're marked
// REMOVED via hand-written README.md files) and are never touched beyond
// being listed, as "Removed", in the root table.
//
// Usage:
//
//	go run ./cmd/gendocs
//	go run ./cmd/gendocs -site site -version v1.2.3
//
// Idempotent: running it twice against unchanged PARITY.md files produces no
// further changes.
package main

import (
	"context"
	"errors"
	"flag"
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

	siteDir := flag.String("site", "", "output directory for VitePress doc site generation")
	version := flag.String("version", "", "release version string shown in site metadata")
	flag.Parse()

	if *siteDir != "" {
		if err := runSite(ctx, log, *siteDir, *version); err != nil {
			log.ErrorContext(ctx, "gendocs site generation failed", "error", err)
			os.Exit(1)
		}

		return
	}

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

	entries, docs, generated, err := collectServices(slugs, true)
	if err != nil {
		return err
	}

	if tableErr := injectServiceTable(rootReadmePath, buildServiceTable(entries)); tableErr != nil {
		return tableErr
	}

	if badgeErr := generateBadges(len(slugs), docs); badgeErr != nil {
		return badgeErr
	}

	logParseWarnings(ctx, log, docs)

	if warnErr := checkParseWarnings(docs); warnErr != nil {
		return warnErr
	}

	log.InfoContext(ctx, "gendocs complete",
		"services_total", len(entries),
		"readmes_generated", generated,
	)

	return nil
}

// collectServices discovers and parses every service under servicesDirName.
// When writeReadmes is true, it writes each audited service's README.md.
func collectServices(slugs []string, writeReadmes bool) ([]summaryEntry, []*ParityDoc, int, error) {
	entries := make([]summaryEntry, 0, len(slugs))
	docs := make([]*ParityDoc, 0, len(slugs))
	generated := 0

	for _, slug := range slugs {
		entry, doc, wrote, procErr := processService(slug, writeReadmes)
		if procErr != nil {
			return nil, nil, 0, procErr
		}

		entries = append(entries, entry)
		if doc != nil {
			docs = append(docs, doc)
		}
		if wrote {
			generated++
		}
	}

	return entries, docs, generated, nil
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
// parses that file, optionally renders and writes services/<svc>/README.md,
// and returns a table row, the parsed doc, and wrote=true if written.
func processService(slug string, writeReadme bool) (summaryEntry, *ParityDoc, bool, error) {
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

	wrote := false
	if writeReadme {
		readmePath := filepath.Join(servicesDirName, slug, readmeFileName)
		content := renderServiceReadme(slug, doc, guideExists(slug))

		if writeErr := writeFileAtomic(readmePath, []byte(content)); writeErr != nil {
			return summaryEntry{}, nil, false, fmt.Errorf("write %s: %w", readmePath, writeErr)
		}
		wrote = true
	}

	return newSummaryEntry(slug, doc), doc, wrote, nil
}

// logParseWarnings surfaces every parsed doc's Warnings via the structured
// logger, at Warn level, before run fails the build over them (see the
// totalWarnings check in run) -- a PARITY.md line that looks like an entry
// but doesn't parse, or an entry that parses but carries a status token
// classifyToken doesn't recognize, must be loud, not silently undercounted
// (gopherstack-udc7, gopherstack-7o96, gopherstack-cr41).
func logParseWarnings(ctx context.Context, log *slog.Logger, docs []*ParityDoc) {
	for _, doc := range docs {
		for _, w := range doc.Warnings {
			log.WarnContext(ctx, "PARITY.md entry did not parse", "detail", w)
		}
	}
}

// errUnparsedEntries is checkParseWarnings' sentinel: a PARITY.md line that
// looked like an ops:/families: entry but didn't parse as one, or an entry
// that parsed fine but carries a status token outside the closed vocabulary,
// must fail the run, not just undercount it -- the same principle as
// cmd/opcensus' ERROR-row change (gopherstack-c7s3, gopherstack-7o96,
// gopherstack-cr41).
var errUnparsedEntries = errors.New(
	"PARITY.md line(s) either didn't parse as an ops:/families: entry, or used a status token gendocs doesn't recognize",
)

// checkParseWarnings turns a nonzero Warnings count across docs into a hard
// error instead of a silent omission.
func checkParseWarnings(docs []*ParityDoc) error {
	n := 0
	for _, doc := range docs {
		n += len(doc.Warnings)
	}

	if n == 0 {
		return nil
	}

	return fmt.Errorf("%d %w (see warnings above) -- fix the manifest, don't silently undercount it",
		n, errUnparsedEntries)
}

// guideExists reports whether a hand-written docs/services/<svc>.md guide
// exists, so renderServiceReadme knows whether to link to it.
func guideExists(slug string) bool {
	_, err := os.Stat(filepath.Join(docsServicesDir, slug+".md"))

	return err == nil
}
