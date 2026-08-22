package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const (
	parityFileName = "PARITY.md"

	commitFieldPrefix = "last_audit_commit:"
	dateFieldPrefix   = "last_audit_date:"
)

// hexPrefixRe matches a plausible commit-sha prefix at the start of a
// last_audit_commit value: git's shortest unambiguous abbreviation in this
// repo is at least 7 hex digits, up to a full 40-digit sha.
var hexPrefixRe = regexp.MustCompile(`^[0-9a-f]{7,40}`)

// manifest is the subset of a services/<svc>/PARITY.md front-matter this
// tool reads.
type manifest struct {
	service        string
	rawCommit      string
	rawDate        string
	rawCommitFound bool
	rawDateFound   bool
}

// discoverManifests lists services/<svc>/PARITY.md for every immediate
// subdirectory of servicesDir that has one, sorted by service name. A
// service directory with no PARITY.md is silently skipped, same as
// cmd/checkpins.
func discoverManifests(servicesDir string) ([]manifest, error) {
	entries, err := os.ReadDir(servicesDir)
	if err != nil {
		return nil, fmt.Errorf("read dir %s: %w", servicesDir, err)
	}

	var slugs []string
	for _, e := range entries {
		if e.IsDir() {
			slugs = append(slugs, e.Name())
		}
	}
	sort.Strings(slugs)

	manifests := make([]manifest, 0, len(slugs))
	for _, slug := range slugs {
		path := filepath.Join(servicesDir, slug, parityFileName)

		data, readErr := os.ReadFile(path)
		if readErr != nil {
			if os.IsNotExist(readErr) {
				continue
			}

			return nil, fmt.Errorf("read %s: %w", path, readErr)
		}

		manifests = append(manifests, parseManifest(slug, string(data)))
	}

	return manifests, nil
}

// parseManifest extracts last_audit_commit and last_audit_date from a
// PARITY.md's front-matter. It is the pure decision function
// discoverManifests delegates to, kept separate so it can be unit tested
// against literal fragments without touching the filesystem.
func parseManifest(service, content string) manifest {
	m := manifest{service: service}

	if value, _, found := extractField(content, commitFieldPrefix); found {
		m.rawCommit = value
		m.rawCommitFound = true
	}
	if value, _, found := extractField(content, dateFieldPrefix); found {
		m.rawDate = value
		m.rawDateFound = true
	}

	return m
}

// extractField scans content for the first column-0 line starting with
// fieldPrefix and returns its value (everything after the prefix, up to a
// trailing " #" comment) plus that comment text. found is false if no such
// line exists.
func extractField(content, fieldPrefix string) (string, string, bool) {
	for line := range strings.SplitSeq(content, "\n") {
		rest, ok := strings.CutPrefix(line, fieldPrefix)
		if !ok {
			continue
		}

		before, after, hasComment := strings.Cut(rest, " #")
		if !hasComment {
			return strings.TrimSpace(rest), "", true
		}

		return strings.TrimSpace(before), strings.TrimSpace(after), true
	}

	return "", "", false
}

// citationKind classifies a last_audit_commit value by shape, before any
// git lookup runs.
type citationKind int

const (
	// citationSHA: the value starts with a plausible hex commit prefix.
	citationSHA citationKind = iota
	// citationPlaceholder: no hex prefix at all (HEAD, PENDING, "(uncommitted...", etc.) --
	// a schema violation, but not a provenance claim this tool can check.
	citationPlaceholder
	// citationAbsent: the manifest has no last_audit_commit field at all.
	citationAbsent
)

// citation is a last_audit_commit value classified into a hex-sha prefix
// (if any) plus whatever trailed it.
type citation struct {
	raw    string
	sha    string // hexPrefixRe match, "" if kind != citationSHA
	suffix string // raw with sha trimmed off, e.g. "+uncommitted"
	kind   citationKind
}

func classifyCitation(m manifest) citation {
	if !m.rawCommitFound || strings.TrimSpace(m.rawCommit) == "" {
		return citation{kind: citationAbsent}
	}

	raw := strings.TrimSpace(m.rawCommit)

	sha := hexPrefixRe.FindString(strings.ToLower(raw))
	if sha == "" {
		return citation{raw: raw, kind: citationPlaceholder}
	}

	return citation{
		raw:    raw,
		sha:    sha,
		suffix: strings.TrimSpace(raw[len(sha):]),
		kind:   citationSHA,
	}
}
