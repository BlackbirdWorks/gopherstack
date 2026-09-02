package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const parityFileName = "PARITY.md"

// topLevelKeyRe matches a front-matter key at column 0, e.g. "service: ec2".
// Mirrors cmd/gendocs/parser.go's topLevelKeyRe and cmd/staleclaims/manifest.go's
// own copy of it: same file shape, each tool only needs whatever slice of
// structure it's responsible for.
var topLevelKeyRe = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*):(.*)$`)

// manifest is one discovered services/<svc>/PARITY.md, not yet checked.
type manifest struct {
	service string
	path    string
	content string
}

// discoverManifests lists services/<svc>/PARITY.md for every immediate
// subdirectory of dir that has one, sorted by service slug. A service
// directory with no PARITY.md is silently skipped, same as cmd/checkpins
// and cmd/stampaudit.
func discoverManifests(dir string) ([]manifest, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read dir %s: %w", dir, err)
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
		path := filepath.Join(dir, slug, parityFileName)

		data, readErr := os.ReadFile(path)
		if readErr != nil {
			if os.IsNotExist(readErr) {
				continue
			}

			return nil, fmt.Errorf("read %s: %w", path, readErr)
		}

		manifests = append(manifests, manifest{service: slug, path: path, content: string(data)})
	}

	return manifests, nil
}

// result is one manifest's check outcome.
type result struct {
	service string
	path    string
	docSlug string
	issues  []string
}

// extractFrontmatter returns the front-matter lines of a PARITY.md file,
// tolerating a missing opening/closing "---" the same way
// cmd/gendocs/parser.go's extractFrontmatter and cmd/staleclaims/manifest.go's
// extractFrontmatterRange do: scan from just after an opening "---" if
// present (line 0 otherwise) up to whichever comes first, a "---" line, a
// "## " Markdown heading, or end of file.
func extractFrontmatter(lines []string) []string {
	start := 0
	if len(lines) > 0 && strings.TrimSpace(lines[0]) == "---" {
		start = 1
	}

	for i := start; i < len(lines); i++ {
		t := strings.TrimSpace(lines[i])
		if t == "---" || strings.HasPrefix(t, "## ") {
			return lines[start:i]
		}
	}

	return lines[start:]
}

// cleanScalar trims a single-line front-matter scalar value: strips a
// trailing " #..." comment, then surrounding quotes. Mirrors
// cmd/gendocs/parser.go's cleanScalar.
func cleanScalar(raw string) string {
	v := strings.TrimSpace(raw)
	if strings.HasPrefix(v, "#") {
		return ""
	}
	if idx := strings.Index(v, " #"); idx >= 0 {
		v = strings.TrimSpace(v[:idx])
	}

	return strings.Trim(v, `"'`)
}

// findServiceField returns the value of front-matter's first column-0
// "service:" line, and whether one was found at all.
func findServiceField(fm []string) (string, bool) {
	for _, line := range fm {
		m := topLevelKeyRe.FindStringSubmatch(line)
		if m == nil || m[1] != "service" {
			continue
		}

		return cleanScalar(m[2]), true
	}

	return "", false
}

// findMergeConflictMarker returns the 1-based line number of the first
// unresolved git merge-conflict marker in content, or 0 if none. Unlike an
// unrecognized top-level key -- which the real schema tolerates as
// forward-compatible (cmd/gendocs/parser.go's skipUnknownBlock; real
// manifests carry extra fields like sibling_sdk_modules, botocore_model,
// items_still_open that no reserved-key list here should have to keep in
// lockstep with) -- a conflict marker is never legitimate PARITY.md content
// under any version of the schema.
func findMergeConflictMarker(lines []string) int {
	markers := [3]string{"<<<<<<<", "=======", ">>>>>>>"}

	for i, line := range lines {
		for _, marker := range markers {
			if strings.HasPrefix(line, marker) {
				return i + 1
			}
		}
	}

	return 0
}

// checkManifest checks content (a services/<slug>/PARITY.md's raw bytes)
// against the two structural invariants every real consumer of this file
// (cmd/gendocs, cmd/stampaudit, cmd/staleclaims) implicitly depends on but
// none directly validates: a real, matching service: identity, and no
// unresolved merge-conflict marker. It is the pure decision function
// discoverManifests' caller delegates to, kept separate so it can be unit
// tested against literal fragments without touching the filesystem.
func checkManifest(slug, content string) result {
	r := result{service: slug}

	lines := strings.Split(content, "\n")

	if ln := findMergeConflictMarker(lines); ln > 0 {
		r.issues = append(r.issues, fmt.Sprintf("line %d: unresolved git merge-conflict marker", ln))
	}

	fm := extractFrontmatter(lines)

	docSlug, found := findServiceField(fm)
	r.docSlug = docSlug

	switch {
	case !found || docSlug == "":
		r.issues = append(r.issues, "service: field missing or empty")
	case docSlug != slug:
		r.issues = append(r.issues, fmt.Sprintf("service: %q does not match directory %q", docSlug, slug))
	}

	return r
}
