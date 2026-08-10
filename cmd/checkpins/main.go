// Command checkpins verifies that every services/<svc>/PARITY.md sdk_module
// front-matter pin (aws-sdk-go-v2/service/<name>@v<version>) matches the
// version go.mod actually requires for that module. A stale pin silently
// undermines every wire-shape claim in the file, since those claims were
// checked against the pinned version, not whatever go.mod carries now.
//
// Usage:
//
//	go run ./cmd/checkpins
//
// Exits non-zero if any PARITY.md pin disagrees with go.mod, if a recorded
// module isn't in go.mod without documenting why (see isModuleCacheOnly),
// or if sdk_module is missing or has no parseable @version — an
// unverifiable pin is exactly the failure mode this check exists to catch,
// not a case to wave through.
package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"golang.org/x/mod/modfile"
)

var errPinMismatch = errors.New("PARITY.md sdk_module pin(s) do not match go.mod")

const (
	servicesDirName = "services"
	parityFileName  = "PARITY.md"
	goModPath       = "go.mod"

	sdkServiceModulePrefix = "github.com/aws/aws-sdk-go-v2/service/"
	sdkModuleFieldPrefix   = "sdk_module:"
)

// pinRe matches an sdk_module value shaped like
// "aws-sdk-go-v2/service/dlm@v1.39.4", capturing the module name and the
// version including its leading "v".
var pinRe = regexp.MustCompile(`^aws-sdk-go-v2/service/([A-Za-z0-9_-]+)@(v[0-9][0-9A-Za-z.\-+]*)$`)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "checkpins:", err)
		os.Exit(1)
	}
}

func run() error {
	goModVersions, err := loadGoModVersions(goModPath)
	if err != nil {
		return err
	}

	slugs, err := discoverServiceSlugs(servicesDirName)
	if err != nil {
		return err
	}

	var mismatches []string
	for _, slug := range slugs {
		result, checkErr := checkService(slug, goModVersions)
		if checkErr != nil {
			return checkErr
		}

		if result.kind == resultMismatch {
			mismatches = append(mismatches, result.message)
		}
	}

	if len(mismatches) > 0 {
		for _, m := range mismatches {
			fmt.Fprintln(os.Stdout, m)
		}

		return fmt.Errorf("%w: %d found", errPinMismatch, len(mismatches))
	}

	fmt.Fprintf(os.Stdout, "checkpins: %d services checked, all sdk_module pins match go.mod\n", len(slugs))

	return nil
}

// discoverServiceSlugs lists the immediate subdirectories of dir, sorted.
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

// loadGoModVersions parses go.mod and returns the pinned version (with its
// leading "v") of every aws-sdk-go-v2/service/<name> requirement, keyed by
// <name>. Uses golang.org/x/mod/modfile rather than hand-parsing so both
// block-style and single-line `require` statements are covered correctly.
func loadGoModVersions(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	f, err := modfile.Parse(path, data, nil)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}

	versions := make(map[string]string, len(f.Require))
	for _, req := range f.Require {
		name, ok := strings.CutPrefix(req.Mod.Path, sdkServiceModulePrefix)
		if !ok {
			continue
		}
		versions[name] = req.Mod.Version
	}

	return versions, nil
}

type resultKind int

const (
	resultOK resultKind = iota
	resultMismatch
)

type checkResult struct {
	message string
	kind    resultKind
}

// checkService reads services/<slug>/PARITY.md and evaluates its sdk_module
// pin against goModVersions. A service directory with no PARITY.md (e.g.
// qldb, qldbsession) is silently OK — this checker only audits pins that
// exist.
func checkService(slug string, goModVersions map[string]string) (checkResult, error) {
	path := filepath.Join(servicesDirName, slug, parityFileName)

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return checkResult{kind: resultOK}, nil
		}

		return checkResult{}, fmt.Errorf("read %s: %w", path, err)
	}

	return evaluatePin(slug, string(data), goModVersions), nil
}

// evaluatePin is the pure decision function checkService delegates to, kept
// separate so it can be unit tested against literal PARITY.md fragments
// without touching the filesystem.
func evaluatePin(slug, content string, goModVersions map[string]string) checkResult {
	valueLine, commentText, found := extractSDKModule(content)
	if !found {
		return checkResult{
			kind:    resultMismatch,
			message: fmt.Sprintf("%s: no sdk_module field in PARITY.md", slug),
		}
	}

	module, version, ok := parsePinValue(valueLine)
	if !ok {
		return checkResult{
			kind: resultMismatch,
			message: fmt.Sprintf("%s: sdk_module value %q has no parseable @version",
				slug, strings.TrimSpace(valueLine)),
		}
	}

	actual, inGoMod := goModVersions[module]
	if !inGoMod {
		if isModuleCacheOnly(commentText) {
			return checkResult{kind: resultOK}
		}

		return checkResult{
			kind: resultMismatch,
			message: fmt.Sprintf("%s: recorded module %q not found in go.mod (and not documented as module-cache-only)",
				slug, module),
		}
	}

	if actual != version {
		return checkResult{
			kind:    resultMismatch,
			message: fmt.Sprintf("%s: recorded %s, go.mod %s", slug, version, actual),
		}
	}

	return checkResult{kind: resultOK}
}

// extractSDKModule scans content for the first column-0 "sdk_module:" line
// and returns its value (everything after the colon) plus the text of its
// trailing "# ..." comment, joined with any indented continuation lines
// (opsworks's module-cache-only note wraps across several). found is false
// if no such line exists.
func extractSDKModule(content string) (string, string, bool) {
	lines := strings.Split(content, "\n")
	for i, line := range lines {
		rest, ok := strings.CutPrefix(line, sdkModuleFieldPrefix)
		if !ok {
			continue
		}

		value, comment := splitValueComment(rest)
		commentParts := []string{comment}
		for j := i + 1; j < len(lines); j++ {
			cont := strings.TrimSpace(lines[j])
			indented := strings.HasPrefix(lines[j], " ") || strings.HasPrefix(lines[j], "\t")
			if cont == "" || !indented {
				break
			}
			commentParts = append(commentParts, strings.TrimSpace(strings.TrimPrefix(cont, "#")))
		}

		return value, strings.Join(commentParts, " "), true
	}

	return "", "", false
}

// splitValueComment splits an sdk_module line's remainder at its first
// " #" into the value and the comment text (without the leading "#").
func splitValueComment(rest string) (string, string) {
	before, after, found := strings.Cut(rest, " #")
	if !found {
		return strings.TrimSpace(rest), ""
	}

	return strings.TrimSpace(before), strings.TrimSpace(after)
}

// parsePinValue parses a raw sdk_module value (already stripped of its
// trailing comment) into a module name and version.
func parsePinValue(value string) (string, string, bool) {
	v := strings.Trim(strings.TrimSpace(value), `"'`)

	m := pinRe.FindStringSubmatch(v)
	if m == nil {
		return "", "", false
	}

	return m[1], m[2], true
}

// isModuleCacheOnly reports whether commentText documents the
// audited-from-module-cache-but-not-a-go.mod-dependency convention
// services/opsworks/PARITY.md uses.
func isModuleCacheOnly(commentText string) bool {
	return strings.Contains(strings.ToLower(commentText), "not a go.mod dependency")
}
