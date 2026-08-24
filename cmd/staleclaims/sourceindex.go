package main

import (
	"os"
	"path/filepath"
	"regexp"
)

// identRe extracts every identifier-like token from a Go source file, for
// checkSymbolExistence's cheap substring existence test. This deliberately
// does not use go/parser: an exact identifier match (including inside a
// comment or a string literal) is exactly what's needed to decide "does this
// name still appear anywhere in the package," and a plain scan is far
// cheaper than parsing every services/<svc> package for one boolean per
// candidate token.
var identRe = regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_]*`)

// buildSourceIndex returns a memoized func(service) -> set of every
// identifier-like token appearing anywhere in services/<service>/*.go
// (including _test.go -- a symbol still referenced only by a test is not
// "gone"). servicesDir is the same -dir root the caller passed to
// discoverManifests.
func buildSourceIndex(servicesDir string) func(service string) map[string]bool {
	cache := map[string]map[string]bool{}

	return func(service string) map[string]bool {
		if idx, ok := cache[service]; ok {
			return idx
		}

		idx := scanServiceIdentifiers(filepath.Join(servicesDir, service))
		cache[service] = idx

		return idx
	}
}

func scanServiceIdentifiers(dir string) map[string]bool {
	idx := map[string]bool{}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return idx
	}

	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".go" {
			continue
		}

		data, readErr := os.ReadFile(filepath.Join(dir, e.Name()))
		if readErr != nil {
			continue
		}

		for _, tok := range identRe.FindAllString(string(data), -1) {
			idx[tok] = true
		}
	}

	return idx
}
