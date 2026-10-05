package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

func codeWordRe(code string) *regexp.Regexp {
	return regexp.MustCompile(`(^|[^A-Za-z0-9_])` + regexp.QuoteMeta(code) + `($|[^A-Za-z0-9_])`)
}

// serviceDirOf returns the services/<dir> prefix of a repo-relative file path.
func serviceDirOf(file string) string {
	parts := strings.Split(filepath.ToSlash(file), "/")
	if len(parts) < minPathParts {
		return ""
	}

	return filepath.Join(parts[0], parts[1])
}

// markRecorded flags findings whose code appears as a whole word in their
// service's PARITY.md.
func markRecorded(repoRoot string, findings []finding) {
	parity := map[string]string{}

	for i := range findings {
		dir := serviceDirOf(findings[i].File)
		if dir == "" {
			continue
		}

		text, ok := parity[dir]
		if !ok {
			b, _ := os.ReadFile(filepath.Join(repoRoot, dir, "PARITY.md"))
			text = string(b)
			parity[dir] = text
		}

		if text != "" && codeWordRe(findings[i].Code).MatchString(text) {
			findings[i].Recorded = true
		}
	}
}

const minPathParts = 3
