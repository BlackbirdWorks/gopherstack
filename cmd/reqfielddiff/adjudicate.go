package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// loadParityLines reads a service's PARITY.md as lines; a missing file is no lines.
func loadParityLines(serviceDir string) []string {
	b, err := os.ReadFile(filepath.Join(serviceDir, "PARITY.md"))
	if err != nil {
		return nil
	}

	return strings.Split(string(b), "\n")
}

func wordRe(s string) *regexp.Regexp {
	return regexp.MustCompile(`(^|[^A-Za-z0-9_])` + regexp.QuoteMeta(s) + `($|[^A-Za-z0-9_])`)
}

// isRecorded reports whether one PARITY.md line names both op and field.
func isRecorded(lines []string, op, field string) bool {
	opRe, fieldRe := wordRe(op), wordRe(field)

	for _, l := range lines {
		if opRe.MatchString(l) && fieldRe.MatchString(l) {
			return true
		}
	}

	return false
}

// splitRecorded moves findings whose op and field share a PARITY.md line out
// of r.Findings into r.Recorded.
func splitRecorded(r *serviceReport, lines []string) {
	if len(lines) == 0 {
		return
	}

	kept := r.Findings[:0:0]

	for _, f := range r.Findings {
		if isRecorded(lines, f.Op, f.Field.Name) {
			r.Recorded = append(r.Recorded, f)

			continue
		}

		kept = append(kept, f)
	}

	r.Findings = kept
}
