package main

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const (
	signalDiscoveredFromParent = "discovered-from-closed-parent"
	signalStaleRefMissing      = "stale-file-ref-missing"
	signalStaleRefLineOOB      = "stale-file-ref-oob"

	minTitleWordLen = 4

	scoreStaleRefMissing = 3
	scoreStaleRefLineOOB = 2
)

// stopWords are filtered out of an issue title before computing topical
// overlap with a parent's close reason -- common English words would
// overlap with nearly every close reason and drown the signal.
//
//nolint:gochecknoglobals // read-only lookup table, same pattern as structfielddiff's dirModuleOverride
var stopWords = map[string]bool{
	"the": true, "and": true, "or": true, "of": true, "to": true, "for": true,
	"in": true, "on": true, "with": true, "by": true, "from": true, "that": true,
	"this": true, "is": true, "are": true, "was": true, "were": true, "never": true,
	"not": true, "its": true, "as": true, "at": true, "be": true, "been": true,
	"being": true, "will": true, "would": true, "can": true, "could": true,
	"should": true, "has": true, "have": true, "had": true, "does": true,
	"did": true, "but": true, "if": true, "then": true, "than": true, "when": true,
	"where": true, "which": true, "who": true, "whom": true, "whose": true,
	"into": true, "out": true, "over": true, "under": true, "again": true,
	"further": true, "once": true, "here": true, "there": true, "all": true,
	"any": true, "both": true, "each": true, "few": true, "more": true,
	"most": true, "other": true, "some": true, "such": true, "nor": true,
	"only": true, "own": true, "same": true, "very": true, "just": true,
	"don": true, "now": true, "already": true, "still": true, "while": true,
	"after": true, "before": true, "during": true, "about": true, "against": true,
	"between": true, "may": true, "might": true, "must": true,
}

var titleWordRe = regexp.MustCompile(`[A-Za-z][A-Za-z0-9]*`)

// suspicionRow is one candidate the suspicion pass surfaced. It is
// evidence for a human to check, never a verdict.
type suspicionRow struct {
	IssueID string `json:"issueId"`
	Title   string `json:"title"`
	Signal  string `json:"signal"`
	Detail  string `json:"detail"`
	Score   int    `json:"score"`
}

// significantWords lowercases title, keeps tokens of at least
// minTitleWordLen letters, and drops stopWords -- the residue is what
// distinguishes this issue's subject from any other.
func significantWords(title string) []string {
	var words []string

	seen := map[string]bool{}
	for _, w := range titleWordRe.FindAllString(strings.ToLower(title), -1) {
		if len(w) < minTitleWordLen || stopWords[w] || seen[w] {
			continue
		}

		seen[w] = true

		words = append(words, w)
	}

	return words
}

// discoveredFromCandidates flags open issues whose discovered-from parent
// is closed and whose close reason talks about the same subject, on the
// theory that a sweep closing itself sometimes fixes issues it spawned
// without ever going back to close them individually -- exactly the shape
// that left gopherstack-cqy3 open for five days after 69bbb940a fixed it.
//
// If the parent's own close reason already names the child issue's id, the
// parent explicitly spun it off as separate tracked work ("filed as
// gopherstack-X") rather than silently fixing it -- that is a normal,
// correctly-open issue, not this bug shape, so it is excluded rather than
// flagged.
func discoveredFromCandidates(issues map[string]issue, minOverlap int) []suspicionRow {
	var rows []suspicionRow

	for _, iss := range sortedIssues(issues) {
		if iss.closed() {
			continue
		}

		for _, dep := range iss.Dependencies {
			if dep.Type != depTypeDiscoveredFrom || dep.IssueID != iss.ID {
				continue
			}

			parent, ok := issues[dep.DependsOnID]
			if !ok || !parent.closed() || parent.CloseReason == "" {
				continue
			}
			if strings.Contains(strings.ToLower(parent.CloseReason), strings.ToLower(iss.ID)) {
				continue
			}

			overlap := overlapWords(significantWords(iss.Title), parent.CloseReason)
			if len(overlap) < minOverlap {
				continue
			}

			rows = append(rows, suspicionRow{
				IssueID: iss.ID,
				Title:   iss.Title,
				Signal:  signalDiscoveredFromParent,
				Detail: "parent " + dep.DependsOnID + " (closed) shares words " +
					strings.Join(overlap, ", ") + " with its close reason",
				Score: len(overlap),
			})
		}
	}

	return rows
}

func overlapWords(titleWords []string, closeReason string) []string {
	lower := strings.ToLower(closeReason)

	var overlap []string
	for _, w := range titleWords {
		if strings.Contains(lower, w) {
			overlap = append(overlap, w)
		}
	}

	return overlap
}

func sortedIssues(issues map[string]issue) []issue {
	out := make([]issue, 0, len(issues))
	for _, iss := range issues {
		out = append(out, iss)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out
}

var fileLineRe = regexp.MustCompile(`([A-Za-z0-9_/-]+\.go):(\d+)`)

// repoTopDirs bounds file:line extraction to paths that are plausibly this
// repo's own source -- an SDK source citation like "types.go:102" with no
// directory has no repo-relative meaning and would otherwise false-positive
// as a missing file.
//
//nolint:gochecknoglobals // read-only lookup table, same pattern as stopWords above
var repoTopDirs = []string{"services/", "pkgs/", "cmd/", "ui/", "test/"}

// staleRefCandidates flags open issues that cite a services/pkgs/cmd/ui/test
// file:line that no longer exists, or whose cited line number now exceeds
// the file's length -- weak evidence the code moved since the issue was
// filed and its premise is worth re-checking against current source rather
// than trusted verbatim.
func staleRefCandidates(issues map[string]issue, repoRoot string) []suspicionRow {
	var rows []suspicionRow

	for _, iss := range sortedIssues(issues) {
		if iss.closed() {
			continue
		}

		text := iss.Title + "\n" + iss.Description

		seen := map[string]bool{}
		for _, m := range fileLineRe.FindAllStringSubmatch(text, -1) {
			path, lineStr := m[1], m[2]
			if seen[path] || !hasRepoTopDir(path) {
				continue
			}

			seen[path] = true

			if row, ok := checkStaleRef(iss, repoRoot, path, lineStr); ok {
				rows = append(rows, row)
			}
		}
	}

	return rows
}

func hasRepoTopDir(path string) bool {
	for _, d := range repoTopDirs {
		if strings.HasPrefix(path, d) {
			return true
		}
	}

	return false
}

func checkStaleRef(iss issue, repoRoot, path, lineStr string) (suspicionRow, bool) {
	full := filepath.Join(repoRoot, path)

	info, err := os.Stat(full)
	if err != nil {
		return suspicionRow{
			IssueID: iss.ID,
			Title:   iss.Title,
			Signal:  signalStaleRefMissing,
			Detail:  path + " (cited line " + lineStr + ") no longer exists",
			Score:   scoreStaleRefMissing,
		}, true
	}
	if info.IsDir() {
		return suspicionRow{}, false
	}

	cited, err := strconv.Atoi(lineStr)
	if err != nil {
		return suspicionRow{}, false
	}

	n, err := countLines(full)
	if err != nil || cited <= n {
		return suspicionRow{}, false
	}

	return suspicionRow{
		IssueID: iss.ID,
		Title:   iss.Title,
		Signal:  signalStaleRefLineOOB,
		Detail:  path + " now has " + strconv.Itoa(n) + " lines, cited line " + lineStr,
		Score:   scoreStaleRefLineOOB,
	}, true
}

func countLines(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	if len(data) == 0 {
		return 0, nil
	}

	n := strings.Count(string(data), "\n")
	if data[len(data)-1] != '\n' {
		n++
	}

	return n, nil
}

// suspicionPass runs every suspicion signal and merges the results, ranked
// highest-score first. This is advisory: nothing here is closed
// automatically, and every row states the signal that produced it so a
// human can judge it directly instead of trusting a verdict.
func suspicionPass(issues map[string]issue, repoRoot string, minOverlap int) []suspicionRow {
	rows := append(discoveredFromCandidates(issues, minOverlap), staleRefCandidates(issues, repoRoot)...)

	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].Score != rows[j].Score {
			return rows[i].Score > rows[j].Score
		}

		return rows[i].IssueID < rows[j].IssueID
	})

	return rows
}
