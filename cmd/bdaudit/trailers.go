package main

import (
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"sort"
	"strings"
)

const (
	recordSep = "\x1e"
	fieldSep  = "\x1f"

	maxCitingCommits = 3
)

type gitCommit struct {
	Hash string
	Body string
}

// fetchCommits runs `git log <rangeExpr>` and returns each commit's hash and
// full message body. rangeExpr is passed to git verbatim, so it accepts
// anything git log accepts: "origin/main..HEAD", a bare ref for full
// history, "A..B", etc.
func fetchCommits(repoDir, rangeExpr string) ([]gitCommit, error) {
	cmd := exec.CommandContext(
		context.Background(),
		"git", "-C", repoDir, "log", rangeExpr, "--format=%H"+fieldSep+"%B"+recordSep,
	)

	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git log %s: %w", rangeExpr, err)
	}

	var commits []gitCommit
	for rec := range strings.SplitSeq(string(out), recordSep) {
		rec = strings.TrimLeft(rec, "\n")
		if rec == "" {
			continue
		}

		hash, body, found := strings.Cut(rec, fieldSep)
		if !found {
			continue
		}

		commits = append(commits, gitCommit{Hash: hash, Body: body})
	}

	return commits, nil
}

// trailerKeyword matches the closing-trailer verbs this repo actually uses
// (observed in git history: always present-tense "Closes"/"Fixes"/
// "Resolves", never bare "Close" or past-tense "Closed" as a real trailer).
const trailerKeyword = `(?:closes|fixes|resolves)`

// trailerClauseRe finds a keyword followed by a chain of gopherstack-<id>
// tokens joined by "," and/or "and", optionally repeating the keyword per
// item ("closes X, closes Y"). The chain stops as soon as the connector
// isn't immediately "," or "and" -- so free-text asides between ids ("--
// and corrects...", ", and records the follow-ups filed during the sweep:
// gopherstack-cnhp") correctly break the chain instead of swallowing
// unrelated ids that were merely mentioned nearby.
var trailerClauseRe = regexp.MustCompile(
	`(?i)\b` + trailerKeyword + `\b\s+gopherstack-[0-9a-zA-Z]+` +
		`(?:(?:,\s*|\s+and\s+)(?:` + trailerKeyword + `\s+)?gopherstack-[0-9a-zA-Z]+)*`,
)

var idRe = regexp.MustCompile(`(?i)gopherstack-[0-9a-zA-Z]+`)

// extractClosedIDs returns the deduplicated, lowercased set of issue ids a
// commit message's Closes/Fixes/Resolves trailers claim to close.
func extractClosedIDs(body string) []string {
	seen := map[string]bool{}

	for _, clause := range trailerClauseRe.FindAllString(body, -1) {
		for _, id := range idRe.FindAllString(clause, -1) {
			seen[strings.ToLower(id)] = true
		}
	}

	if len(seen) == 0 {
		return nil
	}

	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	return ids
}

// trailerFinding is one issue id cited by a closing trailer, together with
// the commits that cited it.
type trailerFinding struct {
	IssueID string   `json:"issueId"`
	Status  string   `json:"status,omitempty"` // bd status at scan time; empty for a typo'd id
	Commits []string `json:"commits"`
}

// scanTrailers walks every commit in the range once, extracting closing
// trailers and cross-checking each cited id against bd's exported state.
// notClosed holds ids bd still shows open/in-progress/etc despite a commit
// claiming to close them -- this is the issue's literal ask. typos holds
// ids that don't exist in bd at all, catching a truncated/mistyped id
// before it misleads a future reader.
func scanTrailers(commits []gitCommit, issues map[string]issue) ([]trailerFinding, []trailerFinding) {
	byID := map[string][]string{}

	var order []string
	for _, c := range commits {
		for _, id := range extractClosedIDs(c.Body) {
			if _, ok := byID[id]; !ok {
				order = append(order, id)
			}
			byID[id] = append(byID[id], c.Hash)
		}
	}

	var notClosed, typos []trailerFinding
	for _, id := range order {
		commitHashes := byID[id]
		if len(commitHashes) > maxCitingCommits {
			commitHashes = commitHashes[:maxCitingCommits]
		}

		iss, known := issues[id]
		switch {
		case !known:
			typos = append(typos, trailerFinding{IssueID: id, Commits: commitHashes})
		case !iss.closed():
			notClosed = append(notClosed, trailerFinding{IssueID: id, Status: iss.Status, Commits: commitHashes})
		}
	}

	return notClosed, typos
}
