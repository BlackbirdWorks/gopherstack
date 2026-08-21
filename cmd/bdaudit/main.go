// Command bdaudit finds bd issues whose recorded state disagrees with what
// git history actually shows, and reports them -- it never writes to bd.
//
// The concrete cost this guards against: gopherstack-cqy3 (cloudformation
// stack policy never enforced on UpdateStack) was fixed on 2026-08-15 in
// commit 69bbb940a, as a side effect of closing its parent issue -- whose
// own close reason names the cloudformation fix explicitly. cqy3 itself was
// never closed. Five days later it was dispatched to a worker as live P2
// work, and a full agent run was spent rediscovering a fix already on main.
// bd does not parse commit messages or infer closure through a parent's
// close reason, so nothing else in the toolchain would have caught this.
//
// Three independent checks:
//
//  1. Trailer check (deterministic). Scans commits in -range for
//     "Closes|Fixes|Resolves gopherstack-<id>" trailers and flags any id
//     bd does not show as closed. This is the literal ask behind
//     gopherstack-101r: 45 issues shipped this way in one session because
//     bd never parses commit messages.
//
//  2. Typo check (deterministic, the inverse of 1). Flags a trailer id
//     that does not exist in bd at all -- cheap, and it has bitten before
//     (a ledger and a PR cited gopherstack-mtqf from truncated `bd create`
//     output; the real id was gopherstack-c7s3).
//
//  3. Suspicion pass (heuristic, NOT deterministic). cqy3's fixing commit
//     carried no trailer for cqy3 -- it was closed by implication through
//     a parent issue's sweep, a shape check 1 structurally cannot see no
//     matter how wide its range. This pass flags open issues whose
//     discovered-from parent is closed and whose close reason shares
//     unusual words with the issue's own title, and open issues whose
//     cited file:line no longer matches the source tree. These are leads
//     for a human, not verdicts: expect false positives, and see the
//     ranked SUSPICION LIST section for the signal behind each row.
//
// This tool only reports. It never calls bd, never closes anything, and
// exits non-zero only for the two deterministic checks -- the suspicion
// list never affects the exit code.
//
// Usage:
//
//	go run ./cmd/bdaudit                        # origin/main..HEAD, this branch's own commits
//	go run ./cmd/bdaudit -full-history           # every commit reachable from -base
//	go run ./cmd/bdaudit -range origin/main...HEAD
//	go run ./cmd/bdaudit -json out.json
//	go run ./cmd/bdaudit -no-suspicion           # only the two deterministic checks
package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	issuesPath := flag.String("issues", ".beads/issues.jsonl", "path to bd's committed JSONL export")
	repoRoot := flag.String("repo", ".", "repository root (also passed to git -C)")
	base := flag.String("base", "origin/main", "ref representing the main branch tip")
	rangeFlag := flag.String("range", "", "git log range/ref, passed verbatim; overrides -base/-full-history")
	fullHistory := flag.Bool(
		"full-history",
		false,
		"scan every commit reachable from -base instead of just -base..HEAD",
	)
	noSuspicion := flag.Bool(
		"no-suspicion",
		false,
		"skip the heuristic suspicion pass; run only the two deterministic checks",
	)
	minOverlap := flag.Int(
		"min-overlap",
		defaultMinOverlap,
		"minimum shared-word count for the discovered-from suspicion signal",
	)
	jsonPath := flag.String("json", "", "also write the full report to this path as JSON")
	flag.Parse()

	rep, err := run(runOptions{
		issuesPath:  *issuesPath,
		repoRoot:    *repoRoot,
		base:        *base,
		rangeExpr:   *rangeFlag,
		fullHistory: *fullHistory,
		noSuspicion: *noSuspicion,
		minOverlap:  *minOverlap,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "bdaudit:", err)
		os.Exit(1)
	}

	if *jsonPath != "" {
		if writeErr := writeJSONReport(*jsonPath, rep); writeErr != nil {
			fmt.Fprintln(os.Stderr, "bdaudit:", writeErr)
			os.Exit(1)
		}
	}

	printReport(os.Stdout, rep)

	if len(rep.NotClosed) > 0 || len(rep.Typos) > 0 {
		os.Exit(1)
	}
}

const defaultMinOverlap = 2

type runOptions struct {
	issuesPath  string
	repoRoot    string
	base        string
	rangeExpr   string
	fullHistory bool
	noSuspicion bool
	minOverlap  int
}

func (o runOptions) effectiveRange() string {
	if o.rangeExpr != "" {
		return o.rangeExpr
	}
	if o.fullHistory {
		return o.base
	}

	return o.base + "..HEAD"
}

func run(opts runOptions) (report, error) {
	issues, err := loadIssues(opts.issuesPath)
	if err != nil {
		return report{}, err
	}

	rangeExpr := opts.effectiveRange()

	commits, err := fetchCommits(opts.repoRoot, rangeExpr)
	if err != nil {
		return report{}, err
	}

	notClosed, typos := scanTrailers(commits, issues)

	var suspicion []suspicionRow
	if !opts.noSuspicion {
		suspicion = suspicionPass(issues, opts.repoRoot, opts.minOverlap)
	}

	return report{
		Range:          rangeExpr,
		CommitsScanned: len(commits),
		NotClosed:      notClosed,
		Typos:          typos,
		Suspicion:      suspicion,
	}, nil
}
