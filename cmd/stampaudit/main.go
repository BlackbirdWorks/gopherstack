// Command stampaudit reports whether each services/<svc>/PARITY.md
// last_audit_commit stamp still means what the manifest schema says it
// means (services/_PARITY_TEMPLATE.md: "HEAD when this manifest was
// written," re-audited via `git diff <last_audit_commit>..HEAD --
// services/<svc>/`).
//
// gopherstack-z31a found two independent defect classes:
//
//   - structural: the stamp's sha is unreachable from the target ref
//     because the audit ran on a branch that later got squash-merged, so
//     the diff protocol above has never worked once that branch landed.
//   - authoring: the stamp's sha predates its own last_audit_date by an
//     implausible margin, which is what a copied-forward stamp looks like.
//
// A THIRD, INVALID test was tried four separate times against this issue
// before this tool existed: "does the cited sha touch the service's own
// services/<svc>/ directory." last_audit_commit means HEAD-at-write-time,
// not "a commit about this service" -- one audit session routinely covers
// several services under one sha, so that test flags entirely legitimate
// stamps. Every one of the four false positives it produced (see the bd
// notes) reached for it despite an explicit prompt warning not to; the
// only fix that held was removing the capability, not wording around it.
// THAT TEST DOES NOT EXIST IN THIS PACKAGE. Do not add a service-directory
// diff helper here -- see gopherstack-z31a before you do.
//
// Usage:
//
//	go run ./cmd/stampaudit                       # report to stdout
//	go run ./cmd/stampaudit -threshold 14          # widen the date-gap threshold
//	go run ./cmd/stampaudit -json out.json         # also write full per-manifest detail
//	go run ./cmd/stampaudit -suggest=false          # omit merge-base suggestions
package main

import (
	"flag"
	"fmt"
	"os"
)

const defaultThresholdDays = 7

func main() {
	servicesDir := flag.String("dir", "services", "path to the services directory")
	repoDir := flag.String("repo", ".", "git repository root")
	ref := flag.String("ref", "origin/main", "ref to check sha reachability/merge-base against")
	threshold := flag.Int(
		"threshold",
		defaultThresholdDays,
		"stale-date threshold in days: flag when a sha's commit date predates last_audit_date by more than this",
	)
	jsonPath := flag.String("json", "", "also write full per-manifest detail to this path as JSON")
	suggest := flag.Bool("suggest", true, "print a merge-base(sha, ref) suggestion for unreachable citations")
	flag.Parse()

	if err := run(*servicesDir, *repoDir, *ref, *threshold, *jsonPath, *suggest); err != nil {
		fmt.Fprintln(os.Stderr, "stampaudit:", err)
		os.Exit(1)
	}
}

func run(servicesDir, repoDir, ref string, threshold int, jsonPath string, suggest bool) error {
	manifests, err := discoverManifests(servicesDir)
	if err != nil {
		return err
	}

	g := gitRunner{dir: repoDir}

	results := make([]result, 0, len(manifests))
	for _, m := range manifests {
		results = append(results, auditOne(g, ref, threshold, m))
	}

	if jsonPath != "" {
		if writeErr := writeJSON(jsonPath, results); writeErr != nil {
			return writeErr
		}
	}

	printReport(os.Stdout, results, threshold, ref, suggest)

	return nil
}
