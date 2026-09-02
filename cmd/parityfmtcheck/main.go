// Command parityfmtcheck checks two structural invariants of every
// services/<svc>/PARITY.md that every real consumer of the file (cmd/gendocs,
// cmd/stampaudit, cmd/staleclaims) depends on but none directly validates: a
// service: front-matter field that exists and names the directory it's
// actually in, and no unresolved git merge-conflict marker anywhere in the
// file.
//
// PARITY.md front-matter is YAML-*shaped*, not valid YAML (see
// services/_PARITY_TEMPLATE.md and cmd/gendocs/parser.go's package doc): a
// naive yaml.safe_load over the block routinely fails on real, correctly
// authored manifests -- unquoted note: prose containing commas/colons/braces,
// or the deliberately unfenced style some manifests use instead of the
// template's opening/closing "---" (gopherstack-lj4n: 34 files flagged this
// way turned out to be zero real defects once checked against the tools that
// actually read this file). This tool intentionally does NOT re-implement
// that strict check, and does not flag an unrecognized top-level key either
// -- the real schema tolerates those as forward-compatible
// (cmd/gendocs/parser.go's skipUnknownBlock; real manifests carry fields like
// sibling_sdk_modules, botocore_model, items_still_open that a second,
// independently-maintained reserved-key list here would only drift out of
// sync with). Full ops:/families: entry-level tolerant parsing already gates
// `make docs` (cmd/gendocs's checkParseWarnings) -- duplicating that here
// would risk exactly the two-parsers-drift failure mode this tool exists to
// avoid. This tool's checks are the narrower, side-effect-free ones nothing
// else validates, runnable in CI without invoking full doc generation.
//
// Usage:
//
//	go run ./cmd/parityfmtcheck                # report to stdout
//	go run ./cmd/parityfmtcheck -dir services   # (default) check this dir
//	go run ./cmd/parityfmtcheck -json out.json  # also write full result list as JSON
//
// Exit codes: 0 every manifest's front-matter checks out clean, 1 a run
// error (can't read the services directory or a manifest), 2 at least one
// manifest failed a front-matter check.
package main

import (
	"flag"
	"fmt"
	"os"
)

const (
	exitClean    = 0
	exitRunError = 1
	exitFindings = 2
)

func main() {
	dir := flag.String("dir", "services", "path to the services directory")
	jsonPath := flag.String("json", "", "also write full per-manifest result list to this path as JSON")
	flag.Parse()

	results, err := run(*dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "parityfmtcheck:", err)
		os.Exit(exitRunError)
	}

	if *jsonPath != "" {
		if writeErr := writeJSON(*jsonPath, results); writeErr != nil {
			fmt.Fprintln(os.Stderr, "parityfmtcheck:", writeErr)
			os.Exit(exitRunError)
		}
	}

	printReport(os.Stdout, results)
	os.Exit(exitCode(results))
}

func run(dir string) ([]result, error) {
	manifests, err := discoverManifests(dir)
	if err != nil {
		return nil, err
	}

	results := make([]result, 0, len(manifests))
	for _, m := range manifests {
		r := checkManifest(m.service, m.content)
		r.path = m.path
		results = append(results, r)
	}

	return results, nil
}

func exitCode(results []result) int {
	for _, r := range results {
		if len(r.issues) > 0 {
			return exitFindings
		}
	}

	return exitClean
}
