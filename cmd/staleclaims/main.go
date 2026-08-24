// Command staleclaims flags a services/<svc>/PARITY.md open-list entry
// (gaps:/items_still_open:/residual_gaps:) or "not fixed" Notes paragraph
// whose named op/field a LATER part of the same file records as fixed --
// the shape gopherstack-anjf measured costing three dispatched workers at
// already-fixed gaps (codebuild, ecr, pipes) plus a near-miss (ecs) and a
// duplicate bug filing (pipes again).
//
// See detect.go's package doc comment for exactly what "later" means for
// each of the two checks it runs (checkStructured, checkProse), plus the
// separate, narrower checkSymbolExistence.
//
// Usage:
//
//	go run ./cmd/staleclaims                    # report to stdout
//	go run ./cmd/staleclaims -json out.json      # also write full detail as JSON
package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	servicesDir := flag.String("dir", "services", "path to the services directory")
	jsonPath := flag.String("json", "", "also write full per-finding detail to this path as JSON")
	symbols := flag.Bool("symbols", false,
		"also run checkSymbolExistence (opt-in: measured near-0% true positive rate on this corpus, see detect.go)")
	flag.Parse()

	if err := run(*servicesDir, *jsonPath, *symbols); err != nil {
		fmt.Fprintln(os.Stderr, "staleclaims:", err)
		os.Exit(1)
	}
}

func run(servicesDir, jsonPath string, symbols bool) error {
	manifests, err := discoverManifests(servicesDir)
	if err != nil {
		return err
	}

	svcSrc := buildSourceIndex(servicesDir)

	var findings []finding
	for _, m := range manifests {
		findings = append(findings, detect(m, svcSrc, symbols)...)
	}

	if jsonPath != "" {
		if writeErr := writeJSON(jsonPath, findings); writeErr != nil {
			return writeErr
		}
	}

	printReport(os.Stdout, findings)

	return nil
}
