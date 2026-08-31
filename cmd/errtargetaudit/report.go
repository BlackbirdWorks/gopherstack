package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// lowResolutionThreshold gates the implausible-resolution guard --
// cmd/reqfielddiff/cmd/reqfieldscan's identical discipline, and this
// package's doc comment explains why it is not optional -- if a service
// emits 200 error codes and this scan resolves 3 to operations, that is a
// bug in this tool, not a finding about the service.
const lowResolutionThreshold = 0.5

// minOpsForResolutionGuard avoids firing the guard on a tiny service where a
// low ratio is noise at small N.
const minOpsForResolutionGuard = 5

func coverageWarnings(sr serviceScan) []string {
	var warnings []string

	if sr.OpsGroundTruth == 0 {
		return warnings
	}

	if sr.OpsResolved == 0 {
		warnings = append(warnings, fmt.Sprintf(
			"ZERO of %d operations with SDK ground truth resolved to an emulator handler at all -- "+
				"treat this service as UNSCANNED, not clean; this scan likely doesn't recognise its "+
				"dispatch or naming convention", sr.OpsGroundTruth))

		return warnings
	}

	if sr.OpsGroundTruth >= minOpsForResolutionGuard {
		ratio := float64(sr.OpsResolved) / float64(sr.OpsGroundTruth)
		if ratio < lowResolutionThreshold {
			warnings = append(warnings, fmt.Sprintf(
				"only %d/%d (%.0f%%) of operations with SDK ground truth resolved to a handler -- "+
					"treat this service's coverage as UNVERIFIED, not clean; likely a resolution gap "+
					"in this tool, not a service this thin",
				sr.OpsResolved, sr.OpsGroundTruth, pct(sr.OpsResolved, sr.OpsGroundTruth)))
		}
	}

	return warnings
}

func pct(n, total int) float64 {
	if total == 0 {
		return 0
	}

	const percent = 100

	return float64(n) / float64(total) * percent
}

func writeJSON(path string, scans []serviceScan) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")

	return enc.Encode(scans)
}

func printServiceScan(sr serviceScan) {
	if len(sr.Findings) == 0 && len(sr.Warnings) == 0 {
		return
	}

	fmt.Fprintf(os.Stdout, "## %s (%s)\n", sr.Dir, moduleList(sr.Modules))

	for _, w := range sr.Warnings {
		fmt.Fprintf(os.Stdout, "*** COVERAGE WARNING: %s ***\n", w)
	}

	fmt.Fprintf(os.Stdout, "operations with SDK ground truth: %d, resolved: %d, with an emission found: %d\n",
		sr.OpsGroundTruth, sr.OpsResolved, sr.OpsWithEmission)

	if len(sr.Findings) == 0 {
		fmt.Fprintln(os.Stdout, "no class A findings (real code, wrong operation)")
		fmt.Fprintln(os.Stdout)

		return
	}

	fmt.Fprintf(os.Stdout, "class A findings (%d):\n", len(sr.Findings))

	for _, f := range sr.Findings {
		printFinding(f)
	}

	fmt.Fprintln(os.Stdout)
}

func printFinding(f finding) {
	domain := f.Domain
	if domain == "" {
		domain = "-"
	}

	fmt.Fprintf(os.Stdout, "  op=%s domain=%s code=%s\n", f.Op, domain, f.Code)

	for _, s := range f.Sites {
		fmt.Fprintf(os.Stdout, "    %s:%d  [%s]\n", s.File, s.Line, s.Mechanism)
	}

	if len(f.AcceptedBy) > 0 {
		fmt.Fprintf(os.Stdout, "    declared correctly by: %v\n", f.AcceptedBy)
	}
}

func moduleList(mods []string) string {
	return strings.Join(mods, ",")
}
