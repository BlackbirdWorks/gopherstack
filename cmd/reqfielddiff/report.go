package main

import (
	"fmt"
	"os"
	"sort"
	"strings"
)

// lowResolutionThreshold gates the "handler found but nothing declared"
// guard: within operations this scan DID find a handler for, the fraction
// that also yielded at least one decode/query-param signal. Mirrors
// cmd/reqfieldscan's lowCoverageThreshold and its reasoning: a package
// where most located handlers show zero declared fields is far more likely
// hiding an unrecognised decode shape than genuinely all-parameterless.
const lowResolutionThreshold = 0.5

// lowFieldRatioThreshold gates the field-count sanity guard described in
// this tool's own brief: if the SDK declares many fields across the
// operations this scan resolved a handler for, and the emulator's declared
// field count across those same operations is a tiny fraction of that, the
// likelier explanation is a resolution bug in this tool, not a real gap
// that large.
const lowFieldRatioThreshold = 0.05

// minFieldsForRatioGuard avoids firing the field-ratio guard on a small
// service where a low ratio is just noise (a handful of SDK fields legitimately
// unimplemented looks identical to a resolution failure at small N).
const minFieldsForRatioGuard = 50

// percentScale converts a 0..1 ratio to a percentage for display.
const percentScale = 100

type serviceReport struct {
	Dir               string          `json:"dir"`
	Module            string          `json:"module"`
	ModuleErr         string          `json:"moduleErr,omitempty"`
	Findings          []triageFinding `json:"findings,omitempty"`
	Warnings          []string        `json:"warnings,omitempty"`
	OpsTotal          int             `json:"opsTotal"`
	OpsHandlerFound   int             `json:"opsHandlerFound"`
	OpsWithSignal     int             `json:"opsWithSignal"`
	SDKFieldsResolved int             `json:"sdkFieldsResolved"`
	EmuFieldsResolved int             `json:"emuFieldsResolved"`
	DeprecatedSkipped int             `json:"deprecatedSkipped"`
}

func buildServiceReport(dir, mod string, sdkOps []sdkOp, resolutions map[string]opResolution) serviceReport {
	r := serviceReport{Dir: dir, Module: mod, OpsTotal: len(sdkOps)}

	for _, op := range sdkOps {
		res := resolutions[op.Name]
		if !res.Found {
			continue
		}

		r.OpsHandlerFound++
		r.SDKFieldsResolved += len(op.Fields)

		if res.HasSignal {
			r.OpsWithSignal++
		}

		r.EmuFieldsResolved += len(res.Fields)
	}

	siblingByOp := map[string]map[string]bool{}
	for _, op := range sdkOps {
		siblingByOp[op.Name] = buildSiblingIndex(resolutions, op.Name)
	}

	for _, op := range sdkOps {
		res := resolutions[op.Name]
		if !res.Found {
			continue
		}

		for _, m := range findMissing(op, res) {
			f := triageOne(m, siblingByOp[op.Name])
			if f.Deprecated {
				r.DeprecatedSkipped++

				continue
			}

			r.Findings = append(r.Findings, f)
		}
	}

	sort.SliceStable(r.Findings, func(i, j int) bool {
		if r.Findings[i].Tier != r.Findings[j].Tier {
			return r.Findings[i].Tier < r.Findings[j].Tier
		}

		if r.Findings[i].Op != r.Findings[j].Op {
			return r.Findings[i].Op < r.Findings[j].Op
		}

		return r.Findings[i].Field.Name < r.Findings[j].Field.Name
	})

	r.Warnings = coverageWarnings(r)

	return r
}

// coverageWarnings implements the coverage guard this tool's brief
// requires: loud, not silent, whenever a number looks implausible rather
// than merely low. See the package doc for the two axes checked and why
// each threshold was chosen.
func coverageWarnings(r serviceReport) []string {
	var warnings []string

	if r.OpsTotal > 0 && r.OpsHandlerFound == 0 {
		warnings = append(warnings, fmt.Sprintf(
			"ZERO of %d SDK operations resolved to an emulator handler at all -- "+
				"treat this service as UNSCANNED, not clean; this scan likely doesn't "+
				"recognise its dispatch or naming convention", r.OpsTotal,
		))

		return warnings
	}

	if r.OpsHandlerFound > 0 {
		ratio := float64(r.OpsWithSignal) / float64(r.OpsHandlerFound)
		if ratio < lowResolutionThreshold {
			warnings = append(warnings, fmt.Sprintf(
				"only %d/%d (%.0f%%) of resolved handlers show ANY declared field at all -- "+
					"treat this service's field coverage as UNVERIFIED, not clean; this scan "+
					"likely can't see most of this package's decode shape",
				r.OpsWithSignal, r.OpsHandlerFound, pct(r.OpsWithSignal, r.OpsHandlerFound),
			))
		}
	}

	if r.SDKFieldsResolved >= minFieldsForRatioGuard {
		ratio := float64(r.EmuFieldsResolved) / float64(r.SDKFieldsResolved)
		if ratio < lowFieldRatioThreshold {
			warnings = append(warnings, fmt.Sprintf(
				"the SDK declares %d input fields across resolved operations but this scan "+
					"found only %d emulator-declared fields (%.1f%%) -- more likely a resolution "+
					"bug in this tool than a service this thin; treat the gap count as UNVERIFIED",
				r.SDKFieldsResolved, r.EmuFieldsResolved, ratio*percentScale,
			))
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

func printServiceReport(r serviceReport) {
	fmt.Fprintf(os.Stdout, "## %s (%s)\n", r.Dir, r.Module)

	if r.ModuleErr != "" {
		fmt.Fprintf(os.Stdout, "SKIPPED: %s\n\n", r.ModuleErr)

		return
	}

	for _, w := range r.Warnings {
		fmt.Fprintf(os.Stdout, "*** COVERAGE WARNING: %s ***\n", w)
	}

	fmt.Fprintf(os.Stdout, "SDK operations: %d, handler resolved: %d, with declared fields: %d\n",
		r.OpsTotal, r.OpsHandlerFound, r.OpsWithSignal)
	fmt.Fprintf(os.Stdout, "SDK input fields (resolved ops): %d, emulator-declared fields: %d\n",
		r.SDKFieldsResolved, r.EmuFieldsResolved)

	if r.DeprecatedSkipped > 0 {
		fmt.Fprintf(os.Stdout, "excluded as deprecated in the SDK: %d\n", r.DeprecatedSkipped)
	}

	if len(r.Findings) == 0 {
		fmt.Fprintln(os.Stdout, "no undeclared SDK input fields found")
		fmt.Fprintln(os.Stdout)

		return
	}

	fmt.Fprintf(os.Stdout, "undeclared SDK input fields (%d), ranked:\n", len(r.Findings))

	for _, f := range r.Findings {
		req := ""
		if f.Field.Required {
			req = " [required]"
		}

		fmt.Fprintf(os.Stdout, "  tier%d  %s.%s%s  (%s)\n", f.Tier, f.Op, f.Field.Name, req, signalsText(f.Signals))
	}

	fmt.Fprintln(os.Stdout)
}

func signalsText(signals []string) string {
	if len(signals) == 0 {
		return "no strong signal -- likely a legitimate structural gap or output-only field"
	}

	return strings.Join(signals, "; ")
}
