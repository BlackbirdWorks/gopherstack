package main

import (
	"fmt"
	"os"
	"sort"
	"strings"
)

type flaggedField struct {
	Type  string   `json:"type"`
	Field string   `json:"field"`
	File  string   `json:"file"`
	Ops   []string `json:"ops"`
	Line  int      `json:"line"`
}

// lowCoverageThreshold gates the coverage guard: a package that mentions
// service.JSONOpFunc at all (see packageMentionsJSONOpFunc) but resolves
// less than this fraction of its own dispatch table is far more likely to
// be hiding an unrecognised dispatch shape than to be a genuinely small or
// incomplete service -- every JSONOpFunc-using service in this repo
// resolves at 87% or higher once gopherstack-43o8's four blind spots and
// its own anonymous-struct-decode shape are handled; nothing here trips
// this guard as of that fix.
const lowCoverageThreshold = 0.5

// serviceReport is the coverage/finding summary for one services/<dir>.
type serviceReport struct {
	Dir              string          `json:"dir"`
	LowConfidence    string          `json:"lowConfidence,omitempty"`
	UnresolvedOps    []dispatchEntry `json:"unresolvedOps"`
	FlaggedFields    []flaggedField  `json:"flaggedFields"`
	DispatchTotal    int             `json:"dispatchTotal"`
	LiteralOnlyCount int             `json:"literalOnlyCount"`
	ResolvedCount    int             `json:"resolvedCount"`
	TypesFound       int             `json:"typesFound"`
	FieldsFound      int             `json:"fieldsFound"`
}

func buildServiceReport(dir string, scan *packageScan) serviceReport {
	r := serviceReport{Dir: dir, DispatchTotal: len(scan.Dispatch)}

	resolvedTypes := map[string][]string{}

	for _, d := range scan.Dispatch {
		classifyDispatchEntry(d, &r, resolvedTypes)
	}

	r.TypesFound = len(resolvedTypes)

	for _, t := range sortedKeys(resolvedTypes) {
		def := scan.Structs[t]
		r.FieldsFound += len(def.Fields)

		for _, fld := range def.Fields {
			info := scan.Coverage[coverageKey{t, fld.Name}]
			if info.Read {
				continue
			}

			r.FlaggedFields = append(r.FlaggedFields, flaggedField{
				Type: t, Field: fld.Name, File: fld.File, Line: fld.Line, Ops: resolvedTypes[t],
			})
		}
	}

	r.LowConfidence = lowConfidenceReason(scan.UsesJSONOpFunc, r.DispatchTotal, r.ResolvedCount)

	return r
}

// lowConfidenceReason is empty for every service this scan can actually
// vouch for. It is set, loudly, whenever a package that uses
// service.JSONOpFunc still shows a zero or implausible dispatch/coverage
// number -- rather than letting that number print as if it were a
// verified result (gopherstack-43o8's whole point: the tool's own failure
// mode is a false CLEAN verdict, not a false alarm).
func lowConfidenceReason(usesJSONOpFunc bool, dispatchTotal, resolvedCount int) string {
	if !usesJSONOpFunc {
		return ""
	}

	if dispatchTotal == 0 {
		return "this package uses service.JSONOpFunc but NO dispatch table entries were found at all -- " +
			"treat 0 operations as an UNSCANNED service, not a clean one; the scanner likely doesn't " +
			"recognise this package's dispatch-table construction shape"
	}

	if float64(resolvedCount)/float64(dispatchTotal) < lowCoverageThreshold {
		return "resolved coverage is implausibly low for a service.JSONOpFunc-using package -- " +
			"treat this coverage number as UNVERIFIED, not a clean result; the scanner likely can't " +
			"resolve most of this package's handlers"
	}

	return ""
}

func classifyDispatchEntry(d dispatchEntry, r *serviceReport, resolvedTypes map[string][]string) {
	switch {
	case d.Anchor == anchorLiteral && d.ReqType != "":
		r.LiteralOnlyCount++
		r.ResolvedCount++
		resolvedTypes[d.ReqType] = append(resolvedTypes[d.ReqType], d.Op)
	case d.Anchor == anchorWrapOp && d.ReqType != "":
		r.ResolvedCount++
		resolvedTypes[d.ReqType] = append(resolvedTypes[d.ReqType], d.Op)
	default:
		r.UnresolvedOps = append(r.UnresolvedOps, d)
	}
}

func sortedKeys(m map[string][]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

func pct(n, total int) float64 {
	if total == 0 {
		return 0
	}

	const percent = 100

	return float64(n) / float64(total) * percent
}

func printServiceReport(r serviceReport) {
	fmt.Fprintf(os.Stdout, "## %s\n", r.Dir)

	if r.LowConfidence != "" {
		fmt.Fprintf(os.Stdout, "*** COVERAGE WARNING: %s ***\n", r.LowConfidence)
	}

	fmt.Fprintf(os.Stdout, "dispatch table: %d operations\n", r.DispatchTotal)
	fmt.Fprintf(
		os.Stdout, "literal-decode-only coverage (pre-WrapOp resolution): %d/%d (%.0f%%)\n",
		r.LiteralOnlyCount, r.DispatchTotal, pct(r.LiteralOnlyCount, r.DispatchTotal),
	)
	fmt.Fprintf(
		os.Stdout, "WrapOp-resolved coverage: %d/%d (%.0f%%)\n",
		r.ResolvedCount, r.DispatchTotal, pct(r.ResolvedCount, r.DispatchTotal),
	)
	fmt.Fprintf(os.Stdout, "types found: %d, fields found: %d\n", r.TypesFound, r.FieldsFound)

	if len(r.UnresolvedOps) > 0 {
		fmt.Fprintf(os.Stdout, "unresolved operations (%d):\n", len(r.UnresolvedOps))

		for _, d := range r.UnresolvedOps {
			fmt.Fprintf(os.Stdout, "  %s: %s\n", d.Op, d.Reason)
		}
	}

	if len(r.FlaggedFields) == 0 {
		fmt.Fprintln(os.Stdout, "no unread fields found")
	} else {
		fmt.Fprintf(os.Stdout, "unread fields (%d):\n", len(r.FlaggedFields))

		for _, ff := range r.FlaggedFields {
			fmt.Fprintf(
				os.Stdout, "  %s.%s  %s:%d  ops=%s\n",
				ff.Type, ff.Field, ff.File, ff.Line, strings.Join(ff.Ops, ","),
			)
		}
	}

	fmt.Fprintln(os.Stdout)
}
