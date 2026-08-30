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

// serviceReport is the coverage/finding summary for one services/<dir>.
type serviceReport struct {
	Dir              string          `json:"dir"`
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

	return r
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
