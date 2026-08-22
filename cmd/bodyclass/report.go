package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

type serviceReport struct {
	Service string     `json:"service"`
	Mod     string     `json:"mod"`
	Ver     string     `json:"ver"`
	Err     string     `json:"err,omitempty"`
	Ops     []opResult `json:"ops"`
}

func classifyService(repoRoot, cache, service string) (serviceReport, error) {
	r, err := resolveModule(repoRoot, cache, service)
	if err != nil {
		return serviceReport{}, err
	}

	idx, err := indexDeserializers(r.ModPath)
	if err != nil {
		return serviceReport{}, err
	}

	rep := serviceReport{Service: service, Mod: r.Mod, Ver: r.Ver}
	for _, op := range idx.ops() {
		rep.Ops = append(rep.Ops, classifyOp(idx, op))
	}

	return rep, nil
}

// classifyAll walks every services/<dir>, classifying whatever resolves. A
// dir with no pinned SDK module (nothing to diff against) or no
// deserializers.go for its resolved module is skipped with its error
// recorded, not treated as a fatal condition -- one unresolved service
// should never abort the fleet-wide run.
func classifyAll(repoRoot, cache string) ([]serviceReport, error) {
	dirs, err := listServiceDirs(repoRoot)
	if err != nil {
		return nil, err
	}

	results := make([]serviceReport, 0, len(dirs))

	for _, d := range dirs {
		rep, repErr := classifyService(repoRoot, cache, d)
		if repErr != nil {
			results = append(results, serviceReport{Service: d, Err: repErr.Error()})

			continue
		}

		results = append(results, rep)
	}

	return results, nil
}

func writeJSON(path string, results []serviceReport) {
	f, err := os.Create(path)
	if err != nil {
		fatal(err)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")

	if encErr := enc.Encode(results); encErr != nil {
		fatal(encErr)
	}

	fmt.Fprintln(os.Stderr, "wrote", path)
}

func printService(rep serviceReport) {
	fmt.Fprintf(os.Stdout, "# %s (%s %s) -- %d op(s)\n\n", rep.Service, rep.Mod, rep.Ver, len(rep.Ops))

	for _, o := range rep.Ops {
		printOp(o)
	}
}

func printOp(o opResult) {
	fmt.Fprintf(os.Stdout, "%-32s %-12s %s", o.Op, o.Protocol, o.Class)

	switch {
	case o.Member != "":
		fmt.Fprintf(os.Stdout, "  member=%s", o.Member)
	case len(o.Keys) > 0:
		fmt.Fprintf(os.Stdout, "  keys=%v", o.Keys)
	}

	if o.Callee != "" {
		fmt.Fprintf(os.Stdout, "  callee=%s", o.Callee)
	}

	if o.Detail != "" {
		fmt.Fprintf(os.Stdout, "  -- %s", o.Detail)
	}

	fmt.Fprintln(os.Stdout)
}

// printFleet ranks services by total op count and, per service, breaks down
// how many ops fell in each class -- surfacing at a glance which services
// mix wrapped and flat/payload ops within the same service (the case a
// per-service assumption gets wrong; see appmesh/amplify in gopherstack-cnhp).
func printFleet(results []serviceReport) {
	sort.Slice(results, func(i, j int) bool { return len(results[i].Ops) > len(results[j].Ops) })

	totals := map[class]int{}

	fmt.Fprintf(os.Stdout, "%-24s %6s %8s %8s %12s %6s %8s  %s\n",
		"service", "ops", "wrapped", "flat", "hdr-only", "void", "unknown", "mixed")

	for _, r := range results {
		if r.Err != "" {
			continue
		}

		counts := classCounts(r.Ops)
		for c, n := range counts {
			totals[c] += n
		}

		mixed := ""
		if counts[classWrapped] > 0 && counts[classFlat] > 0 {
			mixed = "MIXED"
		}

		fmt.Fprintf(os.Stdout, "%-24s %6d %8d %8d %12d %6d %8d  %s\n",
			r.Service, len(r.Ops), counts[classWrapped], counts[classFlat],
			counts[classHeaderOnly], counts[classVoid], counts[classUnknown], mixed)
	}

	fmt.Fprintln(os.Stdout)
	fmt.Fprintf(os.Stdout, "TOTAL wrapped=%d flat/payload=%d header-only=%d void=%d unknown=%d\n",
		totals[classWrapped], totals[classFlat], totals[classHeaderOnly], totals[classVoid], totals[classUnknown])

	printUnresolved(results)
}

func printUnresolved(results []serviceReport) {
	var unresolved []string

	for _, r := range results {
		if r.Err != "" {
			unresolved = append(unresolved, r.Service+": "+r.Err)
		}
	}

	if len(unresolved) == 0 {
		return
	}

	fmt.Fprintln(os.Stdout, "\nunresolved (no pinned SDK module or no deserializers.go found):")

	for _, u := range unresolved {
		fmt.Fprintln(os.Stdout, "  "+u)
	}
}

func classCounts(ops []opResult) map[class]int {
	counts := map[class]int{}
	for _, o := range ops {
		counts[o.Class]++
	}

	return counts
}
