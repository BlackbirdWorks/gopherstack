// Command bodyclass answers, per operation, what a gopherstack service's
// restjson1/awsjson1.x response body actually is: wrapped (a genuine
// key-switch JSON object), flat/payload (the whole body collapses onto one
// Output member), header-only (no body, everything HTTP-header-bound), or
// void (no members beyond ResultMetadata).
//
// It exists for gopherstack-cnhp: smithy-go emits, for restjson1
// single-payload ops, BOTH a deserializeOpDocument<Op>Output helper that
// decodes named keys AND the deserializeOp<Op>.HandleDeserialize method that
// actually runs. For some ops the helper is dead code and the live path
// decodes the body flat onto one field instead. Trusting the helper's
// key-switch without checking whether HandleDeserialize calls it produces a
// confident, wrong "missing wrapper key" finding -- this happened twice
// (appmesh, glacier). Naming and call signature aren't reliable either:
// polly's deserializeOpDocumentSynthesizeSpeechOutput looks like an ordinary
// wrapped helper and is called normally, but its entire body is
// "v.AudioStream = body" -- no JSON decode at all.
//
// bodyclass resolves the pinned aws-sdk-go-v2/service/<mod>@<version> for a
// services/<dir> from go.mod (module name resolved from the directory's own
// imports, not assumed equal to the directory name -- see resolve.go),
// parses deserializers.go with go/ast, and for each op AST-walks the BODY of
// whatever function actually consumes the HTTP response body in
// HandleDeserialize -- never the helper's name or its call site alone.
//
// Usage:
//
//	go run ./cmd/bodyclass -service appmesh                # every op, text
//	go run ./cmd/bodyclass -service appmesh -op CreateMesh  # one op
//	go run ./cmd/bodyclass -service appmesh -json out.json  # full JSON dump
//	go run ./cmd/bodyclass -all                             # every service, ranked totals
//	go run ./cmd/bodyclass -all -json out.json               # every service, full JSON dump
package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	service := flag.String("service", "", "services/<dir> name")
	op := flag.String("op", "", "limit to a single operation name (requires -service)")
	all := flag.Bool("all", false, "classify every services/<dir> that resolves to a pinned SDK module")
	jsonPath := flag.String("json", "", "write full detail to this path as JSON instead of stdout text")
	flag.Parse()

	if *service == "" && !*all {
		fmt.Fprintln(os.Stderr, "error: one of -service or -all is required")
		os.Exit(1)
	}

	if *service != "" && *all {
		fmt.Fprintln(os.Stderr, "error: -service and -all are mutually exclusive")
		os.Exit(1)
	}

	if *op != "" && *all {
		fmt.Fprintln(os.Stderr, "error: -op requires -service, not -all")
		os.Exit(1)
	}

	repoRoot, err := repoRootDir()
	if err != nil {
		fatal(err)
	}

	cache, err := gomodcache(repoRoot)
	if err != nil {
		fatal(err)
	}

	if *all {
		runAll(repoRoot, cache, *jsonPath)

		return
	}

	runOne(repoRoot, cache, *service, *op, *jsonPath)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

func runOne(repoRoot, cache, service, op, jsonPath string) {
	result, err := classifyService(repoRoot, cache, service)
	if err != nil {
		fatal(err)
	}

	if op != "" {
		result = filterOp(result, op)
	}

	if jsonPath != "" {
		writeJSON(jsonPath, []serviceReport{result})

		return
	}

	printService(result)
}

func runAll(repoRoot, cache, jsonPath string) {
	results, err := classifyAll(repoRoot, cache)
	if err != nil {
		fatal(err)
	}

	if jsonPath != "" {
		writeJSON(jsonPath, results)
	}

	printFleet(results)
}

func filterOp(r serviceReport, op string) serviceReport {
	for _, o := range r.Ops {
		if o.Op == op {
			r.Ops = []opResult{o}

			return r
		}
	}

	r.Ops = nil

	return r
}
