// Command reqfieldscan finds gopherstack request-struct fields that are
// declared on the wire but never read anywhere in the handling service's
// package -- gopherstack-4shm's class: a field decoded off the wire and
// then silently ignored, discarding a parameter or a whole request.
//
// GROUND TRUTH is structural, go/ast only, no go/types: for each
// services/<dir>, every `map[string]service.JSONOpFunc{...}` composite
// literal (there can be several per service, merged at startup -- see
// route53resolver/handler.go's buildOps, which unions 13 of them) gives the
// dispatch table: an operation name mapped to a value expression. Two
// decode paths are resolved:
//
//   - service.WrapOp(fn): fn's own second parameter (context.Context, *In)
//     -- REFLECTION-BASED decode, gopherstack-4shm's blind spot: no literal
//     json.Unmarshal call anchors it anywhere. fn is resolved whether it's
//     a bound method (h.handleFoo), a package-level func, or a func
//     literal; *In's local struct type gives every wire field via its own
//     json tags.
//   - a literal `json.Unmarshal(body, &x)` inside some other function,
//     where x's type is inferrable from its own declaration in that same
//     function -- this repo's OTHER decode path (e.g. batch's
//     handleTagResource, whose TagResource op is not in the WrapOp table
//     at all: it is dispatched by HTTP method inside handleTags, never
//     through h.ops). Linked to a same-named entry in
//     GetSupportedOperations's own static []string{} literal, when it has
//     one (batch-style; route53resolver, workspaces, and dms instead build
//     that list from h.ops's own keys at runtime, contributing nothing
//     extra here).
//
// COVERAGE is reported as a fraction of the dispatch table -- every op name
// found across all JSONOpFunc map literals, unioned with
// GetSupportedOperations's own static list when it has one -- specifically
// so an implausible number is visible on its face. This is the lesson
// gopherstack-4shm was filed for: a scan anchored on literal decode calls
// alone found two types and five fields in a service that dispatches
// nearly everything through WrapOp. Report that fraction plainly rather
// than a bare finding count.
//
// FIELD COVERAGE: for every function declared in the package (not only the
// one function WrapOp was handed), a parameter or `:=`/`=`-bound local
// whose type is a known request struct -- by pointer, by value, or by a
// single-hop alias (`x := in`, `x := *in`) -- binds that identifier to the
// type for the rest of that function's body; every `ident.FieldName`
// selector anywhere in the body then marks (type, field) covered. Identity
// is the (struct TYPE, field name) pair, never a bare field name, so two
// structs that happen to share a field name never collide. This is wider
// than a strict single hop: a helper function that receives the request
// struct as its own typed parameter and reads a field there is caught too,
// since every function in the package is scanned independently for its own
// bindings, not only the one function actually registered with WrapOp. It
// does NOT follow a field through further indirection -- a value copied
// into a variable of some OTHER, untracked type and read only from that
// copy is invisible, the same single-hop limitation cmd/enumcheck
// discloses for its own struct-field resolution.
//
// WHOLE-STRUCT CONVERSION SUPPRESSION: `SomeType(req)` or `SomeType(*req)`
// -- a Go type conversion of the entire request value, this repo's other
// common way of "using" every field at once with no per-field selector
// anywhere for the tool to see -- marks every field of req's type covered,
// tagged covered-via-conversion in the report rather than silently
// indistinguishable from an ordinary read. Confirmed necessary: an earlier
// pass in this campaign found 23 of 25 raw flags were exactly this shape.
// This is a blunt instrument: it does not check that SomeType actually
// declares a same-named field for each one, so a conversion that
// legitimately drops a field on the floor is invisible to this rule too --
// hand-verification is still required before treating any flagged field as
// a real bug, per gopherstack-4shm's own instruction.
//
// BLIND SPOTS, disclosed rather than silently under-covered:
//   - Only files directly in services/<dir> are scanned, no recursion into
//     subpackages, and _test.go files are excluded from both the dispatch
//     scan and the field-read scan (a field read only from a test would
//     still be reported unread, which is the intended, conservative
//     answer for a "does production code use this" question).
//   - A method name that exists on more than one receiver type in the same
//     package resolves to whichever FuncDecl was encountered first while
//     walking files in directory order. This repo's one-Handler-type-per-
//     service convention makes that collision rare -- never observed
//     across the four services this tool has been run against -- but it
//     is not structurally impossible.
//   - An embedded (anonymous) struct field, a *In resolved to a type
//     imported from another package, or a WrapOp argument shape other than
//     a bound method / package function / func literal contributes no
//     fields and surfaces as an unresolved dispatch entry in the report,
//     never a silently dropped one.
//   - A local variable reassigned with `=` to something the resolver can't
//     statically type keeps its PRIOR binding rather than being cleared --
//     a theoretical source of a missed field-write count. Never seen to
//     matter across the four services this tool covers; documented rather
//     than chased, matching cmd/enumcheck's own single-assignment
//     discipline.
//   - This tool proves a field was REFERENCED somewhere reachable, never
//     that the value was used CORRECTLY: gopherstack-4shm's own "cascade
//     flag read but never passed to the delete that needed it" shape reads
//     the field (covered, no flag raised) and is still a real bug. Only a
//     human reading the flagged AND unflagged fields against each
//     operation's own intended behavior catches that; this tool only
//     narrows where to look.
//   - service.WrapOp is the only reflective request-decode helper found in
//     pkgs/ (pkgs/service/jsondisp.go). pkgs/service/restdispatch.go's
//     RESTRouter and pkgs/service/rpcv2cbor.go's CBOR helpers were checked
//     and use no such generic decode: RESTRouter.Dispatch is a
//     per-service function supplied by the caller, not a reflection-based
//     struct decode, and the CBOR helpers write raw cbor.Value trees, never
//     decode into a typed Go struct at all. A repo-wide grep for other
//     `reflect.` uses in pkgs/ turned up only pkgs/sdkcheck (SDK
//     method-set enumeration for completeness tests, unrelated to request
//     decode). If a future generic dispatcher gains a reflective decode of
//     its own, it needs its own resolution added here.
//
// Usage:
//
//	go run ./cmd/reqfieldscan                                # scan every services/<dir>
//	go run ./cmd/reqfieldscan -dir route53resolver,batch      # scan only these
//	go run ./cmd/reqfieldscan -json out.json                  # also write full report as JSON
//
// Exit codes: 0 no unread fields found, 1 a run error, 2 at least one
// unread field flagged in at least one scanned service.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

const (
	exitClean    = 0
	exitRunError = 1
	exitFindings = 2
)

func main() {
	dirFlag := flag.String("dir", "", "comma-separated services/<dir> basenames to scan (default: all)")
	jsonOut := flag.String("json", "", "write the full report list to this path as JSON")
	flag.Parse()

	reports, err := run(*dirFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(exitRunError)
	}

	if *jsonOut != "" {
		if werr := writeJSON(*jsonOut, reports); werr != nil {
			fmt.Fprintln(os.Stderr, "write json:", werr)
			os.Exit(exitRunError)
		}
	}

	unread := 0
	for _, r := range reports {
		printServiceReport(r)

		unread += len(r.FlaggedFields)
	}

	if unread > 0 {
		os.Exit(exitFindings)
	}

	os.Exit(exitClean)
}

func run(dirFlag string) ([]serviceReport, error) {
	repoRoot, err := repoRootDir()
	if err != nil {
		return nil, err
	}

	dirs, err := targetDirs(filepath.Join(repoRoot, "services"), dirFlag)
	if err != nil {
		return nil, err
	}

	var reports []serviceReport

	for _, dir := range dirs {
		scan, scanErr := scanServiceDir(dir)
		if scanErr != nil {
			return nil, fmt.Errorf("%s: %w", dir, scanErr)
		}

		if len(scan.Dispatch) == 0 {
			continue
		}

		reports = append(reports, buildServiceReport(filepath.Base(dir), scan))
	}

	return reports, nil
}

func targetDirs(svcRoot, dirFlag string) ([]string, error) {
	if dirFlag != "" {
		dirs := make([]string, 0, strings.Count(dirFlag, ",")+1)
		for d := range strings.SplitSeq(dirFlag, ",") {
			dirs = append(dirs, filepath.Join(svcRoot, strings.TrimSpace(d)))
		}

		sort.Strings(dirs)

		return dirs, nil
	}

	entries, err := os.ReadDir(svcRoot)
	if err != nil {
		return nil, err
	}

	var dirs []string

	for _, e := range entries {
		if e.IsDir() {
			dirs = append(dirs, filepath.Join(svcRoot, e.Name()))
		}
	}

	sort.Strings(dirs)

	return dirs, nil
}

func repoRootDir() (string, error) {
	out, err := exec.CommandContext(context.Background(), "go", "list", "-m", "-f", "{{.Dir}}").Output()
	if err != nil {
		return "", fmt.Errorf("go list -m: %w", err)
	}

	return strings.TrimSpace(string(out)), nil
}

func writeJSON(path string, reports []serviceReport) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")

	return enc.Encode(reports)
}
