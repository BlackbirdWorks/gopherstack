// Command reqfieldscan finds gopherstack request-struct fields that are
// declared on the wire but never read anywhere in the handling service's
// package -- gopherstack-4shm's class: a field decoded off the wire and
// then silently ignored, discarding a parameter or a whole request.
//
// GROUND TRUTH is structural, go/ast only, no go/types: for each
// services/<dir>, every dispatch-table construction that yields
// service.JSONOpFunc values gives an operation name mapped to a value
// expression. Two shapes are recognised (collectDispatchTableEntries),
// possibly several per service, merged at startup -- see
// route53resolver/handler.go's buildOps, which unions 13 map literals:
//
//   - `map[string]service.JSONOpFunc{...}` composite literals, the common
//     shape.
//   - a slice-of-struct binder table -- glue's real shape:
//     `[]struct{ name string; bind func(*Handler) service.JSONOpFunc }{...}`,
//     ranged over at startup to build the actual map. Before
//     gopherstack-43o8's fix this shape contributed no dispatch entries at
//     all: 0 of 0, not a plausible small number but an invisible one.
//
// Each entry's own value expression is resolved directly to its handler's
// request type (resolveValueExprToReqType) -- through service.WrapOp
// itself, or through a local function whose entire body forwards to
// service.WrapOp (cognitoidp's wrapAccuracy[I,O](fn), handler.go:484;
// collectLocalWrapOpWrappers). Resolving the VALUE actually bound to an op,
// rather than reconstructing "handle"+opName and searching for a
// same-named handler, also means a handler's name -- handle<Op>Full,
// handle<Op>Accurate, handle<Op>WithOpts, or anything else -- no longer
// matters: gopherstack-43o8's blind spots 2 and 3 were really one gap
// (matching the literal selector name "WrapOp" instead of the value
// bound), closed by the same fix. The "handle"+opName reconstruction
// (collectWrapOpFuncNames, matched case-insensitively) survives as a
// FALLBACK, still needed for batch's dispatch table, which is keyed by REST
// path ("/v1/createcomputeenvironment") rather than by the canonical
// operation name its own GetSupportedOperations advertises -- a shape the
// direct, op-keyed resolution above can never reach by construction.
//
// A THIRD decode path exists outside any dispatch table at all: a literal
// `json.Unmarshal(body, &x)` inside some other function, where x's type is
// inferrable from its own declaration in that same function (e.g. batch's
// handleTagResource, whose TagResource op is dispatched by HTTP method
// inside handleTags, never through h.ops). Linked to a same-named entry in
// GetSupportedOperations's own static []string{} literal, when it has one
// (batch-style; route53resolver, workspaces, and dms instead build that
// list from h.ops's own keys at runtime, contributing nothing extra here).
// x's declaration can be a named local struct type, OR an anonymous inline
// one (`var req struct{...}`) -- opsworks's real shape: every handler there
// IS a service.JSONOpFunc directly, no WrapOp anywhere, decoding into its
// own anonymous struct literal. collectAnonReqStructs registers each such
// declaration under a name derived purely from its file:line, so it
// resolves through this same literal-decode path. Before this fix opsworks
// reported 0 of 74 resolved.
//
// COVERAGE is reported as a fraction of the dispatch table -- every op name
// found across all dispatch-table shapes above, unioned with
// GetSupportedOperations's own static list when it has one -- specifically
// so an implausible number is visible on its face. This is the lesson
// gopherstack-4shm was filed for: a scan anchored on literal decode calls
// alone found two types and five fields in a service that dispatches
// nearly everything through WrapOp. Report that fraction plainly rather
// than a bare finding count.
//
// THE COVERAGE GUARD (gopherstack-43o8): a fraction alone can still read as
// a plausible result when it's actually a measurement failure -- glue's old
// 0-of-0 and cognitoidp's old 62% both did, and both survived because an
// agent's own judgment, not the tool, caught them. Any packageScan whose
// files mention service.JSONOpFunc at all (packageMentionsJSONOpFunc) but
// resolve zero dispatch entries, or resolve less than lowCoverageThreshold
// (report.go) of them, now gets an explicit "*** COVERAGE WARNING ***" line
// ahead of its numbers, and counts toward a nonzero exit code -- loud by
// construction, not by an agent's judgment call. A package that never
// mentions service.JSONOpFunc (this repo's Query/XML-protocol and
// REST-routed services -- sns's map[string]snsActionFn, s3, ec2, iam, and
// roughly 60 others) is legitimately outside this scan's ground truth; the
// guard stays silent for those, the same way it always has. As of this fix,
// nothing in this repo's services/ trips the guard -- it is a sentinel
// against a FUTURE unrecognised shape, not a currently-firing warning.
//
// FIELD COVERAGE: for every function declared in the package (not only the
// one function WrapOp was handed), a method RECEIVER, a parameter, or a
// `:=`/`=`-bound local whose type is a known request struct -- by pointer,
// by value, or by a single-hop alias (`x := in`, `x := *in`) -- binds that
// identifier to the type for the rest of that function's body (method
// body, for a receiver); codecommit's `func (r mergeBranchesRequest)
// options()` reads r.TargetBranch, r.CommitMessage, r.AuthorName, and
// r.Email this way -- before this fix, a request struct's own methods were
// invisible to field coverage, a FALSE POSITIVE (over-reporting unread
// fields), the opposite failure from this tool's earlier under-reporting
// hardening passes. Every `ident.FieldName`
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
// A GO TYPE ALIAS (`type X = Y`, or a defined type `type X Y`) whose target
// is a known request struct now resolves too (resolveStructAliases): glue's
// `type updateJobFromSourceControlInput = jobSourceControlInput`
// (handler_jobs.go:386) reaches its request struct only through this
// indirection, invisible to a struct collector that only ever registered
// ast.StructType TypeSpecs by name. Two glue operations were hand-verified
// clean but structurally invisible before this fix.
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
//     across any service this tool has been run against -- but it is not
//     structurally impossible.
//   - lowerKeyedHandlers' case-insensitive "handle"+op fallback (dispatch.go)
//     used to pick a winner by ranging directly over the wrapOpFuncs map,
//     so two handler names differing only by acronym case (route53resolver's
//     real handleAssociate ResolverEndpointIPAddress vs the AWS operation's
//     own IpAddress spelling is what this fallback exists for at all) would
//     resolve nondeterministically if BOTH happened to exist in one package
//     -- gopherstack-fr30, reported first against cmd/reqfielddiff's
//     identical-shaped bug. Fixed to iterate wrapOpFuncs' keys in sorted
//     order so the lexicographically smallest original name wins any
//     collision, deterministically. A repo-wide census (every
//     services/<dir>, current repo state) found ZERO actual collisions in
//     this package's narrower universe: wrapOpFuncs only holds names
//     actually passed to service.WrapOp somewhere in the package, which
//     excludes the exported Backend/business-logic methods that DO collide
//     with cmd/reqfielddiff's broader ctx.methods/ctx.funcs scan (177
//     operations, 26 services -- see that tool's package doc). The fix is
//     a determinism guard against a real structural risk, not a change in
//     today's output for any service.
//   - An embedded (anonymous) struct field, a *In resolved to a type
//     imported from another package, or a WrapOp argument shape other than
//     a bound method / package function / func literal contributes no
//     fields and surfaces as an unresolved dispatch entry in the report,
//     never a silently dropped one.
//   - A slice-of-struct binder element must be a KEYED composite literal
//     (`{name: "...", bind: func(...) {...}}`, glue's real shape and the
//     only one observed); a positional (unkeyed) element contributes
//     nothing. A binder func literal's dispatch value must also be its
//     first top-level return statement -- true for every binder in this
//     repo today, but a binder with real branching logic before its
//     return would not resolve.
//   - A dispatch-table denominator built from GetSupportedOperations's own
//     static []string{} literal (batch-style) is never cross-checked
//     against collectDispatchTableEntries's own key set; a static list
//     that has drifted out of sync with the table it describes would
//     surface as unresolved ops, never a silently wrong count, but the
//     two are not reconciled against each other.
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
// Exit codes: 0 no unread fields found and no coverage warning, 1 a run
// error, 2 at least one unread field flagged, or at least one service
// tripped the coverage guard above, in at least one scanned service.
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
	lowConfidence := 0

	for _, r := range reports {
		printServiceReport(r)

		unread += len(r.FlaggedFields)

		if r.LowConfidence != "" {
			lowConfidence++
		}
	}

	if unread > 0 || lowConfidence > 0 {
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

		if len(scan.Dispatch) == 0 && !scan.UsesJSONOpFunc {
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
