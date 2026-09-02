// Command errcodeaudit finds an error code string gopherstack emits that
// names no real AWS error type at all -- the class commit fa0e68c21 fixed
// by hand in services/ecs, which emitted eleven codes (TaskNotFoundException,
// ClusterAlreadyExistsException, CapacityProviderNotFoundException, ...)
// corresponding to no type anywhere in the real pinned SDK. They read
// entirely plausible -- AWS's own `<Thing><Condition>Exception` convention
// exactly -- which is why five existing tests asserted them as correct. A
// typed client's errors.As can never match one, so every such failure
// arrives opaque and retry/waiter/conditional logic all fall through.
//
// This is set membership over strings, not dataflow: the set of error code
// names a service can legitimately emit is enumerable from its pinned SDK,
// and anything outside that set is wrong regardless of how it got there.
//
// GROUND TRUTH. For each services/<dir>, the pinned aws-sdk-go-v2/service/
// <mod>@<ver> module(s) are resolved straight from that service's own
// import paths (go/ast, not a name table), same approach as
// cmd/enumcheck/cmd/zeroguard's modresolve.go. Two files from each module
// are read (sdktruth.go):
//
//   - types/errors.go: every declared exception type's own ErrorCode()
//     method, read as the literal string in its `return "Foo"` fallback
//     branch -- NOT the Go type name, which can differ (iam@v1.58.1's
//     NoSuchEntityException.ErrorCode() returns "NoSuchEntity"). Treated
//     as PRIMARY/canonical: this is exactly the ground truth a real
//     client's errors.As matches against, and exactly what the eleven
//     pre-fix ecs codes had none of.
//   - deserializers.go: every literal in a `strings.EqualFold("Foo",
//     errorCode)` case inside a deserializeOpError* function -- the codes
//     actually matched on the wire for some operation. Confirmed the same
//     shape across ecs@v1.90.0 (awsjson1.1) and iam@v1.58.1 (awsquery), so
//     no protocol-specific branch is needed. Unioned into the module's
//     legitimate set as a SECONDARY source: it can only ever ADD codes a
//     client also recognizes (a case that reaches a real deserializeError*
//     function), never remove one types/errors.go already established.
//
// A service whose resolved module(s) model NO codes at all via either
// source (ec2's documented case: 785 operations, zero typed exceptions in
// this SDK version) contributes no ground truth and is skipped entirely --
// flagging every emission there would be a false positive by construction,
// not a finding.
//
// EXTRACTION. gopherstack's own emitted codes are read from services/<dir>
// (test files excluded) via six syntactic rules, chosen by reading
// services/ecs, services/iam, services/lambda and services/cloudformation's
// handler.go files -- confirmed to be four different mechanisms (extract.go
// has the per-rule reasoning):
//
//   - awserr.New("Code", sentinel) / awserr.Newf -- ecs's mechanism.
//   - stdlib errors.New("Code"), where the sentinel's own message IS the
//     code -- lambda's mechanism.
//   - a literal argument, any position, to a call to anything named
//     "...Error" (never "...Errorf") -- lambda's writeError and
//     cloudformation's xmlError both read this way without needing to know
//     each call's argument order, since a human-readable message literal
//     never matches the code-shape filter.
//   - a mapping table: a struct/map composite literal's Code/Type-labeled
//     field, keyed (IAMError{Code: code}) or positional
//     (iamErrorMapping{ErrX, codeY, status}, resolved against the struct's
//     own declared field order) -- iam's mechanism, and also ecs's
//     map[string]string{keyTypeField: "Code", ...} shape.
//   - a code-shaped literal assigned to a code-named variable/const
//     (code := "X", const errCodeValidation = "X") -- cloudformation's
//     handler_stack_refactors.go/handler_stack_sets.go shape.
//   - a return statement inside a function named like an error-code
//     classifier, returning a code-shaped literal directly --
//     cloudformation's mapCreateStackError/stackInstancesErrorCode shape.
//     Always NEEDS REVIEW: the weakest signal here, since a
//     "...Error..."-named function can return any string.
//
// Every candidate is filtered through a code-shape regex (PascalCase or
// SCREAMING, no spaces/punctuation, 4+ chars) before any of the above rules
// even applies it -- this alone is what keeps "StackName is required" and
// "unknown action: "+action out, since neither is a bare code-shaped
// literal.
//
// BLIND SPOTS, disclosed rather than silently under-covered: a code
// assembled through more than one hop of identifier indirection (a local
// variable threaded through two function calls before reaching a
// mapping-table field) resolves to nothing and produces no finding, never
// a wrong one -- matching cmd/enumcheck's own single-hop discipline. A code
// built by string concatenation, fmt.Sprintf, or read from a request field
// is invisible to this scan entirely. A service that emits its error codes
// through some fifth mechanism this tool's four-file survey never saw is
// silently unaudited -- a clean run there is NOT proof of correctness, only
// proof this tool found nothing to check.
//
// CALIBRATION. genericcodes.go allowlists the protocol-level codes AWS's
// wire frontend recognizes for every service and never models as a
// per-service typed exception (ValidationError, InvalidAction,
// MissingParameter, Throttling, InternalFailure, AccessDenied, and their
// common siblings) -- these would otherwise false-positive on every
// service that legitimately emits them. A finding is CONFIDENT only when
// the candidate is a direct literal (not a resolved identifier, not a
// return-statement heuristic hit) AND the service resolved exactly one SDK
// module (2+ modules means which one's exception set applies is unknown,
// the same ambiguity cmd/enumcheck treats as an "ambiguous key" rather than
// silently picking one). Every other case is NEEDS REVIEW, never dropped:
// this tool's own anti-false-positive filters could just as easily hide a
// real bug sitting one line from one they catch, the same blind spot
// measured in cmd/enumcheck's own filter after the fact.
//
// Usage:
//
//	go run ./cmd/errcodeaudit                 # report to stdout
//	go run ./cmd/errcodeaudit -json out.json  # also write full finding list as JSON
//
// Exit codes: 0 no confident findings (needs-review hits may still print),
// 1 a run error, 2 at least one confident finding.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

const (
	exitClean      = 0
	exitRunError   = 1
	exitConfidence = 2
)

func main() {
	jsonOut := flag.String("json", "", "write the full finding list to this path as JSON")
	flag.Parse()

	findings, err := run()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(exitRunError)
	}

	if *jsonOut != "" {
		if werr := writeJSON(*jsonOut, findings); werr != nil {
			fmt.Fprintln(os.Stderr, "write json:", werr)
			os.Exit(exitRunError)
		}
	}

	printReport(findings)
	os.Exit(exitCode(findings))
}

func run() ([]finding, error) {
	repoRoot, err := repoRootDir()
	if err != nil {
		return nil, err
	}

	cache, err := gomodcacheDir(repoRoot)
	if err != nil {
		return nil, err
	}

	goModVersions, err := loadGoModVersions(filepath.Join(repoRoot, "go.mod"))
	if err != nil {
		return nil, err
	}

	return scan(repoRoot, cache, goModVersions)
}

func exitCode(findings []finding) int {
	for _, f := range findings {
		if f.Confident {
			return exitConfidence
		}
	}

	return exitClean
}
