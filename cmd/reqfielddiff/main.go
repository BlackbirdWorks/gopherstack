// Command reqfielddiff finds SDK request-input fields the emulator never
// declared at all -- gopherstack-4glf's class, invisible to
// cmd/reqfieldscan by construction, since that scan enumerates fields the
// emulator's own decode structs DECLARE and checks each is read: a field
// with no struct field to enumerate is invisible to it. Confirmed
// concretely before this tool existed: apigateway's GetResources drops the
// SDK's documented Embed parameter, and "Embed" appears nowhere in
// services/apigateway; reqfieldscan reports zero findings for that service.
//
// GROUND TRUTH, for one operation, is two independently-resolved field
// sets: the pinned aws-sdk-go-v2 <Op>Input struct's own top-level fields
// (sdkfields.go, adapted from cmd/structfielddiff's identical parse, which
// already "dumps SDK shapes for manual comparison" per gopherstack-4glf --
// this tool automates the other half, the diff against the emulator, that
// issue says nothing joins), and the fields the union of every struct type
// the emulator's own handler for that op actually decodes into declares
// (structs.go/resolve.go). A field present in the first set with no
// normalized-name match in the second is reported.
//
// RESOLVING THE EMULATOR'S DECODE TARGET is the hard half, and it is a
// STRICTLY HARDER problem than cmd/reqfieldscan's, not the same one reused:
// reqfieldscan's whole dispatch-table machinery is built around
// service.JSONOpFunc / service.WrapOp, and this tool's own confirmed
// ground truth sits OUTSIDE that world entirely. omics -- the service
// carrying the three-undeclared-defaulted-parameter finding this tool was
// built to catch -- dispatches through
// `map[string]func(*Handler,*echo.Context,string) error` closures that
// call a plain `h.handleStartRun(c)`, decoding into an ANONYMOUS INLINE
// struct with no WrapOp anywhere. apigateway -- the service carrying the
// other two confirmed instances (GetResources' Embed, GetBasePathMapping's
// DomainNameId) -- dispatches through `map[string]actionFn` (a locally
// named func type, not service.JSONOpFunc) into functions decoding a NAMED
// local struct via a bare json.Unmarshal call. cloudfront's third confirmed
// instance (ListDistributionsByRealtimeLogConfig's RealtimeLogConfigName)
// resolves only through a helper function called FROM the dispatched
// closure, whose return type -- not any decode call inside it -- IS the
// request struct.
//
// So resolution here generalizes cmd/reqfieldscan's two building blocks
// rather than reusing them outright (dispatch.go, resolve.go):
//
//   - Dispatch-table recognition (isDispatchMapType) accepts ANY
//     map[string]<func-shaped-value> composite literal -- a literal func
//     type, a locally-declared named func type (apigateway's actionFn), or
//     service.JSONOpFunc specifically -- not only the latter. The
//     slice-of-struct binder shape generalizes the same way (any bind
//     field of function type, not only one returning JSONOpFunc).
//   - A resolved dispatch value is unwrapped through func-literal closures
//     (their first return statement, recursively) to either a
//     service.WrapOp call (resolved exactly as cmd/reqfieldscan does, via
//     the handler's own *In parameter type) OR a plain function/method
//     call or reference, whose OWN BODY is then scanned directly
//     (resolve.go's scanBody) for a decode signal: a json/xml.Unmarshal or
//     echo Bind call binding a locally-known struct type, a call to a
//     package function whose declared return type IS a known struct
//     (cloudfront's decodeXBody(c) shape), or a literal
//     QueryParam/Param/FormValue("name") call, harvested directly as a
//     declared wire name with no struct behind it at all (apigateway's
//     resourceActions shape, and the many services that read echo params
//     with no struct in between). Exactly ONE hop of recursion into a
//     `h.<Method>(...)` or bare package-func call the handler makes is
//     followed (maxHop in resolve.go) -- never into `h.Backend.X`, so a
//     backend's own internal field names can never leak in as false
//     "declared" matches. This is the same single-hop discipline
//     cmd/reqfieldscan discloses for its own field-coverage pass.
//   - When a dispatch-table entry doesn't exist AT ALL for an op, or
//     resolves to nothing usable, a name-convention search
//     (findHandlerByName) looks for "handle"+Op (then the suffixed
//     variants cmd/reqfieldscan's package doc names -- Full/Accurate/
//     WithOpts -- then case-insensitively), and this repo's other observed
//     convention, lowerCamel(Op)+"Action" / Op+"Action" (apigateway's own
//     shape). resolveOp in resolve.go runs BOTH the dispatch-table and
//     name-convention searches and UNIONS whatever each finds, rather than
//     picking one and stopping the moment either "succeeds" -- deliberately
//     over-inclusive, so an unresolved dispatch value can never suppress a
//     handler sitting right there under its conventional name.
//
// THIS TOOL'S OWN INHERITED BLIND SPOTS, checked against
// cmd/reqfieldscan's seven:
//  1. Slice-of-struct dispatch table (glue): generalized in binderFields,
//     same as reqfieldscan's fix.
//  2. Local generic wrapper (cognitoidp's wrapAccuracy[I,O](fn)):
//     collectLocalWrapOpWrappers is reqfieldscan's identical logic,
//     type-parameter-agnostic since it only inspects the function body's
//     first return statement.
//  3. Handler name suffixes (handle<Op>Full/Accurate/WithOpts):
//     findHandlerByName tries all three explicitly.
//  4. Go type alias in the struct collector: resolveStructAliases,
//     reqfieldscan's identical logic.
//  5. Anonymous inline struct decoding (opsworks, and THIS TOOL'S OWN
//     omics ground truth): collectAnonReqStructs, reqfieldscan's identical
//     logic, keyed by file:line.
//  6. Method receiver not bound during local-binding collection:
//     bindFieldList binds fd.Recv exactly as cmd/reqfieldscan's
//     coverage.go does.
//  7. A second in-package dispatch table behind suffixed/colliding names:
//     UNCHANGED FROM REQFIELDSCAN, still unpatched there, and this tool
//     inherits the same exposure -- collectDispatchEntries unions every
//     map/binder literal it finds package-wide with no de-duplication by
//     which "logical" table they belong to, so two colliding op names
//     across two separate tables would silently let the second overwrite
//     the first in the entries map. Not observed to matter in this
//     campaign's services, exactly as reqfieldscan's own note says; not
//     patched here either, for the same reason -- a fix would need a
//     concrete failing instance to design against, and none has surfaced.
//
// TRIAGE (triage.go) ranks each undeclared field by, in order: a
// documented default (defaultLanguageRe) -- a field the campaign's own
// history says produced 19 of its confirmed bugs, and the entirety of this
// tool's omics ground truth (RetentionMode/ScratchStorageMode/
// StorageCapacity/StorageType on StartRun, each with a stated default,
// none declared); a filter/range/page-size field on a List/Describe/Search
// op; a sibling operation in the same service that DOES declare the same
// normalized field name; and SDK-required. A field whose doc comment
// starts "Deprecated:" is excluded from findings entirely, counted
// separately. Everything else ranks lowest, explicitly labeled "no strong
// signal" rather than omitted -- this tool reports a raw, ranked queue, it
// does not decide what's a bug.
//
// WHAT THIS TOOL CANNOT TELL YOU, stated plainly rather than left implicit:
//   - It cannot distinguish a missing field from a deliberate, already-
//     recorded structural gap (a capability this backend does not model at
//     all -- a cross-account view, a VPC association). Roughly thirty such
//     gaps are on record across this campaign, reasoned individually
//     ("its enum has exactly one legal value and every record carries it",
//     "the listing returns an empty slice unconditionally") -- this tool
//     has no access to that reasoning and will re-flag every one of them.
//     Every finding is a LEAD for a human or a sweep, never a verdict.
//   - It only compares an operation's TOP-LEVEL Input fields, never fields
//     nested inside a sub-struct (a Filter type's own members, a nested
//     config object). A field missing one level down is invisible to this
//     scan by construction, the same way an undeclared field was invisible
//     to cmd/reqfieldscan. This was a deliberate scope cut, not an
//     oversight: every ground-truth instance this tool was validated
//     against (omics' four StartRun parameters, apigateway's Embed and
//     DomainNameId, cloudfront's RealtimeLogConfigName) is a top-level
//     Input field, so the cut cost nothing against known ground truth --
//     but it means a nested filter struct missing members entirely would
//     not be caught here.
//   - Name matching (normalizeWireName) is a case-and-separator-insensitive
//     fold, nothing more. A wire name that diverges semantically from a
//     simple case-fold of the Go field name -- an abbreviation expanded or
//     contracted, a genuine rename -- will not match, and reads as a false
//     "undeclared" finding.
//   - It says nothing about whether a DECLARED field is read correctly, or
//     at all -- that's cmd/reqfieldscan's axis for whether it's read, and
//     gopherstack-uox6's axis entirely for whether it's read CORRECTLY. A
//     field this tool calls "declared" might still be silently ignored or
//     misapplied; those are different bugs on different axes.
//   - The coverage guard (report.go) catches an implausible RESOLUTION
//     number, never an implausible TRIAGE. A field ranked "no strong
//     signal" that is in fact a real bug will not be elevated by anything
//     here -- the triage signals are a ranking heuristic over a raw diff,
//     not a classifier, and the tool says so in its own output rather than
//     implying otherwise.
//   - QUERY-PROTOCOL FORM-READ DETECTION (formreads.go, gopherstack-99nj).
//     An AWS query-protocol service (ec2, rds, s3, iam, autoscaling, elb,
//     ses, cloudwatch, ...) reads its fields off a raw url.Values, not any
//     struct decode call this scan otherwise recognises -- a field read
//     this way is invisible to every other signal in this file, so a
//     correctly-handled field still reads as undeclared. A blanket
//     `.Get("literal")` signal was deliberately rejected as too risky: that
//     name is used for every unrelated map/cache Get() call in this repo,
//     and matching it by name alone would trade a real resolution gain for
//     a worse one -- false "declared" matches that silently suppress
//     genuine findings. What's actually implemented is narrower, because
//     this scan already resolves each operation's own handler AND already
//     has that operation's own SDK Input field names in hand (sdkfields.go)
//     before it ever scans a body: the candidate key set a form-read call
//     is allowed to match is restricted to THIS operation's own field
//     names, normalized, plus a singular variant for the query-protocol
//     convention where a plural field (KeyNames) is read from singular
//     indexed member keys (KeyName.1, KeyName.2, ...). Two shapes are
//     recognised, both gated on the receiver/argument being a url.Values-
//     typed PARAMETER of the function being scanned (never a reassigned
//     local, never a package-level cache): `vals.Get("Name")` directly, and
//     a call to a package-level helper whose own first parameter is
//     url.Values (ec2's parseMemberList, rds's extractMemberList/
//     extractIndexedList, iam's parseIndexedValues, autoscaling/elb's
//     parseMembers, ses's parseSESMemberList, cloudwatch's parseMemberList/
//     parseDimensionsFromForm, ... -- recognised structurally by that
//     signature, not by name) carrying a PascalCase string-literal
//     argument. A nested-prefix literal ("AssociationTarget.InstanceId") is
//     matched by its first dot-segment against the top-level field this
//     scan is scoped to. NOT covered, deliberately left as findings rather
//     than guessed at: a url.Values held in a reassigned local rather than
//     a parameter (`q := c.Request().URL.Query()`); a chained accessor
//     (`c.Request().Form.Get(...)`); a helper that is a method rather than
//     a bare package function; a nested-prefix literal more than one
//     dot-segment deep; and irregular English plurals singularVariant's
//     simple suffix-strip doesn't cover. Validated against ground truth:
//     ec2's 26 hand-verified identifier-list fields and the six
//     MaxResults/NextToken fields fixed
//     in 427bd2b15 are no longer reported (both confirmed present before
//     this change and absent after); ecs and omics -- neither
//     query-protocol, neither using url.Values at all -- produce byte-
//     identical findings before and after.
//   - dynamodbstreams decodes directly into the real aws-sdk-go-v2 input
//     type itself (`var input dynamodbstreams.GetRecordsInput`, and a
//     generic `dispatchStreamsOp[In any, Out any]` helper inferring In from
//     the backend method's own signature) -- a foreign, imported qualified
//     type this scan's struct collector (locally-declared types only)
//     cannot see. Hand-confirmed: this makes dynamodbstreams's true
//     coverage 100% by construction, not the "0/4, zero declared fields"
//     the coverage guard reports for it -- a case where the guard's own
//     loud failure is the CORRECT caution (a human must still read the
//     flagged service to learn this), not a false alarm to silence.
//
// Usage:
//
//	go run ./cmd/reqfielddiff                       # scan every services/<dir>
//	go run ./cmd/reqfielddiff -dir omics,apigateway  # scan only these
//	go run ./cmd/reqfielddiff -json out.json         # also write the full report as JSON
//
// Exit codes: 0 no findings and no coverage warning in any scanned service,
// 1 a run error, 2 at least one non-deprecated undeclared field found, or
// at least one service tripped a coverage warning.
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

	findings := 0
	warned := 0

	for _, r := range reports {
		printServiceReport(r)

		findings += len(r.Findings)
		if len(r.Warnings) > 0 {
			warned++
		}
	}

	if findings > 0 || warned > 0 {
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
		r, scanErr := scanOneService(repoRoot, dir)
		if scanErr != nil {
			return nil, fmt.Errorf("%s: %w", dir, scanErr)
		}

		reports = append(reports, r)
	}

	return reports, nil
}

// skippedReport carries a module- or SDK-resolution failure for one
// services/<dir> as a report field rather than a run error: that service is
// skipped, the whole scan continues.
func skippedReport(dir, mod string, err error) serviceReport {
	return serviceReport{Dir: dir, Module: mod, ModuleErr: err.Error()}
}

func scanOneService(repoRoot, dir string) (serviceReport, error) {
	name := filepath.Base(dir)

	mod, _, modPath, err := resolveModule(repoRoot, name)
	if err != nil {
		return skippedReport(name, "", err), nil
	}

	sdkOps, err := loadSDKOps(modPath)
	if err != nil {
		return skippedReport(name, mod, err), nil
	}

	if len(sdkOps) == 0 {
		return serviceReport{Dir: name, Module: mod, ModuleErr: "no <Op>Input structs found in pinned SDK module"}, nil
	}

	idx, err := buildPackageIndex(dir)
	if err != nil {
		return serviceReport{}, err
	}

	resolutions := idx.resolveOps(sdkOps)

	return buildServiceReport(name, mod, sdkOps, resolutions), nil
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
