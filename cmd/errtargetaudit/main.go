// Command errtargetaudit finds gopherstack-o46l's class: a REAL,
// correctly-spelled AWS error code -- present in the pinned SDK, legitimately
// correct elsewhere in the same service -- emitted by an operation whose OWN
// error deserializer never declares it. A real client's errors.As into the
// typed exception it should see never fires; the request fails with an
// opaque smithy.GenericAPIError instead. This is a DIFFERENT question from
// cmd/errcodeaudit's: that tool finds a code the SDK never defines ANYWHERE
// (fabricated out of nothing). Two manual sweeps (commits d7149d0f8,
// 19f3d65f0) found 29 of this tool's class across five services;
// cmd/errcodeaudit reported zero findings in all five, correctly -- it was
// answering a different question, not missing this one.
//
// GROUND TRUTH is per-operation, not per-service (deser.go): for each
// services/<dir>, every resolved pinned SDK module's own deserializers.go is
// read for every function whose name contains "deserializeOpError" (the
// same protocol-agnostic marker cmd/errcodeaudit's sdktruth.go already
// relies on -- confirmed the same shape across every protocol in this
// repo's pinned SDKs). The codes matched via strings.EqualFold inside THAT
// FUNCTION'S OWN BODY are that operation's declared set -- never merged
// across operations, which is the entire point: a code declared for a
// sibling operation must never leak into this one's. types/errors.go's own
// ErrorCode() literals are unioned with every operation's declared codes
// into a separate, SERVICE-WIDE "real code universe", used only to tell a
// real-but-misplaced code (class A, this tool) apart from a fabricated one
// (class B, cmd/errcodeaudit's job -- silently excluded here, never
// double-reported).
//
// RESOLVING WHICH HANDLER SERVES AN OPERATION reuses cmd/reqfielddiff's
// solved half of this problem (resolveop.go, dispatch.go, pkgindex.go):
// dispatch-table recognition (map literal, slice-of-struct binder,
// switch-statement dispatch) UNIONED with a name-convention fallback
// ("handle"+Op, the Full/Accurate/WithOpts suffixes, lowerCamel(Op)+"Action",
// bare lowerCamel(Op), then a case-insensitive match) -- reimplemented, not
// imported, because cmd/reqfielddiff and cmd/reqfieldscan are existing
// tools this campaign does not modify; see gopherstack-o46l's own filing
// for why re-deriving that resolution here, rather than reusing it, is
// exactly the mistake to avoid -- this reimplementation deliberately tracks
// the same shapes and the same "union, don't pick one and stop" philosophy.
//
// WHERE THIS TOOL GENERALIZES PAST THAT RESOLUTION, because a request FIELD
// and an error CODE live in structurally different places:
//
//   - Recursion depth is the same one hop (maxEmitHop in emit.go) as
//     cmd/reqfieldscan/cmd/reqfielddiff's maxHop, but the receiver it
//     follows is NOT restricted to this repo's uniform "h" Handler name.
//     Those tools stop at "h.<Method>" specifically to keep a BACKEND's
//     internal field names from leaking in as false "declared wire field"
//     matches. That hazard does not exist here: in three of the four
//     commits this tool was validated against, the actual sentinel-error
//     return sits in the BACKEND method a handler calls, one hop away, and
//     finding it is the whole point. So this tool follows any
//     `X.Method(...)` or bare `func(...)` call one hop, any receiver.
//   - Ground truth is never a request TYPE, only a function BODY -- so
//     resolveop.go's roots carry no struct bindings at all, only the
//     *ast.BlockStmt to scan and the resolved FuncDecl's receiver-type name
//     ("domain"), needed only for module assignment below.
//
// THE CLASSIFIER LAYER (classifiers.go) is this tool's own addition, built
// once per service and shared across every operation's walk, because the
// real emission site in this repo is almost never a literal code string --
// it is a SENTINEL passed through a shared mapper. Three shapes, all
// observed directly in the four validated commits:
//
//   - services/bedrock, services/iot, services/backup: a package-level
//     `var ErrX = errors.New(...)` sentinel, matched via `errors.Is(err,
//     ErrX)` in a switch or if-chain whose branch renders a fixed code
//     literal (`c.JSON(status, errorResponse("ConflictException", ...))`).
//     sentinelCodes scans every such switch/if in the package (there can be
//     more than one mapper -- services/iot's real shape has a general one
//     plus a stricter override the FIX introduces, never the pre-fix bug
//     state this tool targets) and builds one flat sentinel-name -> code
//     table.
//   - services/networkmanager: the sentinel is wrapped one hop deeper,
//     inside a locally-declared error TYPE's field (`&apiError{cause:
//     errNotFoundSentinel, ...}`), built by a constructor function
//     (notFoundError, validationError, ...) whose own body never mentions a
//     code literal at all -- the SAME sentinel table still resolves it,
//     because that constructor's cause field is just another bare
//     reference to a known sentinel one AST level down. constructorCode
//     follows exactly one hop of this indirection (any package-level func
//     whose LAST result is bare `error`, scanning its own return
//     statements -- including nested composite-literal field values and
//     fmt.Errorf's %w slot -- for a sentinel reference), matching this
//     repo's standing one-hop discipline. A constructor that itself calls
//     ANOTHER constructor, rather than referencing a sentinel directly, is
//     NOT resolved -- disclosed below, not silently missed.
//   - Outside the sentinel-mapper pattern entirely: services/ecs's own
//     direct mechanism (awserr.New/Newf, a Code/ErrorCode/Type-labeled
//     composite-literal field, a code-named var/const) is matched too, a
//     narrowed subset of cmd/errcodeaudit/extract.go's six rules (no
//     sink.go call-signature table, no positional-struct-field resolution,
//     no mapper.go central-table detection) -- enough to catch a service
//     that emits codes directly, at the cost of a call-site-argument sink
//     this tool cannot recognise as one; see BLIND SPOTS.
//
// AN OPERATION NAME COLLIDING ACROSS TWO PINNED MODULES
// (services/bedrock's PutResourcePolicy: a real, DIFFERENT operation in
// each of the bedrock and bedrockagent APIs, sharing one op-name string) is
// resolved by moduleassign.go DATA-DRIVEN, never by matching a Go type name
// to a module name: each domain (the resolved handler's own receiver-type
// name) is assigned to whichever candidate module's own known-operation set
// overlaps it most -- bedrock's "Handler" domain resolves ~108 operations
// that overlap heavily with the "bedrock" module and barely with
// "bedrockagent", and vice versa for "AgentsHandler". A domain whose best
// overlap is zero or tied is left UNASSIGNED, and every operation reachable
// only through it is skipped rather than checked against a guessed module.
// This is the one piece of machinery neither cmd/reqfieldscan nor
// cmd/reqfielddiff needed at all: a request FIELD's ground truth is always
// exactly one operation's Input struct, never ambiguous across modules the
// way an error code's operation-name key can be.
//
// INHERITED BLIND SPOTS, checked one by one against cmd/reqfieldscan's
// seven and cmd/reqfielddiff's identical list:
//  1. Slice-of-struct dispatch table: generalized in binderFields, same fix.
//  2. Local generic wrapper (cognitoidp's wrapAccuracy[I,O](fn)):
//     collectLocalWrapOpWrappers, identical logic.
//  3. Handler name suffixes (Full/Accurate/WithOpts): findHandlersByName
//     tries all three explicitly.
//  4. Go type alias in the struct collector: DOES NOT APPLY -- this tool
//     collects no structs at all, only function bodies, so there is no
//     struct-alias indirection to miss in the first place.
//  5. Anonymous inline struct decoding (opsworks): DOES NOT APPLY, same
//     reason as (4) -- this tool never needs to know a decode TARGET TYPE,
//     only whether a call site emits a code.
//  6. Method receiver not bound during local-binding collection: DOES NOT
//     APPLY -- this tool collects no per-function local bindings at all
//     (no field reads to resolve), only calls and returns.
//  7. A second in-package dispatch table behind suffixed/colliding names:
//     CHECKED DIRECTLY against this tool's own bedrock validation target,
//     which is exactly this shape (two real dispatch mechanisms in one
//     package, one op name -- PutResourcePolicy -- shared between them).
//     It does NOT bite here, but not because collectDispatchEntries
//     resolves the collision: bedrock's own two PutResourcePolicy handlers
//     have DIFFERENT Go names (handlePutResourcePolicy vs
//     handlePutKnowledgeBaseResourcePolicy), so the name-convention
//     fallback resolves each uniquely without ever needing to disambiguate
//     a shared key. A service where two same-named handlers ALSO shared a
//     literal Go function name would still silently collide in
//     idx.Dispatch exactly as reqfieldscan/reqfielddiff's own disclosed
//     blind spot describes -- unpatched here for the same reason those
//     tools give: no concrete failing instance has surfaced to design
//     against.
//
// THIS TOOL'S OWN BLIND SPOTS, new to this class rather than inherited:
//   - errors.As / type-switch classification (`switch err.(type) { case
//     *NotFoundError: ... }`) is NOT modeled -- only errors.Is-against-a-
//     sentinel is. Every one of the 29 validated bugs resolves through a
//     sentinel-var mapper (even services/networkmanager's apiError type
//     ultimately renders via classifyError's own errors.Is switch on its
//     wrapped cause), so this cut cost nothing against known ground truth,
//     but a service whose ONLY mapper switches on concrete error TYPES
//     with no underlying sentinel at all would be invisible to this scan.
//   - A constructor function that wraps ANOTHER constructor, rather than a
//     sentinel directly, resolves to nothing (one hop only, matching this
//     repo's standing discipline) -- silently unresolved, never a false
//     finding. And a function IS EXCLUDED from constructor candidacy the
//     moment its own name matches a real ground-truth operation name
//     (buildClassifiers's opNames parameter) -- confirmed necessary, not
//     precautionary: an early version treated every backend method with a
//     bare `error` return (services/iot's DeleteThing/CancelJob/... shape,
//     extremely common in this repo) as a constructor too, which not only
//     double-counted a finding under two mechanisms but, worse, BYPASSED
//     the override-suppression below entirely (a misclassified backend
//     method's code is baked in at buildClassifiers time, before any
//     op-specific override is known) -- caught by this tool's own
//     TestScan_OverrideMapper_SuppressesGeneralMapping test failing against
//     the pre-fix implementation.
//   - An "override" mapper -- a helper taking the comparison sentinel as
//     its OWN parameter (services/iot's post-fix respondAsInvalidRequest
//     shape: `if errors.Is(err, sentinel) { return fixedCode }`) IS modeled
//     (detectOverrideFuncs/effectiveClassifiers), added during this tool's
//     own validation pass after it produced two confirmed false positives
//     on iot's CancelJob and DeleteThing (already-fixed, post-fix code
//     using exactly this shape) -- see classifiers.go's doc comment for the
//     mechanism. Still not modeled: an override whose comparison argument
//     at the call site is itself a computed/indirect expression rather than
//     a bare sentinel identifier, and an override applied only at hop 1 or
//     deeper (this scan looks for the override call in hop-0 roots only).
//   - Direct-literal extraction is a narrowed subset of
//     cmd/errcodeaudit/extract.go's six rules -- no sink.go call-argument
//     position table (a "...Error"-suffixed call is invisible here, where
//     errcodeaudit resolves its actual sink argument), no mapper.go central
//     table detection, no positional (unkeyed) struct-field resolution. A
//     composite-literal "Code"-labeled field is DELIBERATELY excluded (only
//     "ErrorCode"/"Type" are read) after a confirmed false positive on
//     services/bedrock's BatchDeleteAdvancedPromptOptimizationJobError{Code:
//     "ResourceNotFoundException", ...} -- a per-ITEM result field in a 200
//     OK batch response, not a wire error envelope; see emit.go's
//     isCodeFieldLabel doc comment. A service relying on one of those
//     narrowed-out shapes for its ONLY emission mechanism is under-covered
//     here, though such a service would also need the sentinel-mapper
//     machinery above to be absent for a finding to be missed entirely.
//   - A code assembled through string concatenation, fmt.Sprintf, or read
//     from a request field is invisible, same limitation cmd/errcodeaudit
//     already discloses.
//   - PER-OPERATION GROUND TRUTH ITSELF IS ABSENT for a pinned SDK module
//     using newer Smithy/RPCv2CBOR-generation codegen with NO
//     deserializers.go file at all (confirmed live: services/appstream,
//     services/cloudwatch's OWN module, at aws-sdk-go-v2 versions pinned by
//     this repo's go.mod) -- error matching there lives per-operation in
//     each api_op_<Op>.go file, in a DIFFERENT shape entirely (`switch
//     string(errorName) { case "ConflictException": ... }`, a plain string
//     switch, never strings.EqualFold). deser.go's stringSwitchCaseLiteral
//     reads THIS shape too, when it lives in deserializers.go (appstream's
//     case: a deserializeOpError<Op> function exists, just with plain
//     string-literal case labels rather than EqualFold calls) -- before
//     this was added, every RPCv2CBOR-protocol operation read as declaring
//     ZERO codes, so EVERY emission there was flagged, a confirmed
//     large false-positive source (appstream: ~15 of 17 emitting ops, all
//     spurious, before the fix). But a module with NO deserializers.go file
//     on disk at all (services/cloudwatch's own "cloudwatch" module -- its
//     112 "ground truth" operations in this scan's own early output turned
//     out to be an UNRELATED cross-imported "s3" module's op set instead,
//     caught only by the coverage guard reporting 0/112 resolved) is
//     invisible to this tool's ground truth by construction: the codes now
//     live in each api_op_<Op>.go file, one file per operation, which this
//     tool does not read at all. Confirmed to affect at least
//     services/cloudwatch; not resurveyed against the other 12 services this
//     scan's own coverage guard flagged (see next point) to know how many
//     more share it.
//   - A REPO-WIDE PATTERN, not specific to this tool: many services/<dir>
//     test files import an entirely unrelated service (s3 and dynamodb by
//     far the most common, confirmed live across cloudformation, cloudwatch,
//     dax, dynamodb, firehose, glacier, iam, kinesisanalytics, mgn,
//     stepfunctions) for some shared cross-service test helper --
//     cmd/errcodeaudit's own doc comment names the identical ec2/outposts
//     case. resolveServiceModules (matching cmd/errcodeaudit's own,
//     deliberately test-file-inclusive resolution) has no way to tell "the
//     service's own module" apart from "an incidental cross-import," so a
//     service whose OWN module contributes little or no per-op ground truth
//     (the RPCv2CBOR case above, or simply a module this repo's go.mod
//     doesn't pin a version for) can have its OpsGroundTruth count silently
//     dominated by the unrelated import's op names instead -- exactly what
//     happened to services/cloudwatch. The coverage guard's "resolved
//     ratio" check is what actually catches this in practice (a foreign
//     service's op names essentially never resolve to this service's own
//     handlers), and did so for all 13 services this pattern or the one
//     above touches in this scan's own full run -- but the guard is a
//     symptom check, not a diagnosis; a human still has to read which case
//     produced the warning, as this section did for two of the thirteen.
//
// WHAT THIS TOOL CANNOT TELL YOU, stated plainly:
//   - It cannot distinguish a REACHABLE handler from an unreachable one. A
//     dead code path a real client can never route to (this campaign has
//     found at least one, in services/iot) produces a finding exactly as
//     confident as a live one. Only driving a real client and watching the
//     router settles that.
//   - A code being DECLARED for an operation does not mean it is the RIGHT
//     code for the actual failure condition -- only that a real client's
//     errors.As into it would succeed. This tool checks wire-shape
//     reachability, never semantic correctness (gopherstack-uox6's axis
//     entirely).
//   - Attribution through a shared sentinel is APPROXIMATE, not certain: a
//     sentinel or constructor used correctly by most callers and wrongly by
//     one is attributed per (operation, domain) pair as precisely as this
//     tool's one-hop resolution reaches, but a deeper or more indirect
//     emission path than what classifiers.go models will simply not
//     surface, silently, not as a wrong finding.
//   - A "declared" verdict is drawn from EqualFold code LITERALS in the
//     operation's own deserializer switch; an SDK version whose
//     deserializer falls straight through to a generic decode for most
//     codes (cmd/errcodeaudit's own s3-class "sparsely modeled" case) would
//     make every absence here weak evidence, not strong -- this tool does
//     not currently carry that module's own sparse-coverage flag the way
//     cmd/errcodeaudit's moduleCodes.sparselyModeled does; a finding
//     against a thinly-modeled module should be treated with the same
//     caution that flag exists for.
//
// Usage:
//
//	go run ./cmd/errtargetaudit                       # scan every services/<dir>
//	go run ./cmd/errtargetaudit -dir bedrock,iot       # scan only these
//	go run ./cmd/errtargetaudit -json out.json         # also write the full report as JSON
//
// Exit codes: 0 no findings and no coverage warning in any scanned service,
// 1 a run error, 2 at least one class A finding, or at least one service
// tripped the resolution guard above, in at least one scanned service.
package main

import (
	"flag"
	"fmt"
	"os"
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
	jsonOut := flag.String("json", "", "write the full scan list to this path as JSON")
	flag.Parse()

	scans, err := run(*dirFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(exitRunError)
	}

	if *jsonOut != "" {
		if werr := writeJSON(*jsonOut, scans); werr != nil {
			fmt.Fprintln(os.Stderr, "write json:", werr)
			os.Exit(exitRunError)
		}
	}

	findings := 0
	warned := 0

	for _, sr := range scans {
		printServiceScan(sr)

		findings += len(sr.Findings)
		if len(sr.Warnings) > 0 {
			warned++
		}
	}

	summarize(scans, findings, warned)

	if findings > 0 || warned > 0 {
		os.Exit(exitFindings)
	}

	os.Exit(exitClean)
}

func summarize(scans []serviceScan, findings, warned int) {
	scanned := 0

	for _, sr := range scans {
		if sr.OpsGroundTruth > 0 {
			scanned++
		}
	}

	fmt.Fprintf(os.Stdout, "# %d services scanned, %d class A findings, %d coverage warnings\n",
		scanned, findings, warned)
}

func run(dirFlag string) ([]serviceScan, error) {
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

	dirs, err := targetDirs(filepath.Join(repoRoot, "services"), dirFlag)
	if err != nil {
		return nil, err
	}

	var scans []serviceScan

	for _, dir := range dirs {
		sr, scanErr := scanServiceDir(dir, repoRoot, cache, goModVersions)
		if scanErr != nil {
			return nil, fmt.Errorf("%s: %w", dir, scanErr)
		}

		if sr.OpsGroundTruth == 0 {
			continue
		}

		scans = append(scans, sr)
	}

	return scans, nil
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

	return serviceDirs(svcRoot)
}
