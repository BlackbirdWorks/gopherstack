// Command keycheck verifies that the string keys a service handler writes
// into its map[string]<T> wire responses, AND the json struct tags on any
// locally-declared *Output-suffixed struct it constructs, actually exist in
// the pinned AWS SDK's own response deserializer -- not the Go field name,
// the deserializer's literal switch-case string. It exists for
// gopherstack-zquj (the map-key half) and gopherstack-v4a4 (the struct-tag
// half, added after glue's querySchemaVersionMetadataOutput was found
// tagged json:"MetadataInfo" where the SDK expects "MetadataInfoMap",
// commit c3aa73e59): a wrong key or tag is checked by no compiler and no
// existing scan, so it is silently dropped by any real client and invisible
// to a raw-body test (which asserts the same key the author typed).
//
// For each op it builds the real wire key set from the pinned SDK's
// <prefix>deserializeOpDocument<Op>Output case-switch, recursing through
// nested <prefix>deserializeDocument<Type> calls, and diffs that set against
// every string key the handler's reachable call graph writes into a
// map[string]<T>.
//
// PROTOCOL COVERAGE. Validated on awsjson1.1 (shield, ssoadmin,
// gopherstack-zquj first pass, 115 ops) and on restjson1 document(body)-bound
// members (scheduler's pre-fix awsvpcConfiguration/capacityProvider bug,
// commit 8469dcdd9). Both protocols' codegen emits the same
// <prefix>deserializeOpDocument<Op>Output / <prefix>deserializeDocument<Type>
// functions with a map[string]interface{} type-switch body, which is the
// only shape this scanner parses -- pass the matching -prefix
// (awsAwsjson11_, awsAwsjson10_, awsRestjson1_) and it works.
//
// It does NOT understand restjson1 members bound to an HTTP header or the
// status line: those never appear in a deserializeOpDocument function, so a
// handler that legitimately writes such a key will false-positive as
// NotInTree. Hand-check any restjson1 MISMATCH against the op's http.header
// trait before trusting it.
//
// It does NOT understand query, ec2query, or restxml/xml protocols at all --
// their deserializers are xml.Decoder based with no map[string]interface{}
// type assertion, so -sdk parsing resolves zero ops and zero types against
// them. See FAIL-LOUD: that state is reported as an explicit error, never a
// silent zero.
//
// FAIL-LOUD. Any state meaning "this service was not actually checked" is an
// explicit stderr row and a non-zero exit, never a bare zero:
//
//   - -sdk yields zero deserializeOpDocument/deserializeDocument matches for
//     -prefix: wrong prefix, or a protocol this tool can't read.
//   - the -svc package's dispatcher resolves zero op-to-handler bindings.
//   - a dispatched op has no way to resolve its allowed key set: neither a
//     deserializeOpDocument<Op>Output function NOR a wrapper type
//     (<prefix>deserializeOp<Op>) confirming a genuinely empty output. (The
//     SDK omits the document deserializer entirely when an Output struct has
//     no members beyond ResultMetadata; that is confirmed by the wrapper's
//     HandleDeserialize body calling no deserializeOpDocument* function, not
//     merely by the function's absence -- six ssoadmin ops are this case and
//     are correctly resolved as empty, not unresolved.)
//   - an op is bound to more than one distinct handler name (KNOWN BLIND
//     SPOT #6, sqs's dual JSON/Query handlers) -- neither is silently
//     preferred; the op is reported ambiguous instead of checked.
//
// The service writing zero map[string]<T> literal keys anywhere is reported
// as N/A, not as "0 mismatches, clean": it means the service builds
// responses from tagged structs rather than hand-written maps, which is a
// different construction this tool has nothing to check.
//
// A found MISMATCH exits non-zero too (a different code from an unresolved
// service), so this can gate CI once trusted.
//
// KNOWN BLIND SPOT #1, disclosed rather than fixed: this checks whether a
// written key exists ANYWHERE in the op's transitively reachable shape, not
// whether it sits at the correct nesting level. A key real at one depth but
// wrongly placed at another will not be caught. Hand-check the
// highest-surface op in any service audited with this tool.
//
// KNOWN BLIND SPOT #2, found live during the gopherstack-zquj sweep: the
// written-key BFS walks the op handler's full same-package call graph
// (writtenKeys, capped at 200 funcs) with no way to tell "this map literal
// becomes the wire response" from "this map literal is written somewhere
// else entirely" -- internal persisted state (event-history records),
// request-body transformation (rewriting a state-machine definition before
// handing it to an internal executor), or any other side effect reachable
// from the handler. A handler that calls into unrelated backend code
// (openCountsLocked -> the timeout-sweep janitor -> terminateExecutionLocked
// in swf, confirmed live) can pull in keys that were never going on this
// op's wire response at all, and will misreport as MISMATCH. The signal to
// distrust a hit: a large FuncsWalked count (or hitting the 200-func cap
// outright, as dynamodb does) relative to what the op plausibly needs to
// build its own response. Re-run with -op and KEYCHECK_DEBUG_WALK=<Op> (see
// below) to print the exact call chain and hand-verify which function
// actually wrote the flagged key, and whether that write reaches the HTTP
// response, before trusting any MISMATCH this tool reports.
//
// TWO RECURRING SHAPES of blind spot #2, both found live re-sweeping for
// gopherstack-zquj and worth naming explicitly rather than re-discovering
// per service: (a) a shared helper writes a key inside an "if cond"
// conditional (e.g. comprehend's matchResult only sets "Type" when its kind
// argument is non-empty); every caller of the helper is credited with the
// key regardless of whether that call site's arguments ever satisfy the
// condition, so a caller that always passes the empty case (DetectKeyPhrases
// calling matchResult with kind="") gets a false MISMATCH for a key it can
// never actually emit. (b) the walk reaches an op's own error-path
// construction (a *_test.go-free exception/failure type built for the same
// op, e.g. timestreamwrite's RejectedRecords[].ExistingVersion), which is
// real and correct on the error response but doesn't appear in the success
// deserializer this tool diffs against.
//
// KNOWN BLIND SPOT #4, found live during the gopherstack-0kk8 dispatch-table
// sweep: analyzeFunc/extractCases only understands a deserializer function
// shaped as a map[string]interface{} type-assert followed by a switch on
// fixed, known key strings (an AWS "structure" shape). A genuine AWS *map*
// member (map[string]T with caller-controlled dynamic keys, e.g. glue's
// QuerySchemaVersionMetadata MetadataInfoMap or personalize's
// GetSolutionMetrics Metrics) deserializes with the same map[string]
// interface{} type-assert but a for-range-and-single-call loop instead of a
// switch -- extractCases finds no *ast.SwitchStmt, so that type resolves
// with an empty case list. Every key the handler legitimately writes into
// the corresponding hand-written map then reports as a false NotInTree
// MISMATCH. Confirmed live sweeping glue (MetadataInfoMap), ssm
// (aggregation-result Count fields) and personalize (Metrics) after fixing
// their dispatch-table resolution: all of their post-fix MISMATCH rows
// trace to this gap or to blind spot #2, not to a real dropped key -- see
// gopherstack-0kk8. Hand-check any MISMATCH whose written keys look like
// data (language codes, entity-type names, metric names) rather than
// members of a fixed schema before trusting it.
//
// KNOWN BLIND SPOT #5, the struct-tag half added for gopherstack-v4a4: only a
// composite literal of a locally-declared struct type whose NAME ends in
// "Output" is recognised (this repo's overwhelming convention, 542 non-test
// occurrences) -- an anonymous struct literal, a differently-named type, or a
// struct built field-by-field via `var out X; out.Field = ...` rather than
// one literal contributes nothing and is invisible to this scan. A field with
// no json tag at all is assumed to marshal under its Go field name (encoding/
// json's real default), which is occasionally wrong on its own and is not
// itself flagged. Neither gap is fixed to keep the corresponding NoWrittenKeys
// (N/A) path honest rather than silently under-reporting as clean. FIXED, in
// the same fallback: an UNEXPORTED field with no json tag used to fall back
// to its lowercase Go name too, which encoding/json never does -- it never
// marshals an unexported field regardless of a tag's presence. Found live
// triaging batch's newly-resolved (KNOWN BLIND SPOT #7) ops:
// ComputeEnvironment's deliberately-unexported `region` field (kept off the
// wire on purpose, see its own doc comment in services/batch/models.go)
// fabricated a false "region" MISMATCH on every op reachable from
// DescribeComputeEnvironmentsOutput -- five real ops, zero real bugs.
// structTagFields now skips any field whose Names[0] is unexported entirely
// (no key, no recursion into its type), pinned by
// TestRunCheck_StructTagIgnoresUnexportedField.
//
// KNOWN BLIND SPOT #6, found live sweeping sqs for gopherstack-v4a4, FIXED
// for gopherstack-kiwf: a package can declare TWO handler functions for the
// same op name -- sqs hosts both a "handle<Op>" JSON handler (the one the
// pinned aws-sdk-go-v2 client actually talks to, confirmed against
// deserializers.go: sqs is awsAwsjson10_, case-sensitive) and a legacy
// "query<Op>" XML/Query-protocol handler for the same op string, left over
// from before SQS's protocol switch. Every op-to-handler write now goes
// through pkgScan.bindOp, which detects a second, DIFFERENT handler name
// claiming an already-bound op and refuses to pick either: the op is pulled
// out of normal checking and reported as its own ERROR row (AmbiguousOps /
// AmbiguousHandlers in checkResult) naming every conflicting handler, rather
// than one silently winning by file-processing order. Before the fix, sqs's
// ~13 dually-bound ops (handler.go's sqsDispatchTable vs query.go's
// queryActionTable, e.g. DeleteMessageBatch) resolved to the XML handler
// only because query.go sorts after handler.go, producing 85 MISMATCH rows
// that were all comparing the wrong handler's fields against the JSON SDK's
// key set -- confirmed by hand not real; sqs's real JSON handlers
// (handleDeleteMessageBatch etc.) already write correctly.
// TestRunCheck_AmbiguousHandlerBinding reproduces this exact shape as a
// fixture and fails against the unfixed tool.
//
// KNOWN BLIND SPOT #3: a written key absent from the real reachable shape is
// reported identically whether it REPLACES a real required key (the real
// value is silently dropped on every client -- the gopherstack-6flj/zquj
// class this tool exists to catch) or sits ALONGSIDE all the real keys as a
// harmless extra the real client's typed struct has no field to receive.
// Telling these apart requires reading whether the handler also writes (or
// omits) the correspondingly-named real key -- confirmed both ways live in
// wafv2 (CheckCapacity's "ConsumedCapacity" replaced the real "Capacity" and
// dropped the value entirely; GetWebACLForResource's "LockToken" sits beside
// a correct response and is just ignored noise).
//
// KNOWN BLIND SPOT #7, found live during the gopherstack-zquj re-sweep,
// FIXED for op-naming, VERIFIED by a full sweep of all 70 restjson1
// services (not the 69 last estimated -- see the sweep note below): op
// resolution used to match the handler's dispatch-table KEY against the
// SDK's PascalCase operation name verbatim. Several restjson1 services key
// their dispatch table by REST path (or method+path) instead of the
// operation name -- account ("/acceptPrimaryEmailUpdate"), batch
// ("/v1/canceljob"), mgn ("DELETE tags"), xray ("/CancelTraceRetrieval") --
// so every op reported UnresolvedOps even when HandlerOpsResolved showed the
// dispatch table itself resolved fully (mgn: 95/95, resiliencehub: 63/63,
// xray: 38/38). recoverOpName/resolveOpNames now recover the real op name
// from the handler's OWN name (this repo's "handle<Op>"/"json<Op>"
// convention, already used elsewhere in this file to find the handler in
// the first place) whenever the raw dispatch key itself isn't found in the
// SDK's op index, and refuse to guess (report AmbiguousOps instead) if that
// recovery would make two differently-bound keys collide on the same real
// op name.
//
// SWEPT 2026-08-22 against all 70 restjson1 services (protocol verified
// per-service by reading each pinned deserializers.go directly, not assumed
// from services/_PROTOCOLS.md, though that doc's independent classification
// agreed): of the 13 services gopherstack-zquj's prior triage named as
// affected, 10 were CONFIRMED and newly resolve at least one op --
// account, apigatewayv2, appmesh, batch, bedrock, mgn, opensearch, pinpoint,
// resiliencehub, xray -- and 5 of those 10 (account, bedrock, mgn, pinpoint,
// resiliencehub) now resolve EVERY op their dispatch table binds, exiting
// clean. The other 3 named services (amplify, appsync, outposts) get ZERO
// benefit from this fix: their dispatch KEY is a bare resource path
// ("branches", "webhooks", "capacity") whose single bound HANDLER itself
// internally multiplexes several distinct real ops by HTTP method
// (handleBranches serves both ListBranches and CreateBranch) -- there is no
// single real op name to recover, so recoverOpName correctly finds nothing
// and (where two such multiplexing handlers collide on one dispatch key,
// amplify's actual shape) resolveOpNames's own ambiguity guard fires
// instead. That is a genuinely different, deeper gap (a dispatch KEY
// resolving to more than one real op, not a naming mismatch) and is not
// fixed here. opensearch separately mixes in an unrelated-SDK gap: its
// already-PascalCase dispatch keys (CreateCollection, CreateAccessPolicy,
// ...) belong to OpenSearch Serverless, a different SDK package than
// classic opensearch's pinned deserializers.go, so no op-name recovery
// changes them; only 1 of its 96 SDK ops (a REST-path-keyed classic-domain
// op) was actually fixed by this change.
//
// ENUM/TYPE-STRING DISPATCH TABLE MISREAD AS OP DISPATCH, found sweeping the
// 12-service PARTIAL tier for gopherstack-85e3, FIXED: a per-item
// classification switch or map keyed by an enum string that happens to look
// like an op name (apigateway's IntegrationType, glacier's job-type Action,
// lightsail's ResourceType, swf's DecisionType) got recorded as a real
// op-to-handler binding, then reported as a false "no deserializeOpDocument"
// ERROR once it failed SDK resolution -- 90%+ of the noise across those 12
// services. filterEnumGroups groups every candidate op by the single switch
// statement or map/slice literal that bound it (ps.opGroup) and reclassifies
// the WHOLE group as FILTERED, not unresolved, only when it has 2+ candidates
// and EVERY one failed resolution: a real dispatch table drawn from the same
// SDK/prefix almost always resolves at least one member, so batting 0-for-N
// is the corroborating signal, not a name pattern. A lone failing candidate
// (N=1) has no sibling to corroborate it and is never filtered -- that stays
// ordinary KNOWN BLIND SPOT #7 territory. Filtered ops are still printed in
// full (FILTERED: ...), never silently dropped.
//
// LAMBDA-TRIGGER-ENVELOPE POLLUTION, a further refinement of blind spot #2
// found sweeping cognitoidp for gopherstack-ck9f, FIXED: cognitoidp's auth
// ops (SignUp, InitiateAuth, RespondToAuthChallenge, ...) each reach a shared
// Lambda-trigger-invocation helper whose own envelope map (version,
// triggerSource, userName, callerContext, request, response, ...) and each
// caller's own request/response maps got attributed wholesale to the op
// being checked -- ~85% of cognitoidp's 304 pre-triage mismatches, plus a
// coincidental CASE-MISMATCH on lowercase envelope keys (userName,
// challengeName, session) that collide with the op's own correctly-cased
// struct fields written by an unrelated code path. isBoundaryCall recognizes
// two structural shapes, neither a name pattern: (a) a call that crosses an
// injected-dependency boundary -- a method invoked on a struct field whose
// declared type is a package-local interface (cognitoidp's
// b.lambdaInvoker.LambdaTriggerInvoker), marked transitively up the call
// graph (computeCrossesBoundary); (b) a call to a same-package function whose
// signature converts a map into a slice of some OTHER named type
// (computeMapConversionFuncs, cognitoidp's
// sortedAttributeList(map[string]string) []attributeType -- map KEYS become
// list-item Name VALUES, never JSON keys). A composite literal or variable
// passed to either is excluded from writtenKeys UNLESS it is independently
// part of what the enclosing function itself returns (returnedRoots), so a
// value that legitimately crosses the boundary AND is handed back as real
// output is never suppressed. computeBoundaryProducerFuncs extends the same
// idea one hop further for a helper (cognitoidp's userAttrsWithSub) that
// exists solely to build a map every one of its callers feeds into a
// conversion func: its OWN return-bound writes are excluded too, at their
// construction site.
//
// DETERMINISTIC-VS-GENUINE AMBIGUITY, a refinement of blind spot #6 found
// sweeping cognitoidp for gopherstack-ck9f, FIXED: cognitoidp keeps both a
// legacy handler and a hardened "Full"/"Accurate" variant for many ops, bound
// in separate OpsA/OpsB/OpsC family maps that dispatchTable() merges via
// SEQUENTIAL maps.Copy calls -- Go's maps.Copy overwrites on collision, so
// whichever family is copied LAST deterministically wins, unlike sqs's real
// ambiguity (two tables queried independently, never merged). Before this fix
// all 27 such ops were pulled into AmbiguousOps/ERROR and masked from
// checking entirely. resolveDeterministicOverrides finds, for a conflicting
// op, whether every candidate handler's own enclosing function
// (handlerSourceFunc) appears in the SAME maps.Copy chain to the SAME
// destination (findCopyChains) -- and only then resolves to the
// textually-last one, printing a DETERMINISTIC OVERRIDE line naming both
// sides so the choice stays independently verifiable against the assembler's
// own call order. An op whose conflicting handlers are never merged into a
// shared destination (sqs's shape) is left exactly as ambiguous as before.
//
// SHARED-ERROR-HELPER POLLUTION, a named instance of blind spot #2 shape
// (b), found live re-sweeping medialive and quicksight for gopherstack-v4a4
// (disclosed, not fixed -- same rationale as blind spot #1): both services
// route nearly every handler's error branch through one package-level
// helper (medialive's respondErr, quicksight's writeError) whose own
// map[string]any{"Message": ...}/{"Code": ..., "Message": ..., "Status": ...}
// literal gets attributed to every calling op's writtenKeys, producing a
// near-op-total MISMATCH set (medialive: 122/123 ops) that is entirely the
// error envelope, not the success response the SDK deserializer this tool
// diffs against ever covers. The tell: the SAME 1-3 keys recur, verbatim,
// across dozens of otherwise-unrelated ops. KEYCHECK_DEBUG_WALK confirms the
// call chain terminates in the shared helper every time.
//
// OUTPUT-SUFFIX NAME COLLISION WITH AN INTERNAL BACKEND CONTRACT, found live
// sweeping kinesis for gopherstack-v4a4 (disclosed, not fixed): blind spot
// #5's "*Output"-suffix heuristic assumes that name means "this literal is
// what gets marshaled to the wire" (this repo's overwhelming convention).
// kinesis instead names its StorageBackend-interface return values
// <Op>Output (models.go's DescribeStreamOutput, ListShardsOutput, ...) as a
// domain-modeling convention unrelated to marshaling -- the actual wire
// response is built separately, per op, from correctly-tagged jsonXxx
// structs (handler_shards.go's jsonShardDescription, handler_consumers.go's
// jsonConsumer/jsonConsumerDescription, both confirmed against the pinned
// SDK's deserializers.go by their own doc comments) that this scan's naming
// heuristic never looks at because they don't end in "Output". Every one of
// kinesis's CASE-MISMATCH/MISMATCH rows this session traced back to this
// gap, confirmed by hand against handler_shards.go/handler_consumers.go: the
// real wire structs were already correctly tagged throughout.
//
// Usage:
//
//	go run ./cmd/keycheck -sdk <path to deserializers.go> -prefix awsAwsjson11_ -svc <service dir> [-op OpName]
//
// Exit codes: 0 clean or N/A, 1 NOTHING in the service was verified (see
// FAIL-LOUD -- zero ops checked), 2 every dispatched op resolved and a real
// key mismatch was found, 3 SOME ops were resolved and checked (real
// MISMATCH data or a real clean result) but at least one other op remains
// unresolved or ambiguous -- a substantially-checked service, never to be
// conflated with exit 1's "nothing checked" (see VERDICT in the report,
// added because exit 1 alone let 13 substantially-checked services,
// cognitoidp alone 102 ops/304 mismatches, hide behind the same code as a
// service with zero dispatch resolved).
package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// ---------- SDK-side: real wire key extraction from deserializers.go ----------

type funcInfo struct {
	kind  string // "object", "list", "unknown"
	cases map[string]string
	elem  string
}

type sdkIndex struct {
	types    map[string]funcInfo // bare type name -> info
	ops      map[string]funcInfo // op name -> info (object kind only)
	emptyOps map[string]bool     // ops confirmed genuinely empty-output via wrapper inspection
}

func exprString(fset *token.FileSet, e ast.Expr) string {
	var sb strings.Builder
	if err := format.Node(&sb, fset, e); err != nil {
		return ""
	}

	return sb.String()
}

func parseSDK(path, prefix string) (*sdkIndex, error) {
	fset := token.NewFileSet()

	f, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		return nil, fmt.Errorf("parse sdk file: %w", err)
	}

	idx := &sdkIndex{types: map[string]funcInfo{}, ops: map[string]funcInfo{}, emptyOps: map[string]bool{}}

	docRe := regexp.MustCompile(`^` + regexp.QuoteMeta(prefix) + `deserializeDocument(.+)$`)
	opRe := regexp.MustCompile(`^` + regexp.QuoteMeta(prefix) + `deserializeOpDocument(.+)Output$`)

	for _, decl := range f.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if !ok || fd.Body == nil {
			continue
		}

		if m := opRe.FindStringSubmatch(fd.Name.Name); m != nil {
			idx.ops[m[1]] = analyzeFunc(fset, fd, docRe)

			continue
		}

		if m := docRe.FindStringSubmatch(fd.Name.Name); m != nil {
			idx.types[m[1]] = analyzeFunc(fset, fd, docRe)
		}
	}

	resolveEmptyOutputOps(f, idx, prefix, opRe)

	return idx, nil
}

// resolveEmptyOutputOps finds every <prefix>deserializeOp<Op> wrapper type
// (the per-operation HandleDeserialize struct every op gets, distinct from
// the Document/Error variants) whose HandleDeserialize body calls no
// deserializeOpDocument function at all -- the shape the SDK generates when
// an Output struct declares no members beyond ResultMetadata. Those ops are
// resolved as a genuinely empty allowed-key set rather than left unresolved.
func resolveEmptyOutputOps(f *ast.File, idx *sdkIndex, prefix string, opRe *regexp.Regexp) {
	wrapperRe := regexp.MustCompile(`^` + regexp.QuoteMeta(prefix) + `deserializeOp([A-Za-z0-9]+)$`)

	wrapperSeen := map[string]bool{}
	docCallSeen := map[string]bool{}

	for _, decl := range f.Decls {
		if gd, ok := decl.(*ast.GenDecl); ok && gd.Tok == token.TYPE {
			recordWrapperTypes(gd, wrapperRe, wrapperSeen)

			continue
		}

		fd, ok := decl.(*ast.FuncDecl)
		if !ok || fd.Recv == nil || fd.Name.Name != "HandleDeserialize" || fd.Body == nil {
			continue
		}

		if m := wrapperRe.FindStringSubmatch(recvTypeName(fd.Recv)); m != nil && containsCall(fd.Body, opRe) {
			docCallSeen[m[1]] = true
		}
	}

	for op := range wrapperSeen {
		if _, ok := idx.ops[op]; ok {
			continue
		}
		if !docCallSeen[op] {
			idx.ops[op] = funcInfo{kind: "object", cases: map[string]string{}}
			idx.emptyOps[op] = true
		}
	}
}

func recordWrapperTypes(gd *ast.GenDecl, wrapperRe *regexp.Regexp, seen map[string]bool) {
	for _, spec := range gd.Specs {
		ts, ok := spec.(*ast.TypeSpec)
		if !ok {
			continue
		}
		if _, isStruct := ts.Type.(*ast.StructType); !isStruct {
			continue
		}
		if m := wrapperRe.FindStringSubmatch(ts.Name.Name); m != nil {
			seen[m[1]] = true
		}
	}
}

func recvTypeName(recv *ast.FieldList) string {
	if recv == nil || len(recv.List) == 0 {
		return ""
	}
	t := recv.List[0].Type
	if se, ok := t.(*ast.StarExpr); ok {
		t = se.X
	}
	if id, ok := t.(*ast.Ident); ok {
		return id.Name
	}

	return ""
}

func containsCall(body *ast.BlockStmt, re *regexp.Regexp) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		id, ok := call.Fun.(*ast.Ident)
		if ok && re.MatchString(id.Name) {
			found = true

			return false
		}

		return true
	})

	return found
}

// analyzeFunc classifies one deserializer function by its input type
// assertion (map[string]interface{} => object with a case switch;
// []interface{} => list wrapping a single element type), then extracts
// either its case-list (object) or its element type (list).
func analyzeFunc(fset *token.FileSet, fd *ast.FuncDecl, callRe *regexp.Regexp) funcInfo {
	info := funcInfo{kind: "unknown", cases: map[string]string{}}

	var assertType string

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		ta, ok := n.(*ast.TypeAssertExpr)
		if !ok || assertType != "" {
			return true
		}
		assertType = exprString(fset, ta.Type)

		return true
	})

	switch {
	case strings.Contains(assertType, "map[string]interface{}"):
		info.kind = "object"
		info.cases = extractCases(fd.Body, callRe)
	case strings.Contains(assertType, "[]interface{}"):
		info.kind = "list"
		info.elem = extractFirstCall(fd.Body, callRe)
	}

	return info
}

// extractCases finds the switch-on-key statement and, for each case, the
// first nested deserializeDocument<Target> call in its body (empty target =
// inline/scalar leaf).
func extractCases(body *ast.BlockStmt, callRe *regexp.Regexp) map[string]string {
	cases := map[string]string{}

	ast.Inspect(body, func(n ast.Node) bool {
		sw, ok := n.(*ast.SwitchStmt)
		if !ok {
			return true
		}
		for _, stmt := range sw.Body.List {
			cc, isCase := stmt.(*ast.CaseClause)
			if !isCase {
				continue
			}
			target := extractFirstCallInStmts(cc.Body, callRe)
			for _, expr := range cc.List {
				lit, isLit := expr.(*ast.BasicLit)
				if !isLit || lit.Kind != token.STRING {
					continue
				}
				cases[trimQuotes(lit.Value)] = target
			}
		}

		return false // don't descend into nested switches (list-elem funcs are separate top-level funcs)
	})

	return cases
}

func extractFirstCall(body *ast.BlockStmt, callRe *regexp.Regexp) string {
	return extractFirstCallInStmts(body.List, callRe)
}

func extractFirstCallInStmts(stmts []ast.Stmt, callRe *regexp.Regexp) string {
	target := ""
	for _, stmt := range stmts {
		ast.Inspect(stmt, func(n ast.Node) bool {
			if target != "" {
				return false
			}
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			id, ok := call.Fun.(*ast.Ident)
			if !ok {
				return true
			}
			if m := callRe.FindStringSubmatch(id.Name); m != nil {
				target = m[1]

				return false
			}

			return true
		})
		if target != "" {
			break
		}
	}

	return target
}

func trimQuotes(s string) string { return strings.Trim(s, "\"`") }

// reachable returns every wire key reachable from info, transitively through
// nested object/list types, guarding against cycles and unbounded depth.
func reachable(idx *sdkIndex, info funcInfo, visited map[string]bool, depth int) map[string]bool {
	const maxDepth = 14
	if depth > maxDepth {
		return map[string]bool{}
	}

	if info.kind == "list" {
		if info.elem == "" || visited[info.elem] {
			return map[string]bool{}
		}
		visited[info.elem] = true
		sub, ok := idx.types[info.elem]
		if !ok {
			return map[string]bool{}
		}

		return reachable(idx, sub, visited, depth+1)
	}

	result := map[string]bool{}
	for key, target := range info.cases {
		result[key] = true
		if target == "" || visited[target] {
			continue
		}
		visited[target] = true
		sub, ok := idx.types[target]
		if !ok {
			continue
		}
		for k := range reachable(idx, sub, visited, depth+1) {
			result[k] = true
		}
	}

	return result
}

// ---------- handler-side: what the service actually writes ----------

type pkgScan struct {
	fset          *token.FileSet
	funcDecls     map[string]*ast.FuncDecl
	constVals     map[string]string
	funcTypeNames map[string]bool
	structTypes   map[string]*ast.StructType
	mapAnyVars    map[string]bool
	opToHandler   map[string]string
	ambiguousOps  map[string]map[string]bool

	// opGroup records, for each accepted (non-conflicting) op binding, an
	// identifier for the single switch statement or map/slice literal that
	// bound it -- see filterEnumGroups.
	opGroup map[string]string

	// interfaceTypes/interfaceFields/mapConversionFuncs/crossesBoundary and
	// handlerSourceFunc/copyChains back the two writtenKeys narrowings (see
	// isBoundaryCall, computeFuncBoundaryInfo) and the deterministic-override
	// resolution (see resolveDeterministicOverrides).
	interfaceTypes     map[string]bool
	interfaceFields    map[string]bool
	mapConversionFuncs map[string]bool
	crossesBoundary    map[string]bool
	handlerSourceFunc  map[string]string
	copyChains         map[string][]string
	boundaryInfoCache  map[string]funcBoundaryInfo

	// boundaryProducers holds every same-package, map-returning function
	// whose result is used, at EVERY call site in the package, only as a
	// boundary-call argument (isBoundaryCall) -- cognitoidp's
	// userAttrsWithSub(u) map[string]string, called solely to feed
	// sortedAttributeList. A producer's own return-bound map writes are then
	// excluded at their construction site, not just where they're consumed.
	boundaryProducers map[string]bool

	deterministicOverrideNotes []string
}

func scanPackage(dir string) (*pkgScan, error) {
	fset := token.NewFileSet()
	ps := &pkgScan{
		fset:               fset,
		funcDecls:          map[string]*ast.FuncDecl{},
		constVals:          map[string]string{},
		funcTypeNames:      map[string]bool{},
		structTypes:        map[string]*ast.StructType{},
		mapAnyVars:         map[string]bool{},
		opToHandler:        map[string]string{},
		ambiguousOps:       map[string]map[string]bool{},
		opGroup:            map[string]string{},
		interfaceTypes:     map[string]bool{},
		interfaceFields:    map[string]bool{},
		mapConversionFuncs: map[string]bool{},
		crossesBoundary:    map[string]bool{},
		handlerSourceFunc:  map[string]string{},
		copyChains:         map[string][]string{},
		boundaryInfoCache:  map[string]funcBoundaryInfo{},
		boundaryProducers:  map[string]bool{},
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var files []*ast.File
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, filepath.Join(dir, e.Name()), nil, 0)
		if perr != nil {
			return nil, fmt.Errorf("parse %s: %w", e.Name(), perr)
		}
		files = append(files, f)
	}

	for _, f := range files {
		ps.indexFile(f)
	}
	for _, f := range files {
		ps.findMapAnyVars(f)
	}
	ps.computeInterfaceFields()
	ps.computeMapConversionFuncs()
	ps.computeCrossesBoundary()
	ps.boundaryProducers = ps.computeBoundaryProducerFuncs()
	for _, f := range files {
		ps.findOpDispatch(f)
	}
	for _, f := range files {
		ps.findCopyChains(f)
	}
	ps.deterministicOverrideNotes = ps.resolveDeterministicOverrides()

	return ps, nil
}

func (ps *pkgScan) indexFile(f *ast.File) {
	for _, decl := range f.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			ps.funcDecls[d.Name.Name] = d
		case *ast.GenDecl:
			ps.indexConsts(d)
			ps.indexFuncTypes(d)
			ps.indexStructTypes(d)
			ps.indexInterfaceTypes(d)
		}
	}
}

// indexInterfaceTypes records every package-level `type X interface{...}`
// declaration, so computeInterfaceFields can recognize a struct field whose
// declared type crosses an injected-dependency boundary (e.g. cognitoidp's
// InMemoryBackend.lambdaInvoker LambdaTriggerInvoker) rather than being part
// of the op's own wire response.
func (ps *pkgScan) indexInterfaceTypes(d *ast.GenDecl) {
	if d.Tok != token.TYPE {
		return
	}
	for _, spec := range d.Specs {
		ts, ok := spec.(*ast.TypeSpec)
		if !ok {
			continue
		}
		if _, isIface := ts.Type.(*ast.InterfaceType); isIface {
			ps.interfaceTypes[ts.Name.Name] = true
		}
	}
}

// computeInterfaceFields records every struct field name (across every
// package-level struct) whose declared type is a package-level interface.
// Field names are tracked package-wide rather than per-struct: a call site
// only has the field's SELECTOR name available syntactically (b.lambdaInvoker),
// not its receiver's resolved type, so isBoundaryCall matches on name alone --
// narrow because it requires BOTH a field of that exact name to exist AND its
// declared type to resolve to a locally-declared interface.
func (ps *pkgScan) computeInterfaceFields() {
	for _, st := range ps.structTypes {
		for _, field := range st.Fields.List {
			id, ok := field.Type.(*ast.Ident)
			if !ok || !ps.interfaceTypes[id.Name] {
				continue
			}
			for _, name := range field.Names {
				ps.interfaceFields[name.Name] = true
			}
		}
	}
}

// computeMapConversionFuncs records every function taking a map[string]<T>
// parameter and returning a slice of some OTHER (non-map) named type --
// cognitoidp's sortedAttributeList(map[string]string) []attributeType shape,
// which turns map KEYS into attribute-Name VALUES rather than JSON keys. This
// is a direct signature match, not transitive: the map argument is excluded
// at the exact call site, regardless of which function contains it.
func (ps *pkgScan) computeMapConversionFuncs() {
	for name, fd := range ps.funcDecls {
		if fd.Type.Params == nil || fd.Type.Results == nil {
			continue
		}
		hasMapParam := false
		for _, p := range fd.Type.Params.List {
			if _, ok := p.Type.(*ast.MapType); ok {
				hasMapParam = true

				break
			}
		}
		if !hasMapParam {
			continue
		}
		for _, r := range fd.Type.Results.List {
			at, ok := r.Type.(*ast.ArrayType)
			if !ok || at.Len != nil {
				continue
			}
			if _, isMap := at.Elt.(*ast.MapType); isMap {
				continue
			}
			ps.mapConversionFuncs[name] = true
		}
	}
}

// computeCrossesBoundary marks every function that, directly or transitively
// through a same-package call, invokes a method on an interface-typed struct
// field (computeInterfaceFields) -- cognitoidp's
// b.lambdaInvoker.InvokeTrigger(...), the real Lambda-trigger invocation
// point. A fixed-point pass over the package's own call graph propagates the
// mark up through every caller, so a top-level op handler that merely calls
// into the trigger machinery a few hops deep is still recognized.
func (ps *pkgScan) computeCrossesBoundary() {
	changed := true
	for changed {
		changed = false
		for name, fd := range ps.funcDecls {
			if ps.crossesBoundary[name] || fd.Body == nil {
				continue
			}
			if ps.bodyCrossesBoundary(fd.Body) {
				ps.crossesBoundary[name] = true
				changed = true
			}
		}
	}
}

func (ps *pkgScan) bodyCrossesBoundary(body *ast.BlockStmt) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if recv, isSel := sel.X.(*ast.SelectorExpr); isSel && ps.interfaceFields[recv.Sel.Name] {
			found = true

			return false
		}
		if ps.crossesBoundary[sel.Sel.Name] {
			found = true

			return false
		}

		return true
	})

	return found
}

// isBoundaryCall reports whether call is either a direct interface-field
// method invocation or a call to a same-package function already known to
// cross that boundary (computeCrossesBoundary) or to convert a map's keys
// into non-key data (computeMapConversionFuncs).
func (ps *pkgScan) isBoundaryCall(call *ast.CallExpr) bool {
	switch fn := call.Fun.(type) {
	case *ast.SelectorExpr:
		if recv, ok := fn.X.(*ast.SelectorExpr); ok && ps.interfaceFields[recv.Sel.Name] {
			return true
		}

		return ps.crossesBoundary[fn.Sel.Name] || ps.mapConversionFuncs[fn.Sel.Name]
	case *ast.Ident:
		return ps.crossesBoundary[fn.Name] || ps.mapConversionFuncs[fn.Name]
	default:
		return false
	}
}

// funcBoundaryInfo is the per-function result of computeFuncBoundaryInfo,
// cached by function name since writtenKeys' BFS can revisit the same
// function from multiple ops.
type funcBoundaryInfo struct {
	excludedVars map[string]bool
	excludedLits map[*ast.CompositeLit]bool
}

func (ps *pkgScan) funcBoundaryInfo(fd *ast.FuncDecl) funcBoundaryInfo {
	if info, ok := ps.boundaryInfoCache[fd.Name.Name]; ok {
		return info
	}
	info := ps.computeFuncBoundaryInfo(fd)
	ps.boundaryInfoCache[fd.Name.Name] = info

	return info
}

// computeFuncBoundaryInfo finds every argument of a boundary call (isBoundaryCall)
// within fd -- a locally-declared *ast.CompositeLit passed inline, or a local
// variable passed by name -- and excludes it from writtenKeys UNLESS that same
// value is independently part of what fd itself returns (collectReturnRoots):
// a value that only ever flows INTO a Lambda-trigger invocation or an
// attribute map->list conversion is input to that machinery, not the op's own
// wire response, but a value fd also hands back untouched must never be
// excluded just because it was ALSO sent somewhere else along the way.
func (ps *pkgScan) computeFuncBoundaryInfo(fd *ast.FuncDecl) funcBoundaryInfo {
	info := funcBoundaryInfo{excludedVars: map[string]bool{}, excludedLits: map[*ast.CompositeLit]bool{}}
	if fd.Body == nil {
		return info
	}

	returned := returnedRoots(fd)
	ps.markBoundaryCallArgs(fd, returned, info)
	ps.markBoundaryProducerReturns(fd, returned, info)
	markExcludedVarLiterals(fd, info)

	return info
}

// markBoundaryCallArgs marks every argument of a boundary call (isBoundaryCall)
// within fd -- a locally-declared *ast.CompositeLit passed inline, or a local
// variable passed by name -- for exclusion, UNLESS that same value is
// independently part of what fd itself returns (returned): a value that only
// ever flows INTO a Lambda-trigger invocation or an attribute map->list
// conversion is input to that machinery, not the op's own wire response, but
// a value fd also hands back untouched must never be excluded just because it
// was ALSO sent somewhere else along the way.
func (ps *pkgScan) markBoundaryCallArgs(fd *ast.FuncDecl, returned map[string]bool, info funcBoundaryInfo) {
	ast.Inspect(fd.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || !ps.isBoundaryCall(call) {
			return true
		}
		for _, arg := range call.Args {
			switch a := arg.(type) {
			case *ast.CompositeLit:
				info.excludedLits[a] = true
			case *ast.Ident:
				if !returned[a.Name] {
					info.excludedVars[a.Name] = true
				}
			}
		}

		return true
	})
}

// markBoundaryProducerReturns handles a boundary-producer function
// (computeBoundaryProducerFuncs), which exists solely to build the map its
// OWN callers feed into a boundary call -- its return-bound variable(s) are
// excluded here too, at the point they are actually written (e.g.
// cognitoidp's userAttrsWithSub's attrs["sub"] = ...), not just where a
// caller later consumes them.
func (ps *pkgScan) markBoundaryProducerReturns(fd *ast.FuncDecl, returned map[string]bool, info funcBoundaryInfo) {
	if !ps.boundaryProducers[fd.Name.Name] {
		return
	}
	for root := range returned {
		info.excludedVars[root] = true
	}
}

// markExcludedVarLiterals extends an excluded variable's exclusion back to
// the composite literal that defines it (`event := map[string]any{...}`), so
// the envelope's own keys are excluded at their construction site too, not
// just at the call site that consumes the variable.
func markExcludedVarLiterals(fd *ast.FuncDecl, info funcBoundaryInfo) {
	ast.Inspect(fd.Body, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || len(as.Lhs) != len(as.Rhs) {
			return true
		}
		for i, lhs := range as.Lhs {
			id, isIdent := lhs.(*ast.Ident)
			if !isIdent || !info.excludedVars[id.Name] {
				continue
			}
			if cl, isLit := as.Rhs[i].(*ast.CompositeLit); isLit {
				info.excludedLits[cl] = true
			}
		}

		return true
	})
}

// computeBoundaryProducerFuncs finds every same-package function returning a
// map type whose result is used, at EVERY call site in the package, only as
// a boundary-call argument (directly inline, or via a variable that never
// escapes to a return) -- cognitoidp's userAttrsWithSub(u) map[string]string,
// called solely to feed sortedAttributeList(attrs). Computed once, before any
// op is checked, using each caller's own freshly-computed (not cached)
// boundary info, so this determination can never be contaminated by a
// producer flag it is itself in the middle of deciding.
func (ps *pkgScan) computeBoundaryProducerFuncs() map[string]bool {
	callerOf, callSites := ps.findCallSites()

	producers := map[string]bool{}
	for name, fd := range ps.funcDecls {
		if !returnsMapType(fd) || len(callSites[name]) == 0 {
			continue
		}
		if ps.allCallsAreBoundaryOnly(callSites[name], callerOf) {
			producers[name] = true
		}
	}

	return producers
}

// findCallSites indexes every CallExpr in the package by its callee name and
// its enclosing FuncDecl, so computeBoundaryProducerFuncs can inspect every
// call site of a candidate producer function.
func (ps *pkgScan) findCallSites() (map[*ast.CallExpr]*ast.FuncDecl, map[string][]*ast.CallExpr) {
	callerOf := map[*ast.CallExpr]*ast.FuncDecl{}
	callSites := map[string][]*ast.CallExpr{}
	for _, fd := range ps.funcDecls {
		if fd.Body == nil {
			continue
		}
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			name := calleeName(call.Fun)
			if name == "" {
				return true
			}
			callSites[name] = append(callSites[name], call)
			callerOf[call] = fd

			return true
		})
	}

	return callerOf, callSites
}

func (ps *pkgScan) allCallsAreBoundaryOnly(calls []*ast.CallExpr, callerOf map[*ast.CallExpr]*ast.FuncDecl) bool {
	for _, call := range calls {
		if !ps.isCallResultBoundaryOnly(call, callerOf[call]) {
			return false
		}
	}

	return true
}

// isCallResultBoundaryOnly reports whether target's result, within callerFd,
// is used only as a direct boundary-call argument or assigned to a local
// variable that callerFd's own boundary info already proves never escapes to
// a return.
func (ps *pkgScan) isCallResultBoundaryOnly(target *ast.CallExpr, callerFd *ast.FuncDecl) bool {
	if ps.isInlineBoundaryArg(target, callerFd) {
		return true
	}

	assignedVar := assignedVarName(target, callerFd)
	if assignedVar == "" {
		return false
	}

	info := ps.computeFuncBoundaryInfo(callerFd)

	return info.excludedVars[assignedVar]
}

// isInlineBoundaryArg reports whether target appears directly as an argument
// of a boundary call within callerFd.
func (ps *pkgScan) isInlineBoundaryArg(target *ast.CallExpr, callerFd *ast.FuncDecl) bool {
	found := false
	ast.Inspect(callerFd.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || !ps.isBoundaryCall(call) {
			return true
		}
		for _, arg := range call.Args {
			if arg == target {
				found = true
			}
		}

		return true
	})

	return found
}

// assignedVarName returns the name of the local variable target's result is
// assigned to within callerFd, or "" if it is never assigned to a bare
// identifier.
func assignedVarName(target *ast.CallExpr, callerFd *ast.FuncDecl) string {
	name := ""
	ast.Inspect(callerFd.Body, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for i, rhs := range as.Rhs {
			if rhs != target || i >= len(as.Lhs) {
				continue
			}
			if id, isIdent := as.Lhs[i].(*ast.Ident); isIdent {
				name = id.Name
			}
		}

		return true
	})

	return name
}

// returnsMapType reports whether fd declares at least one map-typed return
// value.
func returnsMapType(fd *ast.FuncDecl) bool {
	if fd.Type.Results == nil {
		return false
	}
	for _, r := range fd.Type.Results.List {
		if _, ok := r.Type.(*ast.MapType); ok {
			return true
		}
	}

	return false
}

// rootIdent returns the base identifier of a (possibly wrapped) expression --
// x for x, x.Field, x[i], &x, (x) -- so a value handed back via a struct
// field or index assignment can still be traced to the variable that holds
// it.
func rootIdent(e ast.Expr) string {
	switch v := e.(type) {
	case *ast.Ident:
		return v.Name
	case *ast.SelectorExpr:
		return rootIdent(v.X)
	case *ast.IndexExpr:
		return rootIdent(v.X)
	case *ast.StarExpr:
		return rootIdent(v.X)
	case *ast.UnaryExpr:
		return rootIdent(v.X)
	case *ast.ParenExpr:
		return rootIdent(v.X)
	default:
		return ""
	}
}

// returnedRoots collects every root identifier reachable from fd's own return
// statements, descending into a returned call's own arguments too (e.g.
// `return c.JSON(200, resp)`) so a value handed to a real response-writing
// call is not mistaken for one that only ever flows into a boundary call.
func returnedRoots(fd *ast.FuncDecl) map[string]bool {
	roots := map[string]bool{}
	if fd.Body == nil {
		return roots
	}
	ast.Inspect(fd.Body, func(n ast.Node) bool {
		ret, ok := n.(*ast.ReturnStmt)
		if !ok {
			return true
		}
		for _, res := range ret.Results {
			collectReturnRoots(res, roots)
		}

		return true
	})

	return roots
}

func collectReturnRoots(e ast.Expr, roots map[string]bool) {
	if r := rootIdent(e); r != "" {
		roots[r] = true
	}
	if call, ok := e.(*ast.CallExpr); ok {
		for _, arg := range call.Args {
			collectReturnRoots(arg, roots)
		}
	}
}

// indexFuncTypes records every package-level `type X func(...)` declaration
// (e.g. comprehend's "operation", ssm's "ssmActionFn"), so recordMapDispatch
// can tell a real op-dispatch table (map[string]<func type>) apart from an
// unrelated map literal before it loosens its handler-selector matching.
func (ps *pkgScan) indexFuncTypes(d *ast.GenDecl) {
	if d.Tok != token.TYPE {
		return
	}
	for _, spec := range d.Specs {
		ts, ok := spec.(*ast.TypeSpec)
		if !ok {
			continue
		}
		if _, isFunc := ts.Type.(*ast.FuncType); isFunc {
			ps.funcTypeNames[ts.Name.Name] = true
		}
	}
}

// indexStructTypes records every package-level `type X struct{...}`
// declaration so collectStructTagKeys can resolve a composite literal's json
// tags, and structTagFields can recurse into a field's own locally-declared
// struct type.
func (ps *pkgScan) indexStructTypes(d *ast.GenDecl) {
	if d.Tok != token.TYPE {
		return
	}
	for _, spec := range d.Specs {
		ts, ok := spec.(*ast.TypeSpec)
		if !ok {
			continue
		}
		if st, isStruct := ts.Type.(*ast.StructType); isStruct {
			ps.structTypes[ts.Name.Name] = st
		}
	}
}

func (ps *pkgScan) indexConsts(d *ast.GenDecl) {
	if d.Tok != token.CONST {
		return
	}
	for _, spec := range d.Specs {
		vs, ok := spec.(*ast.ValueSpec)
		if !ok {
			continue
		}
		for i, name := range vs.Names {
			if i >= len(vs.Values) {
				continue
			}
			lit, isLit := vs.Values[i].(*ast.BasicLit)
			if isLit && lit.Kind == token.STRING {
				ps.constVals[name.Name] = trimQuotes(lit.Value)
			}
		}
	}
}

// isMapAnyType reports whether t is any string-keyed map type
// (map[string]any, map[string]interface{}, map[string]string,
// map[string][]string, ...). The unchecked-key exposure this scanner hunts
// applies to every one of them equally -- a hand-written key is just as
// invisible to the compiler in a map[string]string literal as in
// map[string]any.
func isMapAnyType(t ast.Expr) bool {
	mt, ok := t.(*ast.MapType)
	if !ok {
		return false
	}
	keyID, ok := mt.Key.(*ast.Ident)
	if !ok || keyID.Name != "string" {
		return false
	}
	if st, isStruct := mt.Value.(*ast.StructType); isStruct && len(st.Fields.List) == 0 {
		return false // map[string]struct{} -- a set, not a wire-output map
	}
	if vid, isIdent := mt.Value.(*ast.Ident); isIdent && vid.Name == "bool" {
		return false // map[string]bool -- almost always a set/membership check, not wire output
	}

	return true
}

func (ps *pkgScan) findMapAnyVars(f *ast.File) {
	ast.Inspect(f, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.ValueSpec:
			ps.recordMapAnyValueSpec(v)
		case *ast.AssignStmt:
			ps.recordMapAnyAssign(v)
		}

		return true
	})
}

func (ps *pkgScan) recordMapAnyValueSpec(v *ast.ValueSpec) {
	if v.Type == nil || !isMapAnyType(v.Type) {
		return
	}
	for _, name := range v.Names {
		ps.mapAnyVars[name.Name] = true
	}
}

func (ps *pkgScan) recordMapAnyAssign(v *ast.AssignStmt) {
	for i, rhs := range v.Rhs {
		cl, isLit := rhs.(*ast.CompositeLit)
		if !isLit || !isMapAnyType(cl.Type) || i >= len(v.Lhs) {
			continue
		}
		if id, isIdent := v.Lhs[i].(*ast.Ident); isIdent {
			ps.mapAnyVars[id.Name] = true
		}
	}
}

// handleNameRe matches this repo's two observed per-op handler naming
// conventions: "handle<Op>" (the majority, e.g. shield/ssoadmin) and
// "json<Op>" (e.g. acm/acmpca). It deliberately does NOT match "dispatch"/
// "route" (intermediate multi-level dispatchers, which ast.Inspect already
// finds independently via their own nested case clauses) or "apply"
// (apigateway's internal JSON-Patch-op appliers, unrelated to op dispatch).
var handleNameRe = regexp.MustCompile(`^(handle|json)[A-Z]`)

// findOpDispatch walks each top-level declaration individually (rather than
// the whole file in one ast.Inspect) so every dispatch binding it finds can
// be tagged with its ENCLOSING function name (empty for a package-level var)
// -- handlerSourceFunc, used by resolveDeterministicOverrides to tell a real
// maps.Copy-merged op family from an unrelated table that happens to share a
// key.
func (ps *pkgScan) findOpDispatch(f *ast.File) {
	for _, decl := range f.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			if d.Body != nil {
				ps.findOpDispatchIn(d.Body, d.Name.Name)
			}
		case *ast.GenDecl:
			if d.Tok != token.VAR {
				continue
			}
			for _, spec := range d.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for _, val := range vs.Values {
					ps.findOpDispatchIn(val, "")
				}
			}
		}
	}
}

func (ps *pkgScan) findOpDispatchIn(n ast.Node, enclosingFunc string) {
	ast.Inspect(n, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.SwitchStmt:
			ps.recordSwitchDispatch(v, enclosingFunc)
		case *ast.CompositeLit:
			ps.recordMapDispatch(v, enclosingFunc)
			ps.recordSliceBindingDispatch(v, enclosingFunc)
		}

		return true
	})
}

// recordSwitchDispatch groups every case clause of ONE switch statement under
// a single group ID (see filterEnumGroups) -- glacier's `switch j.Action` and
// apigateway's `switch integration.Type` each bind several candidate "ops"
// from one switch, all of which turn out to be an enum/type-string table, not
// op dispatch.
func (ps *pkgScan) recordSwitchDispatch(sw *ast.SwitchStmt, enclosingFunc string) {
	groupID := "switch@" + ps.fset.Position(sw.Pos()).String()
	for _, stmt := range sw.Body.List {
		cc, ok := stmt.(*ast.CaseClause)
		if !ok {
			continue
		}
		ps.recordCaseDispatch(cc, groupID, enclosingFunc)
	}
}

func (ps *pkgScan) recordCaseDispatch(cc *ast.CaseClause, groupID, enclosingFunc string) {
	var opNames []string
	for _, expr := range cc.List {
		if op, dyn := ps.resolveKey(expr); !dyn && op != "" {
			opNames = append(opNames, op)
		}
	}
	if len(opNames) == 0 {
		return
	}

	handler := ps.findHandlerCall(cc.Body)
	if handler == "" {
		return
	}
	ps.handlerSourceFunc[handler] = enclosingFunc
	for _, op := range opNames {
		ps.bindOp(op, handler, groupID)
	}
}

// bindOp records handler as op's dispatch target. If op is already bound to
// a DIFFERENT handler name (sqs's real shape, gopherstack-kiwf: a modern
// "handle<Op>" JSON handler and a legacy "query<Op>" XML handler both claim
// the same op string), neither silently overwrites the other by
// file-processing order. Both names are recorded in ambiguousOps so runCheck
// can refuse to guess and report the conflict instead of comparing the
// wrong handler's keys against the SDK -- unless resolveDeterministicOverrides
// later proves the conflict is a known, visible, deterministic one instead of
// true ambiguity.
func (ps *pkgScan) bindOp(op, handler, groupID string) {
	if handler == "" {
		return
	}
	existing, bound := ps.opToHandler[op]
	if !bound {
		ps.opToHandler[op] = handler
		ps.opGroup[op] = groupID

		return
	}
	if existing == handler {
		return
	}
	if ps.ambiguousOps[op] == nil {
		ps.ambiguousOps[op] = map[string]bool{existing: true}
	}
	ps.ambiguousOps[op][handler] = true
}

func (ps *pkgScan) findHandlerCall(stmts []ast.Stmt) string {
	handler := ""
	for _, stmt := range stmts {
		ast.Inspect(stmt, func(n ast.Node) bool {
			if handler != "" {
				return false
			}
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if ok && handleNameRe.MatchString(sel.Sel.Name) {
				handler = sel.Sel.Name

				return false
			}

			return true
		})
		if handler != "" {
			break
		}
	}

	return handler
}

// recordMapDispatch handles this repo's dispatch-table conventions keyed by
// a string literal (map[string]T{"CreateFoo": ...}) or a package-level const
// identifier (map[string]T{opCreateFoo: ...}, e.g. dms's op families,
// resolved through ps.constVals, already populated by indexFile before
// findOpDispatch runs). The handler-selector match tries findHandlerSelector
// (the handle/json-prefix convention) first; only when that fails AND the
// literal's own map value type is a locally-declared function type
// (mapValueIsFuncType) does it fall back to findHandlerSelectorLoose, which
// covers comprehend/personalize/translate's bare lowercase method values and
// ssm's jsonOp(h.Backend.X)-wrapped and closure-wrapped backend calls. The
// func-type gate keeps the loosened match from firing on some unrelated
// map[string]<non-func> literal that happens to reference a local function.
func (ps *pkgScan) recordMapDispatch(cl *ast.CompositeLit, enclosingFunc string) {
	loose := ps.mapValueIsFuncType(cl.Type)
	groupID := "map@" + ps.fset.Position(cl.Pos()).String()
	for _, elt := range cl.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, dyn := ps.resolveKey(kv.Key)
		if dyn || key == "" {
			continue
		}
		name := findHandlerSelector(kv.Value)
		if name == "" && loose {
			name = ps.findHandlerSelectorLoose(kv.Value)
		}
		if name == "" {
			continue
		}
		ps.handlerSourceFunc[name] = enclosingFunc
		ps.bindOp(key, name, groupID)
	}
}

// mapValueIsFuncType reports whether t is a map[string]<F> type where F is
// either an inline func type or a package-level function type indexed by
// indexFuncTypes.
func (ps *pkgScan) mapValueIsFuncType(t ast.Expr) bool {
	mt, ok := t.(*ast.MapType)
	if !ok {
		return false
	}
	switch v := mt.Value.(type) {
	case *ast.FuncType:
		return true
	case *ast.Ident:
		return ps.funcTypeNames[v.Name]
	default:
		return false
	}
}

// findHandlerSelectorLoose resolves a dispatch-table value to a handler name
// for shapes findHandlerSelector's handle/json-prefix rule can't match: a
// bare lowercase method value (comprehend's h.detectSentiment,
// personalize/translate's h.createDatasetGroup) or a real backend method
// wrapped in a single-argument helper call (ssm's
// jsonOp(h.Backend.PutParameter)) or a closure that decodes the body and
// calls the real backend method in its return statement (ssm's
// AddTagsToResource/RemoveTagsFromResource). It requires the resolved name
// to be a function actually DECLARED in this package (ps.funcDecls) -- that
// is what keeps it from grabbing an unrelated call such as the
// json.Unmarshal decode step every ssm closure calls before its real
// backend call: encoding/json's Unmarshal has no local FuncDecl, so it is
// never a candidate.
func (ps *pkgScan) findHandlerSelectorLoose(e ast.Expr) string {
	switch v := e.(type) {
	case *ast.SelectorExpr:
		if _, ok := ps.funcDecls[v.Sel.Name]; ok {
			return v.Sel.Name
		}
	case *ast.CallExpr:
		for _, arg := range v.Args {
			if name := ps.findHandlerSelectorLoose(arg); name != "" {
				return name
			}
		}
	case *ast.FuncLit:
		return ps.findLocalCallInReturns(v.Body)
	}

	return ""
}

// findLocalCallInReturns looks only inside return statements (nested ones
// included) for a call to a locally-declared function -- scoped to returns,
// not the whole body, so it finds ssm's real
// "return struct{}{}, h.Backend.AddTagsToResource(ctx, &input)" call and not
// the json.Unmarshal decode step that precedes it in the same closure.
func (ps *pkgScan) findLocalCallInReturns(body *ast.BlockStmt) string {
	name := ""
	ast.Inspect(body, func(n ast.Node) bool {
		if name != "" {
			return false
		}
		ret, ok := n.(*ast.ReturnStmt)
		if !ok {
			return true
		}
		for _, res := range ret.Results {
			call, isCall := res.(*ast.CallExpr)
			if !isCall {
				continue
			}
			sel, isSel := call.Fun.(*ast.SelectorExpr)
			if !isSel {
				continue
			}
			if _, isLocal := ps.funcDecls[sel.Sel.Name]; isLocal {
				name = sel.Sel.Name

				break
			}
		}

		return false
	})

	return name
}

// recordSliceBindingDispatch handles glue's ordered-binding-slice dispatch
// convention (handler_routing.go): a package-level
// []struct{ bind func(*Handler) service.JSONOpFunc; name string }{...}
// literal, iterated in buildOps() rather than a map[string]X{...} literal.
// Gated on the slice's element type being an inline anonymous struct type,
// so this cannot misattribute an unrelated named-struct slice elsewhere in
// the package. The handler selector is resolved with the ordinary strict
// findHandlerSelector (glue's binding funcs already use the
// service.WrapOp(h.handleX) shape 36 other services use), so this convention
// needs only the new slice-shape recognition, not a matching loosening.
func (ps *pkgScan) recordSliceBindingDispatch(cl *ast.CompositeLit, enclosingFunc string) {
	at, isArray := cl.Type.(*ast.ArrayType)
	if !isArray || at.Len != nil {
		return
	}
	if _, isStruct := at.Elt.(*ast.StructType); !isStruct {
		return
	}
	groupID := "slice@" + ps.fset.Position(cl.Pos()).String()
	for _, elt := range cl.Elts {
		ecl, isLit := elt.(*ast.CompositeLit)
		if !isLit {
			continue
		}
		name := ps.structFieldString(ecl)
		if name == "" {
			continue
		}
		handler := findHandlerSelector(ecl)
		if handler == "" {
			continue
		}
		ps.handlerSourceFunc[handler] = enclosingFunc
		ps.bindOp(name, handler, groupID)
	}
}

// structFieldString returns the first field value in a keyed struct literal
// that resolves to a string literal or package-level string const (glue's
// binding struct's "name" field) without hardcoding that field's name.
func (ps *pkgScan) structFieldString(cl *ast.CompositeLit) string {
	for _, elt := range cl.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		if s, dyn := ps.resolveKey(kv.Value); !dyn && s != "" {
			return s
		}
	}

	return ""
}

// findHandlerSelector returns the first handler-shaped method name (see
// handleNameRe) reachable from e -- a bare selector (h.handleX) or one
// wrapped in a helper call (service.WrapOp(h.handleX), a common convention
// in this repo's op-dispatch tables).
func findHandlerSelector(e ast.Expr) string {
	name := ""
	ast.Inspect(e, func(n ast.Node) bool {
		if name != "" {
			return false
		}
		sel, ok := n.(*ast.SelectorExpr)
		if ok && handleNameRe.MatchString(sel.Sel.Name) {
			name = sel.Sel.Name

			return false
		}

		return true
	})

	return name
}

// isErrorEnvelopeKey reports whether k is one of the awsjson1.1/restjson1
// protocol-reserved error-envelope members ("__type", "message"): every op's
// error path writes these via a shared writeError-style helper reachable
// from nearly every handler, but they are never members of a successful
// Output shape, so a BFS that can't distinguish the error path from the
// success path must not count them as candidate wire-output keys for the
// op's OWN response type.
func isErrorEnvelopeKey(k string) bool { return k == "__type" || k == "message" }

// writtenKeys does a bounded BFS from the op's handler func over same-package
// calls, collecting every string key written into a map[string]<T> anywhere
// in the reachable body: composite-literal keys and X["key"]=... assignments
// where X is a known map[string]<T> variable.
func (ps *pkgScan) writtenKeys(opHandler string) (map[string]bool, int, []string) {
	keys := map[string]bool{}
	visited := map[string]bool{}
	queue := []string{opHandler}
	dynamicSkipped := 0
	var funcsWalked []string

	const maxFuncs = 200

	for len(queue) > 0 && len(funcsWalked) < maxFuncs {
		name := queue[0]
		queue = queue[1:]
		if visited[name] {
			continue
		}
		visited[name] = true
		fd, ok := ps.funcDecls[name]
		if !ok || fd.Body == nil {
			continue
		}
		funcsWalked = append(funcsWalked, name)

		var dyn int
		dyn, queue = ps.walkFuncBody(fd, keys, visited, queue)
		dynamicSkipped += dyn
	}

	return keys, dynamicSkipped, funcsWalked
}

func (ps *pkgScan) walkFuncBody(
	fd *ast.FuncDecl, keys map[string]bool, visited map[string]bool, queue []string,
) (int, []string) {
	dynamicSkipped := 0
	boundary := ps.funcBoundaryInfo(fd)

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.CompositeLit:
			if boundary.excludedLits[v] {
				// Don't descend: a nested map value inside an excluded
				// envelope literal (e.g. the trigger event's own
				// "callerContext": map[string]any{...}) is part of the SAME
				// excluded envelope, not independent wire-output data.
				return false
			}
			dynamicSkipped += ps.collectLitKeys(v, keys)
			ps.collectStructTagKeys(v, keys)
		case *ast.AssignStmt:
			dynamicSkipped += ps.collectIndexAssignKeys(v, keys, boundary.excludedVars)
		case *ast.CallExpr:
			name := calleeName(v.Fun)
			if name == "" {
				return true
			}
			if _, ok := ps.funcDecls[name]; ok && !visited[name] {
				queue = append(queue, name)
			}
		}

		return true
	})

	return dynamicSkipped, queue
}

func (ps *pkgScan) collectLitKeys(v *ast.CompositeLit, keys map[string]bool) int {
	if !isMapAnyType(v.Type) {
		return 0
	}
	dynamicSkipped := 0
	for _, elt := range v.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		k, dyn := ps.resolveKey(kv.Key)
		if dyn {
			dynamicSkipped++
			debugDynamic(ps.fset, kv.Pos(), "key")

			continue
		}
		if k != "" && !isErrorEnvelopeKey(k) {
			keys[k] = true
		}
	}

	return dynamicSkipped
}

// outputStructRe scopes struct-tag detection to this repo's overwhelming
// response-type naming convention (542 non-test `type XOutput struct`
// occurrences) so the same-call-graph BFS (blind spot #2) doesn't pull in an
// unrelated locally-declared struct's tags.
var outputStructRe = regexp.MustCompile(`Output$`)

// collectStructTagKeys is the struct-tag analogue of collectLitKeys: where
// that catches a map[string]<T> wire key typo, this catches a json struct
// tag typo on the *Output-suffixed struct type the handler actually
// constructs (gopherstack-v4a4, glue's querySchemaVersionMetadataOutput
// tagged json:"MetadataInfo" instead of the real "MetadataInfoMap",
// commit c3aa73e59). It has no dynamic-key concept -- a struct tag is
// always a compile-time literal -- so unlike collectLitKeys it returns
// nothing to add to dynamicSkipped.
func (ps *pkgScan) collectStructTagKeys(v *ast.CompositeLit, keys map[string]bool) {
	id, isIdent := v.Type.(*ast.Ident)
	if !isIdent || !outputStructRe.MatchString(id.Name) {
		return
	}
	if _, known := ps.structTypes[id.Name]; !known {
		return
	}
	for k := range ps.structTagFields(id.Name, map[string]bool{}, 0) {
		if !isErrorEnvelopeKey(k) {
			keys[k] = true
		}
	}
}

// structTagFields returns every wire key reachable from typeName's own
// json-tagged fields, recursing into any field whose type (after unwrapping
// a pointer, slice, or map value) resolves to another locally-declared
// struct type -- the handler-side mirror of reachable() on the SDK side. An
// embedded field contributes no key of its own (Go flattens it into the
// parent object) but is still recursed into. A field with no json tag falls
// back to its Go field name, matching encoding/json's real default.
func (ps *pkgScan) structTagFields(typeName string, visited map[string]bool, depth int) map[string]bool {
	const maxDepth = 14

	keys := map[string]bool{}
	if depth > maxDepth || visited[typeName] {
		return keys
	}
	visited[typeName] = true

	st, ok := ps.structTypes[typeName]
	if !ok {
		return keys
	}

	for _, field := range st.Fields.List {
		// encoding/json never marshals an unexported field, tagged or not --
		// a lowercase Go field name (e.g. batch's deliberately-unexported
		// ComputeEnvironment.region, kept off the wire by construction) must
		// never be treated as a written key, or as something worth recursing
		// into for further nested keys either.
		if len(field.Names) > 0 && !field.Names[0].IsExported() {
			continue
		}

		key, skip := jsonTagKey(field)
		if skip {
			continue
		}
		if len(field.Names) > 0 {
			if key == "" {
				key = field.Names[0].Name
			}
			keys[key] = true
		}
		if nested := localStructName(field.Type); nested != "" {
			for k := range ps.structTagFields(nested, visited, depth+1) {
				keys[k] = true
			}
		}
	}

	return keys
}

// jsonTagKey extracts a struct field's json tag key. skip is true for an
// explicit json:"-" (the field never marshals). An empty key with skip false
// means no json tag was present at all -- the caller falls back to the Go
// field name.
func jsonTagKey(field *ast.Field) (string, bool) {
	if field.Tag == nil {
		return "", false
	}
	raw, err := strconv.Unquote(field.Tag.Value)
	if err != nil {
		return "", false
	}
	tag := reflect.StructTag(raw).Get("json")
	if tag == "" {
		return "", false
	}
	name, _, _ := strings.Cut(tag, ",")
	if name == "-" {
		return "", true
	}

	return name, false
}

// localStructName unwraps a pointer, slice, or map-value type expression
// down to a bare identifier, so a field like `Foo *Bar`, `Foo []Bar`, or
// `Foo map[string]Bar` all resolve to "Bar" for the recursive tag walk. It
// does not verify Bar is actually a locally-declared struct -- the caller's
// ps.structTypes lookup does that, returning an empty map for anything else
// (a builtin, an imported type, or a plain scalar) with no infinite-loop risk.
func localStructName(t ast.Expr) string {
	switch v := t.(type) {
	case *ast.Ident:
		return v.Name
	case *ast.StarExpr:
		return localStructName(v.X)
	case *ast.ArrayType:
		return localStructName(v.Elt)
	case *ast.MapType:
		return localStructName(v.Value)
	default:
		return ""
	}
}

func (ps *pkgScan) collectIndexAssignKeys(v *ast.AssignStmt, keys map[string]bool, excludedVars map[string]bool) int {
	dynamicSkipped := 0
	for _, lhs := range v.Lhs {
		idx, ok := lhs.(*ast.IndexExpr)
		if !ok {
			continue
		}
		id, ok := idx.X.(*ast.Ident)
		if !ok || !ps.mapAnyVars[id.Name] || excludedVars[id.Name] {
			continue
		}
		k, dyn := ps.resolveKey(idx.Index)
		if dyn {
			dynamicSkipped++
			debugDynamic(ps.fset, idx.Pos(), "index")

			continue
		}
		if k != "" && !isErrorEnvelopeKey(k) {
			keys[k] = true
		}
	}

	return dynamicSkipped
}

func debugDynamic(fset *token.FileSet, pos token.Pos, kind string) {
	if os.Getenv("KEYCHECK_DEBUG_DYN") != "" {
		fmt.Fprintln(os.Stderr, "dynamic "+kind+" at", fset.Position(pos))
	}
}

func calleeName(fun ast.Expr) string {
	switch f := fun.(type) {
	case *ast.Ident:
		return f.Name
	case *ast.SelectorExpr:
		return f.Sel.Name
	default:
		return ""
	}
}

func (ps *pkgScan) resolveKey(e ast.Expr) (string, bool) {
	switch v := e.(type) {
	case *ast.BasicLit:
		if v.Kind == token.STRING {
			return trimQuotes(v.Value), false
		}
	case *ast.Ident:
		if s, ok := ps.constVals[v.Name]; ok {
			return s, false
		}

		return "", true
	}

	return "", true
}

// findCopyChains records every maps.Copy(dst, fn()) call it finds, grouped by
// "<enclosing func>|<dst variable>", in the exact textual order those calls
// appear -- the same order Go's maps.Copy applies them at runtime, so
// whichever family's map is copied LAST is the one that actually wins a
// same-key collision. This is cognitoidp's real dispatchTable() shape: ~30
// sequential maps.Copy calls merging OpsA/OpsB/OpsC family tables into one
// destination.
func (ps *pkgScan) findCopyChains(f *ast.File) {
	for _, decl := range f.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if !ok || fd.Body == nil {
			continue
		}
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			if call, isCall := n.(*ast.CallExpr); isCall {
				ps.recordCopyChainCall(fd.Name.Name, call)
			}

			return true
		})
	}
}

func (ps *pkgScan) recordCopyChainCall(assembler string, call *ast.CallExpr) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Copy" || len(call.Args) != 2 {
		return
	}
	pkgID, ok := sel.X.(*ast.Ident)
	if !ok || pkgID.Name != "maps" {
		return
	}
	srcCall, ok := call.Args[1].(*ast.CallExpr)
	if !ok {
		return
	}
	callee := calleeName(srcCall.Fun)
	if callee == "" {
		return
	}
	key := assembler + "|" + exprString(ps.fset, call.Args[0])
	ps.copyChains[key] = append(ps.copyChains[key], callee)
}

// resolveDeterministicOverrides resolves an op bound to multiple handlers
// when every conflicting handler's own enclosing function is merged into the
// SAME destination table by the SAME assembler function via maps.Copy
// (findCopyChains) -- cognitoidp's real shape: dispatchTable()'s sequential
// maps.Copy calls merge OpsA/OpsB/OpsC family tables, and Go's maps.Copy
// overwrites on collision, so whichever family is copied LAST deterministically
// wins. That is knowable by reading the assembler's own call order, not a
// guess -- unlike sqs's real ambiguity (KNOWN BLIND SPOT #6), where the two
// conflicting tables are never merged into a shared destination at all, so no
// chain will ever contain both sides and the op stays reported ambiguous.
func (ps *pkgScan) resolveDeterministicOverrides() []string {
	var notes []string
	for op, handlers := range ps.ambiguousOps {
		winner, losers, chainKey, ok := ps.deterministicWinner(handlers)
		if !ok {
			continue
		}
		ps.opToHandler[op] = winner
		delete(ps.ambiguousOps, op)
		notes = append(notes, fmt.Sprintf(
			"%s -> %s (preferred over %s: both merged into %s, %s copied last)",
			op, winner, strings.Join(losers, ", "), chainKey, winner))
	}
	sort.Strings(notes)

	return notes
}

func (ps *pkgScan) deterministicWinner(
	handlers map[string]bool,
) (string, []string, string, bool) {
	names := make([]string, 0, len(handlers))
	for h := range handlers {
		names = append(names, h)
	}
	sort.Strings(names)

	chainKeys := make([]string, 0, len(ps.copyChains))
	for key := range ps.copyChains {
		chainKeys = append(chainKeys, key)
	}
	sort.Strings(chainKeys)

	for _, key := range chainKeys {
		order := ps.copyChains[key]
		pos := map[string]int{}
		for i, fn := range order {
			pos[fn] = i
		}

		best, bestPos, matched := "", -1, 0
		for _, h := range names {
			p, inChain := pos[ps.handlerSourceFunc[h]]
			if !inChain {
				continue
			}
			matched++
			if p > bestPos {
				best, bestPos = h, p
			}
		}
		if matched != len(names) {
			continue
		}

		var losers []string
		for _, h := range names {
			if h != best {
				losers = append(losers, h)
			}
		}

		return best, losers, key, true
	}

	return "", nil, "", false
}

// ---------- checking ----------

type opResult struct {
	Op      string
	Handler string
	// DispatchKey is set only when Op was recovered from the handler's own
	// name rather than matched directly against the dispatch table's key
	// (KNOWN BLIND SPOT #7) -- the raw REST-path/method key the service
	// actually dispatches on, kept for the report so a fixed op is visibly
	// traceable back to it.
	DispatchKey  string
	Written      []string
	NotInTree    []string
	CaseMismatch []string
	DynSkipped   int
	FuncsWalked  int
	EmptyOutput  bool
}

type checkResult struct {
	OpsChecked         []opResult
	UnresolvedOps      []string
	AmbiguousOps       []string
	AmbiguousHandlers  map[string][]string
	InternalOpsSkipped []string

	// FilteredOps holds ops moved out of UnresolvedOps by filterEnumGroups: an
	// entire switch/map dispatch source where EVERY candidate op failed SDK
	// resolution -- an enum/type-string table misread as op dispatch, not a
	// genuinely missing real op. Still fully visible in the report, just not
	// counted toward "unresolved".
	FilteredOps []string

	// DeterministicOverrides names every op resolveDeterministicOverrides
	// pulled out of true ambiguity because both conflicting handlers' source
	// functions are merged into the same maps.Copy destination, in a fixed
	// textual order -- visible so a reader can verify the chosen winner
	// against the assembler's own call order.
	DeterministicOverrides []string

	SDKOpsResolved     int
	SDKTypesResolved   int
	HandlerOpsResolved int
	TotalWritten       int
	TotalDynSkipped    int
	NoWrittenKeys      bool
}

// pascalOpRe matches a dispatch key that already looks like a real SDK
// operation name -- used only to make the ERROR message for a genuinely
// unresolved op distinguish "this looked like an op name and still wasn't
// found" from "this was never an op name to begin with" (KNOWN BLIND SPOT
// #7). It plays no part in whether recovery is attempted.
var pascalOpRe = regexp.MustCompile(`^[A-Z][A-Za-z0-9]*$`)

// recoverOpName strips this repo's "handle"/"json" handler-name prefix (see
// handleNameRe) to recover the real PascalCase SDK operation name a
// REST-path-keyed dispatch table's handler encodes even though its dispatch
// KEY does not (KNOWN BLIND SPOT #7, e.g. mgn's real
// map[string]routeEntry{"DELETE tags": {op: "UntagResource",
// fn: h.handleUntagResource}} -- the map key is a method+path string, not
// "UntagResource"). It never guesses: the caller only trusts the result if
// it independently exists in the SDK's own op index, so a "handle<Anything>"
// that isn't a real op name recovers to nothing usable.
func recoverOpName(handler string) string {
	switch {
	case strings.HasPrefix(handler, "handle") && len(handler) > len("handle"):
		return handler[len("handle"):]
	case strings.HasPrefix(handler, "json") && len(handler) > len("json"):
		return handler[len("json"):]
	default:
		return ""
	}
}

// resolvedOp pairs a raw dispatch-table key (as recorded in ps.opToHandler)
// with the real SDK operation name it resolves to and the handler bound to
// it. RealOp equals DispatchKey unless Recovered is true.
type resolvedOp struct {
	DispatchKey string
	RealOp      string
	Handler     string
	Recovered   bool
}

// filterEnumGroups moves an unresolved op from res.UnresolvedOps into
// res.FilteredOps when EVERY op sharing its dispatch source (the same switch
// statement, or the same map/slice literal -- ps.opGroup) also failed to
// resolve, and that source bound at least 2 candidate op names. A source
// batting 0-for-N against the real SDK op index is far more likely to be a
// per-item enum/type-string table misread as op dispatch (apigateway's
// IntegrationType switch, glacier's job-type switch, lightsail's
// taggableResolvers, swf's decisionHandlers) than N genuinely missing real
// ops -- a real dispatch table drawn from the same protocol/prefix almost
// always resolves at least one member. Requiring N>=2 keeps a single
// genuinely-missing op (KNOWN BLIND SPOT #7 territory) from ever being
// silently reclassified: it has no sibling in its own source to corroborate
// it, so it stays a normal, visible UnresolvedOps ERROR.
// minCorroboratingGroupSize is the smallest dispatch-source group
// filterEnumGroups will ever reclassify as an enum table: a lone unresolved
// candidate has no sibling in its own source to corroborate the guess, so it
// always stays ordinary KNOWN BLIND SPOT #7 territory.
const minCorroboratingGroupSize = 2

func filterEnumGroups(ops []string, ps *pkgScan, res *checkResult) {
	unresolved := map[string]bool{}
	for _, op := range res.UnresolvedOps {
		unresolved[op] = true
	}

	groupOps, groupOrder := groupOpsBySource(ops, ps)

	filtered := map[string]bool{}
	for _, gid := range groupOrder {
		members := groupOps[gid]
		if len(members) < minCorroboratingGroupSize || !allUnresolved(members, unresolved) {
			continue
		}
		for _, op := range members {
			filtered[op] = true
			res.FilteredOps = append(res.FilteredOps, op)
		}
	}

	if len(filtered) == 0 {
		return
	}

	var kept []string
	for _, op := range res.UnresolvedOps {
		if !filtered[op] {
			kept = append(kept, op)
		}
	}
	res.UnresolvedOps = kept
	sort.Strings(res.FilteredOps)
}

// groupOpsBySource buckets ops by the single switch statement or map/slice
// literal that bound each one (ps.opGroup), preserving the order each group
// was first seen.
func groupOpsBySource(ops []string, ps *pkgScan) (map[string][]string, []string) {
	groupOps := map[string][]string{}
	var groupOrder []string
	for _, op := range ops {
		gid, ok := ps.opGroup[op]
		if !ok {
			continue
		}
		if _, seen := groupOps[gid]; !seen {
			groupOrder = append(groupOrder, gid)
		}
		groupOps[gid] = append(groupOps[gid], op)
	}

	return groupOps, groupOrder
}

func allUnresolved(members []string, unresolved map[string]bool) bool {
	for _, op := range members {
		if !unresolved[op] {
			return false
		}
	}

	return true
}

// resolveOpNames matches every checkable dispatch binding against the SDK's
// own op index, falling back to recoverOpName for a binding whose raw key
// isn't itself a real op name (KNOWN BLIND SPOT #7). It refuses to guess:
// if two different dispatch keys (recovered or not) resolve to the same
// real op name under DIFFERENT handlers, neither is silently preferred --
// both are pulled into res.AmbiguousOps/AmbiguousHandlers instead, the same
// refuse-to-guess contract KNOWN BLIND SPOT #6 established for a literal op
// string bound twice.
func resolveOpNames(ops []string, ps *pkgScan, idx *sdkIndex, res *checkResult) []resolvedOp {
	candidates := map[string][]resolvedOp{}
	var order []string

	addCandidate := func(realOp string, ro resolvedOp) {
		if _, seen := candidates[realOp]; !seen {
			order = append(order, realOp)
		}
		candidates[realOp] = append(candidates[realOp], ro)
	}

	for _, op := range ops {
		handler := ps.opToHandler[op]

		if _, ok := idx.ops[op]; ok {
			addCandidate(op, resolvedOp{DispatchKey: op, RealOp: op, Handler: handler})

			continue
		}

		if cand := recoverOpName(handler); cand != "" {
			if _, ok := idx.ops[cand]; ok {
				addCandidate(cand, resolvedOp{DispatchKey: op, RealOp: cand, Handler: handler, Recovered: true})

				continue
			}
		}

		res.UnresolvedOps = append(res.UnresolvedOps, op)
	}

	var resolved []resolvedOp
	for _, realOp := range order {
		cands := candidates[realOp]
		handlers := map[string]bool{}
		for _, c := range cands {
			handlers[c.Handler] = true
		}
		if len(handlers) > 1 {
			names := make([]string, 0, len(handlers))
			for h := range handlers {
				names = append(names, h)
			}
			sort.Strings(names)
			res.AmbiguousOps = append(res.AmbiguousOps, realOp)
			res.AmbiguousHandlers[realOp] = names

			continue
		}
		resolved = append(resolved, cands[0])
	}

	filterEnumGroups(ops, ps, res)

	sort.Strings(res.UnresolvedOps)
	sort.Slice(resolved, func(i, j int) bool { return resolved[i].RealOp < resolved[j].RealOp })

	return resolved
}

// resolveCheckableOps returns the ops eligible for a normal SDK comparison,
// sorted: onlyOp-filtered, with gopherstack-internal "__"-prefixed ops
// routed into res.InternalOpsSkipped and ambiguously-bound ops (see
// collectAmbiguousOps) excluded entirely.
func resolveCheckableOps(ps *pkgScan, onlyOp string, res *checkResult) []string {
	var ops []string
	for op := range ps.opToHandler {
		if onlyOp != "" && op != onlyOp {
			continue
		}
		if strings.HasPrefix(op, "__") {
			// gopherstack-internal chaos/test endpoint (e.g. shield's
			// "__SimulateAttack"), not a real AWS operation -- there is no
			// SDK deserializer to check it against by definition.
			res.InternalOpsSkipped = append(res.InternalOpsSkipped, op)

			continue
		}
		if _, ambiguous := ps.ambiguousOps[op]; ambiguous {
			// Bound to more than one distinct handler (gopherstack-kiwf) --
			// reported separately by collectAmbiguousOps so the wrong
			// handler's keys are never compared against the SDK.
			continue
		}
		ops = append(ops, op)
	}
	sort.Strings(ops)
	sort.Strings(res.InternalOpsSkipped)

	return ops
}

// collectAmbiguousOps fills res.AmbiguousOps/AmbiguousHandlers from every op
// pkgScan.bindOp found bound to more than one distinct handler name.
func collectAmbiguousOps(ps *pkgScan, onlyOp string, res *checkResult) {
	for op, handlers := range ps.ambiguousOps {
		if onlyOp != "" && op != onlyOp {
			continue
		}
		if strings.HasPrefix(op, "__") {
			continue
		}
		names := make([]string, 0, len(handlers))
		for h := range handlers {
			names = append(names, h)
		}
		sort.Strings(names)
		res.AmbiguousOps = append(res.AmbiguousOps, op)
		res.AmbiguousHandlers[op] = names
	}
	sort.Strings(res.AmbiguousOps)
}

func runCheck(sdkPath, prefix, svcDir, onlyOp string) (*checkResult, error) {
	idx, err := parseSDK(sdkPath, prefix)
	if err != nil {
		return nil, fmt.Errorf("sdk parse error: %w", err)
	}

	ps, err := scanPackage(svcDir)
	if err != nil {
		return nil, fmt.Errorf("svc parse error: %w", err)
	}

	res := &checkResult{
		SDKOpsResolved:         len(idx.ops),
		SDKTypesResolved:       len(idx.types),
		HandlerOpsResolved:     len(ps.opToHandler),
		AmbiguousHandlers:      map[string][]string{},
		DeterministicOverrides: ps.deterministicOverrideNotes,
	}

	ops := resolveCheckableOps(ps, onlyOp, res)
	collectAmbiguousOps(ps, onlyOp, res)

	resolved := resolveOpNames(ops, ps, idx, res)
	sort.Strings(res.AmbiguousOps)

	for _, ro := range resolved {
		info := idx.ops[ro.RealOp]

		written, dynSkipped, walked := ps.writtenKeys(ro.Handler)
		// KEYCHECK_DEBUG_WALK=<Op> prints the exact same-package call chain
		// writtenKeys followed for that op -- use it to hand-verify a
		// MISMATCH against blind spot #2 (an unrelated function reachable
		// deep in the call graph, not the op's real response builder).
		if os.Getenv("KEYCHECK_DEBUG_WALK") == ro.RealOp {
			fmt.Fprintln(os.Stderr, "WALKED:", walked)
		}
		allowed := reachable(idx, info, map[string]bool{}, 0)
		res.TotalWritten += len(written)
		res.TotalDynSkipped += dynSkipped

		or := buildOpResult(ro.RealOp, ro.Handler, idx.emptyOps[ro.RealOp], written, allowed, dynSkipped, len(walked))
		if ro.Recovered {
			or.DispatchKey = ro.DispatchKey
		}
		res.OpsChecked = append(res.OpsChecked, or)
	}

	res.NoWrittenKeys = res.TotalWritten == 0 && len(res.OpsChecked) > 0

	return res, nil
}

func buildOpResult(
	op, handler string, emptyOutput bool, written, allowed map[string]bool, dynSkipped, walked int,
) opResult {
	or := opResult{
		Op: op, Handler: handler, EmptyOutput: emptyOutput,
		DynSkipped: dynSkipped, FuncsWalked: walked,
	}
	for k := range written {
		or.Written = append(or.Written, k)
	}
	sort.Strings(or.Written)

	for k := range written {
		if allowed[k] {
			continue
		}
		or.NotInTree = append(or.NotInTree, k)
		for a := range allowed {
			if strings.EqualFold(a, k) {
				or.CaseMismatch = append(or.CaseMismatch, k+" (sdk expects: "+a+")")

				break
			}
		}
	}
	sort.Strings(or.NotInTree)
	sort.Strings(or.CaseMismatch)

	return or
}

// ---------- report ----------

const (
	exitClean      = 0
	exitUnresolved = 1
	exitMismatch   = 2
	// exitPartial is returned when at least one op WAS resolved and checked
	// (real MISMATCH data, or a real clean result, exists in res.OpsChecked)
	// but at least one other op in the same service remains unresolved or
	// ambiguous. This exists so "unresolved" can never again be misread as
	// "unchecked": exitUnresolved means NOTHING in the service was verified;
	// exitPartial means most of it was, and the report says exactly how
	// much. Found necessary when the 42-op "unresolved" tier collapsed 13
	// substantially-checked services (cognitoidp alone: 102 ops checked, 304
	// mismatched keys) into the same bucket as services with zero dispatch
	// resolved at all.
	exitPartial = 3
)

func main() {
	sdkPath := flag.String("sdk", "", "path to SDK deserializers.go")
	prefix := flag.String("prefix", "awsAwsjson11_", "deserializer func prefix")
	svcDir := flag.String("svc", "", "service directory")
	onlyOp := flag.String("op", "", "restrict to one op (optional)")
	dumpType := flag.String("dump-type", "", "print the parsed case-list for one SDK type name and exit")
	flag.Parse()

	if *dumpType != "" {
		runDumpType(*sdkPath, *prefix, *dumpType)

		return
	}

	if *svcDir == "" {
		fmt.Fprintln(os.Stderr, "ERROR: -svc is required (or use -dump-type for sdk-only inspection)")
		os.Exit(exitUnresolved)
	}

	res, err := runCheck(*sdkPath, *prefix, *svcDir, *onlyOp)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(exitUnresolved)
	}

	os.Exit(report(res, *svcDir, *prefix))
}

func runDumpType(sdkPath, prefix, dumpType string) {
	idx, err := parseSDK(sdkPath, prefix)
	if err != nil {
		fmt.Fprintln(os.Stderr, "sdk parse error:", err)
		os.Exit(exitUnresolved)
	}

	info, ok := idx.types[dumpType]
	if !ok {
		info, ok = idx.ops[dumpType]
	}
	if !ok {
		fmt.Fprintf(os.Stdout, "type/op %q not found\n", dumpType)
		os.Exit(exitUnresolved)
	}

	fmt.Fprintf(os.Stdout, "kind=%s elem=%s\n", info.kind, info.elem)
	keys := make([]string, 0, len(info.cases))
	for k := range info.cases {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(os.Stdout, "  %q -> target=%q\n", k, info.cases[k])
	}
}

// report prints the full result and returns the process exit code. Any
// state meaning "this service was not actually checked" is reported as an
// explicit ERROR and outranks a found MISMATCH in the exit code, per the
// fail-loud contract documented at the top of this file.
func report(res *checkResult, svcDir, prefix string) int {
	fmt.Fprintf(os.Stdout, "SDK ops resolved: %d, types resolved: %d\n", res.SDKOpsResolved, res.SDKTypesResolved)
	fmt.Fprintf(os.Stdout, "handler dispatch resolved: %d ops\n", res.HandlerOpsResolved)

	if res.SDKOpsResolved == 0 && res.SDKTypesResolved == 0 {
		fmt.Fprintf(os.Stderr,
			"ERROR: zero deserializer functions matched -prefix %q -- wrong prefix, or a protocol\n"+
				"keycheck can't read (query/ec2query/restxml). NOT verified.\n",
			prefix)

		return exitUnresolved
	}

	if len(res.InternalOpsSkipped) > 0 {
		fmt.Fprintf(os.Stdout, "SKIPPED (gopherstack-internal, not a real AWS op): %s\n",
			strings.Join(res.InternalOpsSkipped, ", "))
	}

	if res.HandlerOpsResolved == 0 {
		fmt.Fprintf(os.Stderr,
			"ERROR: zero op-to-handler dispatch bindings resolved in %s -- unrecognised routing style.\n"+
				"NOT verified.\n", svcDir)

		return exitUnresolved
	}

	printUnresolvedOpErrors(res.UnresolvedOps)
	printAmbiguousOpErrors(res)
	printFilteredOps(res.FilteredOps)
	printDeterministicOverrides(res.DeterministicOverrides)

	if res.NoWrittenKeys {
		fmt.Fprintf(os.Stdout,
			"N/A: %s writes zero detectable wire-output keys -- no map[string]<T> literal and no\n"+
				"locally-declared *Output-suffixed struct literal reachable. Out of scope for keycheck\n"+
				"(see KNOWN BLIND SPOT #5).\n",
			svcDir)
	}

	printRecoveredOps(res.OpsChecked)

	mismatches := printOpResults(res.OpsChecked)
	checked := len(res.OpsChecked)
	unresolvedOrAmbiguous := len(res.UnresolvedOps) + len(res.AmbiguousOps)

	fmt.Fprintf(os.Stdout, "\nTotal ops checked: %d, unresolved sdk ops: %d, ambiguous handler bindings: %d, "+
		"total mismatched keys: %d, total written keys: %d, total dynamic-key sites skipped: %d\n",
		checked, len(res.UnresolvedOps), len(res.AmbiguousOps), mismatches,
		res.TotalWritten, res.TotalDynSkipped)

	return printVerdict(checked, mismatches, unresolvedOrAmbiguous)
}

// printUnresolvedOpErrors reports each genuinely-unresolved op, distinguishing
// (KNOWN BLIND SPOT #7) a REST-path/method dispatch key that recovery
// couldn't turn into a real SDK op from an op-shaped name the SDK simply
// doesn't have.
func printUnresolvedOpErrors(unresolvedOps []string) {
	for _, op := range unresolvedOps {
		if !pascalOpRe.MatchString(op) {
			fmt.Fprintf(os.Stderr,
				"ERROR: op %q looks like a REST-path/method dispatch key, not an SDK operation name --\n"+
					"its handler's own name was checked for a recoverable op name (KNOWN BLIND SPOT #7) and\n"+
					"either doesn't follow the handle/json<Op> convention or the recovered name isn't a\n"+
					"real SDK op either. Allowed keys unknown, NOT verified.\n",
				op)

			continue
		}
		fmt.Fprintf(os.Stderr,
			"ERROR: op %s has no deserializeOpDocument%sOutput function and no confirmed-empty\n"+
				"wrapper -- allowed keys unknown, NOT verified.\n",
			op, op)
	}
}

func printAmbiguousOpErrors(res *checkResult) {
	for _, op := range res.AmbiguousOps {
		fmt.Fprintf(os.Stderr,
			"ERROR: op %s is bound to %d conflicting handlers (%s) -- a case clause or dispatch\n"+
				"table entry rebinds an op already bound to a different handler, so ps.opToHandler\n"+
				"cannot tell which one the real dispatcher uses; refusing to guess and compare the\n"+
				"wrong handler's keys against the SDK. See KNOWN BLIND SPOT #6. NOT verified.\n",
			op, len(res.AmbiguousHandlers[op]), strings.Join(res.AmbiguousHandlers[op], ", "))
	}
}

// printFilteredOps surfaces every op filterEnumGroups pulled out of
// UnresolvedOps -- an entire switch/map dispatch source that batted 0-for-N
// against the real SDK op index (an enum/type-string table misread as op
// dispatch), never silently dropped.
func printFilteredOps(filteredOps []string) {
	if len(filteredOps) == 0 {
		return
	}
	fmt.Fprintf(os.Stdout,
		"FILTERED (enum/type-string table, not real op dispatch -- every candidate from the same\n"+
			"switch/map source failed SDK resolution): %s\n",
		strings.Join(filteredOps, ", "))
}

// printDeterministicOverrides surfaces every op resolveDeterministicOverrides
// resolved out of true ambiguity, naming the winning handler and the losing
// one(s) so a reader can independently verify the choice against the
// assembler function's own maps.Copy order.
func printDeterministicOverrides(notes []string) {
	for _, note := range notes {
		fmt.Fprintf(os.Stdout, "DETERMINISTIC OVERRIDE: %s\n", note)
	}
}

// printRecoveredOps surfaces (KNOWN BLIND SPOT #7) which checked ops only
// resolved because their real SDK name was recovered from their handler's
// name rather than matched against the dispatch table's own key -- so a
// fixed op stays traceable back to the raw key it came from.
func printRecoveredOps(opsChecked []opResult) {
	var recovered []string
	for _, or := range opsChecked {
		if or.DispatchKey != "" {
			recovered = append(recovered, fmt.Sprintf("%s (dispatch key %q)", or.Op, or.DispatchKey))
		}
	}
	if len(recovered) > 0 {
		fmt.Fprintf(os.Stdout,
			"RECOVERED (KNOWN BLIND SPOT #7, REST-path-keyed dispatch resolved via handler name): %s\n",
			strings.Join(recovered, ", "))
	}
}

// printVerdict prints the single line meant to be read at a glance -- an
// unresolved service (checked == 0) can never again be misread as "clean" or
// conflated with a substantially-checked one that merely has a residual
// unresolved/ambiguous op (exitPartial) -- and returns the exit code.
func printVerdict(checked, mismatches, unresolvedOrAmbiguous int) int {
	switch {
	case checked == 0 && unresolvedOrAmbiguous > 0:
		fmt.Fprintln(os.Stdout, "VERDICT: UNRESOLVED -- zero ops verified. Do not read this as clean.")

		return exitUnresolved
	case unresolvedOrAmbiguous > 0:
		fmt.Fprintf(os.Stdout,
			"VERDICT: PARTIAL -- %d op(s) verified (%d mismatched key(s) found) but %d op(s) remain\n"+
				"unresolved or ambiguous and were NOT checked. This is a substantially-checked service,\n"+
				"not an unchecked one -- do not conflate it with a service reporting zero ops checked.\n",
			checked, mismatches, unresolvedOrAmbiguous)

		return exitPartial
	case mismatches > 0:
		fmt.Fprintf(os.Stdout, "VERDICT: MISMATCH -- all %d dispatched op(s) resolved and checked; real key "+
			"mismatches found.\n", checked)

		return exitMismatch
	default:
		fmt.Fprintf(os.Stdout, "VERDICT: CLEAN -- all %d dispatched op(s) resolved and checked; zero "+
			"mismatches.\n", checked)

		return exitClean
	}
}

func printOpResults(ops []opResult) int {
	mismatches := 0
	for _, or := range ops {
		if len(or.NotInTree) == 0 {
			continue
		}
		mismatches += len(or.NotInTree)
		empty := ""
		if or.EmptyOutput {
			empty = " (confirmed-empty-output op)"
		}
		fmt.Fprintf(os.Stdout, "MISMATCH\top=%s handler=%s funcsWalked=%d dynSkipped=%d%s\n",
			or.Op, or.Handler, or.FuncsWalked, or.DynSkipped, empty)
		for _, k := range or.NotInTree {
			fmt.Fprintf(os.Stdout, "  wrote %q -- not in real reachable shape for %s\n", k, or.Op)
		}
		for _, c := range or.CaseMismatch {
			fmt.Fprintf(os.Stdout, "  CASE-MISMATCH: wrote %s\n", c)
		}
	}

	return mismatches
}
