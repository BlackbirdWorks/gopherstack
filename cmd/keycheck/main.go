// Command keycheck verifies that the string keys a service handler writes
// into its map[string]<T> wire responses actually exist in the pinned AWS
// SDK's own response deserializer -- not the Go field name, the
// deserializer's literal switch-case string. It exists for gopherstack-zquj:
// a hand-written map[string]any response key is checked by no compiler and
// no existing scan, so a typo'd or wrong-cased key is silently dropped by
// any real client and invisible to a raw-body test (which asserts the same
// key the author typed).
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
// Usage:
//
//	go run ./cmd/keycheck -sdk <path to deserializers.go> -prefix awsAwsjson11_ -svc <service dir> [-op OpName]
//
// Exit codes: 0 clean or N/A, 1 the service could not be resolved (see
// FAIL-LOUD), 2 a real key mismatch was found.
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
	"regexp"
	"sort"
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
	fset        *token.FileSet
	funcDecls   map[string]*ast.FuncDecl
	constVals   map[string]string
	mapAnyVars  map[string]bool
	opToHandler map[string]string
}

func scanPackage(dir string) (*pkgScan, error) {
	fset := token.NewFileSet()
	ps := &pkgScan{
		fset:        fset,
		funcDecls:   map[string]*ast.FuncDecl{},
		constVals:   map[string]string{},
		mapAnyVars:  map[string]bool{},
		opToHandler: map[string]string{},
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
	for _, f := range files {
		ps.findOpDispatch(f)
	}

	return ps, nil
}

func (ps *pkgScan) indexFile(f *ast.File) {
	for _, decl := range f.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			ps.funcDecls[d.Name.Name] = d
		case *ast.GenDecl:
			ps.indexConsts(d)
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

func (ps *pkgScan) findOpDispatch(f *ast.File) {
	ast.Inspect(f, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.CaseClause:
			ps.recordCaseDispatch(v)
		case *ast.CompositeLit:
			ps.recordMapDispatch(v)
		}

		return true
	})
}

func (ps *pkgScan) recordCaseDispatch(cc *ast.CaseClause) {
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
	for _, op := range opNames {
		ps.opToHandler[op] = handler
	}
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

// recordMapDispatch handles both dispatch-table conventions this repo uses:
// a string literal key (map[string]T{"CreateFoo": ...}) and a package-level
// const identifier key (map[string]T{opCreateFoo: ...}, e.g. dms's op
// families) resolved through ps.constVals, already populated by indexFile
// before findOpDispatch runs.
func (ps *pkgScan) recordMapDispatch(cl *ast.CompositeLit) {
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
		if name == "" {
			continue
		}
		ps.opToHandler[key] = name
	}
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

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.CompositeLit:
			dynamicSkipped += ps.collectLitKeys(v, keys)
		case *ast.AssignStmt:
			dynamicSkipped += ps.collectIndexAssignKeys(v, keys)
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

func (ps *pkgScan) collectIndexAssignKeys(v *ast.AssignStmt, keys map[string]bool) int {
	dynamicSkipped := 0
	for _, lhs := range v.Lhs {
		idx, ok := lhs.(*ast.IndexExpr)
		if !ok {
			continue
		}
		id, ok := idx.X.(*ast.Ident)
		if !ok || !ps.mapAnyVars[id.Name] {
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

// ---------- checking ----------

type opResult struct {
	Op           string
	Handler      string
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
	InternalOpsSkipped []string
	SDKOpsResolved     int
	SDKTypesResolved   int
	HandlerOpsResolved int
	TotalWritten       int
	TotalDynSkipped    int
	NoMapAnyLiterals   bool
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
		SDKOpsResolved:     len(idx.ops),
		SDKTypesResolved:   len(idx.types),
		HandlerOpsResolved: len(ps.opToHandler),
	}

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
		ops = append(ops, op)
	}
	sort.Strings(ops)
	sort.Strings(res.InternalOpsSkipped)

	for _, op := range ops {
		info, ok := idx.ops[op]
		if !ok {
			res.UnresolvedOps = append(res.UnresolvedOps, op)

			continue
		}

		handler := ps.opToHandler[op]
		written, dynSkipped, walked := ps.writtenKeys(handler)
		// KEYCHECK_DEBUG_WALK=<Op> prints the exact same-package call chain
		// writtenKeys followed for that op -- use it to hand-verify a
		// MISMATCH against blind spot #2 (an unrelated function reachable
		// deep in the call graph, not the op's real response builder).
		if os.Getenv("KEYCHECK_DEBUG_WALK") == op {
			fmt.Fprintln(os.Stderr, "WALKED:", walked)
		}
		allowed := reachable(idx, info, map[string]bool{}, 0)
		res.TotalWritten += len(written)
		res.TotalDynSkipped += dynSkipped

		or := buildOpResult(op, handler, idx.emptyOps[op], written, allowed, dynSkipped, len(walked))
		res.OpsChecked = append(res.OpsChecked, or)
	}

	res.NoMapAnyLiterals = res.TotalWritten == 0 && len(res.OpsChecked) > 0

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

	for _, op := range res.UnresolvedOps {
		fmt.Fprintf(os.Stderr,
			"ERROR: op %s has no deserializeOpDocument%sOutput function and no confirmed-empty\n"+
				"wrapper -- allowed keys unknown, NOT verified.\n",
			op, op)
	}

	if res.NoMapAnyLiterals {
		fmt.Fprintf(os.Stdout,
			"N/A: %s writes zero map[string]<T> literal keys -- struct-tag construction, out of scope\n"+
				"for keycheck.\n",
			svcDir)
	}

	mismatches := printOpResults(res.OpsChecked)

	fmt.Fprintf(os.Stdout, "\nTotal ops checked: %d, unresolved sdk ops: %d, total mismatched keys: %d, "+
		"total written keys: %d, total dynamic-key sites skipped: %d\n",
		len(res.OpsChecked), len(res.UnresolvedOps), mismatches, res.TotalWritten, res.TotalDynSkipped)

	switch {
	case len(res.UnresolvedOps) > 0:
		return exitUnresolved
	case mismatches > 0:
		return exitMismatch
	default:
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
