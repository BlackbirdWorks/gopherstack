// Command opcensus counts each service's List/Describe/Get operations for
// gopherstack-6flj (see services/_WRAPPER_KEY_SWEEP_REMAINDER.md).
//
// The notes on 6flj recorded per-service remainders that turned out wrong
// twice, each time by a large factor (ec2 "~144" vs. the real ~220+;
// rds "130+ remaining" vs. the real 26), both times because the wrong
// number was carried forward instead of being read from the service's own
// operation list. This tool reads that list directly: for each
// services/<dir>, it parses every non-test .go file, locates the
// GetSupportedOperations method (every service implements it; it is the
// dispatcher's own declared operation set, not a doc comment or a guess),
// and collects the string literals it returns -- following same-package
// function and method calls it makes (ec2 delegates through two helper
// functions; omics delegates through a dispatch-table constructor),
// resolving named consts used as map keys or table entries in place of
// literal strings, and, where the field it reads is only ever populated
// indirectly -- assigned in a constructor from a same-package method call,
// or threaded through an intermediate local variable
// (`keys := collections.SortedKeys(h.ops)`) -- chasing that assignment
// wherever it lives in the package (resolveName/chaseExpr/chaseCall).
//
// This counts what each service's own dispatcher claims to support, then
// buckets by List/Describe/Get prefix -- a proxy for "collection or
// nested-shape response surface," the shape of bug this issue tracks. It
// says nothing about whether any individual op is correct; that is still
// a per-op hand read against the pinned SDK deserializer.
//
// gopherstack-c7s3 / gopherstack-jq8x: a service whose op count could not be
// resolved used to print as a bare 0 -- indistinguishable from a real
// small, clean service, which is exactly how ssm and route53resolver went
// unnoticed. Any service whose SDK module or op list can't be resolved now
// prints as an explicit ERROR row instead, and the process exits non-zero,
// so a silent zero is no longer possible: it is either a verified count or
// a loud failure.
//
// Usage:
//
//	go run ./cmd/opcensus                # ranked summary to stdout
//	go run ./cmd/opcensus -json out.json # full per-service detail
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
)

const (
	resolutionDirect          = "direct"
	resolutionChased          = "chased"
	resolutionDynamicFallback = "dynamic-fallback"
	resolutionUnresolved      = "unresolved"
	resolutionAmbiguous       = "ambiguous"

	maxWalkDepth = 8
)

var (
	opNameRe    = regexp.MustCompile(`^[A-Z][A-Za-z0-9]*$`)
	sdkModuleRe = regexp.MustCompile(`aws-sdk-go-v2/service/([a-z0-9]+)`)
)

type serviceResult struct {
	Service          string   `json:"service"`
	Resolution       string   `json:"resolution"`
	ErrorReason      string   `json:"errorReason,omitempty"`
	DescribeOps      []string `json:"describeOps"`
	AllOps           []string `json:"allOps"`
	ListOps          []string `json:"listOps"`
	SDKModules       []string `json:"sdkModules"`
	GetOps           []string `json:"getOps"`
	DeclaredBy       []string `json:"declaredBy,omitempty"`
	AmbiguousReasons []string `json:"ambiguousReasons,omitempty"`
	Total            int      `json:"total"`
	LDG              int      `json:"ldg"`
	Aliased          bool     `json:"aliased"`
	Error            bool     `json:"error,omitempty"`
}

func main() {
	jsonPath := flag.String("json", "", "write full per-service detail to this path")
	servicesDir := flag.String("dir", "services", "path to the services directory")
	flag.Parse()

	results, err := censusAll(*servicesDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "read services dir:", err)
		os.Exit(1)
	}

	if *jsonPath != "" {
		if writeErr := writeJSON(*jsonPath, results); writeErr != nil {
			fmt.Fprintln(os.Stderr, writeErr)
			os.Exit(1)
		}
	}

	printReport(os.Stdout, results)

	if hasErrors(results) {
		os.Exit(1)
	}
}

func hasErrors(results []serviceResult) bool {
	for _, r := range results {
		if r.Error {
			return true
		}
	}

	return false
}

func censusAll(servicesDir string) ([]serviceResult, error) {
	entries, err := os.ReadDir(servicesDir)
	if err != nil {
		return nil, err
	}

	var results []serviceResult
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), "_") {
			continue
		}
		dir := filepath.Join(servicesDir, e.Name())
		if !hasGoFiles(dir) {
			// A tombstone directory (e.g. qldb, qldbsession: deprecated
			// service, README only, no Go code, not wired into the router).
			// Not a service to sweep, so counting it as a 0-op row would make
			// it indistinguishable from an unresolved real service.
			continue
		}
		results = append(results, censusService(dir, e.Name()))
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].Error != results[j].Error {
			return results[i].Error // error rows sort to the top, ahead of any rank
		}

		return results[i].LDG > results[j].LDG
	})

	return results, nil
}

func hasGoFiles(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".go") {
			return true
		}
	}

	return false
}

// resolveSDKModules finds every aws-sdk-go-v2/service/<x> import path used
// anywhere in the service's own .go files (including tests), by scanning
// file contents directly rather than trusting the directory name -- the
// directory name and the pinned SDK module diverge for some services
// (services/dms pins databasemigrationservice, services/elb pins
// elasticloadbalancing).
func resolveSDKModules(dir string) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}

	seen := map[string]bool{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") {
			continue
		}
		data, readErr := os.ReadFile(filepath.Join(dir, e.Name()))
		if readErr != nil {
			continue
		}
		for _, m := range sdkModuleRe.FindAllStringSubmatch(string(data), -1) {
			seen[m[1]] = true
		}
	}

	mods := make([]string, 0, len(seen))
	for m := range seen {
		mods = append(mods, m)
	}
	sort.Strings(mods)

	return mods
}

func writeJSON(path string, results []serviceResult) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create json: %w", err)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if encErr := enc.Encode(results); encErr != nil {
		return fmt.Errorf("encode json: %w", encErr)
	}

	return nil
}

func printReport(w *os.File, results []serviceResult) {
	printErrorBanner(w, results)

	fmt.Fprintf(w, "%-28s %6s %6s %6s %6s %6s  %-16s %-14s %s\n",
		"service", "total", "list", "descr", "get", "L+D+G", "resolution", "sdk-module", "")
	for _, r := range results {
		module := formatModule(r)
		if r.Error {
			fmt.Fprintf(w, "%-28s %6s %6s %6s %6s %6s  %-16s %-14s ERROR: %s\n",
				r.Service, "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", r.Resolution, module, r.ErrorReason)

			continue
		}
		note := ""
		if len(r.DeclaredBy) > 1 {
			note = fmt.Sprintf("  [union of %d: %s]", len(r.DeclaredBy), strings.Join(r.DeclaredBy, ","))
		}
		fmt.Fprintf(w, "%-28s %6d %6d %6d %6d %6d  %-16s %-14s%s\n",
			r.Service, r.Total, len(r.ListOps), len(r.DescribeOps), len(r.GetOps), r.LDG, r.Resolution, module, note)
	}
}

func printErrorBanner(w *os.File, results []serviceResult) {
	var errs []serviceResult
	for _, r := range results {
		if r.Error {
			errs = append(errs, r)
		}
	}
	if len(errs) == 0 {
		return
	}

	fmt.Fprintf(w,
		"ERROR: %d service(s) could not be fully resolved -- these are NOT zero-op services, they are unchecked:\n",
		len(errs))
	for _, r := range errs {
		fmt.Fprintf(w, "  %-28s %s\n", r.Service, r.ErrorReason)
	}
	fmt.Fprintln(w)
}

func formatModule(r serviceResult) string {
	module := strings.Join(r.SDKModules, ",")
	if r.Aliased {
		module += " (alias)"
	}

	return module
}

// pkgIndex is the parsed, indexed form of one service package: every
// function declaration, every package-level string const, and every
// package-level var whose initializer isn't a plain string (a table/slice
// literal or a call worth walking).
type pkgIndex struct {
	files map[string]*ast.File
	// funcDecls is a last-one-wins lookup by function/method name, used to
	// chase same-package helper calls -- safe because every name other than
	// GetSupportedOperations is unique across a service package (verified by
	// AST scan, gopherstack-1t0m). allFuncDecls keeps every declaration of a
	// name, used at the GetSupportedOperations entry point specifically,
	// since that method is legitimately declared more than once per package
	// (one per handler type) in exactly two services today.
	funcDecls    map[string]*ast.FuncDecl
	allFuncDecls map[string][]*ast.FuncDecl
	constVals    map[string]string
	varSpecs     map[string]ast.Expr
	typeDecls    map[string]ast.Expr
	fset         *token.FileSet
}

func indexPackage(dir string) pkgIndex {
	idx := pkgIndex{
		files:        map[string]*ast.File{},
		funcDecls:    map[string]*ast.FuncDecl{},
		allFuncDecls: map[string][]*ast.FuncDecl{},
		constVals:    map[string]string{},
		varSpecs:     map[string]ast.Expr{},
		typeDecls:    map[string]ast.Expr{},
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return idx
	}

	fset := token.NewFileSet()
	idx.fset = fset
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(dir, e.Name())
		f, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			continue
		}
		idx.files[path] = f
		idx.indexDecls(f.Decls)
	}

	return idx
}

func (idx pkgIndex) indexDecls(decls []ast.Decl) {
	for _, decl := range decls {
		if fd, isFunc := decl.(*ast.FuncDecl); isFunc {
			idx.funcDecls[fd.Name.Name] = fd
			idx.allFuncDecls[fd.Name.Name] = append(idx.allFuncDecls[fd.Name.Name], fd)

			continue
		}
		gd, isGen := decl.(*ast.GenDecl)
		if !isGen {
			continue
		}
		if gd.Tok == token.TYPE {
			idx.indexTypeSpecs(gd.Specs)

			continue
		}
		idx.indexValueSpecs(gd.Specs)
	}
}

func (idx pkgIndex) indexTypeSpecs(specs []ast.Spec) {
	for _, spec := range specs {
		if ts, isType := spec.(*ast.TypeSpec); isType {
			idx.typeDecls[ts.Name.Name] = ts.Type
		}
	}
}

func (idx pkgIndex) indexValueSpecs(specs []ast.Spec) {
	for _, spec := range specs {
		vs, isValue := spec.(*ast.ValueSpec)
		if !isValue || len(vs.Names) != len(vs.Values) {
			continue
		}
		for i, vname := range vs.Names {
			lit, isStringLit := vs.Values[i].(*ast.BasicLit)
			if isStringLit && lit.Kind == token.STRING {
				idx.constVals[vname.Name] = trimQuotes(lit.Value)
			} else {
				idx.varSpecs[vname.Name] = vs.Values[i]
			}
		}
	}
}

func trimQuotes(s string) string { return strings.Trim(s, "\"`") }

// opWalker accumulates operation-name string literals reachable from
// GetSupportedOperations, chasing same-package function and method calls,
// function values used as table entries, and const identifiers used in
// place of literal strings. It also records "dynamic sources" -- field or
// variable names read but not defined in the walked function -- resolved
// afterward by resolveDynamicSources.
type opWalker struct {
	idx              pkgIndex
	seen             map[string]bool
	visited          map[*ast.FuncDecl]bool
	dynamicSources   map[string]bool
	resolvedNames    map[string]bool
	ambiguousReasons []string
	ambiguous        bool
}

func newOpWalker(idx pkgIndex) *opWalker {
	return &opWalker{
		idx:            idx,
		seen:           map[string]bool{},
		visited:        map[*ast.FuncDecl]bool{},
		dynamicSources: map[string]bool{},
		resolvedNames:  map[string]bool{},
	}
}

func (w *opWalker) recordLiteral(lit *ast.BasicLit) {
	if lit.Kind != token.STRING {
		return
	}
	if s := trimQuotes(lit.Value); opNameRe.MatchString(s) {
		w.seen[s] = true
	}
}

func (w *opWalker) walk(fd *ast.FuncDecl, depth int) {
	if fd == nil || fd.Body == nil || depth > maxWalkDepth || w.visited[fd] {
		return
	}
	if depth > 0 && !w.isTableShaped(fd) {
		// Past the entry point, a function's own return shape is what
		// tells a table-builder helper (single []string or map[string]X
		// result) apart from an operation handler wired into the table as
		// a value (comprehend's h.getIteration, signature
		// func(map[string]any) (map[string]any, error)) -- the walker has
		// no other way to tell them apart, since both are just an
		// identifier that happens to name a local function. Walking into
		// a handler pulls in whatever op-name-shaped strings its business
		// logic happens to contain.
		return
	}
	w.visited[fd] = true

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		return w.visitNode(n, depth)
	})
}

// isTableShaped reports whether fd's signature looks like a table-builder
// (returns exactly one value, a slice or a map, possibly under a named type
// like appstream's opTable) rather than an operation handler. Every op-table
// builder in this codebase -- GetSupportedOperations itself, family functions
// like ssmParameterOps, dispatch constructors like buildOps -- has this
// shape; operation handlers uniformly don't (comprehend's operation is
// func(map[string]any) (map[string]any, error), two results).
func (w *opWalker) isTableShaped(fd *ast.FuncDecl) bool {
	if fd.Type.Results == nil || len(fd.Type.Results.List) != 1 {
		return false
	}

	return w.isTableType(fd.Type.Results.List[0].Type, 0)
}

func (w *opWalker) isTableType(t ast.Expr, depth int) bool {
	if depth > maxWalkDepth {
		return false
	}
	switch v := t.(type) {
	case *ast.ArrayType, *ast.MapType:
		return true
	case *ast.Ident:
		if underlying, ok := w.idx.typeDecls[v.Name]; ok {
			return w.isTableType(underlying, depth+1)
		}
	}

	return false
}

// visitNode returns whether ast.Inspect should descend into n's children.
// It returns false for two shapes the walker cannot statically resolve to
// real operation names -- a bare string concatenation, and a range over a
// lookup/spec table whose body builds names by concatenation rather than
// using the range's own keys directly -- so their fragments never enter
// w.seen looking like real ops (gopherstack-k9n5: comprehend's
// ops["Start"+prefix] over asyncJobSpecs()/resourceSpecs() produced entries
// like "Start" and "DocumentClassificationJob", neither a real operation).
func (w *opWalker) visitNode(n ast.Node, depth int) bool {
	switch v := n.(type) {
	case *ast.BasicLit:
		w.recordLiteral(v)
	case *ast.BinaryExpr:
		if isStringConcat(v) {
			w.recordAmbiguous(v, "operation name built by string concatenation")

			return false
		}
	case *ast.CallExpr:
		if id, isIdent := v.Fun.(*ast.Ident); isIdent {
			if callee, ok := w.idx.funcDecls[id.Name]; ok {
				w.walk(callee, depth+1)
			}
		}
	case *ast.Ident:
		w.visitIdent(v, depth)
	case *ast.RangeStmt:
		if v.Body != nil && containsStringConcat(v.Body) {
			w.recordAmbiguous(
				v,
				"range body builds operation names by string concatenation over a lookup table, not a direct op-name table",
			)

			return false
		}
		w.recordRangeTarget(v.X)
	case *ast.SelectorExpr:
		// A field/method access anywhere in the walked body, not just a
		// range target -- ssm reads its ops table via
		// `slices.Collect(maps.Keys(h.ops))`, route53resolver via a bare
		// `return h.supportedOpsCache`. Neither is a for-range, so
		// recordRangeTarget alone never sees them. Recording every
		// selector unconditionally is safe: it only feeds
		// resolveDynamicSources, which is only consulted once the direct
		// walk found zero literals (see censusService).
		w.dynamicSources[v.Sel.Name] = true
	}

	return true
}

// containsStringConcat reports whether n contains a string concatenation
// anywhere in its subtree.
func containsStringConcat(n ast.Node) bool {
	found := false
	ast.Inspect(n, func(nn ast.Node) bool {
		if found {
			return false
		}
		if b, ok := nn.(*ast.BinaryExpr); ok && isStringConcat(b) {
			found = true

			return false
		}

		return true
	})

	return found
}

// isStringConcat reports whether b is a "+" building up a string, as
// opposed to int/other arithmetic that also uses token.ADD (redshift's
// GetSupportedOperations does `len(g1)+len(g2)`, not a string build) --
// go/ast carries no type info, so this is a syntactic heuristic: at least
// one operand must itself be, or resolve through nested "+" to, a string
// literal.
func isStringConcat(b *ast.BinaryExpr) bool {
	if b.Op != token.ADD {
		return false
	}

	return isStringOperand(b.X) || isStringOperand(b.Y)
}

func isStringOperand(e ast.Expr) bool {
	switch v := e.(type) {
	case *ast.BasicLit:
		return v.Kind == token.STRING
	case *ast.BinaryExpr:
		return isStringConcat(v)
	default:
		return false
	}
}

// recordAmbiguous marks the whole service as unresolvable rather than let a
// partial, plausible-looking count stand in for a real one -- the same
// precedent gopherstack-c7s3 set for total resolution failure.
func (w *opWalker) recordAmbiguous(n ast.Node, why string) {
	w.ambiguous = true

	pos := "?"
	if w.idx.fset != nil {
		pos = w.idx.fset.Position(n.Pos()).String()
	}

	reason := pos + ": " + why
	if !slices.Contains(w.ambiguousReasons, reason) {
		w.ambiguousReasons = append(w.ambiguousReasons, reason)
	}
}

// visitIdent catches functions and package-level const/var tables
// referenced as values, not just called directly -- e.g. ec2's
// []func() []string{ fooSupportedOps, ... } provider table (invoked
// indirectly through a loop variable, never literally "fooSupportedOps()"),
// or sqs's []string{ opAddPermission, ... } naming a package const instead
// of writing the string inline. It also fires on the Sel identifier of a
// method call like h.ssmParameterOps(), since ast.Inspect visits that Sel
// as its own *ast.Ident node -- that is how the walker chases method calls
// even though the CallExpr case above only matches bare-identifier calls.
func (w *opWalker) visitIdent(id *ast.Ident, depth int) {
	if callee, ok := w.idx.funcDecls[id.Name]; ok {
		w.walk(callee, depth+1)

		return
	}
	if s, ok := w.idx.constVals[id.Name]; ok {
		if opNameRe.MatchString(s) {
			w.seen[s] = true
		}

		return
	}
	if _, ok := w.idx.varSpecs[id.Name]; ok {
		w.dynamicSources[id.Name] = true
	}
}

func (w *opWalker) recordRangeTarget(x ast.Expr) {
	switch v := x.(type) {
	case *ast.SelectorExpr:
		w.dynamicSources[v.Sel.Name] = true
	case *ast.Ident:
		w.dynamicSources[v.Name] = true
	}
}

// extractLiterals walks an arbitrary subtree (a var's initializer
// expression) collecting op-shaped string literals directly, and resolving
// bare identifiers against package consts -- covers table entries keyed by
// a const identifier (omics: map[string]opHandlerFunc{opCreateReferenceStore:
// ...}) rather than a raw string literal.
func (w *opWalker) extractLiterals(n ast.Node) {
	ast.Inspect(n, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.BasicLit:
			w.recordLiteral(v)
		case *ast.BinaryExpr:
			if isStringConcat(v) {
				w.recordAmbiguous(v, "operation name built by string concatenation")

				return false
			}
		case *ast.Ident:
			if s, ok := w.idx.constVals[v.Name]; ok && opNameRe.MatchString(s) {
				w.seen[s] = true
			}
		}

		return true
	})
}

// resolveDynamicSources handles GetSupportedOperations bodies that don't
// hand the walker literal op strings directly.
//
// Tier 1: the source names a package-level var (glue's
// `for i, b := range glueOpBindings { names[i] = b.name }`) -- walk that
// var's own declaration/initializer.
//
// Tier 2 (only if tier 1 found nothing): the source is a struct field or a
// local variable with no matching package var -- resolveName searches the
// whole package for wherever it is assigned (typically the constructor)
// and chases that assignment, however many hops of indirection it takes
// (ssm: one hop through a method call; route53resolver: two, through an
// intermediate local variable). This is what resolves the shape that
// motivated gopherstack-jq8x.
//
// Tier 3 (only if tier 2 also found nothing): scan every map literal and
// index-assignment in the whole package for a key matching a known
// operation name (rekognition/appstream's "h.ops" populated inline in a
// constructor with no separate named builder to chase). This is
// deliberately a last resort -- it isn't scoped to the field name, so it
// can pick up unrelated map[string]T literals (dms's total was
// historically overcounted this way); tier 2 resolving precisely is what
// keeps most services out of this tier.
func (w *opWalker) resolveDynamicSources() {
	for src := range w.dynamicSources {
		if val, ok := w.idx.varSpecs[src]; ok {
			w.extractLiterals(val)
		}
	}

	if len(w.seen) > 0 {
		return
	}

	for src := range w.dynamicSources {
		w.resolveName(src, 0)
	}

	if len(w.seen) > 0 {
		return
	}

	for _, f := range w.idx.files {
		ast.Inspect(f, func(n ast.Node) bool {
			w.visitFallbackNode(n)

			return true
		})
	}
}

// resolveName searches every file in the package for an assignment whose
// left-hand side names a field or variable called name (`h.ops = ...` or
// `keys := ...`), and chases each match's right-hand side. Package-wide,
// unscoped by function -- a heuristic, not a real dataflow analysis -- so
// it is guarded by resolvedNames (visit each name once) and maxWalkDepth
// (bound the hop count), same discipline the funcDecl chase already uses.
func (w *opWalker) resolveName(name string, depth int) {
	if depth > maxWalkDepth || w.resolvedNames[name] {
		return
	}
	w.resolvedNames[name] = true

	for _, f := range w.idx.files {
		for _, rhs := range findAssignments(f, name) {
			w.chaseExpr(rhs, depth+1)
		}
	}
}

// chaseExpr resolves one assigned expression. A call to a known
// same-package function or method is walked like any other reachable
// function. A call to something outside the package (maps.Keys,
// collections.SortedKeys) can't itself be walked, but its arguments might
// name the real source, so each identifier/selector argument is chased as
// a name in turn. A bare identifier is chased as a name too, for a local
// variable assigned from another local variable or field
// (`h.supportedOpsCache = keys`, then `keys := ...` elsewhere).
func (w *opWalker) chaseExpr(expr ast.Expr, depth int) {
	switch e := expr.(type) {
	case *ast.CallExpr:
		w.chaseCall(e, depth)
	case *ast.Ident:
		w.resolveName(e.Name, depth)
	case *ast.SelectorExpr:
		w.resolveName(e.Sel.Name, depth)
	}
}

func (w *opWalker) chaseCall(call *ast.CallExpr, depth int) {
	if name := calleeName(call.Fun); name != "" {
		if callee, ok := w.idx.funcDecls[name]; ok {
			w.walk(callee, depth)

			return
		}
	}
	for _, arg := range call.Args {
		w.chaseExpr(arg, depth)
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

// findAssignments returns the right-hand side of every assignment in f
// (`=` or `:=`) whose left-hand side is a bare identifier or a selector
// (struct field) named name.
func findAssignments(f *ast.File, name string) []ast.Expr {
	var exprs []ast.Expr
	ast.Inspect(f, func(n ast.Node) bool {
		as, isAssign := n.(*ast.AssignStmt)
		if !isAssign || len(as.Lhs) != len(as.Rhs) {
			return true
		}
		for i, lhs := range as.Lhs {
			if lhsName(lhs) == name {
				exprs = append(exprs, as.Rhs[i])
			}
		}

		return true
	})

	return exprs
}

func lhsName(e ast.Expr) string {
	switch v := e.(type) {
	case *ast.Ident:
		return v.Name
	case *ast.SelectorExpr:
		return v.Sel.Name
	default:
		return ""
	}
}

func (w *opWalker) visitFallbackNode(n ast.Node) {
	switch v := n.(type) {
	case *ast.KeyValueExpr:
		w.recordFallbackKey(v.Key)
	case *ast.IndexExpr:
		w.visitFallbackIndex(v)
	}
}

// recordFallbackKey handles both literal-string map keys
// (map[string]T{"Foo": ...}) and named-const keys (map[string]T{opFoo: ...},
// dms's style across its opsXxx() family builders).
func (w *opWalker) recordFallbackKey(key ast.Expr) {
	switch k := key.(type) {
	case *ast.BasicLit:
		if k.Kind == token.STRING {
			w.recordLiteral(k)
		}
	case *ast.Ident:
		if s, ok := w.idx.constVals[k.Name]; ok && opNameRe.MatchString(s) {
			w.seen[s] = true
		}
	}
}

func (w *opWalker) visitFallbackIndex(v *ast.IndexExpr) {
	lit, isStringIndex := v.Index.(*ast.BasicLit)
	if !isStringIndex || lit.Kind != token.STRING {
		return
	}

	nameOK := false
	switch sel := v.X.(type) {
	case *ast.SelectorExpr:
		nameOK = w.dynamicSources[sel.Sel.Name]
	case *ast.Ident:
		nameOK = w.dynamicSources[sel.Name]
	}
	if nameOK {
		w.recordLiteral(lit)
	}
}

func censusService(dir, name string) serviceResult {
	modules := resolveSDKModules(dir)

	idx := indexPackage(dir)

	// entries holds every GetSupportedOperations declaration in the
	// package. Almost every service has exactly one; bedrock and redshift
	// each have two (one handler type per file) -- both legitimate Go, so
	// every entry is walked and the results unioned rather than picking
	// one and silently dropping the rest (gopherstack-1t0m).
	entries := idx.allFuncDecls["GetSupportedOperations"]
	if len(entries) == 0 {
		return finalizeResult(name, resolutionUnresolved, modules, nil, nil, nil)
	}

	w := newOpWalker(idx)
	for _, entry := range entries {
		w.walk(entry, 0)
	}

	resolution := resolutionDirect
	switch {
	case len(w.seen) == 0 && len(w.dynamicSources) > 0:
		resolution = resolutionDynamicFallback
		w.resolveDynamicSources()
	case len(w.seen) > 0 && len(w.visited) > len(entries):
		resolution = resolutionChased
	}
	if len(w.seen) == 0 {
		resolution = resolutionUnresolved
	}
	if w.ambiguous {
		resolution = resolutionAmbiguous
	}

	var declaredBy []string
	if len(entries) > 1 {
		for _, e := range entries {
			declaredBy = append(declaredBy, receiverTypeName(e))
		}
		sort.Strings(declaredBy)
	}

	return finalizeResult(name, resolution, modules, w.seen, declaredBy, w.ambiguousReasons)
}

// receiverTypeName returns fd's receiver type name (e.g. "*Handler"), or "?"
// for a bare function with no receiver.
func receiverTypeName(fd *ast.FuncDecl) string {
	if fd.Recv == nil || len(fd.Recv.List) != 1 {
		return "?"
	}

	return exprTypeName(fd.Recv.List[0].Type)
}

func exprTypeName(e ast.Expr) string {
	switch v := e.(type) {
	case *ast.Ident:
		return v.Name
	case *ast.StarExpr:
		return "*" + exprTypeName(v.X)
	default:
		return "?"
	}
}

// finalizeResult buckets the resolved ops and marks the row as an explicit
// error -- rather than a bare zero -- whenever either resolution axis
// failed: no SDK module could be identified from the package's own source,
// or no operations could be resolved from GetSupportedOperations. Either
// failure means the numeric columns are "not checked," not "checked and
// found small," and a reader must be able to tell those apart without
// cross-referencing the resolution column by hand.
func finalizeResult(
	name, resolution string, modules []string, seen map[string]bool, declaredBy, ambiguousReasons []string,
) serviceResult {
	r := bucketize(name, resolution, seen)
	r.SDKModules = modules
	r.Aliased = len(modules) > 0 && !slices.Contains(modules, name)
	r.DeclaredBy = declaredBy
	r.AmbiguousReasons = ambiguousReasons

	var reasons []string
	if len(modules) == 0 {
		reasons = append(reasons, "no aws-sdk-go-v2/service/* import found in package")
	}
	if resolution == resolutionUnresolved {
		reasons = append(reasons, "no operations resolved from GetSupportedOperations")
	}
	if resolution == resolutionAmbiguous {
		reasons = append(
			reasons,
			"operation names cannot be statically resolved: "+strings.Join(ambiguousReasons, "; "),
		)
	}
	if len(reasons) > 0 {
		r.Error = true
		r.ErrorReason = strings.Join(reasons, "; ")
	}

	return r
}

func bucketize(name, resolution string, seen map[string]bool) serviceResult {
	all := make([]string, 0, len(seen))

	var list, describe, get []string
	for op := range seen {
		all = append(all, op)
		switch {
		case strings.HasPrefix(op, "List"):
			list = append(list, op)
		case strings.HasPrefix(op, "Describe"):
			describe = append(describe, op)
		case strings.HasPrefix(op, "Get"):
			get = append(get, op)
		}
	}
	sort.Strings(all)
	sort.Strings(list)
	sort.Strings(describe)
	sort.Strings(get)

	return serviceResult{
		Service:     name,
		Resolution:  resolution,
		AllOps:      all,
		ListOps:     list,
		DescribeOps: describe,
		GetOps:      get,
		Total:       len(all),
		LDG:         len(list) + len(describe) + len(get),
	}
}
