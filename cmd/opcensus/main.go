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
// function calls it makes (ec2 delegates through two helper functions;
// omics delegates through a dispatch-table constructor) and, where the
// method instead ranges over a struct field populated elsewhere (a
// "h.ops" map built in a constructor), falling back to a whole-package
// scan for string-keyed map literals and index assignments on that field
// name.
//
// This counts what each service's own dispatcher claims to support, then
// buckets by List/Describe/Get prefix -- a proxy for "collection or
// nested-shape response surface," the shape of bug this issue tracks. It
// says nothing about whether any individual op is correct; that is still
// a per-op hand read against the pinned SDK deserializer.
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
	"sort"
	"strings"
)

const (
	resolutionDirect          = "direct"
	resolutionChased          = "chased"
	resolutionDynamicFallback = "dynamic-fallback"
	resolutionUnresolved      = "unresolved"

	maxWalkDepth = 8
)

var opNameRe = regexp.MustCompile(`^[A-Z][A-Za-z0-9]*$`)

type serviceResult struct {
	Service     string   `json:"service"`
	Resolution  string   `json:"resolution"` // direct, chased, dynamic-fallback, unresolved
	AllOps      []string `json:"allOps"`
	ListOps     []string `json:"listOps"`
	DescribeOps []string `json:"describeOps"`
	GetOps      []string `json:"getOps"`
	Total       int      `json:"total"`
	LDG         int      `json:"ldg"` // len(ListOps)+len(DescribeOps)+len(GetOps)
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
		results = append(results, censusService(filepath.Join(servicesDir, e.Name()), e.Name()))
	}

	sort.Slice(results, func(i, j int) bool { return results[i].LDG > results[j].LDG })

	return results, nil
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
	fmt.Fprintf(w, "%-28s %6s %6s %6s %6s %6s  %s\n", "service", "total", "list", "descr", "get", "L+D+G", "resolution")
	for _, r := range results {
		fmt.Fprintf(w, "%-28s %6d %6d %6d %6d %6d  %s\n",
			r.Service, r.Total, len(r.ListOps), len(r.DescribeOps), len(r.GetOps), r.LDG, r.Resolution)
	}
}

// pkgIndex is the parsed, indexed form of one service package: every
// function declaration, every package-level string const, and every
// package-level var whose initializer isn't a plain string (a table/slice
// literal or a call worth walking).
type pkgIndex struct {
	files     map[string]*ast.File
	funcDecls map[string]*ast.FuncDecl
	constVals map[string]string
	varSpecs  map[string]ast.Expr
}

func indexPackage(dir string) pkgIndex {
	idx := pkgIndex{
		files:     map[string]*ast.File{},
		funcDecls: map[string]*ast.FuncDecl{},
		constVals: map[string]string{},
		varSpecs:  map[string]ast.Expr{},
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return idx
	}

	fset := token.NewFileSet()
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

			continue
		}
		gd, isGen := decl.(*ast.GenDecl)
		if !isGen {
			continue
		}
		idx.indexValueSpecs(gd.Specs)
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
// GetSupportedOperations, chasing same-package function calls, function
// values used as table entries, and const identifiers used in place of
// literal strings. It also records "dynamic sources" -- range targets that
// aren't literal string collections, resolved afterward by
// resolveDynamicSources.
type opWalker struct {
	idx            pkgIndex
	seen           map[string]bool
	visited        map[string]bool
	dynamicSources map[string]bool
}

func newOpWalker(idx pkgIndex) *opWalker {
	return &opWalker{
		idx:            idx,
		seen:           map[string]bool{},
		visited:        map[string]bool{},
		dynamicSources: map[string]bool{},
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
	if fd == nil || fd.Body == nil || depth > maxWalkDepth || w.visited[fd.Name.Name] {
		return
	}
	w.visited[fd.Name.Name] = true

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		w.visitNode(n, depth)

		return true
	})
}

func (w *opWalker) visitNode(n ast.Node, depth int) {
	switch v := n.(type) {
	case *ast.BasicLit:
		w.recordLiteral(v)
	case *ast.CallExpr:
		if id, isIdent := v.Fun.(*ast.Ident); isIdent {
			if callee, ok := w.idx.funcDecls[id.Name]; ok {
				w.walk(callee, depth+1)
			}
		}
	case *ast.Ident:
		w.visitIdent(v, depth)
	case *ast.RangeStmt:
		w.recordRangeTarget(v.X)
	}
}

// visitIdent catches functions and package-level const/var tables
// referenced as values, not just called directly -- e.g. ec2's
// []func() []string{ fooSupportedOps, ... } provider table (invoked
// indirectly through a loop variable, never literally "fooSupportedOps()"),
// or sqs's []string{ opAddPermission, ... } naming a package const instead
// of writing the string inline.
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
		case *ast.Ident:
			if s, ok := w.idx.constVals[v.Name]; ok && opNameRe.MatchString(s) {
				w.seen[s] = true
			}
		}

		return true
	})
}

// resolveDynamicSources handles GetSupportedOperations bodies that range
// over something other than a literal string collection.
//
// Tier 1: the range target names a package-level var (glue's
// `for i, b := range glueOpBindings { names[i] = b.name }`) -- walk that
// var's own declaration/initializer.
//
// Tier 2 (only if tier 1 found nothing): the range target is a bare struct
// field with no matching package var (rekognition/appstream's "h.ops"
// populated inside a constructor) -- scan every map literal and
// index-assignment in the whole package for that field name.
func (w *opWalker) resolveDynamicSources() {
	for src := range w.dynamicSources {
		if val, ok := w.idx.varSpecs[src]; ok {
			w.extractLiterals(val)
		}
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

func (w *opWalker) visitFallbackNode(n ast.Node) {
	switch v := n.(type) {
	case *ast.KeyValueExpr:
		lit, isStringKey := v.Key.(*ast.BasicLit)
		if isStringKey && lit.Kind == token.STRING {
			w.recordLiteral(lit)
		}
	case *ast.IndexExpr:
		w.visitFallbackIndex(v)
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
	idx := indexPackage(dir)

	entry, ok := idx.funcDecls["GetSupportedOperations"]
	if !ok {
		return serviceResult{Service: name, Resolution: resolutionUnresolved}
	}

	w := newOpWalker(idx)
	w.walk(entry, 0)

	resolution := resolutionDirect
	switch {
	case len(w.seen) == 0 && len(w.dynamicSources) > 0:
		resolution = resolutionDynamicFallback
		w.resolveDynamicSources()
	case len(w.seen) > 0 && len(w.visited) > 1:
		resolution = resolutionChased
	}
	if len(w.seen) == 0 {
		resolution = resolutionUnresolved
	}

	return bucketize(name, resolution, w.seen)
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
