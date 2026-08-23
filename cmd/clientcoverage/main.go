// Command clientcoverage measures which AWS operations gopherstack's own
// test suite actually exercises through a real aws-sdk-go-v2 client, as
// opposed to a raw-body httptest assertion or a direct handler call.
//
// Filed for gopherstack-n3zi, which needed a real number in place of the
// inherited "77 percent never touched" estimate. The denominator comes from
// cmd/opcensus's own -json output (each service's GetSupportedOperations,
// 10,565 ops / 160 services as of gopherstack-jq8x). This tool supplies the
// numerator: an AST walk over every _test.go file under the given roots
// (test/integration and services/... by default) that finds
//
//   - a client construction, either a direct <pkg>.NewFromConfig(...) call,
//     or a call to a same-package helper function whose declared return
//     type is *<pkg>.Client at some position (chases one level through
//     helpers like newTestHandlerAndClient, which itself calls
//     newRoundTripClient, which calls NewFromConfig -- resolved via
//     signature, not by tracing the helper's body);
//   - a function-literal parameter typed *<pkg>.Client, the dominant idiom
//     in test/integration (`verify func(t *testing.T, client *s3.Client)`);
//   - then any call of the form boundVar.<Op>(...) on a variable bound by
//     either of the above, where <Op> is a name in that SDK module's real
//     operation set (per opcensus -- this is what excludes non-API methods
//     like Options()).
//
// A module can be imported by more than one service's own dispatcher (e.g.
// "s3" appears in both s3's and sagemaker's sdkModules); ownership of a
// given (module, op) pair is resolved by checking which of the module's
// candidate services actually declares that op name. See BLIND SPOTS below
// for what this walk cannot see.
//
// Usage:
//
//	go run ./cmd/opcensus -json /tmp/opcensus.json
//	go run ./cmd/clientcoverage -opcensus /tmp/opcensus.json
//	go run ./cmd/clientcoverage -opcensus /tmp/opcensus.json -json /tmp/coverage.json
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

var (
	opNameRe  = regexp.MustCompile(`^[A-Z][A-Za-z0-9]*$`)
	sdkPathRe = regexp.MustCompile(`^"github\.com/aws/aws-sdk-go-v2/service/([a-z0-9]+)"$`)
	paginRe   = regexp.MustCompile(`^New([A-Za-z0-9]+)Paginator$`)
)

const percentScale = 100

// opService mirrors the fields of cmd/opcensus's serviceResult that this
// tool needs; extra fields in the source JSON are ignored by Unmarshal.
type opService struct {
	Service    string   `json:"service"`
	SDKModules []string `json:"sdkModules"`
	AllOps     []string `json:"allOps"`
}

type evidence struct {
	file string
	line int
}

type moduleIndex struct {
	moduleToServices map[string][]string
	serviceOps       map[string]map[string]bool
}

func buildModuleIndex(services []opService) moduleIndex {
	idx := moduleIndex{
		moduleToServices: map[string][]string{},
		serviceOps:       map[string]map[string]bool{},
	}

	for _, s := range services {
		ops := make(map[string]bool, len(s.AllOps))
		for _, op := range s.AllOps {
			ops[op] = true
		}
		idx.serviceOps[s.Service] = ops

		for _, m := range s.SDKModules {
			idx.moduleToServices[m] = append(idx.moduleToServices[m], s.Service)
		}
	}

	return idx
}

// resolveOwner picks the single service that both imports module and
// declares op as a real operation. Ambiguous (>1 match) and unresolved (0
// match) results are reported separately rather than guessed at.
//
// A module can legitimately appear in another service's sdkModules purely
// because that service's own source imports it for unrelated type reuse
// (opcensus's sdkModules field records every aws-sdk-go-v2/service import a
// package makes, not just the one it dispatches through -- e.g. glacier
// imports s3 and genuinely happens to share several of S3's multipart op
// names). When that produces >1 candidate, a candidate whose own service
// name is exactly the module name is its native owner and wins the tie;
// true ambiguity (no exact-name candidate, or more than one) is still
// reported rather than guessed at.
func (idx moduleIndex) resolveOwner(module, op string) (string, bool) {
	var matches []string
	for _, svc := range idx.moduleToServices[module] {
		if idx.serviceOps[svc][op] {
			matches = append(matches, svc)
		}
	}

	if len(matches) == 1 {
		return matches[0], false
	}

	if len(matches) > 1 {
		var native []string
		for _, svc := range matches {
			if svc == module {
				native = append(native, svc)
			}
		}
		if len(native) == 1 {
			return native[0], false
		}
	}

	return "", len(matches) > 1
}

// fileImports maps a local identifier to the SDK module it refers to, for
// imports of the exact form ".../aws-sdk-go-v2/service/<mod>" (subpackages
// like ".../service/s3/types" are deliberately excluded: they never declare
// NewFromConfig or a *Client type, so keeping them out avoids noise).
type fileImports map[string]string

func parseSDKImports(f *ast.File) fileImports {
	imports := fileImports{}

	for _, imp := range f.Imports {
		m := sdkPathRe.FindStringSubmatch(imp.Path.Value)
		if m == nil {
			continue
		}

		module := m[1]
		local := module
		if imp.Name != nil {
			local = imp.Name.Name
		}
		imports[local] = module
	}

	return imports
}

// funcSignature records, per return position, which SDK module a function
// returns a *<module>.Client for ("" if that position isn't a client).
type funcSignature []string

func collectFuncSignatures(
	files []*ast.File,
	importsOf map[*ast.File]fileImports,
) map[string]funcSignature {
	sigs := map[string]funcSignature{}

	for _, f := range files {
		imports := importsOf[f]
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Recv != nil || fd.Type.Results == nil {
				continue
			}

			sig := resultModules(fd.Type.Results, imports)
			if hasNonEmpty(sig) {
				sigs[fd.Name.Name] = sig
			}
		}
	}

	return sigs
}

func hasNonEmpty(sig funcSignature) bool {
	for _, m := range sig {
		if m != "" {
			return true
		}
	}

	return false
}

func resultModules(results *ast.FieldList, imports fileImports) funcSignature {
	var sig funcSignature
	for _, field := range results.List {
		mod := clientModuleOf(field.Type, imports)
		n := len(field.Names)
		if n == 0 {
			n = 1
		}
		for range n {
			sig = append(sig, mod)
		}
	}

	return sig
}

// clientModuleOf returns the SDK module name if t is *<pkg>.Client for a
// pkg resolvable through imports, else "".
func clientModuleOf(t ast.Expr, imports fileImports) string {
	star, ok := t.(*ast.StarExpr)
	if !ok {
		return ""
	}

	sel, ok := star.X.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Client" {
		return ""
	}

	xIdent, ok := sel.X.(*ast.Ident)
	if !ok {
		return ""
	}

	return imports[xIdent.Name]
}

// extractClientParams returns paramName -> module for any *<pkg>.Client
// typed parameters in ft, the pattern behind test/integration's dominant
// `verify func(t *testing.T, client *s3.Client)` idiom.
func extractClientParams(ft *ast.FuncType, imports fileImports) map[string]string {
	out := map[string]string{}
	if ft.Params == nil {
		return out
	}

	for _, field := range ft.Params.List {
		mod := clientModuleOf(field.Type, imports)
		if mod == "" {
			continue
		}
		for _, name := range field.Names {
			if name.Name != "_" {
				out[name.Name] = mod
			}
		}
	}

	return out
}

// classifyRHS looks at a single call expression on the right of an
// assignment and reports, per left-hand position, which SDK module it
// produces. It also reports a directly-observed paginator op: gopherstack's
// test suite has no NewXPaginator usage today (checked), but a client bound
// only via a paginator constructor's first argument would otherwise be
// silently missed, so the constructor name itself is read as evidence for
// its op.
func classifyRHS(
	rhs ast.Expr,
	imports fileImports,
	sigs map[string]funcSignature,
) (funcSignature, string, string) {
	ce, isCall := rhs.(*ast.CallExpr)
	if !isCall {
		return nil, "", ""
	}

	switch fun := ce.Fun.(type) {
	case *ast.SelectorExpr:
		xIdent, isIdent := fun.X.(*ast.Ident)
		if !isIdent {
			return nil, "", ""
		}

		mod, isSDK := imports[xIdent.Name]
		if !isSDK {
			return nil, "", ""
		}

		if fun.Sel.Name == "NewFromConfig" {
			return funcSignature{mod}, "", ""
		}

		if pm := paginRe.FindStringSubmatch(fun.Sel.Name); pm != nil {
			return nil, mod, pm[1]
		}
	case *ast.Ident:
		if sig, isProducer := sigs[fun.Name]; isProducer {
			return sig, "", ""
		}
	}

	return nil, "", ""
}

type coverageResult struct {
	covered   map[string]map[string]evidence // service -> op -> evidence
	ambiguous []string                       // "module.op at file:line" for calls that matched >1 service
}

func newCoverageResult() *coverageResult {
	return &coverageResult{covered: map[string]map[string]evidence{}}
}

func (r *coverageResult) record(idx moduleIndex, module, op, file string, line int) {
	svc, ambiguous := idx.resolveOwner(module, op)
	if ambiguous {
		r.ambiguous = append(r.ambiguous, fmt.Sprintf("%s.%s at %s:%d", module, op, file, line))

		return
	}
	if svc == "" {
		return
	}

	if r.covered[svc] == nil {
		r.covered[svc] = map[string]evidence{}
	}
	if _, exists := r.covered[svc][op]; !exists {
		r.covered[svc][op] = evidence{file: file, line: line}
	}
}

// walkFunc seeds a bindings table (varName -> SDK module) from entryParams
// (the enclosing FuncDecl or FuncLit's own *<pkg>.Client-typed parameters --
// e.g. a top-level `func testDescribeThing(t *testing.T, client *svc.Client)`
// referenced by name from a test table, not called inline) plus every
// client construction and *<pkg>.Client-typed closure parameter found
// anywhere in body -- including inside nested func literals, deliberately
// flattened into one scope rather than modeled per-block. That flattening
// can, in principle, let a binding from one closure leak into a sibling
// closure in the same top-level function; in practice every instance found
// in this repo uses one client per top-level test function, so this has not
// been observed to over-count here. It is the tool's main documented blind
// spot.
func walkFunc(
	body *ast.BlockStmt,
	entryParams map[string]string,
	file string,
	fset *token.FileSet,
	imports fileImports,
	sigs map[string]funcSignature,
	idx moduleIndex,
	result *coverageResult,
) {
	bindings := map[string]string{}
	maps.Copy(bindings, entryParams)

	ast.Inspect(body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.FuncLit:
			maps.Copy(bindings, extractClientParams(x.Type, imports))
		case *ast.AssignStmt:
			bindAssign(x.Lhs, x.Rhs, file, fset, imports, sigs, idx, result, bindings)
		case *ast.ValueSpec:
			bindAssign(identsToExprs(x.Names), x.Values, file, fset, imports, sigs, idx, result, bindings)
		}

		return true
	})

	ast.Inspect(body, func(n ast.Node) bool {
		ce, isCall := n.(*ast.CallExpr)
		if !isCall {
			return true
		}

		sel, isSel := ce.Fun.(*ast.SelectorExpr)
		if !isSel {
			return true
		}

		xIdent, isIdent := sel.X.(*ast.Ident)
		if !isIdent {
			return true
		}

		mod, isBound := bindings[xIdent.Name]
		if !isBound || !opNameRe.MatchString(sel.Sel.Name) {
			return true
		}

		result.record(idx, mod, sel.Sel.Name, file, fset.Position(ce.Pos()).Line)

		return true
	})
}

func identsToExprs(idents []*ast.Ident) []ast.Expr {
	out := make([]ast.Expr, len(idents))
	for i, id := range idents {
		out[i] = id
	}

	return out
}

func bindAssign(
	lhs, rhs []ast.Expr,
	file string,
	fset *token.FileSet,
	imports fileImports,
	sigs map[string]funcSignature,
	idx moduleIndex,
	result *coverageResult,
	bindings map[string]string,
) {
	if len(rhs) == 1 && len(lhs) >= 1 {
		modules, paginatorModule, paginatorOp := classifyRHS(rhs[0], imports, sigs)
		if paginatorModule != "" {
			result.record(idx, paginatorModule, paginatorOp, file, fset.Position(rhs[0].Pos()).Line)

			return
		}
		if len(lhs) == len(modules) || (len(lhs) == 1 && len(modules) >= 1) {
			applyBindings(lhs, modules, bindings)

			return
		}
	}

	if len(lhs) == len(rhs) {
		for i, r := range rhs {
			modules, paginatorModule, paginatorOp := classifyRHS(r, imports, sigs)
			if paginatorModule != "" {
				result.record(idx, paginatorModule, paginatorOp, file, fset.Position(r.Pos()).Line)

				continue
			}
			if len(modules) >= 1 {
				applyBindings(lhs[i:i+1], modules[:1], bindings)
			}
		}
	}
}

func applyBindings(lhs []ast.Expr, modules funcSignature, bindings map[string]string) {
	for i, l := range lhs {
		if i >= len(modules) || modules[i] == "" {
			continue
		}
		ident, ok := l.(*ast.Ident)
		if !ok || ident.Name == "_" {
			continue
		}
		bindings[ident.Name] = modules[i]
	}
}

// findTestFiles groups every _test.go file under root by its containing
// directory, since Go identifier resolution (a helper function shared
// across files) never crosses a directory boundary.
func findTestFiles(root string) (map[string][]string, error) {
	groups := map[string][]string{}

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		dir := filepath.Dir(path)
		groups[dir] = append(groups[dir], path)

		return nil
	})

	return groups, err
}

func censusDir(dir string, files []string, idx moduleIndex, result *coverageResult) error {
	fset := token.NewFileSet()

	astFiles := make([]*ast.File, 0, len(files))
	importsOf := map[*ast.File]fileImports{}
	fileNameOf := map[*ast.File]string{}

	for _, path := range files {
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return fmt.Errorf("parse %s: %w", path, err)
		}
		astFiles = append(astFiles, f)
		importsOf[f] = parseSDKImports(f)
		fileNameOf[f] = path
	}

	sigs := collectFuncSignatures(astFiles, importsOf)

	for _, f := range astFiles {
		imports := importsOf[f]
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			entryParams := extractClientParams(fd.Type, imports)
			walkFunc(fd.Body, entryParams, fileNameOf[f], fset, imports, sigs, idx, result)
		}
	}

	_ = dir

	return nil
}

func run(roots []string, services []opService) (*coverageResult, error) {
	idx := buildModuleIndex(services)
	result := newCoverageResult()

	for _, root := range roots {
		groups, err := findTestFiles(root)
		if err != nil {
			return nil, fmt.Errorf("walk %s: %w", root, err)
		}

		dirs := make([]string, 0, len(groups))
		for d := range groups {
			dirs = append(dirs, d)
		}
		sort.Strings(dirs)

		for _, dir := range dirs {
			if censusErr := censusDir(dir, groups[dir], idx, result); censusErr != nil {
				return nil, censusErr
			}
		}
	}

	return result, nil
}

type serviceCoverage struct {
	Service      string   `json:"service"`
	UncoveredOps []string `json:"uncoveredOps"`
	Total        int      `json:"total"`
	Covered      int      `json:"covered"`
}

type report struct {
	Services   []serviceCoverage `json:"services"`
	Ambiguous  []string          `json:"ambiguous"`
	TotalOps   int               `json:"totalOps"`
	CoveredOps int               `json:"coveredOps"`
}

func buildReport(services []opService, result *coverageResult) report {
	rpt := report{Ambiguous: result.ambiguous}

	for _, s := range services {
		covered := result.covered[s.Service]

		var uncovered []string
		for _, op := range s.AllOps {
			if _, ok := covered[op]; !ok {
				uncovered = append(uncovered, op)
			}
		}
		sort.Strings(uncovered)

		rpt.Services = append(rpt.Services, serviceCoverage{
			Service:      s.Service,
			Total:        len(s.AllOps),
			Covered:      len(s.AllOps) - len(uncovered),
			UncoveredOps: uncovered,
		})
		rpt.TotalOps += len(s.AllOps)
		rpt.CoveredOps += len(s.AllOps) - len(uncovered)
	}

	sort.Slice(rpt.Services, func(i, j int) bool {
		ui := len(rpt.Services[i].UncoveredOps)
		uj := len(rpt.Services[j].UncoveredOps)
		if ui != uj {
			return ui > uj
		}

		return rpt.Services[i].Service < rpt.Services[j].Service
	})

	return rpt
}

func printReport(w io.Writer, rpt report, top int) {
	pct := 0.0
	if rpt.TotalOps > 0 {
		pct = percentScale * float64(rpt.CoveredOps) / float64(rpt.TotalOps)
	}

	fmt.Fprintf(
		w,
		"typed-client coverage: %d / %d ops (%.1f%%) invoked by a real aws-sdk-go-v2 client\n\n",
		rpt.CoveredOps,
		rpt.TotalOps,
		pct,
	)
	fmt.Fprintf(w, "%-28s %8s %8s %8s %8s\n", "service", "total", "covered", "gap", "pct")

	for i, s := range rpt.Services {
		if i >= top {
			break
		}
		spct := 0.0
		if s.Total > 0 {
			spct = percentScale * float64(s.Covered) / float64(s.Total)
		}
		fmt.Fprintf(
			w,
			"%-28s %8d %8d %8d %7.1f%%\n",
			s.Service,
			s.Total,
			s.Covered,
			s.Total-s.Covered,
			spct,
		)
	}

	if len(rpt.Ambiguous) > 0 {
		fmt.Fprintf(
			w,
			"\n%d ambiguous call(s) (module maps to >1 candidate service by op name; not counted):\n",
			len(rpt.Ambiguous),
		)
		for _, a := range rpt.Ambiguous {
			fmt.Fprintf(w, "  %s\n", a)
		}
	}
}

func loadOpcensus(path string) ([]opService, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var services []opService
	if unmarshalErr := json.Unmarshal(data, &services); unmarshalErr != nil {
		return nil, fmt.Errorf("parse %s: %w", path, unmarshalErr)
	}

	return services, nil
}

func main() {
	opcensusPath := flag.String("opcensus", "", "path to cmd/opcensus -json output (required)")
	rootsFlag := flag.String(
		"roots",
		"test/integration,services",
		"comma-separated directories to scan for _test.go files",
	)
	jsonPath := flag.String("json", "", "write full per-service detail to this path")
	top := flag.Int("top", 25, "number of worst services to print")
	flag.Parse()

	if *opcensusPath == "" {
		fmt.Fprintln(
			os.Stderr,
			"-opcensus is required (run: go run ./cmd/opcensus -json <path> first)",
		)
		os.Exit(1)
	}

	services, err := loadOpcensus(*opcensusPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "load opcensus json:", err)
		os.Exit(1)
	}

	roots := strings.Split(*rootsFlag, ",")
	result, err := run(roots, services)
	if err != nil {
		fmt.Fprintln(os.Stderr, "scan:", err)
		os.Exit(1)
	}

	rpt := buildReport(services, result)

	if *jsonPath != "" {
		data, marshalErr := json.MarshalIndent(rpt, "", "  ")
		if marshalErr != nil {
			fmt.Fprintln(os.Stderr, "marshal report:", marshalErr)
			os.Exit(1)
		}
		if writeErr := os.WriteFile(*jsonPath, data, 0o600); writeErr != nil {
			fmt.Fprintln(os.Stderr, "write report:", writeErr)
			os.Exit(1)
		}
	}

	printReport(os.Stdout, rpt, *top)
}
