// Command routecollisions regenerates the RouteMatcher over-claim candidate
// list for gopherstack-op3e (see services/_ROUTE_COLLISIONS.md).
//
// gopherstack-op3e found that inspector2 and macie2 both claimed
// "/findings*" and "/members*" unconditionally in their RouteMatcher, and
// both register (cli.go's getServiceProviders chain) before securityhub --
// so pkgs/service/router.go, which evaluates matchers in priority order and
// takes the first that returns true, sent every securityhub findings/members
// request to the wrong service. Unit tests never caught it because they call
// h.Handler() directly, bypassing RouteMatcher and the router entirely. This
// tool asks the opposite of what a prior sweep (gopherstack-k9bl) asked --
// not "does each matcher accept its own paths" but "does it also accept
// paths that belong to somebody else."
//
// For every services/<dir>, it parses every non-test .go file with go/ast,
// locates each RouteMatcher() service.Matcher method (there can be more than
// one per package: bedrock's agents dispatcher, redshift's serverless
// handler, s3/dynamodb's differently-named receivers), and extracts the
// path-prefix string literals it tests against the request path -- both
// literal ("/findings/") and package-const-resolved ("/"+pathAnalyzer). It
// also resolves each service's MatchPriority() and its registration order
// from cli.go's getServiceProviders chain, and flags whether the matcher
// already gates any of its claims behind a SigV4 signing-service check
// (httputils.ExtractServiceFromRequest, or a local isXRequest helper) --
// the established disambiguation pattern (securityhub, mediapackage, iot,
// managedblockchain, ...).
//
// This is a CANDIDATE list only, built by text/regex extraction over a
// RouteMatcher body -- it does not simulate the router. A flagged
// "collision" still needs a human read of both services' matchers (prefix
// vs. exact, nested-path narrowing, an existing guard that isn't reflected
// in this coarse per-service "guarded" bit) before it is a confirmed bug --
// see services/_ROUTE_COLLISIONS.md for the triage already done.
//
// Usage:
//
//	go run ./cmd/routecollisions            # ranked collision summary to stdout
//	go run ./cmd/routecollisions -json out.json   # full per-service claim detail
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
	"sort"
	"strconv"
	"strings"
)

const cliGoPath = "cli.go"

func main() {
	jsonOut := flag.String("json", "", "write full per-service claim detail to this path as JSON")
	flag.Parse()

	results, err := run()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if *jsonOut != "" {
		if writeErr := writeJSONReport(*jsonOut, results); writeErr != nil {
			fmt.Fprintln(os.Stderr, "write json out:", writeErr)
			os.Exit(1)
		}
	}

	printCollisionReport(results)
}

func run() ([]svcInfo, error) {
	root, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("getwd: %w", err)
	}

	cliSrc, err := os.ReadFile(filepath.Join(root, cliGoPath))
	if err != nil {
		return nil, fmt.Errorf("read cli.go: %w", err)
	}

	aliasToDir := parseAliasToDir(cliSrc)
	regOrder := parseRegOrder(cliSrc, aliasToDir)

	priorityConsts, err := parsePriorityConsts(filepath.Join(root, "pkgs", "service", "priorities.go"))
	if err != nil {
		return nil, fmt.Errorf("parse priorities.go: %w", err)
	}

	dirs, err := listServiceDirs(filepath.Join(root, "services"))
	if err != nil {
		return nil, fmt.Errorf("list services: %w", err)
	}

	return analyzeAllDirs(root, dirs, regOrder, priorityConsts), nil
}

func analyzeAllDirs(root string, dirs []string, regOrder, priorityConsts map[string]int) []svcInfo {
	var results []svcInfo

	for _, dir := range dirs {
		infos, analyzeErr := analyzeDir(filepath.Join(root, "services", dir), dir, priorityConsts)
		if analyzeErr != nil {
			fmt.Fprintf(os.Stderr, "analyze %s: %v\n", dir, analyzeErr)

			continue
		}

		for i := range infos {
			infos[i].RegOrder = regOrder[dir]
		}

		results = append(results, infos...)
	}

	sort.SliceStable(results, func(i, j int) bool {
		if results[i].Priority != results[j].Priority {
			return results[i].Priority > results[j].Priority
		}

		return results[i].RegOrder < results[j].RegOrder
	})

	return results
}

func writeJSONReport(path string, results []svcInfo) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")

	return enc.Encode(results)
}

func listServiceDirs(servicesDir string) ([]string, error) {
	entries, err := os.ReadDir(servicesDir)
	if err != nil {
		return nil, err
	}

	var dirs []string

	for _, e := range entries {
		if e.IsDir() {
			dirs = append(dirs, e.Name())
		}
	}

	sort.Strings(dirs)

	return dirs, nil
}

func parseAliasToDir(cliSrc []byte) map[string]string {
	out := map[string]string{}

	for _, m := range importAliasRe.FindAllStringSubmatch(string(cliSrc), -1) {
		out[m[1]] = m[2]
	}

	return out
}

// parseRegOrder scans cli.go in file order for &alias.XxxProvider{} references
// and assigns each directory the index of its FIRST such reference, which is
// its effective registration order for router.go's SliceStable priority sort.
func parseRegOrder(cliSrc []byte, aliasToDir map[string]string) map[string]int {
	out := map[string]int{}
	idx := 0

	for _, m := range providerRefRe.FindAllStringSubmatch(string(cliSrc), -1) {
		dir, ok := aliasToDir[m[1]]
		if !ok {
			continue
		}

		if _, seen := out[dir]; !seen {
			out[dir] = idx
			idx++
		}
	}

	return out
}

func parsePriorityConsts(path string) (map[string]int, error) {
	src, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	out := map[string]int{}

	for _, m := range priorityRe.FindAllStringSubmatch(string(src), -1) {
		n, convErr := strconv.Atoi(m[2])
		if convErr != nil {
			continue
		}

		out[m[1]] = n
	}

	return out, nil
}

// pkgData accumulates everything analyzeDir needs across every non-test .go
// file in a single services/<dir> package: the const/priority/slice lookup
// tables extracted from source, every RouteMatcher method found, and the raw
// source bytes each was found in (bodyText needs the exact file it came
// from, since multiple files in one dir can each declare their own
// RouteMatcher, e.g. redshift's serverless handler).
type pkgData struct {
	fset              *token.FileSet
	consts            map[string]string
	selectorConsts    map[string]string
	intConsts         map[string]int
	sliceConsts       map[string][]string
	srcByFile         map[string][]byte
	matchPriorityBody string
	routeMatchers     []*ast.FuncDecl
}

// analyzeDir parses every non-test .go file in a service directory, builds a
// package-wide string-const table, then finds every RouteMatcher method and
// extracts the path claims from its body text.
func analyzeDir(dir, name string, priorityConsts map[string]int) ([]svcInfo, error) {
	pd, err := parsePackage(dir)
	if err != nil {
		return nil, err
	}

	if len(pd.routeMatchers) == 0 {
		return nil, nil
	}

	priority := resolvePriority(pd.matchPriorityBody, pd.selectorConsts, pd.intConsts, priorityConsts)

	return buildServiceInfos(pd, name, priority), nil
}

func parsePackage(dir string) (*pkgData, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	pd := &pkgData{
		fset:           token.NewFileSet(),
		consts:         map[string]string{},
		selectorConsts: map[string]string{},
		intConsts:      map[string]int{},
		srcByFile:      map[string][]byte{},
	}

	var pkgSrc strings.Builder

	for _, fe := range files {
		if !isPackageGoFile(fe) {
			continue
		}

		if parseErr := parsePackageFile(pd, dir, fe.Name(), &pkgSrc); parseErr != nil {
			return nil, parseErr
		}
	}

	pd.sliceConsts = extractSliceConsts(pkgSrc.String())

	return pd, nil
}

func isPackageGoFile(fe os.DirEntry) bool {
	return !fe.IsDir() && strings.HasSuffix(fe.Name(), ".go") && !strings.HasSuffix(fe.Name(), "_test.go")
}

func parsePackageFile(pd *pkgData, dir, name string, pkgSrc *strings.Builder) error {
	fp := filepath.Join(dir, name)

	src, err := os.ReadFile(fp)
	if err != nil {
		return err
	}

	pd.srcByFile[fp] = src
	pkgSrc.Write(src)
	pkgSrc.WriteByte('\n')

	f, err := parser.ParseFile(pd.fset, fp, src, 0)
	if err != nil {
		return fmt.Errorf("parse %s: %w", fp, err)
	}

	collectConsts(f, pd.consts, pd.selectorConsts, pd.intConsts)
	collectRouteMatcherFuncs(pd, f, src)

	return nil
}

func collectRouteMatcherFuncs(pd *pkgData, f *ast.File, src []byte) {
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv == nil {
			continue
		}

		switch fn.Name.Name {
		case "RouteMatcher":
			pd.routeMatchers = append(pd.routeMatchers, fn)
		case "MatchPriority":
			if fn.Body != nil {
				pd.matchPriorityBody = bodyText(pd.fset, src, fn.Body)
			}
		}
	}
}

func extractSliceConsts(pkgSrc string) map[string][]string {
	out := map[string][]string{}

	for _, m := range sliceLitRe.FindAllStringSubmatch(pkgSrc, -1) {
		matches := quotedRe.FindAllStringSubmatch(m[2], -1)
		elems := make([]string, 0, len(matches))

		for _, qm := range matches {
			elems = append(elems, qm[1])
		}

		if len(elems) > 0 {
			out[m[1]] = elems
		}
	}

	return out
}

func buildServiceInfos(pd *pkgData, name string, priority int) []svcInfo {
	var out []svcInfo

	for _, fn := range pd.routeMatchers {
		fp := pd.fset.Position(fn.Pos()).Filename

		body := bodyText(pd.fset, pd.srcByFile[fp], fn.Body)
		claims := extractClaims(body, pd.consts, pd.sliceConsts)

		if len(claims) == 0 {
			isQueryProtocol := queryProtocolContentTypeRe.MatchString(body) && queryProtocolVersionRe.MatchString(body)
			if isQueryProtocol {
				out = append(out, svcInfo{Dir: name, Priority: priority, Immune: true})
			}

			continue
		}

		out = append(out, svcInfo{
			Dir:      name,
			Priority: priority,
			Guarded:  guardRe.MatchString(body),
			Claims:   claims,
		})
	}

	return out
}

func bodyText(fset *token.FileSet, src []byte, body *ast.BlockStmt) string {
	start := fset.Position(body.Pos()).Offset
	end := fset.Position(body.End()).Offset

	if start < 0 || end > len(src) || start > end {
		return ""
	}

	return string(src[start:end])
}
