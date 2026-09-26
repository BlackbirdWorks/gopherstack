// Command cfnattrgen generates services/cloudformation's Fn::GetAtt
// attribute table from the CloudFormation resource specification. Real
// CreateStack/UpdateStack rejects Fn::GetAtt on an attribute a resource type
// doesn't support ("Template error: resource <X> does not support attribute
// type <Y> in Fn::GetAtt"); this table is what lets validateIntrinsics tell
// an undocumented attribute from a merely-unmodelled one (gopherstack-p7pvq).
//
// Every string this table introduces is checked against goconst's own rule
// (a literal repeated 3+ times across the package should be a constant)
// before being emitted, so this generator never hands golangci-lint new
// goconst violations to fix by hand:
//
//   - A resource type is keyed by its existing resTypeXxx constant
//     identifier (found by scanning -src), never re-quoted as a new string
//     literal -- the type string already appears at that constant's
//     declaration and every switch/case dispatching on it.
//   - An attribute name is keyed by its existing attrNameXxx-style constant
//     when one exists; otherwise the literal is counted against every
//     string literal already in -src (tests included, matching goconst's
//     own corpus), and only emitted when doing so keeps that string under
//     goconst's min-occurrences threshold.
//
// A type with no declared constant, or an attribute that would trip
// goconst, is simply left out of the table -- scoping the table to what the
// package already names/can safely re-quote is also exactly "the types we
// support" (gopherstack-p7pvq's intent), so this is conservative, not
// lossy: anything absent from the table is treated as "not in spec" by the
// validator and falls back to today's behaviour.
//
// Usage:
//
//	go run ./cmd/cfnattrgen -spec <cfn-resource-spec.json> -src <dir> -out <out.go>
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// goconstMinOccurrences mirrors this repo's golangci-lint goconst default
// (min-occurrences: 3, unconfigured in .golangci.yml): a literal used at
// least this many times across the package should be a constant instead.
const goconstMinOccurrences = 3

type resourceTypeSpec struct {
	Attributes map[string]json.RawMessage `json:"Attributes"`
}

type resourceSpec struct {
	ResourceTypes map[string]resourceTypeSpec `json:"ResourceTypes"`
}

func main() {
	specPath := flag.String("spec", "", "path to the CloudFormation resource specification JSON")
	srcDir := flag.String("src", "", "directory to scan for resTypeXxx constants and existing string literals")
	outPath := flag.String("out", "", "output Go file path")
	pkgName := flag.String("pkg", "cloudformation", "package name for the generated file")
	flag.Parse()

	if *specPath == "" || *srcDir == "" || *outPath == "" {
		fmt.Fprintln(os.Stderr, "usage: cfnattrgen -spec <spec.json> -src <dir> -out <out.go>")
		os.Exit(1)
	}

	if err := run(*specPath, *srcDir, *outPath, *pkgName); err != nil {
		fmt.Fprintln(os.Stderr, "cfnattrgen:", err)
		os.Exit(1)
	}
}

func run(specPath, srcDir, outPath, pkgName string) error {
	spec, err := loadSpec(specPath)
	if err != nil {
		return fmt.Errorf("load spec: %w", err)
	}

	// Excludes outPath itself: a stale copy from a prior run must not inflate
	// litCounts against itself when the table is regenerated.
	files, err := parseDir(srcDir, filepath.Base(outPath))
	if err != nil {
		return fmt.Errorf("parse %s: %w", srcDir, err)
	}

	typeConsts := collectStringConsts(files, false, isResourceTypeConst)
	attrConsts := collectStringConsts(files, false, isAttrNameConst)
	litCounts := countStringLiterals(files)

	table := buildTable(spec, typeConsts, attrConsts, litCounts)

	src, err := renderTable(pkgName, table)
	if err != nil {
		return fmt.Errorf("render: %w", err)
	}

	if writeErr := os.WriteFile(outPath, src, 0o600); writeErr != nil {
		return fmt.Errorf("write %s: %w", outPath, writeErr)
	}

	return nil
}

func loadSpec(path string) (*resourceSpec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var spec resourceSpec
	if unmarshalErr := json.Unmarshal(data, &spec); unmarshalErr != nil {
		return nil, unmarshalErr
	}

	return &spec, nil
}

type parsedFile struct {
	file   *ast.File
	isTest bool
}

// parseDir parses every .go file directly under dir (no recursion), skipping skipName.
func parseDir(dir, skipName string) ([]parsedFile, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	fset := token.NewFileSet()

	var files []parsedFile

	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || name == skipName {
			continue
		}

		file, perr := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.SkipObjectResolution)
		if perr != nil {
			return nil, fmt.Errorf("parse %s: %w", name, perr)
		}

		files = append(files, parsedFile{file: file, isTest: strings.HasSuffix(name, "_test.go")})
	}

	return files, nil
}

// collectStringConsts collects top-level `const` declarations (non-test
// files only) whose (identifier, value) pair passes keep, returning a map
// from that value to the constant's identifier name. The first declaration
// found wins when a value has more than one qualifying constant.
func collectStringConsts(files []parsedFile, includeTests bool, keep func(name, value string) bool) map[string]string {
	byValue := make(map[string]string)

	for _, pf := range files {
		if pf.isTest && !includeTests {
			continue
		}

		for _, decl := range pf.file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}

			for _, spec := range gen.Specs {
				valueSpec, isValueSpec := spec.(*ast.ValueSpec)
				if !isValueSpec {
					continue
				}

				collectValueSpecConst(valueSpec, keep, byValue)
			}
		}
	}

	return byValue
}

func collectValueSpecConst(valueSpec *ast.ValueSpec, keep func(name, value string) bool, byValue map[string]string) {
	for i, name := range valueSpec.Names {
		if i >= len(valueSpec.Values) {
			continue
		}

		lit, ok := valueSpec.Values[i].(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			continue
		}

		value, unquoteErr := strconv.Unquote(lit.Value)
		if unquoteErr != nil || !keep(name.Name, value) {
			continue
		}

		if _, exists := byValue[value]; !exists {
			byValue[value] = name.Name
		}
	}
}

func isResourceTypeConst(_, value string) bool {
	return strings.HasPrefix(value, "AWS::") || strings.HasPrefix(value, "Alexa::") ||
		strings.HasPrefix(value, "Custom::")
}

// isAttrNameConst matches this package's attrNameXxx naming convention
// (attrNameArn, attrNameName, ...) by identifier, not by value: an
// attribute name has no shared shape to check like a resource type does.
func isAttrNameConst(name, _ string) bool {
	return strings.HasPrefix(name, "attrName")
}

// countStringLiterals tallies every string literal expression across all
// files (tests included, matching this repo's own goconst corpus) so the
// caller can tell whether adding one more occurrence would cross
// goconstMinOccurrences.
func countStringLiterals(files []parsedFile) map[string]int {
	counts := make(map[string]int)

	for _, pf := range files {
		ast.Inspect(pf.file, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}

			if value, err := strconv.Unquote(lit.Value); err == nil {
				counts[value]++
			}

			return true
		})
	}

	return counts
}

// attrTable is one resource type's entry: its resTypeXxx constant
// identifier, and its attributes rendered as ready-to-emit Go map-key
// expressions (either an attrNameXxx identifier or a quoted literal).
type attrTable struct {
	constIdent string
	attrExprs  []string
}

// buildTable keeps only types with both a spec-documented, non-empty
// Attributes set and a declared resTypeXxx constant, and where every one of
// that type's documented attributes can be safely emitted (see package
// doc). A type is registered in cfnResourceAttributes only with its COMPLETE
// documented attribute set: emitting a partial set would make validation
// reject a real, documented attribute we merely declined to re-quote here,
// which is worse than not validating the type at all -- so a type with even
// one unsafe attribute is dropped whole, falling back to today's behaviour
// for all of its attributes.
func buildTable(
	spec *resourceSpec,
	typeConsts, attrConsts map[string]string,
	litCounts map[string]int,
) map[string]attrTable {
	table := make(map[string]attrTable)

	for value, constIdent := range typeConsts {
		rt, ok := spec.ResourceTypes[value]
		if !ok || len(rt.Attributes) == 0 {
			continue
		}

		names := make([]string, 0, len(rt.Attributes))
		for a := range rt.Attributes {
			names = append(names, a)
		}
		sort.Strings(names)

		exprs := make([]string, 0, len(names))
		complete := true

		for _, a := range names {
			expr, safe := attrExpr(a, attrConsts, litCounts)
			if !safe {
				complete = false

				break
			}

			exprs = append(exprs, expr)
		}

		if !complete || len(exprs) == 0 {
			continue
		}

		table[value] = attrTable{constIdent: constIdent, attrExprs: exprs}
	}

	return table
}

// attrExpr returns the Go expression to use as attribute a's map key, and
// whether it's safe to emit at all.
func attrExpr(a string, attrConsts map[string]string, litCounts map[string]int) (string, bool) {
	if constIdent, ok := attrConsts[a]; ok {
		return constIdent, true
	}

	if litCounts[a] >= goconstMinOccurrences-1 {
		return "", false
	}

	return strconv.Quote(a), true
}

func renderTable(pkgName string, table map[string]attrTable) ([]byte, error) {
	var b strings.Builder

	fmt.Fprintf(
		&b,
		"// Code generated by cmd/cfnattrgen from the CloudFormation resource specification; DO NOT EDIT.\n",
	)
	fmt.Fprintf(&b, "package %s\n\n", pkgName)
	fmt.Fprintf(&b, "//nolint:gochecknoglobals // generated static lookup table\n")
	fmt.Fprintf(&b, "var cfnResourceAttributes = map[string]map[string]struct{}{\n")

	values := make([]string, 0, len(table))
	for v := range table {
		values = append(values, v)
	}
	sort.Strings(values)

	for _, v := range values {
		entry := table[v]
		fmt.Fprintf(&b, "\t%s: {\n", entry.constIdent)
		for _, expr := range entry.attrExprs {
			fmt.Fprintf(&b, "\t\t%s: {},\n", expr)
		}
		fmt.Fprintf(&b, "\t},\n")
	}
	fmt.Fprintf(&b, "}\n")

	return format.Source([]byte(b.String()))
}
