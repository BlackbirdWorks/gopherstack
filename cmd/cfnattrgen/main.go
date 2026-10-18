// Command cfnattrgen generates services/cloudformation's Fn::GetAtt
// attribute table from the CloudFormation resource specification. Real
// CreateStack/UpdateStack rejects Fn::GetAtt on an attribute a resource type
// doesn't support ("Template error: resource <X> does not support attribute
// type <Y> in Fn::GetAtt"); this table is what lets validateIntrinsics tell
// an undocumented attribute from a merely-unmodelled one (gopherstack-p7pvq).
//
// The table is emitted as JSON (services/cloudformation/cfn_attributes.json),
// not Go source: goconst counts string literals package-wide, and a
// generated .go file's literals still push hand-written files over the
// min-occurrences threshold even when the generated file itself is excluded
// from lint reporting (verified empirically -- see gopherstack-erj2j). A
// resource type is included only when the package declares a resTypeXxx
// constant for it (the provisioner's supported-types surface) and the spec
// documents a non-empty Attributes set for it; every documented attribute is
// emitted, since there's no literal-count reason left to drop any.
//
// Usage:
//
//	go run ./cmd/cfnattrgen -spec <cfn-resource-spec.json> -src <dir> -out <out.json>
//
// Or, to refresh the trimmed spec fixture used for -spec from a fresh
// download of the full CloudFormation resource specification:
//
//	go run ./cmd/cfnattrgen -spec <full-spec.json> -trimspec-out <fixture.json>
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

type resourceTypeSpec struct {
	Attributes map[string]json.RawMessage `json:"Attributes"`
}

type resourceSpec struct {
	ResourceTypes map[string]resourceTypeSpec `json:"ResourceTypes"`
}

func main() {
	specPath := flag.String("spec", "", "path to the CloudFormation resource specification JSON")
	srcDir := flag.String("src", "", "directory to scan for resTypeXxx constants")
	outPath := flag.String("out", "", "output attribute-table JSON file path")
	servicesOut := flag.String(
		"services-out", "",
		"also write the sorted list of known CloudFormation service namespaces here",
	)
	trimSpecOut := flag.String(
		"trimspec-out", "",
		"write a trimmed (types+attribute names only) copy of -spec here instead of generating the table",
	)
	typesOut := flag.String(
		"types-out", "",
		"also write the sorted list of resource types the provisioner references (Cloud Control coverage) here",
	)
	flag.Parse()

	if *trimSpecOut != "" {
		if *specPath == "" {
			fmt.Fprintln(os.Stderr, "usage: cfnattrgen -spec <full-spec.json> -trimspec-out <fixture.json>")
			os.Exit(1)
		}

		if err := runTrimSpec(*specPath, *trimSpecOut); err != nil {
			fmt.Fprintln(os.Stderr, "cfnattrgen:", err)
			os.Exit(1)
		}

		return
	}

	if *specPath == "" || *srcDir == "" || *outPath == "" {
		fmt.Fprintln(os.Stderr, "usage: cfnattrgen -spec <spec.json> -src <dir> -out <out.json>")
		os.Exit(1)
	}

	if err := run(*specPath, *srcDir, *outPath, *servicesOut, *typesOut); err != nil {
		fmt.Fprintln(os.Stderr, "cfnattrgen:", err)
		os.Exit(1)
	}
}

func run(specPath, srcDir, outPath, servicesOut, typesOut string) error {
	spec, err := loadSpec(specPath)
	if err != nil {
		return fmt.Errorf("load spec: %w", err)
	}

	files, err := parseDir(srcDir)
	if err != nil {
		return fmt.Errorf("parse %s: %w", srcDir, err)
	}

	supportedTypes := collectResourceTypeConsts(files)

	table := buildTable(spec, supportedTypes)

	out, err := renderJSON(table)
	if err != nil {
		return fmt.Errorf("render: %w", err)
	}

	if writeErr := os.WriteFile(outPath, out, 0o600); writeErr != nil {
		return fmt.Errorf("write %s: %w", outPath, writeErr)
	}

	if servicesOut != "" {
		svcJSON, svcErr := renderServices(buildServices(spec, collectResourceTypeLiterals(files)))
		if svcErr != nil {
			return fmt.Errorf("render services: %w", svcErr)
		}

		if writeErr := os.WriteFile(servicesOut, svcJSON, 0o600); writeErr != nil {
			return fmt.Errorf("write %s: %w", servicesOut, writeErr)
		}
	}

	if typesOut == "" {
		return nil
	}

	typesJSON, err := renderServices(buildProvisionableTypes(collectResourceTypeLiterals(files)))
	if err != nil {
		return fmt.Errorf("render types: %w", err)
	}

	if writeErr := os.WriteFile(typesOut, typesJSON, 0o600); writeErr != nil {
		return fmt.Errorf("write %s: %w", typesOut, writeErr)
	}

	return nil
}

// runTrimSpec strips the full CloudFormation resource specification down to
// just the ResourceTypes/Attributes this generator reads, so a fixture small
// enough to commit can be refreshed from a fresh download without hand
// editing.
func runTrimSpec(specPath, outPath string) error {
	spec, err := loadSpec(specPath)
	if err != nil {
		return fmt.Errorf("load spec: %w", err)
	}

	trimmed := resourceSpec{ResourceTypes: make(map[string]resourceTypeSpec, len(spec.ResourceTypes))}

	for name, rt := range spec.ResourceTypes {
		if len(rt.Attributes) == 0 {
			continue
		}

		attrs := make(map[string]json.RawMessage, len(rt.Attributes))
		for attr := range rt.Attributes {
			attrs[attr] = json.RawMessage("{}")
		}

		trimmed.ResourceTypes[name] = resourceTypeSpec{Attributes: attrs}
	}

	out, err := json.MarshalIndent(trimmed, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal trimmed spec: %w", err)
	}

	out = append(out, '\n')

	if writeErr := os.WriteFile(outPath, out, 0o600); writeErr != nil {
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

// parseDir parses every non-test .go file directly under dir (no recursion).
func parseDir(dir string) ([]*ast.File, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	fset := token.NewFileSet()

	var files []*ast.File

	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}

		file, perr := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.SkipObjectResolution)
		if perr != nil {
			return nil, fmt.Errorf("parse %s: %w", name, perr)
		}

		files = append(files, file)
	}

	return files, nil
}

// collectResourceTypeConsts returns the set of CloudFormation resource type
// strings named by a top-level resTypeXxx constant anywhere in files -- the
// provisioner's supported-types surface.
func collectResourceTypeConsts(files []*ast.File) map[string]struct{} {
	types := make(map[string]struct{})

	for _, file := range files {
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}

			for _, spec := range gen.Specs {
				collectConstResourceType(spec, types)
			}
		}
	}

	return types
}

func collectConstResourceType(spec ast.Spec, types map[string]struct{}) {
	valueSpec, ok := spec.(*ast.ValueSpec)
	if !ok {
		return
	}

	for _, val := range valueSpec.Values {
		lit, isBasicLit := val.(*ast.BasicLit)
		if !isBasicLit || lit.Kind != token.STRING {
			continue
		}

		value, err := strconv.Unquote(lit.Value)
		if err != nil || !isResourceTypeValue(value) {
			continue
		}

		types[value] = struct{}{}
	}
}

func isResourceTypeValue(value string) bool {
	return strings.HasPrefix(value, "AWS::") || strings.HasPrefix(value, "Alexa::") ||
		strings.HasPrefix(value, "Custom::")
}

// buildTable keeps every supported type (declared via a resTypeXxx constant)
// that the spec documents a non-empty Attributes set for, with its complete
// documented attribute set.
func buildTable(spec *resourceSpec, supportedTypes map[string]struct{}) map[string][]string {
	table := make(map[string][]string, len(supportedTypes))

	for value := range supportedTypes {
		rt, ok := spec.ResourceTypes[value]
		if !ok || len(rt.Attributes) == 0 {
			continue
		}

		names := make([]string, 0, len(rt.Attributes))
		for a := range rt.Attributes {
			names = append(names, a)
		}

		sort.Strings(names)
		table[value] = names
	}

	return table
}

func renderJSON(table map[string][]string) ([]byte, error) {
	out, err := json.MarshalIndent(table, "", "  ")
	if err != nil {
		return nil, err
	}

	return append(out, '\n'), nil
}

// collectResourceTypeLiterals returns every resource-type string literal
// anywhere in files, not only resTypeXxx constants.
func collectResourceTypeLiterals(files []*ast.File) map[string]struct{} {
	types := make(map[string]struct{})

	for _, file := range files {
		ast.Inspect(file, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}

			if value, err := strconv.Unquote(lit.Value); err == nil && isResourceTypeValue(value) {
				types[value] = struct{}{}
			}

			return true
		})
	}

	return types
}

// buildServices returns the sorted "AWS::<Service>" namespaces named by the
// spec or by any literal the provisioner references.
func buildServices(spec *resourceSpec, literals map[string]struct{}) []string {
	set := make(map[string]struct{})

	add := func(typeName string) {
		parts := strings.Split(typeName, "::")
		if len(parts) >= minTypeNameParts && parts[0] == "AWS" && parts[1] != "" {
			set[parts[1]] = struct{}{}
		}
	}

	for name := range spec.ResourceTypes {
		add(name)
	}

	for name := range literals {
		add(name)
	}

	out := make([]string, 0, len(set))
	for svc := range set {
		out = append(out, svc)
	}

	sort.Strings(out)

	return out
}

const minTypeNameParts = 3

func renderServices(services []string) ([]byte, error) {
	out, err := json.MarshalIndent(services, "", "  ")
	if err != nil {
		return nil, err
	}

	return append(out, '\n'), nil
}

// buildProvisionableTypes returns the sorted three-segment AWS:: types the
// provisioner references, minus the CloudFormation-only families (nested
// stacks, custom resources, macros) and SAM's pre-transform types, none of
// which Cloud Control manages.
func buildProvisionableTypes(literals map[string]struct{}) []string {
	out := make([]string, 0, len(literals))

	for name := range literals {
		parts := strings.Split(name, "::")
		if len(parts) != minTypeNameParts || parts[0] != "AWS" || parts[2] == "" {
			continue
		}

		if parts[1] == "CloudFormation" || parts[1] == "Serverless" {
			continue
		}

		out = append(out, name)
	}

	sort.Strings(out)

	return out
}
