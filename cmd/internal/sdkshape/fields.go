// Package sdkshape parses a pinned aws-sdk-go-v2 service module's own
// generated source (api_op_*.go, types/types.go) into its declared
// request/response struct shapes: field name, declared Go type, and whether
// the SDK's doc comment marks it required. This is ground truth shared by
// cmd/structfielddiff (full field dumps for hand comparison) and
// cmd/enumcheck (resolving a gopherstack struct field to its one real SDK
// member type), so both read the exact same parse instead of two
// implementations drifting apart.
//
// The SDK's own generated types carry no encoding/json struct tags at all --
// the wire name for a JSON-family protocol field IS the Go field name -- so
// this needs no tag handling, only the field name and its declared type.
//
// Parsed with go/ast, not a line/brace-counting scan: an earlier version
// counted "{"/"}" per line including inside doc comments, and AWS's own doc
// comments routinely show unbalanced example braces (a JSON snippet, a
// mid-sentence "{"), which silently threw off the brace depth and skipped
// every struct declared after the first such comment in a file -- confirmed
// live against sagemaker@v1.263.2's types.go, which lost ~500 of ~1600 real
// structs (including TrainingJob itself) this way. go/ast can't miscount:
// a comment is trivia, never struct syntax.
package sdkshape

import (
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const requiredLine = "This member is required."

// Field is one declared field of a pinned SDK struct.
type Field struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
}

// StructDef is one pinned SDK "type X struct { ... }" declaration.
type StructDef struct {
	Name   string  `json:"name"`
	Fields []Field `json:"fields"`
}

// LoadModuleStructs reads every api_op_<Op>.go file (for op names and their
// Input/Output structs) and types/types.go (for nested structs) under
// modPath, returning every struct found keyed by bare name, plus the sorted
// list of operation names.
func LoadModuleStructs(modPath string) (map[string]StructDef, []string, error) {
	structs := map[string]StructDef{}

	opNames, err := collectOpFiles(modPath, structs)
	if err != nil {
		return nil, nil, err
	}

	typesFile := filepath.Join(modPath, "types", "types.go")
	if src, readErr := os.ReadFile(typesFile); readErr == nil {
		defs, perr := parseFile(typesFile, src)
		if perr != nil {
			return nil, nil, perr
		}

		maps.Copy(structs, defs)
	}

	sort.Strings(opNames)

	return structs, opNames, nil
}

func collectOpFiles(modPath string, structs map[string]StructDef) ([]string, error) {
	entries, err := os.ReadDir(modPath)
	if err != nil {
		return nil, err
	}

	var opNames []string

	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasPrefix(name, "api_op_") || !strings.HasSuffix(name, ".go") ||
			strings.HasSuffix(name, "_test.go") {
			continue
		}

		opName := strings.TrimSuffix(strings.TrimPrefix(name, "api_op_"), ".go")
		opNames = append(opNames, opName)

		path := filepath.Join(modPath, name)

		src, readErr := os.ReadFile(path)
		if readErr != nil {
			continue
		}

		defs, perr := parseFile(path, src)
		if perr != nil {
			return nil, perr
		}

		maps.Copy(structs, defs)
	}

	return opNames, nil
}

// parseFile finds every "type X struct { ... }" declared directly in src
// and returns each as a StructDef keyed by bare name X.
func parseFile(filename string, src []byte) (map[string]StructDef, error) {
	fset := token.NewFileSet()

	f, err := parser.ParseFile(fset, filename, src, parser.ParseComments)
	if err != nil {
		return nil, err
	}

	out := map[string]StructDef{}

	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.TYPE {
			continue
		}

		for _, spec := range gd.Specs {
			addTypeSpec(fset, spec, out)
		}
	}

	return out, nil
}

func addTypeSpec(fset *token.FileSet, spec ast.Spec, out map[string]StructDef) {
	ts, ok := spec.(*ast.TypeSpec)
	if !ok {
		return
	}

	st, ok := ts.Type.(*ast.StructType)
	if !ok || st.Fields == nil {
		return
	}

	var fields []Field

	for _, field := range st.Fields.List {
		fields = append(fields, structFields(fset, field)...)
	}

	out[ts.Name.Name] = StructDef{Name: ts.Name.Name, Fields: fields}
}

// structFields returns field's own declared Field(s) -- normally one, or
// none for an embedded marker field (no Names, e.g. the generated
// noSmithyDocumentSerde) this codegen shape never declares by more than one
// name at a time in practice, but the AST allows it (`A, B string`).
func structFields(fset *token.FileSet, field *ast.Field) []Field {
	if len(field.Names) == 0 {
		return nil
	}

	typeStr := exprText(fset, field.Type)
	required := field.Doc != nil && strings.Contains(field.Doc.Text(), requiredLine)

	fields := make([]Field, 0, len(field.Names))

	for _, n := range field.Names {
		fields = append(fields, Field{Name: n.Name, Type: typeStr, Required: required})
	}

	return fields
}

func exprText(fset *token.FileSet, e ast.Expr) string {
	var sb strings.Builder
	if err := format.Node(&sb, fset, e); err != nil {
		return ""
	}

	return sb.String()
}

// BareTypeName strips pointer/slice/map decoration and a "types." or other
// package-qualifier prefix, returning the identifier a struct lookup uses
// (e.g. "*types.ClusterStatus" -> "ClusterStatus", "[]*types.Tag" -> "Tag",
// "map[string]*string" -> "string").
func BareTypeName(t string) string {
	t = strings.TrimPrefix(t, "*")
	t = strings.TrimPrefix(t, "[]")
	t = strings.TrimPrefix(t, "*")

	if strings.HasPrefix(t, "map[") {
		if idx := strings.Index(t, "]"); idx != -1 {
			t = t[idx+1:]
		}

		t = strings.TrimPrefix(t, "*")
	}

	if idx := strings.LastIndex(t, "."); idx != -1 {
		t = t[idx+1:]
	}

	return t
}
