package main

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

// sdkOpFields is one real pinned SDK operation's Input member-name ground
// truth: the exact-cased names as declared, and the same set folded to
// lower case for case/abbreviation-tolerant matching (zeroguard's
// matchSDKField precedent -- AuthorizerResultTTLInSeconds vs.
// AuthorizerResultTtlInSeconds differ only in letter case).
type sdkOpFields struct {
	folded map[string]bool
}

// sdkFieldCache memoizes, per resolved SDK module path, a per-operation
// Input field set (fieldsFor, itself expanded through the module's own
// nested-struct/union ground truth -- sdktypes.go), the UNION of every
// operation's Input fields in that module (moduleFields, the ground truth
// for "this member name is real somewhere in this service, just not on the
// op examined" -- task's documented non-bug: a field that lives on a
// sibling or Create/Update-paired Input), and the module's own parsed
// types.go (typeFacts).
type sdkFieldCache struct {
	byOp   map[string]*sdkOpFields
	module map[string]map[string]bool
	types  map[string]*moduleTypeFacts
}

func newSDKFieldCache() *sdkFieldCache {
	return &sdkFieldCache{
		byOp: map[string]*sdkOpFields{}, module: map[string]map[string]bool{}, types: map[string]*moduleTypeFacts{},
	}
}

// fieldsFor returns opName's real Input field set from modPath, or ok=false
// when modPath has no api_op_<opName>.go -- a normal, common outcome (wrong
// op-name derivation, or this service's SDK module doesn't define this
// operation), never an error.
func (c *sdkFieldCache) fieldsFor(modPath, opName string) (*sdkOpFields, bool, error) {
	key := modPath + "\x00" + opName

	if f, ok := c.byOp[key]; ok {
		return f, f != nil, nil
	}

	fields, ok, err := loadInputStructFieldExprs(filepath.Join(modPath, "api_op_"+opName+".go"), opName+"Input")
	if err != nil {
		return nil, false, err
	}

	if !ok {
		c.byOp[key] = nil

		return nil, false, nil
	}

	facts, err := c.typeFacts(modPath)
	if err != nil {
		return nil, false, err
	}

	folded := map[string]bool{}
	for _, field := range fields {
		facts.expand(field.name, field.typeExpr, folded)
	}

	f := &sdkOpFields{folded: folded}
	c.byOp[key] = f

	return f, true, nil
}

// moduleFields returns the union of every api_op_*.go file's own "*Input"
// struct fields in modPath, folded to lower case. Computed once per modPath
// and cached -- a module directory holds every operation's own file, so this
// is a single directory scan regardless of how many operations get checked
// against it.
func (c *sdkFieldCache) moduleFields(modPath string) (map[string]bool, error) {
	if fields, ok := c.module[modPath]; ok {
		return fields, nil
	}

	entries, err := os.ReadDir(modPath)
	if err != nil {
		return nil, err
	}

	fields := map[string]bool{}

	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), "api_op_") || !strings.HasSuffix(e.Name(), ".go") {
			continue
		}

		names, ok, loadErr := loadAnyInputStructFields(filepath.Join(modPath, e.Name()))
		if loadErr != nil {
			return nil, loadErr
		}

		if !ok {
			continue
		}

		for _, n := range names {
			fields[strings.ToLower(n)] = true
		}
	}

	c.module[modPath] = fields

	return fields, nil
}

func foldSet(names []string) map[string]bool {
	out := make(map[string]bool, len(names))
	for _, n := range names {
		out[strings.ToLower(n)] = true
	}

	return out
}

// has reports whether name matches a real Input field, case/abbreviation
// insensitively (strings.ToLower fold, same tolerance as zeroguard's
// matchSDKField).
func (f *sdkOpFields) has(name string) bool {
	return f.folded[strings.ToLower(name)]
}

// sdkInputField is one real Input struct field's name and declared type
// expression -- the latter is what sdktypes.go's expand needs to flatten a
// nested struct or union member into the accepted-name set.
type sdkInputField struct {
	typeExpr ast.Expr
	name     string
}

// loadInputStructFieldExprs parses path and returns the top-level fields of
// its structName struct declaration, names and type expressions both.
func loadInputStructFieldExprs(path, structName string) ([]sdkInputField, bool, error) {
	if _, statErr := os.Stat(path); errors.Is(statErr, os.ErrNotExist) {
		return nil, false, nil
	} else if statErr != nil {
		return nil, false, statErr
	}

	fset := token.NewFileSet()

	f, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		return nil, false, err
	}

	st, ok := findStructType(f, structName)
	if !ok || st.Fields == nil {
		return nil, false, nil
	}

	var out []sdkInputField

	for _, field := range st.Fields.List {
		for _, id := range field.Names {
			out = append(out, sdkInputField{name: id.Name, typeExpr: field.Type})
		}
	}

	return out, true, nil
}

func isNotExist(err error) bool {
	return errors.Is(err, os.ErrNotExist)
}

// loadAnyInputStructFields parses path (one api_op_*.go file) and returns the
// field names of the first top-level struct type whose name ends "Input" --
// every such file declares exactly one.
func loadAnyInputStructFields(path string) ([]string, bool, error) {
	fset := token.NewFileSet()

	f, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		return nil, false, err
	}

	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.TYPE {
			continue
		}

		for _, spec := range gd.Specs {
			ts, isSpec := spec.(*ast.TypeSpec)
			if !isSpec || !strings.HasSuffix(ts.Name.Name, "Input") {
				continue
			}

			if st, isStruct := ts.Type.(*ast.StructType); isStruct && st.Fields != nil {
				return structFieldNames(st), true, nil
			}
		}
	}

	return nil, false, nil
}

func findStructType(f *ast.File, name string) (*ast.StructType, bool) {
	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.TYPE {
			continue
		}

		for _, spec := range gd.Specs {
			ts, isSpec := spec.(*ast.TypeSpec)
			if !isSpec || ts.Name.Name != name {
				continue
			}

			if st, isStruct := ts.Type.(*ast.StructType); isStruct {
				return st, true
			}
		}
	}

	return nil, false
}

func structFieldNames(st *ast.StructType) []string {
	var idents []*ast.Ident

	for _, field := range st.Fields.List {
		idents = append(idents, field.Names...)
	}

	out := make([]string, len(idents))
	for i, id := range idents {
		out[i] = id.Name
	}

	return out
}
