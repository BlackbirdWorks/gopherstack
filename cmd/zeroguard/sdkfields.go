package main

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
)

// scalarBaseTypes is every predeclared Go scalar identifier this repo uses
// to model a plain (non-pointer) wire field. A gopherstack field declared
// as one of these, where the real pinned SDK member is a pointer to the
// SAME identifier, is signal A.
//
//nolint:gochecknoglobals // read-only lookup table, same pattern as sibling cmd tools
var scalarBaseTypes = map[string]bool{
	"int32":   true,
	"int64":   true,
	"int":     true,
	"bool":    true,
	"string":  true,
	"float32": true,
	"float64": true,
}

// sdkInputField is one field of a real pinned SDK <Op>Input struct: its
// name, and whether it is a pointer to a predeclared scalar (with that
// scalar's identifier), read directly from api_op_<Op>.go via go/ast.
type sdkInputField struct {
	name            string
	baseType        string
	isPointerScalar bool
}

// sdkOpFieldCache memoizes loadSDKInputFields per (modPath, opName) pair, so
// re-scanning the same operation across services sharing an SDK module
// version parses the SDK source once.
type sdkOpFieldCache struct {
	cache map[string]map[string]sdkInputField
}

func newSDKOpFieldCache() *sdkOpFieldCache {
	return &sdkOpFieldCache{cache: map[string]map[string]sdkInputField{}}
}

// fieldsFor returns opName's real Input struct fields keyed by field name,
// or ok=false when modPath has no api_op_<opName>.go at all -- a normal,
// common outcome (wrong op-name guess, or this service's SDK module doesn't
// define this operation), never an error.
func (c *sdkOpFieldCache) fieldsFor(modPath, opName string) (map[string]sdkInputField, bool, error) {
	key := modPath + "\x00" + opName

	if fields, ok := c.cache[key]; ok {
		return fields, fields != nil, nil
	}

	fields, ok, err := loadSDKInputFields(modPath, opName)
	if err != nil {
		return nil, false, err
	}

	if ok {
		c.cache[key] = fields
	} else {
		c.cache[key] = nil
	}

	return fields, ok, nil
}

// loadSDKInputFields parses modPath/api_op_<opName>.go and returns the
// top-level fields of its "<opName>Input" struct declaration.
func loadSDKInputFields(modPath, opName string) (map[string]sdkInputField, bool, error) {
	path := filepath.Join(modPath, "api_op_"+opName+".go")

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

	st, ok := findStructType(f, opName+"Input")
	if !ok || st.Fields == nil {
		return nil, false, nil
	}

	fields := map[string]sdkInputField{}

	for _, field := range st.Fields.List {
		addSDKField(field, fields)
	}

	return fields, true, nil
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

func addSDKField(field *ast.Field, out map[string]sdkInputField) {
	if len(field.Names) == 0 {
		return
	}

	base, isPtrScalar := pointerScalarBase(field.Type)

	for _, id := range field.Names {
		out[id.Name] = sdkInputField{name: id.Name, baseType: base, isPointerScalar: isPtrScalar}
	}
}

// pointerScalarBase reports whether t is `*<predeclared scalar>` (e.g.
// *int32, *bool, *string) and, if so, the scalar's identifier.
func pointerScalarBase(t ast.Expr) (string, bool) {
	star, ok := t.(*ast.StarExpr)
	if !ok {
		return "", false
	}

	id, ok := star.X.(*ast.Ident)
	if !ok || !scalarBaseTypes[id.Name] {
		return "", false
	}

	return id.Name, true
}
