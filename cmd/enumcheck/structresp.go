package main

import (
	"go/ast"
	"go/token"
	"reflect"
	"strconv"
	"strings"
)

// collectStructFields parses every top-level `type X struct { ... }` in
// files and returns, per struct type name, a map from Go field name to that
// field's wire name -- the name it actually serializes under, which this
// repo's convention (json:"WireName" on every response-struct field) makes
// different from the Go identifier more often than not. Identity is kept
// per TYPE, not a bare field name: two struct types that both happen to
// declare a "Status" field resolve independently through separate map
// entries, so a lookup by (type, field) can never confuse them the same way
// localFieldConsts's (variable, field) keying already avoids that collision
// for the map[string]any path.
func collectStructFields(files []*ast.File) map[string]map[string]string {
	out := map[string]map[string]string{}

	for _, f := range files {
		for _, decl := range f.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.TYPE {
				continue
			}

			for _, spec := range gd.Specs {
				addStructTypeSpec(spec, out)
			}
		}
	}

	return out
}

func addStructTypeSpec(spec ast.Spec, out map[string]map[string]string) {
	ts, ok := spec.(*ast.TypeSpec)
	if !ok {
		return
	}

	st, ok := ts.Type.(*ast.StructType)
	if !ok || st.Fields == nil {
		return
	}

	fields := map[string]string{}

	for _, field := range st.Fields.List {
		collectStructFieldWireNames(field, fields)
	}

	if len(fields) > 0 {
		out[ts.Name.Name] = fields
	}
}

// collectStructFieldWireNames resolves one struct field's wire name(s) into
// fields, keyed by Go field name. An embedded field (no Names) is skipped --
// resolving a promoted field's wire name would need to look outside this
// single field, one hop further than the rest of this scan reaches.
func collectStructFieldWireNames(field *ast.Field, fields map[string]string) {
	if len(field.Names) == 0 {
		return
	}

	for _, name := range field.Names {
		if !name.IsExported() {
			continue
		}

		if wireName, ok := fieldWireName(field, name.Name); ok {
			fields[name.Name] = wireName
		}
	}
}

// fieldWireName is the Go field's real wire name: a `json` tag if present,
// else an `xml` tag, else the Go field name itself -- encoding/json's own
// default when a field carries no tag at all. Reading the tag rather than
// assuming the field name IS the wire name matters: this repo's response
// structs tag every field explicitly, and the two are not always equal
// (e.g. Go field StatementID tagged json:"StatementId" in services/lambda).
// ok is false only for a field explicitly excluded via json:"-".
func fieldWireName(field *ast.Field, goName string) (string, bool) {
	if field.Tag == nil || len(field.Names) != 1 {
		return goName, true
	}

	tagVal, err := strconv.Unquote(field.Tag.Value)
	if err != nil {
		return goName, true
	}

	tag := reflect.StructTag(tagVal)

	if wire, present, excluded := tagWireName(tag, "json"); excluded {
		return "", false
	} else if present {
		return wire, true
	}

	if wire, present, excluded := tagWireName(tag, "xml"); excluded {
		return "", false
	} else if present {
		return wire, true
	}

	return goName, true
}

// tagWireName reads one struct tag key (json or xml) and splits off its
// name component from any trailing options (,omitempty / >Nested / ,attr).
// present is false when the tag key is absent or names nothing explicit
// (falls through to the Go field name); excluded is true only for the
// `key:"-"` convention that removes the field from the wire entirely.
func tagWireName(tag reflect.StructTag, key string) (string, bool, bool) {
	v, ok := tag.Lookup(key)
	if !ok {
		return "", false, false
	}

	name := v
	if idx := strings.IndexAny(v, ",>"); idx >= 0 {
		name = v[:idx]
	}

	if name == "-" {
		return "", false, true
	}

	return name, name != "", false
}

// checkStructResponsesInFunc is CONFIDENT check A's third sibling: a keyed
// field in a composite literal of a named struct type declared in this same
// package (bare `Type{...}` or pointer `&Type{...}` -- ast.Inspect reaches
// the inner CompositeLit either way, no unwrap needed) whose wire name is a
// known wire key. This is the response-struct blind spot the package doc
// documents (`c.JSON(http.StatusOK, SomeType{...})`): it is not gated on
// c.JSON at all, deliberately mirroring checkLiteralsInFunc, which likewise
// matches any map[string]any literal wherever it appears in the function,
// not only ones passed directly to a response writer -- consistent scope,
// not a new risk. Nested struct literals (a sub-struct field's own value)
// are reached automatically since ast.Inspect visits every CompositeLit,
// however deep. An unkeyed (positional) element is skipped outright: there
// is no field identity to resolve a wire name from without one.
func checkStructResponsesInFunc(
	fd *ast.FuncDecl, fset *token.FileSet, reg *enumRegistry,
	wireKeys map[string]wireKeyFact, localConsts, pkgConsts map[string]string,
	structFields map[string]map[string]string, repoRoot string,
) []finding {
	var out []finding

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		cl, ok := n.(*ast.CompositeLit)
		if !ok || cl.Type == nil {
			return true
		}

		typeIdent, ok := cl.Type.(*ast.Ident)
		if !ok {
			return true
		}

		fields, known := structFields[typeIdent.Name]
		if !known {
			return true
		}

		for _, elt := range cl.Elts {
			f, found := checkStructFieldElt(elt, fset, reg, wireKeys, localConsts, pkgConsts, fields, repoRoot)
			if found {
				out = append(out, f)
			}
		}

		return true
	})

	return out
}

func checkStructFieldElt(
	elt ast.Expr, fset *token.FileSet, reg *enumRegistry, wireKeys map[string]wireKeyFact,
	localConsts, pkgConsts map[string]string, fields map[string]string, repoRoot string,
) (finding, bool) {
	kv, ok := elt.(*ast.KeyValueExpr)
	if !ok {
		return finding{}, false
	}

	fieldIdent, ok := kv.Key.(*ast.Ident)
	if !ok {
		return finding{}, false
	}

	wireKey, known := fields[fieldIdent.Name]
	if !known {
		return finding{}, false
	}

	return evalKeyValue(wireKey, kv.Value, fset, reg, wireKeys, localConsts, pkgConsts, repoRoot)
}
