package main

import (
	"go/ast"
	"go/token"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
)

// emuField is one field of an emulator-declared struct, keyed for matching
// by its wire name (the json tag when present, else the Go field name).
type emuField struct {
	WireName string
	GoName   string
}

// structDef is one locally-declared struct type this scan can resolve
// emulator-declared fields for -- named types, anonymous inline `var req
// struct{...}` declarations, and single-hop type aliases. Adapted from
// cmd/reqfieldscan's identical collector (see that package's doc for why
// each shape is here); duplicated rather than imported so this tool never
// depends on, or risks modifying, cmd/reqfieldscan.
type structDef struct {
	Name   string
	Fields []emuField
}

func collectStructTypes(files []*ast.File, fset *token.FileSet) map[string]structDef {
	out := map[string]structDef{}

	var aliases []aliasSpec

	for _, f := range files {
		for _, decl := range f.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.TYPE {
				continue
			}

			for _, spec := range gd.Specs {
				addTypeSpec(spec, out, &aliases)
			}
		}
	}

	resolveStructAliases(aliases, out)
	collectAnonReqStructs(files, fset, out)

	return out
}

type aliasSpec struct {
	Name   string
	Target string
}

func addTypeSpec(spec ast.Spec, out map[string]structDef, aliases *[]aliasSpec) {
	ts, ok := spec.(*ast.TypeSpec)
	if !ok {
		return
	}

	switch t := ts.Type.(type) {
	case *ast.StructType:
		out[ts.Name.Name] = structDef{Name: ts.Name.Name, Fields: collectFields(t)}
	case *ast.Ident:
		*aliases = append(*aliases, aliasSpec{Name: ts.Name.Name, Target: t.Name})
	}
}

func resolveStructAliases(aliases []aliasSpec, out map[string]structDef) {
	for range aliases {
		changed := false

		for _, a := range aliases {
			if _, known := out[a.Name]; known {
				continue
			}

			if def, ok := out[a.Target]; ok {
				out[a.Name] = structDef{Name: a.Name, Fields: def.Fields}
				changed = true
			}
		}

		if !changed {
			break
		}
	}
}

// collectAnonReqStructs registers a request struct declared inline as `var
// req struct{...}` -- opsworks's shape, and omics's handleStartRun --
// keyed by file:line so it can be looked up again from a local-binding
// resolution pass by recomputing the identical key.
func collectAnonReqStructs(files []*ast.File, fset *token.FileSet, out map[string]structDef) {
	for _, f := range files {
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}

			ast.Inspect(fd.Body, func(n ast.Node) bool {
				vs, st, isAnon := anonStructVarSpec(n)
				if !isAnon {
					return true
				}

				name := anonStructName(fset, vs)
				out[name] = structDef{Name: name, Fields: collectFields(st)}

				return true
			})
		}
	}
}

func anonStructVarSpec(n ast.Node) (*ast.ValueSpec, *ast.StructType, bool) {
	ds, ok := n.(*ast.DeclStmt)
	if !ok {
		return nil, nil, false
	}

	gd, ok := ds.Decl.(*ast.GenDecl)
	if !ok || gd.Tok != token.VAR || len(gd.Specs) != 1 {
		return nil, nil, false
	}

	vs, ok := gd.Specs[0].(*ast.ValueSpec)
	if !ok || len(vs.Names) != 1 {
		return nil, nil, false
	}

	st, ok := vs.Type.(*ast.StructType)
	if !ok {
		return nil, nil, false
	}

	return vs, st, true
}

func anonStructName(fset *token.FileSet, vs *ast.ValueSpec) string {
	pos := fset.Position(vs.Pos())

	return "anon@" + filepath.Base(pos.Filename) + ":" + strconv.Itoa(pos.Line)
}

// collectFields skips embedded (anonymous) fields and any field tagged
// `json:"-"`. wireName falls back to the Go field name when there's no json
// tag, or when the tag's name segment is empty -- most REST-routed services
// in this repo tag with the AWS query/body parameter name even outside the
// JSON protocol, so this one rule covers both.
func collectFields(st *ast.StructType) []emuField {
	var out []emuField

	if st.Fields == nil {
		return out
	}

	for _, f := range st.Fields.List {
		if len(f.Names) == 0 {
			continue
		}

		tag := jsonTagOf(f)
		if tag == "-" {
			continue
		}

		for _, n := range f.Names {
			if n.Name == "_" {
				continue
			}

			wire := tag
			if wire == "" {
				wire = n.Name
			}

			out = append(out, emuField{WireName: wire, GoName: n.Name})
		}
	}

	return out
}

func jsonTagOf(f *ast.Field) string {
	if f.Tag == nil {
		return ""
	}

	unquoted, err := strconv.Unquote(f.Tag.Value)
	if err != nil {
		return ""
	}

	tag, _, _ := strings.Cut(reflect.StructTag(unquoted).Get("json"), ",")

	return tag
}
