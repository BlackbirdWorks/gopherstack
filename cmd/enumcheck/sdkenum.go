package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
)

// enumConst is one declared member of a pinned SDK string enum: the Go
// const identifier's owning type and its literal wire value.
type enumConst struct {
	typeName string
	value    string
}

// enumRegistry is every named string enum this service's pinned SDK
// declares in types/enums.go: membersByType is the real declared member set
// per enum type name (e.g. "DataSource" -> {"FLOW_LOGS", ...}), and
// constByIdent resolves a Go const identifier (e.g. "DataSourceFlowLogs")
// back to its owning type and value, for reading a service's own
// types.XxxEnumMember selector expressions.
type enumRegistry struct {
	membersByType map[string]map[string]bool
	constByIdent  map[string]enumConst
}

// loadEnumRegistry parses a pinned SDK's types/enums.go. Every enum in this
// codegen shape is a top-level `type X string` with a `const ( XFoo X =
// "FOO"; ... )` block repeating the type on every line (no iota) -- this
// walks every const ValueSpec directly rather than the type's Values()
// method, since the const block alone gives both the member set and the
// identifier->value mapping in one pass.
func loadEnumRegistry(enumsGoPath string) (*enumRegistry, error) {
	fset := token.NewFileSet()

	f, err := parser.ParseFile(fset, enumsGoPath, nil, 0)
	if err != nil {
		return nil, err
	}

	reg := &enumRegistry{
		membersByType: map[string]map[string]bool{},
		constByIdent:  map[string]enumConst{},
	}

	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.CONST {
			continue
		}

		for _, spec := range gd.Specs {
			reg.addValueSpec(spec)
		}
	}

	return reg, nil
}

func (reg *enumRegistry) addValueSpec(spec ast.Spec) {
	vs, ok := spec.(*ast.ValueSpec)
	if !ok || len(vs.Names) != 1 || len(vs.Values) != 1 {
		return
	}

	typeIdent, ok := vs.Type.(*ast.Ident)
	if !ok {
		return
	}

	lit, ok := vs.Values[0].(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return
	}

	value, err := strconv.Unquote(lit.Value)
	if err != nil {
		return
	}

	typeName := typeIdent.Name

	if reg.membersByType[typeName] == nil {
		reg.membersByType[typeName] = map[string]bool{}
	}

	reg.membersByType[typeName][value] = true
	reg.constByIdent[vs.Names[0].Name] = enumConst{typeName: typeName, value: value}
}

// isMemberOfAny reports whether value belongs to at least one of the named
// enum types.
func (reg *enumRegistry) isMemberOfAny(value string, types []string) bool {
	for _, t := range types {
		if reg.membersByType[t][value] {
			return true
		}
	}

	return false
}

// sameMemberSet reports whether two enum types declare exactly the same
// member values -- used to decide whether reusing one value source across
// both is even structurally possible without a bug.
func (reg *enumRegistry) sameMemberSet(typeA, typeB string) bool {
	a, b := reg.membersByType[typeA], reg.membersByType[typeB]
	if len(a) != len(b) {
		return false
	}

	for v := range a {
		if !b[v] {
			return false
		}
	}

	return true
}
