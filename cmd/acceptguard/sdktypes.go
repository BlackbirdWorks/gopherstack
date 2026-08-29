package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"slices"
	"strings"
)

// sdkTypesPkgName is this repo's universal unaliased import name for a
// service module's types subpackage -- same constant cmd/enumcheck and
// cmd/zeroguard resolve against.
const sdkTypesPkgName = "types"

// moduleTypeFacts is one SDK module's types/types.go ground truth, parsed
// once and cached: every top-level struct's own field names, and every
// smithy union's alternative member names.
//
// A gopherstack field named for a NESTED struct's own member, or for a
// UNION's alternative (its "Member<X>" struct suffix -- codegen's own naming
// convention, confirmed live: ACM's CreateAcmeDomainValidationParams.
// DNSPrevalidation is real, just one level down real AWS's
// PrevalidationOptions union member PrevalidationOptionsMemberDnsPrevalidation
// -- not this tool's own name guess), is the repo's documented "lives on a
// sibling or nested type" non-bug and must not be flagged. This is what lets
// fieldsFor treat that name as real for the enclosing op.
type moduleTypeFacts struct {
	structFields map[string]map[string]bool
	unionAlts    map[string]map[string]bool
}

func (c *sdkFieldCache) typeFacts(modPath string) (*moduleTypeFacts, error) {
	if facts, ok := c.types[modPath]; ok {
		return facts, nil
	}

	facts, err := loadModuleTypeFacts(filepath.Join(modPath, sdkTypesPkgName, "types.go"))
	if err != nil {
		return nil, err
	}

	c.types[modPath] = facts

	return facts, nil
}

func loadModuleTypeFacts(typesGoPath string) (*moduleTypeFacts, error) {
	facts := &moduleTypeFacts{structFields: map[string]map[string]bool{}, unionAlts: map[string]map[string]bool{}}

	fset := token.NewFileSet()

	f, err := parser.ParseFile(fset, typesGoPath, nil, 0)
	if err != nil {
		if isNotExist(err) {
			return facts, nil
		}

		return nil, err
	}

	structNames, typeNames := collectTypeDecls(f, facts)
	collectUnionAlts(structNames, typeNames, facts)

	return facts, nil
}

// collectTypeDecls records every top-level struct type's own field-name set
// and returns every struct name AND every top-level type name of any kind
// (struct, interface, ...) seen -- collectUnionAlts's "<Base>Member<Alt>"
// naming-convention pass needs the latter, since a smithy union's base name
// (PrevalidationOptions) is declared as an INTERFACE, not a struct.
func collectTypeDecls(f *ast.File, facts *moduleTypeFacts) ([]string, []string) {
	var structNames, typeNames []string

	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.TYPE {
			continue
		}

		for _, spec := range gd.Specs {
			ts, isSpec := spec.(*ast.TypeSpec)
			if !isSpec {
				continue
			}

			typeNames = append(typeNames, ts.Name.Name)

			st, isStruct := ts.Type.(*ast.StructType)
			if !isStruct || st.Fields == nil {
				continue
			}

			structNames = append(structNames, ts.Name.Name)
			facts.structFields[ts.Name.Name] = foldSet(structFieldNames(st))
		}
	}

	return structNames, typeNames
}

// collectUnionAlts finds every smithy union alternative by its codegen
// naming convention: a struct literally named "<Union>Member<Alt>" for a
// union base type "<Union>" (an interface, in every real case observed) --
// e.g. PrevalidationOptionsMemberDnsPrevalidation for the union
// PrevalidationOptions, giving alternative name "DnsPrevalidation". This is
// codegen-structural (every aws-sdk-go-v2 union alternative struct is named
// exactly this way), not a per-service guess.
func collectUnionAlts(structNames, typeNames []string, facts *moduleTypeFacts) {
	for _, name := range structNames {
		union, alt, ok := unionMemberParts(name, typeNames)
		if !ok {
			continue
		}

		if facts.unionAlts[union] == nil {
			facts.unionAlts[union] = map[string]bool{}
		}

		facts.unionAlts[union][strings.ToLower(alt)] = true
	}
}

// unionMemberParts reports whether name is "<Union>Member<Alt>" for some
// OTHER type name "<Union>" also declared in this module (ruling out an
// unrelated struct that merely contains the substring "Member").
func unionMemberParts(name string, allTypeNames []string) (string, string, bool) {
	idx := strings.Index(name, "Member")
	if idx <= 0 {
		return "", "", false
	}

	candidateUnion := name[:idx]
	candidateAlt := name[idx+len("Member"):]

	if candidateAlt == "" || !slices.Contains(allTypeNames, candidateUnion) {
		return "", "", false
	}

	return candidateUnion, candidateAlt, true
}

// expand adds, for a real Input field named fieldName whose declared type is
// typeExpr, the flattened acceptable names an emitting gopherstack field
// could legitimately carry: the field's own name, plus -- when typeExpr
// resolves to a types.<X> this module declares -- X's own struct fields or
// union alternatives.
func (facts *moduleTypeFacts) expand(fieldName string, typeExpr ast.Expr, into map[string]bool) {
	into[strings.ToLower(fieldName)] = true

	typeName, ok := sdkTypesSelector(typeExpr)
	if !ok {
		return
	}

	for name := range facts.structFields[typeName] {
		into[name] = true
	}

	for name := range facts.unionAlts[typeName] {
		into[name] = true
	}
}

// sdkTypesSelector reports X when t is `types.X`, `*types.X`, or `[]types.X`.
func sdkTypesSelector(t ast.Expr) (string, bool) {
	switch e := t.(type) {
	case *ast.StarExpr:
		return sdkTypesSelector(e.X)
	case *ast.ArrayType:
		return sdkTypesSelector(e.Elt)
	case *ast.SelectorExpr:
		pkgIdent, isIdent := e.X.(*ast.Ident)
		if !isIdent || pkgIdent.Name != sdkTypesPkgName {
			return "", false
		}

		return e.Sel.Name, true
	default:
		return "", false
	}
}
