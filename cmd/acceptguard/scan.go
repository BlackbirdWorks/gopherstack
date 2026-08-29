package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
)

const (
	kindInvented = "invented-member"
	kindSibling  = "sibling-op-member"
	kindFallback = "tolerated-fallback-alias"
)

// requestSuffixes are the struct-name suffixes this repo uses for a type
// representing what a handler accepts off the wire (a decoded JSON body, or
// a hand-populated params struct upstream of one) -- "Input" (apigatewayv2's
// own convention, zeroguard's validated case), "Req"/"Request" (networkmanager
// and most JSON-family services), "Params". Checked longest-first so a name
// ending "...Request" is not also mis-trimmed as ending "...Req" (it isn't,
// since "Request" doesn't end in "Req", but keeping the specific forms first
// documents the intent).
var requestSuffixes = []string{"Input", "Request", "Params", "Req"} //nolint:gochecknoglobals // read-only lookup table

// finding is one acceptguard result. CONFIDENT (kindInvented) is a
// gopherstack request-struct field, reachable through a func that actually
// reads it, whose name (case/abbreviation-folded) matches NO member of ANY
// real Input struct anywhere in the resolved SDK module -- invented
// wholesale. NEEDS REVIEW (kindSibling) is the same shape except the name
// DOES match a real member, just on a different operation's Input --
// possibly wired to the wrong op, but also the repo's documented non-bug
// (a field that lives on a sibling or Create/Update-paired Input).
type finding struct {
	File      string `json:"file"`
	Kind      string `json:"kind"`
	Op        string `json:"op"`
	Struct    string `json:"struct"`
	Field     string `json:"field"`
	Func      string `json:"func"`
	Line      int    `json:"line"`
	Confident bool   `json:"confident"`
}

// scanPackage checks every non-test .go file directly in dir (no recursion
// into subpackages, matching the sibling cmd tools' disclosed scope) against
// the real SDK Input ground truth resolvable from mods.
func scanPackage(dir, repoRoot string, mods []sdkModule, cache *sdkFieldCache) ([]finding, error) {
	fset := token.NewFileSet()

	files, err := parseDirFiles(fset, dir)
	if err != nil {
		return nil, err
	}

	structTypes := map[string]*ast.StructType{}
	for _, f := range files {
		maps.Copy(structTypes, topLevelStructs(f))
	}

	var out []finding

	for structName, st := range structTypes {
		found, scanErr := checkRequestStruct(fset, files, repoRoot, structName, st, mods, cache)
		if scanErr != nil {
			return nil, scanErr
		}

		out = append(out, found...)
	}

	out = dedupeFindings(out)

	sort.Slice(out, func(i, j int) bool {
		if out[i].File != out[j].File {
			return out[i].File < out[j].File
		}

		return out[i].Line < out[j].Line
	})

	return out, nil
}

func checkRequestStruct(
	fset *token.FileSet, files []*ast.File, repoRoot, structName string, st *ast.StructType,
	mods []sdkModule, cache *sdkFieldCache,
) ([]finding, error) {
	opName, ok := deriveOpName(structName)
	if !ok || st.Fields == nil {
		return nil, nil
	}

	opFields, mod, ok, err := resolveOpFields(mods, cache, opName)
	if err != nil {
		return nil, err
	}

	// opFields.has("PatchOperations"): the op is a JSON-Patch-document
	// endpoint (apigateway's Update* family -- api_op_UpdateAccount.go etc.
	// declare ONLY {ids..., PatchOperations []types.PatchOperation}, no
	// typed fields at all). gopherstack deliberately flattens the resolved
	// patch document into named fields before decoding into its own struct
	// (confirmed live: models.go's UpdateAccountInput doc comment says so
	// explicitly) -- comparing that POST-resolution shape against the real
	// PRE-resolution wire shape is a protocol-level category error, not a
	// finding, and produced 11 of this tool's first 37 confident hits
	// before this filter (calibration finding, not a tool bug).
	if !ok || opFields.has("PatchOperations") || !structIsJSONDecoded(files, structName) {
		return nil, nil
	}

	var out []finding

	for _, field := range st.Fields.List {
		name, wireKey, isWireField := ownFieldWireKey(field)
		if !isWireField || opFields.has(wireKey) {
			continue
		}

		fd, funcName, line, used := findFieldUsage(files, fset, structName, name)
		if !used {
			continue
		}

		moduleFields, modErr := cache.moduleFields(mod.path)
		if modErr != nil {
			return nil, modErr
		}

		f := finding{
			Op: opName, Struct: structName, Field: name, Func: funcName,
			Line: line, File: relPath(repoRoot, fset.Position(field.Pos()).Filename),
		}

		switch {
		case isToleratedFallback(fd, name, opFields):
			f.Kind = kindFallback
		case moduleFields[strings.ToLower(wireKey)]:
			f.Kind = kindSibling
		default:
			f.Kind, f.Confident = kindInvented, true
		}

		out = append(out, f)
	}

	return out, nil
}

func resolveOpFields(
	mods []sdkModule, cache *sdkFieldCache, opName string,
) (*sdkOpFields, sdkModule, bool, error) {
	for _, mod := range mods {
		fields, ok, err := cache.fieldsFor(mod.path, opName)
		if err != nil {
			return nil, sdkModule{}, false, err
		}

		if ok {
			return fields, mod, true, nil
		}
	}

	return nil, sdkModule{}, false, nil
}

// deriveOpName reports the real AWS operation name a gopherstack request
// struct is named for, by stripping the trailing requestSuffixes entry it
// ends with and capitalizing the first rune (createVpcAttachmentReq ->
// CreateVpcAttachment; UpdateAuthorizerInput -> UpdateAuthorizer already
// capitalized). Whether that derived name is a REAL operation is verified
// separately, against the pinned SDK's own file layout (resolveOpFields) --
// this only proposes a candidate.
func deriveOpName(structName string) (string, bool) {
	for _, suf := range requestSuffixes {
		trimmed, ok := strings.CutSuffix(structName, suf)
		if !ok || trimmed == "" {
			continue
		}

		return capitalizeFirst(trimmed), true
	}

	return "", false
}

func capitalizeFirst(s string) string {
	if s == "" {
		return s
	}

	return strings.ToUpper(s[:1]) + s[1:]
}

// ownFieldWireKey returns field's single Go name and the wire key it decodes
// under -- the json tag's name segment when present and not "-", else the Go
// name itself (encoding/json's own default, and this repo's apigatewayv2-
// style Input structs carry no tags at all and rely on it). Embedded fields
// (no Names) and explicitly untagged ("-") fields are not request members
// and return ok=false.
func ownFieldWireKey(field *ast.Field) (string, string, bool) {
	if len(field.Names) != 1 {
		return "", "", false
	}

	name := field.Names[0].Name
	if !field.Names[0].IsExported() {
		return "", "", false
	}

	tag := jsonTagName(field.Tag)
	if tag == "-" {
		return "", "", false
	}

	if tag != "" {
		return name, tag, true
	}

	return name, name, true
}

func jsonTagName(tag *ast.BasicLit) string {
	if tag == nil {
		return ""
	}

	raw, err := strconv.Unquote(tag.Value)
	if err != nil {
		return ""
	}

	name, _, _ := strings.Cut(reflect.StructTag(raw).Get("json"), ",")

	return name
}

// findFieldUsage looks across every file in files for a func whose body
// binds a local identifier to structName (a parameter of type structName or
// *structName, or a `var x structName` declaration) and, within that SAME
// func, reads a `<that identifier>.<fieldName>` selector -- proof the
// field is actually consumed somewhere reachable from the accepting
// handler, not merely decoded and dropped (this repo's documented non-bug:
// an emulator-internal hook unreachable from the real wire path).
func findFieldUsage(
	files []*ast.File, fset *token.FileSet, structName, fieldName string,
) (*ast.FuncDecl, string, int, bool) {
	for _, f := range files {
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}

			if line, found := funcReadsField(fd, structName, fieldName, fset); found {
				return fd, fd.Name.Name, line, true
			}
		}
	}

	return nil, "", 0, false
}

// isToleratedFallback reports whether fieldName is read only as a fallback
// value for a local variable ALSO assigned from a real member of structName
// -- this repo's documented non-bug, "a deliberately tolerant handler that
// reads a member for backwards compatibility" (confirmed live:
// sesv2's updateReputationEntityCustomerManagedStatusInput, whose own
// comments read "SendingStatus is the field name used by the AWS SDK" /
// "CustomerManagedStatus is accepted as an alias for callers that post it
// directly"). The shape: some local ident is assigned from
// `<var>.<realField>`, then an `if ident == "" { ident = <var>.<fieldName>
// }` (or the inverse: `<var>.<fieldName>` assigned first, guarded, THEN
// overwritten by the real field) reassigns it from fieldName -- a
// zero-guarded alias read, not a plain accepted-and-used member.
func isToleratedFallback(fd *ast.FuncDecl, fieldName string, opFields *sdkOpFields) bool {
	if fd == nil {
		return false
	}

	found := false

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		if found {
			return false
		}

		ifStmt, ok := n.(*ast.IfStmt)
		if !ok {
			return true
		}

		alias, ok := zeroGuardedIdent(ifStmt.Cond)
		if !ok || !assignsIdentFromField(ifStmt.Body, alias, fieldName) {
			return true
		}

		if identAssignedFromRealField(fd.Body, alias, opFields) {
			found = true

			return false
		}

		return true
	})

	return found
}

// zeroGuardedIdent reports the identifier name when cond is `<ident> == ""`
// or `<ident> != ""` (either direction covers "fall back when empty" and
// "already set, don't overwrite" phrasings of the same alias shape), or such
// a comparison ANDed with further conditions (`<ident> == "" && ...` --
// bedrockruntime's StartAsyncInvoke ModelId/InferenceProfileIdentifier
// fallback also guards on the fallback field itself being non-empty).
func zeroGuardedIdent(cond ast.Expr) (string, bool) {
	bin, ok := cond.(*ast.BinaryExpr)
	if !ok {
		return "", false
	}

	if bin.Op == token.LAND {
		return zeroGuardedIdent(bin.X)
	}

	if bin.Op != token.EQL && bin.Op != token.NEQ {
		return "", false
	}

	id, ok := bin.X.(*ast.Ident)
	if !ok {
		return "", false
	}

	lit, ok := bin.Y.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING || lit.Value != `""` {
		return "", false
	}

	return id.Name, true
}

// assignsIdentFromField reports whether block assigns `<ident> =
// <structName-typed var>.<fieldName>` anywhere -- the varName the field is
// selected off doesn't need to match a specific name, only its selector's
// field, since findFieldUsage already proved structName's own instance in
// this func reads fieldName.
func assignsIdentFromField(block ast.Stmt, ident, fieldName string) bool {
	found := false

	ast.Inspect(block, func(n ast.Node) bool {
		if found {
			return false
		}

		as, ok := n.(*ast.AssignStmt)
		if !ok || len(as.Lhs) != 1 || len(as.Rhs) != 1 {
			return true
		}

		lhs, isIdent := as.Lhs[0].(*ast.Ident)
		if !isIdent || lhs.Name != ident {
			return true
		}

		sel, isSel := as.Rhs[0].(*ast.SelectorExpr)
		if isSel && sel.Sel.Name == fieldName {
			found = true

			return false
		}

		return true
	})

	return found
}

// identAssignedFromRealField reports whether body assigns ident from
// `<var>.<X>` for some X that is a genuine member of opFields anywhere
// (not restricted to before/after the fallback -- a same-var double
// assignment to a real field elsewhere in the func is what marks the
// mismatched read as an intentional alias rather than the sole source).
func identAssignedFromRealField(body ast.Stmt, ident string, opFields *sdkOpFields) bool {
	found := false

	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}

		as, ok := n.(*ast.AssignStmt)
		if !ok || len(as.Lhs) != 1 || len(as.Rhs) != 1 {
			return true
		}

		lhs, isIdent := as.Lhs[0].(*ast.Ident)
		if !isIdent || lhs.Name != ident {
			return true
		}

		sel, isSel := as.Rhs[0].(*ast.SelectorExpr)
		if isSel && opFields.has(sel.Sel.Name) {
			found = true

			return false
		}

		return true
	})

	return found
}

func funcReadsField(fd *ast.FuncDecl, structName, fieldName string, fset *token.FileSet) (int, bool) {
	for _, varName := range boundVarNames(fd, structName) {
		if line, found := selectorLine(fd.Body, varName, fieldName, fset); found {
			return line, true
		}
	}

	return 0, false
}

// boundVarNames returns every local identifier fd binds to structName: its
// parameters (by value or pointer) and any `var x structName` declaration in
// its body.
func boundVarNames(fd *ast.FuncDecl, structName string) []string {
	names := paramNamesOfType(fd, structName)
	names = append(names, varDeclNamesOfType(fd.Body, structName)...)

	return names
}

func paramNamesOfType(fd *ast.FuncDecl, structName string) []string {
	var names []string

	if fd.Type.Params == nil {
		return names
	}

	for _, field := range fd.Type.Params.List {
		if !typeIsNamed(field.Type, structName) {
			continue
		}

		for _, id := range field.Names {
			names = append(names, id.Name)
		}
	}

	return names
}

func varDeclNamesOfType(body *ast.BlockStmt, structName string) []string {
	var names []string

	ast.Inspect(body, func(n ast.Node) bool {
		gd, ok := n.(*ast.GenDecl)
		if !ok || gd.Tok != token.VAR {
			return true
		}

		for _, spec := range gd.Specs {
			vs, isVal := spec.(*ast.ValueSpec)
			if !isVal || vs.Type == nil || !typeIsNamed(vs.Type, structName) {
				continue
			}

			for _, id := range vs.Names {
				names = append(names, id.Name)
			}
		}

		return true
	})

	return names
}

// structIsJSONDecoded reports whether structName is actually populated by
// decoding the raw request body somewhere in files, not merely a struct
// gopherstack's authors named as if it were one -- the signal that rules out
// this repo's other common "Input"/"Params" shape, a struct hand-populated
// field-by-field from URL path/query parameters (a GET's *Input has no body
// at all; its field names are internal choices, not real wire keys, and
// comparing them to the real SDK's members the way a JSON body's tags can be
// compared is unsound). Confirmed by finding some func binding a local
// identifier to structName (boundVarNames) and, in that SAME func, passing
// `&identifier` to a call this scan recognizes as a JSON decode
// (isJSONDecodeCall) -- e.g. `json.Unmarshal(body, &req)` or this repo's own
// `decodeJSONBody(body, &req)` helpers.
func structIsJSONDecoded(files []*ast.File, structName string) bool {
	for _, f := range files {
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}

			names := boundVarNames(fd, structName)
			if len(names) == 0 {
				continue
			}

			if funcDecodesInto(fd.Body, names) {
				return true
			}
		}
	}

	return false
}

func funcDecodesInto(body *ast.BlockStmt, varNames []string) bool {
	decoded := false

	ast.Inspect(body, func(n ast.Node) bool {
		if decoded {
			return false
		}

		call, ok := n.(*ast.CallExpr)
		if !ok || !isJSONDecodeCall(call.Fun) {
			return true
		}

		for _, arg := range call.Args {
			if addrOfNamed(arg, varNames) {
				decoded = true

				return false
			}
		}

		return true
	})

	return decoded
}

func addrOfNamed(arg ast.Expr, varNames []string) bool {
	un, ok := arg.(*ast.UnaryExpr)
	if !ok || un.Op != token.AND {
		return false
	}

	id, ok := un.X.(*ast.Ident)
	if !ok {
		return false
	}

	return slices.Contains(varNames, id.Name)
}

// isJSONDecodeCall reports whether fun is a call this scan trusts to decode
// JSON: the standard library's json.Unmarshal/json.NewDecoder(...).Decode
// (a "json" package selector anywhere in fun), or a local helper whose OWN
// name says so (decodeJSONBody, decodeJSON, unmarshalJSON, ... -- this
// repo's own observed helper names, all of which literally contain "json").
// A bare "unmarshal"/"decode" helper with no "json" in its name is NOT
// trusted -- it could just as well wrap encoding/xml for a query-family
// service, and this scan's field-name comparison is unsound for those
// (see this package's doc comment's PROTOCOL SCOPE section).
func isJSONDecodeCall(fun ast.Expr) bool {
	switch e := fun.(type) {
	case *ast.SelectorExpr:
		if id, ok := e.X.(*ast.Ident); ok && id.Name == "json" {
			return true
		}

		return isJSONDecodeCall(e.X)
	case *ast.CallExpr:
		return isJSONDecodeCall(e.Fun)
	case *ast.Ident:
		return strings.Contains(strings.ToLower(e.Name), "json")
	default:
		return false
	}
}

func typeIsNamed(t ast.Expr, name string) bool {
	if star, ok := t.(*ast.StarExpr); ok {
		t = star.X
	}

	id, ok := t.(*ast.Ident)

	return ok && id.Name == name
}

func selectorLine(body *ast.BlockStmt, varName, fieldName string, fset *token.FileSet) (int, bool) {
	line, found := 0, false

	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}

		sel, ok := n.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != fieldName {
			return true
		}

		id, isIdent := sel.X.(*ast.Ident)
		if isIdent && id.Name == varName {
			found = true
			line = fset.Position(sel.Pos()).Line

			return false
		}

		return true
	})

	return line, found
}

// dedupeFindings drops exact repeats: the same struct field found reachable
// through more than one function (a dispatcher and the backend method it
// calls, both taking the same request struct) reports the same field once
// per function otherwise.
func dedupeFindings(in []finding) []finding {
	type key struct {
		file, structName, field, kind string
	}

	seen := map[key]bool{}
	out := make([]finding, 0, len(in))

	for _, f := range in {
		k := key{file: f.File, structName: f.Struct, field: f.Field, kind: f.Kind}
		if seen[k] {
			continue
		}

		seen[k] = true

		out = append(out, f)
	}

	return out
}

func parseDirFiles(fset *token.FileSet, dir string) ([]*ast.File, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var files []*ast.File

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}

		f, perr := parser.ParseFile(fset, filepath.Join(dir, e.Name()), nil, 0)
		if perr != nil {
			return nil, perr
		}

		files = append(files, f)
	}

	return files, nil
}

func topLevelStructs(f *ast.File) map[string]*ast.StructType {
	out := map[string]*ast.StructType{}

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

			if st, isStruct := ts.Type.(*ast.StructType); isStruct {
				out[ts.Name.Name] = st
			}
		}
	}

	return out
}

func relPath(repoRoot, path string) string {
	rel, err := filepath.Rel(repoRoot, path)
	if err != nil {
		return path
	}

	return rel
}
