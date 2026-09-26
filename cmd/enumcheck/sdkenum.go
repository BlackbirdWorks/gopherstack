package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"unicode"
	"unicode/utf8"
)

// enumConst is one declared member of a pinned SDK string enum: the Go
// const identifier's owning type and its literal wire value.
type enumConst struct {
	typeName string
	value    string
}

// fieldResolution is how precisely resolveRealField pinned down a
// gopherstack struct field's real SDK member -- gopherstack-cpztm's core
// fix: resolve the ONE real member a field maps to, instead of comparing a
// value against every enum that happens to share its wire key.
type fieldResolution int

const (
	// fieldUnknownType: structTypeName has no real pinned-SDK struct of the
	// same name at all -- nothing to resolve against. Unresolved bucket.
	fieldUnknownType fieldResolution = iota
	// fieldAbsent: the real type is known, but no field named wireKey exists
	// on it or on a directly nested field's own type (one hop) -- the
	// phantom-field shape (gopherstack-7fps).
	fieldAbsent
	// fieldNotEnum: the field exists and its real declared Go type is known,
	// but that type isn't a named SDK enum (plain *string, *int32, ...) --
	// never checked, never flagged.
	fieldNotEnum
	// fieldIsEnum: the field exists and its real declared type is exactly
	// one named SDK enum -- the sole candidate to check value membership
	// against.
	fieldIsEnum
)

// enumRegistry is every named string enum this service's pinned SDK
// declares in types/enums.go: membersByType is the real declared member set
// per enum type name (e.g. "DataSource" -> {"FLOW_LOGS", ...}), and
// constByIdent resolves a Go const identifier (e.g. "DataSourceFlowLogs")
// back to its owning type and value, for reading a service's own
// types.XxxEnumMember selector expressions.
//
// keyEnumModules and nativeModules are gopherstack-7fps ground truth,
// sdkFieldTypes is gopherstack-cpztm's, all populated only by the
// per-directory merge in main.go (a registry built directly by
// loadEnumRegistry, as every existing test in this package does, leaves
// them nil/empty -- confidentModuleOK treats an empty nativeModules as
// "nothing to prefer over", never as "refuse everything", so those tests
// are unaffected):
//
//   - keyEnumModules resolves a (wire key, enum type) PAIR -- keyed as
//     "wireKey\x00EnumType" -- back to every SDK module whose OWN
//     deserializers.go actually deserialized that key into that type
//     (recorded where wireKeys is merged, mergeModuleGroundTruth in
//     main.go, since only there are the key, the type, AND the
//     contributing module all in scope together). Deliberately NOT keyed
//     by the bare enum type name alone: ec2 itself declares its own
//     "ResourceType" enum (the CreateTags resource-type list --
//     "instance", "image", ...), entirely unrelated to the
//     ImageReferenceResourceType/TransitGatewayAttachmentResourceType
//     enums the real bug is about, and to outposts' own same-named
//     "ResourceType" enum (OUTPOST/ORDER) -- three distinct real enums that
//     happen to share one bare Go identifier across two SDKs. Scoping by
//     type name alone (tried first, reverted) would have kept ec2 "native"
//     for the wrong reason: ec2's module DOES declare a type named
//     "ResourceType", just never THIS key's real one -- its own
//     ec2query/XML deserializers.go contributes no case for the key at
//     all, only outposts' restjson1 one does, so keyEnumModules records
//     only "outposts" for this exact pair. See confidentModuleOK.
//   - nativeModules is the subset of a directory's resolved SDK modules
//     whose OWN module name equals the service directory's own basename
//     (nativeModuleSet in main.go) -- as opposed to a second SDK the
//     directory also happens to import.
//   - sdkFieldTypes is, per real pinned-SDK struct name (from that module's
//     own api_op_*.go/types/types.go, via cmd/internal/sdkshape -- the SDK's
//     own generated code carries no json tags, so the wire name for a
//     JSON-family field IS the Go field name), the field's own bare declared
//     Go type name: "ClusterStatus" for an enum-typed field, "string" for a
//     plain *string, "JobSummary" for a nested struct, etc. Ground truth for
//     resolveRealField.
type enumRegistry struct {
	membersByType  map[string]map[string]bool
	constByIdent   map[string]enumConst
	keyEnumModules map[string]map[string]bool
	nativeModules  map[string]bool
	sdkFieldTypes  map[string]map[string]string
}

// resolveRealField resolves wireKey on structTypeName to the ONE real SDK
// member it maps to: a direct field of the real same-named type, or --
// amplify's Job/JobSummary shape -- a field on a type one of structTypeName's
// own fields directly nests. This replaces comparing a value against every
// enum sharing a bare wire key: gopherstack-cpztm's false-positive class.
//
// wireKey is looked up as its own Go-exported form (first rune uppercased):
// enumRegistry.sdkFieldTypes is keyed by the real SDK's own Go field names
// (from cmd/internal/sdkshape, always PascalCase -- "Status", "ClusterName"),
// while wireKey comes from gopherstack's own json tag, which mirrors the
// real WIRE format and so varies by protocol -- restjson1 camelCase
// ("status", confirmed live against eks's deserializers.go), awsjson1.0/1.1
// already PascalCase and identical to the Go field name (confirmed against
// dynamodb's). Uppercasing wireKey's first rune is a correct transform for
// both: it fixes the restjson1 case and is a no-op where it's already
// PascalCase.
func (reg *enumRegistry) resolveRealField(structTypeName, wireKey string) (fieldResolution, string) {
	fields, known := reg.sdkFieldTypes[structTypeName]
	if !known {
		return fieldUnknownType, ""
	}

	goName := exportedFieldName(wireKey)

	if bareType, ok := fields[goName]; ok {
		return reg.classifyFieldType(bareType)
	}

	for _, nestedType := range fields {
		nestedFields, isStruct := reg.sdkFieldTypes[nestedType]
		if !isStruct {
			continue
		}

		if bareType, ok := nestedFields[goName]; ok {
			return reg.classifyFieldType(bareType)
		}
	}

	return fieldAbsent, ""
}

// exportedFieldName uppercases wireKey's first rune -- see resolveRealField's
// doc comment for why this is the right (and only) transform needed.
func exportedFieldName(wireKey string) string {
	r, size := utf8.DecodeRuneInString(wireKey)
	if r == utf8.RuneError {
		return wireKey
	}

	return string(unicode.ToUpper(r)) + wireKey[size:]
}

func (reg *enumRegistry) classifyFieldType(bareType string) (fieldResolution, string) {
	if _, isEnum := reg.membersByType[bareType]; isEnum {
		return fieldIsEnum, bareType
	}

	return fieldNotEnum, ""
}

func keyEnumModuleKey(wireKey, enumType string) string {
	return wireKey + "\x00" + enumType
}

// recordKeyEnumModule records that mod's own deserializers.go deserialized
// wireKey into enumType -- see keyEnumModules's doc comment for why this is
// keyed by the pair, not the bare enum type name.
func (reg *enumRegistry) recordKeyEnumModule(wireKey, enumType, mod string) {
	if reg.keyEnumModules == nil {
		reg.keyEnumModules = map[string]map[string]bool{}
	}

	k := keyEnumModuleKey(wireKey, enumType)
	if reg.keyEnumModules[k] == nil {
		reg.keyEnumModules[k] = map[string]bool{}
	}

	reg.keyEnumModules[k][mod] = true
}

// confidentModuleOK reports whether the (wireKey, enumType) pair is backed
// by at least one SDK module native to the directory currently being
// scanned, eligible to back a CONFIDENT (single-candidate) finding.
// gopherstack-7fps's cross-module-contamination class: services/ec2
// imports both the AWS SDK's ec2 module and its outposts module (only from
// cross_service_test.go, a round-trip completeness test -- see
// nativeModuleSet's own doc comment for why import location can't be the
// signal here: even ec2 itself is only referenced from *_test.go files in
// this directory, same as most of this repo's services). ec2's own
// ec2query/XML "ResourceType" key is outside this tool's disclosed
// JSON-family scope (see the package doc comment), so this (key, type)
// pair had NO candidate from ec2's own module at all, and outposts'
// unrelated ResourceType enum (OUTPOST/ORDER) became the ONLY candidate.
// All five ec2 confident findings were this shape: real enums exist
// somewhere in ec2's own SDK that legally contain every value actually
// emitted (ImageReferenceResourceType, TransitGatewayAttachmentResourceType
// -- just never under the "ResourceType" key literal this scan's flat
// key-name matching could ever discover).
//
// When nativeModules is empty (this directory's own basename matches none
// of its resolved modules by name at all -- common, since this repo's
// directory names frequently diverge from their SDK module's own name)
// there is nothing to prefer over, so every module is OK: this scoping only
// ever REFUSES a candidate, never invents one, and a directory whose own
// SDK module can't be positively named keeps its existing coverage exactly
// as before this fix. Scoped to the single-candidate CONFIDENT case only --
// an ambiguous-key or cross-enum-reuse finding never claims certainty about
// which candidate applies in the first place, so module provenance has
// nothing to add there.
//
// COST: a service whose directory name diverges from BOTH its own SDK
// module's name and a second, legitimately-used SDK's name (nativeModules
// then empty, or matching neither) gets no protection either way -- no
// false positive removed, no real bug suppressed, unchanged from before
// this fix. The real cost lands on the opposite shape: a directory whose
// basename happens to equal its own SDK module's name (the common case)
// but that also legitimately emits a second, correctly-imported SDK's enum
// under some wire key its OWN SDK never deserializes at all -- that second
// SDK's real candidate is not native, so a genuine bug there would be
// refused exactly like the ec2 false positive is. This is the deliberately
// narrower of the two directions gopherstack-7fps proposed (scope
// candidates to the owning module, vs. refuse only when EVERY candidate is
// non-native): safe because refusing to report is never a "wrong" answer,
// merely a missed one, same discipline this whole scan already applies to
// unresolvable values.
func (reg *enumRegistry) confidentModuleOK(wireKey, enumType string) bool {
	if len(reg.nativeModules) == 0 {
		return true
	}

	for mod := range reg.keyEnumModules[keyEnumModuleKey(wireKey, enumType)] {
		if reg.nativeModules[mod] {
			return true
		}
	}

	return false
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

// isMemberOfAll reports whether value belongs to every one of the named enum
// types -- used by the ambiguous-key NEEDS REVIEW check, where "belongs to
// every candidate sense of this key" is the only true-negative signal
// available without knowing which sense actually applies.
func (reg *enumRegistry) isMemberOfAll(value string, types []string) bool {
	for _, t := range types {
		if !reg.membersByType[t][value] {
			return false
		}
	}

	return true
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
