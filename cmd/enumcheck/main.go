// Command enumcheck finds values gopherstack emits into a wire response
// field whose real pinned aws-sdk-go-v2 type is a named string enum, but
// that are not members of that enum's real declared value set --
// gopherstack-6flj's guardduty class (GetUsageStatistics.sumByDataSource
// emitting DetectorFeature names like "S3_DATA_EVENTS" under a field whose
// real type is the unrelated six-member types.DataSource enum). The key was
// right, the Go type was right; only the values came from the wrong enum. A
// typed client decodes this without error -- no key check and no shape
// check can see it, only comparing the emitted VALUE against the target
// enum's real declared members does.
//
// GROUND TRUTH, not a naming guess. For each services/<dir>, the pinned
// aws-sdk-go-v2/service/<mod>@<ver> module is resolved straight from that
// service's own import paths (go/ast, not a name table) cross-referenced
// against go.mod (golang.org/x/mod/modfile, same approach as cmd/checkpins).
// Two files from that module are then parsed with go/ast:
//
//   - types/enums.go: every `type X string` with a `const ( XFoo X = "FOO";
//     ... )` block gives X's real declared member set.
//   - deserializers.go: every JSON-family protocol this repo pins
//     (restjson1, awsjson1.0/1.1 -- confirmed live against
//     guardduty@v1.85.4, a restjson1 service) generates `case "wireKey": ...
//     sv.Field = types.SomeEnum(jtv)` inside a switch on a decoded
//     map[string]interface{} key. That structural shape -- a CaseClause's
//     own literal string(s), paired with an enum-typed conversion assignment
//     in its body -- is read directly as "wireKey really deserializes into
//     SomeEnum", with no name matching at all. query/EC2-query/REST-XML
//     protocols use an xml.Decoder with no such switch, so this resolves
//     zero wire keys for them -- same disclosed protocol scope as
//     cmd/keycheck. The same parse pass also records, per real SDK type
//     (from a deserializeDocument<Type> function's own `**types.Type`
//     parameter, structural again, never a name guess off the function
//     identifier, whose prefix varies by protocol), the FULL wire-key set
//     that type's deserializer handles, enum-typed or not -- ground truth
//     for the phantom-field check below, and for which SDK module actually
//     proved a given (wire key, enum type) pair, ground truth for the
//     cross-module check below.
//
// gopherstack-7fps hand-triaged this tool's own confident tier (21
// findings, 7 real, 14 false positives) against the pinned SDK and found
// two of the four false-positive shapes were structural, fixable here
// rather than requiring human judgement each time:
//
//   - CROSS-MODULE CONTAMINATION: a directory that imports more than one
//     aws-sdk-go-v2 service module (services/ec2 imports both ec2 and
//     outposts) can have a wire key's only real enum candidate come from
//     the SECONDARY module, not the one the directory is actually about --
//     confirmed live: ec2's own ec2query/XML protocol contributes nothing
//     to wire-key ground truth (outside this tool's JSON-family scope), so
//     outposts' unrelated restjson1 "ResourceType" enum (OUTPOST/ORDER)
//     was the ONLY candidate for an ec2 "ResourceType" key, even though
//     real ec2 enums (ImageReferenceResourceType,
//     TransitGatewayAttachmentResourceType) legally contain every value
//     actually emitted. The confident check now refuses to promote a
//     single-candidate finding whose (wire key, enum type) pair was proved
//     ONLY by a module that isn't native to the directory being scanned
//     (enumRegistry.confidentModuleOK; nativeModuleSet in this file decides
//     "native" by directory-basename equality, not import location -- see
//     its own doc comment for why import location can't be the signal in
//     this repo). This only ever refuses a candidate, never invents one:
//     the cost is a directory that legitimately emits a second SDK's enum
//     under a wire key its OWN SDK never deserializes at all would have
//     that real bug suppressed too, same as the false positive this exists
//     to remove.
//   - PHANTOM FIELD: a gopherstack response-struct field whose wire key
//     resolves to some real SDK enum, but the REAL SDK type of the exact
//     same name as the gopherstack struct has NO field under that wire key
//     at all -- meaning the matched enum belongs to an entirely unrelated
//     real operation. Confirmed live: cloudtrail's Event.EventCategory
//     (real types.Event has no such field; the match was
//     EventCategoryAggregation's) and sagemaker's
//     PipelineExecutionStep.StepType (real type has no such field; the
//     match was Inference Recommender's). Rather than silently discard
//     these -- a field with no real counterpart is itself either dead code
//     or a fabricated capability, both worth a human's judgement -- they
//     are reported as a distinct NEEDS REVIEW kind (kindPhantomField,
//     checkPhantomField in structresp.go) instead of a wrong-value claim
//     that was never the real defect. Scope: only checked for a struct
//     type name that has known real-type ground truth at all; most
//     gopherstack response structs don't share their exact name with a
//     real SDK type and get no finding here.
//
// FOUR CHECKS, two confidence levels (see scan.go/reuse.go/structresp.go
// for the full mechanics):
//
//   - CONFIDENT (literal-value): a map[string]any entry, OR an
//     `out["wireKey"] = value` index-assignment onto one, keyed to a
//     resolved wire key with exactly ONE real SDK enum candidate and no
//     Polymorphic plain-string sighting, whose value statically resolves (a
//     string literal, a same-package string const, a
//     types.SomeEnumMember/types.SomeEnum("x") selector/conversion, or a
//     `structVar.Field` read of a field this same function assigned exactly
//     once) to a string that is not a member of that key's real enum, AND
//     whose (wire key, enum type) pair is backed by a module native to this
//     directory (confidentModuleOK; see the cross-module bullet above).
//     Sound: both the value and which enum applies are fully known, and the
//     enum's members are ground truth from the SDK itself.
//   - NEEDS REVIEW (phantom-field): the struct-literal position only (see
//     the phantom-field bullet above) -- a wire key that resolves to a real
//     enum somewhere in the SDK, but not on the real type of the same name
//     as the gopherstack struct actually being built here.
//   - NEEDS REVIEW (cross-enum-reuse): the guardduty shape itself, where the
//     wrong value is a runtime variable, not a literal, so check A can't see
//     it. reuse.go detects the STRUCTURE instead: a package-level helper that
//     takes a slice parameter and a string "field name" parameter and uses
//     the latter as a literal map[string]any key (dynamicKeyHelper), called
//     twice from the same enclosing function with the textually identical
//     value-source argument but two different literal field-name arguments
//     that resolve to two different real SDK enums with DIFFERENT declared
//     member sets. This never inspects the actual runtime values, so it can
//     never be promoted to confident -- it is flagged purely because reusing
//     one value source across two enums that don't even share the same
//     member set can only be correct by accident.
//   - NEEDS REVIEW (ambiguous-key): a map[string]any entry statically
//     resolved exactly like the confident check, but keyed to a wire key
//     with 2+ real SDK enum candidates (or a Polymorphic one) -- which
//     candidate applies at this emission site is unknown, so this can never
//     be confident, but a value failing membership in at least one candidate
//     is still worth a human's judgement. This is what catches
//     inspector2's rescanDurationState reusing statusEnabled ("ENABLED")
//     under the 13-enum-wide "status" key, valid only for the
//     Status/DelegatedAdminStatus senses of that key and never for the
//     EcrRescanDurationStatus actually in play there -- a real bug the
//     all-or-nothing ambiguous-key filter dropped silently until this tier
//     was added.
//
// A wire-key VALUE position is reached three ways in this repo, all
// covered: a map[string]any composite-literal entry (checkLiteralElt), an
// `out["wireKey"] = value` index-assignment onto an already-built map
// (checkIndexAssignsInFunc) -- added for gopherstack-3dzb, whose real bug
// (comprehend's resourceMap: `out := cloneMap(...); out["Status"] =
// resource.Status`) is exactly this shape and was invisible to the former --
// and a keyed field in a composite literal of a NAMED struct type declared
// in the same package, `SomeType{Field: value}` or `&SomeType{Field:
// value}` (checkStructResponsesInFunc), this repo's other dominant response
// convention alongside map[string]any (`c.JSON(http.StatusOK,
// listApisOutput{...})`). Every position's value resolves the same
// single-hop way: a literal, a same-package const, a
// types.SomeEnumMember/types.SomeEnum("x") selector/conversion, or --
// gopherstack-3dzb -- a `structVar.Field` read of a field this same
// function assigned exactly once (localFieldConsts), keyed by the (local
// variable, field name) pair so two different local structs sharing a
// field name never collide within one function. This closes the blind spot
// gopherstack-3dzb was filed for: an enum-typed value assigned into a
// struct field and only later marshalled onto the wire (this repo's
// dominant status-field pattern) previously defeated resolution entirely --
// confirmed empirically: re-running against comprehend's actual pre-fix
// commit (caf2a5f9f^) produced no finding for any of its four real
// wrong-enum bugs.
//
// The struct-literal position resolves a Go field to its real wire name by
// reading the field's own `json` tag, falling back to an `xml` tag, falling
// back to the Go field name itself only when neither tag is present --
// never assuming the field name IS the wire name, since this repo's
// response structs routinely tag a field under a different name (e.g. Go
// field StatementID tagged json:"StatementId" in services/lambda). A field
// tagged `json:"-"` is excluded outright, and an unkeyed (positional)
// literal element is skipped -- there is no field identity to resolve a
// wire name from without one. Identity is the (struct TYPE, field) pair,
// resolved through that type's own tag-derived field map, never a bare
// field name -- two struct types that happen to both declare a "Status"
// field can never collide, the same discipline localFieldConsts already
// applies one level down for (local variable, field) within one function.
// This is not gated on `c.JSON` at all, deliberately: it mirrors
// checkLiteralsInFunc, which likewise matches any map[string]any literal
// wherever it appears in a function body, not only ones passed directly to
// a response writer -- consistent scope, not a new risk. A composite
// literal of an IMPORTED struct type (an SDK type, or another package's)
// is out of scope: this repo's own response structs, the ones actually
// examined, are declared in the service's own files, where their tags are
// readable.
//
// SCOPE, disclosed rather than silently under-covered: only files directly
// in services/<dir> are scanned (no recursion into subpackages). Local
// value resolution (including the struct-field hop) is a single hop each --
// a value assembled through more indirection than that (a field set in one
// function and read in another, e.g. this exact scan can't see
// comprehend's actual historical bug, which crossed from store.go's
// constructor into a different file's resourceMap; equally, a struct
// literal built in one function and only later passed to a response writer
// after further field mutation in a different function) resolves to
// nothing and produces no finding, never a wrong one. Attempting full
// cross-function dataflow was considered and rejected (gopherstack-3dzb's
// own recommendation): two other auditors in this campaign hit roughly 85
// percent false positives on an ambitious first pass.
//
// checkPhantomField's own blind spot, disclosed rather than fixed: it
// matches a gopherstack struct against a real SDK type of the EXACT same
// name only, expanded one hop through that type's own field references
// (expandOneHopNestedFields, for gopherstack's common "flatten a wrapper +
// summary type into one local struct" pattern -- confirmed live, amplify's
// Job wraps Steps/Summary with Status/Type actually on the nested
// JobSummary). It does NOT follow the AWS naming convention where a List
// operation's summary type carries a "Summary"/"Detail" suffix the full
// type lacks (confirmed live: securityhub's real
// ConfigurationPolicyAssociationSummary has AssociationStatus/AssociationType,
// but gopherstack's local ConfigurationPolicyAssociation -- matched against
// the real ConfigurationPolicyAssociation, a different, smaller type --
// reports both as phantom; same shape for swf's ActivityType/WorkflowType).
// This yields a small residual false-positive rate in the phantom-field
// kind specifically, not chased further: fuzzy suffix matching against
// every type in a module risks trading one systematic false-positive class
// for another, and phantom-field is NEEDS REVIEW, not CONFIDENT -- a human
// judgement call was always the intended outcome here.
//
// Usage:
//
//	go run ./cmd/enumcheck                   # report to stdout
//	go run ./cmd/enumcheck -json out.json     # also write full finding list as JSON
//
// Exit codes: 0 no confident findings (needs-review hits may still print),
// 1 a run error, 2 at least one confident finding.
package main

import (
	"errors"
	"flag"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sort"
)

const (
	exitClean      = 0
	exitRunError   = 1
	exitConfidence = 2
)

func main() {
	jsonOut := flag.String("json", "", "write the full finding list to this path as JSON")
	flag.Parse()

	findings, err := run()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(exitRunError)
	}

	if *jsonOut != "" {
		if werr := writeJSON(*jsonOut, findings); werr != nil {
			fmt.Fprintln(os.Stderr, "write json:", werr)
			os.Exit(exitRunError)
		}
	}

	printReport(findings)
	os.Exit(exitCode(findings))
}

func run() ([]finding, error) {
	repoRoot, err := repoRootDir()
	if err != nil {
		return nil, err
	}

	cache, err := gomodcacheDir(repoRoot)
	if err != nil {
		return nil, err
	}

	goModVersions, err := loadGoModVersions(filepath.Join(repoRoot, "go.mod"))
	if err != nil {
		return nil, err
	}

	svcDirs, err := serviceDirs(filepath.Join(repoRoot, "services"))
	if err != nil {
		return nil, err
	}

	var all []finding

	for _, dir := range svcDirs {
		found, scanErr := auditServiceDir(dir, repoRoot, cache, goModVersions)
		if scanErr != nil {
			return nil, fmt.Errorf("%s: %w", dir, scanErr)
		}

		all = append(all, found...)
	}

	sort.Slice(all, func(i, j int) bool {
		if all[i].File != all[j].File {
			return all[i].File < all[j].File
		}

		return all[i].Line < all[j].Line
	})

	return all, nil
}

func serviceDirs(svcRoot string) ([]string, error) {
	entries, err := os.ReadDir(svcRoot)
	if err != nil {
		return nil, err
	}

	var dirs []string

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}

		dirs = append(dirs, filepath.Join(svcRoot, e.Name()))
	}

	sort.Strings(dirs)

	return dirs, nil
}

// auditServiceDir resolves every aws-sdk-go-v2 module dir's own files
// import, merges each module's enum registry and wire-key ground truth, and
// runs both checks. A service with no resolvable SDK module (no pinned
// aws-sdk-go-v2 import, e.g. opsworks/qldb) or with an SDK module that has
// no types/enums.go or deserializers.go to read contributes nothing --
// never an error, since "nothing to check" is a normal, common outcome.
func auditServiceDir(dir, repoRoot, cache string, goModVersions map[string]string) ([]finding, error) {
	mods, err := resolveServiceModules(dir)
	if err != nil {
		return nil, err
	}

	reg := &enumRegistry{
		membersByType: map[string]map[string]bool{},
		constByIdent:  map[string]enumConst{},
		nativeModules: nativeModuleSet(dir, mods),
	}
	wireKeys := map[string]wireKeyFact{}

	for _, mod := range mods {
		ver, ok := goModVersions[mod]
		if !ok {
			continue
		}

		if loadErr := mergeModuleGroundTruth(cache, mod, ver, reg, wireKeys); loadErr != nil {
			return nil, loadErr
		}
	}

	if len(wireKeys) == 0 {
		return nil, nil
	}

	return scanPackage(dir, reg, wireKeys, repoRoot)
}

// nativeModuleSet is this directory's SDK module ground truth for
// enumRegistry.confidentModuleOK: the subset of mods whose OWN module name
// equals dir's own basename exactly -- a live structural comparison of two
// already-known strings, never a hand-maintained dir->module override
// table (see resolveServiceModules's own doc comment for why this repo
// avoids those). Import location (production vs test file) was tried and
// rejected: this repo's dominant convention -- confirmed for guardduty by
// the package doc comment, and equally true of ec2 itself -- is that even a
// directory's OWN eponymous SDK is referenced only from a *_test.go
// round-trip client, never production code, so "does a non-test file
// import it" cannot tell a directory's own SDK apart from an incidental
// second one. Name equality can: services/ec2 and its ec2 SDK share a name,
// services/ec2 and the outposts SDK it also imports (only in
// cross_service_test.go, aws-sdk-go-v2/service/outposts) do not.
//
// When dir's basename matches none of mods at all (this repo's directory
// names frequently diverge from their SDK module's own name -- cognitoidp
// vs cognitoidentityprovider, ...), the result is empty, which
// confidentModuleOK treats as "nothing to prefer over" and refuses
// nothing: this only ever narrows an already-multi-module directory whose
// own name it can positively identify, never a single-module one.
func nativeModuleSet(dir string, mods []string) map[string]bool {
	base := filepath.Base(dir)
	native := map[string]bool{}

	for _, m := range mods {
		if m == base {
			native[m] = true
		}
	}

	return native
}

func mergeModuleGroundTruth(cache, mod, ver string, reg *enumRegistry, wireKeys map[string]wireKeyFact) error {
	modPath := filepath.Join(cache, "github.com", "aws", "aws-sdk-go-v2", "service", mod+"@"+ver)

	enumsPath := filepath.Join(modPath, sdkTypesPkgName, "enums.go")
	if _, statErr := os.Stat(enumsPath); errors.Is(statErr, os.ErrNotExist) {
		return nil
	} else if statErr != nil {
		return statErr
	}

	modReg, err := loadEnumRegistry(enumsPath)
	if err != nil {
		return err
	}

	mergeEnumRegistry(reg, modReg)

	deserPath := filepath.Join(modPath, "deserializers.go")
	if _, statErr := os.Stat(deserPath); errors.Is(statErr, os.ErrNotExist) {
		return nil
	} else if statErr != nil {
		return statErr
	}

	modWireKeys, modWireFields, err := wireGroundTruth(deserPath, modReg)
	if err != nil {
		return err
	}

	for key, fact := range modWireKeys {
		wireKeys[key] = mergeWireKeyFact(wireKeys[key], fact)

		for _, enumType := range fact.Enums {
			reg.recordKeyEnumModule(key, enumType, mod)
		}
	}

	typesPath := filepath.Join(modPath, sdkTypesPkgName, "types.go")
	if _, statErr := os.Stat(typesPath); statErr == nil {
		nestedRefs, nerr := loadNestedTypeRefs(typesPath)
		if nerr != nil {
			return nerr
		}

		modWireFields = expandOneHopNestedFields(modWireFields, nestedRefs)
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}

	mergeWireFields(reg, modWireFields)

	return nil
}

func mergeWireFields(reg *enumRegistry, add map[string]map[string]bool) {
	if reg.wireFieldsByType == nil {
		reg.wireFieldsByType = map[string]map[string]bool{}
	}

	for typeName, keys := range add {
		if reg.wireFieldsByType[typeName] == nil {
			reg.wireFieldsByType[typeName] = map[string]bool{}
		}

		for k := range keys {
			reg.wireFieldsByType[typeName][k] = true
		}
	}
}

func mergeWireKeyFact(existing, add wireKeyFact) wireKeyFact {
	return wireKeyFact{
		Enums:       mergeUnique(existing.Enums, add.Enums),
		Polymorphic: existing.Polymorphic || add.Polymorphic,
	}
}

func mergeEnumRegistry(dst, src *enumRegistry) {
	for typeName, members := range src.membersByType {
		if dst.membersByType[typeName] == nil {
			dst.membersByType[typeName] = map[string]bool{}
		}

		for v := range members {
			dst.membersByType[typeName][v] = true
		}
	}

	maps.Copy(dst.constByIdent, src.constByIdent)
}

func mergeUnique(existing, add []string) []string {
	seen := map[string]bool{}
	for _, v := range existing {
		seen[v] = true
	}

	for _, v := range add {
		if !seen[v] {
			seen[v] = true

			existing = append(existing, v)
		}
	}

	return existing
}

func exitCode(findings []finding) int {
	for _, f := range findings {
		if f.Confident {
			return exitConfidence
		}
	}

	return exitClean
}
