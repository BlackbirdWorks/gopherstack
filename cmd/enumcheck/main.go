// Command enumcheck finds values gopherstack emits into a wire response
// field whose real pinned aws-sdk-go-v2 type is a named string enum, but
// that are not members of that enum's real declared value set --
// gopherstack-6flj's guardduty class (GetUsageStatistics.sumByDataSource
// emitting DetectorFeature names like "S3_DATA_EVENTS" under a field whose
// real type is the unrelated six-member types.DataSource enum). A typed
// client decodes this without error -- no key check and no shape check can
// see it, only comparing the emitted VALUE against the target enum's real
// declared members does.
//
// GROUND TRUTH, not a naming guess. For each services/<dir>, the pinned
// aws-sdk-go-v2/service/<mod>@<ver> module is resolved from that service's
// own import paths (go/ast) cross-referenced against go.mod
// (golang.org/x/mod/modfile, same approach as cmd/checkpins). Three sources
// feed enumRegistry:
//
//   - types/enums.go: every `type X string` with a `const ( XFoo X = "FOO";
//     ... )` block gives X's real declared member set (loadEnumRegistry).
//   - api_op_*.go and types/types.go, via cmd/internal/sdkshape (shared with
//     cmd/structfielddiff): every real struct's own field name and declared
//     Go type -- enumRegistry.sdkFieldTypes. The SDK's generated code
//     carries no json tags at all, so for a JSON-family protocol the wire
//     name IS the Go field name.
//   - deserializers.go: a `case "wireKey": ... sv.Field = types.SomeEnum(jtv)`
//     structural match (no name-guessing) still backs the bare-key
//     candidate set the map[string]any/index-assignment paths use, and
//     confidentModuleOK's cross-module native-module check (gopherstack-7fps:
//     services/ec2 imports both ec2 and outposts; a wire key's only
//     candidate coming from the non-native module is refused). query/
//     EC2-query/REST-XML protocols use an xml.Decoder with no such switch,
//     so this resolves zero wire keys for them -- same disclosed scope as
//     cmd/keycheck.
//
// gopherstack-cpztm found the ORIGINAL design's flaw: it compared an emitted
// value against every enum SDK-wide that happened to share the bare wire
// key ("status", "type", ...), rather than resolving the emitting Go field
// to the ONE real SDK member it actually maps to -- 104 eks/sagemaker/glue
// findings, 102 false. Fixed by resolving precisely wherever a Go field
// exists to resolve through:
//
//   - Named-struct-literal position (checkStructResponsesInFunc /
//     resolveStructField, structresp.go): the emitting field's wire name
//     (its own json/xml tag, or the Go name itself) is looked up directly
//     in enumRegistry.sdkFieldTypes for the real same-named SDK type,
//     expanded one hop through a directly nested field's own type (amplify's
//     Job -> JobSummary shape) when absent on the type itself
//     (enumRegistry.resolveRealField). Four outcomes, never a candidate set:
//     the real type is unknown (kindUnresolved), the field doesn't exist on
//     it even one hop away (kindPhantomField -- gopherstack-7fps: cloudtrail's
//     Event.EventCategory, sagemaker's PipelineExecutionStep.StepType, both
//     matched to an unrelated real operation's enum before this fix), the
//     field is real but not enum-typed (a plain *string -- ItemError.Code,
//     DevEndpoint.Status, BatchDescribeModelPackageError.ErrorCode -- never
//     checked), or the field resolves to exactly one real enum (checked,
//     confident).
//   - map[string]any composite-literal entry (checkLiteralElt), and an
//     `out["wireKey"] = value` index-assignment onto an already-built map
//     (checkIndexAssignsInFunc, gopherstack-3dzb) have no Go field to
//     resolve through at all, so they still fall back to the bare-key
//     candidate set (evalKeyValue): a single real SDK-wide candidate is
//     still confident (sound by elimination), but 2+ candidates (or a
//     Polymorphic plain-string sighting elsewhere) can no longer be reported
//     as a needs-review finding -- which sense applies here is unknown, so a
//     value failing at least one candidate lands in the UNRESOLVED bucket
//     instead (kindUnresolved), never an accusation.
//   - cross-enum-reuse (checkCrossEnumReuse, reuse.go) is unaffected: it
//     never compares an actual value against any enum at all, only flags the
//     STRUCTURE of one value source feeding two wire keys whose real SDK
//     enums declare different member sets -- never confident, but not part
//     of the bare-key false-positive class either.
//
// Every value position resolves the same single-hop way: a literal, a
// same-package const, a types.SomeEnumMember/types.SomeEnum("x")
// selector/conversion, or a `structVar.Field` read of a field this same
// function assigned exactly once (localFieldConsts, gopherstack-3dzb),
// keyed by the (local variable, field name) pair so two different local
// structs sharing a field name never collide. The struct-literal position
// separately resolves a Go field to its real WIRE NAME via its own `json`
// tag, falling back to `xml`, falling back to the Go name itself -- this
// repo's structs routinely tag a field under a different name (e.g. Go
// field StatementID tagged json:"StatementId" in services/lambda). A field
// tagged `json:"-"` is excluded outright; an unkeyed (positional) literal
// element is skipped. Identity throughout is the (struct TYPE, field) pair,
// never a bare field name.
//
// SCOPE, disclosed rather than silently under-covered: only files directly
// in services/<dir> are scanned (no recursion). Local value resolution
// (including the struct-field hop) is a single hop each -- a value
// assembled through more indirection resolves to nothing and produces no
// finding, never a wrong one (gopherstack-3dzb's own recommendation after
// two auditors hit ~85% false positives attempting full dataflow).
// resolveRealField's own nested-type expansion is likewise one hop only:
// this does NOT follow AWS's convention where a List operation's summary
// type carries a "Summary"/"Detail" suffix the full type lacks (confirmed
// live: securityhub's real ConfigurationPolicyAssociationSummary has
// AssociationStatus/AssociationType, but gopherstack's local
// ConfigurationPolicyAssociation -- matched against the real, smaller,
// same-named type -- resolves to unresolved or phantom instead; same shape
// for swf's ActivityType/WorkflowType).
//
// Usage:
//
//	go run ./cmd/enumcheck                    # report to stdout, every service
//	go run ./cmd/enumcheck -dir eks            # limit to services/eks
//	go run ./cmd/enumcheck -json out.json      # also write the full finding list as JSON
//
// Exit codes: 0 no confident findings (needs-review/unresolved hits may
// still print), 1 a run error, 2 at least one confident finding.
package main

import (
	"errors"
	"flag"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sort"

	"github.com/blackbirdworks/gopherstack/cmd/internal/sdkshape"
)

const (
	exitClean      = 0
	exitRunError   = 1
	exitConfidence = 2
)

func main() {
	jsonOut := flag.String("json", "", "write the full finding list to this path as JSON")
	dirFilter := flag.String("dir", "", "limit the scan to a single services/<dir> (basename, e.g. eks)")
	flag.Parse()

	findings, err := run(*dirFilter)
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

func run(dirFilter string) ([]finding, error) {
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

	if dirFilter != "" {
		svcDirs, err = filterServiceDir(svcDirs, dirFilter)
		if err != nil {
			return nil, err
		}
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

// errNoSuchServiceDir is wrapped with the requested -dir value when it
// matches no services/<dir> basename.
var errNoSuchServiceDir = errors.New("no such services directory")

// filterServiceDir narrows dirs to the one whose basename equals name.
func filterServiceDir(dirs []string, name string) ([]string, error) {
	for _, d := range dirs {
		if filepath.Base(d) == name {
			return []string{d}, nil
		}
	}

	return nil, fmt.Errorf("%w: %s", errNoSuchServiceDir, name)
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

	if fieldErr := mergeSDKFieldTypes(modPath, reg); fieldErr != nil {
		return fieldErr
	}

	deserPath := filepath.Join(modPath, "deserializers.go")
	if _, statErr := os.Stat(deserPath); errors.Is(statErr, os.ErrNotExist) {
		return nil
	} else if statErr != nil {
		return statErr
	}

	modWireKeys, err := wireGroundTruth(deserPath, modReg)
	if err != nil {
		return err
	}

	for key, fact := range modWireKeys {
		wireKeys[key] = mergeWireKeyFact(wireKeys[key], fact)

		for _, enumType := range fact.Enums {
			reg.recordKeyEnumModule(key, enumType, mod)
		}
	}

	return nil
}

// mergeSDKFieldTypes loads modPath's own declared struct shapes via
// cmd/internal/sdkshape (the same api_op_*.go/types.go parse
// cmd/structfielddiff uses) and records each field's bare declared Go type
// into reg.sdkFieldTypes -- gopherstack-cpztm's precise per-field
// resolution. A module with neither file readable contributes nothing, same
// "nothing to check" discipline as the enum/deserializer ground truth.
func mergeSDKFieldTypes(modPath string, reg *enumRegistry) error {
	structs, _, err := sdkshape.LoadModuleStructs(modPath)
	if err != nil {
		return err
	}

	if reg.sdkFieldTypes == nil {
		reg.sdkFieldTypes = map[string]map[string]string{}
	}

	for typeName, def := range structs {
		if reg.sdkFieldTypes[typeName] == nil {
			reg.sdkFieldTypes[typeName] = map[string]string{}
		}

		for _, f := range def.Fields {
			reg.sdkFieldTypes[typeName][f.Name] = sdkshape.BareTypeName(f.Type)
		}
	}

	return nil
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
