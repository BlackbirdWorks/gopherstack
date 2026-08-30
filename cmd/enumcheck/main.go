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
//     cmd/keycheck.
//
// THREE CHECKS, two confidence levels (see scan.go/reuse.go for the full
// mechanics):
//
//   - CONFIDENT (literal-value): a map[string]any entry, OR an
//     `out["wireKey"] = value` index-assignment onto one, keyed to a
//     resolved wire key with exactly ONE real SDK enum candidate and no
//     Polymorphic plain-string sighting, whose value statically resolves (a
//     string literal, a same-package string const, a
//     types.SomeEnumMember/types.SomeEnum("x") selector/conversion, or a
//     `structVar.Field` read of a field this same function assigned exactly
//     once) to a string that is not a member of that key's real enum.
//     Sound: both the value and which enum applies are fully known, and the
//     enum's members are ground truth from the SDK itself.
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

	reg := &enumRegistry{membersByType: map[string]map[string]bool{}, constByIdent: map[string]enumConst{}}
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

	modWireKeys, err := wireEnumKeys(deserPath, modReg)
	if err != nil {
		return err
	}

	for key, fact := range modWireKeys {
		wireKeys[key] = mergeWireKeyFact(wireKeys[key], fact)
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
