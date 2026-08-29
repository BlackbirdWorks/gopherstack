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
// TWO CHECKS, two confidence levels (see scan.go/reuse.go for the full
// mechanics):
//
//   - CONFIDENT (literal-value): a map[string]any entry keyed to a resolved
//     wire key whose value statically resolves (a string literal, a
//     same-package string const, or a types.SomeEnumMember/types.SomeEnum("x")
//     selector/conversion) to a string that is not a member of that key's
//     real enum. Sound: the value is fully known and the enum's members are
//     ground truth from the SDK itself.
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
//
// SCOPE, disclosed rather than silently under-covered: only files directly
// in services/<dir> are scanned (no recursion into subpackages); only
// explicitly-typed `map[string]any`/`map[string]interface{}` composite
// literals are examined (a *_test.go-free *Output struct literal, or an
// elided inner literal in a slice of maps, is invisible to this scan); local
// value resolution is a single hop (one `:=` with a literal RHS, or one
// package-level const) -- a value assembled through more indirection than
// that resolves to nothing and produces no finding, never a wrong one.
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
