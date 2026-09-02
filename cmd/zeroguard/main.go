// Command zeroguard finds gopherstack Update/Put/Modify handlers that
// cannot distinguish "the caller omitted this field" from "the caller sent
// the zero value" and silently resolve the ambiguity the wrong way --
// gopherstack-6flj's newest bug class, first confirmed in
// apigatewayv2.UpdateAuthorizer and fixed in commit 406c1dcc3.
//
// TWO SIGNALS, read straight from the pinned aws-sdk-go-v2 source with
// go/ast (the SDK module resolution is cmd/enumcheck's own approach,
// modresolve.go, copied verbatim):
//
//   - A: a gopherstack <Op>Input struct field declared as a plain
//     predeclared scalar (int32, int64, int, bool, string, float32,
//     float64) where the real pinned SDK's <Op>Input declares the SAME
//     field (matched case-insensitively, since gopherstack and the SDK
//     sometimes differ only in an abbreviation's casing --
//     AuthorizerResultTTLInSeconds vs. AuthorizerResultTtlInSeconds) as a
//     POINTER to that same scalar type. Read from api_op_<Op>.go's own
//     struct declaration, sdkfields.go -- not a name guess, since every
//     aws-sdk-go-v2 service is smithy-go codegen and this shape is uniform
//     across all wire protocols, unlike enum/wire-key ground truth which
//     varies by protocol.
//   - B: an if-statement in the handler gating a use of that field on it
//     being non-zero (!= 0, != "") or, for a bool field, directly truthy --
//     the exact shape the pre-fix apigatewayv2.UpdateAuthorizer guards had.
//
// CONFIDENT requires BOTH: the real member is a pointer, gopherstack's is
// not, AND a zero-guard gates its application. Signal A alone is common and
// often harmless (many fields are genuinely required, or a required
// identifier is always present from routing and never guarded at all) --
// reported as NEEDS REVIEW.
//
// SCOPE: only files directly in services/<dir> (no recursion into
// subpackages), only Update/Put/Modify-named operations (a Create op takes
// a fresh resource with no prior state an omission could accidentally
// erase), only a handler's OWN Input-struct fields (a nested struct field
// inside, e.g. Route53AutoNaming's DnsConfig.DnsRecords, is a different
// shape -- a pointer-to-struct presence check whose omission needs to
// CASCADE a delete, not a scalar zero-guard -- and is out of this tool's
// signal entirely; see the package's final report for why).
//
// Usage:
//
//	go run ./cmd/zeroguard                   # report to stdout
//	go run ./cmd/zeroguard -json out.json     # also write full finding list as JSON
//
// Exit codes: 0 no confident findings (needs-review hits may still print),
// 1 a run error, 2 at least one confident finding.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

const (
	exitClean      = 0
	exitRunError   = 1
	exitConfidence = 2
)

// sdkModule is one resolved aws-sdk-go-v2/service/<name> module a
// services/<dir> package imports, with its on-disk GOMODCACHE path at the
// version pinned in go.mod.
type sdkModule struct {
	name string
	path string
}

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

	fieldCache := newSDKOpFieldCache()

	var all []finding

	for _, dir := range svcDirs {
		found, scanErr := auditServiceDir(dir, repoRoot, cache, goModVersions, fieldCache)
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
// import (test files included) and scans dir against each resolved
// module's own pinned Input-struct ground truth. A service with no
// resolvable SDK module contributes nothing -- never an error.
func auditServiceDir(
	dir, repoRoot, cache string, goModVersions map[string]string, fieldCache *sdkOpFieldCache,
) ([]finding, error) {
	names, err := resolveServiceModules(dir)
	if err != nil {
		return nil, err
	}

	var mods []sdkModule

	for _, name := range names {
		ver, ok := goModVersions[name]
		if !ok {
			continue
		}

		modPath := filepath.Join(cache, "github.com", "aws", "aws-sdk-go-v2", "service", name+"@"+ver)
		mods = append(mods, sdkModule{name: name, path: modPath})
	}

	if len(mods) == 0 {
		return nil, nil
	}

	return scanPackage(dir, repoRoot, mods, fieldCache)
}

func exitCode(findings []finding) int {
	for _, f := range findings {
		if f.Confident {
			return exitConfidence
		}
	}

	return exitClean
}
