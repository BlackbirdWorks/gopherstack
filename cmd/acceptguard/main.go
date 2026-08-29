// Command acceptguard finds gopherstack handlers that accept a REQUEST
// member the real pinned aws-sdk-go-v2 Input type does not declare -- the
// mirror image of every wire bug this campaign has found so far, which were
// all on the response side (a member emitted under the wrong key, dropped,
// or invented). networkmanager's ListAttachments/ListPeerings EdgeLocation
// filter was the case that first surfaced this direction (gopherstack-6flj);
// see this package's doc comment continuation in scan.go and this tool's own
// test file for why that specific historical commit (5591e3014) turned out,
// on structural inspection, NOT to be an instance of this class after all --
// an important calibration finding in its own right, not a tool bug.
//
// GROUND TRUTH, not a naming guess, reusing cmd/enumcheck's and
// cmd/zeroguard's own per-service SDK module resolution (modresolve.go,
// copied verbatim) and go/ast struct parsing (sdkfields.go):
//
//   - A gopherstack top-level struct whose name ends in one of
//     requestSuffixes (Input/Request/Params/Req) is a candidate "what this
//     handler accepts" shape. Stripping the suffix and capitalizing the
//     first rune proposes a real AWS operation name (createVpcAttachmentReq
//     -> CreateVpcAttachment).
//   - That candidate is verified, not assumed: it only proceeds if the
//     pinned SDK module actually declares api_op_<Op>.go with an
//     <Op>Input struct (sdkfields.go's fieldsFor).
//   - Every one of the candidate struct's own top-level fields is compared,
//     case/abbreviation-folded (zeroguard's matchSDKField precedent), against
//     that real Input's field set. A field present there is fine and
//     produces nothing.
//   - A field ABSENT from the target op's real Input is only reported once
//     REACHABILITY is confirmed structurally: some function in the package
//     binds a local identifier to the struct's type (a parameter or `var`
//     declaration) and reads `<that identifier>.<field>` somewhere in its
//     body. A decoded-but-never-read field is this repo's documented
//     non-bug (an emulator-internal hook unreachable from the real wire
//     path) and is silently skipped, not reported at either confidence
//     level.
//   - CONFIDENT (kindInvented): the field's name (folded) matches NO member
//     of ANY real Input struct anywhere in the resolved SDK module -- not
//     just absent from this op, absent from the entire service's real
//     surface. Invented wholesale.
//   - NEEDS REVIEW (kindSibling): the field's name IS a real member, just of
//     a different operation's Input in the same module -- the repo's other
//     documented non-bug (a field that lives on a sibling or Create/Update-
//     paired Input) made concrete and worth a human's look rather than
//     silently dropped, since the field could genuinely be wired to the
//     wrong op.
//
// PROTOCOL SCOPE, disclosed rather than silently under-covered: this signal
// only sees a REQUEST shape gopherstack represents as a genuine Go struct
// with named fields -- every JSON-family service this repo has (a decoded
// body, or an apigatewayv2-style hand-populated params struct) qualifies.
// Query and ec2-query services pull request members out of url.Values by
// literal key (`vals.Get("SomeParam")`) with no struct to enumerate fields
// from at all, and REST-XML services with flattened/indexed member names
// (Filters.Filter.1.Name) would need a wire-key grammar this tool does not
// implement -- both protocol families see zero candidates and zero
// findings, not a false "clean" verdict for a different reason: there was
// never a struct here for this tool to examine in the first place.
//
// SCOPE, disclosed rather than silently under-covered: only files directly
// in services/<dir> are scanned for candidate structs and their usage (no
// recursion into subpackages, no _test.go files); only a struct's own
// TOP-LEVEL fields are checked -- a mismatch nested inside a pointer-to-
// struct member (e.g. Options *vpcOptionsWire) is a different shape and out
// of this tool's signal entirely, matching zeroguard's own disclosed nested-
// struct exclusion.
//
// Usage:
//
//	go run ./cmd/acceptguard                   # report to stdout
//	go run ./cmd/acceptguard -json out.json     # also write full finding list as JSON
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

	fieldCache := newSDKFieldCache()

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
// import (test files included -- resolveServiceModules's own doc comment)
// and scans dir against each resolved module's own pinned Input ground
// truth. A service with no resolvable SDK module contributes nothing --
// never an error.
func auditServiceDir(
	dir, repoRoot, cache string, goModVersions map[string]string, fieldCache *sdkFieldCache,
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

	preferOwnModule(mods, filepath.Base(dir))

	return scanPackage(dir, repoRoot, mods, fieldCache)
}

// preferOwnModule reorders mods in place so the module named for the
// service's own directory (dax/handler.go imports dax's own SDK for its
// round-trip tests, matching every service here) sorts first -- ahead of
// any OTHER aws-sdk-go-v2 module a package's test files import for cross-
// service validation (dax's own dataplane_integration_test.go imports
// dynamodb; networkmanager's crossservice.go pattern has services import
// each other's real backends too). Without this, resolveOpFields's
// first-match-wins search over mods could resolve an operation name TWO
// unrelated services both happen to define (TagResource/UntagResource are
// nearly universal) against the WRONG service's Input shape entirely --
// confirmed live: dax's own TagResourceInput/UntagResourceInput both
// declare ResourceName correctly, but dynamodb's own TagResourceInput uses
// ResourceArn, and alphabetical file iteration resolved dax's module
// import after dynamodb's, producing a false CONFIDENT finding on a field
// that was never wrong.
func preferOwnModule(mods []sdkModule, dirName string) {
	for i, m := range mods {
		if m.name == dirName {
			mods[0], mods[i] = mods[i], mods[0]

			return
		}
	}
}

func exitCode(findings []finding) int {
	for _, f := range findings {
		if f.Confident {
			return exitConfidence
		}
	}

	return exitClean
}
