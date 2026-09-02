package main

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/mod/modfile"
)

const (
	sdkServiceModulePrefix = "github.com/aws/aws-sdk-go-v2/service/"
	// sdkTypesPkgName is this repo's universal unaliased import name for a
	// pinned SDK's types package, both in the SDK's own generated code and
	// in every gopherstack service that imports it directly.
	sdkTypesPkgName = "types"
)

func repoRootDir() (string, error) {
	out, err := exec.CommandContext(context.Background(), "go", "list", "-m", "-f", "{{.Dir}}").Output()
	if err != nil {
		return "", fmt.Errorf("go list -m: %w", err)
	}

	return strings.TrimSpace(string(out)), nil
}

func gomodcacheDir(repoRoot string) (string, error) {
	cmd := exec.CommandContext(context.Background(), "go", "env", "GOMODCACHE")
	cmd.Dir = repoRoot

	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("go env GOMODCACHE: %w", err)
	}

	return strings.TrimSpace(string(out)), nil
}

// loadGoModVersions parses go.mod via golang.org/x/mod/modfile (not a hand
// rolled scan, so both block-style and single-line require statements are
// covered) and returns the pinned version of every aws-sdk-go-v2/service/*
// requirement, keyed by module name -- same approach as cmd/checkpins.
func loadGoModVersions(goModPath string) (map[string]string, error) {
	data, err := os.ReadFile(goModPath)
	if err != nil {
		return nil, err
	}

	f, err := modfile.Parse(goModPath, data, nil)
	if err != nil {
		return nil, err
	}

	versions := make(map[string]string, len(f.Require))

	for _, req := range f.Require {
		name, ok := strings.CutPrefix(req.Mod.Path, sdkServiceModulePrefix)
		if !ok {
			continue
		}

		versions[name] = req.Mod.Version
	}

	return versions, nil
}

// resolveServiceModules returns, deduped, every aws-sdk-go-v2/service/<name>
// module ANY .go file in dir imports -- test files included, and parsed with
// parser.ImportsOnly (imports only, no bodies, cheap). Read straight from
// go/ast's own parsed import specs (sdkModuleFromImportPath), not a text
// scan or a hand-maintained dir->module override table, so it works
// regardless of how a service's directory name diverges from its SDK module
// name (cognitoidp -> cognitoidentityprovider, ...). Test files matter here:
// most service packages build wire responses as bare map[string]any and
// never import the typed SDK client at all in non-test code -- confirmed
// live for guardduty, whose only aws-sdk-go-v2/service/guardduty import
// anywhere is in its *_test.go round-trip clients (this repo's
// sdk_completeness_test.go convention, on 158 of 161 services).
func resolveServiceModules(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	fset := token.NewFileSet()
	seen := map[string]bool{}

	var out []string

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") {
			continue
		}

		f, perr := parser.ParseFile(fset, filepath.Join(dir, e.Name()), nil, parser.ImportsOnly)
		if perr != nil {
			return nil, perr
		}

		for _, imp := range f.Imports {
			name, ok := sdkModuleFromImportPath(imp)
			if !ok || seen[name] {
				continue
			}

			seen[name] = true

			out = append(out, name)
		}
	}

	return out, nil
}

func sdkModuleFromImportPath(imp *ast.ImportSpec) (string, bool) {
	path, err := strconv.Unquote(imp.Path.Value)
	if err != nil {
		return "", false
	}

	name, ok := strings.CutPrefix(path, sdkServiceModulePrefix)
	if !ok {
		return "", false
	}

	if idx := strings.Index(name, "/"); idx >= 0 {
		name = name[:idx]
	}

	return name, true
}
