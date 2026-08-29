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

const sdkServiceModulePrefix = "github.com/aws/aws-sdk-go-v2/service/"

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

// loadGoModVersions is the same approach as cmd/enumcheck/cmd/zeroguard: parse
// go.mod with golang.org/x/mod/modfile and return the pinned version of every
// aws-sdk-go-v2/service/* requirement, keyed by module name.
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
// module ANY .go file in dir imports -- test files included, since most
// service packages only reach the typed SDK client from their own
// *_test.go round-trip clients (see cmd/enumcheck's modresolve.go doc
// comment for the guardduty example this same approach was built from).
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
