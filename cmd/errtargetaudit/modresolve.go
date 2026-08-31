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
	"sort"
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

// loadGoModVersions parses go.mod for the pinned version of every
// aws-sdk-go-v2/service/* requirement, keyed by module name -- same approach
// as cmd/errcodeaudit/cmd/enumcheck/cmd/zeroguard's modresolve.go.
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
// module ANY .go file in dir imports -- test files included, since a
// service's typed SDK client round-trip is often only reached from its own
// *_test.go files.
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

	sort.Strings(out)

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

func serviceDirs(svcRoot string) ([]string, error) {
	entries, err := os.ReadDir(svcRoot)
	if err != nil {
		return nil, err
	}

	var dirs []string

	for _, e := range entries {
		if e.IsDir() {
			dirs = append(dirs, filepath.Join(svcRoot, e.Name()))
		}
	}

	sort.Strings(dirs)

	return dirs, nil
}

func fileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false, nil
	}

	return err == nil, err
}
