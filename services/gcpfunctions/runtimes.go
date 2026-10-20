package gcpfunctions

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"cloud.google.com/go/functions/apiv2/functionspb"
)

type lang string

const (
	langNode   lang = "nodejs"
	langPython lang = "python"
	langGo     lang = "go"
)

// runtimeSpec describes one supported runtime and the base image its Functions Framework runs in.
type runtimeSpec struct {
	id      string
	display string
	image   string
	lang    lang
}

func runtimeTable() []runtimeSpec {
	return []runtimeSpec{
		{"go122", "Go 1.22", "golang:1.22", langGo},
		{"go123", "Go 1.23", "golang:1.23", langGo},
		{"go124", "Go 1.24", "golang:1.24", langGo},
		{"go125", "Go 1.25", "golang:1.25", langGo},
		{"nodejs20", "Node.js 20", "node:20", langNode},
		{"nodejs22", "Node.js 22", "node:22", langNode},
		{"nodejs24", "Node.js 24", "node:24", langNode},
		{"python310", "Python 3.10", "python:3.10", langPython},
		{"python311", "Python 3.11", "python:3.11", langPython},
		{"python312", "Python 3.12", "python:3.12", langPython},
		{"python313", "Python 3.13", "python:3.13", langPython},
	}
}

func nodeEntryFiles() []string {
	return []string{"index.js", "index.mjs", "index.cjs", "function.js", "app.js"}
}

func lookupRuntime(id string) (*runtimeSpec, error) {
	table := runtimeTable()
	for i := range table {
		if table[i].id == id {
			return &table[i], nil
		}
	}

	ids := make([]string, 0, len(table))
	for _, r := range table {
		ids = append(ids, r.id)
	}

	return nil, errInvalid("Runtime %q is not supported by the emulator; supported: %s.", id, strings.Join(ids, ", "))
}

// missing names what the archive lacks for the runtime to start, or "" when it looks runnable.
func (r *runtimeSpec) missing(files []string) string {
	has := func(name string) bool { return slices.Contains(files, name) }

	switch r.lang {
	case langNode:
		if has("package.json") || slices.ContainsFunc(nodeEntryFiles(), has) {
			return ""
		}

		return "no package.json or index.js found at the archive root."
	case langPython:
		if has("main.py") {
			return ""
		}

		return "main.py not found at the archive root."
	case langGo:
		if slices.ContainsFunc(
			files,
			func(f string) bool { return path.Dir(f) == "." && strings.HasSuffix(f, ".go") },
		) {
			return ""
		}

		return "no .go source file found at the archive root."
	}

	return ""
}

func supportedRuntimes(env functionspb.Environment) []*functionspb.ListRuntimesResponse_Runtime {
	out := make([]*functionspb.ListRuntimesResponse_Runtime, 0, len(runtimeTable()))

	if env == functionspb.Environment_GEN_1 {
		return out
	}

	for _, r := range runtimeTable() {
		out = append(out, &functionspb.ListRuntimesResponse_Runtime{
			Name: r.id, DisplayName: r.display,
			Stage:       functionspb.ListRuntimesResponse_GA,
			Environment: functionspb.Environment_GEN_2,
		})
	}

	return out
}

const (
	srcMount  = "/src"
	workspace = "/workspace"
)

const nodeScript = `set -e
mkdir -p ` + workspace + ` && cp -a ` + srcMount + `/. ` + workspace + ` && cd ` + workspace + `
if [ -f package.json ]; then npm install --omit=dev --no-audit --no-fund --loglevel=error; fi
exec npx --yes @google-cloud/functions-framework --target="$FUNCTION_TARGET" \
  --signature-type="$FUNCTION_SIGNATURE_TYPE" --port="$PORT"`

const pythonScript = `set -e
mkdir -p ` + workspace + ` && cp -a ` + srcMount + `/. ` + workspace + ` && cd ` + workspace + `
pip install --quiet --no-cache-dir functions-framework
if [ -f requirements.txt ]; then pip install --quiet --no-cache-dir -r requirements.txt; fi
exec functions-framework --target="$FUNCTION_TARGET" \\
  --signature-type="$FUNCTION_SIGNATURE_TYPE" --port="$PORT" --host=0.0.0.0`

const goScript = `set -e
mkdir -p ` + workspace + ` && cp -a ` + srcMount + `/. ` + workspace + ` && cd ` + workspace + `
go get github.com/GoogleCloudPlatform/functions-framework-go@latest
go mod tidy
exec go run ./` + goMainDir

const goMainDir = "gopherstack_main"

var goModuleRe = regexp.MustCompile(`(?m)^\s*module\s+(\S+)`)

const goMainSource = `package main

import (
	"log"
	"os"

	"github.com/GoogleCloudPlatform/functions-framework-go/funcframework"
	_ "%s"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	if err := funcframework.Start(port); err != nil {
		log.Fatalf("funcframework.Start: %%v", err)
	}
}
`

// prepareGo adds the Functions Framework entry point that imports the user's root package.
func prepareGo(dir string) error {
	mod := "function"
	goMod := filepath.Join(dir, "go.mod")

	if raw, err := os.ReadFile(goMod); err == nil {
		if m := goModuleRe.FindSubmatch(raw); m != nil {
			mod = string(m[1])
		}
	} else if err = os.WriteFile(goMod, []byte("module "+mod+"\n\ngo 1.22\n"), fileMode); err != nil {
		return fmt.Errorf("write go.mod: %w", err)
	}

	mainDir := filepath.Join(dir, goMainDir)
	if err := os.MkdirAll(mainDir, dirMode); err != nil {
		return fmt.Errorf("create %s: %w", goMainDir, err)
	}

	src := fmt.Sprintf(goMainSource, mod)
	if err := os.WriteFile( //nolint:gosec // path is inside our own scratch dir
		filepath.Join(mainDir, "main.go"),
		[]byte(src),
		fileMode,
	); err != nil {
		return fmt.Errorf("write main.go: %w", err)
	}

	return nil
}

func (r *runtimeSpec) script() string {
	switch r.lang {
	case langNode:
		return nodeScript
	case langPython:
		return pythonScript
	case langGo:
		return goScript
	}

	return ""
}
