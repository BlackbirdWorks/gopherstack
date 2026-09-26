// Command structfielddiff dumps every field of every <Op>Input, <Op>Output
// and nested struct the pinned aws-sdk-go-v2 source declares for one or more
// services, so the fields can be diffed by hand against gopherstack's own
// wire model.
//
// This generalizes the mechanical struct-field diff that found real bugs in
// s3 (c9b6c702a) and dynamodb (89eac08ea) beyond those two services. Unlike
// cmd/overwidecandidates and cmd/requiredoutputfields, which each look for
// one shape (over-wide List responses, required-but-unpopulated outputs),
// this tool dumps the FULL field set of every request/response shape so a
// human can compare it field-by-field against gopherstack's handler and
// catch the other bug classes: a field declared on gopherstack's domain
// model and never wired to the wire type, a member modeled with the wrong
// Go type, or a nested struct missing a field its parent has.
//
// It resolves the pinned aws-sdk-go-v2/service/<mod> version from go.mod
// (dirModuleOverride maps the handful of services where the directory name
// and module name diverge, same table as the sibling tools), reads every
// api_op_<Op>.go and types/types.go file from
// $(go env GOMODCACHE)/github.com/aws/aws-sdk-go-v2/service/<mod>@<version>,
// and for each "type X struct {" walks blank-line-separated top-level field
// blocks (brace-depth tracked) to pull out field name, declared type and
// whether the doc comment marks it required. Fields are then expanded
// recursively through nested struct types (cycle-guarded, depth-limited) so
// the dump includes everything reachable from an Input or Output.
//
// This only extracts the SDK side. It says nothing about whether
// gopherstack's handler populates or reads any of it -- that comparison,
// and the hand-verification against the real serializer that the noise
// rate demands, stays a human step.
//
// Usage:
//
//	go run ./cmd/structfielddiff -service sts
//	go run ./cmd/structfielddiff -service sts -op AssumeRole
//	go run ./cmd/structfielddiff -service secretsmanager -json out.json
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/blackbirdworks/gopherstack/cmd/internal/sdkshape"
)

// dirModuleOverride maps services/<dir> to its aws-sdk-go-v2/service module
// name where the two diverge. Same table as cmd/overwidecandidates and
// cmd/requiredoutputfields.
//
//nolint:gochecknoglobals // read-only lookup table, same pattern as account's operationNames map
var dirModuleOverride = map[string]string{
	"awsconfig":      "configservice",
	"ce":             "costexplorer",
	"cognitoidp":     "cognitoidentityprovider",
	"dms":            "databasemigrationservice",
	"elasticsearch":  "elasticsearchservice",
	"elb":            "elasticloadbalancing",
	"elbv2":          "elasticloadbalancingv2",
	"serverlessrepo": "serverlessapplicationrepository",
	"stepfunctions":  "sfn",
}

// maxDepth bounds nested-struct expansion so a self-referential or deeply
// nested SDK type (e.g. a policy document tree) can't recurse forever.
const maxDepth = 6

type opDump struct {
	Op     string               `json:"op"`
	Input  []sdkshape.StructDef `json:"input"`
	Output []sdkshape.StructDef `json:"output"`
}

func main() {
	service := flag.String("service", "", "services/<dir> name (required)")
	op := flag.String("op", "", "limit to a single operation name (optional)")
	jsonPath := flag.String("json", "", "write full dump to this path as JSON instead of stdout text")
	flag.Parse()

	if *service == "" {
		fmt.Fprintln(os.Stderr, "error: -service is required")
		os.Exit(1)
	}

	mod, ver, modPath, err := resolveModule(*service)
	if err != nil {
		fatal(err)
	}

	structs, opNames, err := sdkshape.LoadModuleStructs(modPath)
	if err != nil {
		fatal(err)
	}

	dumps := dumpOps(structs, opNames, *op)

	if *jsonPath != "" {
		writeJSON(*jsonPath, dumps)

		return
	}

	printText(mod, ver, dumps)
}

// errNoVersion is wrapped with the service/module pair that failed to resolve.
var errNoVersion = errors.New("no go.mod version resolved")

// resolveModule maps a services/<dir> name to its pinned aws-sdk-go-v2
// module name, version and on-disk GOMODCACHE path.
func resolveModule(service string) (string, string, string, error) {
	repoRoot, err := repoRootDir()
	if err != nil {
		return "", "", "", err
	}

	cache, err := gomodcache(repoRoot)
	if err != nil {
		return "", "", "", err
	}

	mod := service
	if override, ok := dirModuleOverride[service]; ok {
		mod = override
	}

	goModSrc, err := os.ReadFile(filepath.Join(repoRoot, "go.mod"))
	if err != nil {
		return "", "", "", err
	}

	ver := moduleVersion(string(goModSrc), mod)
	if ver == "" {
		return "", "", "", fmt.Errorf("%w: service %s -> module %s", errNoVersion, service, mod)
	}

	modPath := filepath.Join(cache, "github.com", "aws", "aws-sdk-go-v2", "service", mod+"@"+ver)

	return mod, ver, modPath, nil
}

// dumpOps expands every op in opNames (or just filterOp, when non-empty)
// into its Input/Output field dump.
func dumpOps(structs map[string]sdkshape.StructDef, opNames []string, filterOp string) []opDump {
	dumps := make([]opDump, 0, len(opNames))

	for _, name := range opNames {
		if filterOp != "" && name != filterOp {
			continue
		}

		in, inOK := structs[name+"Input"]
		out, outOK := structs[name+"Output"]

		if !inOK && !outOK {
			continue
		}

		d := opDump{Op: name}
		if inOK {
			d.Input = expand(structs, in, map[string]bool{}, 0)
		}

		if outOK {
			d.Output = expand(structs, out, map[string]bool{}, 0)
		}

		dumps = append(dumps, d)
	}

	return dumps
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

func repoRootDir() (string, error) {
	out, err := exec.CommandContext(context.Background(), "go", "list", "-m", "-f", "{{.Dir}}").Output()
	if err != nil {
		return "", fmt.Errorf("go list -m: %w", err)
	}

	return strings.TrimSpace(string(out)), nil
}

func gomodcache(repoRoot string) (string, error) {
	cmd := exec.CommandContext(context.Background(), "go", "env", "GOMODCACHE")
	cmd.Dir = repoRoot

	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("go env GOMODCACHE: %w", err)
	}

	return strings.TrimSpace(string(out)), nil
}

// moduleVersion finds mod's pinned version in go.mod. go.mod mixes a
// require(...) block with standalone "require x v..." lines -- both forms
// must match.
func moduleVersion(goModSrc, mod string) string {
	pat := regexp.MustCompile(`^(?:require )?github\.com/aws/aws-sdk-go-v2/service/` +
		regexp.QuoteMeta(mod) + `\s+(v\S+)`)

	for line := range strings.SplitSeq(goModSrc, "\n") {
		if m := pat.FindStringSubmatch(strings.TrimSpace(line)); m != nil {
			return m[1]
		}
	}

	return ""
}

// expand walks def's fields, recursively expanding any field whose type
// resolves to a known struct, cycle- and depth-guarded.
func expand(
	structs map[string]sdkshape.StructDef, def sdkshape.StructDef, seen map[string]bool, depth int,
) []sdkshape.StructDef {
	if seen[def.Name] || depth > maxDepth {
		return nil
	}

	seen = cloneSeen(seen)
	seen[def.Name] = true

	result := []sdkshape.StructDef{def}

	for _, f := range def.Fields {
		nested, ok := structs[sdkshape.BareTypeName(f.Type)]
		if !ok {
			continue
		}

		result = append(result, expand(structs, nested, seen, depth+1)...)
	}

	return result
}

func cloneSeen(seen map[string]bool) map[string]bool {
	out := make(map[string]bool, len(seen)+1)
	maps.Copy(out, seen)

	return out
}

func writeJSON(path string, dumps []opDump) {
	f, err := os.Create(path)
	if err != nil {
		fatal(err)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", " ")

	if encErr := enc.Encode(dumps); encErr != nil {
		fatal(encErr)
	}

	fmt.Fprintln(os.Stderr, "wrote", path)
}

func printText(mod, ver string, dumps []opDump) {
	fmt.Fprintf(os.Stdout, "# %s %s -- %d ops\n\n", mod, ver, len(dumps))

	for _, d := range dumps {
		fmt.Fprintf(os.Stdout, "## %s\n\nInput:\n", d.Op)
		printStructs(d.Input)
		fmt.Fprintf(os.Stdout, "\nOutput:\n")
		printStructs(d.Output)
		fmt.Fprintln(os.Stdout)
	}
}

func printStructs(structs []sdkshape.StructDef) {
	if len(structs) == 0 {
		fmt.Fprintln(os.Stdout, "  (none)")

		return
	}

	for _, s := range structs {
		fmt.Fprintf(os.Stdout, "  %s:\n", s.Name)

		for _, f := range s.Fields {
			req := ""
			if f.Required {
				req = " [required]"
			}

			fmt.Fprintf(os.Stdout, "    %-30s %s%s\n", f.Name, f.Type, req)
		}
	}
}
