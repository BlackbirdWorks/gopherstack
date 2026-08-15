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
	"sort"
	"strings"
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

var fieldNameRe = regexp.MustCompile(`^([A-Z]\w*)\s+(.+)$`)

const requiredLine = "This member is required."

// maxDepth bounds nested-struct expansion so a self-referential or deeply
// nested SDK type (e.g. a policy document tree) can't recurse forever.
const maxDepth = 6

type sdkField struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
}

type sdkStruct struct {
	Name   string     `json:"name"`
	Fields []sdkField `json:"fields"`
}

type opDump struct {
	Op     string      `json:"op"`
	Input  []sdkStruct `json:"input"`
	Output []sdkStruct `json:"output"`
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

	structs, opNames, err := parseModule(modPath)
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
func dumpOps(structs map[string]sdkStruct, opNames []string, filterOp string) []opDump {
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

// parseModule reads every api_op_*.go file (for op names and Input/Output
// structs) and types/types.go (for nested structs) under modPath, returning
// every struct found keyed by bare name, plus the sorted list of op names.
func parseModule(modPath string) (map[string]sdkStruct, []string, error) {
	structs := map[string]sdkStruct{}

	opNames, err := collectOpFiles(modPath, structs)
	if err != nil {
		return nil, nil, err
	}

	typesFile := filepath.Join(modPath, "types", "types.go")
	if src, readErr := os.ReadFile(typesFile); readErr == nil {
		maps.Copy(structs, parseFile(string(src)))
	}

	sort.Strings(opNames)

	return structs, opNames, nil
}

func collectOpFiles(modPath string, structs map[string]sdkStruct) ([]string, error) {
	entries, err := os.ReadDir(modPath)
	if err != nil {
		return nil, err
	}

	var opNames []string

	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasPrefix(name, "api_op_") || !strings.HasSuffix(name, ".go") ||
			strings.HasSuffix(name, "_test.go") {
			continue
		}

		opName := strings.TrimSuffix(strings.TrimPrefix(name, "api_op_"), ".go")
		opNames = append(opNames, opName)

		src, readErr := os.ReadFile(filepath.Join(modPath, name))
		if readErr != nil {
			continue
		}

		maps.Copy(structs, parseFile(string(src)))
	}

	return opNames, nil
}

// parseFile finds every "type X struct { ... }" in src and returns each as
// an sdkStruct keyed by bare name X.
func parseFile(src string) map[string]sdkStruct {
	out := map[string]sdkStruct{}

	lines := strings.Split(src, "\n")
	typeDeclRe := regexp.MustCompile(`^type\s+(\w+)\s+struct\s*\{`)

	for i := 0; i < len(lines); i++ {
		m := typeDeclRe.FindStringSubmatch(strings.TrimSpace(lines[i]))
		if m == nil {
			continue
		}

		name := m[1]
		body, end := extractBody(lines, i)
		out[name] = sdkStruct{Name: name, Fields: fields(body)}
		i = end
	}

	return out
}

// extractBody returns the lines making up the struct body starting at
// declLine (brace-depth tracked, so a nested struct/map literal never
// closes it early) and the index of the line where it closed.
func extractBody(lines []string, declLine int) ([]string, int) {
	depth := strings.Count(lines[declLine], "{") - strings.Count(lines[declLine], "}")

	var body []string

	i := declLine + 1

	for ; i < len(lines) && depth > 0; i++ {
		depth += strings.Count(lines[i], "{") - strings.Count(lines[i], "}")
		if depth > 0 {
			body = append(body, lines[i])
		}
	}

	return body, i
}

// fields splits body into blank-line-separated top-level field blocks
// (brace-depth tracked) and parses each into an sdkField.
func fields(body []string) []sdkField {
	var (
		out   []sdkField
		block []string
		depth int
	)

	flush := func() {
		if len(block) == 0 {
			return
		}

		if f, ok := parseFieldBlock(block); ok {
			out = append(out, f)
		}

		block = block[:0]
	}

	for _, line := range body {
		if strings.TrimSpace(line) == "" && depth == 0 {
			flush()

			continue
		}

		block = append(block, line)
		depth += strings.Count(line, "{") - strings.Count(line, "}")
	}

	flush()

	return out
}

func parseFieldBlock(block []string) (sdkField, bool) {
	required := false

	var fieldLine string

	for _, l := range block {
		trimmed := strings.TrimSpace(l)
		if trimmed == "// "+requiredLine || trimmed == "//"+requiredLine {
			required = true
		}

		if !strings.HasPrefix(trimmed, "//") && trimmed != "" {
			fieldLine = trimmed
		}
	}

	if fieldLine == "" {
		return sdkField{}, false
	}

	m := fieldNameRe.FindStringSubmatch(fieldLine)
	if m == nil {
		return sdkField{}, false
	}

	if m[1] == "noSmithyDocumentSerde" {
		return sdkField{}, false
	}

	return sdkField{Name: m[1], Type: strings.TrimSpace(m[2]), Required: required}, true
}

// bareTypeName strips pointer/slice/map decoration and a "types." or
// package-qualifier prefix, returning the identifier to look up in structs.
func bareTypeName(t string) string {
	t = strings.TrimPrefix(t, "*")
	t = strings.TrimPrefix(t, "[]")
	t = strings.TrimPrefix(t, "*")

	if strings.HasPrefix(t, "map[") {
		if idx := strings.Index(t, "]"); idx != -1 {
			t = t[idx+1:]
		}

		t = strings.TrimPrefix(t, "*")
	}

	if idx := strings.LastIndex(t, "."); idx != -1 {
		t = t[idx+1:]
	}

	return t
}

// expand walks def's fields, recursively expanding any field whose type
// resolves to a known struct, cycle- and depth-guarded.
func expand(structs map[string]sdkStruct, def sdkStruct, seen map[string]bool, depth int) []sdkStruct {
	if seen[def.Name] || depth > maxDepth {
		return nil
	}

	seen = cloneSeen(seen)
	seen[def.Name] = true

	result := []sdkStruct{def}

	for _, f := range def.Fields {
		nested, ok := structs[bareTypeName(f.Type)]
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

func printStructs(structs []sdkStruct) {
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
