// Command requiredoutputfields regenerates the required-response-member
// ranking for gopherstack-r80d (see services/_REQUIRED_OUTPUT_CANDIDATES.md).
//
// For each services/<dir>, it resolves the pinned
// aws-sdk-go-v2/service/<mod>@<version> from go.mod (directory name and
// module name diverge for a handful of services — see dirModuleOverride,
// shared with cmd/overwidecandidates), reads every api_op_<Op>.go file from
// $(go env GOMODCACHE)/github.com/aws/aws-sdk-go-v2/service/<mod>@<version>,
// and for each "type <Op>Output struct{...}" walks blank-line-separated
// top-level field blocks (brace-depth tracked, so a nested struct's own
// blank lines never split a block early) flagging any field whose doc
// comment contains the exact line "This member is required."
//
// This counts required OUTPUT members only — it says nothing about whether
// gopherstack's handler actually populates them. That verification is a
// per-service hand-read; this tool only ranks where to spend it.
//
// Usage:
//
//	go run ./cmd/requiredoutputfields                # ranked summary to stdout
//	go run ./cmd/requiredoutputfields -json out.json # full per-op detail
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
)

// dirModuleOverride maps services/<dir> to its aws-sdk-go-v2/service module
// name where the two diverge. Same table as cmd/overwidecandidates.
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

var (
	// Leading quote anchors this to a real import line; unanchored, it also
	// matches a host that merely embeds the path (evil.com/github.com/...).
	sdkImportRe = regexp.MustCompile(`"github\.com/aws/aws-sdk-go-v2/service/([a-z0-9]+)`)
	fieldNameRe = regexp.MustCompile(`^([A-Z]\w*)\b`)
)

// requiredLine is the exact doc-comment line the SDK codegen emits above a
// required field.
const requiredLine = "This member is required."

type opResult struct {
	Op       string   `json:"op"`
	Required []string `json:"required"`
}

type serviceResult struct {
	Mod            string     `json:"mod"`
	Ver            string     `json:"ver"`
	Ops            []opResult `json:"ops"`
	TotalRequired  int        `json:"totalRequired"`
	OpsWithNonzero int        `json:"opsWithNonzero"`
	TotalOps       int        `json:"totalOps"`
}

func main() {
	jsonPath := flag.String("json", "", "write full per-op detail to this path")
	flag.Parse()

	repoRoot, err := repoRootDir()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}

	cache, err := gomodcache(repoRoot)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}

	dirMap, err := buildDirModuleMap(repoRoot)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}

	goModSrc, err := os.ReadFile(filepath.Join(repoRoot, "go.mod"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}

	results := map[string]serviceResult{}

	dirs := make([]string, 0, len(dirMap))
	for d := range dirMap {
		dirs = append(dirs, d)
	}

	sort.Strings(dirs)

	for _, d := range dirs {
		mod := dirMap[d]

		ver := moduleVersion(string(goModSrc), mod)
		if ver == "" {
			fmt.Fprintf(os.Stderr, "warning: no go.mod version resolved for %s -> %s\n", d, mod)

			continue
		}

		modPath := filepath.Join(cache, "github.com", "aws", "aws-sdk-go-v2", "service", mod+"@"+ver)
		results[d] = parseServiceOutputs(mod, ver, modPath)
	}

	if *jsonPath != "" {
		writeJSON(*jsonPath, results)
	}

	printRanking(results)
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

func buildDirModuleMap(repoRoot string) (map[string]string, error) {
	svcDir := filepath.Join(repoRoot, "services")

	entries, err := os.ReadDir(svcDir)
	if err != nil {
		return nil, err
	}

	dirMap := map[string]string{}

	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), "_") {
			continue
		}

		mods, modsErr := sdkModsFor(filepath.Join(svcDir, e.Name()))
		if modsErr != nil {
			return nil, modsErr
		}

		if slices.Contains(mods, e.Name()) {
			dirMap[e.Name()] = e.Name()
		} else if override, ok := dirModuleOverride[e.Name()]; ok {
			dirMap[e.Name()] = override
		}
		// else: no pinned aws-sdk-go-v2 dependency (e.g. opsworks, qldb,
		// qldbsession) -- nothing to diff against, excluded.
	}

	return dirMap, nil
}

func sdkModsFor(dirPath string) ([]string, error) {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	set := map[string]struct{}{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") {
			continue
		}

		data, readErr := os.ReadFile(filepath.Join(dirPath, e.Name()))
		if readErr != nil {
			return nil, readErr
		}

		for _, m := range sdkImportRe.FindAllStringSubmatch(string(data), -1) {
			set[m[1]] = struct{}{}
		}
	}

	mods := make([]string, 0, len(set))
	for m := range set {
		mods = append(mods, m)
	}

	sort.Strings(mods)

	return mods, nil
}

// moduleVersion finds mod's pinned version in go.mod. go.mod mixes a
// require(...) block with standalone "require x v..." lines -- both forms
// must match or those services silently vanish from the list.
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

func parseServiceOutputs(mod, ver, modPath string) serviceResult {
	entries, err := os.ReadDir(modPath)
	if err != nil {
		return serviceResult{Mod: mod, Ver: ver}
	}

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}

	sort.Strings(names)

	res := serviceResult{Mod: mod, Ver: ver}

	for _, fn := range names {
		if !strings.HasPrefix(fn, "api_op_") || !strings.HasSuffix(fn, ".go") {
			continue
		}

		opName := strings.TrimSuffix(strings.TrimPrefix(fn, "api_op_"), ".go")

		src, readErr := os.ReadFile(filepath.Join(modPath, fn))
		if readErr != nil {
			continue
		}

		body, ok := extractOutputBody(string(src), opName)
		if !ok {
			continue
		}

		res.TotalOps++

		required := requiredFields(body)
		if len(required) > 0 {
			res.Ops = append(res.Ops, opResult{Op: opName, Required: required})
			res.OpsWithNonzero++
			res.TotalRequired += len(required)
		}
	}

	return res
}

// extractOutputBody returns the body of "type <opName>Output struct {" up
// to its matching closing brace, tracking brace depth so a nested
// struct/interface literal inside a field type doesn't terminate the match
// early.
func extractOutputBody(src, opName string) (string, bool) {
	marker := "type " + opName + "Output struct {"

	start := strings.Index(src, marker)
	if start == -1 {
		return "", false
	}

	bodyStart := start + len(marker)
	depth := 1

	for i := bodyStart; i < len(src); i++ {
		switch src[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return src[bodyStart:i], true
			}
		}
	}

	return "", false
}

// fieldBlocks splits body into blank-line-separated top-level field blocks,
// tracking brace depth so a blank line inside a nested struct/map literal
// never splits a block early.
func fieldBlocks(body string) []string {
	var blocks []string

	var block []string

	depth := 0

	for line := range strings.SplitSeq(body, "\n") {
		if strings.TrimSpace(line) == "" && depth == 0 {
			if len(block) > 0 {
				blocks = append(blocks, strings.Join(block, "\n"))
				block = block[:0]
			}

			continue
		}

		block = append(block, line)
		depth += strings.Count(line, "{") - strings.Count(line, "}")
	}

	if len(block) > 0 {
		blocks = append(blocks, strings.Join(block, "\n"))
	}

	return blocks
}

// requiredFieldName reports the field name declared in block, and whether
// block's doc comment marks it required (the exact line "This member is
// required.").
func requiredFieldName(block string) (string, bool) {
	hasRequired := false

	var fieldLine string

	for l := range strings.SplitSeq(block, "\n") {
		trimmed := strings.TrimSpace(l)
		if trimmed == "// "+requiredLine || trimmed == "//"+requiredLine {
			hasRequired = true
		}

		if !strings.HasPrefix(trimmed, "//") && trimmed != "" {
			fieldLine = trimmed
		}
	}

	if !hasRequired || fieldLine == "" {
		return "", false
	}

	m := fieldNameRe.FindStringSubmatch(fieldLine)
	if m == nil {
		return "", false
	}

	return m[1], true
}

// requiredFields returns the field names of body's top-level blocks marked
// required (see requiredFieldName).
func requiredFields(body string) []string {
	var required []string

	for _, block := range fieldBlocks(body) {
		if name, ok := requiredFieldName(block); ok {
			required = append(required, name)
		}
	}

	return required
}

func writeJSON(path string, results map[string]serviceResult) {
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)

		return
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", " ")

	if encErr := enc.Encode(results); encErr != nil {
		fmt.Fprintln(os.Stderr, "error:", encErr)
	}

	fmt.Fprintln(os.Stderr, "wrote", path)
}

func printRanking(results map[string]serviceResult) {
	type row struct {
		svc  string
		info serviceResult
	}

	rows := make([]row, 0, len(results))
	totalRequired := 0

	for svc, info := range results {
		if info.TotalRequired == 0 {
			continue
		}

		rows = append(rows, row{svc: svc, info: info})
		totalRequired += info.TotalRequired
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].info.TotalRequired != rows[j].info.TotalRequired {
			return rows[i].info.TotalRequired > rows[j].info.TotalRequired
		}

		return rows[i].svc < rows[j].svc
	})

	fmt.Fprintf(os.Stdout, "# %d services resolved, %d with >=1 required output field, %d required fields total\n\n",
		len(results), len(rows), totalRequired)

	for _, r := range rows {
		fmt.Fprintf(os.Stdout, "%4d  %-25s ops=%-3d ops-with-required=%d\n",
			r.info.TotalRequired, r.svc, r.info.TotalOps, r.info.OpsWithNonzero)
	}
}
