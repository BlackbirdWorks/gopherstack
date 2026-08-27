// Command overwidecandidates regenerates the over-wide List-response
// candidate list for gopherstack-dv4s (see services/_OVERWIDE_CANDIDATES.md).
//
// For each services/<dir>, it resolves the pinned
// aws-sdk-go-v2/service/<mod>@<version> from go.mod (directory name and
// module name diverge for a handful of services — see dirModuleOverride),
// reads every api_op_List*.go Output struct from
// $(go env GOMODCACHE)/github.com/aws/aws-sdk-go-v2/service/<mod>@<version>,
// and flags slice fields whose element type is a types.* struct matching
// Summary|Item|Brief|Entry|Ref|Preview|Metadata|Info — plus one level of
// pointer-to-wrapper-struct indirection (the classic REST-XML
// FooList{ Items []FooSummary } shape).
//
// This is a candidate list only: it flags AWS's real Output shape as
// narrower than a raw domain struct by name pattern. It does not read
// gopherstack's own handler/converter code, so a hit still needs per-op
// verification against the real Summary struct before being called a leak
// — see services/_OVERWIDE_CANDIDATES.md for the false-positive classes
// already paid for.
//
// Usage:
//
//	go run ./cmd/overwidecandidates            # ranked summary to stdout
//	go run ./cmd/overwidecandidates -json out.json   # full per-op detail
package main

import (
	"context"
	"encoding/json"
	"errors"
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
// name where the two diverge. Resolved by hand against each dir's actual
// imports; see services/_PROTOCOLS.md for the same divergence list used by
// the protocol map.
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
	// sdkModsFor runs grep -o with this same pattern, so each output line is
	// exactly one match. Anchored per-line: unanchored, this would also match a
	// lookalike host such as evil.com/github.com/aws/aws-sdk-go-v2/service/s3.
	sdkImportRe   = regexp.MustCompile(`(?m)^github\.com/aws/aws-sdk-go-v2/service/([a-z0-9]+)$`)
	summaryNameRe = regexp.MustCompile(`(Summary|Item|Brief|Entry|Ref|Preview|Metadata|Info)$`)
	sliceFieldRe  = regexp.MustCompile(`(?m)^\s*(\w+)\s+\[\](\*?)(\w+\.)?(\w+)`)
	ptrFieldRe    = regexp.MustCompile(`(?m)^\s*(\w+)\s+\*(\w+\.)?(\w+)\b`)
)

type candidateOp struct {
	Op        string `json:"op"`
	Field     string `json:"field"`
	Elem      string `json:"elem"`
	Depth     string `json:"depth"`
	Candidate bool   `json:"candidate"`
}

type serviceResult struct {
	Mod string        `json:"mod"`
	Ver string        `json:"ver"`
	Ops []candidateOp `json:"ops"`
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
		results[d] = serviceResult{Mod: mod, Ver: ver, Ops: parseListOutputs(modPath)}
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
	out, err := exec.CommandContext(context.Background(), "grep", "-rhoP",
		`github\.com/aws/aws-sdk-go-v2/service/[a-z0-9]+`, dirPath).Output()
	if err != nil {
		// grep exits 1 when it finds nothing -- not an error for us.
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
			return nil, nil
		}

		return nil, err
	}

	set := map[string]struct{}{}
	for _, m := range sdkImportRe.FindAllStringSubmatch(string(out), -1) {
		set[m[1]] = struct{}{}
	}

	mods := make([]string, 0, len(set))
	for m := range set {
		mods = append(mods, m)
	}

	sort.Strings(mods)

	return mods, nil
}

// moduleVersion finds mod's pinned version in go.mod. go.mod mixes a
// require(...) block with standalone "require x v..." lines (10 of them,
// e.g. bedrockagent, vpclattice, cleanrooms) -- both forms must match or
// those services silently vanish from the list.
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

func parseListOutputs(modPath string) []candidateOp {
	entries, err := os.ReadDir(modPath)
	if err != nil {
		return nil
	}

	typesSrc := ""
	if b, readErr := os.ReadFile(filepath.Join(modPath, "types", "types.go")); readErr == nil {
		typesSrc = string(b)
	}

	var results []candidateOp

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}

	sort.Strings(names)

	for _, fn := range names {
		if !strings.HasPrefix(fn, "api_op_List") || !strings.HasSuffix(fn, ".go") {
			continue
		}

		opName := strings.TrimSuffix(strings.TrimPrefix(fn, "api_op_"), ".go")

		src, readErr := os.ReadFile(filepath.Join(modPath, fn))
		if readErr != nil {
			continue
		}

		body, ok := extractStructBody(string(src), opName+"Output")
		if !ok {
			continue
		}

		results = append(results, directSliceCandidates(opName, body)...)
		results = append(results, wrapperSliceCandidates(opName, body, typesSrc)...)
	}

	return results
}

func directSliceCandidates(opName, body string) []candidateOp {
	var results []candidateOp

	for _, m := range sliceFieldRe.FindAllStringSubmatch(body, -1) {
		fieldName, pkg, elemType := m[1], m[3], m[4]
		if fieldName == "NextToken" {
			continue
		}

		isStructElem := pkg == "types."
		results = append(results, candidateOp{
			Op: opName, Field: fieldName, Elem: elemType,
			Candidate: isStructElem && summaryNameRe.MatchString(elemType),
			Depth:     "direct",
		})
	}

	return results
}

// wrapperSliceCandidates follows one level of pointer-to-wrapper-struct
// indirection: a single wrapper field whose own slice member is the real
// narrow type (CloudFront's classic DistributionList{ Items
// []DistributionSummary } shape).
func wrapperSliceCandidates(opName, body, typesSrc string) []candidateOp {
	var results []candidateOp

	for _, m := range ptrFieldRe.FindAllStringSubmatch(body, -1) {
		fieldName, pkg, wrapperType := m[1], m[2], m[3]
		if pkg != "types." || fieldName == "ResultMetadata" {
			continue
		}

		wbody, wrapperOK := extractStructBody(typesSrc, wrapperType)
		if !wrapperOK {
			continue
		}

		for _, wm := range sliceFieldRe.FindAllStringSubmatch(wbody, -1) {
			welemType := wm[4]
			results = append(results, candidateOp{
				Op: opName, Field: wrapperType + "." + wm[1], Elem: welemType,
				Candidate: summaryNameRe.MatchString(welemType),
				Depth:     "wrapper:" + wrapperType,
			})
		}
	}

	return results
}

// extractStructBody returns the body between "type <name> struct {" and its
// matching closing brace, tracking brace depth so a nested struct/interface
// literal inside a field type doesn't terminate the match early.
func extractStructBody(src, name string) (string, bool) {
	marker := "type " + name + " struct {"

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
		svc string
		ops []string
	}

	var rows []row

	totalOps := 0

	for svc, info := range results {
		set := map[string]struct{}{}
		for _, op := range info.Ops {
			if op.Candidate {
				set[op.Op] = struct{}{}
			}
		}

		if len(set) == 0 {
			continue
		}

		ops := make([]string, 0, len(set))
		for op := range set {
			ops = append(ops, op)
		}

		sort.Strings(ops)
		rows = append(rows, row{svc: svc, ops: ops})
		totalOps += len(ops)
	}

	sort.Slice(rows, func(i, j int) bool {
		if len(rows[i].ops) != len(rows[j].ops) {
			return len(rows[i].ops) > len(rows[j].ops)
		}

		return rows[i].svc < rows[j].svc
	})

	fmt.Fprintf(os.Stdout, "# %d services resolved, %d with >=1 candidate op, %d candidate ops total\n\n",
		len(results), len(rows), totalOps)

	for _, r := range rows {
		fmt.Fprintf(os.Stdout, "%3d  %-25s %v\n", len(r.ops), r.svc, r.ops)
	}
}
