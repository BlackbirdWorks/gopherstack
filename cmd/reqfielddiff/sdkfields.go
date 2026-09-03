package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// dirModuleOverride maps services/<dir> to its aws-sdk-go-v2/service module
// name where the two diverge. Same table as cmd/structfielddiff,
// cmd/overwidecandidates and cmd/requiredoutputfields keep independently --
// duplicated here rather than imported, since cmd/reqfielddiff must not
// modify or depend on any existing cmd/ tool.
//
//nolint:gochecknoglobals // read-only lookup table, same pattern as its siblings
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

// errNoVersion is wrapped with the service/module pair that failed to resolve.
var errNoVersion = errors.New("no go.mod version resolved")

// sdkField is one top-level field of an SDK <Op>Input struct, as declared in
// the pinned aws-sdk-go-v2 source.
type sdkField struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	DocText  string `json:"docText,omitempty"`
	Required bool   `json:"required"`
}

// sdkOp is one operation's Input field set, as the pinned SDK declares it.
// Only top-level Input fields are captured -- a disclosed scope limit, see
// the package doc.
type sdkOp struct {
	Name   string
	Fields []sdkField
}

var fieldNameRe = regexp.MustCompile(`^([A-Z]\w*)\s+(.+)$`)

const requiredLine = "This member is required."

// resolveModule maps a services/<dir> name to its pinned aws-sdk-go-v2
// module name, version and on-disk GOMODCACHE path. Identical resolution to
// cmd/structfielddiff's, duplicated for the same "don't touch other cmd/
// tools" reason as dirModuleOverride above.
func resolveModule(repoRoot, service string) (string, string, string, error) {
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

func gomodcache(repoRoot string) (string, error) {
	cmd := exec.Command("go", "env", "GOMODCACHE") //nolint:noctx // fixed argv, local tool, no request context to plumb
	cmd.Dir = repoRoot

	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("go env GOMODCACHE: %w", err)
	}

	return strings.TrimSpace(string(out)), nil
}

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

// loadSDKOps reads every api_op_<Op>.go file under modPath and returns each
// operation's Input struct as a sdkOp, sorted by operation name. Only the
// Input struct's own top-level field block is parsed -- output shapes and
// nested struct types are out of scope, see the package doc.
func loadSDKOps(modPath string) ([]sdkOp, error) {
	entries, err := os.ReadDir(modPath)
	if err != nil {
		return nil, err
	}

	var ops []sdkOp

	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasPrefix(name, "api_op_") || !strings.HasSuffix(name, ".go") ||
			strings.HasSuffix(name, "_test.go") {
			continue
		}

		opName := strings.TrimSuffix(strings.TrimPrefix(name, "api_op_"), ".go")

		src, readErr := os.ReadFile(filepath.Join(modPath, name))
		if readErr != nil {
			continue
		}

		fields, found := parseInputStruct(string(src), opName+"Input")
		if !found {
			continue
		}

		ops = append(ops, sdkOp{Name: opName, Fields: fields})
	}

	sort.Slice(ops, func(i, j int) bool { return ops[i].Name < ops[j].Name })

	return ops, nil
}

// parseInputStruct finds "type <structName> struct { ... }" in src and
// returns its top-level field blocks.
func parseInputStruct(src, structName string) ([]sdkField, bool) {
	lines := strings.Split(src, "\n")
	decl := regexp.MustCompile(`^type\s+` + regexp.QuoteMeta(structName) + `\s+struct\s*\{`)

	for i, line := range lines {
		if !decl.MatchString(strings.TrimSpace(line)) {
			continue
		}

		body, _ := extractBody(lines, i)

		return fieldBlocks(body), true
	}

	return nil, false
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

// fieldBlocks splits body into blank-line-separated top-level field blocks
// (brace-depth tracked) and parses each into an sdkField.
func fieldBlocks(body []string) []sdkField {
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

	var (
		fieldLine string
		docLines  []string
	)

	for _, l := range block {
		trimmed := strings.TrimSpace(l)
		if trimmed == "// "+requiredLine || trimmed == "//"+requiredLine {
			required = true
		}

		if after, ok := strings.CutPrefix(trimmed, "//"); ok {
			docLines = append(docLines, strings.TrimSpace(after))

			continue
		}

		if trimmed != "" {
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

	return sdkField{
		Name:     m[1],
		Type:     strings.TrimSpace(m[2]),
		DocText:  strings.Join(docLines, " "),
		Required: required,
	}, true
}
