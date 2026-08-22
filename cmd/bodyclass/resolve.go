package main

import (
	"context"
	"errors"
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
// name where the two diverge. Same table as cmd/overwidecandidates and
// cmd/requiredoutputfields, resolved by hand against each dir's own imports.
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

// sdkModsFor runs grep -o with this same pattern, so each output line is
// exactly one match. Anchored per-line: unanchored, this would also match a
// lookalike host such as evil.com/github.com/aws/aws-sdk-go-v2/service/s3.
var sdkImportRe = regexp.MustCompile(`(?m)^github\.com/aws/aws-sdk-go-v2/service/([a-z0-9]+)$`)

var errNoModule = errors.New("no pinned aws-sdk-go-v2 module found for service")

var errNoVersion = errors.New("no go.mod version resolved")

type resolved struct {
	Service string
	Mod     string
	Ver     string
	ModPath string
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

// sdkModsFor greps every aws-sdk-go-v2/service/<mod> import path actually
// used under dirPath, so module resolution comes from the directory's own
// imports rather than an assumption that directory name == module name.
func sdkModsFor(dirPath string) ([]string, error) {
	out, err := exec.CommandContext(context.Background(), "grep", "-rhoP",
		`github\.com/aws/aws-sdk-go-v2/service/[a-z0-9]+`, dirPath).Output()
	if err != nil {
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

// resolveModuleName picks the module name for services/<service>: the
// directory's own dominant import if that matches the directory name, the
// override table entry if not, and an error if neither resolves (no pinned
// SDK dependency at all).
func resolveModuleName(repoRoot, service string) (string, error) {
	mods, err := sdkModsFor(filepath.Join(repoRoot, "services", service))
	if err != nil {
		return "", err
	}

	if slices.Contains(mods, service) {
		return service, nil
	}

	if override, ok := dirModuleOverride[service]; ok {
		return override, nil
	}

	if len(mods) == 1 {
		return mods[0], nil
	}

	return "", fmt.Errorf("%w: %s (imports found: %v)", errNoModule, service, mods)
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

func resolveModule(repoRoot, cache, service string) (resolved, error) {
	mod, err := resolveModuleName(repoRoot, service)
	if err != nil {
		return resolved{}, err
	}

	goModSrc, err := os.ReadFile(filepath.Join(repoRoot, "go.mod"))
	if err != nil {
		return resolved{}, err
	}

	ver := moduleVersion(string(goModSrc), mod)
	if ver == "" {
		return resolved{}, fmt.Errorf("%w: service %s -> module %s", errNoVersion, service, mod)
	}

	modPath := filepath.Join(cache, "github.com", "aws", "aws-sdk-go-v2", "service", mod+"@"+ver)

	return resolved{Service: service, Mod: mod, Ver: ver, ModPath: modPath}, nil
}

func listServiceDirs(repoRoot string) ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(repoRoot, "services"))
	if err != nil {
		return nil, err
	}

	var dirs []string
	for _, e := range entries {
		if e.IsDir() && !strings.HasPrefix(e.Name(), "_") {
			dirs = append(dirs, e.Name())
		}
	}

	sort.Strings(dirs)

	return dirs, nil
}
