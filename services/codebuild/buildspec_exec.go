package codebuild

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	specVersion01 = "0.1"
	specVersion02 = "0.2"

	onFailureContinue = "CONTINUE"

	cbEnvFile = "/tmp/.cb_env"
	cbCwdFile = "/tmp/.cb_cwd"
)

const failCheckFmt = `if [ "$cb_rc" -ne 0 ]; then printf ` +
	`'[Container] %%s Command did not exit successfully %%s exit status %%d\n' %s %s "$cb_rc"; ` +
	`exit "$cb_rc"; fi` + "\n"

var errBuildspec = errors.New("invalid buildspec")

// execPhaseNames returns the buildspec phases in execution order.
func execPhaseNames() []string {
	return []string{"install", "pre_build", "build", "post_build"}
}

type specFile struct {
	Env       *specEnv             `yaml:"env"`
	Phases    map[string]specPhase `yaml:"phases"`
	Artifacts *specArtifacts       `yaml:"artifacts"`
	Version   string               `yaml:"version"`
}

type specEnv struct {
	Variables      map[string]any    `yaml:"variables"`
	ParameterStore map[string]string `yaml:"parameter-store"`
	SecretsManager map[string]string `yaml:"secrets-manager"`
}

type specPhase struct {
	OnFailure string   `yaml:"on-failure"`
	Commands  []string `yaml:"commands"`
	Finally   []string `yaml:"finally"`
}

type specArtifact struct {
	DiscardPaths  any      `yaml:"discard-paths"`
	BaseDirectory string   `yaml:"base-directory"`
	Name          string   `yaml:"name"`
	Files         []string `yaml:"files"`
}

type specArtifacts struct {
	Secondary    map[string]specArtifact `yaml:"secondary-artifacts"`
	specArtifact `                        yaml:",inline"`
}

// parseBuildspecFile parses and validates a buildspec for execution.
func parseBuildspecFile(src string) (*specFile, error) {
	var f specFile

	if err := yaml.Unmarshal([]byte(src), &f); err != nil {
		return nil, fmt.Errorf("%w: %w", errBuildspec, err)
	}

	switch f.Version {
	case specVersion01, specVersion02:
	case "":
		return nil, fmt.Errorf("%w: version is required", errBuildspec)
	default:
		return nil, fmt.Errorf("%w: unsupported version %q", errBuildspec, f.Version)
	}

	for name := range f.Phases {
		if !isExecPhase(name) {
			return nil, fmt.Errorf("%w: unknown phase %q", errBuildspec, name)
		}
	}

	return &f, nil
}

func isExecPhase(name string) bool {
	return slices.Contains(execPhaseNames(), name)
}

func yamlTruthy(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		s := strings.ToLower(t)

		return s == "yes" || s == "true"
	default:
		return false
	}
}

// specVars returns the buildspec's plaintext variables as sorted pairs.
func (f *specFile) specVars() []EnvironmentVariable {
	if f.Env == nil {
		return nil
	}

	out := make([]EnvironmentVariable, 0, len(f.Env.Variables))
	for k, v := range f.Env.Variables {
		out = append(out, EnvironmentVariable{Name: k, Value: fmt.Sprint(v), Type: "PLAINTEXT"})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

// specRefs returns the buildspec's parameter-store and secrets-manager variables as sorted pairs.
func (f *specFile) specRefs() []EnvironmentVariable {
	if f.Env == nil {
		return nil
	}

	var out []EnvironmentVariable
	for k, v := range f.Env.ParameterStore {
		out = append(out, EnvironmentVariable{Name: k, Value: v, Type: "PARAMETER_STORE"})
	}

	for k, v := range f.Env.SecretsManager {
		out = append(out, EnvironmentVariable{Name: k, Value: v, Type: "SECRETS_MANAGER"})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

// shQuote single-quotes s for a POSIX shell.
func shQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

const cbTimestamp = `"$(date -u +'%Y/%m/%d %H:%M:%S.000000')"`

// envFileScript returns a script that writes the build's exported variables to the shared env file.
func envFileScript(vars []EnvironmentVariable) string {
	var sb strings.Builder

	sb.WriteString("cat > " + cbEnvFile + " <<'CB_ENV_EOF'\n")

	for _, v := range vars {
		sb.WriteString("export " + v.Name + "=" + shQuote(v.Value) + "\n")
	}

	sb.WriteString("CB_ENV_EOF\n")

	return sb.String()
}

// commandScripts turns a phase's commands into shell scripts: one shared shell
// for buildspec 0.2, one shell per command for 0.1.
func commandScripts(version string, cmds []string, srcDir string, succeeding bool) []string {
	if len(cmds) == 0 {
		return nil
	}

	if version == specVersion02 {
		return []string{commandScript(cmds, srcDir, succeeding, true)}
	}

	out := make([]string, len(cmds))
	for i, c := range cmds {
		out[i] = commandScript([]string{c}, srcDir, succeeding, false)
	}

	return out
}

func commandScript(cmds []string, srcDir string, succeeding, shareShell bool) string {
	var sb strings.Builder

	sb.WriteString("exec 2>&1\n")
	sb.WriteString("[ -f " + cbEnvFile + " ] && . " + cbEnvFile + "\n")

	flag := "0"
	if succeeding {
		flag = "1"
	}

	sb.WriteString("CODEBUILD_BUILD_SUCCEEDING=" + flag + "; export CODEBUILD_BUILD_SUCCEEDING\n")

	if shareShell {
		fmt.Fprintf(&sb, "cd \"$(cat %s 2>/dev/null || echo %s)\" || exit 1\n", cbCwdFile, shQuote(srcDir))
		fmt.Fprintf(&sb, "trap 'pwd > %s; export -p > %s' EXIT\n", cbCwdFile, cbEnvFile)
	} else {
		sb.WriteString("cd " + shQuote(srcDir) + " || exit 1\n")
	}

	for _, c := range cmds {
		fmt.Fprintf(&sb, "printf '[Container] %%s Running command %%s\\n' %s %s\n", cbTimestamp, shQuote(c))
		sb.WriteString(c + "\n")
		sb.WriteString("cb_rc=$?\n")
		fmt.Fprintf(&sb, failCheckFmt, cbTimestamp, shQuote(c))
	}

	return sb.String()
}

// shellInvocation runs script with bash when the image has it, else sh.
func shellInvocation(script string) []string {
	return []string{
		"sh", "-c",
		`if command -v bash >/dev/null 2>&1; then exec bash -c "$1"; else exec sh -c "$1"; fi`,
		"_", script,
	}
}

var failedCommandRe = regexp.MustCompile(`(?s)Command did not exit successfully (.*) exit status (\d+)\s*$`)

// failureMessage extracts the CodeBuild-style context message from a failed script's output.
func failureMessage(output string, exit int) string {
	if m := failedCommandRe.FindStringSubmatch(output); m != nil {
		return fmt.Sprintf("Error while executing command: %s. Reason: exit status %s", m[1], m[2])
	}

	return fmt.Sprintf("Error while executing command. Reason: exit status %d", exit)
}
