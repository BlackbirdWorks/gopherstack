package ssm

import (
	"cmp"
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	actionRunShellScript      = "aws:runShellScript"
	actionRunPowerShellScript = "aws:runPowerShellScript"

	defaultExecutionTimeoutSecs = 3600
)

// scriptStep is one runnable plugin of a Command document.
type scriptStep struct {
	name        string
	action      string
	workDir     string
	script      string
	timeoutSecs int
	critical    bool
}

// pluginName is the action without its colon (S3 folder name).
func (s scriptStep) pluginName() string { return strings.ReplaceAll(s.action, ":", "") }

func (s scriptStep) cloudWatchName() string { return "aws-" + strings.TrimPrefix(s.action, "aws:") }

func (s scriptStep) interpreter(file string) []string {
	if s.action == actionRunPowerShellScript {
		return []string{"pwsh", "-NoProfile", "-NonInteractive", "-File", file}
	}

	return []string{"sh", file}
}

type rawDocStep struct {
	IsCritical any            `json:"isCritical" yaml:"isCritical"`
	Inputs     map[string]any `json:"inputs"     yaml:"inputs"`
	Action     string         `json:"action"     yaml:"action"`
	Name       string         `json:"name"       yaml:"name"`
	OnFailure  string         `json:"onFailure"  yaml:"onFailure"`
}

type rawDocument struct {
	Parameters map[string]struct {
		Default any `json:"default" yaml:"default"`
	} `json:"parameters" yaml:"parameters"`
	MainSteps []rawDocStep `json:"mainSteps"  yaml:"mainSteps"`
}

var docParamRef = regexp.MustCompile(`\{\{\s*([A-Za-z0-9_.:-]+)\s*\}\}`)

// parseScriptSteps extracts the runnable steps of a Command document with parameters substituted.
// Steps with other actions are skipped; ok is false when no step is runnable.
func parseScriptSteps(content, format string, params map[string][]string) ([]scriptStep, bool) {
	var doc rawDocument

	var err error
	if format == documentFormatYAML {
		err = yaml.Unmarshal([]byte(content), &doc)
	} else {
		err = json.Unmarshal([]byte(content), &doc)
	}

	if err != nil {
		return nil, false
	}

	vals := make(map[string][]string, len(doc.Parameters)+len(params))

	for name, p := range doc.Parameters {
		if p.Default != nil {
			vals[name] = anyStrings(p.Default)
		}
	}

	maps.Copy(vals, params)

	var steps []scriptStep

	for _, raw := range doc.MainSteps {
		if raw.Action != actionRunShellScript && raw.Action != actionRunPowerShellScript {
			continue
		}

		steps = append(steps, buildScriptStep(raw, vals))
	}

	return steps, len(steps) > 0
}

func buildScriptStep(raw rawDocStep, vals map[string][]string) scriptStep {
	step := scriptStep{
		name:        cmp.Or(raw.Name, raw.Action),
		action:      raw.Action,
		timeoutSecs: defaultExecutionTimeoutSecs,
		critical:    raw.OnFailure == "exit" || anyBool(raw.IsCritical),
	}

	cmds := raw.Inputs["runCommand"]
	if cmds == nil {
		cmds = raw.Inputs["commands"]
	}

	lines := make([]string, 0, 1)

	for _, c := range anyStrings(cmds) {
		lines = append(lines, substituteDocRefs(c, vals)...)
	}

	step.script = strings.Join(lines, "\n") + "\n"

	if wd, ok := raw.Inputs["workingDirectory"].(string); ok {
		step.workDir = strings.Join(substituteDocRefs(wd, vals), "")
	}

	if t := strings.Join(substituteDocRefs(anyString(raw.Inputs["timeoutSeconds"]), vals), ""); t != "" {
		if n, err := strconv.Atoi(t); err == nil && n > 0 {
			step.timeoutSecs = n
		}
	}

	return step
}

// substituteDocRefs replaces {{ name }}; a list parameter that is the whole entry expands to one entry per value.
func substituteDocRefs(s string, vals map[string][]string) []string {
	if m := docParamRef.FindStringSubmatch(s); m != nil && m[0] == strings.TrimSpace(s) {
		if v, ok := vals[m[1]]; ok {
			return v
		}
	}

	return []string{docParamRef.ReplaceAllStringFunc(s, func(ref string) string {
		name := docParamRef.FindStringSubmatch(ref)[1]
		if v, ok := vals[name]; ok {
			return strings.Join(v, "\n")
		}

		return ref
	})}
}

func anyStrings(v any) []string {
	switch t := v.(type) {
	case nil:
		return nil
	case []any:
		out := make([]string, 0, len(t))
		for _, e := range t {
			out = append(out, anyString(e))
		}

		return out
	default:
		return []string{anyString(v)}
	}
}

func anyString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	default:
		return fmt.Sprint(t)
	}
}

func anyBool(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		return strings.EqualFold(t, "true")
	default:
		return false
	}
}
