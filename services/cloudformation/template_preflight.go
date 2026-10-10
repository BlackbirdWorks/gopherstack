package cloudformation

import (
	"encoding/json"
	"errors"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"
)

// ErrTemplateFormat is the "Template format error: ..." family real
// CreateStack/UpdateStack return synchronously as a ValidationError.
var ErrTemplateFormat = errors.New("template format error")

var (
	errInvalidStackName   = errors.New("invalid stack name")
	errParameterValues    = errors.New("invalid parameters")
	errStackNotUpdatable  = errors.New("stack cannot be updated in its current state")
	stackNamePattern      = regexp.MustCompile(`^[a-zA-Z][-a-zA-Z0-9]*$`)
	yamlLinePattern       = regexp.MustCompile(`line (\d+)`)
	errEmptyOrTransformed = errors.New("template not statically checkable")
)

const maxStackNameLen = 128

// validateStackNameForCreate mirrors CreateStackInput.StackName's
// "[a-zA-Z][-a-zA-Z0-9]*" pattern and 128-character ceiling.
func validateStackNameForCreate(name string) error {
	if strings.HasPrefix(name, "arn:") {
		return nil
	}

	if len(name) <= maxStackNameLen && stackNamePattern.MatchString(name) {
		return nil
	}

	return awsErrorf(errInvalidStackName,
		"1 validation error detected: Value '%s' at 'stackName' failed to satisfy constraint: "+
			"Member must satisfy regular expression pattern: [a-zA-Z][-a-zA-Z0-9]*|arn:[-a-zA-Z0-9:/._+]*",
		name,
	)
}

func templateFormatErr(format string, args ...any) error {
	return awsErrorf(ErrTemplateFormat, "Template format error: "+format, args...)
}

func lineColumn(body string, offset int64) (int, int) {
	line, col := 1, 0
	for i := 0; i < len(body) && int64(i) < offset; i++ {
		if body[i] == '\n' {
			line++
			col = 0

			continue
		}
		col++
	}

	return line, col
}

// templateParseFormatErr maps a ParseTemplate failure to AWS's synchronous
// "Template format error" wording; non-syntax failures pass through unchanged.
func templateParseFormatErr(body string, err error) error {
	if syn, ok := errors.AsType[*json.SyntaxError](err); ok {
		line, col := lineColumn(strings.TrimSpace(body), syn.Offset)

		return templateFormatErr("JSON not well-formed. (line %d, column %d)", line, col)
	}

	if typ, ok := errors.AsType[*json.UnmarshalTypeError](err); ok {
		if strings.HasSuffix(typ.Field, "Description") {
			return templateFormatErr("Every Description member must be a string.")
		}

		return templateFormatErr("%s has an invalid type", typ.Field)
	}

	if strings.Contains(err.Error(), "failed to parse YAML template") {
		if m := yamlLinePattern.FindStringSubmatch(err.Error()); m != nil {
			return templateFormatErr("YAML not well-formed. (line %s, column 1)", m[1])
		}

		return templateFormatErr("YAML not well-formed.")
	}

	return err
}

// preflightTemplateErr runs the synchronous checks real CreateStack applies
// before any stack exists. A template with a Transform or Fn::ForEach is only
// checked for syntax: macro expansion can add the resources and parameters
// the static checks would otherwise flag.
func preflightTemplateErr(body string, params []Parameter) error {
	tmpl, err := preflightTemplateStructure(body)
	if errors.Is(err, errEmptyOrTransformed) {
		return nil
	}

	if err != nil {
		return err
	}

	return parameterValuesErr(tmpl, params)
}

// preflightTemplateStructure runs the parameter-independent checks and returns
// the parsed template, or errEmptyOrTransformed when no static check applies.
func preflightTemplateStructure(body string) (*Template, error) {
	if strings.TrimSpace(body) == "" {
		return nil, errEmptyOrTransformed
	}

	tmpl, err := ParseTemplate(body)
	if err != nil {
		if fmtErr := templateParseFormatErr(body, err); errors.Is(fmtErr, ErrTemplateFormat) {
			return nil, fmtErr
		}

		return nil, errEmptyOrTransformed
	}

	if len(tmpl.Transform) > 0 || strings.Contains(body, forEachPrefix) {
		return nil, errEmptyOrTransformed
	}

	if len(tmpl.Resources) == 0 {
		return nil, templateFormatErr("At least one Resources member must be defined.")
	}

	for _, id := range sortedResourceIDs(tmpl) {
		if tmpl.Resources[id].Type == "" {
			return nil, templateFormatErr("[/Resources/%s] Every Resources object must contain a Type member.", id)
		}
	}

	if err = unresolvedReferencesErr(tmpl); err != nil {
		return nil, err
	}

	if _, err = topoSortResources(tmpl.Resources); err != nil {
		return nil, err
	}

	return tmpl, nil
}

// validateTemplateStructure is preflightTemplateStructure for callers that
// only need the verdict.
func validateTemplateStructure(body string) error {
	if _, err := preflightTemplateStructure(body); err != nil && !errors.Is(err, errEmptyOrTransformed) {
		return err
	}

	return nil
}

func sortedResourceIDs(tmpl *Template) []string {
	ids := make([]string, 0, len(tmpl.Resources))
	for id := range tmpl.Resources {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	return ids
}

func unresolvedReferencesErr(tmpl *Template) error {
	missing := map[string]struct{}{}
	known := func(name string) bool {
		if _, ok := tmpl.Resources[name]; ok {
			return true
		}
		if _, ok := tmpl.Parameters[name]; ok {
			return true
		}

		return strings.HasPrefix(name, "AWS::")
	}

	for _, res := range tmpl.Resources {
		collectUnknownRefs(res.Properties, known, missing)
		for _, dep := range res.DependsOn {
			if _, ok := tmpl.Resources[dep]; !ok {
				missing[dep] = struct{}{}
			}
		}
	}

	for _, out := range tmpl.Outputs {
		collectUnknownRefs(out.Value, known, missing)
	}

	if len(missing) == 0 {
		return nil
	}

	names := make([]string, 0, len(missing))
	for n := range missing {
		names = append(names, n)
	}
	sort.Strings(names)

	return templateFormatErr(
		"Unresolved resource dependencies [%s] in the Resources block of the template", strings.Join(names, ", "),
	)
}

func collectUnknownRefs(v any, known func(string) bool, missing map[string]struct{}) {
	switch val := v.(type) {
	case map[string]any:
		if ref, ok := val["Ref"].(string); ok && len(val) == 1 && !known(ref) {
			missing[ref] = struct{}{}
		}
		for _, child := range val {
			collectUnknownRefs(child, known, missing)
		}
	case []any:
		for _, child := range val {
			collectUnknownRefs(child, known, missing)
		}
	}
}

// parameterValuesErr mirrors "Parameters: [X] must have values" and
// "Parameters: [X] do not exist in the template".
func parameterValuesErr(tmpl *Template, params []Parameter) error {
	given := make(map[string]struct{}, len(params))
	var extra []string
	for _, p := range params {
		given[p.ParameterKey] = struct{}{}
		if _, ok := tmpl.Parameters[p.ParameterKey]; !ok {
			extra = append(extra, p.ParameterKey)
		}
	}

	if len(extra) > 0 {
		sort.Strings(extra)

		return awsErrorf(errParameterValues, "Parameters: [%s] do not exist in the template", strings.Join(extra, ", "))
	}

	var missing []string
	for name, p := range tmpl.Parameters {
		if _, ok := given[name]; !ok && p.Default == nil {
			missing = append(missing, name)
		}
	}

	if len(missing) > 0 {
		sort.Strings(missing)

		return awsErrorf(errParameterValues, "Parameters: [%s] must have values", strings.Join(missing, ", "))
	}

	return nil
}

func stackUpdatableErr(stack *Stack) error {
	switch stack.StackStatus {
	case statusCreateComplete, statusUpdateComplete, statusUpdateRollbackComplete,
		"IMPORT_COMPLETE", "IMPORT_ROLLBACK_COMPLETE":
		return nil
	}

	return awsErrorf(errStackNotUpdatable,
		"Stack:%s is in %s state and can not be updated.", stack.StackID, stack.StackStatus)
}

// mergePreviousParameters resolves UsePreviousValue entries against the
// stack's current parameters.
func mergePreviousParameters(stack *Stack, params []Parameter) ([]Parameter, error) {
	if params == nil {
		return nil, nil
	}

	prev := make(map[string]string, len(stack.Parameters))
	for _, p := range stack.Parameters {
		prev[p.ParameterKey] = p.ParameterValue
	}

	out := make([]Parameter, 0, len(params))
	var missing []string
	for _, p := range params {
		if p.UsePreviousValue {
			v, ok := prev[p.ParameterKey]
			if !ok {
				missing = append(missing, p.ParameterKey)

				continue
			}
			p.ParameterValue = v
			p.UsePreviousValue = false
		}
		out = append(out, p)
	}

	if len(missing) > 0 {
		sort.Strings(missing)

		return nil, awsErrorf(errParameterValues, "Parameters: [%s] must have values", strings.Join(missing, ", "))
	}

	return out, nil
}

// isNoOpUpdate reports whether an UpdateStack request changes nothing, which
// real CloudFormation rejects with "No updates are to be performed.".
func isNoOpUpdate(stack *Stack, templateBody string, params []Parameter, opts StackOptions) bool {
	if opts.StackPolicyBody != "" {
		return false
	}

	if opts.RoleARN != "" && opts.RoleARN != stack.RoleARN {
		return false
	}

	if len(opts.Tags) > 0 && !sameTags(opts.Tags, stack.Tags) {
		return false
	}

	if templateBody == "" || strings.TrimSpace(templateBody) == strings.TrimSpace(stack.TemplateBody) {
		return params == nil || sameParams(stack, params, nil)
	}

	oldTmpl, oldErr := ParseTemplate(stack.TemplateBody)
	newTmpl, newErr := ParseTemplate(templateBody)
	if oldErr != nil || newErr != nil || !reflect.DeepEqual(oldTmpl, newTmpl) {
		return false
	}

	return params == nil || sameParams(stack, params, newTmpl)
}

func sameTags(a, b []Tag) bool {
	if len(a) != len(b) {
		return false
	}

	m := make(map[string]string, len(b))
	for _, t := range b {
		m[t.Key] = t.Value
	}
	for _, t := range a {
		if v, ok := m[t.Key]; !ok || v != t.Value {
			return false
		}
	}

	return true
}

func sameParams(stack *Stack, params []Parameter, tmpl *Template) bool {
	if tmpl == nil {
		tmpl, _ = ParseTemplate(stack.TemplateBody)
	}
	if tmpl == nil {
		return false
	}

	return reflect.DeepEqual(ResolveParameters(tmpl, params), ResolveParameters(tmpl, stack.Parameters))
}

func validStackStatuses() []string {
	return []string{
		"CREATE_IN_PROGRESS", "CREATE_FAILED", statusCreateComplete, "ROLLBACK_IN_PROGRESS", "ROLLBACK_FAILED",
		"ROLLBACK_COMPLETE", "DELETE_IN_PROGRESS", "DELETE_FAILED", "DELETE_COMPLETE", "UPDATE_IN_PROGRESS",
		"UPDATE_COMPLETE_CLEANUP_IN_PROGRESS", "UPDATE_COMPLETE", "UPDATE_FAILED", "UPDATE_ROLLBACK_IN_PROGRESS",
		"UPDATE_ROLLBACK_FAILED", "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS", "UPDATE_ROLLBACK_COMPLETE",
		"REVIEW_IN_PROGRESS", "IMPORT_IN_PROGRESS", "IMPORT_COMPLETE", "IMPORT_ROLLBACK_IN_PROGRESS",
		"IMPORT_ROLLBACK_FAILED", "IMPORT_ROLLBACK_COMPLETE",
	}
}

func invalidStackStatuses(filter []string) []string {
	var bad []string
	for _, f := range filter {
		if !slices.Contains(validStackStatuses(), f) {
			bad = append(bad, f)
		}
	}

	return bad
}
