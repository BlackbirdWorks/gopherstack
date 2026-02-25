package cloudformation

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// ErrEmptyTemplate is returned when a template body is empty.
var ErrEmptyTemplate = errors.New("template body is empty")

// Template represents a parsed CloudFormation template.
type Template struct {
	Parameters               map[string]TemplateParameter `json:"Parameters"               yaml:"Parameters"`
	Resources                map[string]TemplateResource  `json:"Resources"                yaml:"Resources"`
	Outputs                  map[string]TemplateOutput    `json:"Outputs"                  yaml:"Outputs"`
	AWSTemplateFormatVersion string                       `json:"AWSTemplateFormatVersion" yaml:"AWSTemplateFormatVersion"`
	Description              string                       `json:"Description"              yaml:"Description"`
}

// TemplateParameter represents a CloudFormation template parameter.
type TemplateParameter struct {
	Type        string `json:"Type"        yaml:"Type"`
	Default     any    `json:"Default"     yaml:"Default"`
	Description string `json:"Description" yaml:"Description"`
}

// TemplateResource represents a CloudFormation template resource.
type TemplateResource struct {
	Properties map[string]any `json:"Properties" yaml:"Properties"`
	Type       string         `json:"Type"       yaml:"Type"`
}

// TemplateOutput represents a CloudFormation template output.
type TemplateOutput struct {
	Value       any    `json:"Value"       yaml:"Value"`
	Description string `json:"Description" yaml:"Description"`
}

// ParseTemplate parses a CloudFormation template from a JSON or YAML string.
func ParseTemplate(body string) (*Template, error) {
	body = strings.TrimSpace(body)
	if body == "" {
		return nil, ErrEmptyTemplate
	}

	var tmpl Template

	if strings.HasPrefix(body, "{") {
		if err := json.Unmarshal([]byte(body), &tmpl); err != nil {
			return nil, fmt.Errorf("failed to parse JSON template: %w", err)
		}

		return &tmpl, nil
	}

	if err := yaml.Unmarshal([]byte(body), &tmpl); err != nil {
		return nil, fmt.Errorf("failed to parse YAML template: %w", err)
	}

	return &tmpl, nil
}

// ResolveParameters merges template defaults with provided overrides.
func ResolveParameters(tmpl *Template, overrides []Parameter) map[string]string {
	resolved := make(map[string]string)

	for name, param := range tmpl.Parameters {
		if param.Default != nil {
			resolved[name] = fmt.Sprintf("%v", param.Default)
		}
	}

	for _, p := range overrides {
		resolved[p.ParameterKey] = p.ParameterValue
	}

	return resolved
}

// ResolveValue resolves a CloudFormation property value, handling intrinsic functions.
func ResolveValue(v any, params map[string]string, physicalIDs map[string]string) string {
	if v == nil {
		return ""
	}

	switch val := v.(type) {
	case string:
		return val
	case bool:
		if val {
			return "true"
		}

		return "false"
	case int, int64, float64:
		return fmt.Sprintf("%v", val)
	case map[string]any:
		if ref, ok := val["Ref"].(string); ok {
			if pval, exists := params[ref]; exists {
				return pval
			}
			if pid, exists := physicalIDs[ref]; exists {
				return pid
			}

			return ref
		}
		if subStr, ok := val["Fn::Sub"].(string); ok {
			return resolveSub(subStr, params, physicalIDs)
		}
		if joinArgs, ok := val["Fn::Join"].([]any); ok && len(joinArgs) >= 2 {
			return resolveJoin(joinArgs, params, physicalIDs)
		}

		return fmt.Sprintf("%v", val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

func resolveSub(s string, params map[string]string, physicalIDs map[string]string) string {
	result := s
	for key, val := range params {
		result = strings.ReplaceAll(result, "${"+key+"}", val)
	}
	for key, val := range physicalIDs {
		result = strings.ReplaceAll(result, "${"+key+"}", val)
	}

	return result
}

func resolveJoin(args []any, params map[string]string, physicalIDs map[string]string) string {
	sep, _ := args[0].(string)
	items, _ := args[1].([]any)
	parts := make([]string, 0, len(items))
	for _, item := range items {
		parts = append(parts, ResolveValue(item, params, physicalIDs))
	}

	return strings.Join(parts, sep)
}

func resolveOutputs(tmpl *Template, params map[string]string, physicalIDs map[string]string) []Output {
	if len(tmpl.Outputs) == 0 {
		return nil
	}
	outputs := make([]Output, 0, len(tmpl.Outputs))
	for key, o := range tmpl.Outputs {
		outputs = append(outputs, Output{
			OutputKey:   key,
			OutputValue: ResolveValue(o.Value, params, physicalIDs),
			Description: o.Description,
		})
	}

	return outputs
}
