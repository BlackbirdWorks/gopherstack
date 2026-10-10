package awsconfig

import (
	"encoding/json"
	"sort"

	"gopkg.in/yaml.v3"
)

// resourceTypeConfigRule is the CloudFormation resource type a conformance
// pack template uses to deploy a config rule (verified against real AWS
// Config's PutConformancePack docs: "You can use a YAML template with two
// resource types: Config rule (AWS::Config::ConfigRule) and remediation
// action (AWS::Config::RemediationConfiguration)").
const resourceTypeConfigRule = "AWS::Config::ConfigRule"

// conformancePackTemplateResource is a single CloudFormation-shaped resource
// entry inside a conformance pack template's top-level "Resources" map.
type conformancePackTemplateResource struct {
	Type       string                                      `json:"Type"`
	Properties conformancePackConfigRuleResourceProperties `json:"Properties"`
}

// conformancePackConfigRuleResourceProperties mirrors the CloudFormation
// AWS::Config::ConfigRule resource's "Properties" block. Field names match the
// real CFN resource schema (see the AWS::Config::ConfigRule page in the
// CloudFormation User Guide), which reuses the same PascalCase Source/Scope
// shapes as the ConfigRule API type.
type conformancePackConfigRuleResourceProperties struct {
	Source                    *ConfigRuleSource `json:"Source,omitempty"`
	Scope                     *ConfigRuleScope  `json:"Scope,omitempty"`
	ConfigRuleName            string            `json:"ConfigRuleName,omitempty"`
	Description               string            `json:"Description,omitempty"`
	MaximumExecutionFrequency string            `json:"MaximumExecutionFrequency,omitempty"`
	InputParameters           json.RawMessage   `json:"InputParameters,omitempty"`
}

// conformancePackTemplate is the minimal top-level shape parsed out of a
// conformance pack's TemplateBody.
type conformancePackTemplate struct {
	Resources map[string]conformancePackTemplateResource `json:"Resources"`
}

// parseConformancePackConfigRules extracts the AWS::Config::ConfigRule
// resources from a conformance pack's TemplateBody and returns the ConfigRule
// values they deploy, sorted by logical ID for deterministic ordering.
// TemplateBody is tried as JSON first (the common/fast case), falling back to
// YAML (real AWS Config's documented alternative format) via yamlToJSON when JSON decoding fails. An unparsable
// body deploys zero rules rather than erroring.
func parseConformancePackConfigRules(
	templateBody, packName string, params []ConformancePackInputParameter,
) []*ConfigRule {
	if templateBody == "" {
		return nil
	}

	jsonBody := []byte(templateBody)

	var doc map[string]any
	if err := json.Unmarshal(jsonBody, &doc); err != nil {
		converted, yamlErr := yamlToJSON(jsonBody)
		if yamlErr != nil {
			return nil
		}

		if jsonErr := json.Unmarshal(converted, &doc); jsonErr != nil {
			return nil
		}
	}

	resolved, err := json.Marshal(resolveTemplateRefs(doc, templateParamValues(doc, params)))
	if err != nil {
		return nil
	}

	var tmpl conformancePackTemplate
	if err = json.Unmarshal(resolved, &tmpl); err != nil {
		return nil
	}

	logicalIDs := make([]string, 0, len(tmpl.Resources))
	for id := range tmpl.Resources {
		logicalIDs = append(logicalIDs, id)
	}

	sort.Strings(logicalIDs)

	rules := make([]*ConfigRule, 0, len(logicalIDs))

	for _, id := range logicalIDs {
		res := tmpl.Resources[id]
		if res.Type != resourceTypeConfigRule {
			continue
		}

		rules = append(rules, configRuleFromTemplateResource(packName, id, res.Properties))
	}

	return rules
}

// configRuleFromTemplateResource builds a *ConfigRule from one parsed
// AWS::Config::ConfigRule template resource.
func configRuleFromTemplateResource(
	packName, logicalID string,
	props conformancePackConfigRuleResourceProperties,
) *ConfigRule {
	name := props.ConfigRuleName
	if name == "" {
		name = packName + "-" + logicalID
	}

	inputParams := ""
	if len(props.InputParameters) > 0 {
		inputParams = string(props.InputParameters)
	}

	return &ConfigRule{
		ConfigRuleName:            name,
		Description:               props.Description,
		InputParameters:           inputParams,
		MaximumExecutionFrequency: props.MaximumExecutionFrequency,
		Source:                    props.Source,
		Scope:                     props.Scope,
	}
}

// templateParamValues merges the template's Parameters defaults with the
// caller's ConformancePackInputParameters (the latter win).
func templateParamValues(doc map[string]any, params []ConformancePackInputParameter) map[string]any {
	values := map[string]any{}

	decls, _ := doc["Parameters"].(map[string]any)
	for name, decl := range decls {
		if d, ok := decl.(map[string]any); ok {
			if def, has := d["Default"]; has {
				values[name] = def
			}
		}
	}

	for _, p := range params {
		values[p.ParameterName] = p.ParameterValue
	}

	return values
}

// resolveTemplateRefs replaces every {"Ref": "<Param>"} node naming a known
// parameter with that parameter's value.
func resolveTemplateRefs(node any, values map[string]any) any {
	switch n := node.(type) {
	case map[string]any:
		if ref, ok := n["Ref"].(string); ok && len(n) == 1 {
			if v, known := values[ref]; known {
				return v
			}
		}

		out := make(map[string]any, len(n))
		for k, v := range n {
			out[k] = resolveTemplateRefs(v, values)
		}

		return out
	case []any:
		out := make([]any, len(n))
		for i, v := range n {
			out[i] = resolveTemplateRefs(v, values)
		}

		return out
	default:
		return node
	}
}

// yamlToJSON decodes a YAML document into a generic value and re-encodes it
// as JSON, so the rest of the parser (which only understands JSON struct
// tags) can consume either format uniformly. yaml.v3 decodes mappings into
// map[string]any (unlike yaml.v2's map[interface{}]any), which is already
// JSON-marshalable without a key-type conversion pass.
func yamlToJSON(body []byte) ([]byte, error) {
	var root yaml.Node
	if err := yaml.Unmarshal(body, &root); err != nil {
		return nil, err
	}

	rewriteRefTags(&root)

	var v any
	if err := root.Decode(&v); err != nil {
		return nil, err
	}

	return json.Marshal(v)
}

// rewriteRefTags turns the YAML short form "!Ref Name" into the long form
// mapping {Ref: Name} so it reaches resolveTemplateRefs.
func rewriteRefTags(n *yaml.Node) {
	if n.Tag == "!Ref" && n.Kind == yaml.ScalarNode {
		key := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "Ref"}
		val := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: n.Value}
		*n = yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Content: []*yaml.Node{key, val}}

		return
	}

	for _, c := range n.Content {
		rewriteRefTags(c)
	}
}
