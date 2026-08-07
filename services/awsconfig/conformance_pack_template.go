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
// YAML (real AWS Config's documented alternative format -- "You can use a
// YAML template...") via yamlToJSON when JSON decoding fails. TemplateS3Uri
// and TemplateSSMDocumentDetails (the other two mutually-exclusive template
// sources PutConformancePack accepts) are validated for presence by the
// caller (see PutConformancePack) but deploy no rules here: fetching them
// needs cross-service S3/SSM access this backend has no wiring for within
// its edit boundary -- an honest gap (documented in PARITY.md), not a silent
// misparse. An unparsable-as-either body also deploys zero rules rather than
// erroring, matching PutConformancePack's existing "doesn't require a valid
// template to succeed" behavior.
func parseConformancePackConfigRules(templateBody, packName string) []*ConfigRule {
	if templateBody == "" {
		return nil
	}

	var tmpl conformancePackTemplate

	jsonBody := []byte(templateBody)
	if err := json.Unmarshal(jsonBody, &tmpl); err != nil {
		converted, yamlErr := yamlToJSON(jsonBody)
		if yamlErr != nil {
			return nil
		}

		if jsonErr := json.Unmarshal(converted, &tmpl); jsonErr != nil {
			return nil
		}
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

// yamlToJSON decodes a YAML document into a generic value and re-encodes it
// as JSON, so the rest of the parser (which only understands JSON struct
// tags) can consume either format uniformly. yaml.v3 decodes mappings into
// map[string]any (unlike yaml.v2's map[interface{}]any), which is already
// JSON-marshalable without a key-type conversion pass.
func yamlToJSON(body []byte) ([]byte, error) {
	var v any
	if err := yaml.Unmarshal(body, &v); err != nil {
		return nil, err
	}

	return json.Marshal(v)
}
