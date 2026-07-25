package awsconfig

import (
	"encoding/json"
	"sort"
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
// values they deploy, sorted by logical ID for deterministic ordering. Only
// JSON template bodies are supported (real AWS Config also accepts YAML,
// TemplateS3Uri, and TemplateSSMDocumentDetails; this emulator has no template
// fetcher/YAML parser, so those sources deploy no rules -- an honest gap, not
// a silent misparse: a YAML or unparsable body simply deploys zero rules
// rather than erroring, since PutConformancePack does not require a valid
// template to succeed in this emulator). Each resource without an explicit
// ConfigRuleName gets one derived deterministically from the pack name and
// logical ID, mirroring the "<pack>-<logicalId>-<suffix>" naming AWS assigns.
func parseConformancePackConfigRules(templateBody, packName string) []*ConfigRule {
	if templateBody == "" {
		return nil
	}

	var tmpl conformancePackTemplate
	if err := json.Unmarshal([]byte(templateBody), &tmpl); err != nil {
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
