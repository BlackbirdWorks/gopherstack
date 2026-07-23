package elasticbeanstalk

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"
)

// --- Configuration Settings ---

type configurationOptionSettingType struct {
	Namespace  string `xml:"Namespace"`
	OptionName string `xml:"OptionName"`
	Value      string `xml:"Value"`
}

// configurationSettingsDescType mirrors AWS's ConfigurationSettingsDescription
// shape (aws-sdk-go-v2/service/elasticbeanstalk/types), which real AWS reuses
// verbatim for three distinct wire responses: DescribeConfigurationSettings's
// ConfigurationSettings list members, CreateConfigurationTemplateOutput, and
// UpdateConfigurationTemplateOutput -- see toConfigurationSettingsDesc and
// toEnvironmentConfigurationSettingsDesc below.
type configurationSettingsDescType struct {
	ApplicationName   string                           `xml:"ApplicationName"`
	EnvironmentName   string                           `xml:"EnvironmentName,omitempty"`
	TemplateName      string                           `xml:"TemplateName,omitempty"`
	SolutionStackName string                           `xml:"SolutionStackName"`
	PlatformArn       string                           `xml:"PlatformArn,omitempty"`
	DeploymentStatus  string                           `xml:"DeploymentStatus,omitempty"`
	DateCreated       string                           `xml:"DateCreated,omitempty"`
	DateUpdated       string                           `xml:"DateUpdated,omitempty"`
	Description       string                           `xml:"Description,omitempty"`
	OptionSettings    []configurationOptionSettingType `xml:"OptionSettings>member"`
}

// toOptionSettingsXML converts stored OptionSetting values to their XML wire shape.
func toOptionSettingsXML(settings []OptionSetting) []configurationOptionSettingType {
	out := make([]configurationOptionSettingType, 0, len(settings))

	for _, setting := range settings {
		out = append(out, configurationOptionSettingType{
			Namespace: setting.Namespace, OptionName: setting.OptionName, Value: setting.Value,
		})
	}

	return out
}

// toConfigurationSettingsDesc builds the ConfigurationSettingsDescription
// shape for a configuration template. DeploymentStatus is intentionally left
// empty: real AWS documents it as null "if this configuration is not
// associated with a running environment", which is always true for a
// template.
func toConfigurationSettingsDesc(tmpl *ConfigurationTemplate) configurationSettingsDescType {
	return configurationSettingsDescType{
		ApplicationName:   tmpl.ApplicationName,
		TemplateName:      tmpl.TemplateName,
		Description:       tmpl.Description,
		DateCreated:       tmpl.DateCreated,
		DateUpdated:       tmpl.DateUpdated,
		SolutionStackName: tmpl.SolutionStackName,
		PlatformArn:       tmpl.PlatformArn,
		OptionSettings:    toOptionSettingsXML(tmpl.OptionSettings),
	}
}

// toEnvironmentConfigurationSettingsDesc builds the
// ConfigurationSettingsDescription shape for an environment's live
// configuration set. DeploymentStatus is always "deployed": this backend
// applies environment updates synchronously (see PARITY.md), so there is
// never an in-flight "pending"/"failed" configuration set to observe.
func toEnvironmentConfigurationSettingsDesc(env *Environment) configurationSettingsDescType {
	return configurationSettingsDescType{
		ApplicationName:   env.ApplicationName,
		EnvironmentName:   env.EnvironmentName,
		SolutionStackName: env.SolutionStackName,
		PlatformArn:       env.PlatformARN,
		DeploymentStatus:  configDeploymentStatusDeployed,
		DateCreated:       env.DateCreated,
		DateUpdated:       env.DateUpdated,
		OptionSettings:    toOptionSettingsXML(env.OptionSettings),
	}
}

type describeConfigurationSettingsResult struct {
	ConfigurationSettings []configurationSettingsDescType `xml:"ConfigurationSettings>member"`
}

type describeConfigurationSettingsResponse struct {
	XMLName                             xml.Name                            `xml:"DescribeConfigurationSettingsResponse"`
	ResponseMetadata                    responseMetadata                    `xml:"ResponseMetadata"`
	Xmlns                               string                              `xml:"xmlns,attr"`
	DescribeConfigurationSettingsResult describeConfigurationSettingsResult `xml:"DescribeConfigurationSettingsResult"`
}

// handleDescribeConfigurationSettings returns the configuration settings for an environment
// or a configuration template. The Terraform provider calls this after environment creation
// to populate all_settings. SolutionStackName must be populated to prevent the provider
// from dereferencing a nil pointer.
func (h *Handler) handleDescribeConfigurationSettings(ctx context.Context, vals url.Values) (any, error) {
	appName := vals.Get("ApplicationName")
	envName := vals.Get("EnvironmentName")
	templateName := vals.Get("TemplateName")

	settings := make([]configurationSettingsDescType, 0)

	if envName != "" {
		envs := h.Backend.DescribeEnvironments(ctx, appName, []string{envName}, nil)

		if len(envs) > 0 {
			settings = append(settings, toEnvironmentConfigurationSettingsDesc(envs[0]))
		}
	} else if templateName != "" {
		templates := h.Backend.DescribeConfigurationTemplates(ctx, appName)

		for _, tmpl := range templates {
			if tmpl.TemplateName == templateName {
				settings = append(settings, toConfigurationSettingsDesc(tmpl))

				break
			}
		}
	}

	return &describeConfigurationSettingsResponse{
		Xmlns: ebXMLNS,
		DescribeConfigurationSettingsResult: describeConfigurationSettingsResult{
			ConfigurationSettings: settings,
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-describe-config-settings"},
	}, nil
}

// createConfigurationTemplateResponse is the XML response for CreateConfigurationTemplate.
type createConfigurationTemplateResponse struct {
	XMLName                           xml.Name                      `xml:"CreateConfigurationTemplateResponse"`
	Xmlns                             string                        `xml:"xmlns,attr"`
	ResponseMetadata                  responseMetadata              `xml:"ResponseMetadata"`
	CreateConfigurationTemplateResult configurationSettingsDescType `xml:"CreateConfigurationTemplateResult"`
}

// handleCreateConfigurationTemplate creates a new configuration template.
func (h *Handler) handleCreateConfigurationTemplate(ctx context.Context, vals url.Values) (any, error) {
	appName := vals.Get("ApplicationName")
	if appName == "" {
		return nil, fmt.Errorf("%w: ApplicationName is required", ErrInvalidParameter)
	}

	templateName := vals.Get("TemplateName")
	if templateName == "" {
		return nil, fmt.Errorf("%w: TemplateName is required", ErrInvalidParameter)
	}

	description := vals.Get("Description")
	solutionStack := vals.Get("SolutionStackName")
	platformArn := vals.Get("PlatformArn")
	tags := parseTagList(vals, "Tags.member")
	optionSettings := parseOptionSettings(vals, "OptionSettings.member")

	tmpl, err := h.Backend.CreateConfigurationTemplateWithParams(
		ctx,
		appName,
		templateName,
		description,
		solutionStack,
		tags,
		ConfigurationTemplateParams{PlatformArn: platformArn, OptionSettings: optionSettings},
	)
	if err != nil {
		return nil, err
	}

	return &createConfigurationTemplateResponse{
		Xmlns:                             ebXMLNS,
		CreateConfigurationTemplateResult: toConfigurationSettingsDesc(tmpl),
		ResponseMetadata:                  responseMetadata{RequestID: "eb-create-config-tmpl"},
	}, nil
}

// deleteConfigurationTemplateResponse is the XML response for DeleteConfigurationTemplate.
type deleteConfigurationTemplateResponse struct {
	XMLName          xml.Name         `xml:"DeleteConfigurationTemplateResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
}

// handleDeleteConfigurationTemplate deletes a configuration template.
func (h *Handler) handleDeleteConfigurationTemplate(ctx context.Context, vals url.Values) (any, error) {
	appName := vals.Get("ApplicationName")
	if appName == "" {
		return nil, fmt.Errorf("%w: ApplicationName is required", ErrInvalidParameter)
	}

	templateName := vals.Get("TemplateName")
	if templateName == "" {
		return nil, fmt.Errorf("%w: TemplateName is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteConfigurationTemplate(ctx, appName, templateName); err != nil {
		return nil, err
	}

	return &deleteConfigurationTemplateResponse{
		Xmlns:            ebXMLNS,
		ResponseMetadata: responseMetadata{RequestID: "eb-delete-config-tmpl"},
	}, nil
}

// deleteEnvironmentConfigurationResponse is the XML response for DeleteEnvironmentConfiguration.
type deleteEnvironmentConfigurationResponse struct {
	XMLName          xml.Name         `xml:"DeleteEnvironmentConfigurationResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
}

// handleDeleteEnvironmentConfiguration deletes the draft configuration for an environment.
func (h *Handler) handleDeleteEnvironmentConfiguration(ctx context.Context, vals url.Values) (any, error) {
	appName := vals.Get("ApplicationName")
	if appName == "" {
		return nil, fmt.Errorf("%w: ApplicationName is required", ErrInvalidParameter)
	}

	envName := vals.Get("EnvironmentName")
	if envName == "" {
		return nil, fmt.Errorf("%w: EnvironmentName is required", ErrInvalidParameter)
	}

	_ = h.Backend.DeleteEnvironmentConfiguration(ctx, appName, envName)

	return &deleteEnvironmentConfigurationResponse{
		Xmlns:            ebXMLNS,
		ResponseMetadata: responseMetadata{RequestID: "eb-delete-env-config"},
	}, nil
}

// describeConfigurationOptionsResponse is the XML response for DescribeConfigurationOptions.
type configurationOptionDescription struct {
	MaxLength      *int32   `xml:"MaxLength,omitempty"`
	MinValue       *int32   `xml:"MinValue,omitempty"`
	MaxValue       *int32   `xml:"MaxValue,omitempty"`
	Namespace      string   `xml:"Namespace"`
	Name           string   `xml:"Name"`
	DefaultValue   string   `xml:"DefaultValue,omitempty"`
	ChangeSeverity string   `xml:"ChangeSeverity,omitempty"`
	ValueType      string   `xml:"ValueType"`
	ValueOptions   []string `xml:"ValueOptions>member,omitempty"`
	UserDefined    bool     `xml:"UserDefined"`
}

type describeConfigurationOptionsResult struct {
	SolutionStackName string                           `xml:"SolutionStackName,omitempty"`
	PlatformArn       string                           `xml:"PlatformArn,omitempty"`
	Options           []configurationOptionDescription `xml:"Options>member"`
}

type describeConfigurationOptionsResponse struct {
	XMLName                            xml.Name                           `xml:"DescribeConfigurationOptionsResponse"`
	Xmlns                              string                             `xml:"xmlns,attr"`
	ResponseMetadata                   responseMetadata                   `xml:"ResponseMetadata"`
	DescribeConfigurationOptionsResult describeConfigurationOptionsResult `xml:"DescribeConfigurationOptionsResult"`
}

// resolveConfigurationOptionsPlatform determines the SolutionStackName/PlatformArn
// to echo back on a DescribeConfigurationOptions response, resolving from an
// explicit request parameter first and falling back to the referenced
// environment or configuration template, matching the request's own
// resolution order (ApplicationName+EnvironmentName or
// ApplicationName+TemplateName -- see DescribeConfigurationOptionsInput).
func (h *Handler) resolveConfigurationOptionsPlatform(ctx context.Context, vals url.Values) (string, string) {
	if ss := vals.Get("SolutionStackName"); ss != "" {
		return ss, vals.Get("PlatformArn")
	}

	if pa := vals.Get("PlatformArn"); pa != "" {
		return "", pa
	}

	appName := vals.Get("ApplicationName")

	if envName := vals.Get("EnvironmentName"); envName != "" {
		if envs := h.Backend.DescribeEnvironments(ctx, appName, []string{envName}, nil); len(envs) > 0 {
			return envs[0].SolutionStackName, envs[0].PlatformARN
		}
	}

	if templateName := vals.Get("TemplateName"); templateName != "" {
		for _, tmpl := range h.Backend.DescribeConfigurationTemplates(ctx, appName) {
			if tmpl.TemplateName == templateName {
				return tmpl.SolutionStackName, tmpl.PlatformArn
			}
		}
	}

	return "", ""
}

// handleDescribeConfigurationOptions returns the configuration option catalog,
// optionally restricted to the options named in the request's Options
// parameter (see filterConfigurationOptions/configurationOptionsCatalog in
// configuration_options.go).
func (h *Handler) handleDescribeConfigurationOptions(ctx context.Context, vals url.Values) (any, error) {
	filters := parseOptionSettings(vals, "Options.member")
	entries := filterConfigurationOptions(filters)

	options := make([]configurationOptionDescription, 0, len(entries))

	for _, e := range entries {
		options = append(options, configurationOptionDescription{
			Namespace:      e.Namespace,
			Name:           e.Name,
			DefaultValue:   e.DefaultValue,
			ChangeSeverity: e.ChangeSeverity,
			ValueType:      e.ValueType,
			ValueOptions:   e.ValueOptions,
			MaxLength:      e.MaxLength,
			MinValue:       e.MinValue,
			MaxValue:       e.MaxValue,
		})
	}

	solutionStackName, platformArn := h.resolveConfigurationOptionsPlatform(ctx, vals)

	return &describeConfigurationOptionsResponse{
		Xmlns: ebXMLNS,
		DescribeConfigurationOptionsResult: describeConfigurationOptionsResult{
			Options:           options,
			SolutionStackName: solutionStackName,
			PlatformArn:       platformArn,
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-describe-config-options"},
	}, nil
}

// updateConfigurationTemplateResponse is the XML response for UpdateConfigurationTemplate.
type updateConfigurationTemplateResponse struct {
	XMLName                           xml.Name                      `xml:"UpdateConfigurationTemplateResponse"`
	Xmlns                             string                        `xml:"xmlns,attr"`
	ResponseMetadata                  responseMetadata              `xml:"ResponseMetadata"`
	UpdateConfigurationTemplateResult configurationSettingsDescType `xml:"UpdateConfigurationTemplateResult"`
}

func (h *Handler) handleUpdateConfigurationTemplate(ctx context.Context, vals url.Values) (any, error) {
	appName := vals.Get("ApplicationName")
	if appName == "" {
		return nil, fmt.Errorf("%w: ApplicationName is required", ErrInvalidParameter)
	}

	templateName := vals.Get("TemplateName")
	if templateName == "" {
		return nil, fmt.Errorf("%w: TemplateName is required", ErrInvalidParameter)
	}

	params := UpdateConfigurationTemplateParams{
		Description:     vals.Get("Description"),
		OptionSettings:  parseOptionSettings(vals, "OptionSettings.member"),
		OptionsToRemove: parseOptionSettings(vals, "OptionsToRemove.member"),
	}

	tmpl, err := h.Backend.UpdateConfigurationTemplateWithParams(ctx, appName, templateName, params)
	if err != nil {
		return nil, err
	}

	return &updateConfigurationTemplateResponse{
		Xmlns:                             ebXMLNS,
		UpdateConfigurationTemplateResult: toConfigurationSettingsDesc(tmpl),
		ResponseMetadata:                  responseMetadata{RequestID: "eb-update-config-tmpl"},
	}, nil
}

// validateConfigurationSettingsResponse is the XML response for ValidateConfigurationSettings.
type validationMessage struct {
	Message    string `xml:"Message"`
	Severity   string `xml:"Severity"`
	Namespace  string `xml:"Namespace"`
	OptionName string `xml:"OptionName"`
}

type validateConfigurationSettingsResult struct {
	Messages []validationMessage `xml:"Messages>member"`
}

type validateConfigurationSettingsResponse struct {
	XMLName                             xml.Name                            `xml:"ValidateConfigurationSettingsResponse"`
	Xmlns                               string                              `xml:"xmlns,attr"`
	ResponseMetadata                    responseMetadata                    `xml:"ResponseMetadata"`
	ValidateConfigurationSettingsResult validateConfigurationSettingsResult `xml:"ValidateConfigurationSettingsResult"`
}

// knownNamespaces is the set of valid EB configuration namespaces for validation (improvement #13).
//
//nolint:gochecknoglobals // package-level constant set
var knownNamespaces = map[string]bool{
	nsAutoScalingASG:          true,
	nsAutoScalingLaunchConfig: true,
	nsAutoScalingTrigger:      true,
	nsEC2VPC:                  true,
	nsEBApplication:           true,
	nsEBCloudWatchLogs:        true,
	nsEBEnvironment:           true,
	nsEBEnvironmentProxy:      true,
	nsEBHealthReportingSystem: true,
	nsEBManagedActions:        true,
	nsEBMonitoring:            true,
	nsEBSNSTopics:             true,
	nsEBXRay:                  true,
	nsELBLoadBalancer:         true,
	nsELBv2LoadBalancer:       true,
	nsRDSDBInstance:           true,
}

func (h *Handler) handleValidateConfigurationSettings(_ context.Context, vals url.Values) (any, error) {
	messages := make([]validationMessage, 0)

	// Validate option settings namespaces (improvement #13)
	for i := 1; ; i++ {
		nsKey := fmt.Sprintf("OptionSettings.member.%d.Namespace", i)
		ns := vals.Get(nsKey)

		if ns == "" {
			break
		}

		if !knownNamespaces[ns] {
			optName := vals.Get(fmt.Sprintf("OptionSettings.member.%d.OptionName", i))
			messages = append(messages, validationMessage{
				Message:    fmt.Sprintf("Invalid namespace: %s", ns),
				Severity:   "error",
				Namespace:  ns,
				OptionName: optName,
			})
		}
	}

	return &validateConfigurationSettingsResponse{
		Xmlns: ebXMLNS,
		ValidateConfigurationSettingsResult: validateConfigurationSettingsResult{
			Messages: messages,
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-validate-config-settings"},
	}, nil
}
