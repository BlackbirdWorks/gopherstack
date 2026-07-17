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

type configurationSettingsDescType struct {
	ApplicationName   string                           `xml:"ApplicationName"`
	EnvironmentName   string                           `xml:"EnvironmentName,omitempty"`
	TemplateName      string                           `xml:"TemplateName,omitempty"`
	SolutionStackName string                           `xml:"SolutionStackName"`
	OptionSettings    []configurationOptionSettingType `xml:"OptionSettings>member"`
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
			env := envs[0]
			optionSettings := make([]configurationOptionSettingType, 0, len(env.OptionSettings))

			for _, setting := range env.OptionSettings {
				optionSettings = append(optionSettings, configurationOptionSettingType{
					Namespace: setting.Namespace, OptionName: setting.OptionName, Value: setting.Value,
				})
			}

			settings = append(settings, configurationSettingsDescType{
				ApplicationName:   env.ApplicationName,
				EnvironmentName:   env.EnvironmentName,
				SolutionStackName: env.SolutionStackName,
				OptionSettings:    optionSettings,
			})
		}
	} else if templateName != "" {
		templates := h.Backend.DescribeConfigurationTemplates(ctx, appName)

		for _, tmpl := range templates {
			if tmpl.TemplateName == templateName {
				settings = append(settings, configurationSettingsDescType{
					ApplicationName:   tmpl.ApplicationName,
					TemplateName:      tmpl.TemplateName,
					SolutionStackName: tmpl.SolutionStackName,
					OptionSettings:    make([]configurationOptionSettingType, 0),
				})

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

// configurationTemplateDescType is used in XML responses for configuration templates.
type configurationTemplateDescType struct {
	ApplicationName   string `xml:"ApplicationName"`
	TemplateName      string `xml:"TemplateName"`
	SolutionStackName string `xml:"SolutionStackName,omitempty"`
	Description       string `xml:"Description,omitempty"`
}

func toConfigTemplateDesc(tmpl *ConfigurationTemplate) configurationTemplateDescType {
	return configurationTemplateDescType{
		ApplicationName:   tmpl.ApplicationName,
		TemplateName:      tmpl.TemplateName,
		SolutionStackName: tmpl.SolutionStackName,
		Description:       tmpl.Description,
	}
}

// createConfigurationTemplateResponse is the XML response for CreateConfigurationTemplate.
type createConfigurationTemplateResponse struct {
	XMLName                           xml.Name                      `xml:"CreateConfigurationTemplateResponse"`
	Xmlns                             string                        `xml:"xmlns,attr"`
	CreateConfigurationTemplateResult configurationTemplateDescType `xml:"CreateConfigurationTemplateResult"`
	ResponseMetadata                  responseMetadata              `xml:"ResponseMetadata"`
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
	tags := parseTagList(vals, "Tags.member")

	tmpl, err := h.Backend.CreateConfigurationTemplate(
		ctx,
		appName,
		templateName,
		description,
		solutionStack,
		tags,
	)
	if err != nil {
		return nil, err
	}

	return &createConfigurationTemplateResponse{
		Xmlns:                             ebXMLNS,
		CreateConfigurationTemplateResult: toConfigTemplateDesc(tmpl),
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
	Namespace string `xml:"Namespace"`
	Name      string `xml:"Name"`
	ValueType string `xml:"ValueType"`
}

type describeConfigurationOptionsResult struct {
	Options []configurationOptionDescription `xml:"Options>member"`
}

type describeConfigurationOptionsResponse struct {
	XMLName                            xml.Name                           `xml:"DescribeConfigurationOptionsResponse"`
	Xmlns                              string                             `xml:"xmlns,attr"`
	ResponseMetadata                   responseMetadata                   `xml:"ResponseMetadata"`
	DescribeConfigurationOptionsResult describeConfigurationOptionsResult `xml:"DescribeConfigurationOptionsResult"`
}

func (h *Handler) handleDescribeConfigurationOptions(_ context.Context, _ url.Values) (any, error) {
	return &describeConfigurationOptionsResponse{
		Xmlns: ebXMLNS,
		DescribeConfigurationOptionsResult: describeConfigurationOptionsResult{
			Options: []configurationOptionDescription{
				{
					Namespace: nsAutoScalingASG,
					Name:      "MinSize",
					ValueType: optionValueTypeScalar,
				},
				{
					Namespace: nsAutoScalingASG,
					Name:      "MaxSize",
					ValueType: optionValueTypeScalar,
				},
				{
					Namespace: "aws:elasticbeanstalk:environment",
					Name:      "EnvironmentType",
					ValueType: optionValueTypeScalar,
				},
			},
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-describe-config-options"},
	}, nil
}

// updateConfigurationTemplateResponse is the XML response for UpdateConfigurationTemplate.
type updateConfigurationTemplateResponse struct {
	XMLName                           xml.Name                      `xml:"UpdateConfigurationTemplateResponse"`
	Xmlns                             string                        `xml:"xmlns,attr"`
	UpdateConfigurationTemplateResult configurationTemplateDescType `xml:"UpdateConfigurationTemplateResult"`
	ResponseMetadata                  responseMetadata              `xml:"ResponseMetadata"`
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

	description := vals.Get("Description")

	tmpl, err := h.Backend.UpdateConfigurationTemplate(ctx, appName, templateName, description)
	if err != nil {
		return nil, err
	}

	return &updateConfigurationTemplateResponse{
		Xmlns:                             ebXMLNS,
		UpdateConfigurationTemplateResult: toConfigTemplateDesc(tmpl),
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
	nsAutoScalingASG:                              true,
	"aws:autoscaling:launchconfiguration":         true,
	"aws:autoscaling:trigger":                     true,
	"aws:ec2:vpc":                                 true,
	"aws:elasticbeanstalk:application":            true,
	"aws:elasticbeanstalk:cloudwatch:logs":        true,
	"aws:elasticbeanstalk:environment":            true,
	"aws:elasticbeanstalk:environment:proxy":      true,
	"aws:elasticbeanstalk:healthreporting:system": true,
	"aws:elasticbeanstalk:managedactions":         true,
	"aws:elasticbeanstalk:monitoring":             true,
	"aws:elasticbeanstalk:sns:topics":             true,
	"aws:elasticbeanstalk:xray":                   true,
	"aws:elb:loadbalancer":                        true,
	"aws:elbv2:loadbalancer":                      true,
	"aws:rds:dbinstance":                          true,
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
