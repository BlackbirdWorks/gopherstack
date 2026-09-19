package elasticbeanstalk

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
)

// --- Application operations ---

// appConfigTemplatesXML wraps the ConfigurationTemplates list for XML encoding.
// Using a pointer to this struct enables omitempty to suppress the outer element when empty.
type appConfigTemplatesXML struct {
	Members []string `xml:"member"`
}

// appVersionsXML wraps the Versions list for XML encoding, mirroring
// appConfigTemplatesXML. Real AWS's ApplicationDescription.Versions lists the
// version labels belonging to the application (see the CreateApplication API
// doc example response, which renders an empty `<Versions/>` element on a
// freshly created application).
type appVersionsXML struct {
	Members []string `xml:"member"`
}

// applicationDescType is used in XML responses.
type applicationDescType struct {
	ConfigurationTemplates  *appConfigTemplatesXML              `xml:"ConfigurationTemplates,omitempty"`
	Versions                *appVersionsXML                     `xml:"Versions,omitempty"`
	ResourceLifecycleConfig *applicationResourceLifecycleConfig `xml:"ResourceLifecycleConfig,omitempty"`
	ApplicationName         string                              `xml:"ApplicationName"`
	ApplicationArn          string                              `xml:"ApplicationArn"`
	Description             string                              `xml:"Description,omitempty"`
	DateCreated             string                              `xml:"DateCreated,omitempty"`
	DateUpdated             string                              `xml:"DateUpdated,omitempty"`
}

func toApplicationDesc(app *Application, configTemplateNames, versionLabels []string) applicationDescType {
	var templates *appConfigTemplatesXML
	if len(configTemplateNames) > 0 {
		templates = &appConfigTemplatesXML{Members: configTemplateNames}
	}

	var versions *appVersionsXML
	if len(versionLabels) > 0 {
		versions = &appVersionsXML{Members: versionLabels}
	}

	// ResourceLifecycleConfig is only rendered once a lifecycle service role
	// or version lifecycle rule has been set (via CreateApplication or
	// UpdateApplicationResourceLifecycle): the backend stores it on the
	// Application, but until this field existed it was never surfaced back
	// through CreateApplication/DescribeApplications/UpdateApplication,
	// making the stored value permanently unreadable.
	return applicationDescType{
		ApplicationName:         app.ApplicationName,
		ApplicationArn:          app.ApplicationARN,
		Description:             app.Description,
		DateCreated:             app.DateCreated,
		DateUpdated:             app.DateUpdated,
		ConfigurationTemplates:  templates,
		Versions:                versions,
		ResourceLifecycleConfig: toApplicationResourceLifecycleConfig(app),
	}
}

// applicationConfigTemplateNames returns the sorted configuration template
// names belonging to appName, for embedding in an ApplicationDescription.
func (h *Handler) applicationConfigTemplateNames(ctx context.Context, appName string) []string {
	templates := h.Backend.DescribeConfigurationTemplates(ctx, appName)
	names := make([]string, 0, len(templates))

	for _, tmpl := range templates {
		names = append(names, tmpl.TemplateName)
	}

	return names
}

// applicationVersionLabels returns the sorted application version labels
// belonging to appName, for embedding in an ApplicationDescription.
func (h *Handler) applicationVersionLabels(ctx context.Context, appName string) []string {
	versions := h.Backend.DescribeApplicationVersions(ctx, appName, nil)
	labels := make([]string, 0, len(versions))

	for _, ver := range versions {
		labels = append(labels, ver.VersionLabel)
	}

	return labels
}

type createApplicationResult struct {
	Application applicationDescType `xml:"Application"`
}

type createApplicationResponse struct {
	XMLName                 xml.Name                `xml:"CreateApplicationResponse"`
	Xmlns                   string                  `xml:"xmlns,attr"`
	CreateApplicationResult createApplicationResult `xml:"CreateApplicationResult"`
	ResponseMetadata        responseMetadata        `xml:"ResponseMetadata"`
}

func (h *Handler) handleCreateApplication(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("ApplicationName")
	if name == "" {
		return nil, fmt.Errorf("%w: ApplicationName is required", ErrInvalidParameter)
	}

	description := vals.Get("Description")

	tags := parseTagList(vals, "Tags.member")
	lifecycle := parseResourceLifecycleParams(vals, "ResourceLifecycleConfig")

	app, err := h.Backend.CreateApplicationWithParams(ctx, name, description, tags, lifecycle)
	if err != nil {
		return nil, err
	}

	templateNames := h.applicationConfigTemplateNames(ctx, name)

	return &createApplicationResponse{
		Xmlns: ebXMLNS,
		CreateApplicationResult: createApplicationResult{
			Application: toApplicationDesc(app, templateNames, nil),
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-create-app"},
	}, nil
}

type describeApplicationsResult struct {
	Applications []applicationDescType `xml:"Applications>member"`
}

type describeApplicationsResponse struct {
	XMLName                    xml.Name                   `xml:"DescribeApplicationsResponse"`
	Xmlns                      string                     `xml:"xmlns,attr"`
	ResponseMetadata           responseMetadata           `xml:"ResponseMetadata"`
	DescribeApplicationsResult describeApplicationsResult `xml:"DescribeApplicationsResult"`
}

func (h *Handler) handleDescribeApplications(ctx context.Context, vals url.Values) (any, error) {
	names := parseMembers(vals, "ApplicationNames.member")
	apps := h.Backend.DescribeApplications(ctx, names)

	members := make([]applicationDescType, 0, len(apps))

	for _, app := range apps {
		templateNames := h.applicationConfigTemplateNames(ctx, app.ApplicationName)
		versionLabels := h.applicationVersionLabels(ctx, app.ApplicationName)

		members = append(members, toApplicationDesc(app, templateNames, versionLabels))
	}

	return &describeApplicationsResponse{
		Xmlns:                      ebXMLNS,
		DescribeApplicationsResult: describeApplicationsResult{Applications: members},
		ResponseMetadata:           responseMetadata{RequestID: "eb-describe-apps"},
	}, nil
}

type updateApplicationResult struct {
	Application applicationDescType `xml:"Application"`
}

type updateApplicationResponse struct {
	XMLName                 xml.Name                `xml:"UpdateApplicationResponse"`
	Xmlns                   string                  `xml:"xmlns,attr"`
	UpdateApplicationResult updateApplicationResult `xml:"UpdateApplicationResult"`
	ResponseMetadata        responseMetadata        `xml:"ResponseMetadata"`
}

func (h *Handler) handleUpdateApplication(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("ApplicationName")
	if name == "" {
		return nil, fmt.Errorf("%w: ApplicationName is required", ErrInvalidParameter)
	}

	description := vals.Get("Description")

	app, err := h.Backend.UpdateApplication(ctx, name, description)
	if err != nil {
		return nil, err
	}

	templateNames := h.applicationConfigTemplateNames(ctx, name)
	versionLabels := h.applicationVersionLabels(ctx, name)

	return &updateApplicationResponse{
		Xmlns: ebXMLNS,
		UpdateApplicationResult: updateApplicationResult{
			Application: toApplicationDesc(app, templateNames, versionLabels),
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-update-app"},
	}, nil
}

type deleteApplicationResponse struct {
	XMLName          xml.Name         `xml:"DeleteApplicationResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
}

func (h *Handler) handleDeleteApplication(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("ApplicationName")
	if name == "" {
		return nil, fmt.Errorf("%w: ApplicationName is required", ErrInvalidParameter)
	}

	terminateEnvByForce, _ := strconv.ParseBool(vals.Get("TerminateEnvByForce"))

	if err := h.Backend.DeleteApplication(ctx, name, terminateEnvByForce); err != nil {
		return nil, err
	}

	return &deleteApplicationResponse{
		Xmlns:            ebXMLNS,
		ResponseMetadata: responseMetadata{RequestID: "eb-delete-app"},
	}, nil
}

// updateApplicationResourceLifecycleResponse is the XML response for UpdateApplicationResourceLifecycle.
type applicationResourceLifecycleConfig struct {
	VersionLifecycleConfig *applicationVersionLifecycleConfig `xml:"VersionLifecycleConfig,omitempty"`
	ServiceRole            string                             `xml:"ServiceRole,omitempty"`
}

// applicationVersionLifecycleConfig mirrors types.ApplicationVersionLifecycleConfig.
type applicationVersionLifecycleConfig struct {
	MaxAgeRule   *maxAgeRuleXML   `xml:"MaxAgeRule,omitempty"`
	MaxCountRule *maxCountRuleXML `xml:"MaxCountRule,omitempty"`
}

// maxAgeRuleXML mirrors types.MaxAgeRule.
type maxAgeRuleXML struct {
	Enabled            bool  `xml:"Enabled"`
	DeleteSourceFromS3 bool  `xml:"DeleteSourceFromS3,omitempty"`
	MaxAgeInDays       int32 `xml:"MaxAgeInDays,omitempty"`
}

// maxCountRuleXML mirrors types.MaxCountRule.
type maxCountRuleXML struct {
	Enabled            bool  `xml:"Enabled"`
	DeleteSourceFromS3 bool  `xml:"DeleteSourceFromS3,omitempty"`
	MaxCount           int32 `xml:"MaxCount,omitempty"`
}

// toApplicationResourceLifecycleConfig builds the wire ResourceLifecycleConfig
// from app's stored lifecycle fields, or nil if none is set at all.
func toApplicationResourceLifecycleConfig(app *Application) *applicationResourceLifecycleConfig {
	if app.ResourceLifecycleServiceRole == "" &&
		app.VersionLifecycleMaxAgeRule == nil &&
		app.VersionLifecycleMaxCountRule == nil {
		return nil
	}

	cfg := &applicationResourceLifecycleConfig{ServiceRole: app.ResourceLifecycleServiceRole}

	var versionConfig *applicationVersionLifecycleConfig

	if r := app.VersionLifecycleMaxAgeRule; r != nil {
		versionConfig = &applicationVersionLifecycleConfig{}
		versionConfig.MaxAgeRule = &maxAgeRuleXML{
			Enabled:            r.Enabled,
			DeleteSourceFromS3: r.DeleteSourceFromS3,
			MaxAgeInDays:       r.MaxAgeInDays,
		}
	}

	if r := app.VersionLifecycleMaxCountRule; r != nil {
		if versionConfig == nil {
			versionConfig = &applicationVersionLifecycleConfig{}
		}

		versionConfig.MaxCountRule = &maxCountRuleXML{
			Enabled:            r.Enabled,
			DeleteSourceFromS3: r.DeleteSourceFromS3,
			MaxCount:           r.MaxCount,
		}
	}

	cfg.VersionLifecycleConfig = versionConfig

	return cfg
}

// parseResourceLifecycleParams parses ResourceLifecycleConfig's fields
// (ServiceRole/VersionLifecycleConfig.MaxAgeRule/MaxCountRule) off the wire
// under the given prefix (e.g. "ResourceLifecycleConfig" -- both
// CreateApplication and UpdateApplicationResourceLifecycle share this exact
// shape, api_op_CreateApplication.go/api_op_UpdateApplicationResourceLifecycle.go).
// Returns nil if the request carried nothing at all under prefix.
func parseResourceLifecycleParams(vals url.Values, prefix string) *ApplicationResourceLifecycleParams {
	serviceRole := vals.Get(prefix + ".ServiceRole")

	maxAgePrefix := prefix + ".VersionLifecycleConfig.MaxAgeRule."
	maxCountPrefix := prefix + ".VersionLifecycleConfig.MaxCountRule."

	var maxAgeRule *MaxAgeRule
	if vals.Has(maxAgePrefix + "Enabled") {
		enabled, _ := strconv.ParseBool(vals.Get(maxAgePrefix + "Enabled"))
		deleteFromS3, _ := strconv.ParseBool(vals.Get(maxAgePrefix + "DeleteSourceFromS3"))
		maxAgeInDays, _ := strconv.ParseInt(vals.Get(maxAgePrefix+"MaxAgeInDays"), 10, 32)
		maxAgeRule = &MaxAgeRule{
			Enabled:            enabled,
			DeleteSourceFromS3: deleteFromS3,
			MaxAgeInDays:       int32(maxAgeInDays),
		}
	}

	var maxCountRule *MaxCountRule
	if vals.Has(maxCountPrefix + "Enabled") {
		enabled, _ := strconv.ParseBool(vals.Get(maxCountPrefix + "Enabled"))
		deleteFromS3, _ := strconv.ParseBool(vals.Get(maxCountPrefix + "DeleteSourceFromS3"))
		maxCount, _ := strconv.ParseInt(vals.Get(maxCountPrefix+"MaxCount"), 10, 32)
		maxCountRule = &MaxCountRule{
			Enabled:            enabled,
			DeleteSourceFromS3: deleteFromS3,
			MaxCount:           int32(maxCount),
		}
	}

	if serviceRole == "" && maxAgeRule == nil && maxCountRule == nil {
		return nil
	}

	return &ApplicationResourceLifecycleParams{
		ServiceRole:  serviceRole,
		MaxAgeRule:   maxAgeRule,
		MaxCountRule: maxCountRule,
	}
}

type updateApplicationResourceLifecycleResult struct {
	ApplicationName         string                             `xml:"ApplicationName"`
	ResourceLifecycleConfig applicationResourceLifecycleConfig `xml:"ResourceLifecycleConfig"`
}

type updateApplicationResourceLifecycleResponse struct { //nolint:lll // AWS XML operation name causes inherently long struct declaration
	XMLName                                  xml.Name                                 `xml:"UpdateApplicationResourceLifecycleResponse"` //nolint:lll // AWS XML operation name is inherently long
	Xmlns                                    string                                   `xml:"xmlns,attr"`
	UpdateApplicationResourceLifecycleResult updateApplicationResourceLifecycleResult `xml:"UpdateApplicationResourceLifecycleResult"` //nolint:lll // AWS XML operation name is inherently long
	ResponseMetadata                         responseMetadata                         `xml:"ResponseMetadata"`
}

func (h *Handler) handleUpdateApplicationResourceLifecycle(ctx context.Context, vals url.Values) (any, error) {
	appName := vals.Get("ApplicationName")
	if appName == "" {
		return nil, fmt.Errorf("%w: ApplicationName is required", ErrInvalidParameter)
	}

	lifecycle := parseResourceLifecycleParams(vals, "ResourceLifecycleConfig")
	if lifecycle == nil {
		lifecycle = &ApplicationResourceLifecycleParams{}
	}

	app, err := h.Backend.UpdateApplicationResourceLifecycleWithParams(ctx, appName, *lifecycle)
	if err != nil {
		return nil, err
	}

	cfg := toApplicationResourceLifecycleConfig(app)
	if cfg == nil {
		cfg = &applicationResourceLifecycleConfig{}
	}

	return &updateApplicationResourceLifecycleResponse{
		Xmlns: ebXMLNS,
		UpdateApplicationResourceLifecycleResult: updateApplicationResourceLifecycleResult{
			ApplicationName:         appName,
			ResourceLifecycleConfig: *cfg,
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-update-app-lifecycle"},
	}, nil
}
