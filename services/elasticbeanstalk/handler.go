package elasticbeanstalk

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	errInvalidParameterValue = "InvalidParameterValue"
)

const (
	ebXMLNS = "https://elasticbeanstalk.amazonaws.com/docs/2010-12-01/"

	nsAutoScalingASG = "aws:autoscaling:asg"

	optionValueTypeScalar      = "Scalar"
	platformLifecycleSupported = "Supported"

	quotaApplications        = 75
	quotaApplicationVersions = 1000
	quotaConfigTemplates     = 2000
	quotaCustomPlatforms     = 25
	quotaEnvironments        = 200

	// healthColorGreen is the color label for a healthy environment.
	healthColorGreen = "Green"
	// healthRefreshedAt is a placeholder refresh timestamp for environment health responses.
	healthRefreshedAt = "2026-01-01T00:00:00Z"
)

// formOpFunc is the function type for a dispatched form-encoded operation.
type formOpFunc func(context.Context, url.Values) (any, error)

// Handler is the Echo HTTP handler for Elastic Beanstalk operations.
type Handler struct {
	Backend *InMemoryBackend
	ops     map[string]formOpFunc
}

// NewHandler creates a new Elastic Beanstalk handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// buildOps constructs the dispatch table mapping action names to handlers.
// It is called once in NewHandler and the result is cached on h.ops.
func (h *Handler) buildOps() map[string]formOpFunc {
	return map[string]formOpFunc{
		"AbortEnvironmentUpdate":                  h.handleAbortEnvironmentUpdate,
		"ApplyEnvironmentManagedAction":           h.handleApplyEnvironmentManagedAction,
		"AssociateEnvironmentOperationsRole":      h.handleAssociateEnvironmentOperationsRole,
		"CheckDNSAvailability":                    h.handleCheckDNSAvailability,
		"CloneEnvironment":                        h.handleCloneEnvironment,
		"ComposeEnvironments":                     h.handleComposeEnvironments,
		"CreateApplication":                       h.handleCreateApplication,
		"CreateConfigurationTemplate":             h.handleCreateConfigurationTemplate,
		"CreateEnvironment":                       h.handleCreateEnvironment,
		"CreateApplicationVersion":                h.handleCreateApplicationVersion,
		"CreatePlatformVersion":                   h.handleCreatePlatformVersion,
		"CreateStorageLocation":                   h.handleCreateStorageLocation,
		"DeleteApplication":                       h.handleDeleteApplication,
		"DeleteApplicationVersion":                h.handleDeleteApplicationVersion,
		"DeleteConfigurationTemplate":             h.handleDeleteConfigurationTemplate,
		"DeleteEnvironmentConfiguration":          h.handleDeleteEnvironmentConfiguration,
		"DeletePlatformVersion":                   h.handleDeletePlatformVersion,
		"DescribeAccountAttributes":               h.handleDescribeAccountAttributes,
		"DescribeApplications":                    h.handleDescribeApplications,
		"DescribeApplicationVersions":             h.handleDescribeApplicationVersions,
		"DescribeConfigurationOptions":            h.handleDescribeConfigurationOptions,
		"DescribeConfigurationSettings":           h.handleDescribeConfigurationSettings,
		"DescribeEnvironmentHealth":               h.handleDescribeEnvironmentHealth,
		"DescribeEnvironmentManagedActionHistory": h.handleDescribeEnvironmentManagedActionHistory,
		"DescribeEnvironmentManagedActions":       h.handleDescribeEnvironmentManagedActions,
		"DescribeEnvironmentResources":            h.handleDescribeEnvironmentResources,
		"DescribeEnvironments":                    h.handleDescribeEnvironments,
		"DescribeEvents":                          h.handleDescribeEvents,
		"DescribeInstancesHealth":                 h.handleDescribeInstancesHealth,
		"DescribePlatformVersion":                 h.handleDescribePlatformVersion,
		"DisassociateEnvironmentOperationsRole":   h.handleDisassociateEnvironmentOperationsRole,
		"ListAvailableSolutionStacks":             h.handleListAvailableSolutionStacks,
		"ListPlatformBranches":                    h.handleListPlatformBranches,
		"ListPlatformVersions":                    h.handleListPlatformVersions,
		"ListTagsForResource":                     h.handleListTagsForResource,
		"RebuildEnvironment":                      h.handleRebuildEnvironment,
		"RequestEnvironmentInfo":                  h.handleRequestEnvironmentInfo,
		"RestartAppServer":                        h.handleRestartAppServer,
		"RetrieveEnvironmentInfo":                 h.handleRetrieveEnvironmentInfo,
		"SwapEnvironmentCNAMEs":                   h.handleSwapEnvironmentCNAMEs,
		"TerminateEnvironment":                    h.handleTerminateEnvironment,
		"UpdateApplication":                       h.handleUpdateApplication,
		"UpdateApplicationResourceLifecycle":      h.handleUpdateApplicationResourceLifecycle,
		"UpdateApplicationVersion":                h.handleUpdateApplicationVersion,
		"UpdateConfigurationTemplate":             h.handleUpdateConfigurationTemplate,
		"UpdateEnvironment":                       h.handleUpdateEnvironment,
		"UpdateTagsForResource":                   h.handleUpdateTagsForResource,
		"ValidateConfigurationSettings":           h.handleValidateConfigurationSettings,
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Elasticbeanstalk" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"AbortEnvironmentUpdate",
		"ApplyEnvironmentManagedAction",
		"AssociateEnvironmentOperationsRole",
		"CheckDNSAvailability",
		"CloneEnvironment",
		"ComposeEnvironments",
		"CreateApplication",
		"CreateConfigurationTemplate",
		"CreateEnvironment",
		"CreateApplicationVersion",
		"CreatePlatformVersion",
		"CreateStorageLocation",
		"DeleteApplication",
		"DeleteApplicationVersion",
		"DeleteConfigurationTemplate",
		"DeleteEnvironmentConfiguration",
		"DeletePlatformVersion",
		"DescribeAccountAttributes",
		"DescribeApplications",
		"DescribeApplicationVersions",
		"DescribeConfigurationOptions",
		"DescribeConfigurationSettings",
		"DescribeEnvironmentHealth",
		"DescribeEnvironmentManagedActionHistory",
		"DescribeEnvironmentManagedActions",
		"DescribeEnvironmentResources",
		"DescribeEnvironments",
		"DescribeEvents",
		"DescribeInstancesHealth",
		"DescribePlatformVersion",
		"DisassociateEnvironmentOperationsRole",
		"ListAvailableSolutionStacks",
		"ListPlatformBranches",
		"ListPlatformVersions",
		"ListTagsForResource",
		"RebuildEnvironment",
		"RequestEnvironmentInfo",
		"RestartAppServer",
		"RetrieveEnvironmentInfo",
		"SwapEnvironmentCNAMEs",
		"TerminateEnvironment",
		"UpdateApplication",
		"UpdateApplicationResourceLifecycle",
		"UpdateApplicationVersion",
		"UpdateConfigurationTemplate",
		"UpdateEnvironment",
		"UpdateTagsForResource",
		"ValidateConfigurationSettings",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "elasticbeanstalk" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// ebAPIVersion is the API version string used by Elastic Beanstalk requests.
const ebAPIVersion = "Version=2010-12-01"

// RouteMatcher returns a function that matches Elastic Beanstalk requests.
// Elastic Beanstalk uses the same Version=2010-12-01 as SES, so we disambiguate
// by matching on the Action field against the list of supported EB operations.
// We also require Version=2010-12-01 to avoid matching other services (e.g. SNS
// with Version=2010-03-31 or CloudWatch with Version=2010-08-01) that share the
// same action names (e.g. ListTagsForResource).
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()
		if r.Method != http.MethodPost {
			return false
		}

		if strings.HasPrefix(r.URL.Path, "/dashboard/") {
			return false
		}

		ct := r.Header.Get("Content-Type")
		if !strings.Contains(ct, "application/x-www-form-urlencoded") {
			return false
		}

		body, err := httputils.ReadBody(r)
		if err != nil {
			return false
		}

		if !strings.Contains(string(body), ebAPIVersion) {
			return false
		}

		vals, err := url.ParseQuery(string(body))
		if err != nil {
			return false
		}

		if vals.Get("Version") != "2010-12-01" {
			return false
		}

		action := vals.Get("Action")

		return slices.Contains(h.GetSupportedOperations(), action)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityFormStandard }

// ExtractOperation extracts the Elastic Beanstalk action from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return "Unknown"
	}

	action := r.Form.Get("Action")
	if action == "" {
		return "Unknown"
	}

	return action
}

// ExtractResource extracts a resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return ""
	}

	if name := r.Form.Get("ApplicationName"); name != "" {
		return name
	}

	return r.Form.Get("EnvironmentName")
}

// Handler returns the Echo handler function for Elastic Beanstalk requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		if err := r.ParseForm(); err != nil {
			return h.writeError(
				c,
				http.StatusInternalServerError,
				"InternalFailure",
				"failed to read request body",
			)
		}

		vals := r.Form
		action := vals.Get("Action")
		if action == "" {
			return h.writeError(
				c,
				http.StatusBadRequest,
				"MissingAction",
				"missing Action parameter",
			)
		}

		log := logger.Load(r.Context())
		log.Debug("elasticbeanstalk request", "action", action)

		ctx := r.Context()
		region := httputils.ExtractRegionFromRequest(r, h.Backend.Region())
		ctx = context.WithValue(ctx, regionContextKey{}, region)

		resp, opErr := h.dispatch(ctx, action, vals)
		if opErr != nil {
			return h.handleOpError(c, opErr)
		}

		xmlBytes, err := marshalXML(resp)
		if err != nil {
			return h.writeError(
				c,
				http.StatusInternalServerError,
				"InternalFailure",
				"internal server error",
			)
		}

		return c.Blob(http.StatusOK, "text/xml", xmlBytes)
	}
}

// dispatch routes the Elastic Beanstalk action to the appropriate handler.
func (h *Handler) dispatch(ctx context.Context, action string, vals url.Values) (any, error) {
	if fn, ok := h.ops[action]; ok {
		return fn(ctx, vals)
	}

	return nil, fmt.Errorf("%w: %s", ErrUnknownAction, action)
}

// --- Application operations ---

// appConfigTemplatesXML wraps the ConfigurationTemplates list for XML encoding.
// Using a pointer to this struct enables omitempty to suppress the outer element when empty.
type appConfigTemplatesXML struct {
	Members []string `xml:"member"`
}

// applicationDescType is used in XML responses.
type applicationDescType struct {
	ConfigurationTemplates *appConfigTemplatesXML `xml:"ConfigurationTemplates,omitempty"`
	ApplicationName        string                 `xml:"ApplicationName"`
	ApplicationArn         string                 `xml:"ApplicationArn"`
	Description            string                 `xml:"Description,omitempty"`
	DateCreated            string                 `xml:"DateCreated,omitempty"`
}

func toApplicationDesc(app *Application, configTemplateNames []string) applicationDescType {
	var templates *appConfigTemplatesXML
	if len(configTemplateNames) > 0 {
		templates = &appConfigTemplatesXML{Members: configTemplateNames}
	}

	return applicationDescType{
		ApplicationName:        app.ApplicationName,
		ApplicationArn:         app.ApplicationARN,
		Description:            app.Description,
		DateCreated:            app.DateCreated,
		ConfigurationTemplates: templates,
	}
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

	app, err := h.Backend.CreateApplication(ctx, name, description, tags)
	if err != nil {
		return nil, err
	}

	return &createApplicationResponse{
		Xmlns:                   ebXMLNS,
		CreateApplicationResult: createApplicationResult{Application: toApplicationDesc(app, nil)},
		ResponseMetadata:        responseMetadata{RequestID: "eb-create-app"},
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
		templates := h.Backend.DescribeConfigurationTemplates(ctx, app.ApplicationName)
		templateNames := make([]string, 0, len(templates))

		for _, tmpl := range templates {
			templateNames = append(templateNames, tmpl.TemplateName)
		}

		members = append(members, toApplicationDesc(app, templateNames))
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

	return &updateApplicationResponse{
		Xmlns:                   ebXMLNS,
		UpdateApplicationResult: updateApplicationResult{Application: toApplicationDesc(app, nil)},
		ResponseMetadata:        responseMetadata{RequestID: "eb-update-app"},
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

	if err := h.Backend.DeleteApplication(ctx, name); err != nil {
		return nil, err
	}

	return &deleteApplicationResponse{
		Xmlns:            ebXMLNS,
		ResponseMetadata: responseMetadata{RequestID: "eb-delete-app"},
	}, nil
}

// --- Environment operations ---

type environmentTierType struct {
	Name    string `xml:"Name"`
	Type    string `xml:"Type"`
	Version string `xml:"Version"`
}

type environmentDescType struct {
	ApplicationName   string              `xml:"ApplicationName"`
	EnvironmentName   string              `xml:"EnvironmentName"`
	EnvironmentID     string              `xml:"EnvironmentId"`
	EnvironmentArn    string              `xml:"EnvironmentArn"`
	Description       string              `xml:"Description,omitempty"`
	SolutionStackName string              `xml:"SolutionStackName"`
	PlatformArn       string              `xml:"PlatformArn,omitempty"`
	VersionLabel      string              `xml:"VersionLabel,omitempty"`
	OperationsRole    string              `xml:"OperationsRole,omitempty"`
	DateCreated       string              `xml:"DateCreated,omitempty"`
	Status            string              `xml:"Status"`
	Health            string              `xml:"Health"`
	Tier              environmentTierType `xml:"Tier"`
	CNAME             string              `xml:"CNAME"`
	EndpointURL       string              `xml:"EndpointURL"`
}

func toEnvironmentDesc(env *Environment) environmentDescType {
	cname := env.CNAME
	if cname == "" {
		cname = env.EnvironmentName + "." + env.Region + ".elasticbeanstalk.com"
	}

	tierName := env.TierName
	if tierName == "" {
		tierName = env.Tier
	}

	if tierName == "" {
		tierName = "WebServer"
	}

	tierType := env.TierType
	if tierType == "" {
		tierType = "Standard"
	}

	tierVersion := env.TierVersion
	if tierVersion == "" {
		tierVersion = "1.0"
	}

	return environmentDescType{
		ApplicationName:   env.ApplicationName,
		EnvironmentName:   env.EnvironmentName,
		EnvironmentID:     env.EnvironmentID,
		EnvironmentArn:    env.EnvironmentARN,
		Description:       env.Description,
		SolutionStackName: env.SolutionStackName,
		PlatformArn:       env.PlatformARN,
		VersionLabel:      env.VersionLabel,
		OperationsRole:    env.OperationsRole,
		DateCreated:       env.DateCreated,
		Status:            env.Status,
		Health:            env.Health,
		Tier: environmentTierType{
			Name:    tierName,
			Type:    tierType,
			Version: tierVersion,
		},
		CNAME:       cname,
		EndpointURL: cname,
	}
}

type createEnvironmentResponse struct {
	XMLName                 xml.Name            `xml:"CreateEnvironmentResponse"`
	Xmlns                   string              `xml:"xmlns,attr"`
	CreateEnvironmentResult environmentDescType `xml:"CreateEnvironmentResult"`
	ResponseMetadata        responseMetadata    `xml:"ResponseMetadata"`
}

func (h *Handler) handleCreateEnvironment(ctx context.Context, vals url.Values) (any, error) {
	appName := vals.Get("ApplicationName")
	envName := vals.Get("EnvironmentName")

	if appName == "" {
		return nil, fmt.Errorf("%w: ApplicationName is required", ErrInvalidParameter)
	}

	if envName == "" {
		return nil, fmt.Errorf("%w: EnvironmentName is required", ErrInvalidParameter)
	}

	solutionStack := vals.Get("SolutionStackName")
	description := vals.Get("Description")
	tags := parseTagList(vals, "Tags.member")
	optionSettings := parseOptionSettings(vals, "OptionSettings.member")

	// Parse tier (improvement #1)
	tierName := vals.Get("Tier.Name")
	tierType := vals.Get("Tier.Type")
	tierVersion := vals.Get("Tier.Version")

	// Parse load balancer type from OptionSettings (improvement #14)
	lbType := parseOptionSetting(vals, "aws:elasticbeanstalk:environment", "LoadBalancerType")

	// Parse VPC config from OptionSettings (improvement #15)
	vpcID := parseOptionSetting(vals, "aws:ec2:vpc", "VPCId")
	subnets := parseOptionSetting(vals, "aws:ec2:vpc", "Subnets")

	// Parse instance profile from OptionSettings (improvement #16)
	instanceProfile := parseOptionSetting(vals, "aws:autoscaling:launchconfiguration", "IamInstanceProfile")
	if err := ValidateInstanceProfileARN(instanceProfile); err != nil {
		return nil, err
	}

	// Parse custom AMI from OptionSettings (improvement #5)
	customAMI := parseOptionSetting(vals, "aws:autoscaling:launchconfiguration", "ImageId")

	params := CreateEnvironmentParams{
		TierType:         tierType,
		TierName:         tierName,
		TierVersion:      tierVersion,
		CNAMEPrefix:      vals.Get("CNAMEPrefix"),
		PlatformARN:      vals.Get("PlatformArn"),
		TemplateName:     vals.Get("TemplateName"),
		VersionLabel:     vals.Get("VersionLabel"),
		OperationsRole:   vals.Get("OperationsRole"),
		LoadBalancerType: lbType,
		VPCID:            vpcID,
		Subnets:          subnets,
		InstanceProfile:  instanceProfile,
		CustomAMI:        customAMI,
		OptionSettings:   optionSettings,
	}

	env, err := h.Backend.CreateEnvironment(ctx, appName, envName, solutionStack, description, tags, params)
	if err != nil {
		return nil, err
	}

	return &createEnvironmentResponse{
		Xmlns:                   ebXMLNS,
		CreateEnvironmentResult: toEnvironmentDesc(env),
		ResponseMetadata:        responseMetadata{RequestID: "eb-create-env"},
	}, nil
}

type describeEnvironmentsResult struct {
	Environments []environmentDescType `xml:"Environments>member"`
}

type describeEnvironmentsResponse struct {
	XMLName                    xml.Name                   `xml:"DescribeEnvironmentsResponse"`
	Xmlns                      string                     `xml:"xmlns,attr"`
	ResponseMetadata           responseMetadata           `xml:"ResponseMetadata"`
	DescribeEnvironmentsResult describeEnvironmentsResult `xml:"DescribeEnvironmentsResult"`
}

func (h *Handler) handleDescribeEnvironments(ctx context.Context, vals url.Values) (any, error) {
	appName := vals.Get("ApplicationName")
	envNames := parseMembers(vals, "EnvironmentNames.member")
	envIDs := parseMembers(vals, "EnvironmentIds.member")
	envs := h.Backend.DescribeEnvironments(ctx, appName, envNames, envIDs)

	members := make([]environmentDescType, 0, len(envs))

	for _, env := range envs {
		members = append(members, toEnvironmentDesc(env))
	}

	return &describeEnvironmentsResponse{
		Xmlns:                      ebXMLNS,
		DescribeEnvironmentsResult: describeEnvironmentsResult{Environments: members},
		ResponseMetadata:           responseMetadata{RequestID: "eb-describe-envs"},
	}, nil
}

type updateEnvironmentResponse struct {
	XMLName                 xml.Name            `xml:"UpdateEnvironmentResponse"`
	Xmlns                   string              `xml:"xmlns,attr"`
	UpdateEnvironmentResult environmentDescType `xml:"UpdateEnvironmentResult"`
	ResponseMetadata        responseMetadata    `xml:"ResponseMetadata"`
}

func (h *Handler) handleUpdateEnvironment(ctx context.Context, vals url.Values) (any, error) {
	envName := vals.Get("EnvironmentName")
	if envName == "" {
		return nil, fmt.Errorf("%w: EnvironmentName is required", ErrInvalidParameter)
	}

	appName := vals.Get("ApplicationName")
	description := vals.Get("Description")

	// If no app name provided, search across all environments for this name.
	if appName == "" {
		envs := h.Backend.DescribeEnvironments(ctx, "", []string{envName}, nil)

		if len(envs) == 1 {
			appName = envs[0].ApplicationName
		} else if len(envs) > 1 {
			return nil, fmt.Errorf(
				"%w: multiple environments named %s; please specify ApplicationName",
				ErrInvalidParameter,
				envName,
			)
		}
		// len(envs) == 0: let the backend return a not-found error below.
	}

	env, err := h.Backend.UpdateEnvironmentWithParams(ctx, appName, envName, UpdateEnvironmentParams{
		Description:       description,
		SolutionStackName: vals.Get("SolutionStackName"),
		PlatformARN:       vals.Get("PlatformArn"),
		TemplateName:      vals.Get("TemplateName"),
		VersionLabel:      vals.Get("VersionLabel"),
		TierName:          vals.Get("Tier.Name"),
		TierType:          vals.Get("Tier.Type"),
		TierVersion:       vals.Get("Tier.Version"),
		OptionSettings:    parseOptionSettings(vals, "OptionSettings.member"),
		OptionsToRemove:   parseOptionSettings(vals, "OptionsToRemove.member"),
	})
	if err != nil {
		return nil, err
	}

	return &updateEnvironmentResponse{
		Xmlns:                   ebXMLNS,
		UpdateEnvironmentResult: toEnvironmentDesc(env),
		ResponseMetadata:        responseMetadata{RequestID: "eb-update-env"},
	}, nil
}

type terminateEnvironmentResponse struct {
	XMLName                    xml.Name            `xml:"TerminateEnvironmentResponse"`
	Xmlns                      string              `xml:"xmlns,attr"`
	TerminateEnvironmentResult environmentDescType `xml:"TerminateEnvironmentResult"`
	ResponseMetadata           responseMetadata    `xml:"ResponseMetadata"`
}

func (h *Handler) handleTerminateEnvironment(ctx context.Context, vals url.Values) (any, error) {
	envName := vals.Get("EnvironmentName")
	if envName == "" {
		return nil, fmt.Errorf("%w: EnvironmentName is required", ErrInvalidParameter)
	}

	appName := vals.Get("ApplicationName")

	// If no app name provided, search across all environments for this name.
	if appName == "" {
		envs := h.Backend.DescribeEnvironments(ctx, "", []string{envName}, nil)
		switch len(envs) {
		case 0:
			// No matching environments; let the backend handle the not-found case.
		case 1:
			appName = envs[0].ApplicationName
		default:
			return nil, fmt.Errorf(
				"%w: multiple environments named %s; please specify ApplicationName",
				ErrInvalidParameter,
				envName,
			)
		}
	}

	env, err := h.Backend.TerminateEnvironment(ctx, appName, envName)
	if err != nil {
		return nil, err
	}

	return &terminateEnvironmentResponse{
		Xmlns:                      ebXMLNS,
		TerminateEnvironmentResult: toEnvironmentDesc(env),
		ResponseMetadata:           responseMetadata{RequestID: "eb-terminate-env"},
	}, nil
}

// --- Application Version operations ---

type appVersionDescType struct {
	SourceBundle           *s3LocationType             `xml:"SourceBundle,omitempty"`
	SourceBuildInformation *sourceBuildInformationType `xml:"SourceBuildInformation,omitempty"`
	ApplicationName        string                      `xml:"ApplicationName"`
	VersionLabel           string                      `xml:"VersionLabel"`
	ApplicationVersionArn  string                      `xml:"ApplicationVersionArn"`
	Description            string                      `xml:"Description,omitempty"`
	DateCreated            string                      `xml:"DateCreated,omitempty"`
	Status                 string                      `xml:"Status"`
}

type s3LocationType struct {
	S3Bucket string `xml:"S3Bucket"`
	S3Key    string `xml:"S3Key"`
}

type sourceBuildInformationType struct {
	SourceType       string `xml:"SourceType"`
	SourceRepository string `xml:"SourceRepository"`
	SourceLocation   string `xml:"SourceLocation"`
}

func toAppVersionDesc(ver *ApplicationVersion) appVersionDescType {
	desc := appVersionDescType{
		ApplicationName:       ver.ApplicationName,
		VersionLabel:          ver.VersionLabel,
		ApplicationVersionArn: ver.ApplicationVersionARN,
		Description:           ver.Description,
		DateCreated:           ver.DateCreated,
		Status:                ver.Status,
	}
	if ver.S3Bucket != "" || ver.S3Key != "" {
		desc.SourceBundle = &s3LocationType{S3Bucket: ver.S3Bucket, S3Key: ver.S3Key}
	}
	if ver.SourceBuildInformation != nil {
		desc.SourceBuildInformation = &sourceBuildInformationType{
			SourceType:       ver.SourceBuildInformation.SourceType,
			SourceRepository: ver.SourceBuildInformation.SourceRepository,
			SourceLocation:   ver.SourceBuildInformation.SourceLocation,
		}
	}

	return desc
}

type createApplicationVersionResult struct {
	ApplicationVersion appVersionDescType `xml:"ApplicationVersion"`
}

type createApplicationVersionResponse struct {
	XMLName                        xml.Name                       `xml:"CreateApplicationVersionResponse"`
	Xmlns                          string                         `xml:"xmlns,attr"`
	CreateApplicationVersionResult createApplicationVersionResult `xml:"CreateApplicationVersionResult"`
	ResponseMetadata               responseMetadata               `xml:"ResponseMetadata"`
}

func (h *Handler) handleCreateApplicationVersion(ctx context.Context, vals url.Values) (any, error) {
	appName := vals.Get("ApplicationName")
	versionLabel := vals.Get("VersionLabel")

	if appName == "" {
		return nil, fmt.Errorf("%w: ApplicationName is required", ErrInvalidParameter)
	}

	if versionLabel == "" {
		return nil, fmt.Errorf("%w: VersionLabel is required", ErrInvalidParameter)
	}

	description := vals.Get("Description")
	tags := parseTagList(vals, "Tags.member")

	// Parse S3 source bundle (improvement #8)
	s3Bucket := vals.Get("SourceBundle.S3Bucket")
	s3Key := vals.Get("SourceBundle.S3Key")

	ver, err := h.Backend.CreateApplicationVersionWithParams(ctx, appName, versionLabel, ApplicationVersionParams{
		Description:            description,
		S3Bucket:               s3Bucket,
		S3Key:                  s3Key,
		Tags:                   tags,
		Process:                strings.EqualFold(vals.Get("Process"), "true"),
		AutoCreateApplication:  strings.EqualFold(vals.Get("AutoCreateApplication"), "true"),
		SourceBuildInformation: parseSourceBuildInformation(vals),
	})
	if err != nil {
		return nil, err
	}

	return &createApplicationVersionResponse{
		Xmlns: ebXMLNS,
		CreateApplicationVersionResult: createApplicationVersionResult{
			ApplicationVersion: toAppVersionDesc(ver),
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-create-ver"},
	}, nil
}

type describeApplicationVersionsResult struct {
	ApplicationVersions []appVersionDescType `xml:"ApplicationVersions>member"`
}

type describeApplicationVersionsResponse struct {
	XMLName                           xml.Name                          `xml:"DescribeApplicationVersionsResponse"`
	Xmlns                             string                            `xml:"xmlns,attr"`
	ResponseMetadata                  responseMetadata                  `xml:"ResponseMetadata"`
	DescribeApplicationVersionsResult describeApplicationVersionsResult `xml:"DescribeApplicationVersionsResult"`
}

func (h *Handler) handleDescribeApplicationVersions(ctx context.Context, vals url.Values) (any, error) {
	appName := vals.Get("ApplicationName")
	versionLabels := parseMembers(vals, "VersionLabels.member")
	vers := h.Backend.DescribeApplicationVersions(ctx, appName, versionLabels)

	members := make([]appVersionDescType, 0, len(vers))

	for _, ver := range vers {
		members = append(members, toAppVersionDesc(ver))
	}

	return &describeApplicationVersionsResponse{
		Xmlns: ebXMLNS,
		DescribeApplicationVersionsResult: describeApplicationVersionsResult{
			ApplicationVersions: members,
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-describe-vers"},
	}, nil
}

type deleteApplicationVersionResponse struct {
	XMLName          xml.Name         `xml:"DeleteApplicationVersionResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
}

func (h *Handler) handleDeleteApplicationVersion(ctx context.Context, vals url.Values) (any, error) {
	appName := vals.Get("ApplicationName")
	versionLabel := vals.Get("VersionLabel")

	if appName == "" {
		return nil, fmt.Errorf("%w: ApplicationName is required", ErrInvalidParameter)
	}

	if versionLabel == "" {
		return nil, fmt.Errorf("%w: VersionLabel is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteApplicationVersion(ctx, appName, versionLabel); err != nil {
		return nil, err
	}

	return &deleteApplicationVersionResponse{
		Xmlns:            ebXMLNS,
		ResponseMetadata: responseMetadata{RequestID: "eb-delete-ver"},
	}, nil
}

// --- Tags operations ---

type tagDescType struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type listTagsForResourceResult struct {
	ResourceArn  string        `xml:"ResourceArn"`
	ResourceTags []tagDescType `xml:"ResourceTags>member"`
}

type listTagsForResourceResponse struct {
	XMLName                   xml.Name                  `xml:"ListTagsForResourceResponse"`
	Xmlns                     string                    `xml:"xmlns,attr"`
	ResponseMetadata          responseMetadata          `xml:"ResponseMetadata"`
	ListTagsForResourceResult listTagsForResourceResult `xml:"ListTagsForResourceResult"`
}

func (h *Handler) handleListTagsForResource(ctx context.Context, vals url.Values) (any, error) {
	resourceARN := vals.Get("ResourceArn")
	if resourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrInvalidParameter)
	}

	tags, err := h.Backend.ListTagsForResource(ctx, resourceARN)
	if err != nil {
		return nil, err
	}

	keys := sortedTagKeys(tags)
	members := make([]tagDescType, 0, len(keys))

	for _, k := range keys {
		members = append(members, tagDescType{Key: k, Value: tags[k]})
	}

	return &listTagsForResourceResponse{
		Xmlns: ebXMLNS,
		ListTagsForResourceResult: listTagsForResourceResult{
			ResourceArn:  resourceARN,
			ResourceTags: members,
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-list-tags"},
	}, nil
}

type updateTagsForResourceResponse struct {
	XMLName          xml.Name         `xml:"UpdateTagsForResourceResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
}

func (h *Handler) handleUpdateTagsForResource(ctx context.Context, vals url.Values) (any, error) {
	resourceARN := vals.Get("ResourceArn")
	if resourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrInvalidParameter)
	}

	addTags := parseTagList(vals, "TagsToAdd.member")
	removeTagKeys := parseMembers(vals, "TagsToRemove.member")

	removeTags := make(map[string]string, len(removeTagKeys))

	for _, k := range removeTagKeys {
		removeTags[k] = ""
	}

	if err := h.Backend.UpdateTagsForResource(ctx, resourceARN, addTags, removeTags); err != nil {
		return nil, err
	}

	return &updateTagsForResourceResponse{
		Xmlns:            ebXMLNS,
		ResponseMetadata: responseMetadata{RequestID: "eb-update-tags"},
	}, nil
}

// --- Events ---

type eventDescType struct {
	ApplicationName string `xml:"ApplicationName,omitempty"`
	EnvironmentName string `xml:"EnvironmentName,omitempty"`
	EventDate       string `xml:"EventDate,omitempty"`
	Message         string `xml:"Message,omitempty"`
	Severity        string `xml:"Severity,omitempty"`
}

type describeEventsResult struct {
	Events []eventDescType `xml:"Events>member"`
}

type describeEventsResponse struct {
	XMLName              xml.Name             `xml:"DescribeEventsResponse"`
	ResponseMetadata     responseMetadata     `xml:"ResponseMetadata"`
	Xmlns                string               `xml:"xmlns,attr"`
	DescribeEventsResult describeEventsResult `xml:"DescribeEventsResult"`
}

// handleDescribeEvents returns stored events, filtered by ApplicationName, EnvironmentName,
// EnvironmentId, Severity, and StartTime. The Terraform provider calls DescribeEvents with
// Severity=ERROR and StartTime to poll for errors after environment creation/update.
func (h *Handler) handleDescribeEvents(ctx context.Context, vals url.Values) (any, error) {
	appName := vals.Get("ApplicationName")
	envName := vals.Get("EnvironmentName")

	// EnvironmentId filter: resolve to app/env name for backend lookup.
	if envID := vals.Get("EnvironmentId"); envID != "" {
		envs := h.Backend.DescribeEnvironments(ctx, "", nil, []string{envID})
		if len(envs) > 0 {
			appName = envs[0].ApplicationName
			envName = envs[0].EnvironmentName
		}
	}

	// Severity filter: only return events matching the requested severity.
	severityFilter := vals.Get("Severity")

	// StartTime filter: only return events with EventDate >= StartTime.
	var startTime time.Time
	if s := vals.Get("StartTime"); s != "" {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			startTime = t
		}
	}

	records := h.Backend.DescribeEvents(ctx, appName, envName)
	members := make([]eventDescType, 0, len(records))

	for _, r := range records {
		if severityFilter != "" && r.Severity != severityFilter {
			continue
		}

		if !startTime.IsZero() {
			if t, err := time.Parse(time.RFC3339, r.EventDate); err == nil && t.Before(startTime) {
				continue
			}
		}

		members = append(members, eventDescType{
			ApplicationName: r.ApplicationName,
			EnvironmentName: r.EnvironmentName,
			EventDate:       r.EventDate,
			Message:         r.Message,
			Severity:        r.Severity,
		})
	}

	return &describeEventsResponse{
		Xmlns:                ebXMLNS,
		DescribeEventsResult: describeEventsResult{Events: members},
		ResponseMetadata:     responseMetadata{RequestID: "eb-describe-events"},
	}, nil
}

// --- Environment Resources ---

type environmentResourceDescType struct {
	EnvironmentName      string   `xml:"EnvironmentName"`
	AutoScalingGroups    []string `xml:"AutoScalingGroups>member>Name"`
	Instances            []string `xml:"Instances>member>Id"`
	LaunchConfigurations []string `xml:"LaunchConfigurations>member>Name"`
	LaunchTemplates      []string `xml:"LaunchTemplates>member>Id"`
	LoadBalancers        []string `xml:"LoadBalancers>member>Name"`
	Queues               []string `xml:"Queues>member>URL"`
	Triggers             []string `xml:"Triggers>member>Name"`
}

type describeEnvironmentResourcesResult struct {
	EnvironmentResources environmentResourceDescType `xml:"EnvironmentResources"`
}

type describeEnvironmentResourcesResponse struct {
	XMLName                            xml.Name                           `xml:"DescribeEnvironmentResourcesResponse"`
	ResponseMetadata                   responseMetadata                   `xml:"ResponseMetadata"`
	Xmlns                              string                             `xml:"xmlns,attr"`
	DescribeEnvironmentResourcesResult describeEnvironmentResourcesResult `xml:"DescribeEnvironmentResourcesResult"`
}

func (h *Handler) handleDescribeEnvironmentResources(ctx context.Context, vals url.Values) (any, error) {
	envName := vals.Get("EnvironmentName")
	envID := vals.Get("EnvironmentId")
	if envName == "" && envID == "" {
		return nil, fmt.Errorf("%w: EnvironmentName or EnvironmentId is required", ErrInvalidParameter)
	}

	envs := h.Backend.DescribeEnvironments(ctx, "", []string{envName}, []string{envID})
	if envName == "" {
		envs = h.Backend.DescribeEnvironments(ctx, "", nil, []string{envID})
	} else if envID == "" {
		envs = h.Backend.DescribeEnvironments(ctx, "", []string{envName}, nil)
	}
	if len(envs) == 0 {
		return nil, fmt.Errorf("%w: environment not found", ErrNotFound)
	}
	env := envs[0]
	resources := environmentResourceDescType{
		EnvironmentName:      env.EnvironmentName,
		AutoScalingGroups:    []string{env.EnvironmentName + "-asg"},
		Instances:            []string{"i-" + strings.TrimPrefix(env.EnvironmentID, "e-")},
		LaunchConfigurations: []string{env.EnvironmentName + "-lc"},
	}
	if env.TierName == "Worker" {
		resources.Queues = []string{"https://sqs." + env.Region + ".amazonaws.com/" + env.EnvironmentName}
	} else {
		resources.LoadBalancers = []string{env.EnvironmentName + "-lb"}
	}

	return &describeEnvironmentResourcesResponse{
		Xmlns: ebXMLNS,
		DescribeEnvironmentResourcesResult: describeEnvironmentResourcesResult{
			EnvironmentResources: resources,
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-describe-env-resources"},
	}, nil
}

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

// --- Error handling ---

type ebError struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
	Type    string `xml:"Type"`
}

type ebErrorResponse struct {
	XMLName   xml.Name `xml:"ErrorResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	Error     ebError  `xml:"Error"`
	RequestID string   `xml:"RequestId"`
}

func (h *Handler) handleOpError(c *echo.Context, opErr error) error {
	type errorMapping struct {
		sentinel error
		code     string
	}

	mappings := []errorMapping{
		{ErrNotFound, errInvalidParameterValue},
		{ErrAlreadyExists, errInvalidParameterValue},
		{ErrInvalidParameter, errInvalidParameterValue},
		{ErrValidation, "ValidationException"},
		{ErrUnknownAction, "UnknownOperationException"},
	}

	code := "InternalFailure"

	for _, m := range mappings {
		if errors.Is(opErr, m.sentinel) {
			code = m.code

			break
		}
	}

	statusCode := http.StatusBadRequest
	if code == "InternalFailure" {
		statusCode = http.StatusInternalServerError
	}

	return h.writeError(c, statusCode, code, opErr.Error())
}

func (h *Handler) writeError(c *echo.Context, statusCode int, code, message string) error {
	errResp := &ebErrorResponse{
		Xmlns:     ebXMLNS,
		Error:     ebError{Code: code, Message: message, Type: "Sender"},
		RequestID: "eb-error",
	}

	xmlBytes, err := marshalXML(errResp)
	if err != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	return c.Blob(statusCode, "text/xml", xmlBytes)
}

func marshalXML(v any) ([]byte, error) {
	raw, err := xml.Marshal(v)
	if err != nil {
		return nil, err
	}

	return append([]byte(xml.Header), raw...), nil
}

// responseMetadata is included in every XML response.
type responseMetadata struct {
	RequestID string `xml:"RequestId"`
}

// parseMembers extracts indexed form values with the given prefix (e.g. "ApplicationNames.member").
func parseMembers(vals url.Values, prefix string) []string {
	result := make([]string, 0)

	for i := 1; ; i++ {
		key := fmt.Sprintf("%s.%d", prefix, i)
		v := vals.Get(key)

		if v == "" {
			break
		}

		result = append(result, v)
	}

	return result
}

// parseTagList parses indexed tag key/value pairs from form values.
// e.g. Tags.member.1.Key, Tags.member.1.Value, ...
func parseTagList(vals url.Values, prefix string) map[string]string {
	tags := make(map[string]string)

	for i := 1; ; i++ {
		keyField := fmt.Sprintf("%s.%d.Key", prefix, i)
		valField := fmt.Sprintf("%s.%d.Value", prefix, i)

		k := vals.Get(keyField)
		if k == "" {
			break
		}

		tags[k] = vals.Get(valField)
	}

	return tags
}

// parseOptionSetting parses a specific option setting value from indexed form values.
// AWS EB uses OptionSettings.member.N.Namespace / OptionName / Value format.
func parseOptionSetting(vals url.Values, namespace, optionName string) string {
	for i := 1; ; i++ {
		nsKey := fmt.Sprintf("OptionSettings.member.%d.Namespace", i)
		ns := vals.Get(nsKey)

		if ns == "" {
			break
		}

		if ns == namespace {
			onKey := fmt.Sprintf("OptionSettings.member.%d.OptionName", i)
			if vals.Get(onKey) == optionName {
				return vals.Get(fmt.Sprintf("OptionSettings.member.%d.Value", i))
			}
		}
	}

	return ""
}

func parseOptionSettings(vals url.Values, prefix string) []OptionSetting {
	settings := make([]OptionSetting, 0)
	for i := 1; ; i++ {
		namespace := vals.Get(fmt.Sprintf("%s.%d.Namespace", prefix, i))
		optionName := vals.Get(fmt.Sprintf("%s.%d.OptionName", prefix, i))
		if namespace == "" && optionName == "" {
			break
		}
		settings = append(settings, OptionSetting{
			Namespace:    namespace,
			OptionName:   optionName,
			ResourceName: vals.Get(fmt.Sprintf("%s.%d.ResourceName", prefix, i)),
			Value:        vals.Get(fmt.Sprintf("%s.%d.Value", prefix, i)),
		})
	}

	return settings
}

func parseSourceBuildInformation(vals url.Values) *SourceBuildInformation {
	sourceType := vals.Get("SourceBuildInformation.SourceType")
	sourceRepository := vals.Get("SourceBuildInformation.SourceRepository")
	sourceLocation := vals.Get("SourceBuildInformation.SourceLocation")
	if sourceType == "" && sourceRepository == "" && sourceLocation == "" {
		return nil
	}

	return &SourceBuildInformation{
		SourceType:       sourceType,
		SourceRepository: sourceRepository,
		SourceLocation:   sourceLocation,
	}
}

// Reset clears all backend state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// --- Restart / Rebuild stubs ---

// restartAppServerResponse is the XML response for RestartAppServer.
type restartAppServerResponse struct {
	XMLName          xml.Name         `xml:"RestartAppServerResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
}

// handleRestartAppServer signals a restart of the application servers for an environment.
// Real AWS triggers an in-place rolling restart; the stub is a no-op that returns 200.
func (h *Handler) handleRestartAppServer(_ context.Context, _ url.Values) (any, error) {
	return &restartAppServerResponse{
		Xmlns:            ebXMLNS,
		ResponseMetadata: responseMetadata{RequestID: "eb-restart-app-server"},
	}, nil
}

// rebuildEnvironmentResponse is the XML response for RebuildEnvironment.
type rebuildEnvironmentResponse struct {
	XMLName          xml.Name         `xml:"RebuildEnvironmentResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
}

// handleRebuildEnvironment triggers a full environment rebuild.
// Real AWS terminates and relaunches the environment; the stub is a no-op that returns 200.
func (h *Handler) handleRebuildEnvironment(_ context.Context, _ url.Values) (any, error) {
	return &rebuildEnvironmentResponse{
		Xmlns:            ebXMLNS,
		ResponseMetadata: responseMetadata{RequestID: "eb-rebuild-environment"},
	}, nil
}

// --- New operations ---

// abortEnvironmentUpdateResponse is the XML response for AbortEnvironmentUpdate.
type abortEnvironmentUpdateResponse struct {
	XMLName          xml.Name         `xml:"AbortEnvironmentUpdateResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
}

// handleAbortEnvironmentUpdate aborts an in-progress environment configuration update.
func (h *Handler) handleAbortEnvironmentUpdate(_ context.Context, _ url.Values) (any, error) {
	return &abortEnvironmentUpdateResponse{
		Xmlns:            ebXMLNS,
		ResponseMetadata: responseMetadata{RequestID: "eb-abort-env-update"},
	}, nil
}

// applyEnvironmentManagedActionResponse is the XML response for ApplyEnvironmentManagedAction.
type applyEnvironmentManagedActionResult struct {
	ActionID          string `xml:"ActionId"`
	ActionDescription string `xml:"ActionDescription"`
	ActionType        string `xml:"ActionType"`
	Status            string `xml:"Status"`
}

type applyEnvironmentManagedActionResponse struct {
	XMLName                             xml.Name                            `xml:"ApplyEnvironmentManagedActionResponse"`
	Xmlns                               string                              `xml:"xmlns,attr"`
	ApplyEnvironmentManagedActionResult applyEnvironmentManagedActionResult `xml:"ApplyEnvironmentManagedActionResult"`
	ResponseMetadata                    responseMetadata                    `xml:"ResponseMetadata"`
}

// handleApplyEnvironmentManagedAction applies a scheduled managed action immediately.
func (h *Handler) handleApplyEnvironmentManagedAction(ctx context.Context, vals url.Values) (any, error) {
	actionID := vals.Get("ActionId")
	if actionID == "" {
		return nil, fmt.Errorf("%w: ActionId is required", ErrInvalidParameter)
	}

	_ = h.Backend.ApplyEnvironmentManagedAction(ctx, vals.Get("EnvironmentName"), actionID)

	return &applyEnvironmentManagedActionResponse{
		Xmlns: ebXMLNS,
		ApplyEnvironmentManagedActionResult: applyEnvironmentManagedActionResult{
			ActionID:          actionID,
			ActionDescription: "Managed action applied",
			ActionType:        "InstanceRefresh",
			Status:            "Scheduled",
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-apply-managed-action"},
	}, nil
}

// associateEnvironmentOperationsRoleResponse is the XML response for AssociateEnvironmentOperationsRole.
type associateEnvironmentOperationsRoleResponse struct {
	XMLName          xml.Name         `xml:"AssociateEnvironmentOperationsRoleResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
}

// handleAssociateEnvironmentOperationsRole associates an operations role with an environment.
func (h *Handler) handleAssociateEnvironmentOperationsRole(ctx context.Context, vals url.Values) (any, error) {
	envName := vals.Get("EnvironmentName")
	if envName == "" {
		return nil, fmt.Errorf("%w: EnvironmentName is required", ErrInvalidParameter)
	}

	operationsRole := vals.Get("OperationsRole")
	if operationsRole == "" {
		return nil, fmt.Errorf("%w: OperationsRole is required", ErrInvalidParameter)
	}

	if err := h.Backend.AssociateEnvironmentOperationsRole(ctx, envName, operationsRole); err != nil {
		return nil, err
	}

	return &associateEnvironmentOperationsRoleResponse{
		Xmlns:            ebXMLNS,
		ResponseMetadata: responseMetadata{RequestID: "eb-assoc-ops-role"},
	}, nil
}

// checkDNSAvailabilityResult is the result body for CheckDNSAvailability.
type checkDNSAvailabilityResult struct {
	FullyQualifiedCNAME string `xml:"FullyQualifiedCNAME"`
	Available           bool   `xml:"Available"`
}

// checkDNSAvailabilityResponse is the XML response for CheckDNSAvailability.
type checkDNSAvailabilityResponse struct {
	XMLName                    xml.Name                   `xml:"CheckDNSAvailabilityResponse"`
	Xmlns                      string                     `xml:"xmlns,attr"`
	ResponseMetadata           responseMetadata           `xml:"ResponseMetadata"`
	CheckDNSAvailabilityResult checkDNSAvailabilityResult `xml:"CheckDNSAvailabilityResult"`
}

// handleCheckDNSAvailability checks whether a CNAME prefix is available.
func (h *Handler) handleCheckDNSAvailability(ctx context.Context, vals url.Values) (any, error) {
	cnamePrefix := vals.Get("CNAMEPrefix")
	if cnamePrefix == "" {
		return nil, fmt.Errorf("%w: CNAMEPrefix is required", ErrInvalidParameter)
	}

	available, fqcname := h.Backend.CheckDNSAvailability(ctx, cnamePrefix)

	return &checkDNSAvailabilityResponse{
		Xmlns: ebXMLNS,
		CheckDNSAvailabilityResult: checkDNSAvailabilityResult{
			Available:           available,
			FullyQualifiedCNAME: fqcname,
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-check-dns"},
	}, nil
}

// composeEnvironmentsResult is the result body for ComposeEnvironments.
type composeEnvironmentsResult struct {
	Environments []environmentDescType `xml:"Environments>member"`
}

// composeEnvironmentsResponse is the XML response for ComposeEnvironments.
type composeEnvironmentsResponse struct {
	XMLName                   xml.Name                  `xml:"ComposeEnvironmentsResponse"`
	Xmlns                     string                    `xml:"xmlns,attr"`
	ResponseMetadata          responseMetadata          `xml:"ResponseMetadata"`
	ComposeEnvironmentsResult composeEnvironmentsResult `xml:"ComposeEnvironmentsResult"`
}

// handleComposeEnvironments composes a group of environments for an application.
func (h *Handler) handleComposeEnvironments(ctx context.Context, vals url.Values) (any, error) {
	appName := vals.Get("ApplicationName")
	if appName == "" {
		return nil, fmt.Errorf("%w: ApplicationName is required", ErrInvalidParameter)
	}

	envs := h.Backend.ComposeEnvironments(ctx, appName)

	members := make([]environmentDescType, 0, len(envs))

	for _, env := range envs {
		members = append(members, toEnvironmentDesc(env))
	}

	return &composeEnvironmentsResponse{
		Xmlns:                     ebXMLNS,
		ComposeEnvironmentsResult: composeEnvironmentsResult{Environments: members},
		ResponseMetadata:          responseMetadata{RequestID: "eb-compose-envs"},
	}, nil
}

// cloneEnvironmentResponse is the XML response for CloneEnvironment (improvement #9).
type cloneEnvironmentResponse struct {
	XMLName                xml.Name            `xml:"CloneEnvironmentResponse"`
	Xmlns                  string              `xml:"xmlns,attr"`
	CloneEnvironmentResult environmentDescType `xml:"CloneEnvironmentResult"`
	ResponseMetadata       responseMetadata    `xml:"ResponseMetadata"`
}

// handleCloneEnvironment clones an existing environment into a new environment.
func (h *Handler) handleCloneEnvironment(ctx context.Context, vals url.Values) (any, error) {
	srcEnvName := vals.Get("SourceEnvironmentName")
	if srcEnvName == "" {
		return nil, fmt.Errorf("%w: SourceEnvironmentName is required", ErrInvalidParameter)
	}

	newEnvName := vals.Get("EnvironmentName")
	if newEnvName == "" {
		return nil, fmt.Errorf("%w: EnvironmentName is required", ErrInvalidParameter)
	}

	appName := vals.Get("ApplicationName")

	// Resolve app name from the source environment if not provided.
	if appName == "" {
		envs := h.Backend.DescribeEnvironments(ctx, "", []string{srcEnvName}, nil)
		if len(envs) == 1 {
			appName = envs[0].ApplicationName
		} else {
			return nil, fmt.Errorf(
				"%w: source environment %s not found or ambiguous; specify ApplicationName",
				ErrNotFound,
				srcEnvName,
			)
		}
	}

	env, err := h.Backend.CloneEnvironment(ctx, appName, srcEnvName, newEnvName)
	if err != nil {
		return nil, err
	}

	return &cloneEnvironmentResponse{
		Xmlns:                  ebXMLNS,
		CloneEnvironmentResult: toEnvironmentDesc(env),
		ResponseMetadata:       responseMetadata{RequestID: "eb-clone-env"},
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

// platformVersionDescType is used in XML responses for platform versions.
type platformVersionDescType struct {
	PlatformArn     string `xml:"PlatformArn"`
	PlatformName    string `xml:"PlatformName"`
	PlatformVersion string `xml:"PlatformVersion"`
	PlatformStatus  string `xml:"PlatformStatus"`
}

func toPlatformVersionDesc(pv *PlatformVersion) platformVersionDescType {
	return platformVersionDescType{
		PlatformArn:     pv.PlatformArn,
		PlatformName:    pv.PlatformName,
		PlatformVersion: pv.PlatformVersion,
		PlatformStatus:  pv.PlatformStatus,
	}
}

// createPlatformVersionResult is the result body for CreatePlatformVersion.
type createPlatformVersionResult struct {
	PlatformSummary platformVersionDescType `xml:"PlatformSummary"`
}

// createPlatformVersionResponse is the XML response for CreatePlatformVersion.
type createPlatformVersionResponse struct {
	XMLName                     xml.Name                    `xml:"CreatePlatformVersionResponse"`
	Xmlns                       string                      `xml:"xmlns,attr"`
	CreatePlatformVersionResult createPlatformVersionResult `xml:"CreatePlatformVersionResult"`
	ResponseMetadata            responseMetadata            `xml:"ResponseMetadata"`
}

// handleCreatePlatformVersion creates a new custom platform version.
func (h *Handler) handleCreatePlatformVersion(ctx context.Context, vals url.Values) (any, error) {
	platformName := vals.Get("PlatformName")
	if platformName == "" {
		return nil, fmt.Errorf("%w: PlatformName is required", ErrInvalidParameter)
	}

	platformVersion := vals.Get("PlatformVersion")
	if platformVersion == "" {
		return nil, fmt.Errorf("%w: PlatformVersion is required", ErrInvalidParameter)
	}

	tags := parseTagList(vals, "Tags.member")

	pv, err := h.Backend.CreatePlatformVersion(ctx, platformName, platformVersion, tags)
	if err != nil {
		return nil, err
	}

	return &createPlatformVersionResponse{
		Xmlns: ebXMLNS,
		CreatePlatformVersionResult: createPlatformVersionResult{
			PlatformSummary: toPlatformVersionDesc(pv),
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-create-platform-ver"},
	}, nil
}

// createStorageLocationResult is the result body for CreateStorageLocation.
type createStorageLocationResult struct {
	S3Bucket string `xml:"S3Bucket"`
}

// createStorageLocationResponse is the XML response for CreateStorageLocation.
type createStorageLocationResponse struct {
	XMLName                     xml.Name                    `xml:"CreateStorageLocationResponse"`
	Xmlns                       string                      `xml:"xmlns,attr"`
	CreateStorageLocationResult createStorageLocationResult `xml:"CreateStorageLocationResult"`
	ResponseMetadata            responseMetadata            `xml:"ResponseMetadata"`
}

// handleCreateStorageLocation creates (or returns) the S3 storage bucket.
func (h *Handler) handleCreateStorageLocation(ctx context.Context, _ url.Values) (any, error) {
	bucket := h.Backend.CreateStorageLocation(ctx)

	return &createStorageLocationResponse{
		Xmlns:                       ebXMLNS,
		CreateStorageLocationResult: createStorageLocationResult{S3Bucket: bucket},
		ResponseMetadata:            responseMetadata{RequestID: "eb-create-storage"},
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

// --- 19 newly implemented operations ---

// deletePlatformVersionResponse is the XML response for DeletePlatformVersion.
type deletePlatformVersionResult struct {
	PlatformSummary platformVersionDescType `xml:"PlatformSummary"`
}

type deletePlatformVersionResponse struct {
	XMLName                     xml.Name                    `xml:"DeletePlatformVersionResponse"`
	Xmlns                       string                      `xml:"xmlns,attr"`
	DeletePlatformVersionResult deletePlatformVersionResult `xml:"DeletePlatformVersionResult"`
	ResponseMetadata            responseMetadata            `xml:"ResponseMetadata"`
}

func (h *Handler) handleDeletePlatformVersion(ctx context.Context, vals url.Values) (any, error) {
	platformARN := vals.Get("PlatformArn")
	if platformARN == "" {
		return nil, fmt.Errorf("%w: PlatformArn is required", ErrInvalidParameter)
	}

	pv, err := h.Backend.DeletePlatformVersion(ctx, platformARN)
	if err != nil {
		return nil, err
	}

	return &deletePlatformVersionResponse{
		Xmlns: ebXMLNS,
		DeletePlatformVersionResult: deletePlatformVersionResult{
			PlatformSummary: toPlatformVersionDesc(pv),
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-delete-platform-ver"},
	}, nil
}

// describeAccountAttributesResponse is the XML response for DescribeAccountAttributes.
type resourceQuota struct {
	Maximum int `xml:"Maximum"`
}

type resourceQuotas struct {
	ApplicationQuota           resourceQuota `xml:"ApplicationQuota"`
	ApplicationVersionQuota    resourceQuota `xml:"ApplicationVersionQuota"`
	ConfigurationTemplateQuota resourceQuota `xml:"ConfigurationTemplateQuota"`
	CustomPlatformQuota        resourceQuota `xml:"CustomPlatformQuota"`
	EnvironmentQuota           resourceQuota `xml:"EnvironmentQuota"`
}

type describeAccountAttributesResult struct {
	ResourceQuotas resourceQuotas `xml:"ResourceQuotas"`
}

type describeAccountAttributesResponse struct {
	XMLName                         xml.Name                        `xml:"DescribeAccountAttributesResponse"`
	Xmlns                           string                          `xml:"xmlns,attr"`
	ResponseMetadata                responseMetadata                `xml:"ResponseMetadata"`
	DescribeAccountAttributesResult describeAccountAttributesResult `xml:"DescribeAccountAttributesResult"`
}

func (h *Handler) handleDescribeAccountAttributes(_ context.Context, _ url.Values) (any, error) {
	return &describeAccountAttributesResponse{
		Xmlns: ebXMLNS,
		DescribeAccountAttributesResult: describeAccountAttributesResult{
			ResourceQuotas: resourceQuotas{
				ApplicationQuota:           resourceQuota{Maximum: quotaApplications},
				ApplicationVersionQuota:    resourceQuota{Maximum: quotaApplicationVersions},
				ConfigurationTemplateQuota: resourceQuota{Maximum: quotaConfigTemplates},
				CustomPlatformQuota:        resourceQuota{Maximum: quotaCustomPlatforms},
				EnvironmentQuota:           resourceQuota{Maximum: quotaEnvironments},
			},
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-describe-account-attrs"},
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

// describeEnvironmentHealthResponse is the XML response for DescribeEnvironmentHealth.
type describeEnvironmentHealthResult struct {
	EnvironmentName string `xml:"EnvironmentName"`
	HealthStatus    string `xml:"HealthStatus"`
	Status          string `xml:"Status"`
	Color           string `xml:"Color"`
	RefreshedAt     string `xml:"RefreshedAt"`
}

type describeEnvironmentHealthResponse struct {
	XMLName                         xml.Name                        `xml:"DescribeEnvironmentHealthResponse"`
	Xmlns                           string                          `xml:"xmlns,attr"`
	DescribeEnvironmentHealthResult describeEnvironmentHealthResult `xml:"DescribeEnvironmentHealthResult"`
	ResponseMetadata                responseMetadata                `xml:"ResponseMetadata"`
}

func (h *Handler) handleDescribeEnvironmentHealth(ctx context.Context, vals url.Values) (any, error) {
	envName := vals.Get("EnvironmentName")
	if envName == "" {
		return nil, fmt.Errorf("%w: EnvironmentName is required", ErrInvalidParameter)
	}

	health, status := h.Backend.DescribeEnvironmentHealth(ctx, envName)

	return &describeEnvironmentHealthResponse{
		Xmlns: ebXMLNS,
		DescribeEnvironmentHealthResult: describeEnvironmentHealthResult{
			EnvironmentName: envName,
			HealthStatus:    health,
			Status:          status,
			Color:           healthColorGreen,
			RefreshedAt:     healthRefreshedAt,
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-describe-env-health"},
	}, nil
}

// describeEnvironmentManagedActionHistoryResponse is the XML response for DescribeEnvironmentManagedActionHistory.
type managedActionHistoryItem struct {
	ActionID          string `xml:"ActionId"`
	ActionType        string `xml:"ActionType"`
	ActionDescription string `xml:"ActionDescription"`
	Status            string `xml:"Status"`
	FinishedTime      string `xml:"FinishedTime"`
}

type describeEnvironmentManagedActionHistoryResult struct {
	ManagedActionHistoryItems []managedActionHistoryItem `xml:"ManagedActionHistoryItems>member"`
}

type describeEnvironmentManagedActionHistoryResponse struct { //nolint:lll // AWS XML operation name causes inherently long struct declaration
	XMLName                                       xml.Name                                      `xml:"DescribeEnvironmentManagedActionHistoryResponse"` //nolint:lll // AWS XML operation name is inherently long
	Xmlns                                         string                                        `xml:"xmlns,attr"`
	ResponseMetadata                              responseMetadata                              `xml:"ResponseMetadata"`
	DescribeEnvironmentManagedActionHistoryResult describeEnvironmentManagedActionHistoryResult `xml:"DescribeEnvironmentManagedActionHistoryResult"` //nolint:lll // AWS XML operation name is inherently long
}

func (h *Handler) handleDescribeEnvironmentManagedActionHistory(ctx context.Context, vals url.Values) (any, error) {
	envName := vals.Get("EnvironmentName")

	// Return real stored history (improvement #4)
	historyItems := h.Backend.DescribeEnvironmentManagedActionHistory(ctx, envName)
	members := make([]managedActionHistoryItem, 0, len(historyItems))

	for _, item := range historyItems {
		members = append(members, managedActionHistoryItem{
			ActionID:          item.ActionID,
			ActionType:        item.ActionType,
			ActionDescription: item.ActionDescription,
			Status:            item.Status,
			FinishedTime:      item.FinishedTime,
		})
	}

	return &describeEnvironmentManagedActionHistoryResponse{
		Xmlns: ebXMLNS,
		DescribeEnvironmentManagedActionHistoryResult: describeEnvironmentManagedActionHistoryResult{
			ManagedActionHistoryItems: members,
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-describe-env-managed-history"},
	}, nil
}

// describeEnvironmentManagedActionsResponse is the XML response for DescribeEnvironmentManagedActions.
type managedAction struct {
	ActionID          string `xml:"ActionId"`
	ActionType        string `xml:"ActionType"`
	ActionDescription string `xml:"ActionDescription"`
	Status            string `xml:"Status"`
	WindowStartTime   string `xml:"WindowStartTime"`
}

type describeEnvironmentManagedActionsResult struct {
	ManagedActions []managedAction `xml:"ManagedActions>member"`
}

type describeEnvironmentManagedActionsResponse struct { //nolint:lll // AWS XML operation name causes inherently long struct declaration
	XMLName                                 xml.Name                                `xml:"DescribeEnvironmentManagedActionsResponse"` //nolint:lll // AWS XML operation name is inherently long
	Xmlns                                   string                                  `xml:"xmlns,attr"`
	ResponseMetadata                        responseMetadata                        `xml:"ResponseMetadata"`
	DescribeEnvironmentManagedActionsResult describeEnvironmentManagedActionsResult `xml:"DescribeEnvironmentManagedActionsResult"` //nolint:lll // AWS XML operation name is inherently long
}

func (h *Handler) handleDescribeEnvironmentManagedActions(_ context.Context, _ url.Values) (any, error) {
	return &describeEnvironmentManagedActionsResponse{
		Xmlns: ebXMLNS,
		DescribeEnvironmentManagedActionsResult: describeEnvironmentManagedActionsResult{
			ManagedActions: []managedAction{},
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-describe-env-managed-actions"},
	}, nil
}

// describeInstancesHealthResponse is the XML response for DescribeInstancesHealth.
type singleInstanceHealth struct {
	InstanceID   string `xml:"InstanceId"`
	HealthStatus string `xml:"HealthStatus"`
	Color        string `xml:"Color"`
}

type describeInstancesHealthResult struct {
	InstanceHealthList []singleInstanceHealth `xml:"InstanceHealthList>member"`
}

type describeInstancesHealthResponse struct {
	XMLName                       xml.Name                      `xml:"DescribeInstancesHealthResponse"`
	Xmlns                         string                        `xml:"xmlns,attr"`
	ResponseMetadata              responseMetadata              `xml:"ResponseMetadata"`
	DescribeInstancesHealthResult describeInstancesHealthResult `xml:"DescribeInstancesHealthResult"`
}

func (h *Handler) handleDescribeInstancesHealth(_ context.Context, _ url.Values) (any, error) {
	return &describeInstancesHealthResponse{
		Xmlns: ebXMLNS,
		DescribeInstancesHealthResult: describeInstancesHealthResult{
			InstanceHealthList: []singleInstanceHealth{},
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-describe-instances-health"},
	}, nil
}

// describePlatformVersionResponse is the XML response for DescribePlatformVersion.
type describePlatformVersionResult struct {
	PlatformDescription platformVersionDescType `xml:"PlatformDescription"`
}

type describePlatformVersionResponse struct {
	XMLName                       xml.Name                      `xml:"DescribePlatformVersionResponse"`
	Xmlns                         string                        `xml:"xmlns,attr"`
	DescribePlatformVersionResult describePlatformVersionResult `xml:"DescribePlatformVersionResult"`
	ResponseMetadata              responseMetadata              `xml:"ResponseMetadata"`
}

func (h *Handler) handleDescribePlatformVersion(ctx context.Context, vals url.Values) (any, error) {
	platformARN := vals.Get("PlatformArn")
	if platformARN == "" {
		return nil, fmt.Errorf("%w: PlatformArn is required", ErrInvalidParameter)
	}

	pv, err := h.Backend.DescribePlatformVersion(ctx, platformARN)
	if err != nil {
		return nil, err
	}

	return &describePlatformVersionResponse{
		Xmlns: ebXMLNS,
		DescribePlatformVersionResult: describePlatformVersionResult{
			PlatformDescription: toPlatformVersionDesc(pv),
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-describe-platform-ver"},
	}, nil
}

// disassociateEnvironmentOperationsRoleResponse is the XML response for DisassociateEnvironmentOperationsRole.
type disassociateEnvironmentOperationsRoleResponse struct {
	XMLName          xml.Name         `xml:"DisassociateEnvironmentOperationsRoleResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
}

func (h *Handler) handleDisassociateEnvironmentOperationsRole(ctx context.Context, vals url.Values) (any, error) {
	envName := vals.Get("EnvironmentName")
	if envName == "" {
		return nil, fmt.Errorf("%w: EnvironmentName is required", ErrInvalidParameter)
	}

	if err := h.Backend.DisassociateEnvironmentOperationsRole(ctx, envName); err != nil {
		return nil, err
	}

	return &disassociateEnvironmentOperationsRoleResponse{
		Xmlns:            ebXMLNS,
		ResponseMetadata: responseMetadata{RequestID: "eb-disassoc-ops-role"},
	}, nil
}

// listAvailableSolutionStacksResponse is the XML response for ListAvailableSolutionStacks.
type listAvailableSolutionStacksResult struct {
	SolutionStacks []string `xml:"SolutionStacks>member"`
}

type listAvailableSolutionStacksResponse struct {
	XMLName                           xml.Name                          `xml:"ListAvailableSolutionStacksResponse"`
	Xmlns                             string                            `xml:"xmlns,attr"`
	ResponseMetadata                  responseMetadata                  `xml:"ResponseMetadata"`
	ListAvailableSolutionStacksResult listAvailableSolutionStacksResult `xml:"ListAvailableSolutionStacksResult"`
}

var availableSolutionStacks = []string{ //nolint:gochecknoglobals // package-level constant slice
	"64bit Amazon Linux 2023 v4.3.0 running Python 3.11",
	"64bit Amazon Linux 2023 v4.3.0 running Node.js 20",
	"64bit Amazon Linux 2023 v4.3.0 running Go 1",
	"64bit Amazon Linux 2023 v6.3.0 running PHP 8.3",
	"64bit Amazon Linux 2023 v4.3.0 running Corretto 21",
	"64bit Amazon Linux 2023 v4.3.0 running Corretto 17",
	"64bit Amazon Linux 2023 v4.3.0 running Ruby 3.3",
	"64bit Amazon Linux 2023 v4.3.0 running Docker",
}

func (h *Handler) handleListAvailableSolutionStacks(_ context.Context, _ url.Values) (any, error) {
	return &listAvailableSolutionStacksResponse{
		Xmlns: ebXMLNS,
		ListAvailableSolutionStacksResult: listAvailableSolutionStacksResult{
			SolutionStacks: availableSolutionStacks,
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-list-solution-stacks"},
	}, nil
}

// listPlatformBranchesResponse is the XML response for ListPlatformBranches.
type platformBranchSummary struct {
	PlatformName   string `xml:"PlatformName"`
	BranchName     string `xml:"BranchName"`
	LifecycleState string `xml:"LifecycleState"`
}

type listPlatformBranchesResult struct {
	PlatformBranchSummaryList []platformBranchSummary `xml:"PlatformBranchSummaryList>member"`
}

type listPlatformBranchesResponse struct {
	XMLName                    xml.Name                   `xml:"ListPlatformBranchesResponse"`
	Xmlns                      string                     `xml:"xmlns,attr"`
	ResponseMetadata           responseMetadata           `xml:"ResponseMetadata"`
	ListPlatformBranchesResult listPlatformBranchesResult `xml:"ListPlatformBranchesResult"`
}

// allPlatformBranches is the full list of platform branches returned by ListPlatformBranches.
//
//nolint:gochecknoglobals // package-level constant slice
var allPlatformBranches = []platformBranchSummary{
	{
		PlatformName:   "Python",
		BranchName:     "Python 3.11 running on 64bit Amazon Linux 2023",
		LifecycleState: platformLifecycleSupported,
	},
	{
		PlatformName:   "Node.js",
		BranchName:     "Node.js 20 running on 64bit Amazon Linux 2023",
		LifecycleState: platformLifecycleSupported,
	},
	{
		PlatformName:   "Go",
		BranchName:     "Go 1 running on 64bit Amazon Linux 2023",
		LifecycleState: platformLifecycleSupported,
	},
	{
		PlatformName:   "PHP",
		BranchName:     "PHP 8.3 running on 64bit Amazon Linux 2023",
		LifecycleState: platformLifecycleSupported,
	},
	{
		PlatformName:   "Docker",
		BranchName:     "Docker running on 64bit Amazon Linux 2023",
		LifecycleState: platformLifecycleSupported,
	},
	{
		PlatformName:   "Ruby",
		BranchName:     "Ruby 3.3 running on 64bit Amazon Linux 2023",
		LifecycleState: platformLifecycleSupported,
	},
	{
		PlatformName:   "Java",
		BranchName:     "Corretto 21 running on 64bit Amazon Linux 2023",
		LifecycleState: platformLifecycleSupported,
	},
}

// handleListPlatformBranches lists platform branches with optional filtering (improvement #3).
func (h *Handler) handleListPlatformBranches(_ context.Context, vals url.Values) (any, error) {
	// Collect filters: Filters.member.N.Attribute / Value
	type filterEntry struct{ attribute, value string }

	filters := make([]filterEntry, 0)

	for i := 1; ; i++ {
		attr := vals.Get(fmt.Sprintf("Filters.member.%d.Attribute", i))
		if attr == "" {
			break
		}

		value := vals.Get(fmt.Sprintf("Filters.member.%d.Values.member.1", i))
		filters = append(filters, filterEntry{attribute: attr, value: value})
	}

	branches := make([]platformBranchSummary, 0, len(allPlatformBranches))

	for _, b := range allPlatformBranches {
		match := true

		for _, f := range filters {
			switch f.attribute {
			case "PlatformName":
				if !strings.EqualFold(b.PlatformName, f.value) {
					match = false
				}
			case "LifecycleState":
				if !strings.EqualFold(b.LifecycleState, f.value) {
					match = false
				}
			}
		}

		if match {
			branches = append(branches, b)
		}
	}

	return &listPlatformBranchesResponse{
		Xmlns: ebXMLNS,
		ListPlatformBranchesResult: listPlatformBranchesResult{
			PlatformBranchSummaryList: branches,
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-list-platform-branches"},
	}, nil
}

// listPlatformVersionsResponse is the XML response for ListPlatformVersions.
type platformSummary struct {
	PlatformArn    string `xml:"PlatformArn"`
	PlatformStatus string `xml:"PlatformStatus"`
}

type listPlatformVersionsResult struct {
	PlatformSummaryList []platformSummary `xml:"PlatformSummaryList>member"`
}

type listPlatformVersionsResponse struct {
	XMLName                    xml.Name                   `xml:"ListPlatformVersionsResponse"`
	Xmlns                      string                     `xml:"xmlns,attr"`
	ResponseMetadata           responseMetadata           `xml:"ResponseMetadata"`
	ListPlatformVersionsResult listPlatformVersionsResult `xml:"ListPlatformVersionsResult"`
}

func (h *Handler) handleListPlatformVersions(ctx context.Context, _ url.Values) (any, error) {
	pvs := h.Backend.ListPlatformVersions(ctx)

	summaries := make([]platformSummary, 0, len(pvs))
	for _, pv := range pvs {
		summaries = append(summaries, platformSummary{
			PlatformArn:    pv.PlatformArn,
			PlatformStatus: pv.PlatformStatus,
		})
	}

	return &listPlatformVersionsResponse{
		Xmlns: ebXMLNS,
		ListPlatformVersionsResult: listPlatformVersionsResult{
			PlatformSummaryList: summaries,
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-list-platform-versions"},
	}, nil
}

// requestEnvironmentInfoResponse is the XML response for RequestEnvironmentInfo.
type requestEnvironmentInfoResponse struct {
	XMLName          xml.Name         `xml:"RequestEnvironmentInfoResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
}

func (h *Handler) handleRequestEnvironmentInfo(_ context.Context, _ url.Values) (any, error) {
	return &requestEnvironmentInfoResponse{
		Xmlns:            ebXMLNS,
		ResponseMetadata: responseMetadata{RequestID: "eb-request-env-info"},
	}, nil
}

// retrieveEnvironmentInfoResponse is the XML response for RetrieveEnvironmentInfo.
type environmentInfoDescription struct {
	InfoType        string `xml:"InfoType"`
	Ec2InstanceID   string `xml:"Ec2InstanceId"`
	SampleTimestamp string `xml:"SampleTimestamp"`
	Message         string `xml:"Message"`
}

type retrieveEnvironmentInfoResult struct {
	EnvironmentInfo []environmentInfoDescription `xml:"EnvironmentInfo>member"`
}

type retrieveEnvironmentInfoResponse struct {
	XMLName                       xml.Name                      `xml:"RetrieveEnvironmentInfoResponse"`
	Xmlns                         string                        `xml:"xmlns,attr"`
	ResponseMetadata              responseMetadata              `xml:"ResponseMetadata"`
	RetrieveEnvironmentInfoResult retrieveEnvironmentInfoResult `xml:"RetrieveEnvironmentInfoResult"`
}

func (h *Handler) handleRetrieveEnvironmentInfo(_ context.Context, _ url.Values) (any, error) {
	return &retrieveEnvironmentInfoResponse{
		Xmlns: ebXMLNS,
		RetrieveEnvironmentInfoResult: retrieveEnvironmentInfoResult{
			EnvironmentInfo: []environmentInfoDescription{},
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-retrieve-env-info"},
	}, nil
}

// swapEnvironmentCNAMEsResponse is the XML response for SwapEnvironmentCNAMEs.
type swapEnvironmentCNAMEsResponse struct {
	XMLName          xml.Name         `xml:"SwapEnvironmentCNAMEsResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
}

func (h *Handler) handleSwapEnvironmentCNAMEs(ctx context.Context, vals url.Values) (any, error) {
	sourceEnv := vals.Get("SourceEnvironmentName")
	destEnv := vals.Get("DestinationEnvironmentName")

	if sourceEnv == "" && vals.Get("SourceEnvironmentId") == "" {
		return nil, fmt.Errorf(
			"%w: SourceEnvironmentName or SourceEnvironmentId is required",
			ErrInvalidParameter,
		)
	}

	if destEnv == "" && vals.Get("DestinationEnvironmentId") == "" {
		return nil, fmt.Errorf(
			"%w: DestinationEnvironmentName or DestinationEnvironmentId is required",
			ErrInvalidParameter,
		)
	}

	// Resolve env names from IDs if names not provided
	if sourceEnv == "" {
		srcID := vals.Get("SourceEnvironmentId")
		envs := h.Backend.DescribeEnvironments(ctx, "", nil, []string{srcID})

		if len(envs) > 0 {
			sourceEnv = envs[0].EnvironmentName
		}
	}

	if destEnv == "" {
		dstID := vals.Get("DestinationEnvironmentId")
		envs := h.Backend.DescribeEnvironments(ctx, "", nil, []string{dstID})

		if len(envs) > 0 {
			destEnv = envs[0].EnvironmentName
		}
	}

	// Actually swap CNAMEs (improvement #10)
	if err := h.Backend.SwapEnvironmentCNAMEs(ctx, sourceEnv, destEnv); err != nil {
		return nil, err
	}

	return &swapEnvironmentCNAMEsResponse{
		Xmlns:            ebXMLNS,
		ResponseMetadata: responseMetadata{RequestID: "eb-swap-cnames"},
	}, nil
}

// updateApplicationResourceLifecycleResponse is the XML response for UpdateApplicationResourceLifecycle.
type applicationResourceLifecycleConfig struct {
	ServiceRole string `xml:"ServiceRole,omitempty"`
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

	serviceRole := vals.Get("ResourceLifecycleConfig.ServiceRole")

	// Store lifecycle service role in the application (improvement #7)
	if _, err := h.Backend.UpdateApplicationResourceLifecycle(ctx, appName, serviceRole); err != nil {
		return nil, err
	}

	return &updateApplicationResourceLifecycleResponse{
		Xmlns: ebXMLNS,
		UpdateApplicationResourceLifecycleResult: updateApplicationResourceLifecycleResult{
			ApplicationName: appName,
			ResourceLifecycleConfig: applicationResourceLifecycleConfig{
				ServiceRole: serviceRole,
			},
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-update-app-lifecycle"},
	}, nil
}

// updateApplicationVersionResponse is the XML response for UpdateApplicationVersion.
type updateApplicationVersionResponse struct {
	XMLName                        xml.Name                       `xml:"UpdateApplicationVersionResponse"`
	Xmlns                          string                         `xml:"xmlns,attr"`
	UpdateApplicationVersionResult createApplicationVersionResult `xml:"UpdateApplicationVersionResult"`
	ResponseMetadata               responseMetadata               `xml:"ResponseMetadata"`
}

func (h *Handler) handleUpdateApplicationVersion(ctx context.Context, vals url.Values) (any, error) {
	appName := vals.Get("ApplicationName")
	if appName == "" {
		return nil, fmt.Errorf("%w: ApplicationName is required", ErrInvalidParameter)
	}

	versionLabel := vals.Get("VersionLabel")
	if versionLabel == "" {
		return nil, fmt.Errorf("%w: VersionLabel is required", ErrInvalidParameter)
	}

	description := vals.Get("Description")

	ver, err := h.Backend.UpdateApplicationVersion(ctx, appName, versionLabel, description)
	if err != nil {
		return nil, err
	}

	return &updateApplicationVersionResponse{
		Xmlns: ebXMLNS,
		UpdateApplicationVersionResult: createApplicationVersionResult{
			ApplicationVersion: toAppVersionDesc(ver),
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-update-app-ver"},
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
