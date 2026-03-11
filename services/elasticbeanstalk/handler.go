package elasticbeanstalk

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	ebXMLNS = "https://elasticbeanstalk.amazonaws.com/docs/2010-12-01/"
)

// Handler is the Echo HTTP handler for Elastic Beanstalk operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new Elastic Beanstalk handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Elasticbeanstalk" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateApplication",
		"DescribeApplications",
		"UpdateApplication",
		"DeleteApplication",
		"CreateEnvironment",
		"DescribeEnvironments",
		"UpdateEnvironment",
		"TerminateEnvironment",
		"CreateApplicationVersion",
		"DescribeApplicationVersions",
		"DeleteApplicationVersion",
		"DescribeEvents",
		"DescribeEnvironmentResources",
		"DescribeConfigurationSettings",
		"ListTagsForResource",
		"UpdateTagsForResource",
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
			return h.writeError(c, http.StatusInternalServerError, "InternalFailure", "failed to read request body")
		}

		vals := r.Form
		action := vals.Get("Action")
		if action == "" {
			return h.writeError(c, http.StatusBadRequest, "MissingAction", "missing Action parameter")
		}

		log := logger.Load(r.Context())
		log.Debug("elasticbeanstalk request", "action", action)

		resp, opErr := h.dispatch(action, vals)
		if opErr != nil {
			return h.handleOpError(c, opErr)
		}

		xmlBytes, err := marshalXML(resp)
		if err != nil {
			return h.writeError(c, http.StatusInternalServerError, "InternalFailure", "internal server error")
		}

		return c.Blob(http.StatusOK, "text/xml", xmlBytes)
	}
}

// dispatch routes the Elastic Beanstalk action to the appropriate handler.
func (h *Handler) dispatch(action string, vals url.Values) (any, error) {
	type handlerFn func(url.Values) (any, error)

	handlers := map[string]handlerFn{
		"CreateApplication":             h.handleCreateApplication,
		"DescribeApplications":          h.handleDescribeApplications,
		"UpdateApplication":             h.handleUpdateApplication,
		"DeleteApplication":             h.handleDeleteApplication,
		"CreateEnvironment":             h.handleCreateEnvironment,
		"DescribeEnvironments":          h.handleDescribeEnvironments,
		"UpdateEnvironment":             h.handleUpdateEnvironment,
		"TerminateEnvironment":          h.handleTerminateEnvironment,
		"CreateApplicationVersion":      h.handleCreateApplicationVersion,
		"DescribeApplicationVersions":   h.handleDescribeApplicationVersions,
		"DeleteApplicationVersion":      h.handleDeleteApplicationVersion,
		"ListTagsForResource":           h.handleListTagsForResource,
		"UpdateTagsForResource":         h.handleUpdateTagsForResource,
		"DescribeEvents":                h.handleDescribeEvents,
		"DescribeEnvironmentResources":  h.handleDescribeEnvironmentResources,
		"DescribeConfigurationSettings": h.handleDescribeConfigurationSettings,
	}

	if fn, ok := handlers[action]; ok {
		return fn(vals)
	}

	return nil, fmt.Errorf("%w: %s", ErrUnknownAction, action)
}

// --- Application operations ---

// applicationDescType is used in XML responses.
type applicationDescType struct {
	ApplicationName string `xml:"ApplicationName"`
	ApplicationArn  string `xml:"ApplicationArn"`
	Description     string `xml:"Description,omitempty"`
}

func toApplicationDesc(app *Application) applicationDescType {
	return applicationDescType{
		ApplicationName: app.ApplicationName,
		ApplicationArn:  app.ApplicationARN,
		Description:     app.Description,
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

func (h *Handler) handleCreateApplication(vals url.Values) (any, error) {
	name := vals.Get("ApplicationName")
	if name == "" {
		return nil, fmt.Errorf("%w: ApplicationName is required", ErrInvalidParameter)
	}

	description := vals.Get("Description")

	tags := parseTagList(vals, "Tags.member")

	app, err := h.Backend.CreateApplication(name, description, tags)
	if err != nil {
		return nil, err
	}

	return &createApplicationResponse{
		Xmlns:                   ebXMLNS,
		CreateApplicationResult: createApplicationResult{Application: toApplicationDesc(app)},
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

func (h *Handler) handleDescribeApplications(vals url.Values) (any, error) {
	names := parseMembers(vals, "ApplicationNames.member")
	apps := h.Backend.DescribeApplications(names)

	members := make([]applicationDescType, 0, len(apps))

	for _, app := range apps {
		members = append(members, toApplicationDesc(app))
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

func (h *Handler) handleUpdateApplication(vals url.Values) (any, error) {
	name := vals.Get("ApplicationName")
	if name == "" {
		return nil, fmt.Errorf("%w: ApplicationName is required", ErrInvalidParameter)
	}

	description := vals.Get("Description")

	app, err := h.Backend.UpdateApplication(name, description)
	if err != nil {
		return nil, err
	}

	return &updateApplicationResponse{
		Xmlns:                   ebXMLNS,
		UpdateApplicationResult: updateApplicationResult{Application: toApplicationDesc(app)},
		ResponseMetadata:        responseMetadata{RequestID: "eb-update-app"},
	}, nil
}

type deleteApplicationResponse struct {
	XMLName          xml.Name         `xml:"DeleteApplicationResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
}

func (h *Handler) handleDeleteApplication(vals url.Values) (any, error) {
	name := vals.Get("ApplicationName")
	if name == "" {
		return nil, fmt.Errorf("%w: ApplicationName is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteApplication(name); err != nil {
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
	SolutionStackName string              `xml:"SolutionStackName"`
	Status            string              `xml:"Status"`
	Health            string              `xml:"Health"`
	Tier              environmentTierType `xml:"Tier"`
	CNAME             string              `xml:"CNAME"`
	EndpointURL       string              `xml:"EndpointURL"`
}

func toEnvironmentDesc(env *Environment, region string) environmentDescType {
	cname := env.EnvironmentName + "." + region + ".elasticbeanstalk.com"

	return environmentDescType{
		ApplicationName:   env.ApplicationName,
		EnvironmentName:   env.EnvironmentName,
		EnvironmentID:     env.EnvironmentID,
		EnvironmentArn:    env.EnvironmentARN,
		SolutionStackName: env.SolutionStackName,
		Status:            env.Status,
		Health:            env.Health,
		Tier: environmentTierType{
			Name:    env.Tier,
			Type:    "Standard",
			Version: "1.0",
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

func (h *Handler) handleCreateEnvironment(vals url.Values) (any, error) {
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

	env, err := h.Backend.CreateEnvironment(appName, envName, solutionStack, description, tags)
	if err != nil {
		return nil, err
	}

	return &createEnvironmentResponse{
		Xmlns:                   ebXMLNS,
		CreateEnvironmentResult: toEnvironmentDesc(env, h.Backend.Region()),
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

func (h *Handler) handleDescribeEnvironments(vals url.Values) (any, error) {
	appName := vals.Get("ApplicationName")
	envNames := parseMembers(vals, "EnvironmentNames.member")
	envIDs := parseMembers(vals, "EnvironmentIds.member")
	envs := h.Backend.DescribeEnvironments(appName, envNames, envIDs)

	members := make([]environmentDescType, 0, len(envs))

	for _, env := range envs {
		members = append(members, toEnvironmentDesc(env, h.Backend.Region()))
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

func (h *Handler) handleUpdateEnvironment(vals url.Values) (any, error) {
	envName := vals.Get("EnvironmentName")
	if envName == "" {
		return nil, fmt.Errorf("%w: EnvironmentName is required", ErrInvalidParameter)
	}

	appName := vals.Get("ApplicationName")
	description := vals.Get("Description")
	solutionStack := vals.Get("SolutionStackName")

	env, err := h.Backend.UpdateEnvironment(appName, envName, description, solutionStack)
	if err != nil {
		return nil, err
	}

	return &updateEnvironmentResponse{
		Xmlns:                   ebXMLNS,
		UpdateEnvironmentResult: toEnvironmentDesc(env, h.Backend.Region()),
		ResponseMetadata:        responseMetadata{RequestID: "eb-update-env"},
	}, nil
}

type terminateEnvironmentResponse struct {
	XMLName                    xml.Name            `xml:"TerminateEnvironmentResponse"`
	Xmlns                      string              `xml:"xmlns,attr"`
	TerminateEnvironmentResult environmentDescType `xml:"TerminateEnvironmentResult"`
	ResponseMetadata           responseMetadata    `xml:"ResponseMetadata"`
}

func (h *Handler) handleTerminateEnvironment(vals url.Values) (any, error) {
	envName := vals.Get("EnvironmentName")
	if envName == "" {
		return nil, fmt.Errorf("%w: EnvironmentName is required", ErrInvalidParameter)
	}

	appName := vals.Get("ApplicationName")

	// If no app name provided, search across all environments for this name.
	if appName == "" {
		envs := h.Backend.DescribeEnvironments("", []string{envName}, nil)
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

	env, err := h.Backend.TerminateEnvironment(appName, envName)
	if err != nil {
		return nil, err
	}

	return &terminateEnvironmentResponse{
		Xmlns:                      ebXMLNS,
		TerminateEnvironmentResult: toEnvironmentDesc(env, h.Backend.Region()),
		ResponseMetadata:           responseMetadata{RequestID: "eb-terminate-env"},
	}, nil
}

// --- Application Version operations ---

type appVersionDescType struct {
	ApplicationName       string `xml:"ApplicationName"`
	VersionLabel          string `xml:"VersionLabel"`
	ApplicationVersionArn string `xml:"ApplicationVersionArn"`
	Description           string `xml:"Description,omitempty"`
	Status                string `xml:"Status"`
}

func toAppVersionDesc(ver *ApplicationVersion) appVersionDescType {
	return appVersionDescType{
		ApplicationName:       ver.ApplicationName,
		VersionLabel:          ver.VersionLabel,
		ApplicationVersionArn: ver.ApplicationVersionARN,
		Description:           ver.Description,
		Status:                ver.Status,
	}
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

func (h *Handler) handleCreateApplicationVersion(vals url.Values) (any, error) {
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

	ver, err := h.Backend.CreateApplicationVersion(appName, versionLabel, description, tags)
	if err != nil {
		return nil, err
	}

	return &createApplicationVersionResponse{
		Xmlns:                          ebXMLNS,
		CreateApplicationVersionResult: createApplicationVersionResult{ApplicationVersion: toAppVersionDesc(ver)},
		ResponseMetadata:               responseMetadata{RequestID: "eb-create-ver"},
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

func (h *Handler) handleDescribeApplicationVersions(vals url.Values) (any, error) {
	appName := vals.Get("ApplicationName")
	versionLabels := parseMembers(vals, "VersionLabels.member")
	vers := h.Backend.DescribeApplicationVersions(appName, versionLabels)

	members := make([]appVersionDescType, 0, len(vers))

	for _, ver := range vers {
		members = append(members, toAppVersionDesc(ver))
	}

	return &describeApplicationVersionsResponse{
		Xmlns:                             ebXMLNS,
		DescribeApplicationVersionsResult: describeApplicationVersionsResult{ApplicationVersions: members},
		ResponseMetadata:                  responseMetadata{RequestID: "eb-describe-vers"},
	}, nil
}

type deleteApplicationVersionResponse struct {
	XMLName          xml.Name         `xml:"DeleteApplicationVersionResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
}

func (h *Handler) handleDeleteApplicationVersion(vals url.Values) (any, error) {
	appName := vals.Get("ApplicationName")
	versionLabel := vals.Get("VersionLabel")

	if appName == "" {
		return nil, fmt.Errorf("%w: ApplicationName is required", ErrInvalidParameter)
	}

	if versionLabel == "" {
		return nil, fmt.Errorf("%w: VersionLabel is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteApplicationVersion(appName, versionLabel); err != nil {
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

func (h *Handler) handleListTagsForResource(vals url.Values) (any, error) {
	resourceARN := vals.Get("ResourceArn")
	if resourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrInvalidParameter)
	}

	tags, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return nil, err
	}

	members := make([]tagDescType, 0, len(tags))

	for k, v := range tags {
		members = append(members, tagDescType{Key: k, Value: v})
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

func (h *Handler) handleUpdateTagsForResource(vals url.Values) (any, error) {
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

	if err := h.Backend.UpdateTagsForResource(resourceARN, addTags, removeTags); err != nil {
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

// handleDescribeEvents returns an empty events list.
// The Terraform provider calls DescribeEvents to poll environment creation status.
func (h *Handler) handleDescribeEvents(_ url.Values) (any, error) {
	return &describeEventsResponse{
		Xmlns:                ebXMLNS,
		DescribeEventsResult: describeEventsResult{},
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

// handleDescribeEnvironmentResources returns an empty environment resources list.
// The Terraform provider calls this after environment creation to read resource details.
func (h *Handler) handleDescribeEnvironmentResources(vals url.Values) (any, error) {
	envName := vals.Get("EnvironmentName")

	return &describeEnvironmentResourcesResponse{
		Xmlns: ebXMLNS,
		DescribeEnvironmentResourcesResult: describeEnvironmentResourcesResult{
			EnvironmentResources: environmentResourceDescType{
				EnvironmentName: envName,
			},
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
	EnvironmentName   string                           `xml:"EnvironmentName"`
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

// handleDescribeConfigurationSettings returns the configuration settings for an environment.
// The Terraform provider calls this after environment creation to populate all_settings.
// SolutionStackName must be populated to prevent the provider from dereferencing a nil pointer.
func (h *Handler) handleDescribeConfigurationSettings(vals url.Values) (any, error) {
	appName := vals.Get("ApplicationName")
	envName := vals.Get("EnvironmentName")

	solutionStack := ""

	envs := h.Backend.DescribeEnvironments(appName, []string{envName}, nil)
	if len(envs) > 0 {
		solutionStack = envs[0].SolutionStackName
	}

	return &describeConfigurationSettingsResponse{
		Xmlns: ebXMLNS,
		DescribeConfigurationSettingsResult: describeConfigurationSettingsResult{
			ConfigurationSettings: []configurationSettingsDescType{
				{
					ApplicationName:   appName,
					EnvironmentName:   envName,
					SolutionStackName: solutionStack,
				},
			},
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
		{ErrNotFound, "InvalidParameterValue"},
		{ErrAlreadyExists, "InvalidParameterValue"},
		{ErrInvalidParameter, "InvalidParameterValue"},
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
