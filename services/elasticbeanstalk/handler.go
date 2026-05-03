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
	errInvalidParameterValue = "InvalidParameterValue"
)

const (
	ebXMLNS = "https://elasticbeanstalk.amazonaws.com/docs/2010-12-01/"
)

// formOpFunc is the function type for a dispatched form-encoded operation.
type formOpFunc func(url.Values) (any, error)

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
		"AbortEnvironmentUpdate":             h.handleAbortEnvironmentUpdate,
		"ApplyEnvironmentManagedAction":      h.handleApplyEnvironmentManagedAction,
		"AssociateEnvironmentOperationsRole": h.handleAssociateEnvironmentOperationsRole,
		"CheckDNSAvailability":               h.handleCheckDNSAvailability,
		"ComposeEnvironments":                h.handleComposeEnvironments,
		"CreateApplication":                  h.handleCreateApplication,
		"CreateConfigurationTemplate":        h.handleCreateConfigurationTemplate,
		"CreateEnvironment":                  h.handleCreateEnvironment,
		"CreateApplicationVersion":           h.handleCreateApplicationVersion,
		"CreatePlatformVersion":              h.handleCreatePlatformVersion,
		"CreateStorageLocation":              h.handleCreateStorageLocation,
		"DeleteApplication":                  h.handleDeleteApplication,
		"DeleteApplicationVersion":           h.handleDeleteApplicationVersion,
		"DeleteConfigurationTemplate":        h.handleDeleteConfigurationTemplate,
		"DeleteEnvironmentConfiguration":     h.handleDeleteEnvironmentConfiguration,
		"DescribeApplications":               h.handleDescribeApplications,
		"DescribeApplicationVersions":        h.handleDescribeApplicationVersions,
		"DescribeConfigurationSettings":      h.handleDescribeConfigurationSettings,
		"DescribeEnvironmentResources":       h.handleDescribeEnvironmentResources,
		"DescribeEnvironments":               h.handleDescribeEnvironments,
		"DescribeEvents":                     h.handleDescribeEvents,
		"ListTagsForResource":                h.handleListTagsForResource,
		"RebuildEnvironment":                 h.handleRebuildEnvironment,
		"RestartAppServer":                   h.handleRestartAppServer,
		"TerminateEnvironment":               h.handleTerminateEnvironment,
		"UpdateApplication":                  h.handleUpdateApplication,
		"UpdateEnvironment":                  h.handleUpdateEnvironment,
		"UpdateTagsForResource":              h.handleUpdateTagsForResource,
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
		"DescribeApplications",
		"DescribeApplicationVersions",
		"DescribeConfigurationSettings",
		"DescribeEnvironmentResources",
		"DescribeEnvironments",
		"DescribeEvents",
		"ListTagsForResource",
		"RebuildEnvironment",
		"RestartAppServer",
		"TerminateEnvironment",
		"UpdateApplication",
		"UpdateEnvironment",
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
	if fn, ok := h.ops[action]; ok {
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

	// If no app name provided, search across all environments for this name.
	if appName == "" {
		envs := h.Backend.DescribeEnvironments("", []string{envName}, nil)

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

	if envName != "" {
		envs := h.Backend.DescribeEnvironments(appName, []string{envName}, nil)
		if len(envs) > 0 {
			solutionStack = envs[0].SolutionStackName
		}
	}

	settings := make([]configurationSettingsDescType, 0)

	if envName != "" || appName != "" {
		settings = append(settings, configurationSettingsDescType{
			ApplicationName:   appName,
			EnvironmentName:   envName,
			SolutionStackName: solutionStack,
		})
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
func (h *Handler) handleRestartAppServer(_ url.Values) (any, error) {
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
func (h *Handler) handleRebuildEnvironment(_ url.Values) (any, error) {
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
func (h *Handler) handleAbortEnvironmentUpdate(_ url.Values) (any, error) {
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
func (h *Handler) handleApplyEnvironmentManagedAction(vals url.Values) (any, error) {
	actionID := vals.Get("ActionId")
	if actionID == "" {
		return nil, fmt.Errorf("%w: ActionId is required", ErrInvalidParameter)
	}

	_ = h.Backend.ApplyEnvironmentManagedAction(vals.Get("EnvironmentName"), actionID)

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
func (h *Handler) handleAssociateEnvironmentOperationsRole(vals url.Values) (any, error) {
	envName := vals.Get("EnvironmentName")
	if envName == "" {
		return nil, fmt.Errorf("%w: EnvironmentName is required", ErrInvalidParameter)
	}

	operationsRole := vals.Get("OperationsRole")
	if operationsRole == "" {
		return nil, fmt.Errorf("%w: OperationsRole is required", ErrInvalidParameter)
	}

	if err := h.Backend.AssociateEnvironmentOperationsRole(envName, operationsRole); err != nil {
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
func (h *Handler) handleCheckDNSAvailability(vals url.Values) (any, error) {
	cnamePrefix := vals.Get("CNAMEPrefix")
	if cnamePrefix == "" {
		return nil, fmt.Errorf("%w: CNAMEPrefix is required", ErrInvalidParameter)
	}

	available, fqcname := h.Backend.CheckDNSAvailability(cnamePrefix)

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
func (h *Handler) handleComposeEnvironments(vals url.Values) (any, error) {
	appName := vals.Get("ApplicationName")
	if appName == "" {
		return nil, fmt.Errorf("%w: ApplicationName is required", ErrInvalidParameter)
	}

	envs := h.Backend.ComposeEnvironments(appName)

	members := make([]environmentDescType, 0, len(envs))

	for _, env := range envs {
		members = append(members, toEnvironmentDesc(env, h.Backend.Region()))
	}

	return &composeEnvironmentsResponse{
		Xmlns:                     ebXMLNS,
		ComposeEnvironmentsResult: composeEnvironmentsResult{Environments: members},
		ResponseMetadata:          responseMetadata{RequestID: "eb-compose-envs"},
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
func (h *Handler) handleCreateConfigurationTemplate(vals url.Values) (any, error) {
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

	tmpl, err := h.Backend.CreateConfigurationTemplate(appName, templateName, description, solutionStack, tags)
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
func (h *Handler) handleCreatePlatformVersion(vals url.Values) (any, error) {
	platformName := vals.Get("PlatformName")
	if platformName == "" {
		return nil, fmt.Errorf("%w: PlatformName is required", ErrInvalidParameter)
	}

	platformVersion := vals.Get("PlatformVersion")
	if platformVersion == "" {
		return nil, fmt.Errorf("%w: PlatformVersion is required", ErrInvalidParameter)
	}

	tags := parseTagList(vals, "Tags.member")

	pv, err := h.Backend.CreatePlatformVersion(platformName, platformVersion, tags)
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
func (h *Handler) handleCreateStorageLocation(_ url.Values) (any, error) {
	bucket := h.Backend.CreateStorageLocation()

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
func (h *Handler) handleDeleteConfigurationTemplate(vals url.Values) (any, error) {
	appName := vals.Get("ApplicationName")
	if appName == "" {
		return nil, fmt.Errorf("%w: ApplicationName is required", ErrInvalidParameter)
	}

	templateName := vals.Get("TemplateName")
	if templateName == "" {
		return nil, fmt.Errorf("%w: TemplateName is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteConfigurationTemplate(appName, templateName); err != nil {
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
func (h *Handler) handleDeleteEnvironmentConfiguration(vals url.Values) (any, error) {
	appName := vals.Get("ApplicationName")
	if appName == "" {
		return nil, fmt.Errorf("%w: ApplicationName is required", ErrInvalidParameter)
	}

	envName := vals.Get("EnvironmentName")
	if envName == "" {
		return nil, fmt.Errorf("%w: EnvironmentName is required", ErrInvalidParameter)
	}

	_ = h.Backend.DeleteEnvironmentConfiguration(appName, envName)

	return &deleteEnvironmentConfigurationResponse{
		Xmlns:            ebXMLNS,
		ResponseMetadata: responseMetadata{RequestID: "eb-delete-env-config"},
	}, nil
}
