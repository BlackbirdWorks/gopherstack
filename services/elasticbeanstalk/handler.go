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

	nsAutoScalingASG          = "aws:autoscaling:asg"
	nsAutoScalingLaunchConfig = "aws:autoscaling:launchconfiguration"
	nsAutoScalingTrigger      = "aws:autoscaling:trigger"
	nsEC2VPC                  = "aws:ec2:vpc"
	nsEBApplication           = "aws:elasticbeanstalk:application"
	nsEBCloudWatchLogs        = "aws:elasticbeanstalk:cloudwatch:logs"
	nsEBEnvironment           = "aws:elasticbeanstalk:environment"
	nsEBEnvironmentProxy      = "aws:elasticbeanstalk:environment:proxy"
	nsEBHealthReportingSystem = "aws:elasticbeanstalk:healthreporting:system"
	nsEBManagedActions        = "aws:elasticbeanstalk:managedactions"
	nsEBMonitoring            = "aws:elasticbeanstalk:monitoring"
	nsEBSNSTopics             = "aws:elasticbeanstalk:sns:topics"
	nsEBXRay                  = "aws:elasticbeanstalk:xray"
	nsELBLoadBalancer         = "aws:elb:loadbalancer"
	nsELBv2LoadBalancer       = "aws:elbv2:loadbalancer"
	nsRDSDBInstance           = "aws:rds:dbinstance"

	optionValueTypeScalar      = "Scalar"
	optionValueTypeList        = "List"
	optionValueTypeBoolean     = "Boolean"
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
	// envHealthStatusOk is the EnvironmentHealthStatus enum value ("Ok") that
	// corresponds to this backend's invariant Health color (envHealthGreen,
	// "Green") and Status ("Ready") -- see types.EnvironmentHealthStatus
	// (elasticbeanstalk@v1.37.4 types/enums.go:216-224): "Green" is not a
	// member of that enum at all, only of the separate EnvironmentHealth
	// (color) enum.
	envHealthStatusOk = "Ok"
	// platformOwnerSelf is the PlatformOwner value AWS documents for
	// customer-created (as opposed to AWS-managed) custom platform versions,
	// which is the only kind CreatePlatformVersion produces here.
	platformOwnerSelf = "self"

	// defaultListLimit is the page size applied when a request does not
	// specify MaxRecords/MaxItems (or specifies a non-positive value).
	defaultListLimit = 100
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
		// ErrResourceNotFound must be checked before ErrNotFound: it is the
		// ARN-lookup-specific case (ListTagsForResource/UpdateTagsForResource)
		// that AWS documents as a distinct ResourceNotFoundException, unlike
		// the generic InvalidParameterValue used by name-based lookups.
		{ErrResourceNotFound, "ResourceNotFoundException"},
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
