package autoscaling

import (
	"context"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	autoscalingVersion           = "2011-01-01"
	autoscalingXMLNS             = "http://autoscaling.amazonaws.com/doc/2011-01-01/"
	errValidationError           = "ValidationError"
	resourceTypeAutoScalingGroup = "auto-scaling-group"
	formValueTrue                = "true"
)

// Handler is the Echo HTTP handler for Autoscaling operations.
type Handler struct {
	Backend       StorageBackend
	dispatchTable map[string]func(url.Values) (any, error)
}

// NewHandler creates a new Autoscaling handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.dispatchTable = h.buildDispatchTable()

	return h
}

func (h *Handler) buildDispatchTable() map[string]func(url.Values) (any, error) {
	return map[string]func(url.Values) (any, error){
		"CreateAutoScalingGroup":              h.handleCreateAutoScalingGroup,
		"DescribeAutoScalingGroups":           h.handleDescribeAutoScalingGroups,
		"UpdateAutoScalingGroup":              h.handleUpdateAutoScalingGroup,
		"DeleteAutoScalingGroup":              h.handleDeleteAutoScalingGroup,
		"CreateLaunchConfiguration":           h.handleCreateLaunchConfiguration,
		"DescribeLaunchConfigurations":        h.handleDescribeLaunchConfigurations,
		"DeleteLaunchConfiguration":           h.handleDeleteLaunchConfiguration,
		"DescribeScalingActivities":           h.handleDescribeScalingActivities,
		"AttachInstances":                     h.handleAttachInstances,
		"AttachLoadBalancerTargetGroups":      h.handleAttachLoadBalancerTargetGroups,
		"AttachLoadBalancers":                 h.handleAttachLoadBalancers,
		"AttachTrafficSources":                h.handleAttachTrafficSources,
		"BatchDeleteScheduledAction":          h.handleBatchDeleteScheduledAction,
		"BatchPutScheduledUpdateGroupAction":  h.handleBatchPutScheduledUpdateGroupAction,
		"CancelInstanceRefresh":               h.handleCancelInstanceRefresh,
		"CompleteLifecycleAction":             h.handleCompleteLifecycleAction,
		"CreateOrUpdateTags":                  h.handleCreateOrUpdateTags,
		"DeleteLifecycleHook":                 h.handleDeleteLifecycleHook,
		"SetDesiredCapacity":                  h.handleSetDesiredCapacity,
		"TerminateInstanceInAutoScalingGroup": h.handleTerminateInstanceInAutoScalingGroup,
		"PutLifecycleHook":                    h.handlePutLifecycleHook,
		"DescribeLifecycleHooks":              h.handleDescribeLifecycleHooks,
		"DescribeScheduledActions":            h.handleDescribeScheduledActions,
		"DeleteTags":                          h.handleDeleteTags,
		"DescribeTags":                        h.handleDescribeTags,
		"DescribeAutoScalingInstances":        h.handleDescribeAutoScalingInstances,
		// New operations
		"DeleteNotificationConfiguration":      h.handleDeleteNotificationConfiguration,
		"DeletePolicy":                         h.handleDeletePolicy,
		"DeleteScheduledAction":                h.handleDeleteScheduledAction,
		"DeleteWarmPool":                       h.handleDeleteWarmPool,
		"DescribeAccountLimits":                h.handleDescribeAccountLimits,
		"DescribeAdjustmentTypes":              h.handleDescribeAdjustmentTypes,
		"DescribeAutoScalingNotificationTypes": h.handleDescribeAutoScalingNotificationTypes,
		"DescribeInstanceRefreshes":            h.handleDescribeInstanceRefreshes,
		"DescribeLifecycleHookTypes":           h.handleDescribeLifecycleHookTypes,
		"DescribeLoadBalancerTargetGroups":     h.handleDescribeLoadBalancerTargetGroups,
		"DescribeLoadBalancers":                h.handleDescribeLoadBalancers,
		"DescribeMetricCollectionTypes":        h.handleDescribeMetricCollectionTypes,
		"DescribeNotificationConfigurations":   h.handleDescribeNotificationConfigurations,
		"DescribePolicies":                     h.handleDescribePolicies,
		"DescribeScalingProcessTypes":          h.handleDescribeScalingProcessTypes,
		"DescribeTerminationPolicyTypes":       h.handleDescribeTerminationPolicyTypes,
		"DescribeTrafficSources":               h.handleDescribeTrafficSources,
		"DescribeWarmPool":                     h.handleDescribeWarmPool,
		"DetachInstances":                      h.handleDetachInstances,
		"DetachLoadBalancerTargetGroups":       h.handleDetachLoadBalancerTargetGroups,
		"DetachLoadBalancers":                  h.handleDetachLoadBalancers,
		"DetachTrafficSources":                 h.handleDetachTrafficSources,
		"DisableMetricsCollection":             h.handleDisableMetricsCollection,
		"EnableMetricsCollection":              h.handleEnableMetricsCollection,
		"EnterStandby":                         h.handleEnterStandby,
		"ExecutePolicy":                        h.handleExecutePolicy,
		"ExitStandby":                          h.handleExitStandby,
		"GetPredictiveScalingForecast":         h.handleGetPredictiveScalingForecast,
		"LaunchInstances":                      h.handleLaunchInstances,
		"PutNotificationConfiguration":         h.handlePutNotificationConfiguration,
		"PutScalingPolicy":                     h.handlePutScalingPolicy,
		"PutScheduledUpdateGroupAction":        h.handlePutScheduledUpdateGroupAction,
		"PutWarmPool":                          h.handlePutWarmPool,
		"RecordLifecycleActionHeartbeat":       h.handleRecordLifecycleActionHeartbeat,
		"ResumeProcesses":                      h.handleResumeProcesses,
		"RollbackInstanceRefresh":              h.handleRollbackInstanceRefresh,
		"SetInstanceHealth":                    h.handleSetInstanceHealth,
		"SetInstanceProtection":                h.handleSetInstanceProtection,
		"StartInstanceRefresh":                 h.handleStartInstanceRefresh,
		"SuspendProcesses":                     h.handleSuspendProcesses,
	}
}

// Shutdown stops the backend's in-flight lifecycle-hook timers so no timer
// goroutine outlives the service. Invoked on server shutdown via
// service.Shutdowner.
func (h *Handler) Shutdown(_ context.Context) {
	if c, ok := h.Backend.(interface{ Close() }); ok {
		c.Close()
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Autoscaling" }

// GetSupportedOperations returns the list of supported Autoscaling operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateAutoScalingGroup",
		"DescribeAutoScalingGroups",
		"UpdateAutoScalingGroup",
		"DeleteAutoScalingGroup",
		"CreateLaunchConfiguration",
		"DescribeLaunchConfigurations",
		"DeleteLaunchConfiguration",
		"DescribeScalingActivities",
		"AttachInstances",
		"AttachLoadBalancerTargetGroups",
		"AttachLoadBalancers",
		"AttachTrafficSources",
		"BatchDeleteScheduledAction",
		"BatchPutScheduledUpdateGroupAction",
		"CancelInstanceRefresh",
		"CompleteLifecycleAction",
		"CreateOrUpdateTags",
		"DeleteLifecycleHook",
		"SetDesiredCapacity",
		"TerminateInstanceInAutoScalingGroup",
		"PutLifecycleHook",
		"DescribeLifecycleHooks",
		"DescribeScheduledActions",
		"DeleteTags",
		"DescribeTags",
		"DescribeAutoScalingInstances",
		// New operations
		"DeleteNotificationConfiguration",
		"DeletePolicy",
		"DeleteScheduledAction",
		"DeleteWarmPool",
		"DescribeAccountLimits",
		"DescribeAdjustmentTypes",
		"DescribeAutoScalingNotificationTypes",
		"DescribeInstanceRefreshes",
		"DescribeLifecycleHookTypes",
		"DescribeLoadBalancerTargetGroups",
		"DescribeLoadBalancers",
		"DescribeMetricCollectionTypes",
		"DescribeNotificationConfigurations",
		"DescribePolicies",
		"DescribeScalingProcessTypes",
		"DescribeTerminationPolicyTypes",
		"DescribeTrafficSources",
		"DescribeWarmPool",
		"DetachInstances",
		"DetachLoadBalancerTargetGroups",
		"DetachLoadBalancers",
		"DetachTrafficSources",
		"DisableMetricsCollection",
		"EnableMetricsCollection",
		"EnterStandby",
		"ExecutePolicy",
		"ExitStandby",
		"GetPredictiveScalingForecast",
		"LaunchInstances",
		"PutNotificationConfiguration",
		"PutScalingPolicy",
		"PutScheduledUpdateGroupAction",
		"PutWarmPool",
		"RecordLifecycleActionHeartbeat",
		"ResumeProcesses",
		"RollbackInstanceRefresh",
		"SetInstanceHealth",
		"SetInstanceProtection",
		"StartInstanceRefresh",
		"SuspendProcesses",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "autoscaling" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function that matches Autoscaling requests.
// Autoscaling requests are form-encoded POSTs with Version=2011-01-01.
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

		vals, err := url.ParseQuery(string(body))
		if err != nil {
			return false
		}

		return vals.Get("Version") == autoscalingVersion
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityFormStandard }

// ExtractOperation extracts the Autoscaling action from the request.
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

// ExtractResource extracts the Auto Scaling group name from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return ""
	}

	return r.Form.Get("AutoScalingGroupName")
}

// Handler returns the Echo handler function for Autoscaling operations.
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
		log.Debug("autoscaling request", "action", action)

		resp, opErr := h.dispatch(action, vals)
		if opErr != nil {
			return h.handleOpError(c, action, opErr)
		}

		xmlBytes, err := marshalXML(resp)
		if err != nil {
			return h.writeError(c, http.StatusInternalServerError, "InternalFailure", "internal server error")
		}

		return c.Blob(http.StatusOK, "text/xml", xmlBytes)
	}
}

// dispatch routes the Autoscaling action to the appropriate handler.
func (h *Handler) dispatch(action string, vals url.Values) (any, error) {
	fn, ok := h.dispatchTable[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownAction, action)
	}

	return fn(vals)
}

func (h *Handler) handleCreateAutoScalingGroup(vals url.Values) (any, error) {
	name := vals.Get("AutoScalingGroupName")
	lcName := vals.Get("LaunchConfigurationName")
	healthCheckType := vals.Get("HealthCheckType")

	minSize, err := parseIntVal(vals.Get("MinSize"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid MinSize", ErrInvalidParameter)
	}

	maxSize, err := parseIntVal(vals.Get("MaxSize"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid MaxSize", ErrInvalidParameter)
	}

	desiredCapacity, err := parseIntVal(vals.Get("DesiredCapacity"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid DesiredCapacity", ErrInvalidParameter)
	}

	// Enforce bounds on sizes to prevent excessive memory allocation when creating
	// the initial instances slice in the backend.
	if minSize < 0 || maxSize < 0 || desiredCapacity < 0 {
		return nil, fmt.Errorf("%w: sizes must be non-negative", ErrInvalidParameter)
	}

	if minSize > maxDesiredCapacity || maxSize > maxDesiredCapacity || desiredCapacity > maxDesiredCapacity {
		return nil, fmt.Errorf("%w: sizes must not exceed %d", ErrInvalidParameter, maxDesiredCapacity)
	}

	defaultCooldown, err := parseIntVal(vals.Get("DefaultCooldown"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid DefaultCooldown", ErrInvalidParameter)
	}

	healthCheckGracePeriod, err := parseIntVal(vals.Get("HealthCheckGracePeriod"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid HealthCheckGracePeriod", ErrInvalidParameter)
	}

	maxInstanceLifetime, err := parseIntVal(vals.Get("MaxInstanceLifetime"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid MaxInstanceLifetime", ErrInvalidParameter)
	}

	defaultInstanceWarmup, err := parseIntVal(vals.Get("DefaultInstanceWarmup"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid DefaultInstanceWarmup", ErrInvalidParameter)
	}

	azs := parseMembers(vals, "AvailabilityZones.member")
	lbNames := parseMembers(vals, "LoadBalancerNames.member")
	targetGroupARNs := parseMembers(vals, "TargetGroupARNs.member")
	tags := parseTags(vals, "Tags.member")
	terminationPolicies := parseMembers(vals, "TerminationPolicies.member")
	lt := parseLaunchTemplate(vals, "LaunchTemplate")

	input := CreateAutoScalingGroupInput{
		AutoScalingGroupName:             name,
		LaunchConfigurationName:          lcName,
		LaunchTemplate:                   lt,
		VPCZoneIdentifier:                vals.Get("VPCZoneIdentifier"),
		PlacementGroup:                   vals.Get("PlacementGroup"),
		Context:                          vals.Get("Context"),
		DesiredCapacityType:              vals.Get("DesiredCapacityType"),
		MinSize:                          minSize,
		MaxSize:                          maxSize,
		DesiredCapacity:                  desiredCapacity,
		DefaultCooldown:                  defaultCooldown,
		HealthCheckType:                  healthCheckType,
		HealthCheckGracePeriod:           healthCheckGracePeriod,
		MaxInstanceLifetime:              maxInstanceLifetime,
		DefaultInstanceWarmup:            defaultInstanceWarmup,
		NewInstancesProtectedFromScaleIn: vals.Get("NewInstancesProtectedFromScaleIn") == formValueTrue,
		CapacityRebalance:                vals.Get("CapacityRebalance") == formValueTrue,
		AvailabilityZones:                azs,
		LoadBalancerNames:                lbNames,
		TargetGroupARNs:                  targetGroupARNs,
		Tags:                             tags,
		TerminationPolicies:              terminationPolicies,
	}

	_, createErr := h.Backend.CreateAutoScalingGroup(input)
	if createErr != nil {
		return nil, createErr
	}

	return &createAutoScalingGroupResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-" + name},
	}, nil
}

const (
	// defaultMaxRecords is the default max records for paginated responses.
	defaultMaxRecords = int32(100)
	// maxMaxRecords is the maximum max records allowed.
	maxMaxRecords = int32(100)
)

func (h *Handler) handleDescribeAutoScalingGroups(vals url.Values) (any, error) {
	names := parseMembers(vals, "AutoScalingGroupNames.member")

	groups, err := h.Backend.DescribeAutoScalingGroups(names)
	if err != nil {
		return nil, err
	}

	// Parse MaxRecords (default 100, max 100)
	maxRecords := defaultMaxRecords
	if v := vals.Get("MaxRecords"); v != "" {
		if n, parseErr := parseIntVal(v); parseErr == nil && n > 0 {
			maxRecords = min(n, maxMaxRecords)
		}
	}

	// Apply NextToken cursor (base64-encoded last group name)
	nextToken := vals.Get("NextToken")
	if nextToken != "" {
		if decoded, decErr := base64.StdEncoding.DecodeString(nextToken); decErr == nil {
			lastName := string(decoded)
			// Skip groups up to and including lastName
			for i, g := range groups {
				if g.AutoScalingGroupName == lastName {
					groups = groups[i+1:]

					break
				}
			}
		}
	}

	// Paginate
	var returnedNextToken string
	if int32(len(groups)) > maxRecords { //nolint:gosec // bounded by maxMaxRecords
		returnedNextToken = base64.StdEncoding.EncodeToString(
			[]byte(groups[maxRecords-1].AutoScalingGroupName),
		)
		groups = groups[:maxRecords]
	}

	members := make([]xmlAutoScalingGroup, 0, len(groups))
	for i := range groups {
		members = append(members, toXMLGroup(&groups[i]))
	}

	return &describeAutoScalingGroupsResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeAutoScalingGroupsResult{
			NextToken:         returnedNextToken,
			AutoScalingGroups: xmlAutoScalingGroupList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-groups"},
	}, nil
}

//nolint:gocognit,cyclop // Too complex to refactor given time constraints
func (h *Handler) handleUpdateAutoScalingGroup(vals url.Values) (any, error) {
	name := vals.Get("AutoScalingGroupName")

	input := UpdateAutoScalingGroupInput{
		AutoScalingGroupName:    name,
		LaunchConfigurationName: vals.Get("LaunchConfigurationName"),
		HealthCheckType:         vals.Get("HealthCheckType"),
		VPCZoneIdentifier:       vals.Get("VPCZoneIdentifier"),
		PlacementGroup:          vals.Get("PlacementGroup"),
		Context:                 vals.Get("Context"),
		DesiredCapacityType:     vals.Get("DesiredCapacityType"),
		AvailabilityZones:       parseMembers(vals, "AvailabilityZones.member"),
		TerminationPolicies:     parseMembers(vals, "TerminationPolicies.member"),
		LaunchTemplate:          parseLaunchTemplate(vals, "LaunchTemplate"),
	}

	if v := vals.Get("MinSize"); v != "" {
		n, err := parseIntVal(v)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid MinSize", ErrInvalidParameter)
		}

		input.MinSize = &n
	}

	if v := vals.Get("MaxSize"); v != "" {
		n, err := parseIntVal(v)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid MaxSize", ErrInvalidParameter)
		}

		input.MaxSize = &n
	}

	if v := vals.Get("DesiredCapacity"); v != "" {
		n, err := parseIntVal(v)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid DesiredCapacity", ErrInvalidParameter)
		}

		input.DesiredCapacity = &n
	}

	if v := vals.Get("DefaultCooldown"); v != "" {
		n, err := parseIntVal(v)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid DefaultCooldown", ErrInvalidParameter)
		}

		input.DefaultCooldown = &n
	}

	if v := vals.Get("HealthCheckGracePeriod"); v != "" {
		n, err := parseIntVal(v)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid HealthCheckGracePeriod", ErrInvalidParameter)
		}

		input.HealthCheckGracePeriod = &n
	}

	if v := vals.Get("MaxInstanceLifetime"); v != "" {
		n, err := parseIntVal(v)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid MaxInstanceLifetime", ErrInvalidParameter)
		}

		input.MaxInstanceLifetime = &n
	}

	if v := vals.Get("DefaultInstanceWarmup"); v != "" {
		n, err := parseIntVal(v)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid DefaultInstanceWarmup", ErrInvalidParameter)
		}

		input.DefaultInstanceWarmup = &n
	}

	if v := vals.Get("NewInstancesProtectedFromScaleIn"); v != "" {
		b := v == formValueTrue
		input.NewInstancesProtectedFromScaleIn = &b
	}

	if v := vals.Get("CapacityRebalance"); v != "" {
		b := v == formValueTrue
		input.CapacityRebalance = &b
	}

	_, updateErr := h.Backend.UpdateAutoScalingGroup(input)
	if updateErr != nil {
		return nil, updateErr
	}

	return &updateAutoScalingGroupResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-update-" + name},
	}, nil
}

func (h *Handler) handleDeleteAutoScalingGroup(vals url.Values) (any, error) {
	name := vals.Get("AutoScalingGroupName")
	forceDelete := vals.Get("ForceDelete") == formValueTrue

	if err := h.Backend.DeleteAutoScalingGroup(name, forceDelete); err != nil {
		return nil, err
	}

	return &deleteAutoScalingGroupResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-delete-" + name},
	}, nil
}

func (h *Handler) handleCreateLaunchConfiguration(vals url.Values) (any, error) {
	name := vals.Get("LaunchConfigurationName")
	imageID := vals.Get("ImageId")
	instanceType := vals.Get("InstanceType")
	keyName := vals.Get("KeyName")
	iamInstanceProfile := vals.Get("IamInstanceProfile")
	userData := vals.Get("UserData")
	kernelID := vals.Get("KernelId")
	ramdiskID := vals.Get("RamdiskId")
	securityGroups := parseMembers(vals, "SecurityGroups.member")
	classicLinkSGs := parseMembers(vals, "ClassicLinkVPCSecurityGroups.member")
	blockDeviceMappings := parseBlockDeviceMappings(vals)

	input := CreateLaunchConfigurationInput{
		LaunchConfigurationName:      name,
		ImageID:                      imageID,
		InstanceType:                 instanceType,
		KeyName:                      keyName,
		IAMInstanceProfile:           iamInstanceProfile,
		UserData:                     userData,
		KernelID:                     kernelID,
		RamdiskID:                    ramdiskID,
		SpotPrice:                    vals.Get("SpotPrice"),
		PlacementTenancy:             vals.Get("PlacementTenancy"),
		ClassicLinkVPCID:             vals.Get("ClassicLinkVPCId"),
		SecurityGroups:               securityGroups,
		ClassicLinkVPCSecurityGroups: classicLinkSGs,
		BlockDeviceMappings:          blockDeviceMappings,
		AssociatePublicIPAddress:     vals.Get("AssociatePublicIpAddress") == formValueTrue,
		EbsOptimized:                 vals.Get("EbsOptimized") == formValueTrue,
		InstanceMonitoring:           vals.Get("InstanceMonitoring.Enabled") == formValueTrue,
	}

	_, createErr := h.Backend.CreateLaunchConfiguration(input)
	if createErr != nil {
		return nil, createErr
	}

	return &createLaunchConfigurationResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-create-lc-" + name},
	}, nil
}

func (h *Handler) handleDescribeLaunchConfigurations(vals url.Values) (any, error) {
	names := parseMembers(vals, "LaunchConfigurationNames.member")

	lcs, err := h.Backend.DescribeLaunchConfigurations(names)
	if err != nil {
		return nil, err
	}

	members := make([]xmlLaunchConfiguration, 0, len(lcs))
	for i := range lcs {
		members = append(members, toXMLLaunchConfiguration(&lcs[i]))
	}

	return &describeLaunchConfigurationsResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeLaunchConfigurationsResult{
			LaunchConfigurations: xmlLaunchConfigurationList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-lcs"},
	}, nil
}

func (h *Handler) handleDeleteLaunchConfiguration(vals url.Values) (any, error) {
	name := vals.Get("LaunchConfigurationName")

	if err := h.Backend.DeleteLaunchConfiguration(name); err != nil {
		return nil, err
	}

	return &deleteLaunchConfigurationResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-delete-lc-" + name},
	}, nil
}

func (h *Handler) handleDescribeScalingActivities(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")

	activities, err := h.Backend.DescribeScalingActivities(groupName)
	if err != nil {
		return nil, err
	}

	// Apply MaxRecords if provided
	if maxStr := vals.Get("MaxRecords"); maxStr != "" {
		maxRecords, parseErr := parseIntVal(maxStr)
		if parseErr == nil && maxRecords > 0 && int(maxRecords) < len(activities) {
			activities = activities[:maxRecords]
		}
	}

	members := make([]xmlScalingActivity, 0, len(activities))
	for i := range activities {
		members = append(members, toXMLScalingActivity(&activities[i]))
	}

	return &describeScalingActivitiesResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeScalingActivitiesResult{
			Activities: xmlScalingActivityList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-activities"},
	}, nil
}

func (h *Handler) handleAttachInstances(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	instanceIDs := parseMembers(vals, "InstanceIds.member")

	if err := h.Backend.AttachInstances(groupName, instanceIDs); err != nil {
		return nil, err
	}

	return &attachInstancesResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-attach-instances"},
	}, nil
}

func (h *Handler) handleAttachLoadBalancerTargetGroups(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	targetGroupARNs := parseMembers(vals, "TargetGroupARNs.member")

	if err := h.Backend.AttachLoadBalancerTargetGroups(groupName, targetGroupARNs); err != nil {
		return nil, err
	}

	return &attachLoadBalancerTargetGroupsResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-attach-tgs"},
	}, nil
}

func (h *Handler) handleAttachLoadBalancers(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	lbNames := parseMembers(vals, "LoadBalancerNames.member")

	if err := h.Backend.AttachLoadBalancers(groupName, lbNames); err != nil {
		return nil, err
	}

	return &attachLoadBalancersResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-attach-lbs"},
	}, nil
}

func (h *Handler) handleAttachTrafficSources(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	tss := parseTrafficSources(vals)

	if err := h.Backend.AttachTrafficSources(groupName, tss); err != nil {
		return nil, err
	}

	return &attachTrafficSourcesResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-attach-traffic-sources"},
	}, nil
}

func (h *Handler) handleBatchDeleteScheduledAction(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	actionNames := parseMembers(vals, "ScheduledActionNames.member")

	failed, err := h.Backend.BatchDeleteScheduledAction(groupName, actionNames)
	if err != nil {
		return nil, err
	}

	members := make([]xmlFailedScheduledAction, 0, len(failed))
	for _, f := range failed {
		members = append(members, xmlFailedScheduledAction(f))
	}

	return &batchDeleteScheduledActionResponse{
		Xmlns: autoscalingXMLNS,
		Result: batchDeleteScheduledActionResult{
			FailedScheduledActions: xmlFailedScheduledActionList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-batch-delete-scheduled"},
	}, nil
}

func (h *Handler) handleBatchPutScheduledUpdateGroupAction(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	actions := parseBatchScheduledActions(vals)

	failed, err := h.Backend.BatchPutScheduledUpdateGroupAction(groupName, actions)
	if err != nil {
		return nil, err
	}

	members := make([]xmlFailedScheduledAction, 0, len(failed))
	for _, f := range failed {
		members = append(members, xmlFailedScheduledAction(f))
	}

	return &batchPutScheduledUpdateGroupActionResponse{
		Xmlns: autoscalingXMLNS,
		Result: batchPutScheduledUpdateGroupActionResult{
			FailedScheduledUpdateGroupActions: xmlFailedScheduledActionList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-batch-put-scheduled"},
	}, nil
}

func (h *Handler) handleCancelInstanceRefresh(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")

	refreshID, err := h.Backend.CancelInstanceRefresh(groupName)
	if err != nil {
		return nil, err
	}

	return &cancelInstanceRefreshResponse{
		Xmlns: autoscalingXMLNS,
		Result: cancelInstanceRefreshResult{
			InstanceRefreshID: refreshID,
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-cancel-refresh"},
	}, nil
}

func (h *Handler) handleCompleteLifecycleAction(vals url.Values) (any, error) {
	input := CompleteLifecycleActionInput{
		AutoScalingGroupName:  vals.Get("AutoScalingGroupName"),
		LifecycleHookName:     vals.Get("LifecycleHookName"),
		LifecycleActionToken:  vals.Get("LifecycleActionToken"),
		InstanceID:            vals.Get("InstanceId"),
		LifecycleActionResult: vals.Get("LifecycleActionResult"),
	}

	if err := h.Backend.CompleteLifecycleAction(input); err != nil {
		return nil, err
	}

	return &completeLifecycleActionResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-complete-lifecycle"},
	}, nil
}

func (h *Handler) handleCreateOrUpdateTags(vals url.Values) (any, error) {
	tags := parseResourceTags(vals, "Tags.member")

	if err := h.Backend.CreateOrUpdateTags(tags); err != nil {
		return nil, err
	}

	return &createOrUpdateTagsResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-create-or-update-tags"},
	}, nil
}

func (h *Handler) handleDeleteLifecycleHook(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	hookName := vals.Get("LifecycleHookName")

	if err := h.Backend.DeleteLifecycleHook(groupName, hookName); err != nil {
		return nil, err
	}

	return &deleteLifecycleHookResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-delete-lifecycle-hook"},
	}, nil
}

func (h *Handler) handleSetDesiredCapacity(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")

	desired, parseErr := parseIntVal(vals.Get("DesiredCapacity"))
	if parseErr != nil {
		return nil, fmt.Errorf("%w: invalid DesiredCapacity", ErrInvalidParameter)
	}

	if err := h.Backend.SetDesiredCapacity(groupName, desired); err != nil {
		return nil, err
	}

	return &setDesiredCapacityResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-set-desired-capacity"},
	}, nil
}

func (h *Handler) handleTerminateInstanceInAutoScalingGroup(vals url.Values) (any, error) {
	instanceID := vals.Get("InstanceId")
	decrement := vals.Get("ShouldDecrementDesiredCapacity") == formValueTrue

	activity, err := h.Backend.TerminateInstanceInAutoScalingGroup(instanceID, decrement)
	if err != nil {
		return nil, err
	}

	return &terminateInstanceInAutoScalingGroupResponse{
		Xmlns: autoscalingXMLNS,
		Result: terminateInstanceResult{
			Activity: toXMLScalingActivity(activity),
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-terminate-instance"},
	}, nil
}

func (h *Handler) handlePutLifecycleHook(vals url.Values) (any, error) {
	hook := LifecycleHook{
		LifecycleHookName:     vals.Get("LifecycleHookName"),
		AutoScalingGroupName:  vals.Get("AutoScalingGroupName"),
		LifecycleTransition:   vals.Get("LifecycleTransition"),
		DefaultResult:         vals.Get("DefaultResult"),
		NotificationTargetARN: vals.Get("NotificationTargetARN"),
		RoleARN:               vals.Get("RoleARN"),
	}

	if v := vals.Get("HeartbeatTimeout"); v != "" {
		n, err := parseIntVal(v)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid HeartbeatTimeout", ErrInvalidParameter)
		}

		hook.HeartbeatTimeout = n
	}

	if err := h.Backend.PutLifecycleHook(hook); err != nil {
		return nil, err
	}

	return &putLifecycleHookResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-put-lifecycle-hook"},
	}, nil
}

func (h *Handler) handleDescribeLifecycleHooks(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	hookNames := parseMembers(vals, "LifecycleHookNames.member")

	hooks, err := h.Backend.DescribeLifecycleHooks(groupName, hookNames)
	if err != nil {
		return nil, err
	}

	members := make([]xmlLifecycleHook, 0, len(hooks))
	for _, hook := range hooks {
		members = append(members, xmlLifecycleHook(hook))
	}

	return &describeLifecycleHooksResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeLifecycleHooksResult{
			LifecycleHooks: xmlLifecycleHookList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-lifecycle-hooks"},
	}, nil
}

func (h *Handler) handleDescribeScheduledActions(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	actionNames := parseMembers(vals, "ScheduledActionNames.member")

	actions, err := h.Backend.DescribeScheduledActions(groupName, actionNames)
	if err != nil {
		return nil, err
	}

	members := make([]xmlScheduledAction, 0, len(actions))
	for _, action := range actions {
		startTime := ""
		if !action.StartTime.IsZero() {
			startTime = action.StartTime.UTC().Format(time.RFC3339)
		}

		endTime := ""
		if !action.EndTime.IsZero() {
			endTime = action.EndTime.UTC().Format(time.RFC3339)
		}

		members = append(members, xmlScheduledAction{
			ScheduledActionName:  action.ScheduledActionName,
			ScheduledActionARN:   action.ScheduledActionARN,
			AutoScalingGroupName: action.AutoScalingGroupName,
			Recurrence:           action.Recurrence,
			TimeZone:             action.TimeZone,
			StartTime:            startTime,
			EndTime:              endTime,
			DesiredCapacity:      action.DesiredCapacity,
			MinSize:              action.MinSize,
			MaxSize:              action.MaxSize,
		})
	}

	return &describeScheduledActionsResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeScheduledActionsResult{
			ScheduledUpdateGroupActions: xmlScheduledActionList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-scheduled-actions"},
	}, nil
}

func (h *Handler) handleDeleteTags(vals url.Values) (any, error) {
	tags := parseResourceTags(vals, "Tags.member")

	if err := h.Backend.DeleteTags(tags); err != nil {
		return nil, err
	}

	return &deleteTagsResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-delete-tags"},
	}, nil
}

func (h *Handler) handleDescribeTags(vals url.Values) (any, error) {
	filters := parseTagFilters(vals)

	tags, err := h.Backend.DescribeTags(filters)
	if err != nil {
		return nil, err
	}

	members := make([]xmlResourceTag, 0, len(tags))
	for _, tag := range tags {
		members = append(members, xmlResourceTag(tag))
	}

	return &describeTagsResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeTagsResult{
			Tags: xmlResourceTagList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-tags"},
	}, nil
}

func (h *Handler) handleDescribeAutoScalingInstances(vals url.Values) (any, error) {
	instanceIDs := parseMembers(vals, "InstanceIds.member")

	instances, err := h.Backend.DescribeAutoScalingInstances(instanceIDs)
	if err != nil {
		return nil, err
	}

	members := make([]xmlInstanceDetails, 0, len(instances))
	for _, inst := range instances {
		members = append(members, xmlInstanceDetails{
			InstanceID:              inst.InstanceID,
			AutoScalingGroupName:    inst.AutoScalingGroupName,
			AvailabilityZone:        inst.AvailabilityZone,
			LifecycleState:          inst.LifecycleState,
			HealthStatus:            inst.HealthStatus,
			LaunchConfigurationName: inst.LaunchConfigurationName,
			InstanceType:            inst.InstanceType,
			ProtectedFromScaleIn:    inst.ProtectedFromScaleIn,
		})
	}

	return &describeAutoScalingInstancesResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeAutoScalingInstancesResult{
			AutoScalingInstances: xmlInstanceDetailsList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-instances"},
	}, nil
}

// handleOpError translates an operation error into an HTTP response.
func (h *Handler) handleOpError(c *echo.Context, action string, opErr error) error {
	statusCode := http.StatusBadRequest
	code := autoscalingErrorCode(opErr)

	if code == "" {
		code = "InternalFailure"
		statusCode = http.StatusInternalServerError
		logger.Load(c.Request().Context()).Error("autoscaling internal error", "error", opErr, "action", action)
	}

	return h.writeError(c, statusCode, code, opErr.Error())
}

func autoscalingErrorCode(opErr error) string {
	type errorMapping struct {
		sentinel error
		code     string
	}

	mappings := []errorMapping{
		{ErrGroupNotFound, errValidationError},
		{ErrGroupAlreadyExists, "AlreadyExists"},
		{ErrLaunchConfigurationNotFound, errValidationError},
		{ErrLaunchConfigurationAlreadyExists, "AlreadyExists"},
		{ErrInvalidParameter, errValidationError},
		{ErrUnknownAction, "InvalidAction"},
		{ErrActiveInstanceRefreshNotFound, "ActiveInstanceRefreshNotFound"},
		{ErrLifecycleHookNotFound, errValidationError},
		{ErrScalingActivityInProgress, "ScalingActivityInProgress"},
		{ErrInstanceNotFound, errValidationError},
		{ErrWarmPoolNotFound, errValidationError},
		{ErrPolicyNotFound, errValidationError},
	}

	for _, m := range mappings {
		if errors.Is(opErr, m.sentinel) {
			return m.code
		}
	}

	return ""
}

func (h *Handler) writeError(c *echo.Context, statusCode int, code, message string) error {
	errResp := &autoscalingErrorResponse{
		Xmlns:     autoscalingXMLNS,
		Error:     autoscalingError{Code: code, Message: message, Type: "Sender"},
		RequestID: "autoscaling-error",
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

// --- helper functions ---

// paginateItems applies cursor-based pagination over a slice of string-keyed items.
// nameOf returns the name used for the cursor key from each item.
// Returns the page slice and the next-page token (empty string if last page).
// parseIntVal parses a string to int32. Empty string returns 0, nil.
func parseIntVal(s string) (int32, error) {
	if s == "" {
		return 0, nil
	}

	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0, err
	}

	return int32(n), nil
}

// parseMembers extracts indexed form values with the given prefix (e.g. "AvailabilityZones.member").
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

// parseTags extracts tags from the form values using the standard AWS Tags.member.N.Key/Value pattern.
// PropagateAtLaunch defaults to true when omitted (AWS behavior).
func parseTags(vals url.Values, prefix string) []Tag {
	result := make([]Tag, 0)

	for i := 1; ; i++ {
		keyParam := fmt.Sprintf("%s.%d.Key", prefix, i)
		k := vals.Get(keyParam)

		if k == "" {
			break
		}

		propagate := true
		if v := vals.Get(fmt.Sprintf("%s.%d.PropagateAtLaunch", prefix, i)); v != "" {
			propagate = v == formValueTrue
		}

		result = append(result, Tag{
			Key:               k,
			Value:             vals.Get(fmt.Sprintf("%s.%d.Value", prefix, i)),
			PropagateAtLaunch: propagate,
		})
	}

	return result
}

// toXMLGroup converts an AutoScalingGroup to the XML response type.
func toXMLGroup(g *AutoScalingGroup) xmlAutoScalingGroup { //nolint:funlen // XML projection inherently maps many fields
	azs := make([]xmlStringValue, 0, len(g.AvailabilityZones))
	for _, az := range g.AvailabilityZones {
		azs = append(azs, xmlStringValue{Value: az})
	}

	lbNames := make([]xmlStringValue, 0, len(g.LoadBalancerNames))
	for _, lb := range g.LoadBalancerNames {
		lbNames = append(lbNames, xmlStringValue{Value: lb})
	}

	tgARNs := make([]xmlStringValue, 0, len(g.TargetGroupARNs))
	for _, tg := range g.TargetGroupARNs {
		tgARNs = append(tgARNs, xmlStringValue{Value: tg})
	}

	tags := make([]xmlTag, 0, len(g.Tags))
	for _, t := range g.Tags {
		tags = append(tags, xmlTag{
			Key:               t.Key,
			Value:             t.Value,
			ResourceID:        g.AutoScalingGroupName,
			ResourceType:      resourceTypeAutoScalingGroup,
			PropagateAtLaunch: t.PropagateAtLaunch,
		})
	}

	instances := make([]xmlInstance, 0, len(g.Instances))
	for _, inst := range g.Instances {
		instances = append(instances, xmlInstance{
			InstanceID:              inst.InstanceID,
			AvailabilityZone:        inst.AvailabilityZone,
			LifecycleState:          inst.LifecycleState,
			HealthStatus:            inst.HealthStatus,
			InstanceType:            inst.InstanceType,
			LaunchConfigurationName: inst.LaunchConfigurationName,
			ProtectedFromScaleIn:    inst.ProtectedFromScaleIn,
		})
	}

	suspendedProcesses := make([]xmlSuspendedProcess, 0, len(g.SuspendedProcesses))
	for _, p := range g.SuspendedProcesses {
		suspendedProcesses = append(suspendedProcesses, xmlSuspendedProcess{ProcessName: p})
	}

	trafficSources := make([]xmlTrafficSource, 0, len(g.TrafficSources))
	for _, ts := range g.TrafficSources {
		trafficSources = append(trafficSources, xmlTrafficSource(ts))
	}

	terminationPolicies := make([]xmlStringValue, 0, len(g.TerminationPolicies))
	for _, tp := range g.TerminationPolicies {
		terminationPolicies = append(terminationPolicies, xmlStringValue{Value: tp})
	}

	enabledMetrics := make([]xmlEnabledMetric, 0, len(g.EnabledMetrics))
	for _, m := range g.EnabledMetrics {
		enabledMetrics = append(enabledMetrics, xmlEnabledMetric{Metric: m, Granularity: granularity1Minute})
	}

	var xmlLT *xmlLaunchTemplateSpecification
	if g.LaunchTemplate != nil {
		xmlLT = &xmlLaunchTemplateSpecification{
			LaunchTemplateID:   g.LaunchTemplate.LaunchTemplateID,
			LaunchTemplateName: g.LaunchTemplate.LaunchTemplateName,
			Version:            g.LaunchTemplate.Version,
		}
	}

	return xmlAutoScalingGroup{
		AutoScalingGroupName:             g.AutoScalingGroupName,
		AutoScalingGroupARN:              g.AutoScalingGroupARN,
		LaunchConfigurationName:          g.LaunchConfigurationName,
		LaunchTemplate:                   xmlLT,
		VPCZoneIdentifier:                g.VPCZoneIdentifier,
		PlacementGroup:                   g.PlacementGroup,
		Context:                          g.Context,
		DesiredCapacityType:              g.DesiredCapacityType,
		MinSize:                          g.MinSize,
		MaxSize:                          g.MaxSize,
		DesiredCapacity:                  g.DesiredCapacity,
		DefaultCooldown:                  g.DefaultCooldown,
		HealthCheckType:                  g.HealthCheckType,
		HealthCheckGracePeriod:           g.HealthCheckGracePeriod,
		MaxInstanceLifetime:              g.MaxInstanceLifetime,
		DefaultInstanceWarmup:            g.DefaultInstanceWarmup,
		NewInstancesProtectedFromScaleIn: g.NewInstancesProtectedFromScaleIn,
		CapacityRebalance:                g.CapacityRebalance,
		CreatedTime:                      g.CreatedTime.UTC().Format(time.RFC3339),
		Status:                           g.Status,
		AvailabilityZones:                xmlStringValueList{Members: azs},
		LoadBalancerNames:                xmlStringValueList{Members: lbNames},
		TargetGroupARNs:                  xmlStringValueList{Members: tgARNs},
		TrafficSources:                   xmlTrafficSourceList{Members: trafficSources},
		Tags:                             xmlTagList{Members: tags},
		Instances:                        xmlInstanceList{Members: instances},
		SuspendedProcesses:               xmlSuspendedProcessList{Members: suspendedProcesses},
		TerminationPolicies:              xmlTerminationPoliciesList{Members: terminationPolicies},
		EnabledMetrics:                   xmlEnabledMetricList{Members: enabledMetrics},
		ServiceLinkedRoleARN: fmt.Sprintf(
			"arn:aws:iam::%s:role/aws-service-role/autoscaling.amazonaws.com/AWSServiceRoleForAutoScaling",
			config.DefaultAccountID,
		),
	}
}

// toXMLLaunchConfiguration converts a LaunchConfiguration to the XML response type.
func toXMLLaunchConfiguration(lc *LaunchConfiguration) xmlLaunchConfiguration {
	sgs := make([]xmlStringValue, 0, len(lc.SecurityGroups))
	for _, sg := range lc.SecurityGroups {
		sgs = append(sgs, xmlStringValue{Value: sg})
	}

	clSGs := make([]xmlStringValue, 0, len(lc.ClassicLinkVPCSecurityGroups))
	for _, sg := range lc.ClassicLinkVPCSecurityGroups {
		clSGs = append(clSGs, xmlStringValue{Value: sg})
	}

	bdms := make([]xmlBlockDeviceMapping, 0, len(lc.BlockDeviceMappings))
	for _, bdm := range lc.BlockDeviceMappings {
		xmlBDM := xmlBlockDeviceMapping{
			DeviceName:  bdm.DeviceName,
			VirtualName: bdm.VirtualName,
			NoDevice:    bdm.NoDevice,
		}

		if bdm.Ebs != nil {
			xmlBDM.Ebs = &xmlEbsBlockDevice{
				SnapshotID:          bdm.Ebs.SnapshotID,
				VolumeType:          bdm.Ebs.VolumeType,
				KmsKeyID:            bdm.Ebs.KmsKeyID,
				VolumeSize:          bdm.Ebs.VolumeSize,
				Iops:                bdm.Ebs.Iops,
				Throughput:          bdm.Ebs.Throughput,
				DeleteOnTermination: bdm.Ebs.DeleteOnTermination,
				Encrypted:           bdm.Ebs.Encrypted,
			}
		}

		bdms = append(bdms, xmlBDM)
	}

	return xmlLaunchConfiguration{
		LaunchConfigurationName:      lc.LaunchConfigurationName,
		LaunchConfigurationARN:       lc.LaunchConfigurationARN,
		ImageID:                      lc.ImageID,
		InstanceType:                 lc.InstanceType,
		KeyName:                      lc.KeyName,
		IAMInstanceProfile:           lc.IAMInstanceProfile,
		UserData:                     lc.UserData,
		KernelID:                     lc.KernelID,
		RamdiskID:                    lc.RamdiskID,
		SpotPrice:                    lc.SpotPrice,
		PlacementTenancy:             lc.PlacementTenancy,
		ClassicLinkVPCID:             lc.ClassicLinkVPCID,
		CreatedTime:                  lc.CreatedTime.UTC().Format(time.RFC3339),
		SecurityGroups:               xmlStringValueList{Members: sgs},
		ClassicLinkVPCSecurityGroups: xmlStringValueList{Members: clSGs},
		BlockDeviceMappings:          xmlBlockDeviceMappingList{Members: bdms},
		InstanceMonitoring:           xmlInstanceMonitoring{Enabled: lc.InstanceMonitoring},
		AssociatePublicIPAddress:     lc.AssociatePublicIPAddress,
		EbsOptimized:                 lc.EbsOptimized,
	}
}

// toXMLScalingActivity converts a ScalingActivity to the XML response type.
func toXMLScalingActivity(a *ScalingActivity) xmlScalingActivity {
	endTime := ""
	if !a.EndTime.IsZero() {
		endTime = a.EndTime.UTC().Format(time.RFC3339)
	}

	return xmlScalingActivity{
		ActivityID:           a.ActivityID,
		AutoScalingGroupName: a.AutoScalingGroupName,
		Description:          a.Description,
		StatusCode:           a.StatusCode,
		StatusMessage:        a.StatusMessage,
		Progress:             a.Progress,
		StartTime:            a.StartTime.UTC().Format(time.RFC3339),
		EndTime:              endTime,
	}
}

// --- XML response types ---

type xmlResponseMetadata struct {
	RequestID string `xml:"RequestId"`
}

type autoscalingError struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
	Type    string `xml:"Type"`
}

type autoscalingErrorResponse struct {
	XMLName   xml.Name         `xml:"ErrorResponse"`
	Xmlns     string           `xml:"xmlns,attr"`
	Error     autoscalingError `xml:"Error"`
	RequestID string           `xml:"RequestId"`
}

type createAutoScalingGroupResponse struct {
	XMLName          xml.Name            `xml:"CreateAutoScalingGroupResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type updateAutoScalingGroupResponse struct {
	XMLName          xml.Name            `xml:"UpdateAutoScalingGroupResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type deleteAutoScalingGroupResponse struct {
	XMLName          xml.Name            `xml:"DeleteAutoScalingGroupResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type xmlStringValue struct {
	Value string `xml:",chardata"`
}

type xmlStringValueList struct {
	Members []xmlStringValue `xml:"member"`
}

type xmlTag struct {
	Key               string `xml:"Key"`
	Value             string `xml:"Value"`
	ResourceID        string `xml:"ResourceId,omitempty"`
	ResourceType      string `xml:"ResourceType,omitempty"`
	PropagateAtLaunch bool   `xml:"PropagateAtLaunch,omitempty"`
}

type xmlTagList struct {
	Members []xmlTag `xml:"member"`
}

type xmlInstance struct {
	InstanceID              string `xml:"InstanceId"`
	AvailabilityZone        string `xml:"AvailabilityZone"`
	LifecycleState          string `xml:"LifecycleState"`
	HealthStatus            string `xml:"HealthStatus"`
	InstanceType            string `xml:"InstanceType,omitempty"`
	LaunchConfigurationName string `xml:"LaunchConfigurationName,omitempty"`
	ProtectedFromScaleIn    bool   `xml:"ProtectedFromScaleIn,omitempty"`
}

type xmlInstanceList struct {
	Members []xmlInstance `xml:"member"`
}

type xmlSuspendedProcess struct {
	ProcessName      string `xml:"ProcessName"`
	SuspensionReason string `xml:"SuspensionReason,omitempty"`
}

type xmlSuspendedProcessList struct {
	Members []xmlSuspendedProcess `xml:"member"`
}

type xmlEnabledMetric struct {
	Metric      string `xml:"Metric"`
	Granularity string `xml:"Granularity"`
}

type xmlEnabledMetricList struct {
	Members []xmlEnabledMetric `xml:"member"`
}

type xmlLaunchTemplateSpecification struct {
	LaunchTemplateID   string `xml:"LaunchTemplateId,omitempty"`
	LaunchTemplateName string `xml:"LaunchTemplateName,omitempty"`
	Version            string `xml:"Version,omitempty"`
}

type xmlTrafficSource struct {
	Identifier string `xml:"Identifier"`
	Type       string `xml:"Type,omitempty"`
}

type xmlTrafficSourceList struct {
	Members []xmlTrafficSource `xml:"member"`
}

type xmlTerminationPoliciesList struct {
	Members []xmlStringValue `xml:"member"`
}

type xmlAutoScalingGroup struct {
	LaunchTemplate                   *xmlLaunchTemplateSpecification `xml:"LaunchTemplate,omitempty"`
	AutoScalingGroupARN              string                          `xml:"AutoScalingGroupARN"`
	Status                           string                          `xml:"Status,omitempty"`
	CreatedTime                      string                          `xml:"CreatedTime"`
	HealthCheckType                  string                          `xml:"HealthCheckType"`
	LaunchConfigurationName          string                          `xml:"LaunchConfigurationName,omitempty"`
	AutoScalingGroupName             string                          `xml:"AutoScalingGroupName"`
	VPCZoneIdentifier                string                          `xml:"VPCZoneIdentifier,omitempty"`
	PlacementGroup                   string                          `xml:"PlacementGroup,omitempty"`
	Context                          string                          `xml:"Context,omitempty"`
	DesiredCapacityType              string                          `xml:"DesiredCapacityType,omitempty"`
	ServiceLinkedRoleARN             string                          `xml:"ServiceLinkedRoleARN,omitempty"`
	TargetGroupARNs                  xmlStringValueList              `xml:"TargetGroupARNs"`
	Tags                             xmlTagList                      `xml:"Tags"`
	AvailabilityZones                xmlStringValueList              `xml:"AvailabilityZones"`
	LoadBalancerNames                xmlStringValueList              `xml:"LoadBalancerNames"`
	TrafficSources                   xmlTrafficSourceList            `xml:"TrafficSources"`
	SuspendedProcesses               xmlSuspendedProcessList         `xml:"SuspendedProcesses"`
	TerminationPolicies              xmlTerminationPoliciesList      `xml:"TerminationPolicies"`
	EnabledMetrics                   xmlEnabledMetricList            `xml:"EnabledMetrics"`
	Instances                        xmlInstanceList                 `xml:"Instances"`
	MaxSize                          int32                           `xml:"MaxSize"`
	DesiredCapacity                  int32                           `xml:"DesiredCapacity"`
	DefaultCooldown                  int32                           `xml:"DefaultCooldown"`
	HealthCheckGracePeriod           int32                           `xml:"HealthCheckGracePeriod"`
	MaxInstanceLifetime              int32                           `xml:"MaxInstanceLifetime,omitempty"`
	DefaultInstanceWarmup            int32                           `xml:"DefaultInstanceWarmup,omitempty"`
	MinSize                          int32                           `xml:"MinSize"`
	NewInstancesProtectedFromScaleIn bool                            `xml:"NewInstancesProtectedFromScaleIn,omitempty"`
	CapacityRebalance                bool                            `xml:"CapacityRebalance,omitempty"`
}

type xmlAutoScalingGroupList struct {
	Members []xmlAutoScalingGroup `xml:"member"`
}

type describeAutoScalingGroupsResult struct {
	NextToken         string                  `xml:"NextToken,omitempty"`
	AutoScalingGroups xmlAutoScalingGroupList `xml:"AutoScalingGroups"`
}

type describeAutoScalingGroupsResponse struct {
	XMLName          xml.Name                        `xml:"DescribeAutoScalingGroupsResponse"`
	Xmlns            string                          `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata             `xml:"ResponseMetadata"`
	Result           describeAutoScalingGroupsResult `xml:"DescribeAutoScalingGroupsResult"`
}

type xmlEbsBlockDevice struct {
	SnapshotID          string `xml:"SnapshotId,omitempty"`
	VolumeType          string `xml:"VolumeType,omitempty"`
	KmsKeyID            string `xml:"KmsKeyId,omitempty"`
	VolumeSize          int32  `xml:"VolumeSize,omitempty"`
	Iops                int32  `xml:"Iops,omitempty"`
	Throughput          int32  `xml:"Throughput,omitempty"`
	DeleteOnTermination bool   `xml:"DeleteOnTermination,omitempty"`
	Encrypted           bool   `xml:"Encrypted,omitempty"`
}

type xmlBlockDeviceMapping struct {
	Ebs         *xmlEbsBlockDevice `xml:"Ebs,omitempty"`
	DeviceName  string             `xml:"DeviceName"`
	VirtualName string             `xml:"VirtualName,omitempty"`
	NoDevice    string             `xml:"NoDevice,omitempty"`
}

type xmlBlockDeviceMappingList struct {
	Members []xmlBlockDeviceMapping `xml:"member"`
}

type xmlInstanceMonitoring struct {
	Enabled bool `xml:"Enabled"`
}

type xmlLaunchConfiguration struct {
	LaunchConfigurationName      string                    `xml:"LaunchConfigurationName"`
	LaunchConfigurationARN       string                    `xml:"LaunchConfigurationARN"`
	ImageID                      string                    `xml:"ImageId"`
	InstanceType                 string                    `xml:"InstanceType"`
	KeyName                      string                    `xml:"KeyName,omitempty"`
	IAMInstanceProfile           string                    `xml:"IamInstanceProfile,omitempty"`
	UserData                     string                    `xml:"UserData,omitempty"`
	KernelID                     string                    `xml:"KernelId,omitempty"`
	RamdiskID                    string                    `xml:"RamdiskId,omitempty"`
	SpotPrice                    string                    `xml:"SpotPrice,omitempty"`
	PlacementTenancy             string                    `xml:"PlacementTenancy,omitempty"`
	ClassicLinkVPCID             string                    `xml:"ClassicLinkVPCId,omitempty"`
	CreatedTime                  string                    `xml:"CreatedTime"`
	SecurityGroups               xmlStringValueList        `xml:"SecurityGroups"`
	ClassicLinkVPCSecurityGroups xmlStringValueList        `xml:"ClassicLinkVPCSecurityGroups,omitempty"`
	BlockDeviceMappings          xmlBlockDeviceMappingList `xml:"BlockDeviceMappings,omitempty"`
	InstanceMonitoring           xmlInstanceMonitoring     `xml:"InstanceMonitoring"`
	AssociatePublicIPAddress     bool                      `xml:"AssociatePublicIpAddress,omitempty"`
	EbsOptimized                 bool                      `xml:"EbsOptimized,omitempty"`
}

type xmlLaunchConfigurationList struct {
	Members []xmlLaunchConfiguration `xml:"member"`
}

type describeLaunchConfigurationsResult struct {
	NextToken            string                     `xml:"NextToken,omitempty"`
	LaunchConfigurations xmlLaunchConfigurationList `xml:"LaunchConfigurations"`
}

type createLaunchConfigurationResponse struct {
	XMLName          xml.Name            `xml:"CreateLaunchConfigurationResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type describeLaunchConfigurationsResponse struct {
	XMLName          xml.Name                           `xml:"DescribeLaunchConfigurationsResponse"`
	Xmlns            string                             `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                `xml:"ResponseMetadata"`
	Result           describeLaunchConfigurationsResult `xml:"DescribeLaunchConfigurationsResult"`
}

type deleteLaunchConfigurationResponse struct {
	XMLName          xml.Name            `xml:"DeleteLaunchConfigurationResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type xmlScalingActivity struct {
	ActivityID           string `xml:"ActivityId"`
	AutoScalingGroupName string `xml:"AutoScalingGroupName"`
	Description          string `xml:"Description,omitempty"`
	StatusCode           string `xml:"StatusCode"`
	StatusMessage        string `xml:"StatusMessage,omitempty"`
	StartTime            string `xml:"StartTime"`
	EndTime              string `xml:"EndTime,omitempty"`
	Progress             int32  `xml:"Progress"`
}

type xmlScalingActivityList struct {
	Members []xmlScalingActivity `xml:"member"`
}

type describeScalingActivitiesResult struct {
	NextToken  string                 `xml:"NextToken,omitempty"`
	Activities xmlScalingActivityList `xml:"Activities"`
}

type describeScalingActivitiesResponse struct {
	XMLName          xml.Name                        `xml:"DescribeScalingActivitiesResponse"`
	Xmlns            string                          `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata             `xml:"ResponseMetadata"`
	Result           describeScalingActivitiesResult `xml:"DescribeScalingActivitiesResult"`
}

// parseLaunchTemplate parses a LaunchTemplateSpecification from form values.
// prefix is e.g. "LaunchTemplate" or "MixedInstancesPolicy.LaunchTemplate.LaunchTemplateSpecification".
// Returns nil if no LaunchTemplateId or LaunchTemplateName found.
func parseLaunchTemplate(vals url.Values, prefix string) *LaunchTemplateSpecification {
	id := vals.Get(prefix + ".LaunchTemplateId")
	name := vals.Get(prefix + ".LaunchTemplateName")
	version := vals.Get(prefix + ".Version")

	if id == "" && name == "" {
		return nil
	}

	return &LaunchTemplateSpecification{
		LaunchTemplateID:   id,
		LaunchTemplateName: name,
		Version:            version,
	}
}

// parseBlockDeviceMappings parses BlockDeviceMappings from form values.
//
//nolint:gocognit,nestif // Too complex to refactor given time constraints
func parseBlockDeviceMappings(vals url.Values) []BlockDeviceMapping {
	result := make([]BlockDeviceMapping, 0)

	for i := 1; ; i++ {
		deviceKey := fmt.Sprintf("BlockDeviceMappings.member.%d.DeviceName", i)
		deviceName := vals.Get(deviceKey)

		if deviceName == "" {
			break
		}

		bdm := BlockDeviceMapping{
			DeviceName:  deviceName,
			VirtualName: vals.Get(fmt.Sprintf("BlockDeviceMappings.member.%d.VirtualName", i)),
			NoDevice:    vals.Get(fmt.Sprintf("BlockDeviceMappings.member.%d.NoDevice", i)),
		}

		snapshotID := vals.Get(fmt.Sprintf("BlockDeviceMappings.member.%d.Ebs.SnapshotId", i))
		volumeType := vals.Get(fmt.Sprintf("BlockDeviceMappings.member.%d.Ebs.VolumeType", i))
		kmsKeyID := vals.Get(fmt.Sprintf("BlockDeviceMappings.member.%d.Ebs.KmsKeyId", i))

		if snapshotID != "" || volumeType != "" || kmsKeyID != "" ||
			vals.Get(fmt.Sprintf("BlockDeviceMappings.member.%d.Ebs.VolumeSize", i)) != "" {
			ebs := &EbsBlockDevice{
				SnapshotID: snapshotID,
				VolumeType: volumeType,
				KmsKeyID:   kmsKeyID,
				DeleteOnTermination: vals.Get(
					fmt.Sprintf("BlockDeviceMappings.member.%d.Ebs.DeleteOnTermination", i),
				) != "false",
				Encrypted: vals.Get(
					fmt.Sprintf("BlockDeviceMappings.member.%d.Ebs.Encrypted", i),
				) == formValueTrue,
			}

			if v := vals.Get(fmt.Sprintf("BlockDeviceMappings.member.%d.Ebs.VolumeSize", i)); v != "" {
				if n, parseErr := parseIntVal(v); parseErr == nil {
					ebs.VolumeSize = n
				}
			}

			if v := vals.Get(fmt.Sprintf("BlockDeviceMappings.member.%d.Ebs.Iops", i)); v != "" {
				if n, parseErr := parseIntVal(v); parseErr == nil {
					ebs.Iops = n
				}
			}

			if v := vals.Get(fmt.Sprintf("BlockDeviceMappings.member.%d.Ebs.Throughput", i)); v != "" {
				if n, parseErr := parseIntVal(v); parseErr == nil {
					ebs.Throughput = n
				}
			}

			bdm.Ebs = ebs
		}

		result = append(result, bdm)
	}

	return result
}

// parseTrafficSources parses TrafficSources from form values using the standard AWS pattern.
func parseTrafficSources(vals url.Values) []TrafficSource {
	result := make([]TrafficSource, 0)

	for i := 1; ; i++ {
		idKey := fmt.Sprintf("TrafficSources.member.%d.Identifier", i)
		typeKey := fmt.Sprintf("TrafficSources.member.%d.Type", i)
		id := vals.Get(idKey)

		if id == "" {
			break
		}

		result = append(result, TrafficSource{
			Identifier: id,
			Type:       vals.Get(typeKey),
		})
	}

	return result
}

// parseResourceTags parses resource-scoped tags from form values.
func parseResourceTags(vals url.Values, prefix string) []ResourceTag {
	result := make([]ResourceTag, 0)

	for i := 1; ; i++ {
		keyParam := fmt.Sprintf("%s.%d.Key", prefix, i)
		k := vals.Get(keyParam)

		if k == "" {
			break
		}

		propagate := true
		if v := vals.Get(fmt.Sprintf("%s.%d.PropagateAtLaunch", prefix, i)); v != "" {
			propagate = v == formValueTrue
		}

		result = append(result, ResourceTag{
			ResourceID:        vals.Get(fmt.Sprintf("%s.%d.ResourceId", prefix, i)),
			ResourceType:      vals.Get(fmt.Sprintf("%s.%d.ResourceType", prefix, i)),
			Key:               k,
			Value:             vals.Get(fmt.Sprintf("%s.%d.Value", prefix, i)),
			PropagateAtLaunch: propagate,
		})
	}

	return result
}

// parseBatchScheduledActions parses ScheduledUpdateGroupAction entries from form values.
func parseBatchScheduledActions(vals url.Values) []ScheduledUpdateGroupAction {
	result := make([]ScheduledUpdateGroupAction, 0)

	prefix := "ScheduledUpdateGroupActions.member"

	for i := 1; ; i++ {
		nameKey := fmt.Sprintf("%s.%d.ScheduledActionName", prefix, i)
		name := vals.Get(nameKey)

		if name == "" {
			break
		}

		action := ScheduledUpdateGroupAction{
			ScheduledActionName: name,
			Recurrence:          vals.Get(fmt.Sprintf("%s.%d.Recurrence", prefix, i)),
			TimeZone:            vals.Get(fmt.Sprintf("%s.%d.TimeZone", prefix, i)),
		}

		if v := vals.Get(fmt.Sprintf("%s.%d.DesiredCapacity", prefix, i)); v != "" {
			if n, err := parseIntVal(v); err == nil {
				action.DesiredCapacity = &n
			}
		}

		if v := vals.Get(fmt.Sprintf("%s.%d.MinSize", prefix, i)); v != "" {
			if n, err := parseIntVal(v); err == nil {
				action.MinSize = &n
			}
		}

		if v := vals.Get(fmt.Sprintf("%s.%d.MaxSize", prefix, i)); v != "" {
			if n, err := parseIntVal(v); err == nil {
				action.MaxSize = &n
			}
		}

		result = append(result, action)
	}

	return result
}

// --- new XML response types ---

type attachInstancesResponse struct {
	XMLName          xml.Name            `xml:"AttachInstancesResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type attachLoadBalancerTargetGroupsResponse struct {
	XMLName          xml.Name            `xml:"AttachLoadBalancerTargetGroupsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type attachLoadBalancersResponse struct {
	XMLName          xml.Name            `xml:"AttachLoadBalancersResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type attachTrafficSourcesResponse struct {
	XMLName          xml.Name            `xml:"AttachTrafficSourcesResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type xmlFailedScheduledAction struct {
	ScheduledActionName string `xml:"ScheduledActionName"`
	ErrorCode           string `xml:"ErrorCode"`
	ErrorMessage        string `xml:"ErrorMessage"`
}

type xmlFailedScheduledActionList struct {
	Members []xmlFailedScheduledAction `xml:"member"`
}

type batchDeleteScheduledActionResult struct {
	FailedScheduledActions xmlFailedScheduledActionList `xml:"FailedScheduledActions"`
}

type batchDeleteScheduledActionResponse struct {
	XMLName          xml.Name                         `xml:"BatchDeleteScheduledActionResponse"`
	Xmlns            string                           `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata              `xml:"ResponseMetadata"`
	Result           batchDeleteScheduledActionResult `xml:"BatchDeleteScheduledActionResult"`
}

type batchPutScheduledUpdateGroupActionResult struct {
	FailedScheduledUpdateGroupActions xmlFailedScheduledActionList `xml:"FailedScheduledUpdateGroupActions"`
}

type batchPutScheduledUpdateGroupActionResponse struct {
	XMLName          xml.Name                                 `xml:"BatchPutScheduledUpdateGroupActionResponse"`
	Xmlns            string                                   `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                      `xml:"ResponseMetadata"`
	Result           batchPutScheduledUpdateGroupActionResult `xml:"BatchPutScheduledUpdateGroupActionResult"`
}

type cancelInstanceRefreshResult struct {
	InstanceRefreshID string `xml:"InstanceRefreshId"`
}

type cancelInstanceRefreshResponse struct {
	XMLName          xml.Name                    `xml:"CancelInstanceRefreshResponse"`
	Xmlns            string                      `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata         `xml:"ResponseMetadata"`
	Result           cancelInstanceRefreshResult `xml:"CancelInstanceRefreshResult"`
}

type completeLifecycleActionResponse struct {
	XMLName          xml.Name            `xml:"CompleteLifecycleActionResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type createOrUpdateTagsResponse struct {
	XMLName          xml.Name            `xml:"CreateOrUpdateTagsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type deleteLifecycleHookResponse struct {
	XMLName          xml.Name            `xml:"DeleteLifecycleHookResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type setDesiredCapacityResponse struct {
	XMLName          xml.Name            `xml:"SetDesiredCapacityResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type terminateInstanceResult struct {
	Activity xmlScalingActivity `xml:"Activity"`
}

type terminateInstanceInAutoScalingGroupResponse struct {
	XMLName          xml.Name                `xml:"TerminateInstanceInAutoScalingGroupResponse"`
	Xmlns            string                  `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata     `xml:"ResponseMetadata"`
	Result           terminateInstanceResult `xml:"TerminateInstanceInAutoScalingGroupResult"`
}

type putLifecycleHookResponse struct {
	XMLName          xml.Name            `xml:"PutLifecycleHookResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type xmlLifecycleHook struct {
	LifecycleHookName     string `xml:"LifecycleHookName"`
	AutoScalingGroupName  string `xml:"AutoScalingGroupName"`
	LifecycleTransition   string `xml:"LifecycleTransition,omitempty"`
	DefaultResult         string `xml:"DefaultResult,omitempty"`
	NotificationTargetARN string `xml:"NotificationTargetARN,omitempty"`
	NotificationMetadata  string `xml:"NotificationMetadata,omitempty"`
	RoleARN               string `xml:"RoleARN,omitempty"`
	HeartbeatTimeout      int32  `xml:"HeartbeatTimeout,omitempty"`
	GlobalTimeout         int32  `xml:"GlobalTimeout,omitempty"`
}

type xmlLifecycleHookList struct {
	Members []xmlLifecycleHook `xml:"member"`
}

type describeLifecycleHooksResult struct {
	LifecycleHooks xmlLifecycleHookList `xml:"LifecycleHooks"`
}

type describeLifecycleHooksResponse struct {
	XMLName          xml.Name                     `xml:"DescribeLifecycleHooksResponse"`
	Xmlns            string                       `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata          `xml:"ResponseMetadata"`
	Result           describeLifecycleHooksResult `xml:"DescribeLifecycleHooksResult"`
}

type xmlScheduledAction struct {
	DesiredCapacity      *int32 `xml:"DesiredCapacity,omitempty"`
	MinSize              *int32 `xml:"MinSize,omitempty"`
	MaxSize              *int32 `xml:"MaxSize,omitempty"`
	ScheduledActionName  string `xml:"ScheduledActionName"`
	ScheduledActionARN   string `xml:"ScheduledActionARN,omitempty"`
	AutoScalingGroupName string `xml:"AutoScalingGroupName,omitempty"`
	Recurrence           string `xml:"Recurrence,omitempty"`
	TimeZone             string `xml:"TimeZone,omitempty"`
	StartTime            string `xml:"StartTime,omitempty"`
	EndTime              string `xml:"EndTime,omitempty"`
}

type xmlScheduledActionList struct {
	Members []xmlScheduledAction `xml:"member"`
}

type describeScheduledActionsResult struct {
	NextToken                   string                 `xml:"NextToken,omitempty"`
	ScheduledUpdateGroupActions xmlScheduledActionList `xml:"ScheduledUpdateGroupActions"`
}

type describeScheduledActionsResponse struct {
	XMLName          xml.Name                       `xml:"DescribeScheduledActionsResponse"`
	Xmlns            string                         `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata            `xml:"ResponseMetadata"`
	Result           describeScheduledActionsResult `xml:"DescribeScheduledActionsResult"`
}

type deleteTagsResponse struct {
	XMLName          xml.Name            `xml:"DeleteTagsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type xmlResourceTag struct {
	ResourceID        string `xml:"ResourceId"`
	ResourceType      string `xml:"ResourceType"`
	Key               string `xml:"Key"`
	Value             string `xml:"Value,omitempty"`
	PropagateAtLaunch bool   `xml:"PropagateAtLaunch,omitempty"`
}

type xmlResourceTagList struct {
	Members []xmlResourceTag `xml:"member"`
}

type describeTagsResult struct {
	NextToken string             `xml:"NextToken,omitempty"`
	Tags      xmlResourceTagList `xml:"Tags"`
}

type describeTagsResponse struct {
	XMLName          xml.Name            `xml:"DescribeTagsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
	Result           describeTagsResult  `xml:"DescribeTagsResult"`
}

type xmlInstanceDetails struct {
	InstanceID              string `xml:"InstanceId"`
	AutoScalingGroupName    string `xml:"AutoScalingGroupName"`
	AvailabilityZone        string `xml:"AvailabilityZone"`
	LifecycleState          string `xml:"LifecycleState"`
	HealthStatus            string `xml:"HealthStatus"`
	LaunchConfigurationName string `xml:"LaunchConfigurationName,omitempty"`
	InstanceType            string `xml:"InstanceType,omitempty"`
	ProtectedFromScaleIn    bool   `xml:"ProtectedFromScaleIn"`
}

type xmlInstanceDetailsList struct {
	Members []xmlInstanceDetails `xml:"member"`
}

type describeAutoScalingInstancesResult struct {
	NextToken            string                 `xml:"NextToken,omitempty"`
	AutoScalingInstances xmlInstanceDetailsList `xml:"AutoScalingInstances"`
}

type describeAutoScalingInstancesResponse struct {
	XMLName          xml.Name                           `xml:"DescribeAutoScalingInstancesResponse"`
	Xmlns            string                             `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                `xml:"ResponseMetadata"`
	Result           describeAutoScalingInstancesResult `xml:"DescribeAutoScalingInstancesResult"`
}

// parseTagFilters parses Filters from form values used in DescribeTags.
func parseTagFilters(vals url.Values) []TagFilter {
	var filters []TagFilter

	for i := 1; ; i++ {
		nameKey := fmt.Sprintf("Filters.member.%d.Name", i)
		name := vals.Get(nameKey)

		if name == "" {
			break
		}

		var values []string

		for j := 1; ; j++ {
			valKey := fmt.Sprintf("Filters.member.%d.Values.member.%d", i, j)
			v := vals.Get(valKey)

			if v == "" {
				break
			}

			values = append(values, v)
		}

		filters = append(filters, TagFilter{Name: name, Values: values})
	}

	return filters
}

// Purge implements service.Purgeable by removing all Auto Scaling resources older than cutoff.
func (h *Handler) Purge(ctx context.Context, cutoff time.Time) {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.Purge(ctx, cutoff)
	}
}

// --- New handler implementations ---

func (h *Handler) handleDescribeAccountLimits(_ url.Values) (any, error) {
	limits, err := h.Backend.DescribeAccountLimits()
	if err != nil {
		return nil, err
	}

	return &describeAccountLimitsResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeAccountLimitsResult{
			MaxNumberOfAutoScalingGroups:    limits.MaxNumberOfAutoScalingGroups,
			MaxNumberOfLaunchConfigurations: limits.MaxNumberOfLaunchConfigurations,
			NumberOfAutoScalingGroups:       limits.NumberOfAutoScalingGroups,
			NumberOfLaunchConfigurations:    limits.NumberOfLaunchConfigurations,
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-account-limits"},
	}, nil
}

func (h *Handler) handleDescribeAdjustmentTypes(_ url.Values) (any, error) {
	types, err := h.Backend.DescribeAdjustmentTypes()
	if err != nil {
		return nil, err
	}

	members := make([]xmlAdjustmentType, 0, len(types))
	for _, t := range types {
		members = append(members, xmlAdjustmentType{AdjustmentType: t})
	}

	return &describeAdjustmentTypesResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeAdjustmentTypesResult{
			AdjustmentTypes: xmlAdjustmentTypeList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-adjustment-types"},
	}, nil
}

func (h *Handler) handleDescribeAutoScalingNotificationTypes(_ url.Values) (any, error) {
	types, err := h.Backend.DescribeAutoScalingNotificationTypes()
	if err != nil {
		return nil, err
	}

	members := make([]xmlStringValue, 0, len(types))
	for _, t := range types {
		members = append(members, xmlStringValue{Value: t})
	}

	return &describeAutoScalingNotificationTypesResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeAutoScalingNotificationTypesResult{
			AutoScalingNotificationTypes: xmlStringValueList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-notification-types"},
	}, nil
}

func (h *Handler) handleDescribeLifecycleHookTypes(_ url.Values) (any, error) {
	types, err := h.Backend.DescribeLifecycleHookTypes()
	if err != nil {
		return nil, err
	}

	members := make([]xmlStringValue, 0, len(types))
	for _, t := range types {
		members = append(members, xmlStringValue{Value: t})
	}

	return &describeLifecycleHookTypesResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeLifecycleHookTypesResult{
			LifecycleHookTypes: xmlStringValueList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-lifecycle-hook-types"},
	}, nil
}

func (h *Handler) handleDescribeMetricCollectionTypes(_ url.Values) (any, error) {
	metrics, err := h.Backend.DescribeMetricCollectionTypes()
	if err != nil {
		return nil, err
	}

	members := make([]xmlMetricCollectionType, 0, len(metrics))
	for _, m := range metrics {
		members = append(members, xmlMetricCollectionType(m))
	}

	return &describeMetricCollectionTypesResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeMetricCollectionTypesResult{
			Metrics: xmlMetricCollectionTypeList{Members: members},
			Granularities: xmlGranularityList{
				Members: []xmlGranularity{{Granularity: granularity1Minute}},
			},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-metric-collection-types"},
	}, nil
}

func (h *Handler) handleDescribeScalingProcessTypes(_ url.Values) (any, error) {
	types, err := h.Backend.DescribeScalingProcessTypes()
	if err != nil {
		return nil, err
	}

	members := make([]xmlProcessType, 0, len(types))
	for _, t := range types {
		members = append(members, xmlProcessType{ProcessName: t})
	}

	return &describeScalingProcessTypesResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeScalingProcessTypesResult{
			Processes: xmlProcessTypeList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-scaling-process-types"},
	}, nil
}

func (h *Handler) handleDescribeTerminationPolicyTypes(_ url.Values) (any, error) {
	types, err := h.Backend.DescribeTerminationPolicyTypes()
	if err != nil {
		return nil, err
	}

	members := make([]xmlStringValue, 0, len(types))
	for _, t := range types {
		members = append(members, xmlStringValue{Value: t})
	}

	return &describeTerminationPolicyTypesResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeTerminationPolicyTypesResult{
			TerminationPolicyTypes: xmlStringValueList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-termination-policy-types"},
	}, nil
}

func (h *Handler) handleDescribeInstanceRefreshes(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	refreshIDs := parseMembers(vals, "InstanceRefreshIds.member")

	refreshes, err := h.Backend.DescribeInstanceRefreshes(groupName, refreshIDs)
	if err != nil {
		return nil, err
	}

	members := make([]xmlInstanceRefresh, 0, len(refreshes))
	for _, r := range refreshes {
		endTime := ""
		if !r.EndTime.IsZero() {
			endTime = r.EndTime.UTC().Format(time.RFC3339)
		}

		members = append(members, xmlInstanceRefresh{
			InstanceRefreshID:         r.InstanceRefreshID,
			AutoScalingGroupName:      r.AutoScalingGroupName,
			Status:                    r.Status,
			StatusReason:              r.StatusReason,
			StartTime:                 r.StartTime.UTC().Format(time.RFC3339),
			EndTime:                   endTime,
			Strategy:                  r.Strategy,
			PercentageComplete:        r.PercentageComplete,
			InstancesToUpdate:         r.InstancesToUpdate,
			MinHealthyPercentage:      r.Preferences.MinHealthyPercentage,
			MaxHealthyPercentage:      r.Preferences.MaxHealthyPercentage,
			InstanceWarmup:            r.Preferences.InstanceWarmup,
			SkipMatching:              r.Preferences.SkipMatching,
			AutoRollback:              r.Preferences.AutoRollback,
			ScaleInProtectedInstances: r.Preferences.ScaleInProtectedInstances,
			StandbyInstances:          r.Preferences.StandbyInstances,
		})
	}

	return &describeInstanceRefreshesResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeInstanceRefreshesResult{
			InstanceRefreshes: xmlInstanceRefreshList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-instance-refreshes"},
	}, nil
}

func (h *Handler) handleStartInstanceRefresh(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	strategy := vals.Get("Strategy")

	minHealthy, err := parseIntVal(vals.Get("Preferences.MinHealthyPercentage"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid Preferences.MinHealthyPercentage", ErrInvalidParameter)
	}

	maxHealthy, err := parseIntVal(vals.Get("Preferences.MaxHealthyPercentage"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid Preferences.MaxHealthyPercentage", ErrInvalidParameter)
	}

	instanceWarmup, err := parseIntVal(vals.Get("Preferences.InstanceWarmup"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid Preferences.InstanceWarmup", ErrInvalidParameter)
	}

	checkpointDelay, err := parseIntVal(vals.Get("Preferences.CheckpointDelay"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid Preferences.CheckpointDelay", ErrInvalidParameter)
	}

	prefs := InstanceRefreshPreferences{
		MinHealthyPercentage:      minHealthy,
		MaxHealthyPercentage:      maxHealthy,
		InstanceWarmup:            instanceWarmup,
		CheckpointDelay:           checkpointDelay,
		SkipMatching:              vals.Get("Preferences.SkipMatching") == formValueTrue,
		AutoRollback:              vals.Get("Preferences.AutoRollback") == formValueTrue,
		ScaleInProtectedInstances: vals.Get("Preferences.ScaleInProtectedInstances"),
		StandbyInstances:          vals.Get("Preferences.StandbyInstances"),
	}

	refresh, err := h.Backend.StartInstanceRefreshWithInput(StartInstanceRefreshInput{
		AutoScalingGroupName: groupName,
		Strategy:             strategy,
		Preferences:          prefs,
	})
	if err != nil {
		return nil, err
	}

	return &startInstanceRefreshResponse{
		Xmlns: autoscalingXMLNS,
		Result: startInstanceRefreshResult{
			InstanceRefreshID: refresh.InstanceRefreshID,
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-start-instance-refresh"},
	}, nil
}

func (h *Handler) handleRollbackInstanceRefresh(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")

	refreshID, err := h.Backend.RollbackInstanceRefresh(groupName)
	if err != nil {
		return nil, err
	}

	return &rollbackInstanceRefreshResponse{
		Xmlns: autoscalingXMLNS,
		Result: rollbackInstanceRefreshResult{
			InstanceRefreshID: refreshID,
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-rollback-instance-refresh"},
	}, nil
}

func (h *Handler) handleDescribeLoadBalancers(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")

	lbs, err := h.Backend.DescribeLoadBalancers(groupName)
	if err != nil {
		return nil, err
	}

	members := make([]xmlLoadBalancerState, 0, len(lbs))
	for _, lb := range lbs {
		members = append(members, xmlLoadBalancerState(lb))
	}

	return &describeLoadBalancersResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeLoadBalancersResult{
			LoadBalancers: xmlLoadBalancerStateList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-load-balancers"},
	}, nil
}

func (h *Handler) handleDescribeLoadBalancerTargetGroups(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")

	tgs, err := h.Backend.DescribeLoadBalancerTargetGroups(groupName)
	if err != nil {
		return nil, err
	}

	members := make([]xmlLoadBalancerTargetGroupState, 0, len(tgs))
	for _, tg := range tgs {
		members = append(members, xmlLoadBalancerTargetGroupState(tg))
	}

	return &describeLoadBalancerTargetGroupsResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeLoadBalancerTargetGroupsResult{
			LoadBalancerTargetGroups: xmlLoadBalancerTargetGroupStateList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-lb-target-groups"},
	}, nil
}

func (h *Handler) handleDescribeTrafficSources(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")

	sources, err := h.Backend.DescribeTrafficSources(groupName)
	if err != nil {
		return nil, err
	}

	members := make([]xmlTrafficSourceState, 0, len(sources))
	for _, s := range sources {
		members = append(members, xmlTrafficSourceState(s))
	}

	return &describeTrafficSourcesResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeTrafficSourcesResult{
			TrafficSources: xmlTrafficSourceStateList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-traffic-sources"},
	}, nil
}

func (h *Handler) handleDetachInstances(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	instanceIDs := parseMembers(vals, "InstanceIds.member")
	decrement := vals.Get("ShouldDecrementDesiredCapacity") == formValueTrue

	activities, err := h.Backend.DetachInstances(groupName, instanceIDs, decrement)
	if err != nil {
		return nil, err
	}

	members := make([]xmlScalingActivity, 0, len(activities))
	for i := range activities {
		members = append(members, toXMLScalingActivity(&activities[i]))
	}

	return &detachInstancesResponse{
		Xmlns: autoscalingXMLNS,
		Result: detachInstancesResult{
			Activities: xmlScalingActivityList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-detach-instances"},
	}, nil
}

func (h *Handler) handleDetachLoadBalancerTargetGroups(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	targetGroupARNs := parseMembers(vals, "TargetGroupARNs.member")

	if err := h.Backend.DetachLoadBalancerTargetGroups(groupName, targetGroupARNs); err != nil {
		return nil, err
	}

	return &detachLoadBalancerTargetGroupsResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-detach-lb-target-groups"},
	}, nil
}

func (h *Handler) handleDetachLoadBalancers(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	lbNames := parseMembers(vals, "LoadBalancerNames.member")

	if err := h.Backend.DetachLoadBalancers(groupName, lbNames); err != nil {
		return nil, err
	}

	return &detachLoadBalancersResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-detach-load-balancers"},
	}, nil
}

func (h *Handler) handleDetachTrafficSources(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	tss := parseTrafficSources(vals)

	if err := h.Backend.DetachTrafficSources(groupName, tss); err != nil {
		return nil, err
	}

	return &detachTrafficSourcesResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-detach-traffic-sources"},
	}, nil
}

func (h *Handler) handleEnableMetricsCollection(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	metrics := parseMembers(vals, "Metrics.member")
	granularity := vals.Get("Granularity")

	if err := h.Backend.EnableMetricsCollection(groupName, metrics, granularity); err != nil {
		return nil, err
	}

	return &enableMetricsCollectionResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-enable-metrics-collection"},
	}, nil
}

func (h *Handler) handleDisableMetricsCollection(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	metrics := parseMembers(vals, "Metrics.member")

	if err := h.Backend.DisableMetricsCollection(groupName, metrics); err != nil {
		return nil, err
	}

	return &disableMetricsCollectionResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-disable-metrics-collection"},
	}, nil
}

func (h *Handler) handleSuspendProcesses(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	processes := parseMembers(vals, "ScalingProcesses.member")

	if err := h.Backend.SuspendProcesses(groupName, processes); err != nil {
		return nil, err
	}

	return &suspendProcessesResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-suspend-processes"},
	}, nil
}

func (h *Handler) handleResumeProcesses(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	processes := parseMembers(vals, "ScalingProcesses.member")

	if err := h.Backend.ResumeProcesses(groupName, processes); err != nil {
		return nil, err
	}

	return &resumeProcessesResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-resume-processes"},
	}, nil
}

func (h *Handler) handleEnterStandby(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	instanceIDs := parseMembers(vals, "InstanceIds.member")
	decrement := vals.Get("ShouldDecrementDesiredCapacity") == formValueTrue

	activities, err := h.Backend.EnterStandby(groupName, instanceIDs, decrement)
	if err != nil {
		return nil, err
	}

	members := make([]xmlScalingActivity, 0, len(activities))
	for i := range activities {
		members = append(members, toXMLScalingActivity(&activities[i]))
	}

	return &enterStandbyResponse{
		Xmlns: autoscalingXMLNS,
		Result: enterStandbyResult{
			Activities: xmlScalingActivityList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-enter-standby"},
	}, nil
}

func (h *Handler) handleExitStandby(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	instanceIDs := parseMembers(vals, "InstanceIds.member")

	activities, err := h.Backend.ExitStandby(groupName, instanceIDs)
	if err != nil {
		return nil, err
	}

	members := make([]xmlScalingActivity, 0, len(activities))
	for i := range activities {
		members = append(members, toXMLScalingActivity(&activities[i]))
	}

	return &exitStandbyResponse{
		Xmlns: autoscalingXMLNS,
		Result: exitStandbyResult{
			Activities: xmlScalingActivityList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-exit-standby"},
	}, nil
}

func (h *Handler) handleSetInstanceHealth(vals url.Values) (any, error) {
	instanceID := vals.Get("InstanceId")
	healthStatus := vals.Get("HealthStatus")
	respectGracePeriod := vals.Get("ShouldRespectGracePeriod") != "false"

	if err := h.Backend.SetInstanceHealth(instanceID, healthStatus, respectGracePeriod); err != nil {
		return nil, err
	}

	return &setInstanceHealthResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-set-instance-health"},
	}, nil
}

func (h *Handler) handleSetInstanceProtection(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	instanceIDs := parseMembers(vals, "InstanceIds.member")
	protected := vals.Get("ProtectedFromScaleIn") == formValueTrue

	if err := h.Backend.SetInstanceProtection(groupName, instanceIDs, protected); err != nil {
		return nil, err
	}

	return &setInstanceProtectionResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-set-instance-protection"},
	}, nil
}

func (h *Handler) handleRecordLifecycleActionHeartbeat(vals url.Values) (any, error) {
	input := RecordLifecycleActionHeartbeatInput{
		AutoScalingGroupName: vals.Get("AutoScalingGroupName"),
		LifecycleHookName:    vals.Get("LifecycleHookName"),
		LifecycleActionToken: vals.Get("LifecycleActionToken"),
		InstanceID:           vals.Get("InstanceId"),
	}

	if err := h.Backend.RecordLifecycleActionHeartbeat(input); err != nil {
		return nil, err
	}

	return &recordLifecycleActionHeartbeatResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-record-lifecycle-heartbeat"},
	}, nil
}

func (h *Handler) handleExecutePolicy(vals url.Values) (any, error) {
	input := ExecutePolicyInput{
		AutoScalingGroupName: vals.Get("AutoScalingGroupName"),
		PolicyName:           vals.Get("PolicyName"),
		HonorCooldown:        vals.Get("HonorCooldown") == formValueTrue,
	}

	if err := h.Backend.ExecutePolicy(input); err != nil {
		return nil, err
	}

	return &executePolicyResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-execute-policy"},
	}, nil
}

func (h *Handler) handleLaunchInstances(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")

	desiredCapacity, err := parseIntVal(vals.Get("DesiredCapacity"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid DesiredCapacity", ErrInvalidParameter)
	}

	count := int32(1)
	if desiredCapacity > 0 {
		count = desiredCapacity
	}

	instances, launchErr := h.Backend.LaunchInstances(groupName, count)
	if launchErr != nil {
		return nil, launchErr
	}

	members := make([]xmlInstance, 0, len(instances))
	for _, inst := range instances {
		members = append(members, xmlInstance{
			InstanceID:              inst.InstanceID,
			AvailabilityZone:        inst.AvailabilityZone,
			LifecycleState:          inst.LifecycleState,
			HealthStatus:            inst.HealthStatus,
			LaunchConfigurationName: inst.LaunchConfigurationName,
		})
	}

	return &launchInstancesResponse{
		Xmlns: autoscalingXMLNS,
		Result: launchInstancesResult{
			Instances: xmlInstanceList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-launch-instances"},
	}, nil
}

func (h *Handler) handleGetPredictiveScalingForecast(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")

	if err := h.Backend.GetPredictiveScalingForecast(groupName); err != nil {
		return nil, err
	}

	return &getPredictiveScalingForecastResponse{
		Xmlns: autoscalingXMLNS,
		Result: getPredictiveScalingForecastResult{
			CapacityForecast: xmlCapacityForecast{},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-get-predictive-scaling-forecast"},
	}, nil
}

func (h *Handler) handlePutNotificationConfiguration(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	topicARN := vals.Get("TopicARN")
	types := parseMembers(vals, "NotificationTypes.member")

	if err := h.Backend.PutNotificationConfiguration(groupName, topicARN, types); err != nil {
		return nil, err
	}

	return &putNotificationConfigurationResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-put-notification-configuration"},
	}, nil
}

func (h *Handler) handleDeleteNotificationConfiguration(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	topicARN := vals.Get("TopicARN")

	if err := h.Backend.DeleteNotificationConfiguration(groupName, topicARN); err != nil {
		return nil, err
	}

	return &deleteNotificationConfigurationResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-delete-notification-configuration"},
	}, nil
}

func (h *Handler) handleDescribeNotificationConfigurations(vals url.Values) (any, error) {
	groupNames := parseMembers(vals, "AutoScalingGroupNames.member")

	configs, err := h.Backend.DescribeNotificationConfigurations(groupNames)
	if err != nil {
		return nil, err
	}

	members := make([]xmlNotificationConfiguration, 0, len(configs))
	for _, c := range configs {
		members = append(members, xmlNotificationConfiguration(c))
	}

	return &describeNotificationConfigurationsResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeNotificationConfigurationsResult{
			NotificationConfigurations: xmlNotificationConfigurationList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-notification-configurations"},
	}, nil
}

//nolint:gocognit,cyclop,funlen // parses many optional scaling policy fields
func (h *Handler) handlePutScalingPolicy(vals url.Values) (any, error) {
	scalingAdjustment, err := parseIntVal(vals.Get("ScalingAdjustment"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid ScalingAdjustment", ErrInvalidParameter)
	}

	minAdjustmentStep, err := parseIntVal(vals.Get("MinAdjustmentStep"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid MinAdjustmentStep", ErrInvalidParameter)
	}

	cooldown, err := parseIntVal(vals.Get("Cooldown"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid Cooldown", ErrInvalidParameter)
	}

	estimatedWarmup, err := parseIntVal(vals.Get("TargetTrackingConfiguration.EstimatedInstanceWarmup"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid EstimatedInstanceWarmup", ErrInvalidParameter)
	}

	// Parse TargetTrackingConfiguration fields
	var targetValue float64
	if v := vals.Get("TargetTrackingConfiguration.TargetValue"); v != "" {
		tv, parseErr := strconv.ParseFloat(v, 64)
		if parseErr != nil {
			return nil, fmt.Errorf("%w: invalid TargetTrackingConfiguration.TargetValue", ErrInvalidParameter)
		}

		targetValue = tv
	}

	metricType := vals.Get("TargetTrackingConfiguration.PredefinedMetricSpecification.PredefinedMetricType")
	disableScaleIn := vals.Get("TargetTrackingConfiguration.DisableScaleIn") == formValueTrue

	minAdjustmentMagnitude, err := parseIntVal(vals.Get("MinAdjustmentMagnitude"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid MinAdjustmentMagnitude", ErrInvalidParameter)
	}

	// Parse StepAdjustments.member.N.{ScalingAdjustment,MetricIntervalLowerBound,MetricIntervalUpperBound}
	var stepAdjustments []StepAdjustment
	for i := 1; ; i++ {
		prefix := fmt.Sprintf("StepAdjustments.member.%d.", i)
		saStr := vals.Get(prefix + "ScalingAdjustment")
		if saStr == "" {
			break
		}

		sa, parseErr := parseIntVal(saStr)
		if parseErr != nil {
			return nil, fmt.Errorf("%w: invalid StepAdjustments.member.%d.ScalingAdjustment", ErrInvalidParameter, i)
		}

		adj := StepAdjustment{ScalingAdjustment: sa}
		if v := vals.Get(prefix + "MetricIntervalLowerBound"); v != "" {
			f, floatErr := strconv.ParseFloat(v, 64)
			if floatErr != nil {
				return nil, fmt.Errorf("%w: invalid MetricIntervalLowerBound", ErrInvalidParameter)
			}

			adj.MetricIntervalLowerBound = &f
		}

		if v := vals.Get(prefix + "MetricIntervalUpperBound"); v != "" {
			f, floatErr := strconv.ParseFloat(v, 64)
			if floatErr != nil {
				return nil, fmt.Errorf("%w: invalid MetricIntervalUpperBound", ErrInvalidParameter)
			}

			adj.MetricIntervalUpperBound = &f
		}

		stepAdjustments = append(stepAdjustments, adj)
	}

	input := ScalingPolicyInput{
		AutoScalingGroupName:   vals.Get("AutoScalingGroupName"),
		PolicyName:             vals.Get("PolicyName"),
		PolicyType:             vals.Get("PolicyType"),
		AdjustmentType:         vals.Get("AdjustmentType"),
		ScalingAdjustment:      scalingAdjustment,
		MinAdjustmentStep:      minAdjustmentStep,
		MinAdjustmentMagnitude: minAdjustmentMagnitude,
		StepAdjustments:        stepAdjustments,
		Cooldown:               cooldown,
		TargetValue:            targetValue,
		MetricType:             metricType,
		DisableScaleIn:         disableScaleIn,
		EstimatedWarmup:        estimatedWarmup,
	}

	policy, putErr := h.Backend.PutScalingPolicy(input)
	if putErr != nil {
		return nil, putErr
	}

	return &putScalingPolicyResponse{
		Xmlns: autoscalingXMLNS,
		Result: putScalingPolicyResult{
			PolicyARN: policy.PolicyARN,
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-put-scaling-policy"},
	}, nil
}

func (h *Handler) handleDeletePolicy(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	policyName := vals.Get("PolicyName")

	if err := h.Backend.DeletePolicy(groupName, policyName); err != nil {
		return nil, err
	}

	return &deletePolicyResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-delete-policy"},
	}, nil
}

func (h *Handler) handleDescribePolicies(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	policyNames := parseMembers(vals, "PolicyNames.member")

	policies, err := h.Backend.DescribePolicies(groupName, policyNames)
	if err != nil {
		return nil, err
	}

	members := make([]xmlScalingPolicy, 0, len(policies))
	for _, p := range policies {
		xmlPolicy := xmlScalingPolicy{
			PolicyName:             p.PolicyName,
			PolicyARN:              p.PolicyARN,
			AutoScalingGroupName:   p.AutoScalingGroupName,
			PolicyType:             p.PolicyType,
			AdjustmentType:         p.AdjustmentType,
			ScalingAdjustment:      p.ScalingAdjustment,
			MinAdjustmentMagnitude: p.MinAdjustmentMagnitude,
			Cooldown:               p.Cooldown,
		}

		if len(p.StepAdjustments) > 0 {
			steps := make([]xmlStepAdjustment, 0, len(p.StepAdjustments))
			for _, s := range p.StepAdjustments {
				steps = append(steps, xmlStepAdjustment(s))
			}

			xmlPolicy.StepAdjustments = &xmlStepAdjustmentList{Members: steps}
		}

		if p.PolicyType == "TargetTrackingScaling" {
			ttc := &xmlTargetTrackingConfiguration{
				TargetValue:             p.TargetValue,
				DisableScaleIn:          p.DisableScaleIn,
				EstimatedInstanceWarmup: p.EstimatedWarmup,
			}

			if p.MetricType != "" {
				ttc.PredefinedMetricSpecification = &xmlPredefinedMetricSpecification{
					PredefinedMetricType: p.MetricType,
				}
			}

			xmlPolicy.TargetTrackingConfiguration = ttc
		}

		members = append(members, xmlPolicy)
	}

	return &describePoliciesResponse{
		Xmlns: autoscalingXMLNS,
		Result: describePoliciesResult{
			ScalingPolicies: xmlScalingPolicyList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-policies"},
	}, nil
}

func (h *Handler) handlePutScheduledUpdateGroupAction(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")

	action := ScheduledUpdateGroupAction{
		ScheduledActionName: vals.Get("ScheduledActionName"),
		Recurrence:          vals.Get("Recurrence"),
		TimeZone:            vals.Get("TimeZone"),
	}

	if v := vals.Get("DesiredCapacity"); v != "" {
		if n, err := parseIntVal(v); err == nil {
			action.DesiredCapacity = &n
		}
	}

	if v := vals.Get("MinSize"); v != "" {
		if n, err := parseIntVal(v); err == nil {
			action.MinSize = &n
		}
	}

	if v := vals.Get("MaxSize"); v != "" {
		if n, err := parseIntVal(v); err == nil {
			action.MaxSize = &n
		}
	}

	if err := h.Backend.PutScheduledUpdateGroupAction(groupName, action); err != nil {
		return nil, err
	}

	return &putScheduledUpdateGroupActionResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-put-scheduled-action"},
	}, nil
}

func (h *Handler) handleDeleteScheduledAction(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	actionName := vals.Get("ScheduledActionName")

	if err := h.Backend.DeleteScheduledAction(groupName, actionName); err != nil {
		return nil, err
	}

	return &deleteScheduledActionResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-delete-scheduled-action"},
	}, nil
}

func (h *Handler) handlePutWarmPool(vals url.Values) (any, error) {
	minSize, err := parseIntVal(vals.Get("MinSize"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid MinSize", ErrInvalidParameter)
	}

	maxGroupPreparedCapacity, err := parseIntVal(vals.Get("MaxGroupPreparedCapacity"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid MaxGroupPreparedCapacity", ErrInvalidParameter)
	}

	input := WarmPoolInput{
		AutoScalingGroupName:     vals.Get("AutoScalingGroupName"),
		PoolState:                vals.Get("PoolState"),
		MinSize:                  minSize,
		MaxGroupPreparedCapacity: maxGroupPreparedCapacity,
	}

	if putErr := h.Backend.PutWarmPool(input); putErr != nil {
		return nil, putErr
	}

	return &putWarmPoolResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-put-warm-pool"},
	}, nil
}

func (h *Handler) handleDeleteWarmPool(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")

	if err := h.Backend.DeleteWarmPool(groupName); err != nil {
		return nil, err
	}

	return &deleteWarmPoolResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-delete-warm-pool"},
	}, nil
}

func (h *Handler) handleDescribeWarmPool(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")

	wp, err := h.Backend.DescribeWarmPool(groupName)
	if err != nil {
		return nil, err
	}

	xmlWP := xmlWarmPoolConfiguration{
		MinSize:                  wp.MinSize,
		PoolState:                wp.PoolState,
		Status:                   wp.Status,
		MaxGroupPreparedCapacity: wp.MaxGroupPreparedCapacity,
	}

	if wp.InstanceReusePolicy.ReuseOnScaleIn {
		xmlWP.InstanceReusePolicy = &struct {
			ReuseOnScaleIn bool `xml:"ReuseOnScaleIn,omitempty"`
		}{ReuseOnScaleIn: true}
	}

	return &describeWarmPoolResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeWarmPoolResult{
			WarmPoolConfiguration: xmlWP,
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-warm-pool"},
	}, nil
}

// --- New XML response types ---

type describeAccountLimitsResult struct {
	MaxNumberOfAutoScalingGroups    int32 `xml:"MaxNumberOfAutoScalingGroups"`
	MaxNumberOfLaunchConfigurations int32 `xml:"MaxNumberOfLaunchConfigurations"`
	NumberOfAutoScalingGroups       int32 `xml:"NumberOfAutoScalingGroups"`
	NumberOfLaunchConfigurations    int32 `xml:"NumberOfLaunchConfigurations"`
}

type describeAccountLimitsResponse struct {
	XMLName          xml.Name                    `xml:"DescribeAccountLimitsResponse"`
	Xmlns            string                      `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata         `xml:"ResponseMetadata"`
	Result           describeAccountLimitsResult `xml:"DescribeAccountLimitsResult"`
}

type xmlAdjustmentType struct {
	AdjustmentType string `xml:"AdjustmentType"`
}

type xmlAdjustmentTypeList struct {
	Members []xmlAdjustmentType `xml:"member"`
}

type describeAdjustmentTypesResult struct {
	AdjustmentTypes xmlAdjustmentTypeList `xml:"AdjustmentTypes"`
}

type describeAdjustmentTypesResponse struct {
	XMLName          xml.Name                      `xml:"DescribeAdjustmentTypesResponse"`
	Xmlns            string                        `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata           `xml:"ResponseMetadata"`
	Result           describeAdjustmentTypesResult `xml:"DescribeAdjustmentTypesResult"`
}

type describeAutoScalingNotificationTypesResult struct {
	AutoScalingNotificationTypes xmlStringValueList `xml:"AutoScalingNotificationTypes"`
}

type describeAutoScalingNotificationTypesResponse struct {
	XMLName          xml.Name                                   `xml:"DescribeAutoScalingNotificationTypesResponse"`
	Xmlns            string                                     `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                        `xml:"ResponseMetadata"`
	Result           describeAutoScalingNotificationTypesResult `xml:"DescribeAutoScalingNotificationTypesResult"`
}

type describeLifecycleHookTypesResult struct {
	LifecycleHookTypes xmlStringValueList `xml:"LifecycleHookTypes"`
}

type describeLifecycleHookTypesResponse struct {
	XMLName          xml.Name                         `xml:"DescribeLifecycleHookTypesResponse"`
	Xmlns            string                           `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata              `xml:"ResponseMetadata"`
	Result           describeLifecycleHookTypesResult `xml:"DescribeLifecycleHookTypesResult"`
}

type xmlMetricCollectionType struct {
	Metric      string `xml:"Metric"`
	Granularity string `xml:"Granularity,omitempty"`
}

type xmlMetricCollectionTypeList struct {
	Members []xmlMetricCollectionType `xml:"member"`
}

type xmlGranularity struct {
	Granularity string `xml:"Granularity"`
}

type xmlGranularityList struct {
	Members []xmlGranularity `xml:"member"`
}

type describeMetricCollectionTypesResult struct {
	Metrics       xmlMetricCollectionTypeList `xml:"Metrics"`
	Granularities xmlGranularityList          `xml:"Granularities"`
}

type describeMetricCollectionTypesResponse struct {
	XMLName          xml.Name                            `xml:"DescribeMetricCollectionTypesResponse"`
	Xmlns            string                              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                 `xml:"ResponseMetadata"`
	Result           describeMetricCollectionTypesResult `xml:"DescribeMetricCollectionTypesResult"`
}

type xmlProcessType struct {
	ProcessName string `xml:"ProcessName"`
}

type xmlProcessTypeList struct {
	Members []xmlProcessType `xml:"member"`
}

type describeScalingProcessTypesResult struct {
	Processes xmlProcessTypeList `xml:"Processes"`
}

type describeScalingProcessTypesResponse struct {
	XMLName          xml.Name                          `xml:"DescribeScalingProcessTypesResponse"`
	Xmlns            string                            `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata               `xml:"ResponseMetadata"`
	Result           describeScalingProcessTypesResult `xml:"DescribeScalingProcessTypesResult"`
}

type describeTerminationPolicyTypesResult struct {
	TerminationPolicyTypes xmlStringValueList `xml:"TerminationPolicyTypes"`
}

type describeTerminationPolicyTypesResponse struct {
	XMLName          xml.Name                             `xml:"DescribeTerminationPolicyTypesResponse"`
	Xmlns            string                               `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                  `xml:"ResponseMetadata"`
	Result           describeTerminationPolicyTypesResult `xml:"DescribeTerminationPolicyTypesResult"`
}

type xmlInstanceRefresh struct {
	InstanceRefreshID         string `xml:"InstanceRefreshId"`
	AutoScalingGroupName      string `xml:"AutoScalingGroupName"`
	Status                    string `xml:"Status"`
	StatusReason              string `xml:"StatusReason,omitempty"`
	StartTime                 string `xml:"StartTime"`
	EndTime                   string `xml:"EndTime,omitempty"`
	Strategy                  string `xml:"Strategy,omitempty"`
	ScaleInProtectedInstances string `xml:"Preferences>ScaleInProtectedInstances,omitempty"`
	StandbyInstances          string `xml:"Preferences>StandbyInstances,omitempty"`
	MinHealthyPercentage      int32  `xml:"Preferences>MinHealthyPercentage,omitempty"`
	MaxHealthyPercentage      int32  `xml:"Preferences>MaxHealthyPercentage,omitempty"`
	InstanceWarmup            int32  `xml:"Preferences>InstanceWarmup,omitempty"`
	PercentageComplete        int32  `xml:"PercentageComplete,omitempty"`
	InstancesToUpdate         int32  `xml:"InstancesToUpdate,omitempty"`
	SkipMatching              bool   `xml:"Preferences>SkipMatching,omitempty"`
	AutoRollback              bool   `xml:"Preferences>AutoRollback,omitempty"`
}

type xmlInstanceRefreshList struct {
	Members []xmlInstanceRefresh `xml:"member"`
}

type describeInstanceRefreshesResult struct {
	InstanceRefreshes xmlInstanceRefreshList `xml:"InstanceRefreshes"`
}

type describeInstanceRefreshesResponse struct {
	XMLName          xml.Name                        `xml:"DescribeInstanceRefreshesResponse"`
	Xmlns            string                          `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata             `xml:"ResponseMetadata"`
	Result           describeInstanceRefreshesResult `xml:"DescribeInstanceRefreshesResult"`
}

type startInstanceRefreshResult struct {
	InstanceRefreshID string `xml:"InstanceRefreshId"`
}

type startInstanceRefreshResponse struct {
	XMLName          xml.Name                   `xml:"StartInstanceRefreshResponse"`
	Xmlns            string                     `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata        `xml:"ResponseMetadata"`
	Result           startInstanceRefreshResult `xml:"StartInstanceRefreshResult"`
}

type rollbackInstanceRefreshResult struct {
	InstanceRefreshID string `xml:"InstanceRefreshId"`
}

type rollbackInstanceRefreshResponse struct {
	XMLName          xml.Name                      `xml:"RollbackInstanceRefreshResponse"`
	Xmlns            string                        `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata           `xml:"ResponseMetadata"`
	Result           rollbackInstanceRefreshResult `xml:"RollbackInstanceRefreshResult"`
}

type xmlLoadBalancerState struct {
	LoadBalancerName string `xml:"LoadBalancerName"`
	State            string `xml:"State"`
}

type xmlLoadBalancerStateList struct {
	Members []xmlLoadBalancerState `xml:"member"`
}

type describeLoadBalancersResult struct {
	LoadBalancers xmlLoadBalancerStateList `xml:"LoadBalancers"`
}

type describeLoadBalancersResponse struct {
	XMLName          xml.Name                    `xml:"DescribeLoadBalancersResponse"`
	Xmlns            string                      `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata         `xml:"ResponseMetadata"`
	Result           describeLoadBalancersResult `xml:"DescribeLoadBalancersResult"`
}

type xmlLoadBalancerTargetGroupState struct {
	LoadBalancerTargetGroupARN string `xml:"LoadBalancerTargetGroupARN"`
	State                      string `xml:"State"`
}

type xmlLoadBalancerTargetGroupStateList struct {
	Members []xmlLoadBalancerTargetGroupState `xml:"member"`
}

type describeLoadBalancerTargetGroupsResult struct {
	LoadBalancerTargetGroups xmlLoadBalancerTargetGroupStateList `xml:"LoadBalancerTargetGroups"`
}

type describeLoadBalancerTargetGroupsResponse struct {
	XMLName          xml.Name                               `xml:"DescribeLoadBalancerTargetGroupsResponse"`
	Xmlns            string                                 `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                    `xml:"ResponseMetadata"`
	Result           describeLoadBalancerTargetGroupsResult `xml:"DescribeLoadBalancerTargetGroupsResult"`
}

type xmlTrafficSourceState struct {
	Identifier string `xml:"Identifier"`
	Type       string `xml:"Type"`
	State      string `xml:"State"`
}

type xmlTrafficSourceStateList struct {
	Members []xmlTrafficSourceState `xml:"member"`
}

type describeTrafficSourcesResult struct {
	TrafficSources xmlTrafficSourceStateList `xml:"TrafficSources"`
}

type describeTrafficSourcesResponse struct {
	XMLName          xml.Name                     `xml:"DescribeTrafficSourcesResponse"`
	Xmlns            string                       `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata          `xml:"ResponseMetadata"`
	Result           describeTrafficSourcesResult `xml:"DescribeTrafficSourcesResult"`
}

type detachInstancesResult struct {
	Activities xmlScalingActivityList `xml:"Activities"`
}

type detachInstancesResponse struct {
	XMLName          xml.Name              `xml:"DetachInstancesResponse"`
	Xmlns            string                `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata   `xml:"ResponseMetadata"`
	Result           detachInstancesResult `xml:"DetachInstancesResult"`
}

type detachLoadBalancerTargetGroupsResponse struct {
	XMLName          xml.Name            `xml:"DetachLoadBalancerTargetGroupsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type detachLoadBalancersResponse struct {
	XMLName          xml.Name            `xml:"DetachLoadBalancersResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type detachTrafficSourcesResponse struct {
	XMLName          xml.Name            `xml:"DetachTrafficSourcesResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type enableMetricsCollectionResponse struct {
	XMLName          xml.Name            `xml:"EnableMetricsCollectionResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type disableMetricsCollectionResponse struct {
	XMLName          xml.Name            `xml:"DisableMetricsCollectionResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type suspendProcessesResponse struct {
	XMLName          xml.Name            `xml:"SuspendProcessesResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type resumeProcessesResponse struct {
	XMLName          xml.Name            `xml:"ResumeProcessesResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type enterStandbyResult struct {
	Activities xmlScalingActivityList `xml:"Activities"`
}

type enterStandbyResponse struct {
	XMLName          xml.Name            `xml:"EnterStandbyResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
	Result           enterStandbyResult  `xml:"EnterStandbyResult"`
}

type exitStandbyResult struct {
	Activities xmlScalingActivityList `xml:"Activities"`
}

type exitStandbyResponse struct {
	XMLName          xml.Name            `xml:"ExitStandbyResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
	Result           exitStandbyResult   `xml:"ExitStandbyResult"`
}

type setInstanceHealthResponse struct {
	XMLName          xml.Name            `xml:"SetInstanceHealthResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type setInstanceProtectionResponse struct {
	XMLName          xml.Name            `xml:"SetInstanceProtectionResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type recordLifecycleActionHeartbeatResponse struct {
	XMLName          xml.Name            `xml:"RecordLifecycleActionHeartbeatResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type executePolicyResponse struct {
	XMLName          xml.Name            `xml:"ExecutePolicyResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type launchInstancesResult struct {
	Instances xmlInstanceList `xml:"Instances"`
}

type launchInstancesResponse struct {
	XMLName          xml.Name              `xml:"LaunchInstancesResponse"`
	Xmlns            string                `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata   `xml:"ResponseMetadata"`
	Result           launchInstancesResult `xml:"LaunchInstancesResult"`
}

type xmlCapacityForecast struct {
	Timestamps xmlStringValueList `xml:"Timestamps"`
	Values     xmlStringValueList `xml:"Values"`
}

type getPredictiveScalingForecastResult struct {
	LoadForecast     []string            `xml:"LoadForecast"`
	CapacityForecast xmlCapacityForecast `xml:"CapacityForecast"`
}

type getPredictiveScalingForecastResponse struct {
	XMLName          xml.Name                           `xml:"GetPredictiveScalingForecastResponse"`
	Xmlns            string                             `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                `xml:"ResponseMetadata"`
	Result           getPredictiveScalingForecastResult `xml:"GetPredictiveScalingForecastResult"`
}

type putNotificationConfigurationResponse struct {
	XMLName          xml.Name            `xml:"PutNotificationConfigurationResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type deleteNotificationConfigurationResponse struct {
	XMLName          xml.Name            `xml:"DeleteNotificationConfigurationResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type xmlNotificationConfiguration struct {
	AutoScalingGroupName string `xml:"AutoScalingGroupName"`
	TopicARN             string `xml:"TopicARN"`
	NotificationType     string `xml:"NotificationType"`
}

type xmlNotificationConfigurationList struct {
	Members []xmlNotificationConfiguration `xml:"member"`
}

type describeNotificationConfigurationsResult struct {
	NotificationConfigurations xmlNotificationConfigurationList `xml:"NotificationConfigurations"`
}

type describeNotificationConfigurationsResponse struct {
	XMLName          xml.Name                                 `xml:"DescribeNotificationConfigurationsResponse"`
	Xmlns            string                                   `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                      `xml:"ResponseMetadata"`
	Result           describeNotificationConfigurationsResult `xml:"DescribeNotificationConfigurationsResult"`
}

type putScalingPolicyResult struct {
	PolicyARN string `xml:"PolicyARN"`
}

type putScalingPolicyResponse struct {
	XMLName          xml.Name               `xml:"PutScalingPolicyResponse"`
	Xmlns            string                 `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata    `xml:"ResponseMetadata"`
	Result           putScalingPolicyResult `xml:"PutScalingPolicyResult"`
}

type deletePolicyResponse struct {
	XMLName          xml.Name            `xml:"DeletePolicyResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type xmlPredefinedMetricSpecification struct {
	PredefinedMetricType string `xml:"PredefinedMetricType"`
}

type xmlTargetTrackingConfiguration struct {
	PredefinedMetricSpecification *xmlPredefinedMetricSpecification `xml:"PredefinedMetricSpecification,omitempty"`
	TargetValue                   float64                           `xml:"TargetValue"`
	DisableScaleIn                bool                              `xml:"DisableScaleIn,omitempty"`
	EstimatedInstanceWarmup       int32                             `xml:"EstimatedInstanceWarmup,omitempty"`
}

type xmlStepAdjustment struct {
	MetricIntervalLowerBound *float64 `xml:"MetricIntervalLowerBound,omitempty"`
	MetricIntervalUpperBound *float64 `xml:"MetricIntervalUpperBound,omitempty"`
	ScalingAdjustment        int32    `xml:"ScalingAdjustment"`
}

type xmlStepAdjustmentList struct {
	Members []xmlStepAdjustment `xml:"member"`
}

// xmlScalingPolicy is the XML type for a scaling policy.
type xmlScalingPolicy struct {
	TargetTrackingConfiguration *xmlTargetTrackingConfiguration `xml:"TargetTrackingConfiguration,omitempty"`
	StepAdjustments             *xmlStepAdjustmentList          `xml:"StepAdjustments,omitempty"`
	PolicyName                  string                          `xml:"PolicyName"`
	PolicyARN                   string                          `xml:"PolicyARN"`
	AutoScalingGroupName        string                          `xml:"AutoScalingGroupName"`
	PolicyType                  string                          `xml:"PolicyType,omitempty"`
	AdjustmentType              string                          `xml:"AdjustmentType,omitempty"`
	ScalingAdjustment           int32                           `xml:"ScalingAdjustment,omitempty"`
	MinAdjustmentMagnitude      int32                           `xml:"MinAdjustmentMagnitude,omitempty"`
	Cooldown                    int32                           `xml:"Cooldown,omitempty"`
}

type xmlScalingPolicyList struct {
	Members []xmlScalingPolicy `xml:"member"`
}

type describePoliciesResult struct {
	ScalingPolicies xmlScalingPolicyList `xml:"ScalingPolicies"`
}

type describePoliciesResponse struct {
	XMLName          xml.Name               `xml:"DescribePoliciesResponse"`
	Xmlns            string                 `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata    `xml:"ResponseMetadata"`
	Result           describePoliciesResult `xml:"DescribePoliciesResult"`
}

type putScheduledUpdateGroupActionResponse struct {
	XMLName          xml.Name            `xml:"PutScheduledUpdateGroupActionResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type deleteScheduledActionResponse struct {
	XMLName          xml.Name            `xml:"DeleteScheduledActionResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type putWarmPoolResponse struct {
	XMLName          xml.Name            `xml:"PutWarmPoolResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type deleteWarmPoolResponse struct {
	XMLName          xml.Name            `xml:"DeleteWarmPoolResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type xmlWarmPoolConfiguration struct {
	InstanceReusePolicy *struct {
		ReuseOnScaleIn bool `xml:"ReuseOnScaleIn,omitempty"`
	} `xml:"InstanceReusePolicy,omitempty"`
	PoolState                string `xml:"PoolState"`
	Status                   string `xml:"Status"`
	MinSize                  int32  `xml:"MinSize"`
	MaxGroupPreparedCapacity int32  `xml:"MaxGroupPreparedCapacity,omitempty"`
}

type describeWarmPoolResult struct {
	WarmPoolConfiguration xmlWarmPoolConfiguration `xml:"WarmPoolConfiguration"`
}

type describeWarmPoolResponse struct {
	XMLName          xml.Name               `xml:"DescribeWarmPoolResponse"`
	Xmlns            string                 `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata    `xml:"ResponseMetadata"`
	Result           describeWarmPoolResult `xml:"DescribeWarmPoolResult"`
}
