package codedeploy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const codedeployTargetPrefix = "CodeDeploy_20141006."

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the Echo HTTP handler for AWS CodeDeploy operations.
type Handler struct {
	Backend *InMemoryBackend
	// ops is a pre-built dispatch table to avoid allocating a new map on every request.
	ops map[string]service.JSONOpFunc
}

// NewHandler creates a new CodeDeploy handler with a pre-built dispatch table.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.dispatchTable()

	return h
}

// Reset clears the handler state by delegating to the backend.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Name returns the service name.
func (h *Handler) Name() string { return "CodeDeploy" }

// GetSupportedOperations returns the list of supported CodeDeploy operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateApplication",
		"GetApplication",
		"ListApplications",
		"DeleteApplication",
		"UpdateApplication",
		"CreateDeploymentGroup",
		"GetDeploymentGroup",
		"ListDeploymentGroups",
		"DeleteDeploymentGroup",
		"UpdateDeploymentGroup",
		"CreateDeployment",
		"GetDeployment",
		"ListDeployments",
		"StopDeployment",
		"ContinueDeployment",
		"SkipWaitTimeForInstanceTermination",
		"CreateDeploymentConfig",
		"GetDeploymentConfig",
		"ListDeploymentConfigs",
		"DeleteDeploymentConfig",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
		"AddTagsToOnPremisesInstances",
		"RemoveTagsFromOnPremisesInstances",
		"RegisterOnPremisesInstance",
		"DeregisterOnPremisesInstance",
		"GetOnPremisesInstance",
		"ListOnPremisesInstances",
		"BatchGetApplicationRevisions",
		"BatchGetApplications",
		"BatchGetDeploymentGroups",
		"BatchGetDeploymentInstances",
		"BatchGetDeploymentTargets",
		"BatchGetDeployments",
		"BatchGetOnPremisesInstances",
		"RegisterApplicationRevision",
		"GetApplicationRevision",
		"ListApplicationRevisions",
		"GetDeploymentInstance",
		"GetDeploymentTarget",
		"ListDeploymentInstances",
		"ListDeploymentTargets",
		"PutLifecycleEventHookExecutionStatus",
		"DeleteGitHubAccountToken",
		"ListGitHubAccountTokenNames",
		"DeleteResourcesByExternalId",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "codedeploy" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this CodeDeploy instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches AWS CodeDeploy requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), codedeployTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the CodeDeploy operation from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, codedeployTargetPrefix)

	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

// ExtractResource extracts the application name from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, readErr := httputils.ReadBody(c.Request())
	if readErr != nil {
		return ""
	}

	var input struct {
		ApplicationName string `json:"applicationName"`
	}
	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return ""
	}

	return input.ApplicationName
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"CodeDeploy", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateApplication":                    service.WrapOp(h.handleCreateApplication),
		"GetApplication":                       service.WrapOp(h.handleGetApplication),
		"ListApplications":                     service.WrapOp(h.handleListApplications),
		"DeleteApplication":                    service.WrapOp(h.handleDeleteApplication),
		"UpdateApplication":                    service.WrapOp(h.handleUpdateApplication),
		"CreateDeploymentGroup":                service.WrapOp(h.handleCreateDeploymentGroup),
		"GetDeploymentGroup":                   service.WrapOp(h.handleGetDeploymentGroup),
		"ListDeploymentGroups":                 service.WrapOp(h.handleListDeploymentGroups),
		"DeleteDeploymentGroup":                service.WrapOp(h.handleDeleteDeploymentGroup),
		"UpdateDeploymentGroup":                service.WrapOp(h.handleUpdateDeploymentGroup),
		"CreateDeployment":                     service.WrapOp(h.handleCreateDeployment),
		"GetDeployment":                        service.WrapOp(h.handleGetDeployment),
		"ListDeployments":                      service.WrapOp(h.handleListDeployments),
		"StopDeployment":                       service.WrapOp(h.handleStopDeployment),
		"ContinueDeployment":                   service.WrapOp(h.handleContinueDeployment),
		"SkipWaitTimeForInstanceTermination":   service.WrapOp(h.handleSkipWaitTimeForInstanceTermination),
		"CreateDeploymentConfig":               service.WrapOp(h.handleCreateDeploymentConfig),
		"GetDeploymentConfig":                  service.WrapOp(h.handleGetDeploymentConfig),
		"ListDeploymentConfigs":                service.WrapOp(h.handleListDeploymentConfigs),
		"DeleteDeploymentConfig":               service.WrapOp(h.handleDeleteDeploymentConfig),
		"TagResource":                          service.WrapOp(h.handleTagResource),
		"UntagResource":                        service.WrapOp(h.handleUntagResource),
		"ListTagsForResource":                  service.WrapOp(h.handleListTagsForResource),
		"AddTagsToOnPremisesInstances":         service.WrapOp(h.handleAddTagsToOnPremisesInstances),
		"RemoveTagsFromOnPremisesInstances":    service.WrapOp(h.handleRemoveTagsFromOnPremisesInstances),
		"RegisterOnPremisesInstance":           service.WrapOp(h.handleRegisterOnPremisesInstance),
		"DeregisterOnPremisesInstance":         service.WrapOp(h.handleDeregisterOnPremisesInstance),
		"GetOnPremisesInstance":                service.WrapOp(h.handleGetOnPremisesInstance),
		"ListOnPremisesInstances":              service.WrapOp(h.handleListOnPremisesInstances),
		"BatchGetApplicationRevisions":         service.WrapOp(h.handleBatchGetApplicationRevisions),
		"BatchGetApplications":                 service.WrapOp(h.handleBatchGetApplications),
		"BatchGetDeploymentGroups":             service.WrapOp(h.handleBatchGetDeploymentGroups),
		"BatchGetDeploymentInstances":          service.WrapOp(h.handleBatchGetDeploymentInstances),
		"BatchGetDeploymentTargets":            service.WrapOp(h.handleBatchGetDeploymentTargets),
		"BatchGetDeployments":                  service.WrapOp(h.handleBatchGetDeployments),
		"BatchGetOnPremisesInstances":          service.WrapOp(h.handleBatchGetOnPremisesInstances),
		"RegisterApplicationRevision":          service.WrapOp(h.handleRegisterApplicationRevision),
		"GetApplicationRevision":               service.WrapOp(h.handleGetApplicationRevision),
		"ListApplicationRevisions":             service.WrapOp(h.handleListApplicationRevisions),
		"GetDeploymentInstance":                service.WrapOp(h.handleGetDeploymentInstance),
		"GetDeploymentTarget":                  service.WrapOp(h.handleGetDeploymentTarget),
		"ListDeploymentInstances":              service.WrapOp(h.handleListDeploymentInstances),
		"ListDeploymentTargets":                service.WrapOp(h.handleListDeploymentTargets),
		"PutLifecycleEventHookExecutionStatus": service.WrapOp(h.handlePutLifecycleEventHookExecutionStatus),
		"DeleteGitHubAccountToken":             service.WrapOp(h.handleDeleteGitHubAccountToken),
		"ListGitHubAccountTokenNames":          service.WrapOp(h.handleListGitHubAccountTokenNames),
		"DeleteResourcesByExternalId":          service.WrapOp(h.handleDeleteResourcesByExternalID),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
	}

	result, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

// errorMapping maps sentinel errors to HTTP status and exception type.
type errorMapping struct {
	sentinel error
	code     string
	status   int
}

// errorMappings is the ordered lookup table for handleError.
//
//nolint:gochecknoglobals // package-level lookup table for error mapping
var errorMappings = []errorMapping{
	{ErrNotFound, "ApplicationDoesNotExistException", http.StatusNotFound},
	{ErrDeploymentGroupNotFound, "DeploymentGroupDoesNotExistException", http.StatusNotFound},
	{ErrDeploymentNotFound, "DeploymentDoesNotExistException", http.StatusNotFound},
	{ErrDeploymentConfigNotFound, "DeploymentConfigDoesNotExistException", http.StatusNotFound},
	{ErrGitHubAccountTokenNotFound, "GitHubAccountTokenDoesNotExistException", http.StatusNotFound},
	{ErrAlreadyExists, "ApplicationAlreadyExistsException", http.StatusConflict},
	{ErrDeploymentGroupAlreadyExists, "DeploymentGroupAlreadyExistsException", http.StatusConflict},
	{ErrDeploymentConfigAlreadyExists, "DeploymentConfigAlreadyExistsException", http.StatusConflict},
	{ErrDeploymentConfigInUse, "DeploymentConfigInUseException", http.StatusConflict},
	{ErrInvalidComputePlatform, "InvalidComputePlatformException", http.StatusBadRequest},
	{ErrIamArnRequired, "IamArnRequiredException", http.StatusBadRequest},
	{ErrMultipleIamArns, "MultipleIamArnsProvidedException", http.StatusBadRequest},
	{ErrTagLimitExceeded, "TagLimitExceededException", http.StatusBadRequest},
	{ErrValidation, "InvalidParameterValueException", http.StatusBadRequest},
	{errInvalidRequest, "InvalidRequestException", http.StatusBadRequest},
	{errUnknownAction, "InvalidRequestException", http.StatusBadRequest},
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	makePayload := func(code, msg string) []byte {
		b, _ := json.Marshal(service.JSONErrorResponse{Type: code, Message: msg})

		return b
	}

	for _, m := range errorMappings {
		if errors.Is(err, m.sentinel) {
			return c.JSONBlob(m.status, makePayload(m.code, err.Error()))
		}
	}

	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	if errors.As(err, &syntaxErr) || errors.As(err, &typeErr) {
		return c.JSONBlob(http.StatusBadRequest,
			makePayload("InvalidRequestException", err.Error()))
	}

	return c.JSONBlob(http.StatusInternalServerError,
		makePayload("ServiceException", err.Error()))
}

// --- Input/Output types and handlers ---

type createApplicationInput struct {
	ApplicationName string     `json:"applicationName"`
	ComputePlatform string     `json:"computePlatform"`
	Tags            []tagEntry `json:"tags"`
}

type createApplicationOutput struct {
	ApplicationID string `json:"applicationId"`
}

func (h *Handler) handleCreateApplication(
	_ context.Context,
	in *createApplicationInput,
) (*createApplicationOutput, error) {
	if in.ApplicationName == "" {
		return nil, fmt.Errorf("%w: applicationName is required", errInvalidRequest)
	}

	if in.ComputePlatform == "" {
		in.ComputePlatform = computePlatformServer
	}

	app, err := h.Backend.CreateApplication(in.ApplicationName, in.ComputePlatform, tagEntriesToMap(in.Tags))
	if err != nil {
		return nil, err
	}

	return &createApplicationOutput{ApplicationID: app.ApplicationID}, nil
}

type getApplicationInput struct {
	ApplicationName string `json:"applicationName"`
}

type applicationInfo struct {
	ApplicationID   string `json:"applicationId"`
	ApplicationName string `json:"applicationName"`
	ComputePlatform string `json:"computePlatform"`
	CreateTime      int64  `json:"createTime"`
}

type getApplicationOutput struct {
	Application applicationInfo `json:"application"`
}

func (h *Handler) handleGetApplication(
	_ context.Context,
	in *getApplicationInput,
) (*getApplicationOutput, error) {
	if in.ApplicationName == "" {
		return nil, fmt.Errorf("%w: applicationName is required", errInvalidRequest)
	}

	app, err := h.Backend.GetApplication(in.ApplicationName)
	if err != nil {
		return nil, err
	}

	return &getApplicationOutput{
		Application: applicationInfo{
			ApplicationID:   app.ApplicationID,
			ApplicationName: app.ApplicationName,
			ComputePlatform: app.ComputePlatform,
			CreateTime:      app.CreationTime.UnixMilli(),
		},
	}, nil
}

type listApplicationsInput struct{}

type listApplicationsOutput struct {
	Applications []string `json:"applications"`
}

func (h *Handler) handleListApplications(
	_ context.Context,
	_ *listApplicationsInput,
) (*listApplicationsOutput, error) {
	return &listApplicationsOutput{Applications: h.Backend.ListApplications()}, nil
}

type deleteApplicationInput struct {
	ApplicationName string `json:"applicationName"`
}

type deleteApplicationOutput struct{}

func (h *Handler) handleDeleteApplication(
	_ context.Context,
	in *deleteApplicationInput,
) (*deleteApplicationOutput, error) {
	if in.ApplicationName == "" {
		return nil, fmt.Errorf("%w: applicationName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteApplication(in.ApplicationName); err != nil {
		return nil, err
	}

	return &deleteApplicationOutput{}, nil
}

// tagFilterEntry is the wire format for a tag filter (with Type field).
type tagFilterEntry struct {
	Key   string `json:"Key,omitempty"`
	Value string `json:"Value,omitempty"`
	Type  string `json:"Type,omitempty"`
}

// autoScalingGroupEntry is the wire format for an auto scaling group reference.
type autoScalingGroupEntry struct {
	Name string `json:"name,omitempty"`
	Hook string `json:"hook,omitempty"`
}

// elbInfoEntry is the wire format for an ELB reference.
type elbInfoEntry struct {
	Name string `json:"name,omitempty"`
}

// targetGroupInfoEntry is the wire format for a target group reference.
type targetGroupInfoEntry struct {
	Name string `json:"name,omitempty"`
}

// trafficRouteEntry is the wire format for a traffic route.
type trafficRouteEntry struct {
	ListenerArns []string `json:"listenerArns,omitempty"`
}

// targetGroupPairInfoEntry is the wire format for a target group pair.
type targetGroupPairInfoEntry struct {
	ProdTrafficRoute *trafficRouteEntry     `json:"prodTrafficRoute,omitempty"`
	TestTrafficRoute *trafficRouteEntry     `json:"testTrafficRoute,omitempty"`
	TargetGroups     []targetGroupInfoEntry `json:"targetGroups,omitempty"`
}

// loadBalancerInfoEntry is the wire format for load balancer info.
type loadBalancerInfoEntry struct {
	ElbInfoList             []elbInfoEntry             `json:"elbInfoList,omitempty"`
	TargetGroupInfoList     []targetGroupInfoEntry     `json:"targetGroupInfoList,omitempty"`
	TargetGroupPairInfoList []targetGroupPairInfoEntry `json:"targetGroupPairInfoList,omitempty"`
}

// deploymentStyleEntry is the wire format for deployment style.
type deploymentStyleEntry struct {
	DeploymentType   string `json:"deploymentType,omitempty"`
	DeploymentOption string `json:"deploymentOption,omitempty"`
}

// terminateBlueEntry is the wire format for blue-instance termination config.
type terminateBlueEntry struct {
	Action                       string `json:"action,omitempty"`
	TerminationWaitTimeInMinutes int    `json:"terminationWaitTimeInMinutes,omitempty"`
}

// deploymentReadyEntry is the wire format for deployment ready option.
type deploymentReadyEntry struct {
	ActionOnTimeout   string `json:"actionOnTimeout,omitempty"`
	WaitTimeInMinutes int    `json:"waitTimeInMinutes,omitempty"`
}

// greenFleetEntry is the wire format for green fleet provisioning option.
type greenFleetEntry struct {
	Action string `json:"action,omitempty"`
}

// blueGreenConfigEntry is the wire format for blue/green deployment configuration.
type blueGreenConfigEntry struct {
	TerminateBlueInstancesOnDeploymentSuccess *terminateBlueEntry   `json:"terminateBlueInstancesOnDeploymentSuccess,omitempty"` //nolint:lll // long AWS name
	DeploymentReadyOption                     *deploymentReadyEntry `json:"deploymentReadyOption,omitempty"`
	GreenFleetProvisioningOption              *greenFleetEntry      `json:"greenFleetProvisioningOption,omitempty"`
}

// alarmEntry is the wire format for a CloudWatch alarm reference.
type alarmEntry struct {
	Name string `json:"name,omitempty"`
}

// alarmConfigEntry is the wire format for alarm configuration.
type alarmConfigEntry struct {
	Alarms                 []alarmEntry `json:"alarms,omitempty"`
	Enabled                bool         `json:"enabled,omitempty"`
	IgnorePollAlarmFailure bool         `json:"ignorePollAlarmFailure,omitempty"`
}

// autoRollbackConfigEntry is the wire format for auto-rollback configuration.
type autoRollbackConfigEntry struct {
	Events  []string `json:"events,omitempty"`
	Enabled bool     `json:"enabled,omitempty"`
}

// triggerConfigEntry is the wire format for SNS trigger configuration.
type triggerConfigEntry struct {
	TriggerName      string   `json:"triggerName,omitempty"`
	TriggerTargetArn string   `json:"triggerTargetArn,omitempty"`
	TriggerEvents    []string `json:"triggerEvents,omitempty"`
}

// ec2TagSetEntry is the wire format for an EC2 tag set.
type ec2TagSetEntry struct {
	Ec2TagSetList [][]tagFilterEntry `json:"ec2TagSetList,omitempty"`
}

// onPremTagSetEntry is the wire format for an on-premises tag set.
type onPremTagSetEntry struct {
	OnPremisesTagSetList [][]tagFilterEntry `json:"onPremisesTagSetList,omitempty"`
}

// ecsServiceEntry is the wire format for an ECS service reference.
type ecsServiceEntry struct {
	ServiceName string `json:"serviceName,omitempty"`
	ClusterName string `json:"clusterName,omitempty"`
}

// deploymentGroupInfoOutput is the full wire format for a deployment group (get/batch responses).
type deploymentGroupInfoOutput struct {
	BlueGreenDeploymentConfiguration *blueGreenConfigEntry    `json:"blueGreenDeploymentConfiguration,omitempty"`
	AlarmConfiguration               *alarmConfigEntry        `json:"alarmConfiguration,omitempty"`
	AutoRollbackConfiguration        *autoRollbackConfigEntry `json:"autoRollbackConfiguration,omitempty"`
	LoadBalancerInfo                 *loadBalancerInfoEntry   `json:"loadBalancerInfo,omitempty"`
	DeploymentStyle                  *deploymentStyleEntry    `json:"deploymentStyle,omitempty"`
	Ec2TagSet                        *ec2TagSetEntry          `json:"ec2TagSet,omitempty"`
	OnPremisesTagSet                 *onPremTagSetEntry       `json:"onPremisesTagSet,omitempty"`
	ApplicationName                  string                   `json:"applicationName"`
	DeploymentGroupID                string                   `json:"deploymentGroupId"`
	DeploymentGroupName              string                   `json:"deploymentGroupName"`
	ServiceRoleArn                   string                   `json:"serviceRoleArn"`
	DeploymentConfigName             string                   `json:"deploymentConfigName"`
	ComputePlatform                  string                   `json:"computePlatform,omitempty"`
	OutdatedInstancesStrategy        string                   `json:"outdatedInstancesStrategy,omitempty"`
	Ec2TagFilters                    []tagFilterEntry         `json:"ec2TagFilters,omitempty"`
	OnPremisesInstanceTagFilters     []tagFilterEntry         `json:"onPremisesInstanceTagFilters,omitempty"`
	AutoScalingGroups                []autoScalingGroupEntry  `json:"autoScalingGroups,omitempty"`
	TriggerConfigurations            []triggerConfigEntry     `json:"triggerConfigurations,omitempty"`
	ECSServices                      []ecsServiceEntry        `json:"ecsServices,omitempty"`
	TerminationHookEnabled           bool                     `json:"terminationHookEnabled,omitempty"`
}

// dgToOutput converts a backend DeploymentGroup to the wire output format.
//
//nolint:gocognit,cyclop,funlen // wire-format conversion requires many field mappings
func dgToOutput(dg *DeploymentGroup) deploymentGroupInfoOutput {
	out := deploymentGroupInfoOutput{
		ApplicationName:           dg.ApplicationName,
		DeploymentGroupID:         dg.DeploymentGroupID,
		DeploymentGroupName:       dg.DeploymentGroupName,
		ServiceRoleArn:            dg.ServiceRoleArn,
		DeploymentConfigName:      dg.DeploymentConfigName,
		ComputePlatform:           dg.ComputePlatform,
		OutdatedInstancesStrategy: dg.OutdatedInstancesStrategy,
		TerminationHookEnabled:    dg.TerminationHookEnabled,
	}

	for _, f := range dg.Ec2TagFilters {
		out.Ec2TagFilters = append(out.Ec2TagFilters, tagFilterEntry(f))
	}

	for _, f := range dg.OnPremisesInstanceTagFilters {
		out.OnPremisesInstanceTagFilters = append(out.OnPremisesInstanceTagFilters,
			tagFilterEntry(f))
	}

	for _, asg := range dg.AutoScalingGroups {
		out.AutoScalingGroups = append(out.AutoScalingGroups, autoScalingGroupEntry(asg))
	}

	for _, tc := range dg.TriggerConfigurations {
		out.TriggerConfigurations = append(out.TriggerConfigurations, triggerConfigEntry(tc))
	}

	for _, svc := range dg.ECSServices {
		out.ECSServices = append(out.ECSServices, ecsServiceEntry(svc))
	}

	if dg.LoadBalancerInfo != nil {
		lbi := &loadBalancerInfoEntry{}
		for _, e := range dg.LoadBalancerInfo.ElbInfoList {
			lbi.ElbInfoList = append(lbi.ElbInfoList, elbInfoEntry(e))
		}
		for _, tg := range dg.LoadBalancerInfo.TargetGroupInfoList {
			lbi.TargetGroupInfoList = append(lbi.TargetGroupInfoList, targetGroupInfoEntry(tg))
		}
		for _, pair := range dg.LoadBalancerInfo.TargetGroupPairInfoList {
			p := targetGroupPairInfoEntry{}
			if pair.ProdTrafficRoute != nil {
				p.ProdTrafficRoute = &trafficRouteEntry{ListenerArns: pair.ProdTrafficRoute.ListenerArns}
			}
			if pair.TestTrafficRoute != nil {
				p.TestTrafficRoute = &trafficRouteEntry{ListenerArns: pair.TestTrafficRoute.ListenerArns}
			}
			for _, tg := range pair.TargetGroups {
				p.TargetGroups = append(p.TargetGroups, targetGroupInfoEntry(tg))
			}
			lbi.TargetGroupPairInfoList = append(lbi.TargetGroupPairInfoList, p)
		}
		out.LoadBalancerInfo = lbi
	}

	if dg.DeploymentStyle != nil {
		out.DeploymentStyle = &deploymentStyleEntry{
			DeploymentType:   dg.DeploymentStyle.DeploymentType,
			DeploymentOption: dg.DeploymentStyle.DeploymentOption,
		}
	}

	if dg.BlueGreenDeploymentConfiguration != nil {
		bgc := &blueGreenConfigEntry{}
		if dg.BlueGreenDeploymentConfiguration.TerminateBlueInstancesOnDeploymentSuccess != nil {
			tb := dg.BlueGreenDeploymentConfiguration.TerminateBlueInstancesOnDeploymentSuccess
			bgc.TerminateBlueInstancesOnDeploymentSuccess = &terminateBlueEntry{
				Action:                       tb.Action,
				TerminationWaitTimeInMinutes: tb.TerminationWaitTimeInMinutes,
			}
		}
		if dg.BlueGreenDeploymentConfiguration.DeploymentReadyOption != nil {
			dr := dg.BlueGreenDeploymentConfiguration.DeploymentReadyOption
			bgc.DeploymentReadyOption = &deploymentReadyEntry{
				ActionOnTimeout:   dr.ActionOnTimeout,
				WaitTimeInMinutes: dr.WaitTimeInMinutes,
			}
		}
		if dg.BlueGreenDeploymentConfiguration.GreenFleetProvisioningOption != nil {
			bgc.GreenFleetProvisioningOption = &greenFleetEntry{
				Action: dg.BlueGreenDeploymentConfiguration.GreenFleetProvisioningOption.Action,
			}
		}
		out.BlueGreenDeploymentConfiguration = bgc
	}

	if dg.AlarmConfiguration != nil {
		ac := &alarmConfigEntry{
			Enabled:                dg.AlarmConfiguration.Enabled,
			IgnorePollAlarmFailure: dg.AlarmConfiguration.IgnorePollAlarmFailure,
		}
		for _, a := range dg.AlarmConfiguration.Alarms {
			ac.Alarms = append(ac.Alarms, alarmEntry(a))
		}
		out.AlarmConfiguration = ac
	}

	if dg.AutoRollbackConfiguration != nil {
		out.AutoRollbackConfiguration = &autoRollbackConfigEntry{
			Events:  dg.AutoRollbackConfiguration.Events,
			Enabled: dg.AutoRollbackConfiguration.Enabled,
		}
	}

	if dg.Ec2TagSet != nil {
		ets := &ec2TagSetEntry{}
		for _, group := range dg.Ec2TagSet.Ec2TagSetList {
			row := make([]tagFilterEntry, 0, len(group))
			for _, f := range group {
				row = append(row, tagFilterEntry(f))
			}
			ets.Ec2TagSetList = append(ets.Ec2TagSetList, row)
		}
		out.Ec2TagSet = ets
	}

	if dg.OnPremisesTagSet != nil {
		opts := &onPremTagSetEntry{}
		for _, group := range dg.OnPremisesTagSet.OnPremisesTagSetList {
			row := make([]tagFilterEntry, 0, len(group))
			for _, f := range group {
				row = append(row, tagFilterEntry(f))
			}
			opts.OnPremisesTagSetList = append(opts.OnPremisesTagSetList, row)
		}
		out.OnPremisesTagSet = opts
	}

	return out
}

// dgInputFromWire converts wire-format deployment group input fields to backend DeploymentGroupInput.
//
//nolint:gocognit,cyclop,funlen // wire-format conversion requires many field mappings
func dgInputFromWire(
	serviceRoleArn, deploymentConfigName, outdatedInstancesStrategy string,
	terminationHookEnabled bool,
	ec2TagFilters []tagFilterEntry,
	onPremTagFilters []tagFilterEntry,
	autoScalingGroups []autoScalingGroupEntry,
	triggerConfigs []triggerConfigEntry,
	ecsServices []ecsServiceEntry,
	lbi *loadBalancerInfoEntry,
	style *deploymentStyleEntry,
	bgConfig *blueGreenConfigEntry,
	alarmConfig *alarmConfigEntry,
	autoRollback *autoRollbackConfigEntry,
	ec2TagSet *ec2TagSetEntry,
	onPremTagSet *onPremTagSetEntry,
) DeploymentGroupInput {
	input := DeploymentGroupInput{
		ServiceRoleArn:            serviceRoleArn,
		DeploymentConfigName:      deploymentConfigName,
		OutdatedInstancesStrategy: outdatedInstancesStrategy,
		TerminationHookEnabled:    terminationHookEnabled,
	}

	for _, f := range ec2TagFilters {
		input.Ec2TagFilters = append(input.Ec2TagFilters, TagFilter(f))
	}
	for _, f := range onPremTagFilters {
		input.OnPremisesInstanceTagFilters = append(input.OnPremisesInstanceTagFilters,
			TagFilter(f))
	}
	for _, asg := range autoScalingGroups {
		input.AutoScalingGroups = append(input.AutoScalingGroups, AutoScalingGroup(asg))
	}
	for _, tc := range triggerConfigs {
		input.TriggerConfigurations = append(input.TriggerConfigurations, TriggerConfiguration(tc))
	}
	for _, svc := range ecsServices {
		input.ECSServices = append(input.ECSServices, ECSService(svc))
	}

	if lbi != nil {
		lb := &LoadBalancerInfo{}
		for _, e := range lbi.ElbInfoList {
			lb.ElbInfoList = append(lb.ElbInfoList, ElbInfo(e))
		}
		for _, tg := range lbi.TargetGroupInfoList {
			lb.TargetGroupInfoList = append(lb.TargetGroupInfoList, TargetGroupInfo(tg))
		}
		for _, pair := range lbi.TargetGroupPairInfoList {
			p := TargetGroupPairInfo{}
			if pair.ProdTrafficRoute != nil {
				p.ProdTrafficRoute = &TrafficRoute{ListenerArns: pair.ProdTrafficRoute.ListenerArns}
			}
			if pair.TestTrafficRoute != nil {
				p.TestTrafficRoute = &TrafficRoute{ListenerArns: pair.TestTrafficRoute.ListenerArns}
			}
			for _, tg := range pair.TargetGroups {
				p.TargetGroups = append(p.TargetGroups, TargetGroupInfo(tg))
			}
			lb.TargetGroupPairInfoList = append(lb.TargetGroupPairInfoList, p)
		}
		input.LoadBalancerInfo = lb
	}

	if style != nil {
		input.DeploymentStyle = &DeploymentStyle{
			DeploymentType:   style.DeploymentType,
			DeploymentOption: style.DeploymentOption,
		}
	}

	if bgConfig != nil {
		bgc := &BlueGreenDeploymentConfiguration{}
		if bgConfig.TerminateBlueInstancesOnDeploymentSuccess != nil {
			tb := bgConfig.TerminateBlueInstancesOnDeploymentSuccess
			bgc.TerminateBlueInstancesOnDeploymentSuccess = &TerminateBlueInstancesOnDeploymentSuccess{
				Action:                       tb.Action,
				TerminationWaitTimeInMinutes: tb.TerminationWaitTimeInMinutes,
			}
		}
		if bgConfig.DeploymentReadyOption != nil {
			bgc.DeploymentReadyOption = &DeploymentReadyOption{
				ActionOnTimeout:   bgConfig.DeploymentReadyOption.ActionOnTimeout,
				WaitTimeInMinutes: bgConfig.DeploymentReadyOption.WaitTimeInMinutes,
			}
		}
		if bgConfig.GreenFleetProvisioningOption != nil {
			bgc.GreenFleetProvisioningOption = &GreenFleetProvisioningOption{
				Action: bgConfig.GreenFleetProvisioningOption.Action,
			}
		}
		input.BlueGreenDeploymentConfiguration = bgc
	}

	if alarmConfig != nil {
		ac := &AlarmConfiguration{
			Enabled:                alarmConfig.Enabled,
			IgnorePollAlarmFailure: alarmConfig.IgnorePollAlarmFailure,
		}
		for _, a := range alarmConfig.Alarms {
			ac.Alarms = append(ac.Alarms, Alarm(a))
		}
		input.AlarmConfiguration = ac
	}

	if autoRollback != nil {
		input.AutoRollbackConfiguration = &AutoRollbackConfiguration{
			Events:  autoRollback.Events,
			Enabled: autoRollback.Enabled,
		}
	}

	if ec2TagSet != nil {
		ets := &Ec2TagSet{}
		for _, group := range ec2TagSet.Ec2TagSetList {
			row := make([]TagFilter, 0, len(group))
			for _, f := range group {
				row = append(row, TagFilter(f))
			}
			ets.Ec2TagSetList = append(ets.Ec2TagSetList, row)
		}
		input.Ec2TagSet = ets
	}

	if onPremTagSet != nil {
		opts := &TagSet{}
		for _, group := range onPremTagSet.OnPremisesTagSetList {
			row := make([]TagFilter, 0, len(group))
			for _, f := range group {
				row = append(row, TagFilter(f))
			}
			opts.OnPremisesTagSetList = append(opts.OnPremisesTagSetList, row)
		}
		input.OnPremisesTagSet = opts
	}

	return input
}

type createDeploymentGroupInput struct {
	DeploymentStyle                  *deploymentStyleEntry    `json:"deploymentStyle"`
	OnPremisesTagSet                 *onPremTagSetEntry       `json:"onPremisesTagSet"`
	LoadBalancerInfo                 *loadBalancerInfoEntry   `json:"loadBalancerInfo"`
	Ec2TagSet                        *ec2TagSetEntry          `json:"ec2TagSet"`
	BlueGreenDeploymentConfiguration *blueGreenConfigEntry    `json:"blueGreenDeploymentConfiguration"`
	AlarmConfiguration               *alarmConfigEntry        `json:"alarmConfiguration"`
	AutoRollbackConfiguration        *autoRollbackConfigEntry `json:"autoRollbackConfiguration"`
	ServiceRoleArn                   string                   `json:"serviceRoleArn"`
	DeploymentGroupName              string                   `json:"deploymentGroupName"`
	ApplicationName                  string                   `json:"applicationName"`
	DeploymentConfigName             string                   `json:"deploymentConfigName"`
	OutdatedInstancesStrategy        string                   `json:"outdatedInstancesStrategy"`
	Tags                             []tagEntry               `json:"tags"`
	Ec2TagFilters                    []tagFilterEntry         `json:"ec2TagFilters"`
	OnPremisesInstanceTagFilters     []tagFilterEntry         `json:"onPremisesInstanceTagFilters"`
	AutoScalingGroups                []autoScalingGroupEntry  `json:"autoScalingGroups"`
	TriggerConfigurations            []triggerConfigEntry     `json:"triggerConfigurations"`
	ECSServices                      []ecsServiceEntry        `json:"ecsServices"`
	TerminationHookEnabled           bool                     `json:"terminationHookEnabled"`
}

type createDeploymentGroupOutput struct {
	DeploymentGroupID string `json:"deploymentGroupId"`
}

func (h *Handler) handleCreateDeploymentGroup(
	_ context.Context,
	in *createDeploymentGroupInput,
) (*createDeploymentGroupOutput, error) {
	if in.ApplicationName == "" || in.DeploymentGroupName == "" {
		return nil, fmt.Errorf("%w: applicationName and deploymentGroupName are required", errInvalidRequest)
	}

	input := dgInputFromWire(
		in.ServiceRoleArn, in.DeploymentConfigName, in.OutdatedInstancesStrategy,
		in.TerminationHookEnabled,
		in.Ec2TagFilters, in.OnPremisesInstanceTagFilters,
		in.AutoScalingGroups, in.TriggerConfigurations, in.ECSServices,
		in.LoadBalancerInfo, in.DeploymentStyle,
		in.BlueGreenDeploymentConfiguration, in.AlarmConfiguration,
		in.AutoRollbackConfiguration, in.Ec2TagSet, in.OnPremisesTagSet,
	)

	dg, err := h.Backend.CreateDeploymentGroup(
		in.ApplicationName, in.DeploymentGroupName,
		input,
		tagEntriesToMap(in.Tags),
	)
	if err != nil {
		return nil, err
	}

	return &createDeploymentGroupOutput{DeploymentGroupID: dg.DeploymentGroupID}, nil
}

type getDeploymentGroupInput struct {
	ApplicationName     string `json:"applicationName"`
	DeploymentGroupName string `json:"deploymentGroupName"`
}

type getDeploymentGroupOutput struct {
	DeploymentGroupInfo deploymentGroupInfoOutput `json:"deploymentGroupInfo"`
}

func (h *Handler) handleGetDeploymentGroup(
	_ context.Context,
	in *getDeploymentGroupInput,
) (*getDeploymentGroupOutput, error) {
	if in.ApplicationName == "" || in.DeploymentGroupName == "" {
		return nil, fmt.Errorf("%w: applicationName and deploymentGroupName are required", errInvalidRequest)
	}

	dg, err := h.Backend.GetDeploymentGroup(in.ApplicationName, in.DeploymentGroupName)
	if err != nil {
		return nil, err
	}

	return &getDeploymentGroupOutput{DeploymentGroupInfo: dgToOutput(dg)}, nil
}

type listDeploymentGroupsInput struct {
	ApplicationName string `json:"applicationName"`
}

type listDeploymentGroupsOutput struct {
	ApplicationName  string   `json:"applicationName"`
	DeploymentGroups []string `json:"deploymentGroups"`
}

func (h *Handler) handleListDeploymentGroups(
	_ context.Context,
	in *listDeploymentGroupsInput,
) (*listDeploymentGroupsOutput, error) {
	if in.ApplicationName == "" {
		return nil, fmt.Errorf("%w: applicationName is required", errInvalidRequest)
	}

	names, err := h.Backend.ListDeploymentGroups(in.ApplicationName)
	if err != nil {
		return nil, err
	}

	return &listDeploymentGroupsOutput{
		ApplicationName:  in.ApplicationName,
		DeploymentGroups: names,
	}, nil
}

type deleteDeploymentGroupInput struct {
	ApplicationName     string `json:"applicationName"`
	DeploymentGroupName string `json:"deploymentGroupName"`
}

type deleteDeploymentGroupOutput struct{}

func (h *Handler) handleDeleteDeploymentGroup(
	_ context.Context,
	in *deleteDeploymentGroupInput,
) (*deleteDeploymentGroupOutput, error) {
	if err := h.Backend.DeleteDeploymentGroup(in.ApplicationName, in.DeploymentGroupName); err != nil {
		return nil, err
	}

	return &deleteDeploymentGroupOutput{}, nil
}

type createDeploymentInput struct {
	Revision                      *revisionLocationInput `json:"revision"`
	ApplicationName               string                 `json:"applicationName"`
	DeploymentGroupName           string                 `json:"deploymentGroupName"`
	Description                   string                 `json:"description"`
	FileExistsBehavior            string                 `json:"fileExistsBehavior"`
	UpdateOutdatedInstancesOnly   bool                   `json:"updateOutdatedInstancesOnly"`
	IgnoreApplicationStopFailures bool                   `json:"ignoreApplicationStopFailures"`
}

type createDeploymentOutput struct {
	DeploymentID string `json:"deploymentId"`
}

func (h *Handler) handleCreateDeployment(
	_ context.Context,
	in *createDeploymentInput,
) (*createDeploymentOutput, error) {
	if in.ApplicationName == "" || in.DeploymentGroupName == "" {
		return nil, fmt.Errorf("%w: applicationName and deploymentGroupName are required", errInvalidRequest)
	}

	opts := DeploymentOptions{
		Description:                   in.Description,
		FileExistsBehavior:            in.FileExistsBehavior,
		UpdateOutdatedInstancesOnly:   in.UpdateOutdatedInstancesOnly,
		IgnoreApplicationStopFailures: in.IgnoreApplicationStopFailures,
		Creator:                       "user",
		Revision:                      revisionFromWire(in.Revision),
	}

	d, err := h.Backend.CreateDeployment(in.ApplicationName, in.DeploymentGroupName, opts)
	if err != nil {
		return nil, err
	}

	return &createDeploymentOutput{DeploymentID: d.DeploymentID}, nil
}

type getDeploymentInput struct {
	DeploymentID string `json:"deploymentId"`
}

// deploymentOverview holds a summary of instance counts for a deployment.
type deploymentOverview struct {
	Pending    int64 `json:"Pending"`
	InProgress int64 `json:"InProgress"`
	Succeeded  int64 `json:"Succeeded"`
	Failed     int64 `json:"Failed"`
	Skipped    int64 `json:"Skipped"`
	Ready      int64 `json:"Ready"`
}

type deploymentInfo struct {
	DeploymentOverview            *deploymentOverview    `json:"deploymentOverview,omitempty"`
	Revision                      *revisionLocationInput `json:"revision,omitempty"`
	CompleteTime                  *int64                 `json:"completeTime,omitempty"`
	DeploymentID                  string                 `json:"deploymentId"`
	ApplicationName               string                 `json:"applicationName"`
	DeploymentGroupName           string                 `json:"deploymentGroupName"`
	DeploymentConfigName          string                 `json:"deploymentConfigName,omitempty"`
	Status                        string                 `json:"status"`
	Creator                       string                 `json:"creator"`
	Description                   string                 `json:"description,omitempty"`
	FileExistsBehavior            string                 `json:"fileExistsBehavior,omitempty"`
	CreateTime                    int64                  `json:"createTime"`
	UpdateOutdatedInstancesOnly   bool                   `json:"updateOutdatedInstancesOnly,omitempty"`
	IgnoreApplicationStopFailures bool                   `json:"ignoreApplicationStopFailures,omitempty"`
}

type getDeploymentOutput struct {
	DeploymentInfo deploymentInfo `json:"deploymentInfo"`
}

func (h *Handler) handleGetDeployment(
	_ context.Context,
	in *getDeploymentInput,
) (*getDeploymentOutput, error) {
	if in.DeploymentID == "" {
		return nil, fmt.Errorf("%w: deploymentId is required", errInvalidRequest)
	}

	d, err := h.Backend.GetDeployment(in.DeploymentID)
	if err != nil {
		return nil, err
	}

	info := deploymentInfo{
		DeploymentID:                  d.DeploymentID,
		ApplicationName:               d.ApplicationName,
		DeploymentGroupName:           d.DeploymentGroupName,
		DeploymentConfigName:          d.DeploymentConfigName,
		Status:                        d.Status,
		Creator:                       d.Creator,
		CreateTime:                    d.CreateTime.UnixMilli(),
		Description:                   d.Description,
		FileExistsBehavior:            d.FileExistsBehavior,
		UpdateOutdatedInstancesOnly:   d.UpdateOutdatedInstancesOnly,
		IgnoreApplicationStopFailures: d.IgnoreApplicationStopFailures,
		Revision:                      revisionToWire(d.Revision),
		DeploymentOverview:            deploymentOverviewForStatus(d.Status),
	}

	if d.CompleteTime != nil {
		ms := d.CompleteTime.UnixMilli()
		info.CompleteTime = &ms
	}

	return &getDeploymentOutput{DeploymentInfo: info}, nil
}

// timeRangeEntry is the wire format for a create-time range filter.
type timeRangeEntry struct {
	Start *int64 `json:"start,omitempty"`
	End   *int64 `json:"end,omitempty"`
}

type listDeploymentsInput struct {
	CreateTimeRange     *timeRangeEntry `json:"createTimeRange"`
	ApplicationName     string          `json:"applicationName"`
	DeploymentGroupName string          `json:"deploymentGroupName"`
	IncludeOnlyStatuses []string        `json:"includeOnlyStatuses"`
}

type listDeploymentsOutput struct {
	Deployments []string `json:"deployments"`
}

func (h *Handler) handleListDeployments(
	_ context.Context,
	in *listDeploymentsInput,
) (*listDeploymentsOutput, error) {
	filter := DeploymentFilter{
		ApplicationName:     in.ApplicationName,
		DeploymentGroupName: in.DeploymentGroupName,
		Statuses:            in.IncludeOnlyStatuses,
	}

	if in.CreateTimeRange != nil {
		if in.CreateTimeRange.Start != nil {
			t := time.UnixMilli(*in.CreateTimeRange.Start).UTC()
			filter.CreateTimeStart = &t
		}
		if in.CreateTimeRange.End != nil {
			t := time.UnixMilli(*in.CreateTimeRange.End).UTC()
			filter.CreateTimeEnd = &t
		}
	}

	return &listDeploymentsOutput{
		Deployments: h.Backend.ListDeployments(filter),
	}, nil
}

type tagResourceInput struct {
	ResourceArn string     `json:"resourceArn"`
	Tags        []tagEntry `json:"tags"`
}

type tagResourceOutput struct{}

func (h *Handler) handleTagResource(
	_ context.Context,
	in *tagResourceInput,
) (*tagResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.TagResource(in.ResourceArn, tagEntriesToMap(in.Tags)); err != nil {
		return nil, err
	}

	return &tagResourceOutput{}, nil
}

type untagResourceInput struct {
	ResourceArn string   `json:"resourceArn"`
	TagKeys     []string `json:"tagKeys"`
}

type untagResourceOutput struct{}

func (h *Handler) handleUntagResource(
	_ context.Context,
	in *untagResourceInput,
) (*untagResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(in.ResourceArn, in.TagKeys); err != nil {
		return nil, err
	}

	return &untagResourceOutput{}, nil
}

type listTagsForResourceInput struct {
	ResourceArn string `json:"resourceArn"`
}

type listTagsForResourceOutput struct {
	Tags []tagEntry `json:"tags"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	kv, err := h.Backend.ListTagsForResource(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &listTagsForResourceOutput{Tags: tagsToSortedSlice(kv)}, nil
}

// tagEntry is a key-value tag pair for JSON (de)serialization.
type tagEntry struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// tagsToSortedSlice converts a tag map to a deterministically-sorted slice of tagEntry.
func tagsToSortedSlice(kv map[string]string) []tagEntry {
	entries := make([]tagEntry, 0, len(kv))
	for k, v := range kv {
		entries = append(entries, tagEntry{Key: k, Value: v})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key < entries[j].Key
	})

	return entries
}

// tagEntriesToMap converts a slice of tag entries to a map.
func tagEntriesToMap(entries []tagEntry) map[string]string {
	if len(entries) == 0 {
		return nil
	}

	m := make(map[string]string, len(entries))
	for _, e := range entries {
		m[e.Key] = e.Value
	}

	return m
}

// revisionFromWire converts a wire revisionLocationInput to a backend RevisionLocation.
func revisionFromWire(r *revisionLocationInput) *RevisionLocation {
	if r == nil {
		return nil
	}

	out := &RevisionLocation{RevisionType: r.RevisionType}

	if r.S3Location != nil {
		out.S3Location = &RevisionS3Location{
			Bucket:     r.S3Location.Bucket,
			Key:        r.S3Location.Key,
			BundleType: r.S3Location.BundleType,
			ETag:       r.S3Location.ETag,
			Version:    r.S3Location.Version,
		}
	}

	if r.GitHubLocation != nil {
		out.GitHubLocation = &RevisionGitHubLocation{
			Repository: r.GitHubLocation.Repository,
			CommitID:   r.GitHubLocation.CommitID,
		}
	}

	if r.AppSpecContent != nil {
		out.AppSpecContent = &RevisionAppSpecContent{
			Content: r.AppSpecContent.Content,
			Sha256:  r.AppSpecContent.Sha256,
		}
	}

	return out
}

// revisionToWire converts a backend RevisionLocation to the wire revisionLocationInput.
func revisionToWire(r *RevisionLocation) *revisionLocationInput {
	if r == nil {
		return nil
	}

	out := &revisionLocationInput{RevisionType: r.RevisionType}

	if r.S3Location != nil {
		out.S3Location = &s3LocationEntry{
			Bucket:     r.S3Location.Bucket,
			Key:        r.S3Location.Key,
			BundleType: r.S3Location.BundleType,
			ETag:       r.S3Location.ETag,
			Version:    r.S3Location.Version,
		}
	}

	if r.GitHubLocation != nil {
		out.GitHubLocation = &gitHubLocationEntry{
			Repository: r.GitHubLocation.Repository,
			CommitID:   r.GitHubLocation.CommitID,
		}
	}

	if r.AppSpecContent != nil {
		out.AppSpecContent = &appSpecContentEntry{
			Content: r.AppSpecContent.Content,
			Sha256:  r.AppSpecContent.Sha256,
		}
	}

	return out
}

// deploymentOverviewForStatus returns a synthetic DeploymentOverview based on deployment status.
func deploymentOverviewForStatus(status string) *deploymentOverview {
	switch status {
	case statusSucceeded:
		return &deploymentOverview{Succeeded: 1}
	case "Failed":
		return &deploymentOverview{Failed: 1}
	case statusStopped:
		return &deploymentOverview{Skipped: 1}
	default:
		return &deploymentOverview{InProgress: 1}
	}
}

// --- New operations ---

type addTagsToOnPremisesInstancesInput struct {
	InstanceNames []string   `json:"instanceNames"`
	Tags          []tagEntry `json:"tags"`
}

type addTagsToOnPremisesInstancesOutput struct{}

func (h *Handler) handleAddTagsToOnPremisesInstances(
	_ context.Context,
	in *addTagsToOnPremisesInstancesInput,
) (*addTagsToOnPremisesInstancesOutput, error) {
	if len(in.InstanceNames) == 0 {
		return nil, fmt.Errorf("%w: instanceNames is required", errInvalidRequest)
	}

	if err := h.Backend.AddTagsToOnPremisesInstances(in.InstanceNames, tagEntriesToMap(in.Tags)); err != nil {
		return nil, err
	}

	return &addTagsToOnPremisesInstancesOutput{}, nil
}

// s3LocationEntry is the wire format for an S3 deployment revision.
type s3LocationEntry struct {
	Bucket     string `json:"bucket,omitempty"`
	Key        string `json:"key,omitempty"`
	BundleType string `json:"bundleType,omitempty"`
	ETag       string `json:"eTag,omitempty"`
	Version    string `json:"version,omitempty"`
}

// gitHubLocationEntry is the wire format for a GitHub deployment revision.
type gitHubLocationEntry struct {
	Repository string `json:"repository,omitempty"`
	CommitID   string `json:"commitId,omitempty"`
}

// appSpecContentEntry is the wire format for an AppSpecContent revision.
type appSpecContentEntry struct {
	Content string `json:"content,omitempty"`
	Sha256  string `json:"sha256,omitempty"`
}

type revisionLocationInput struct {
	S3Location     *s3LocationEntry     `json:"s3Location,omitempty"`
	GitHubLocation *gitHubLocationEntry `json:"gitHubLocation,omitempty"`
	AppSpecContent *appSpecContentEntry `json:"appSpecContent,omitempty"`
	RevisionType   string               `json:"revisionType"`
}

type revisionInfoOutput struct {
	RevisionLocation revisionLocationInput `json:"revisionLocation"`
}

type batchGetApplicationRevisionsInput struct {
	ApplicationName string                  `json:"applicationName"`
	Revisions       []revisionLocationInput `json:"revisions"`
}

type batchGetApplicationRevisionsOutput struct {
	ApplicationName string               `json:"applicationName"`
	ErrorMessage    string               `json:"errorMessage,omitempty"`
	Revisions       []revisionInfoOutput `json:"revisions"`
}

func (h *Handler) handleBatchGetApplicationRevisions(
	_ context.Context,
	in *batchGetApplicationRevisionsInput,
) (*batchGetApplicationRevisionsOutput, error) {
	if in.ApplicationName == "" {
		return nil, fmt.Errorf("%w: applicationName is required", errInvalidRequest)
	}

	appName, err := h.Backend.BatchGetApplicationRevisions(in.ApplicationName, len(in.Revisions))
	if err != nil {
		return nil, err
	}

	revisions := make([]revisionInfoOutput, 0, len(in.Revisions))
	for _, r := range in.Revisions {
		revisions = append(revisions, revisionInfoOutput{RevisionLocation: r})
	}

	return &batchGetApplicationRevisionsOutput{
		ApplicationName: appName,
		Revisions:       revisions,
	}, nil
}

type batchGetApplicationsInput struct {
	ApplicationNames []string `json:"applicationNames"`
}

type batchGetApplicationsOutput struct {
	ApplicationsInfo []applicationInfo `json:"applicationsInfo"`
}

func (h *Handler) handleBatchGetApplications(
	_ context.Context,
	in *batchGetApplicationsInput,
) (*batchGetApplicationsOutput, error) {
	if len(in.ApplicationNames) == 0 {
		return nil, fmt.Errorf("%w: applicationNames is required", errInvalidRequest)
	}

	apps := h.Backend.BatchGetApplications(in.ApplicationNames)

	infos := make([]applicationInfo, 0, len(apps))
	for _, app := range apps {
		infos = append(infos, applicationInfo{
			ApplicationID:   app.ApplicationID,
			ApplicationName: app.ApplicationName,
			ComputePlatform: app.ComputePlatform,
			CreateTime:      app.CreationTime.UnixMilli(),
		})
	}

	return &batchGetApplicationsOutput{ApplicationsInfo: infos}, nil
}

type batchGetDeploymentGroupsInput struct {
	ApplicationName      string   `json:"applicationName"`
	DeploymentGroupNames []string `json:"deploymentGroupNames"`
}

type batchGetDeploymentGroupsOutput struct {
	ErrorMessage         string                      `json:"errorMessage,omitempty"`
	DeploymentGroupsInfo []deploymentGroupInfoOutput `json:"deploymentGroupsInfo"`
}

func (h *Handler) handleBatchGetDeploymentGroups(
	_ context.Context,
	in *batchGetDeploymentGroupsInput,
) (*batchGetDeploymentGroupsOutput, error) {
	if in.ApplicationName == "" {
		return nil, fmt.Errorf("%w: applicationName is required", errInvalidRequest)
	}

	dgs, err := h.Backend.BatchGetDeploymentGroups(in.ApplicationName, in.DeploymentGroupNames)
	if err != nil {
		return nil, err
	}

	infos := make([]deploymentGroupInfoOutput, 0, len(dgs))
	for _, dg := range dgs {
		infos = append(infos, dgToOutput(dg))
	}

	return &batchGetDeploymentGroupsOutput{DeploymentGroupsInfo: infos}, nil
}

type batchGetDeploymentInstancesInput struct {
	DeploymentID string   `json:"deploymentId"`
	InstanceIDs  []string `json:"instanceIds"`
}

type batchGetDeploymentInstancesOutput struct {
	ErrorMessage     string                `json:"errorMessage,omitempty"`
	InstancesSummary []InstanceSummaryItem `json:"instancesSummary"`
}

func (h *Handler) handleBatchGetDeploymentInstances(
	_ context.Context,
	in *batchGetDeploymentInstancesInput,
) (*batchGetDeploymentInstancesOutput, error) {
	if in.DeploymentID == "" {
		return nil, fmt.Errorf("%w: deploymentId is required", errInvalidRequest)
	}

	items, err := h.Backend.BatchGetDeploymentInstances(in.DeploymentID, in.InstanceIDs)
	if err != nil {
		return nil, err
	}

	return &batchGetDeploymentInstancesOutput{
		InstancesSummary: items,
	}, nil
}

type batchGetDeploymentTargetsInput struct {
	DeploymentID string   `json:"deploymentId"`
	TargetIDs    []string `json:"targetIds"`
}

type batchGetDeploymentTargetsOutput struct {
	DeploymentTargets []DeploymentTargetItem `json:"deploymentTargets"`
}

func (h *Handler) handleBatchGetDeploymentTargets(
	_ context.Context,
	in *batchGetDeploymentTargetsInput,
) (*batchGetDeploymentTargetsOutput, error) {
	if in.DeploymentID == "" {
		return nil, fmt.Errorf("%w: deploymentId is required", errInvalidRequest)
	}

	items, err := h.Backend.BatchGetDeploymentTargets(in.DeploymentID, in.TargetIDs)
	if err != nil {
		return nil, err
	}

	targets := make([]DeploymentTargetItem, 0, len(items))
	for _, item := range items {
		targets = append(targets, *item)
	}

	return &batchGetDeploymentTargetsOutput{DeploymentTargets: targets}, nil
}

type batchGetDeploymentsInput struct {
	DeploymentIDs []string `json:"deploymentIds"`
}

type batchGetDeploymentsOutput struct {
	DeploymentsInfo []deploymentInfo `json:"deploymentsInfo"`
}

func (h *Handler) handleBatchGetDeployments(
	_ context.Context,
	in *batchGetDeploymentsInput,
) (*batchGetDeploymentsOutput, error) {
	if len(in.DeploymentIDs) == 0 {
		return nil, fmt.Errorf("%w: deploymentIds is required", errInvalidRequest)
	}

	deployments := h.Backend.BatchGetDeployments(in.DeploymentIDs)

	infos := make([]deploymentInfo, 0, len(deployments))
	for _, d := range deployments {
		info := deploymentInfo{
			DeploymentID:                  d.DeploymentID,
			ApplicationName:               d.ApplicationName,
			DeploymentGroupName:           d.DeploymentGroupName,
			DeploymentConfigName:          d.DeploymentConfigName,
			Status:                        d.Status,
			Creator:                       d.Creator,
			CreateTime:                    d.CreateTime.UnixMilli(),
			Description:                   d.Description,
			FileExistsBehavior:            d.FileExistsBehavior,
			UpdateOutdatedInstancesOnly:   d.UpdateOutdatedInstancesOnly,
			IgnoreApplicationStopFailures: d.IgnoreApplicationStopFailures,
			Revision:                      revisionToWire(d.Revision),
			DeploymentOverview:            deploymentOverviewForStatus(d.Status),
		}

		if d.CompleteTime != nil {
			ms := d.CompleteTime.UnixMilli()
			info.CompleteTime = &ms
		}

		infos = append(infos, info)
	}

	return &batchGetDeploymentsOutput{DeploymentsInfo: infos}, nil
}

type onPremisesInstanceInfo struct {
	DeregisterTime *int64     `json:"deregisterTime,omitempty"`
	InstanceName   string     `json:"instanceName"`
	IamSessionArn  string     `json:"iamSessionArn,omitempty"`
	IamUserArn     string     `json:"iamUserArn,omitempty"`
	Tags           []tagEntry `json:"tags"`
	RegisterTime   int64      `json:"registerTime"`
}

type batchGetOnPremisesInstancesInput struct {
	InstanceNames []string `json:"instanceNames"`
}

type batchGetOnPremisesInstancesOutput struct {
	InstanceInfos []onPremisesInstanceInfo `json:"instanceInfos"`
}

func (h *Handler) handleBatchGetOnPremisesInstances(
	_ context.Context,
	in *batchGetOnPremisesInstancesInput,
) (*batchGetOnPremisesInstancesOutput, error) {
	if len(in.InstanceNames) == 0 {
		return nil, fmt.Errorf("%w: instanceNames is required", errInvalidRequest)
	}

	instances := h.Backend.BatchGetOnPremisesInstances(in.InstanceNames)

	infos := make([]onPremisesInstanceInfo, 0, len(instances))
	for _, inst := range instances {
		info := onPremisesInstanceInfo{
			InstanceName:  inst.InstanceName,
			RegisterTime:  inst.RegisterTime.UnixMilli(),
			IamSessionArn: inst.IamSessionArn,
			IamUserArn:    inst.IamUserArn,
		}

		if inst.DeregisterTime != nil {
			ms := inst.DeregisterTime.UnixMilli()
			info.DeregisterTime = &ms
		}

		if inst.Tags != nil {
			info.Tags = tagsToSortedSlice(inst.Tags.Clone())
		} else {
			info.Tags = []tagEntry{}
		}

		infos = append(infos, info)
	}

	return &batchGetOnPremisesInstancesOutput{InstanceInfos: infos}, nil
}

type continueDeploymentInput struct {
	DeploymentID       string `json:"deploymentId"`
	DeploymentWaitType string `json:"deploymentWaitType"`
}

type continueDeploymentOutput struct{}

func (h *Handler) handleContinueDeployment(
	_ context.Context,
	in *continueDeploymentInput,
) (*continueDeploymentOutput, error) {
	if in.DeploymentID == "" {
		return nil, fmt.Errorf("%w: deploymentId is required", errInvalidRequest)
	}

	if err := h.Backend.ContinueDeployment(in.DeploymentID); err != nil {
		return nil, err
	}

	return &continueDeploymentOutput{}, nil
}

// minimumHealthyHostsEntry is the wire format for minimum healthy hosts config.
type minimumHealthyHostsEntry struct {
	Type  string `json:"type,omitempty"`
	Value int    `json:"value,omitempty"`
}

// timeBasedCanaryEntry is the wire format for canary traffic routing.
type timeBasedCanaryEntry struct {
	CanaryPercentage int `json:"canaryPercentage,omitempty"`
	CanaryInterval   int `json:"canaryInterval,omitempty"`
}

// timeBasedLinearEntry is the wire format for linear traffic routing.
type timeBasedLinearEntry struct {
	LinearPercentage int `json:"linearPercentage,omitempty"`
	LinearInterval   int `json:"linearInterval,omitempty"`
}

// trafficRoutingConfigEntry is the wire format for traffic routing configuration.
type trafficRoutingConfigEntry struct {
	TimeBasedCanary *timeBasedCanaryEntry `json:"timeBasedCanary,omitempty"`
	TimeBasedLinear *timeBasedLinearEntry `json:"timeBasedLinear,omitempty"`
	Type            string                `json:"type,omitempty"`
}

// zonalConfigEntry is the wire format for zonal deployment configuration.
type zonalConfigEntry struct {
	MinimumHealthyHostsPerZone        *minimumHealthyHostsEntry `json:"minimumHealthyHostsPerZone,omitempty"`
	FirstZoneMonitorDurationInSeconds int                       `json:"firstZoneMonitorDurationInSeconds,omitempty"`
	MonitorDurationInSeconds          int                       `json:"monitorDurationInSeconds,omitempty"`
}

type createDeploymentConfigInput struct {
	MinimumHealthyHosts  *minimumHealthyHostsEntry  `json:"minimumHealthyHosts"`
	TrafficRoutingConfig *trafficRoutingConfigEntry `json:"trafficRoutingConfig"`
	ZonalConfig          *zonalConfigEntry          `json:"zonalConfig"`
	DeploymentConfigName string                     `json:"deploymentConfigName"`
	ComputePlatform      string                     `json:"computePlatform"`
}

type createDeploymentConfigOutput struct {
	DeploymentConfigID string `json:"deploymentConfigId"`
}

func (h *Handler) handleCreateDeploymentConfig(
	_ context.Context,
	in *createDeploymentConfigInput,
) (*createDeploymentConfigOutput, error) {
	if in.DeploymentConfigName == "" {
		return nil, fmt.Errorf("%w: deploymentConfigName is required", errInvalidRequest)
	}

	var mhh *MinimumHealthyHosts
	if in.MinimumHealthyHosts != nil {
		mhh = &MinimumHealthyHosts{Type: in.MinimumHealthyHosts.Type, Value: in.MinimumHealthyHosts.Value}
	}

	var trc *TrafficRoutingConfig
	if in.TrafficRoutingConfig != nil {
		trc = &TrafficRoutingConfig{Type: in.TrafficRoutingConfig.Type}
		if in.TrafficRoutingConfig.TimeBasedCanary != nil {
			trc.TimeBasedCanary = &TimeBasedCanary{
				CanaryPercentage: in.TrafficRoutingConfig.TimeBasedCanary.CanaryPercentage,
				CanaryInterval:   in.TrafficRoutingConfig.TimeBasedCanary.CanaryInterval,
			}
		}
		if in.TrafficRoutingConfig.TimeBasedLinear != nil {
			trc.TimeBasedLinear = &TimeBasedLinear{
				LinearPercentage: in.TrafficRoutingConfig.TimeBasedLinear.LinearPercentage,
				LinearInterval:   in.TrafficRoutingConfig.TimeBasedLinear.LinearInterval,
			}
		}
	}

	var zc *ZonalConfig
	if in.ZonalConfig != nil {
		zc = &ZonalConfig{
			FirstZoneMonitorDurationInSeconds: in.ZonalConfig.FirstZoneMonitorDurationInSeconds,
			MonitorDurationInSeconds:          in.ZonalConfig.MonitorDurationInSeconds,
		}
		if in.ZonalConfig.MinimumHealthyHostsPerZone != nil {
			zc.MinimumHealthyHostsPerZone = &MinimumHealthyHosts{
				Type:  in.ZonalConfig.MinimumHealthyHostsPerZone.Type,
				Value: in.ZonalConfig.MinimumHealthyHostsPerZone.Value,
			}
		}
	}

	cfg, err := h.Backend.CreateDeploymentConfig(in.DeploymentConfigName, in.ComputePlatform, mhh, trc, zc)
	if err != nil {
		return nil, err
	}

	return &createDeploymentConfigOutput{DeploymentConfigID: cfg.DeploymentConfigID}, nil
}

// --- Additional ops (stubs and full implementations) ---

type updateApplicationInput struct {
	ApplicationName    string `json:"applicationName"`
	NewApplicationName string `json:"newApplicationName"`
}

type updateApplicationOutput struct{}

func (h *Handler) handleUpdateApplication(
	_ context.Context,
	in *updateApplicationInput,
) (*updateApplicationOutput, error) {
	if in.ApplicationName == "" {
		return nil, fmt.Errorf("%w: applicationName is required", errInvalidRequest)
	}

	if err := h.Backend.UpdateApplication(in.ApplicationName, in.NewApplicationName); err != nil {
		return nil, err
	}

	return &updateApplicationOutput{}, nil
}

type updateDeploymentGroupInput struct {
	AutoRollbackConfiguration        *autoRollbackConfigEntry `json:"autoRollbackConfiguration"`
	OnPremisesTagSet                 *onPremTagSetEntry       `json:"onPremisesTagSet"`
	Ec2TagSet                        *ec2TagSetEntry          `json:"ec2TagSet"`
	DeploymentStyle                  *deploymentStyleEntry    `json:"deploymentStyle"`
	LoadBalancerInfo                 *loadBalancerInfoEntry   `json:"loadBalancerInfo"`
	BlueGreenDeploymentConfiguration *blueGreenConfigEntry    `json:"blueGreenDeploymentConfiguration"`
	AlarmConfiguration               *alarmConfigEntry        `json:"alarmConfiguration"`
	DeploymentConfigName             string                   `json:"deploymentConfigName"`
	ApplicationName                  string                   `json:"applicationName"`
	ServiceRoleArn                   string                   `json:"serviceRoleArn"`
	NewDeploymentGroupName           string                   `json:"newDeploymentGroupName"`
	CurrentDeploymentGroupName       string                   `json:"currentDeploymentGroupName"`
	OutdatedInstancesStrategy        string                   `json:"outdatedInstancesStrategy"`
	Ec2TagFilters                    []tagFilterEntry         `json:"ec2TagFilters"`
	OnPremisesInstanceTagFilters     []tagFilterEntry         `json:"onPremisesInstanceTagFilters"`
	AutoScalingGroups                []autoScalingGroupEntry  `json:"autoScalingGroups"`
	TriggerConfigurations            []triggerConfigEntry     `json:"triggerConfigurations"`
	ECSServices                      []ecsServiceEntry        `json:"ecsServices"`
	TerminationHookEnabled           bool                     `json:"terminationHookEnabled"`
}

type updateDeploymentGroupOutput struct {
	HooksNotCleanedUp bool `json:"hooksNotCleanedUp,omitempty"`
}

func (h *Handler) handleUpdateDeploymentGroup(
	_ context.Context,
	in *updateDeploymentGroupInput,
) (*updateDeploymentGroupOutput, error) {
	if in.ApplicationName == "" || in.CurrentDeploymentGroupName == "" {
		return nil, fmt.Errorf("%w: applicationName and currentDeploymentGroupName are required", errInvalidRequest)
	}

	input := dgInputFromWire(
		in.ServiceRoleArn, in.DeploymentConfigName, in.OutdatedInstancesStrategy,
		in.TerminationHookEnabled,
		in.Ec2TagFilters, in.OnPremisesInstanceTagFilters,
		in.AutoScalingGroups, in.TriggerConfigurations, in.ECSServices,
		in.LoadBalancerInfo, in.DeploymentStyle,
		in.BlueGreenDeploymentConfiguration, in.AlarmConfiguration,
		in.AutoRollbackConfiguration, in.Ec2TagSet, in.OnPremisesTagSet,
	)

	hooks, err := h.Backend.UpdateDeploymentGroup(
		in.ApplicationName, in.CurrentDeploymentGroupName, in.NewDeploymentGroupName,
		input,
	)
	if err != nil {
		return nil, err
	}

	return &updateDeploymentGroupOutput{HooksNotCleanedUp: hooks}, nil
}

type stopDeploymentInput struct {
	DeploymentID        string `json:"deploymentId"`
	AutoRollbackEnabled bool   `json:"autoRollbackEnabled"`
}

type stopDeploymentOutput struct {
	Status string `json:"status"`
}

func (h *Handler) handleStopDeployment(
	_ context.Context,
	in *stopDeploymentInput,
) (*stopDeploymentOutput, error) {
	if in.DeploymentID == "" {
		return nil, fmt.Errorf("%w: deploymentId is required", errInvalidRequest)
	}

	if err := h.Backend.StopDeployment(in.DeploymentID); err != nil {
		return nil, err
	}

	return &stopDeploymentOutput{Status: statusStopped}, nil
}

type skipWaitTimeInput struct {
	DeploymentID string `json:"deploymentId"`
}

type skipWaitTimeOutput struct{}

func (h *Handler) handleSkipWaitTimeForInstanceTermination(
	_ context.Context,
	in *skipWaitTimeInput,
) (*skipWaitTimeOutput, error) {
	if in.DeploymentID == "" {
		return nil, fmt.Errorf("%w: deploymentId is required", errInvalidRequest)
	}

	return &skipWaitTimeOutput{}, nil
}

type getDeploymentConfigInput struct {
	DeploymentConfigName string `json:"deploymentConfigName"`
}

type deploymentConfigInfo struct {
	MinimumHealthyHosts  *minimumHealthyHostsEntry  `json:"minimumHealthyHosts,omitempty"`
	TrafficRoutingConfig *trafficRoutingConfigEntry `json:"trafficRoutingConfig,omitempty"`
	ZonalConfig          *zonalConfigEntry          `json:"zonalConfig,omitempty"`
	DeploymentConfigID   string                     `json:"deploymentConfigId"`
	DeploymentConfigName string                     `json:"deploymentConfigName"`
	ComputePlatform      string                     `json:"computePlatform"`
	CreateTime           int64                      `json:"createTime"`
}

type getDeploymentConfigOutput struct {
	DeploymentConfigInfo deploymentConfigInfo `json:"deploymentConfigInfo"`
}

func deploymentConfigToInfo(cfg *DeploymentConfig) deploymentConfigInfo {
	info := deploymentConfigInfo{
		DeploymentConfigID:   cfg.DeploymentConfigID,
		DeploymentConfigName: cfg.DeploymentConfigName,
		ComputePlatform:      cfg.ComputePlatform,
		CreateTime:           cfg.CreateTime.UnixMilli(),
	}

	if cfg.MinimumHealthyHosts != nil {
		info.MinimumHealthyHosts = &minimumHealthyHostsEntry{
			Type:  cfg.MinimumHealthyHosts.Type,
			Value: cfg.MinimumHealthyHosts.Value,
		}
	}

	if cfg.TrafficRoutingConfig != nil {
		trc := &trafficRoutingConfigEntry{Type: cfg.TrafficRoutingConfig.Type}
		if cfg.TrafficRoutingConfig.TimeBasedCanary != nil {
			trc.TimeBasedCanary = &timeBasedCanaryEntry{
				CanaryPercentage: cfg.TrafficRoutingConfig.TimeBasedCanary.CanaryPercentage,
				CanaryInterval:   cfg.TrafficRoutingConfig.TimeBasedCanary.CanaryInterval,
			}
		}
		if cfg.TrafficRoutingConfig.TimeBasedLinear != nil {
			trc.TimeBasedLinear = &timeBasedLinearEntry{
				LinearPercentage: cfg.TrafficRoutingConfig.TimeBasedLinear.LinearPercentage,
				LinearInterval:   cfg.TrafficRoutingConfig.TimeBasedLinear.LinearInterval,
			}
		}
		info.TrafficRoutingConfig = trc
	}

	if cfg.ZonalConfig != nil {
		zc := &zonalConfigEntry{
			FirstZoneMonitorDurationInSeconds: cfg.ZonalConfig.FirstZoneMonitorDurationInSeconds,
			MonitorDurationInSeconds:          cfg.ZonalConfig.MonitorDurationInSeconds,
		}
		if cfg.ZonalConfig.MinimumHealthyHostsPerZone != nil {
			zc.MinimumHealthyHostsPerZone = &minimumHealthyHostsEntry{
				Type:  cfg.ZonalConfig.MinimumHealthyHostsPerZone.Type,
				Value: cfg.ZonalConfig.MinimumHealthyHostsPerZone.Value,
			}
		}
		info.ZonalConfig = zc
	}

	return info
}

func (h *Handler) handleGetDeploymentConfig(
	_ context.Context,
	in *getDeploymentConfigInput,
) (*getDeploymentConfigOutput, error) {
	if in.DeploymentConfigName == "" {
		return nil, fmt.Errorf("%w: deploymentConfigName is required", errInvalidRequest)
	}

	cfg, err := h.Backend.GetDeploymentConfig(in.DeploymentConfigName)
	if err != nil {
		return nil, err
	}

	return &getDeploymentConfigOutput{DeploymentConfigInfo: deploymentConfigToInfo(cfg)}, nil
}

type listDeploymentConfigsInput struct{}

type listDeploymentConfigsOutput struct {
	DeploymentConfigsList []string `json:"deploymentConfigsList"`
}

func (h *Handler) handleListDeploymentConfigs(
	_ context.Context,
	_ *listDeploymentConfigsInput,
) (*listDeploymentConfigsOutput, error) {
	return &listDeploymentConfigsOutput{DeploymentConfigsList: h.Backend.ListDeploymentConfigs()}, nil
}

type deleteDeploymentConfigInput struct {
	DeploymentConfigName string `json:"deploymentConfigName"`
}

type deleteDeploymentConfigOutput struct{}

func (h *Handler) handleDeleteDeploymentConfig(
	_ context.Context,
	in *deleteDeploymentConfigInput,
) (*deleteDeploymentConfigOutput, error) {
	if in.DeploymentConfigName == "" {
		return nil, fmt.Errorf("%w: deploymentConfigName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteDeploymentConfig(in.DeploymentConfigName); err != nil {
		return nil, err
	}

	return &deleteDeploymentConfigOutput{}, nil
}

type removeTagsFromOnPremisesInstancesInput struct {
	InstanceNames []string   `json:"instanceNames"`
	Tags          []tagEntry `json:"tags"`
}

type removeTagsFromOnPremisesInstancesOutput struct{}

func (h *Handler) handleRemoveTagsFromOnPremisesInstances(
	_ context.Context,
	in *removeTagsFromOnPremisesInstancesInput,
) (*removeTagsFromOnPremisesInstancesOutput, error) {
	if len(in.InstanceNames) == 0 {
		return nil, fmt.Errorf("%w: instanceNames is required", errInvalidRequest)
	}

	keys := make([]string, 0, len(in.Tags))
	for _, t := range in.Tags {
		keys = append(keys, t.Key)
	}

	if err := h.Backend.RemoveTagsFromOnPremisesInstances(in.InstanceNames, keys); err != nil {
		return nil, err
	}

	return &removeTagsFromOnPremisesInstancesOutput{}, nil
}

type registerOnPremisesInstanceInput struct {
	InstanceName  string `json:"instanceName"`
	IamSessionArn string `json:"iamSessionArn"`
	IamUserArn    string `json:"iamUserArn"`
}

type registerOnPremisesInstanceOutput struct{}

func (h *Handler) handleRegisterOnPremisesInstance(
	_ context.Context,
	in *registerOnPremisesInstanceInput,
) (*registerOnPremisesInstanceOutput, error) {
	if in.InstanceName == "" {
		return nil, fmt.Errorf("%w: instanceName is required", errInvalidRequest)
	}

	if err := h.Backend.RegisterOnPremisesInstance(in.InstanceName, in.IamSessionArn, in.IamUserArn); err != nil {
		return nil, err
	}

	return &registerOnPremisesInstanceOutput{}, nil
}

type deregisterOnPremisesInstanceInput struct {
	InstanceName string `json:"instanceName"`
}

type deregisterOnPremisesInstanceOutput struct{}

func (h *Handler) handleDeregisterOnPremisesInstance(
	_ context.Context,
	in *deregisterOnPremisesInstanceInput,
) (*deregisterOnPremisesInstanceOutput, error) {
	if in.InstanceName == "" {
		return nil, fmt.Errorf("%w: instanceName is required", errInvalidRequest)
	}

	if err := h.Backend.DeregisterOnPremisesInstance(in.InstanceName); err != nil {
		return nil, err
	}

	return &deregisterOnPremisesInstanceOutput{}, nil
}

type getOnPremisesInstanceInput struct {
	InstanceName string `json:"instanceName"`
}

type getOnPremisesInstanceOutput struct {
	InstanceInfo onPremisesInstanceInfo `json:"instanceInfo"`
}

func (h *Handler) handleGetOnPremisesInstance(
	_ context.Context,
	in *getOnPremisesInstanceInput,
) (*getOnPremisesInstanceOutput, error) {
	if in.InstanceName == "" {
		return nil, fmt.Errorf("%w: instanceName is required", errInvalidRequest)
	}

	inst, err := h.Backend.GetOnPremisesInstance(in.InstanceName)
	if err != nil {
		return nil, err
	}

	info := onPremisesInstanceInfo{
		InstanceName:  inst.InstanceName,
		RegisterTime:  inst.RegisterTime.UnixMilli(),
		IamSessionArn: inst.IamSessionArn,
		IamUserArn:    inst.IamUserArn,
	}

	if inst.DeregisterTime != nil {
		ms := inst.DeregisterTime.UnixMilli()
		info.DeregisterTime = &ms
	}

	if inst.Tags != nil {
		info.Tags = tagsToSortedSlice(inst.Tags.Clone())
	} else {
		info.Tags = []tagEntry{}
	}

	return &getOnPremisesInstanceOutput{InstanceInfo: info}, nil
}

type listOnPremisesInstancesInput struct {
	RegistrationStatus string           `json:"registrationStatus"`
	TagFilters         []tagFilterEntry `json:"tagFilters"`
}

type listOnPremisesInstancesOutput struct {
	InstanceNames []string `json:"instanceNames"`
}

func (h *Handler) handleListOnPremisesInstances(
	_ context.Context,
	in *listOnPremisesInstancesInput,
) (*listOnPremisesInstancesOutput, error) {
	filters := make([]TagFilter, 0, len(in.TagFilters))
	for _, f := range in.TagFilters {
		filters = append(filters, TagFilter(f))
	}

	return &listOnPremisesInstancesOutput{
		InstanceNames: h.Backend.ListOnPremisesInstances(in.RegistrationStatus, filters),
	}, nil
}

type registerApplicationRevisionInput struct {
	ApplicationName string                `json:"applicationName"`
	Description     string                `json:"description"`
	Revision        revisionLocationInput `json:"revision"`
}

type registerApplicationRevisionOutput struct{}

func (h *Handler) handleRegisterApplicationRevision(
	_ context.Context,
	in *registerApplicationRevisionInput,
) (*registerApplicationRevisionOutput, error) {
	if in.ApplicationName == "" {
		return nil, fmt.Errorf("%w: applicationName is required", errInvalidRequest)
	}

	if _, err := h.Backend.GetApplication(in.ApplicationName); err != nil {
		return nil, err
	}

	return &registerApplicationRevisionOutput{}, nil
}

type getApplicationRevisionInput struct {
	ApplicationName string                `json:"applicationName"`
	Revision        revisionLocationInput `json:"revision"`
}

type getApplicationRevisionOutput struct {
	ApplicationName string                `json:"applicationName"`
	Revision        revisionLocationInput `json:"revision"`
}

func (h *Handler) handleGetApplicationRevision(
	_ context.Context,
	in *getApplicationRevisionInput,
) (*getApplicationRevisionOutput, error) {
	if in.ApplicationName == "" {
		return nil, fmt.Errorf("%w: applicationName is required", errInvalidRequest)
	}

	if _, err := h.Backend.GetApplication(in.ApplicationName); err != nil {
		return nil, err
	}

	return &getApplicationRevisionOutput{
		ApplicationName: in.ApplicationName,
		Revision:        in.Revision,
	}, nil
}

type listApplicationRevisionsInput struct {
	ApplicationName string `json:"applicationName"`
}

type listApplicationRevisionsOutput struct {
	Revisions []revisionLocationInput `json:"revisions"`
}

func (h *Handler) handleListApplicationRevisions(
	_ context.Context,
	in *listApplicationRevisionsInput,
) (*listApplicationRevisionsOutput, error) {
	if in.ApplicationName == "" {
		return nil, fmt.Errorf("%w: applicationName is required", errInvalidRequest)
	}

	if _, err := h.Backend.GetApplication(in.ApplicationName); err != nil {
		return nil, err
	}

	return &listApplicationRevisionsOutput{Revisions: []revisionLocationInput{}}, nil
}

type getDeploymentInstanceInput struct {
	DeploymentID string `json:"deploymentId"`
	InstanceID   string `json:"instanceId"`
}

type getDeploymentInstanceOutput struct {
	InstanceSummary InstanceSummaryItem `json:"instanceSummary"`
}

func (h *Handler) handleGetDeploymentInstance(
	_ context.Context,
	in *getDeploymentInstanceInput,
) (*getDeploymentInstanceOutput, error) {
	if in.DeploymentID == "" || in.InstanceID == "" {
		return nil, fmt.Errorf("%w: deploymentId and instanceId are required", errInvalidRequest)
	}

	if _, err := h.Backend.GetDeployment(in.DeploymentID); err != nil {
		return nil, err
	}

	return &getDeploymentInstanceOutput{
		InstanceSummary: InstanceSummaryItem{
			DeploymentID: in.DeploymentID,
			InstanceID:   in.InstanceID,
			Status:       statusSucceeded,
		},
	}, nil
}

type getDeploymentTargetInput struct {
	DeploymentID string `json:"deploymentId"`
	TargetID     string `json:"targetId"`
}

type getDeploymentTargetOutput struct {
	DeploymentTarget DeploymentTargetItem `json:"deploymentTarget"`
}

func (h *Handler) handleGetDeploymentTarget(
	_ context.Context,
	in *getDeploymentTargetInput,
) (*getDeploymentTargetOutput, error) {
	if in.DeploymentID == "" || in.TargetID == "" {
		return nil, fmt.Errorf("%w: deploymentId and targetId are required", errInvalidRequest)
	}

	if _, err := h.Backend.GetDeployment(in.DeploymentID); err != nil {
		return nil, err
	}

	return &getDeploymentTargetOutput{
		DeploymentTarget: DeploymentTargetItem{
			DeploymentID: in.DeploymentID,
			TargetID:     in.TargetID,
			Status:       statusSucceeded,
			TargetType:   "instanceTarget",
		},
	}, nil
}

type listDeploymentInstancesInput struct {
	DeploymentID string `json:"deploymentId"`
}

type listDeploymentInstancesOutput struct {
	InstancesList []string `json:"instancesList"`
}

func (h *Handler) handleListDeploymentInstances(
	_ context.Context,
	in *listDeploymentInstancesInput,
) (*listDeploymentInstancesOutput, error) {
	if in.DeploymentID == "" {
		return nil, fmt.Errorf("%w: deploymentId is required", errInvalidRequest)
	}

	if _, err := h.Backend.GetDeployment(in.DeploymentID); err != nil {
		return nil, err
	}

	return &listDeploymentInstancesOutput{InstancesList: []string{}}, nil
}

type listDeploymentTargetsInput struct {
	DeploymentID string `json:"deploymentId"`
}

type listDeploymentTargetsOutput struct {
	TargetIDs []string `json:"targetIds"`
}

func (h *Handler) handleListDeploymentTargets(
	_ context.Context,
	in *listDeploymentTargetsInput,
) (*listDeploymentTargetsOutput, error) {
	if in.DeploymentID == "" {
		return nil, fmt.Errorf("%w: deploymentId is required", errInvalidRequest)
	}

	if _, err := h.Backend.GetDeployment(in.DeploymentID); err != nil {
		return nil, err
	}

	return &listDeploymentTargetsOutput{TargetIDs: []string{}}, nil
}

type putLifecycleEventHookExecutionStatusInput struct {
	DeploymentID                  string `json:"deploymentId"`
	LifecycleEventHookExecutionID string `json:"lifecycleEventHookExecutionId"`
	Status                        string `json:"status"`
}

type putLifecycleEventHookExecutionStatusOutput struct {
	LifecycleEventHookExecutionID string `json:"lifecycleEventHookExecutionId"`
}

func (h *Handler) handlePutLifecycleEventHookExecutionStatus(
	_ context.Context,
	in *putLifecycleEventHookExecutionStatusInput,
) (*putLifecycleEventHookExecutionStatusOutput, error) {
	if in.DeploymentID == "" {
		return nil, fmt.Errorf("%w: deploymentId is required", errInvalidRequest)
	}

	return &putLifecycleEventHookExecutionStatusOutput{
		LifecycleEventHookExecutionID: in.LifecycleEventHookExecutionID,
	}, nil
}

type deleteGitHubAccountTokenInput struct {
	TokenName string `json:"tokenName"`
}

type deleteGitHubAccountTokenOutput struct {
	TokenName string `json:"tokenName"`
}

func (h *Handler) handleDeleteGitHubAccountToken(
	_ context.Context,
	in *deleteGitHubAccountTokenInput,
) (*deleteGitHubAccountTokenOutput, error) {
	if in.TokenName == "" {
		return nil, fmt.Errorf("%w: tokenName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteGitHubAccountToken(in.TokenName); err != nil {
		return nil, err
	}

	return &deleteGitHubAccountTokenOutput{TokenName: in.TokenName}, nil
}

type listGitHubAccountTokenNamesInput struct{}

type listGitHubAccountTokenNamesOutput struct {
	TokenNameList []string `json:"tokenNameList"`
}

func (h *Handler) handleListGitHubAccountTokenNames(
	_ context.Context,
	_ *listGitHubAccountTokenNamesInput,
) (*listGitHubAccountTokenNamesOutput, error) {
	return &listGitHubAccountTokenNamesOutput{TokenNameList: h.Backend.ListGitHubAccountTokenNames()}, nil
}

type deleteResourcesByExternalIDInput struct {
	ExternalID string `json:"externalId"`
}

type deleteResourcesByExternalIDOutput struct{}

func (h *Handler) handleDeleteResourcesByExternalID(
	_ context.Context,
	_ *deleteResourcesByExternalIDInput,
) (*deleteResourcesByExternalIDOutput, error) {
	return &deleteResourcesByExternalIDOutput{}, nil
}
