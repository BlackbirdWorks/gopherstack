package codedeploy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

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
		"CreateDeploymentGroup",
		"GetDeploymentGroup",
		"ListDeploymentGroups",
		"DeleteDeploymentGroup",
		"CreateDeployment",
		"GetDeployment",
		"ListDeployments",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
		"AddTagsToOnPremisesInstances",
		"BatchGetApplicationRevisions",
		"BatchGetApplications",
		"BatchGetDeploymentGroups",
		"BatchGetDeploymentInstances",
		"BatchGetDeploymentTargets",
		"BatchGetDeployments",
		"BatchGetOnPremisesInstances",
		"ContinueDeployment",
		"CreateDeploymentConfig",
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
		"CreateApplication":            service.WrapOp(h.handleCreateApplication),
		"GetApplication":               service.WrapOp(h.handleGetApplication),
		"ListApplications":             service.WrapOp(h.handleListApplications),
		"DeleteApplication":            service.WrapOp(h.handleDeleteApplication),
		"CreateDeploymentGroup":        service.WrapOp(h.handleCreateDeploymentGroup),
		"GetDeploymentGroup":           service.WrapOp(h.handleGetDeploymentGroup),
		"ListDeploymentGroups":         service.WrapOp(h.handleListDeploymentGroups),
		"DeleteDeploymentGroup":        service.WrapOp(h.handleDeleteDeploymentGroup),
		"CreateDeployment":             service.WrapOp(h.handleCreateDeployment),
		"GetDeployment":                service.WrapOp(h.handleGetDeployment),
		"ListDeployments":              service.WrapOp(h.handleListDeployments),
		"TagResource":                  service.WrapOp(h.handleTagResource),
		"UntagResource":                service.WrapOp(h.handleUntagResource),
		"ListTagsForResource":          service.WrapOp(h.handleListTagsForResource),
		"AddTagsToOnPremisesInstances": service.WrapOp(h.handleAddTagsToOnPremisesInstances),
		"BatchGetApplicationRevisions": service.WrapOp(h.handleBatchGetApplicationRevisions),
		"BatchGetApplications":         service.WrapOp(h.handleBatchGetApplications),
		"BatchGetDeploymentGroups":     service.WrapOp(h.handleBatchGetDeploymentGroups),
		"BatchGetDeploymentInstances":  service.WrapOp(h.handleBatchGetDeploymentInstances),
		"BatchGetDeploymentTargets":    service.WrapOp(h.handleBatchGetDeploymentTargets),
		"BatchGetDeployments":          service.WrapOp(h.handleBatchGetDeployments),
		"BatchGetOnPremisesInstances":  service.WrapOp(h.handleBatchGetOnPremisesInstances),
		"ContinueDeployment":           service.WrapOp(h.handleContinueDeployment),
		"CreateDeploymentConfig":       service.WrapOp(h.handleCreateDeploymentConfig),
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

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	makePayload := func(code, msg string) []byte {
		b, _ := json.Marshal(service.JSONErrorResponse{Type: code, Message: msg})

		return b
	}

	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSONBlob(http.StatusNotFound,
			makePayload("ApplicationDoesNotExistException", err.Error()))
	case errors.Is(err, ErrDeploymentGroupNotFound):
		return c.JSONBlob(http.StatusNotFound,
			makePayload("DeploymentGroupDoesNotExistException", err.Error()))
	case errors.Is(err, ErrDeploymentNotFound):
		return c.JSONBlob(http.StatusNotFound,
			makePayload("DeploymentDoesNotExistException", err.Error()))
	case errors.Is(err, ErrDeploymentConfigNotFound):
		return c.JSONBlob(http.StatusNotFound,
			makePayload("DeploymentConfigDoesNotExistException", err.Error()))
	case errors.Is(err, ErrAlreadyExists):
		return c.JSONBlob(http.StatusConflict,
			makePayload("ApplicationAlreadyExistsException", err.Error()))
	case errors.Is(err, ErrDeploymentGroupAlreadyExists):
		return c.JSONBlob(http.StatusConflict,
			makePayload("DeploymentGroupAlreadyExistsException", err.Error()))
	case errors.Is(err, ErrDeploymentConfigAlreadyExists):
		return c.JSONBlob(http.StatusConflict,
			makePayload("DeploymentConfigAlreadyExistsException", err.Error()))
	case errors.Is(err, ErrValidation):
		return c.JSONBlob(http.StatusBadRequest,
			makePayload("InvalidParameterValueException", err.Error()))
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSONBlob(http.StatusBadRequest,
			makePayload("InvalidRequestException", err.Error()))
	default:
		return c.JSONBlob(http.StatusInternalServerError,
			makePayload("ServiceException", err.Error()))
	}
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
		in.ComputePlatform = "Server"
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

type createDeploymentGroupInput struct {
	ApplicationName      string     `json:"applicationName"`
	DeploymentGroupName  string     `json:"deploymentGroupName"`
	ServiceRoleArn       string     `json:"serviceRoleArn"`
	DeploymentConfigName string     `json:"deploymentConfigName"`
	Tags                 []tagEntry `json:"tags"`
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

	dg, err := h.Backend.CreateDeploymentGroup(
		in.ApplicationName, in.DeploymentGroupName,
		in.ServiceRoleArn, in.DeploymentConfigName,
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

type deploymentGroupInfo struct {
	ApplicationName      string `json:"applicationName"`
	DeploymentGroupID    string `json:"deploymentGroupId"`
	DeploymentGroupName  string `json:"deploymentGroupName"`
	ServiceRoleArn       string `json:"serviceRoleArn"`
	DeploymentConfigName string `json:"deploymentConfigName"`
}

type getDeploymentGroupOutput struct {
	DeploymentGroupInfo deploymentGroupInfo `json:"deploymentGroupInfo"`
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

	return &getDeploymentGroupOutput{
		DeploymentGroupInfo: deploymentGroupInfo{
			ApplicationName:      dg.ApplicationName,
			DeploymentGroupID:    dg.DeploymentGroupID,
			DeploymentGroupName:  dg.DeploymentGroupName,
			ServiceRoleArn:       dg.ServiceRoleArn,
			DeploymentConfigName: dg.DeploymentConfigName,
		},
	}, nil
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
	ApplicationName     string `json:"applicationName"`
	DeploymentGroupName string `json:"deploymentGroupName"`
	Description         string `json:"description"`
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

	d, err := h.Backend.CreateDeployment(in.ApplicationName, in.DeploymentGroupName, in.Description, "user")
	if err != nil {
		return nil, err
	}

	return &createDeploymentOutput{DeploymentID: d.DeploymentID}, nil
}

type getDeploymentInput struct {
	DeploymentID string `json:"deploymentId"`
}

type deploymentInfo struct {
	CompleteTime         *int64 `json:"completeTime,omitempty"`
	DeploymentID         string `json:"deploymentId"`
	ApplicationName      string `json:"applicationName"`
	DeploymentGroupName  string `json:"deploymentGroupName"`
	DeploymentConfigName string `json:"deploymentConfigName,omitempty"`
	Status               string `json:"status"`
	Creator              string `json:"creator"`
	Description          string `json:"description,omitempty"`
	CreateTime           int64  `json:"createTime"`
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
		DeploymentID:         d.DeploymentID,
		ApplicationName:      d.ApplicationName,
		DeploymentGroupName:  d.DeploymentGroupName,
		DeploymentConfigName: d.DeploymentConfigName,
		Status:               d.Status,
		Creator:              d.Creator,
		CreateTime:           d.CreateTime.UnixMilli(),
		Description:          d.Description,
	}

	if d.CompleteTime != nil {
		ms := d.CompleteTime.UnixMilli()
		info.CompleteTime = &ms
	}

	return &getDeploymentOutput{DeploymentInfo: info}, nil
}

type listDeploymentsInput struct {
	ApplicationName     string `json:"applicationName"`
	DeploymentGroupName string `json:"deploymentGroupName"`
}

type listDeploymentsOutput struct {
	Deployments []string `json:"deployments"`
}

func (h *Handler) handleListDeployments(
	_ context.Context,
	in *listDeploymentsInput,
) (*listDeploymentsOutput, error) {
	return &listDeploymentsOutput{
		Deployments: h.Backend.ListDeployments(in.ApplicationName, in.DeploymentGroupName),
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

type revisionLocationInput struct {
	RevisionType string `json:"revisionType"`
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
	ErrorMessage         string                `json:"errorMessage,omitempty"`
	DeploymentGroupsInfo []deploymentGroupInfo `json:"deploymentGroupsInfo"`
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

	infos := make([]deploymentGroupInfo, 0, len(dgs))
	for _, dg := range dgs {
		infos = append(infos, deploymentGroupInfo{
			ApplicationName:      dg.ApplicationName,
			DeploymentGroupID:    dg.DeploymentGroupID,
			DeploymentGroupName:  dg.DeploymentGroupName,
			ServiceRoleArn:       dg.ServiceRoleArn,
			DeploymentConfigName: dg.DeploymentConfigName,
		})
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

	items, errMsg := h.Backend.BatchGetDeploymentInstances(in.DeploymentID, in.InstanceIDs)

	return &batchGetDeploymentInstancesOutput{
		ErrorMessage:     errMsg,
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
			DeploymentID:         d.DeploymentID,
			ApplicationName:      d.ApplicationName,
			DeploymentGroupName:  d.DeploymentGroupName,
			DeploymentConfigName: d.DeploymentConfigName,
			Status:               d.Status,
			Creator:              d.Creator,
			CreateTime:           d.CreateTime.UnixMilli(),
			Description:          d.Description,
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

type createDeploymentConfigInput struct {
	DeploymentConfigName string `json:"deploymentConfigName"`
	ComputePlatform      string `json:"computePlatform"`
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

	cfg, err := h.Backend.CreateDeploymentConfig(in.DeploymentConfigName, in.ComputePlatform)
	if err != nil {
		return nil, err
	}

	return &createDeploymentConfigOutput{DeploymentConfigID: cfg.DeploymentConfigID}, nil
}
