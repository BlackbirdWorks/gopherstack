package dms

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	dmsTargetPrefix = "AmazonDMSv20160101."
	contentType     = "application/x-amz-json-1.1"
)

// errUnknownAction is returned when an unsupported DMS action is requested.
var errUnknownAction = errors.New("UnknownOperationException")

// Handler is the Echo HTTP handler for AWS DMS operations (JSON protocol).
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new DMS handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "DMS" }

// GetSupportedOperations returns the list of supported DMS operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateReplicationInstance",
		"DescribeReplicationInstances",
		"DeleteReplicationInstance",
		"CreateEndpoint",
		"DescribeEndpoints",
		"DeleteEndpoint",
		"CreateReplicationTask",
		"DescribeReplicationTasks",
		"StartReplicationTask",
		"StopReplicationTask",
		"DeleteReplicationTask",
		"AddTagsToResource",
		"ListTagsForResource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "dms" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this DMS instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches AWS DMS requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), dmsTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the DMS operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, dmsTargetPrefix)

	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

// ExtractResource extracts the primary resource identifier from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	action := h.ExtractOperation(c)

	switch action {
	case "CreateReplicationInstance", "DescribeReplicationInstances", "DeleteReplicationInstance":
		return extractField(c, "ReplicationInstanceIdentifier", "ReplicationInstanceArn")
	case "CreateEndpoint", "DescribeEndpoints", "DeleteEndpoint":
		return extractField(c, "EndpointIdentifier", "EndpointArn")
	case "CreateReplicationTask", "DescribeReplicationTasks",
		"StartReplicationTask", "StopReplicationTask", "DeleteReplicationTask":
		return extractField(c, "ReplicationTaskIdentifier", "ReplicationTaskArn")
	case "AddTagsToResource", "ListTagsForResource":
		return extractField(c, "ResourceArn")
	}

	return ""
}

// extractField reads the request body and returns the first non-empty value for the given JSON keys.
func extractField(c *echo.Context, keys ...string) string {
	if c.Request().Body == nil {
		return ""
	}

	bodyBytes, readErr := io.ReadAll(c.Request().Body)
	if readErr != nil || len(bodyBytes) == 0 {
		return ""
	}

	var raw map[string]string
	if unmarshalErr := json.Unmarshal(bodyBytes, &raw); unmarshalErr != nil {
		return ""
	}

	for _, k := range keys {
		if v := raw[k]; v != "" {
			return v
		}
	}

	return ""
}

// Handler returns the Echo handler function for DMS requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c,
			logger.Load(c.Request().Context()),
			"AmazonDMSv20160101", contentType,
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	table := map[string]service.JSONOpFunc{
		"CreateReplicationInstance":    service.WrapOp(h.handleCreateReplicationInstance),
		"DescribeReplicationInstances": service.WrapOp(h.handleDescribeReplicationInstances),
		"DeleteReplicationInstance":    service.WrapOp(h.handleDeleteReplicationInstance),
		"CreateEndpoint":               service.WrapOp(h.handleCreateEndpoint),
		"DescribeEndpoints":            service.WrapOp(h.handleDescribeEndpoints),
		"DeleteEndpoint":               service.WrapOp(h.handleDeleteEndpoint),
		"CreateReplicationTask":        service.WrapOp(h.handleCreateReplicationTask),
		"DescribeReplicationTasks":     service.WrapOp(h.handleDescribeReplicationTasks),
		"StartReplicationTask":         service.WrapOp(h.handleStartReplicationTask),
		"StopReplicationTask":          service.WrapOp(h.handleStopReplicationTask),
		"DeleteReplicationTask":        service.WrapOp(h.handleDeleteReplicationTask),
		"AddTagsToResource":            service.WrapOp(h.handleAddTagsToResource),
		"ListTagsForResource":          service.WrapOp(h.handleListTagsForResource),
	}

	fn, ok := table[action]
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
	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusNotFound, service.JSONErrorResponse{
			Type:    "ResourceNotFoundFault",
			Message: err.Error(),
		})
	case errors.Is(err, ErrAlreadyExists):
		return c.JSON(http.StatusConflict, service.JSONErrorResponse{
			Type:    "ResourceAlreadyExistsFault",
			Message: err.Error(),
		})
	case errors.Is(err, ErrInvalidState):
		return c.JSON(http.StatusBadRequest, service.JSONErrorResponse{
			Type:    "InvalidResourceStateFault",
			Message: err.Error(),
		})
	default:
		return c.JSON(http.StatusInternalServerError, service.JSONErrorResponse{
			Type:    "InternalFailure",
			Message: err.Error(),
		})
	}
}

// --- Replication Instance handlers ---

type createReplicationInstanceInput struct {
	ReplicationInstanceIdentifier *string    `json:"ReplicationInstanceIdentifier"`
	ReplicationInstanceClass      *string    `json:"ReplicationInstanceClass"`
	EngineVersion                 *string    `json:"EngineVersion"`
	AvailabilityZone              *string    `json:"AvailabilityZone"`
	AllocatedStorage              *int32     `json:"AllocatedStorage"`
	MultiAZ                       *bool      `json:"MultiAZ"`
	AutoMinorVersionUpgrade       *bool      `json:"AutoMinorVersionUpgrade"`
	PubliclyAccessible            *bool      `json:"PubliclyAccessible"`
	Tags                          []tagEntry `json:"Tags"`
}

type createReplicationInstanceOutput struct {
	ReplicationInstance replicationInstanceJSON `json:"ReplicationInstance"`
}

func (h *Handler) handleCreateReplicationInstance(
	_ context.Context, in *createReplicationInstanceInput,
) (*createReplicationInstanceOutput, error) {
	identifier := ptrStr(in.ReplicationInstanceIdentifier)
	class := ptrStr(in.ReplicationInstanceClass)

	kv := tagsToMap(in.Tags)
	ri, err := h.Backend.CreateReplicationInstance(
		identifier, class,
		ptrStr(in.EngineVersion),
		ptrStr(in.AvailabilityZone),
		ptrInt32(in.AllocatedStorage),
		ptrBool(in.MultiAZ),
		ptrBool(in.AutoMinorVersionUpgrade),
		ptrBool(in.PubliclyAccessible),
		kv,
	)

	if err != nil {
		return nil, err
	}

	return &createReplicationInstanceOutput{
		ReplicationInstance: riToJSON(ri),
	}, nil
}

type describeReplicationInstancesInput struct {
	Filters []filterEntry `json:"Filters"`
}

type describeReplicationInstancesOutput struct {
	ReplicationInstances []replicationInstanceJSON `json:"ReplicationInstances"`
}

func (h *Handler) handleDescribeReplicationInstances(
	_ context.Context, in *describeReplicationInstancesInput,
) (*describeReplicationInstancesOutput, error) {
	identifier := extractFilterValue(in.Filters, "replication-instance-id")
	list, err := h.Backend.DescribeReplicationInstances(identifier)

	if err != nil {
		return nil, err
	}

	out := make([]replicationInstanceJSON, 0, len(list))
	for _, ri := range list {
		out = append(out, riToJSON(ri))
	}

	return &describeReplicationInstancesOutput{ReplicationInstances: out}, nil
}

type deleteReplicationInstanceInput struct {
	ReplicationInstanceArn *string `json:"ReplicationInstanceArn"`
}

type deleteReplicationInstanceOutput struct {
	ReplicationInstance replicationInstanceJSON `json:"ReplicationInstance"`
}

func (h *Handler) handleDeleteReplicationInstance(
	_ context.Context, in *deleteReplicationInstanceInput,
) (*deleteReplicationInstanceOutput, error) {
	arnOrID := ptrStr(in.ReplicationInstanceArn)
	// Retrieve before deletion to return it in the response.
	instances, err := h.Backend.DescribeReplicationInstances(arnOrID)

	if err != nil {
		// Try ARN lookup via delete directly.
		if delErr := h.Backend.DeleteReplicationInstance(arnOrID); delErr != nil {
			return nil, delErr
		}

		return &deleteReplicationInstanceOutput{}, nil
	}

	if delErr := h.Backend.DeleteReplicationInstance(arnOrID); delErr != nil {
		return nil, delErr
	}

	if len(instances) == 0 {
		return &deleteReplicationInstanceOutput{}, nil
	}

	return &deleteReplicationInstanceOutput{ReplicationInstance: riToJSON(instances[0])}, nil
}

// --- Endpoint handlers ---

type createEndpointInput struct {
	EndpointIdentifier *string    `json:"EndpointIdentifier"`
	EndpointType       *string    `json:"EndpointType"`
	EngineName         *string    `json:"EngineName"`
	ServerName         *string    `json:"ServerName"`
	DatabaseName       *string    `json:"DatabaseName"`
	Username           *string    `json:"Username"`
	Port               *int32     `json:"Port"`
	Tags               []tagEntry `json:"Tags"`
}

type createEndpointOutput struct {
	Endpoint endpointJSON `json:"Endpoint"`
}

func (h *Handler) handleCreateEndpoint(
	_ context.Context, in *createEndpointInput,
) (*createEndpointOutput, error) {
	kv := tagsToMap(in.Tags)
	ep, err := h.Backend.CreateEndpoint(
		ptrStr(in.EndpointIdentifier),
		ptrStr(in.EndpointType),
		ptrStr(in.EngineName),
		ptrStr(in.ServerName),
		ptrStr(in.DatabaseName),
		ptrStr(in.Username),
		ptrInt32(in.Port),
		kv,
	)

	if err != nil {
		return nil, err
	}

	return &createEndpointOutput{Endpoint: epToJSON(ep)}, nil
}

type describeEndpointsInput struct {
	Filters []filterEntry `json:"Filters"`
}

type describeEndpointsOutput struct {
	Endpoints []endpointJSON `json:"Endpoints"`
}

func (h *Handler) handleDescribeEndpoints(
	_ context.Context, in *describeEndpointsInput,
) (*describeEndpointsOutput, error) {
	identifier := extractFilterValue(in.Filters, "endpoint-id")
	list, err := h.Backend.DescribeEndpoints(identifier)

	if err != nil {
		return nil, err
	}

	out := make([]endpointJSON, 0, len(list))
	for _, ep := range list {
		out = append(out, epToJSON(ep))
	}

	return &describeEndpointsOutput{Endpoints: out}, nil
}

type deleteEndpointInput struct {
	EndpointArn *string `json:"EndpointArn"`
}

type deleteEndpointOutput struct {
	Endpoint endpointJSON `json:"Endpoint"`
}

func (h *Handler) handleDeleteEndpoint(
	_ context.Context, in *deleteEndpointInput,
) (*deleteEndpointOutput, error) {
	ep, err := h.Backend.DeleteEndpoint(ptrStr(in.EndpointArn))
	if err != nil {
		return nil, err
	}

	return &deleteEndpointOutput{Endpoint: epToJSON(ep)}, nil
}

// --- Replication Task handlers ---

type createReplicationTaskInput struct {
	ReplicationTaskIdentifier *string    `json:"ReplicationTaskIdentifier"`
	SourceEndpointArn         *string    `json:"SourceEndpointArn"`
	TargetEndpointArn         *string    `json:"TargetEndpointArn"`
	ReplicationInstanceArn    *string    `json:"ReplicationInstanceArn"`
	MigrationType             *string    `json:"MigrationType"`
	TableMappings             *string    `json:"TableMappings"`
	ReplicationTaskSettings   *string    `json:"ReplicationTaskSettings"`
	Tags                      []tagEntry `json:"Tags"`
}

type createReplicationTaskOutput struct {
	ReplicationTask replicationTaskJSON `json:"ReplicationTask"`
}

func (h *Handler) handleCreateReplicationTask(
	_ context.Context, in *createReplicationTaskInput,
) (*createReplicationTaskOutput, error) {
	kv := tagsToMap(in.Tags)
	rt, err := h.Backend.CreateReplicationTask(
		ptrStr(in.ReplicationTaskIdentifier),
		ptrStr(in.SourceEndpointArn),
		ptrStr(in.TargetEndpointArn),
		ptrStr(in.ReplicationInstanceArn),
		ptrStr(in.MigrationType),
		ptrStr(in.TableMappings),
		ptrStr(in.ReplicationTaskSettings),
		kv,
	)

	if err != nil {
		return nil, err
	}

	return &createReplicationTaskOutput{ReplicationTask: rtToJSON(rt)}, nil
}

type describeReplicationTasksInput struct {
	Filters []filterEntry `json:"Filters"`
}

type describeReplicationTasksOutput struct {
	ReplicationTasks []replicationTaskJSON `json:"ReplicationTasks"`
}

func (h *Handler) handleDescribeReplicationTasks(
	_ context.Context, in *describeReplicationTasksInput,
) (*describeReplicationTasksOutput, error) {
	arnOrID := extractFilterValue(in.Filters, "replication-task-id", "replication-task-arn")
	list, err := h.Backend.DescribeReplicationTasks(arnOrID)

	if err != nil {
		return nil, err
	}

	out := make([]replicationTaskJSON, 0, len(list))
	for _, rt := range list {
		out = append(out, rtToJSON(rt))
	}

	return &describeReplicationTasksOutput{ReplicationTasks: out}, nil
}

type startReplicationTaskInput struct {
	ReplicationTaskArn       *string `json:"ReplicationTaskArn"`
	StartReplicationTaskType *string `json:"StartReplicationTaskType"`
}

type startReplicationTaskOutput struct {
	ReplicationTask replicationTaskJSON `json:"ReplicationTask"`
}

func (h *Handler) handleStartReplicationTask(
	_ context.Context, in *startReplicationTaskInput,
) (*startReplicationTaskOutput, error) {
	rt, err := h.Backend.StartReplicationTask(ptrStr(in.ReplicationTaskArn))
	if err != nil {
		return nil, err
	}

	return &startReplicationTaskOutput{ReplicationTask: rtToJSON(rt)}, nil
}

type stopReplicationTaskInput struct {
	ReplicationTaskArn *string `json:"ReplicationTaskArn"`
}

type stopReplicationTaskOutput struct {
	ReplicationTask replicationTaskJSON `json:"ReplicationTask"`
}

func (h *Handler) handleStopReplicationTask(
	_ context.Context, in *stopReplicationTaskInput,
) (*stopReplicationTaskOutput, error) {
	rt, err := h.Backend.StopReplicationTask(ptrStr(in.ReplicationTaskArn))
	if err != nil {
		return nil, err
	}

	return &stopReplicationTaskOutput{ReplicationTask: rtToJSON(rt)}, nil
}

type deleteReplicationTaskInput struct {
	ReplicationTaskArn *string `json:"ReplicationTaskArn"`
}

type deleteReplicationTaskOutput struct {
	ReplicationTask replicationTaskJSON `json:"ReplicationTask"`
}

func (h *Handler) handleDeleteReplicationTask(
	_ context.Context, in *deleteReplicationTaskInput,
) (*deleteReplicationTaskOutput, error) {
	rt, err := h.Backend.DeleteReplicationTask(ptrStr(in.ReplicationTaskArn))
	if err != nil {
		return nil, err
	}

	return &deleteReplicationTaskOutput{ReplicationTask: rtToJSON(rt)}, nil
}

// --- Tag handlers ---

type tagEntry struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type addTagsToResourceInput struct {
	ResourceArn *string    `json:"ResourceArn"`
	Tags        []tagEntry `json:"Tags"`
}

type addTagsToResourceOutput struct{}

func (h *Handler) handleAddTagsToResource(
	_ context.Context, in *addTagsToResourceInput,
) (*addTagsToResourceOutput, error) {
	kv := tagsToMap(in.Tags)
	if err := h.Backend.AddTagsToResource(ptrStr(in.ResourceArn), kv); err != nil {
		return nil, err
	}

	return &addTagsToResourceOutput{}, nil
}

type listTagsForResourceInput struct {
	ResourceArn *string `json:"ResourceArn"`
}

type listTagsForResourceOutput struct {
	TagList []tagEntry `json:"TagList"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context, in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	kv, err := h.Backend.ListTagsForResource(ptrStr(in.ResourceArn))
	if err != nil {
		return nil, err
	}

	list := make([]tagEntry, 0, len(kv))
	for k, v := range kv {
		list = append(list, tagEntry{Key: k, Value: v})
	}

	return &listTagsForResourceOutput{TagList: list}, nil
}

// --- JSON response types ---

type replicationInstanceJSON struct {
	ReplicationInstanceIdentifier string `json:"ReplicationInstanceIdentifier"`
	ReplicationInstanceArn        string `json:"ReplicationInstanceArn"`
	ReplicationInstanceClass      string `json:"ReplicationInstanceClass"`
	EngineVersion                 string `json:"EngineVersion"`
	AvailabilityZone              string `json:"AvailabilityZone"`
	ReplicationInstanceStatus     string `json:"ReplicationInstanceStatus"`
	AllocatedStorage              int32  `json:"AllocatedStorage"`
	MultiAZ                       bool   `json:"MultiAZ"`
	AutoMinorVersionUpgrade       bool   `json:"AutoMinorVersionUpgrade"`
	PubliclyAccessible            bool   `json:"PubliclyAccessible"`
}

func riToJSON(ri *ReplicationInstance) replicationInstanceJSON {
	return replicationInstanceJSON{
		ReplicationInstanceIdentifier: ri.ReplicationInstanceIdentifier,
		ReplicationInstanceArn:        ri.ReplicationInstanceArn,
		ReplicationInstanceClass:      ri.ReplicationInstanceClass,
		EngineVersion:                 ri.EngineVersion,
		AvailabilityZone:              ri.AvailabilityZone,
		ReplicationInstanceStatus:     ri.ReplicationInstanceStatus,
		AllocatedStorage:              ri.AllocatedStorage,
		MultiAZ:                       ri.MultiAZ,
		AutoMinorVersionUpgrade:       ri.AutoMinorVersionUpgrade,
		PubliclyAccessible:            ri.PubliclyAccessible,
	}
}

type endpointJSON struct {
	EndpointIdentifier string `json:"EndpointIdentifier"`
	EndpointArn        string `json:"EndpointArn"`
	EndpointType       string `json:"EndpointType"`
	EngineName         string `json:"EngineName"`
	ServerName         string `json:"ServerName,omitempty"`
	DatabaseName       string `json:"DatabaseName,omitempty"`
	Username           string `json:"Username,omitempty"`
	Status             string `json:"Status"`
	Port               int32  `json:"Port,omitempty"`
}

func epToJSON(ep *Endpoint) endpointJSON {
	return endpointJSON{
		EndpointIdentifier: ep.EndpointIdentifier,
		EndpointArn:        ep.EndpointArn,
		EndpointType:       ep.EndpointType,
		EngineName:         ep.EngineName,
		ServerName:         ep.ServerName,
		DatabaseName:       ep.DatabaseName,
		Username:           ep.Username,
		Status:             ep.Status,
		Port:               ep.Port,
	}
}

type replicationTaskJSON struct {
	ReplicationTaskIdentifier string `json:"ReplicationTaskIdentifier"`
	ReplicationTaskArn        string `json:"ReplicationTaskArn"`
	SourceEndpointArn         string `json:"SourceEndpointArn"`
	TargetEndpointArn         string `json:"TargetEndpointArn"`
	ReplicationInstanceArn    string `json:"ReplicationInstanceArn"`
	MigrationType             string `json:"MigrationType"`
	TableMappings             string `json:"TableMappings,omitempty"`
	ReplicationTaskSettings   string `json:"ReplicationTaskSettings,omitempty"`
	Status                    string `json:"Status"`
}

func rtToJSON(rt *ReplicationTask) replicationTaskJSON {
	return replicationTaskJSON{
		ReplicationTaskIdentifier: rt.ReplicationTaskIdentifier,
		ReplicationTaskArn:        rt.ReplicationTaskArn,
		SourceEndpointArn:         rt.SourceEndpointArn,
		TargetEndpointArn:         rt.TargetEndpointArn,
		ReplicationInstanceArn:    rt.ReplicationInstanceArn,
		MigrationType:             rt.MigrationType,
		TableMappings:             rt.TableMappings,
		ReplicationTaskSettings:   rt.ReplicationTaskSettings,
		Status:                    rt.Status,
	}
}

// --- Filter types ---

type filterEntry struct {
	Name   string   `json:"Name"`
	Values []string `json:"Values"`
}

// extractFilterValue searches filters for the first matching name and returns the first value.
func extractFilterValue(filters []filterEntry, names ...string) string {
	for _, f := range filters {
		for _, name := range names {
			if f.Name == name && len(f.Values) > 0 {
				return f.Values[0]
			}
		}
	}

	return ""
}

// --- Pointer helpers ---

func ptrStr(p *string) string {
	if p == nil {
		return ""
	}

	return *p
}

func ptrInt32(p *int32) int32 {
	if p == nil {
		return 0
	}

	return *p
}

func ptrBool(p *bool) bool {
	if p == nil {
		return false
	}

	return *p
}

// tagsToMap converts a slice of tag entries to a map.
func tagsToMap(entries []tagEntry) map[string]string {
	if len(entries) == 0 {
		return nil
	}

	m := make(map[string]string, len(entries))
	for _, e := range entries {
		m[e.Key] = e.Value
	}

	return m
}
