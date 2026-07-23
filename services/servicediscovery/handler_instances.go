package servicediscovery

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

type registerInstanceRequest struct {
	ServiceID        string            `json:"ServiceId"`
	InstanceID       string            `json:"InstanceId"`
	Attributes       map[string]string `json:"Attributes"`
	CreatorRequestID string            `json:"CreatorRequestId"`
}

func (h *Handler) handleRegisterInstance(_ context.Context, body []byte) ([]byte, error) {
	var req registerInstanceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ServiceID == "" {
		return nil, fmt.Errorf("%w: ServiceId is required", errInvalidRequest)
	}

	if req.InstanceID == "" {
		return nil, fmt.Errorf("%w: InstanceId is required", errInvalidRequest)
	}

	if err := validateInstanceAttributes(req.Attributes); err != nil {
		return nil, err
	}

	opID, err := h.Backend.RegisterInstance(req.ServiceID, req.InstanceID, req.Attributes)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyOperationID: opID})
}

type deregisterInstanceRequest struct {
	ServiceID  string `json:"ServiceId"`
	InstanceID string `json:"InstanceId"`
}

func (h *Handler) handleDeregisterInstance(_ context.Context, body []byte) ([]byte, error) {
	var req deregisterInstanceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ServiceID == "" {
		return nil, fmt.Errorf("%w: ServiceId is required", errInvalidRequest)
	}

	if req.InstanceID == "" {
		return nil, fmt.Errorf("%w: InstanceId is required", errInvalidRequest)
	}

	opID, err := h.Backend.DeregisterInstance(req.ServiceID, req.InstanceID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyOperationID: opID})
}

type getInstanceRequest struct {
	ServiceID  string `json:"ServiceId"`
	InstanceID string `json:"InstanceId"`
}

func (h *Handler) handleGetInstance(_ context.Context, body []byte) ([]byte, error) {
	var req getInstanceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ServiceID == "" {
		return nil, fmt.Errorf("%w: ServiceId is required", errInvalidRequest)
	}

	if req.InstanceID == "" {
		return nil, fmt.Errorf("%w: InstanceId is required", errInvalidRequest)
	}

	inst, err := h.Backend.GetInstance(req.ServiceID, req.InstanceID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"Instance": map[string]any{
			"Id":          inst.ID,
			keyAttributes: inst.Attributes,
		},
	})
}

type listInstancesRequest struct {
	ServiceID  string `json:"ServiceId"`
	MaxResults *int   `json:"MaxResults"`
	NextToken  string `json:"NextToken"`
}

func (h *Handler) handleListInstances(_ context.Context, body []byte) ([]byte, error) {
	var req listInstancesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ServiceID == "" {
		return nil, fmt.Errorf("%w: ServiceId is required", errInvalidRequest)
	}

	instances, err := h.Backend.ListInstances(req.ServiceID)
	if err != nil {
		return nil, err
	}

	maxResults := maxResultsDefault
	if req.MaxResults != nil && *req.MaxResults > 0 {
		maxResults = *req.MaxResults
	}

	page, nextToken := applyPaginationInstances(instances, req.NextToken, maxResults)

	items := make([]map[string]any, 0, len(page))
	for _, inst := range page {
		items = append(items, map[string]any{
			"Id":          inst.ID,
			keyAttributes: inst.Attributes,
		})
	}

	resp := map[string]any{
		"Instances": items,
	}

	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

type getInstancesHealthStatusRequest struct {
	MaxResults *int     `json:"MaxResults,omitempty"`
	ServiceID  string   `json:"ServiceId"`
	NextToken  string   `json:"NextToken,omitempty"`
	Instances  []string `json:"Instances,omitempty"`
}

// handleGetInstancesHealthStatus returns the health status for instances in a service.
// Instances without a recorded custom status default to HEALTHY.
func (h *Handler) handleGetInstancesHealthStatus(_ context.Context, body []byte) ([]byte, error) {
	var req getInstancesHealthStatusRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ServiceID == "" {
		return nil, fmt.Errorf("%w: ServiceId is required", errInvalidRequest)
	}

	statuses, err := h.Backend.GetInstancesHealthStatus(req.ServiceID, req.Instances)
	if err != nil {
		return nil, err
	}

	// Build sorted list of instance IDs for stable pagination.
	ids := collections.SortedKeys(statuses)

	maxResults := maxResultsDefault
	if req.MaxResults != nil && *req.MaxResults > 0 {
		maxResults = *req.MaxResults
	}

	page, nextToken := applyPaginationHealthStatuses(ids, req.NextToken, maxResults)

	paged := make(map[string]string, len(page))
	for _, id := range page {
		paged[id] = statuses[id]
	}

	resp := map[string]any{
		keyStatusField: paged,
	}

	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

type updateInstanceCustomHealthStatusRequest struct {
	ServiceID  string `json:"ServiceId"`
	InstanceID string `json:"InstanceId"`
	Status     string `json:"Status"`
}

func (h *Handler) handleUpdateInstanceCustomHealthStatus(_ context.Context, body []byte) error {
	var req updateInstanceCustomHealthStatusRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ServiceID == "" {
		return fmt.Errorf("%w: ServiceId is required", errInvalidRequest)
	}

	if req.InstanceID == "" {
		return fmt.Errorf("%w: InstanceId is required", errInvalidRequest)
	}

	if req.Status == "" {
		return fmt.Errorf("%w: Status is required", errInvalidRequest)
	}

	return h.Backend.UpdateInstanceCustomHealthStatus(req.ServiceID, req.InstanceID, req.Status)
}

func applyPaginationInstances(items []Instance, nextToken string, maxResults int) ([]Instance, string) {
	if maxResults <= 0 || maxResults > maxResultsCap {
		maxResults = maxResultsDefault
	}

	offset := decodeCursor(nextToken)
	if offset >= len(items) {
		return nil, ""
	}

	end := offset + maxResults

	var newToken string

	if end < len(items) {
		newToken = encodeCursor(end)
	} else {
		end = len(items)
	}

	return items[offset:end], newToken
}

func applyPaginationHealthStatuses(ids []string, nextToken string, maxResults int) ([]string, string) {
	if maxResults <= 0 || maxResults > maxResultsCap {
		maxResults = maxResultsDefault
	}

	offset := decodeCursor(nextToken)
	if offset >= len(ids) {
		return nil, ""
	}

	end := offset + maxResults

	var newToken string

	if end < len(ids) {
		newToken = encodeCursor(end)
	} else {
		end = len(ids)
	}

	return ids[offset:end], newToken
}
