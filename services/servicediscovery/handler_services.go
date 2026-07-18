package servicediscovery

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

type dnsRecordRequest struct {
	Type string `json:"Type"`
	TTL  int64  `json:"TTL"`
}

type dnsConfigRequest struct {
	NamespaceID   string             `json:"NamespaceId"`
	RoutingPolicy string             `json:"RoutingPolicy"`
	DNSRecords    []dnsRecordRequest `json:"DnsRecords"`
}

type healthCheckConfigRequest struct {
	Type             string `json:"Type"`
	ResourcePath     string `json:"ResourcePath"`
	FailureThreshold int    `json:"FailureThreshold"`
}

type healthCheckCustomConfigRequest struct {
	FailureThreshold int `json:"FailureThreshold"`
}

type createServiceRequest struct {
	Name                    string                          `json:"Name"`
	Description             string                          `json:"Description"`
	NamespaceID             string                          `json:"NamespaceId"`
	Type                    string                          `json:"Type"`
	CreatorRequestID        string                          `json:"CreatorRequestId"`
	DNSConfig               *dnsConfigRequest               `json:"DnsConfig"`
	HealthCheckConfig       *healthCheckConfigRequest       `json:"HealthCheckConfig"`
	HealthCheckCustomConfig *healthCheckCustomConfigRequest `json:"HealthCheckCustomConfig"`
	Tags                    []tagEntry                      `json:"Tags"`
}

func parseDNSConfig(req *dnsConfigRequest) *DNSConfig {
	if req == nil {
		return nil
	}

	dc := &DNSConfig{
		NamespaceID:   req.NamespaceID,
		RoutingPolicy: req.RoutingPolicy,
	}

	for _, r := range req.DNSRecords {
		dc.DNSRecords = append(dc.DNSRecords, DNSRecord(r))
	}

	return dc
}

func parseHealthCheckConfig(req *healthCheckConfigRequest) *HealthCheckConfig {
	if req == nil {
		return nil
	}

	return &HealthCheckConfig{
		Type:             req.Type,
		ResourcePath:     req.ResourcePath,
		FailureThreshold: req.FailureThreshold,
	}
}

func parseHealthCheckCustomConfig(req *healthCheckCustomConfigRequest) *HealthCheckCustomConfig {
	if req == nil {
		return nil
	}

	return &HealthCheckCustomConfig{
		FailureThreshold: req.FailureThreshold,
	}
}

func (h *Handler) handleCreateService(_ context.Context, body []byte) ([]byte, error) {
	var req createServiceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	if req.HealthCheckConfig != nil && req.HealthCheckCustomConfig != nil {
		return nil, fmt.Errorf(
			"%w: HealthCheckConfig and HealthCheckCustomConfig are mutually exclusive",
			ErrInvalidInput,
		)
	}

	if err := validateTags(req.Tags); err != nil {
		return nil, err
	}

	svc, err := h.Backend.CreateService(
		req.Name,
		req.NamespaceID,
		req.Description,
		req.Type,
		parseDNSConfig(req.DNSConfig),
		parseHealthCheckConfig(req.HealthCheckConfig),
		parseHealthCheckCustomConfig(req.HealthCheckCustomConfig),
		tagsToMap(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyService: serviceToMap(svc),
	})
}

type deleteServiceRequest struct {
	ID string `json:"Id"`
}

// handleDeleteService deletes a service. It intentionally does NOT deregister
// instances on the caller's behalf: real Cloud Map's DeleteService "fails if
// the service still contains one or more registered instances" -- the caller
// must deregister every instance first. h.Backend.DeleteService already
// enforces this (returns ErrResourceInUse).
func (h *Handler) handleDeleteService(_ context.Context, body []byte) error {
	var req deleteServiceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	return h.Backend.DeleteService(req.ID)
}

type getServiceRequest struct {
	ID string `json:"Id"`
}

func (h *Handler) handleGetService(_ context.Context, body []byte) ([]byte, error) {
	var req getServiceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	svc, err := h.Backend.GetService(req.ID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyService: serviceToMap(svc),
	})
}

type serviceFilter struct {
	Name      string   `json:"Name"`
	Condition string   `json:"Condition"`
	Values    []string `json:"Values"`
}

type listServicesRequest struct {
	MaxResults *int            `json:"MaxResults"`
	NextToken  string          `json:"NextToken"`
	Filters    []serviceFilter `json:"Filters"`
}

func (h *Handler) handleListServices(_ context.Context, body []byte) ([]byte, error) {
	var req listServicesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	filter := ListServicesFilter{}

	for _, f := range req.Filters {
		if f.Name == "NAMESPACE_ID" && len(f.Values) > 0 {
			filter.NamespaceID = f.Values[0]

			break
		}
	}

	services := h.Backend.ListServices(filter)

	maxResults := maxResultsDefault
	if req.MaxResults != nil && *req.MaxResults > 0 {
		maxResults = *req.MaxResults
	}

	page, nextToken := applyPaginationServices(services, req.NextToken, maxResults)

	items := make([]map[string]any, 0, len(page))
	for i := range page {
		items = append(items, serviceToMap(&page[i]))
	}

	resp := map[string]any{
		"Services": items,
	}

	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

// serviceToMap converts a Service to a JSON-serialisable map including DNS and health check config.
func serviceToMap(svc *Service) map[string]any {
	m := map[string]any{
		"Id":            svc.ID,
		keyArn:          svc.ARN,
		"Name":          svc.Name,
		keyNamespaceID:  svc.NamespaceID,
		"Description":   svc.Description,
		keyTags:         mapToTagEntries(svc.Tags),
		keyCreateDate:   awstime.Epoch(svc.CreatedAt),
		"InstanceCount": svc.InstanceCount,
	}

	if svc.Type != "" {
		m[keyType] = svc.Type
	}

	if svc.DNSConfig != nil {
		dc := map[string]any{
			keyNamespaceID:  svc.DNSConfig.NamespaceID,
			"RoutingPolicy": svc.DNSConfig.RoutingPolicy,
		}

		records := make([]map[string]any, 0, len(svc.DNSConfig.DNSRecords))
		for _, r := range svc.DNSConfig.DNSRecords {
			records = append(records, map[string]any{
				keyType: r.Type,
				"TTL":   r.TTL,
			})
		}

		dc["DnsRecords"] = records
		m["DnsConfig"] = dc
	}

	if svc.HealthCheckConfig != nil {
		m["HealthCheckConfig"] = map[string]any{
			keyType:            svc.HealthCheckConfig.Type,
			"ResourcePath":     svc.HealthCheckConfig.ResourcePath,
			"FailureThreshold": svc.HealthCheckConfig.FailureThreshold,
		}
	}

	if svc.HealthCheckCustomConfig != nil {
		m["HealthCheckCustomConfig"] = map[string]any{
			"FailureThreshold": svc.HealthCheckCustomConfig.FailureThreshold,
		}
	}

	return m
}

type updateServiceChange struct {
	DNSConfig         *dnsConfigRequest         `json:"DnsConfig"`
	HealthCheckConfig *healthCheckConfigRequest `json:"HealthCheckConfig"`
	Description       string                    `json:"Description"`
}

type updateServiceRequest struct {
	Service updateServiceChange `json:"Service"`
	ID      string              `json:"Id"`
}

func (h *Handler) handleUpdateService(_ context.Context, body []byte) ([]byte, error) {
	var req updateServiceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	opID, err := h.Backend.UpdateService(
		req.ID,
		req.Service.Description,
		parseDNSConfig(req.Service.DNSConfig),
		parseHealthCheckConfig(req.Service.HealthCheckConfig),
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyOperationID: opID})
}

type getServiceAttributesRequest struct {
	ServiceID string `json:"ServiceId"`
}

func (h *Handler) handleGetServiceAttributes(_ context.Context, body []byte) ([]byte, error) {
	var req getServiceAttributesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ServiceID == "" {
		return nil, fmt.Errorf("%w: ServiceId is required", errInvalidRequest)
	}

	arn, attrs, err := h.Backend.GetServiceAttributes(req.ServiceID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"ServiceAttributes": map[string]any{
			keyArn:        arn,
			keyAttributes: attrs,
		},
	})
}

type updateServiceAttributesRequest struct {
	Attributes map[string]string `json:"Attributes"`
	ServiceARN string            `json:"ServiceArn"`
}

func (h *Handler) handleUpdateServiceAttributes(_ context.Context, body []byte) error {
	var req updateServiceAttributesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ServiceARN == "" {
		return fmt.Errorf("%w: ServiceArn is required", errInvalidRequest)
	}

	return h.Backend.UpdateServiceAttributes(req.ServiceARN, req.Attributes)
}

type deleteServiceAttributesRequest struct {
	ServiceID string `json:"ServiceId"`
}

func (h *Handler) handleDeleteServiceAttributes(_ context.Context, body []byte) error {
	var req deleteServiceAttributesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ServiceID == "" {
		return fmt.Errorf("%w: ServiceId is required", errInvalidRequest)
	}

	return h.Backend.DeleteServiceAttributes(req.ServiceID)
}

func applyPaginationServices(items []Service, nextToken string, maxResults int) ([]Service, string) {
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
