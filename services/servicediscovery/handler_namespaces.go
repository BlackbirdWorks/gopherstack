package servicediscovery

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

type dnsPropertiesRequest struct {
	SOA *struct {
		TTL int64 `json:"TTL"`
	} `json:"SOA"`
}

type createHTTPNamespaceRequest struct {
	Name             string     `json:"Name"`
	Description      string     `json:"Description"`
	CreatorRequestID string     `json:"CreatorRequestId"`
	Tags             []tagEntry `json:"Tags"`
}

func (h *Handler) handleCreateHTTPNamespace(_ context.Context, body []byte) ([]byte, error) {
	var req createHTTPNamespaceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	if err := validateTags(req.Tags); err != nil {
		return nil, err
	}

	opID, err := h.Backend.CreateHTTPNamespace(req.Name, req.Description, tagsToMap(req.Tags))
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyOperationID: opID})
}

type createPrivateDNSNamespaceRequest struct {
	Name             string `json:"Name"`
	Description      string `json:"Description"`
	Vpc              string `json:"Vpc"`
	CreatorRequestID string `json:"CreatorRequestId"`
	Properties       *struct {
		DNSProperties *dnsPropertiesRequest `json:"DNSProperties"`
	} `json:"Properties"`
	Tags []tagEntry `json:"Tags"`
}

func (h *Handler) handleCreatePrivateDNSNamespace(_ context.Context, body []byte) ([]byte, error) {
	var req createPrivateDNSNamespaceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	if err := validateTags(req.Tags); err != nil {
		return nil, err
	}

	var soaTTL int64
	if req.Properties != nil && req.Properties.DNSProperties != nil && req.Properties.DNSProperties.SOA != nil {
		soaTTL = req.Properties.DNSProperties.SOA.TTL
	}

	opID, err := h.Backend.CreatePrivateDNSNamespace(req.Name, req.Description, req.Vpc, soaTTL, tagsToMap(req.Tags))
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyOperationID: opID})
}

type createPublicDNSNamespaceRequest struct {
	Name             string `json:"Name"`
	Description      string `json:"Description"`
	CreatorRequestID string `json:"CreatorRequestId"`
	Properties       *struct {
		DNSProperties *dnsPropertiesRequest `json:"DNSProperties"`
	} `json:"Properties"`
	Tags []tagEntry `json:"Tags"`
}

func (h *Handler) handleCreatePublicDNSNamespace(_ context.Context, body []byte) ([]byte, error) {
	var req createPublicDNSNamespaceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	if err := validateTags(req.Tags); err != nil {
		return nil, err
	}

	var soaTTL int64
	if req.Properties != nil && req.Properties.DNSProperties != nil && req.Properties.DNSProperties.SOA != nil {
		soaTTL = req.Properties.DNSProperties.SOA.TTL
	}

	opID, err := h.Backend.CreatePublicDNSNamespace(req.Name, req.Description, soaTTL, tagsToMap(req.Tags))
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyOperationID: opID})
}

type deleteNamespaceRequest struct {
	ID string `json:"Id"`
}

func (h *Handler) handleDeleteNamespace(_ context.Context, body []byte) ([]byte, error) {
	var req deleteNamespaceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	opID, err := h.Backend.DeleteNamespace(req.ID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyOperationID: opID})
}

type getNamespaceRequest struct {
	ID string `json:"Id"`
}

func (h *Handler) handleGetNamespace(_ context.Context, body []byte) ([]byte, error) {
	var req getNamespaceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	ns, err := h.Backend.GetNamespace(req.ID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"Namespace": namespaceToMap(ns),
	})
}

type namespaceFilter struct {
	Name      string   `json:"Name"`
	Condition string   `json:"Condition"`
	Values    []string `json:"Values"`
}

type listNamespacesRequest struct {
	MaxResults *int              `json:"MaxResults"`
	NextToken  string            `json:"NextToken"`
	Filters    []namespaceFilter `json:"Filters"`
}

func buildNamespacesFilter(filters []namespaceFilter) ListNamespacesFilter {
	f := ListNamespacesFilter{}

	for _, entry := range filters {
		if len(entry.Values) == 0 {
			continue
		}

		switch entry.Name {
		case "TYPE":
			f.Type = entry.Values[0]
		case "NAME":
			f.Name = entry.Values[0]
		}
	}

	return f
}

func (h *Handler) handleListNamespaces(_ context.Context, body []byte) ([]byte, error) {
	var req listNamespacesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	page, nextToken := applyPaginationNamespaces(
		h.Backend.ListNamespaces(buildNamespacesFilter(req.Filters)),
		req.NextToken,
		resolveMaxResults(req.MaxResults),
	)

	items := make([]map[string]any, 0, len(page))
	for i := range page {
		items = append(items, namespaceToMap(&page[i]))
	}

	return marshalPagedResponse("Namespaces", items, nextToken)
}

func namespacePropertiesToMap(ns *Namespace) map[string]any {
	if ns.Properties == nil {
		return nil
	}

	props := map[string]any{}

	if ns.Properties.DNSProperties != nil {
		dp := map[string]any{}

		if ns.Properties.DNSProperties.HostedZoneID != "" {
			dp["HostedZoneId"] = ns.Properties.DNSProperties.HostedZoneID
		}

		if ns.Properties.DNSProperties.SOA != nil {
			dp["SOA"] = map[string]any{
				"TTL": ns.Properties.DNSProperties.SOA.TTL,
			}
		}

		props["DnsProperties"] = dp
	}

	if ns.Properties.HTTPProperties != nil {
		props["HttpProperties"] = map[string]any{
			"HttpName": ns.Properties.HTTPProperties.HTTPName,
		}
	}

	return props
}

// namespaceToMap converts a Namespace to a JSON-serialisable map including Properties.
func namespaceToMap(ns *Namespace) map[string]any {
	m := map[string]any{
		"Id":           ns.ID,
		keyArn:         ns.ARN,
		"Name":         ns.Name,
		keyType:        ns.Type,
		"Description":  ns.Description,
		keyTags:        mapToTagEntries(ns.Tags),
		keyCreateDate:  awstime.Epoch(ns.CreatedAt),
		"ServiceCount": ns.ServiceCount,
	}

	if props := namespacePropertiesToMap(ns); props != nil {
		m["Properties"] = props
	}

	return m
}

type updateNamespaceChange struct {
	Description string `json:"Description"`
}

type updateHTTPNamespaceRequest struct {
	ID               string                `json:"Id"`
	UpdaterRequestID string                `json:"UpdaterRequestId"`
	Namespace        updateNamespaceChange `json:"Namespace"`
}

func (h *Handler) handleUpdateHTTPNamespace(_ context.Context, body []byte) ([]byte, error) {
	var req updateHTTPNamespaceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	opID, err := h.Backend.UpdateHTTPNamespace(req.ID, req.Namespace.Description)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyOperationID: opID})
}

type updatePrivateDNSNamespaceRequest struct {
	ID               string                `json:"Id"`
	UpdaterRequestID string                `json:"UpdaterRequestId"`
	Namespace        updateNamespaceChange `json:"Namespace"`
}

func (h *Handler) handleUpdatePrivateDNSNamespace(_ context.Context, body []byte) ([]byte, error) {
	var req updatePrivateDNSNamespaceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	opID, err := h.Backend.UpdatePrivateDNSNamespace(req.ID, req.Namespace.Description)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyOperationID: opID})
}

type updatePublicDNSNamespaceRequest struct {
	ID               string                `json:"Id"`
	UpdaterRequestID string                `json:"UpdaterRequestId"`
	Namespace        updateNamespaceChange `json:"Namespace"`
}

func (h *Handler) handleUpdatePublicDNSNamespace(_ context.Context, body []byte) ([]byte, error) {
	var req updatePublicDNSNamespaceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	opID, err := h.Backend.UpdatePublicDNSNamespace(req.ID, req.Namespace.Description)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyOperationID: opID})
}

func applyPaginationNamespaces(items []Namespace, nextToken string, maxResults int) ([]Namespace, string) {
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
