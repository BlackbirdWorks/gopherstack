package sesv2

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// ---- multi-region endpoints ----
//
// Field-diffed against aws-sdk-go-v2/service/sesv2 v1.60.1's
// CreateMultiRegionEndpointInput/Output, GetMultiRegionEndpointOutput,
// types.MultiRegionEndpoint, types.Details/RouteDetails/Route, and the
// Status enum (CREATING/READY/FAILED/DELETING). gopherstack provisions
// endpoints synchronously, so Status is always READY immediately after
// create (never CREATING/FAILED) and DELETING immediately after delete
// (matching "status right after the delete request", the exact wording
// DeleteMultiRegionEndpointOutput.Status documents).

const (
	multiRegionEndpointStatusReady    = "READY"
	multiRegionEndpointStatusDeleting = "DELETING"
)

// CreateMultiRegionEndpoint creates a multi-region endpoint (global-endpoint)
// spanning the backend's primary region and the given secondary regions.
func (b *InMemoryBackend) CreateMultiRegionEndpoint(
	endpointName string,
	secondaryRegions []string,
	tags map[string]string,
) (*createMultiRegionEndpointOutput, error) {
	b.mu.Lock("CreateMultiRegionEndpoint")
	defer b.mu.Unlock()

	if _, exists := b.multiRegionEndpoints[endpointName]; exists {
		return nil, fmt.Errorf("%w: MultiRegionEndpoint %s already exists", ErrAlreadyExists, endpointName)
	}

	now := awstime.Epoch(time.Now())
	regions := append([]string{b.region}, secondaryRegions...)

	ep := map[string]any{
		keyEndpointID:          "mre-" + uuid.New().String(),
		"EndpointName":         endpointName,
		keyStatus:              multiRegionEndpointStatusReady,
		"Regions":              regions,
		keyCreatedTimestamp:    now,
		"LastUpdatedTimestamp": now,
	}

	if len(tags) > 0 {
		ep[keyTags] = tagsToEntries(tags)
	}

	b.multiRegionEndpoints[endpointName] = ep

	return &createMultiRegionEndpointOutput{
		EndpointID: mapString(ep, keyEndpointID),
		Status:     mapString(ep, keyStatus),
	}, nil
}

// GetMultiRegionEndpoint returns the full endpoint record, matching
// GetMultiRegionEndpointOutput (CreatedTimestamp/EndpointId/EndpointName/
// LastUpdatedTimestamp/Routes/Status).
func (b *InMemoryBackend) GetMultiRegionEndpoint(endpointName string) (*multiRegionEndpointOutput, error) {
	b.mu.RLock("GetMultiRegionEndpoint")
	defer b.mu.RUnlock()

	ep, ok := b.multiRegionEndpoints[endpointName]
	if !ok {
		return nil, fmt.Errorf("%w: MultiRegionEndpoint %s not found", ErrNotFound, endpointName)
	}

	return toMultiRegionEndpointOutput(ep), nil
}

// DeleteMultiRegionEndpoint removes an endpoint and returns the status right
// after the delete request (DELETING), matching DeleteMultiRegionEndpointOutput.
func (b *InMemoryBackend) DeleteMultiRegionEndpoint(endpointName string) (string, error) {
	b.mu.Lock("DeleteMultiRegionEndpoint")
	defer b.mu.Unlock()

	if _, ok := b.multiRegionEndpoints[endpointName]; !ok {
		return "", fmt.Errorf("%w: MultiRegionEndpoint %s not found", ErrNotFound, endpointName)
	}

	delete(b.multiRegionEndpoints, endpointName)

	return multiRegionEndpointStatusDeleting, nil
}

// multiRegionEndpointSummary projects a full endpoint record down to the
// types.MultiRegionEndpoint shape used by ListMultiRegionEndpoints (no Routes
// field -- Regions instead).
func multiRegionEndpointSummary(ep map[string]any) map[string]any {
	return map[string]any{
		keyEndpointID:          ep[keyEndpointID],
		"EndpointName":         ep["EndpointName"],
		keyStatus:              ep[keyStatus],
		"Regions":              ep["Regions"],
		keyCreatedTimestamp:    ep[keyCreatedTimestamp],
		"LastUpdatedTimestamp": ep["LastUpdatedTimestamp"],
	}
}

// ListMultiRegionEndpoints lists all multi-region endpoints, paginated.
func (b *InMemoryBackend) ListMultiRegionEndpoints(
	nextToken string,
	pageSize int,
) ([]multiRegionEndpointSummaryOutput, string, error) {
	b.mu.RLock("ListMultiRegionEndpoints")

	all := make([]map[string]any, 0, len(b.multiRegionEndpoints))
	for _, ep := range b.multiRegionEndpoints {
		all = append(all, multiRegionEndpointSummary(ep))
	}

	b.mu.RUnlock()

	sortMapsByStringKey(all, "EndpointName")

	page, next, err := paginateMaps(all, nextToken, pageSize, "EndpointName")
	if err != nil {
		return nil, "", err
	}

	out := make([]multiRegionEndpointSummaryOutput, 0, len(page))
	for _, ep := range page {
		out = append(out, toMultiRegionEndpointSummaryOutput(ep))
	}

	return out, next, nil
}
