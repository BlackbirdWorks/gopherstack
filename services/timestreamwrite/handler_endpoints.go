package timestreamwrite

import "context"

// endpointCachePeriodMinutes matches the real AWS DescribeEndpoints response.
const endpointCachePeriodMinutes = 1440

type describeEndpointsInput struct{}

type endpointOutput struct {
	Endpoints []endpointEntry `json:"Endpoints"`
}

type endpointEntry struct {
	Address              string `json:"Address"`
	CachePeriodInMinutes int64  `json:"CachePeriodInMinutes"`
}

// handleDescribeEndpoints echoes back the request's own Host (scheme included,
// since aws-sdk-go-v2 defaults to https for any Address with no "://" prefix).
// Timestream Write's client-side endpoint discovery is mandatory
// (EndpointDiscoveryRequired: true, unlike DynamoDB's opt-in discovery), so
// every subsequent call (CreateDatabase, WriteRecords, ...) is routed to
// whatever this returns -- a hardcoded "localhost" previously sent every
// discovery-driven call to the wrong port and errored with connection refused.
func (h *Handler) handleDescribeEndpoints(
	ctx context.Context,
	_ *describeEndpointsInput,
) (*endpointOutput, error) {
	host, _ := requestHostKey.Get(ctx)
	if host == "" {
		host = "localhost"
	}

	return &endpointOutput{
		Endpoints: []endpointEntry{
			{Address: "http://" + host, CachePeriodInMinutes: endpointCachePeriodMinutes},
		},
	}, nil
}
