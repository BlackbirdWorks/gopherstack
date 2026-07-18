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

func (h *Handler) handleDescribeEndpoints(
	_ context.Context,
	_ *describeEndpointsInput,
) (*endpointOutput, error) {
	return &endpointOutput{
		Endpoints: []endpointEntry{
			{Address: "localhost", CachePeriodInMinutes: endpointCachePeriodMinutes},
		},
	}, nil
}
