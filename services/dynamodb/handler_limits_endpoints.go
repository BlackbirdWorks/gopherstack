// Package dynamodb implements the AWS DynamoDB mock service.
// handler_limits_endpoints.go implements the wire-JSON handlers for
// DescribeLimits and DescribeEndpoints. Routing (dispatchExtraOps) stays in
// handler.go; these are the leaf implementations it calls into. Backend
// logic lives in limits_endpoints.go.
package dynamodb

import (
	"context"

	sdkDDB "github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
)

type describeLimitsOutput struct {
	AccountMaxReadCapacityUnits  int64 `json:"AccountMaxReadCapacityUnits"`
	AccountMaxWriteCapacityUnits int64 `json:"AccountMaxWriteCapacityUnits"`
	TableMaxReadCapacityUnits    int64 `json:"TableMaxReadCapacityUnits"`
	TableMaxWriteCapacityUnits   int64 `json:"TableMaxWriteCapacityUnits"`
}

type endpointWire struct {
	Address              string `json:"Address"`
	CachePeriodInMinutes int64  `json:"CachePeriodInMinutes"`
}

type describeEndpointsOutput struct {
	Endpoints []endpointWire `json:"Endpoints"`
}

func (h *DynamoDBHandler) handleDescribeLimits(ctx context.Context) (any, error) {
	out, err := h.Backend.DescribeLimits(ctx, &sdkDDB.DescribeLimitsInput{})
	if err != nil {
		return nil, err
	}

	var accountRCU, accountWCU, tableRCU, tableWCU int64

	if out.AccountMaxReadCapacityUnits != nil {
		accountRCU = *out.AccountMaxReadCapacityUnits
	}

	if out.AccountMaxWriteCapacityUnits != nil {
		accountWCU = *out.AccountMaxWriteCapacityUnits
	}

	if out.TableMaxReadCapacityUnits != nil {
		tableRCU = *out.TableMaxReadCapacityUnits
	}

	if out.TableMaxWriteCapacityUnits != nil {
		tableWCU = *out.TableMaxWriteCapacityUnits
	}

	return &describeLimitsOutput{
		AccountMaxReadCapacityUnits:  accountRCU,
		AccountMaxWriteCapacityUnits: accountWCU,
		TableMaxReadCapacityUnits:    tableRCU,
		TableMaxWriteCapacityUnits:   tableWCU,
	}, nil
}

func (h *DynamoDBHandler) handleDescribeEndpoints(ctx context.Context) (any, error) {
	out, err := h.Backend.DescribeEndpoints(ctx, &sdkDDB.DescribeEndpointsInput{})
	if err != nil {
		return nil, err
	}

	endpoints := make([]endpointWire, 0, len(out.Endpoints))
	for _, e := range out.Endpoints {
		endpoints = append(endpoints, endpointWire{
			Address:              ptrconv.String(e.Address),
			CachePeriodInMinutes: e.CachePeriodInMinutes,
		})
	}

	return &describeEndpointsOutput{Endpoints: endpoints}, nil
}
