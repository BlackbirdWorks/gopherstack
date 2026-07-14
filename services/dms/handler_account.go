package dms

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type describeAccountAttributesInput struct{}

type accountQuotaJSON struct {
	AccountQuotaName string  `json:"AccountQuotaName"`
	Used             float64 `json:"Used"`
	Max              float64 `json:"Max"`
}

type describeAccountAttributesOutput struct {
	UniqueAccountIdentifier string             `json:"UniqueAccountIdentifier"`
	AccountQuotas           []accountQuotaJSON `json:"AccountQuotas"`
}

const (
	quotaMaxReplicationInstances = float64(60)
	quotaMaxAllocatedStorage     = float64(6000)
	quotaMaxEndpoints            = float64(1000)
	quotaMaxReplicationTasks     = float64(200)
	quotaStoragePerInstance      = int64(50)
)

func (h *Handler) handleDescribeAccountAttributes(
	ctx context.Context, _ *describeAccountAttributesInput,
) (*describeAccountAttributesOutput, error) {
	riCount := int64(len(h.Backend.mustDescribeReplicationInstances(ctx)))
	epCount := int64(len(h.Backend.mustDescribeEndpoints(ctx)))
	taskCount := int64(len(h.Backend.mustDescribeReplicationTasks(ctx)))

	return &describeAccountAttributesOutput{
		UniqueAccountIdentifier: h.Backend.AccountID(),
		AccountQuotas: []accountQuotaJSON{
			{AccountQuotaName: "ReplicationInstances", Used: float64(riCount), Max: quotaMaxReplicationInstances},
			{
				AccountQuotaName: "AllocatedStorage",
				Used:             float64(riCount * quotaStoragePerInstance),
				Max:              quotaMaxAllocatedStorage,
			},
			{AccountQuotaName: "Endpoints", Used: float64(epCount), Max: quotaMaxEndpoints},
			{AccountQuotaName: "ReplicationTasks", Used: float64(taskCount), Max: quotaMaxReplicationTasks},
		},
	}, nil
}

// opsAccount returns the dispatch-table entries for the account operation family.
func (h *Handler) opsAccount() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opDescribeAccountAttributes: service.WrapOp(
			h.handleDescribeAccountAttributes,
		),
	}
}
