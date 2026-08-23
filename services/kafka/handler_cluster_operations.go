package kafka

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v5"
)

type describeClusterOperationOutput struct {
	ClusterOperationInfo *ClusterOperation `json:"clusterOperationInfo"`
}

func (h *Handler) handleDescribeClusterOperation(
	ctx context.Context,
	c *echo.Context,
	clusterOperationArn string,
) error {
	op, err := h.Backend.DescribeClusterOperation(ctx, clusterOperationArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeClusterOperationOutput{ClusterOperationInfo: op})
}

type listClusterOperationsOutput struct {
	ClusterOperationInfoList []*ClusterOperation `json:"clusterOperationInfoList"`
}

type describeClusterOperationV2Output struct {
	ClusterOperationInfo *ClusterOperation `json:"clusterOperationInfo"`
}

// clusterOperationV2SummaryOutput mirrors types.ClusterOperationV2Summary, the
// real ListClusterOperationsV2 element shape -- unlike V1 (where List and
// Describe share one real type, ClusterOperationInfo, so reusing
// *ClusterOperation there is correct), V2 declares a genuinely narrower
// Summary: no sourceClusterInfo/targetClusterInfo. This backend's V2 ops
// forward to the V1 backend (DescribeClusterOperationV2/
// ListClusterOperationsV2 below), which only ever populates the V1-shaped
// *ClusterOperation, so clusterType/startTime/endTime -- real required
// ClusterOperationV2Summary members -- are left absent rather than
// fabricated (this backend tracks none of the three).
//
// operationArn uses the correct real wire key. Describe/V1 already emit the
// same field under "operationArn" too (ClusterOperation.ClusterOperationArn's
// json tag, models.go) -- gopherstack-mk3t's item 1 (wrong key on those ops)
// is stale, fixed by commit fb80d66c. mk3t's item 2, the wider V2
// Provisioned/Serverless/ErrorInfo remodel, is still open.
type clusterOperationV2SummaryOutput struct {
	ClusterArn     string `json:"clusterArn"`
	OperationArn   string `json:"operationArn"`
	OperationState string `json:"operationState"`
	OperationType  string `json:"operationType"`
}

type listClusterOperationsV2Output struct {
	ClusterOperationInfoList []clusterOperationV2SummaryOutput `json:"clusterOperationInfoList"`
}

func (h *Handler) handleDescribeClusterOperationV2(
	ctx context.Context,
	c *echo.Context,
	clusterOperationArn string,
) error {
	op, err := h.Backend.DescribeClusterOperationV2(ctx, clusterOperationArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeClusterOperationV2Output{ClusterOperationInfo: op})
}

func (h *Handler) handleListClusterOperations(
	ctx context.Context,
	c *echo.Context,
	clusterArn string,
) error {
	ops, err := h.Backend.ListClusterOperations(ctx, clusterArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listClusterOperationsOutput{ClusterOperationInfoList: ops})
}

func (h *Handler) handleListClusterOperationsV2(
	ctx context.Context,
	c *echo.Context,
	clusterArn string,
) error {
	ops, err := h.Backend.ListClusterOperationsV2(ctx, clusterArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	summaries := make([]clusterOperationV2SummaryOutput, len(ops))
	for i, op := range ops {
		summaries[i] = clusterOperationV2SummaryOutput{
			ClusterArn:     op.ClusterArn,
			OperationArn:   op.ClusterOperationArn,
			OperationState: op.OperationState,
			OperationType:  op.OperationType,
		}
	}

	return c.JSON(http.StatusOK, listClusterOperationsV2Output{ClusterOperationInfoList: summaries})
}
