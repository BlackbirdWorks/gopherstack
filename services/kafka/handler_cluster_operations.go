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

// clusterOperationV2ProvisionedOutput mirrors types.ClusterOperationV2Provisioned
// (types.go:511, 4 of 4 members per its deserializer's case list).
// operationSteps/vpcConnectionInfo remain unmodeled: this backend doesn't
// track step-by-step operation progress, and CreateVpcConnection/
// DeleteVpcConnection don't create a ClusterOperation record at all
// (disclosed gaps, gopherstack-mk3t).
type clusterOperationV2ProvisionedOutput struct {
	SourceClusterInfo *MutableClusterInfo `json:"sourceClusterInfo,omitempty"`
	TargetClusterInfo *MutableClusterInfo `json:"targetClusterInfo,omitempty"`
}

// clusterOperationV2Output mirrors types.ClusterOperationV2 (types.go:475, 10
// of 10 members per its deserializer's case list) -- the real
// DescribeClusterOperationV2 shape, genuinely different from V1's
// ClusterOperationInfo: no top-level sourceClusterInfo/targetClusterInfo (real
// MSK nests those under provisioned/serverless), plus clusterType/startTime/
// endTime. gopherstack-mk3t item 2: this backend previously reused the V1
// *ClusterOperation struct verbatim here, emitting sourceClusterInfo/
// targetClusterInfo at the wrong (top) level and omitting clusterType
// entirely.
//
// serverless is always omitted, not fabricated: every op that calls
// newClusterOperationLocked (cluster_updates.go) only ever targets a
// provisioned cluster, so this backend never produces a serverless cluster
// operation. errorInfo is always omitted too: operations here always
// complete synchronously as UPDATE_COMPLETE, so there is never a real error
// to report.
type clusterOperationV2Output struct {
	Provisioned    *clusterOperationV2ProvisionedOutput `json:"provisioned,omitempty"`
	ClusterArn     string                               `json:"clusterArn"`
	ClusterType    string                               `json:"clusterType,omitempty"`
	OperationArn   string                               `json:"operationArn"`
	OperationState string                               `json:"operationState"`
	OperationType  string                               `json:"operationType"`
	StartTime      string                               `json:"startTime,omitempty"`
	EndTime        string                               `json:"endTime,omitempty"`
}

type describeClusterOperationV2Output struct {
	ClusterOperationInfo *clusterOperationV2Output `json:"clusterOperationInfo"`
}

// clusterOperationV2SummaryOutput mirrors types.ClusterOperationV2Summary (7
// of 7 members per its deserializer's case list) -- unlike V1 (where List and
// Describe share one real type, ClusterOperationInfo, so reusing
// *ClusterOperation there is correct), V2 declares a genuinely narrower
// Summary: no sourceClusterInfo/targetClusterInfo.
//
// operationArn uses the correct real wire key. Describe/V1 already emit the
// same field under "operationArn" too (ClusterOperation.ClusterOperationArn's
// json tag, models.go) -- gopherstack-mk3t's item 1 (wrong key on those ops)
// is stale, fixed by commit fb80d66c.
type clusterOperationV2SummaryOutput struct {
	ClusterArn     string `json:"clusterArn"`
	ClusterType    string `json:"clusterType,omitempty"`
	OperationArn   string `json:"operationArn"`
	OperationState string `json:"operationState"`
	OperationType  string `json:"operationType"`
	StartTime      string `json:"startTime,omitempty"`
	EndTime        string `json:"endTime,omitempty"`
}

type listClusterOperationsV2Output struct {
	ClusterOperationInfoList []clusterOperationV2SummaryOutput `json:"clusterOperationInfoList"`
}

// clusterTypeForOperation resolves the ClusterType of the cluster op targets.
// Falls back to ClusterTypeProvisioned when the cluster can no longer be
// found (e.g. deleted since the operation ran): every operation this backend
// ever creates targets a provisioned cluster (see clusterOperationV2Output's
// doc comment), so that fallback never actually guesses wrong in practice.
func (h *Handler) clusterTypeForOperation(ctx context.Context, op *ClusterOperation) string {
	if cl, err := h.Backend.DescribeCluster(ctx, op.ClusterArn); err == nil {
		return cl.ClusterType
	}

	return ClusterTypeProvisioned
}

func toClusterOperationV2Output(op *ClusterOperation, clusterType string) *clusterOperationV2Output {
	out := &clusterOperationV2Output{
		ClusterArn:     op.ClusterArn,
		ClusterType:    clusterType,
		OperationArn:   op.ClusterOperationArn,
		OperationState: op.OperationState,
		OperationType:  op.OperationType,
		StartTime:      op.CreationTime,
		EndTime:        op.EndTime,
	}

	if op.SourceClusterInfo != nil || op.TargetClusterInfo != nil {
		out.Provisioned = &clusterOperationV2ProvisionedOutput{
			SourceClusterInfo: op.SourceClusterInfo,
			TargetClusterInfo: op.TargetClusterInfo,
		}
	}

	return out
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

	clusterType := h.clusterTypeForOperation(ctx, op)

	return c.JSON(http.StatusOK, describeClusterOperationV2Output{
		ClusterOperationInfo: toClusterOperationV2Output(op, clusterType),
	})
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

	var clusterType string
	if len(ops) > 0 {
		clusterType = h.clusterTypeForOperation(ctx, ops[0])
	}

	summaries := make([]clusterOperationV2SummaryOutput, len(ops))
	for i, op := range ops {
		summaries[i] = clusterOperationV2SummaryOutput{
			ClusterArn:     op.ClusterArn,
			ClusterType:    clusterType,
			OperationArn:   op.ClusterOperationArn,
			OperationState: op.OperationState,
			OperationType:  op.OperationType,
			StartTime:      op.CreationTime,
			EndTime:        op.EndTime,
		}
	}

	return c.JSON(http.StatusOK, listClusterOperationsV2Output{ClusterOperationInfoList: summaries})
}
