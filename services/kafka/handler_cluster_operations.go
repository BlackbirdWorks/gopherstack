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

type listClusterOperationsV2Output struct {
	ClusterOperationInfoList []*ClusterOperation `json:"clusterOperationInfoList"`
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

	return c.JSON(http.StatusOK, listClusterOperationsV2Output{ClusterOperationInfoList: ops})
}
