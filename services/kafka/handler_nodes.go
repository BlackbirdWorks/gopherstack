package kafka

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v5"
)

type listKafkaVersionsOutput struct {
	KafkaVersions []*MSKVersion `json:"kafkaVersions"`
}

type compatibleKafkaVersionsOutput struct {
	CompatibleKafkaVersions []*CompatibleKafkaVersion `json:"compatibleKafkaVersions"`
}

type listNodesOutput struct {
	NodeInfoList []*NodeInfo `json:"nodeInfoList"`
}

func (h *Handler) handleListKafkaVersions(ctx context.Context, c *echo.Context) error {
	versions := h.Backend.ListKafkaVersions(ctx)

	return c.JSON(http.StatusOK, listKafkaVersionsOutput{KafkaVersions: versions})
}

// handleGetCompatibleKafkaVersions serves GET /v1/compatible-kafka-versions.
// Unlike most cluster-scoped ops, the real MSK API carries the cluster ARN as
// a query parameter (clusterArn) on this top-level path, not a URI path segment.
func (h *Handler) handleGetCompatibleKafkaVersions(ctx context.Context, c *echo.Context) error {
	clusterArn := c.Request().URL.Query().Get("clusterArn")

	versions, err := h.Backend.GetCompatibleKafkaVersions(ctx, clusterArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, compatibleKafkaVersionsOutput{CompatibleKafkaVersions: versions})
}

func (h *Handler) handleListNodes(ctx context.Context, c *echo.Context, clusterArn string) error {
	nodes, err := h.Backend.ListNodes(ctx, clusterArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listNodesOutput{NodeInfoList: nodes})
}
