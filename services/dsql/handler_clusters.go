package dsql

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"
)

// dispatchClusterOps handles cluster CRUD and vpc-endpoint-service-name operations.
func (h *Handler) dispatchClusterOps(
	ctx context.Context,
	c *echo.Context,
	op, resource string,
	body []byte,
) (bool, error) {
	switch op {
	case opCreateCluster:
		return true, h.handleCreateCluster(ctx, c, body)
	case opGetCluster:
		return true, h.handleGetCluster(c, resource)
	case opListClusters:
		return true, h.handleListClusters(c)
	case opUpdateCluster:
		return true, h.handleUpdateCluster(c, resource, body)
	case opDeleteCluster:
		return true, h.handleDeleteCluster(c, resource)
	case opGetVpcEndpointServiceName:
		return true, h.handleGetVpcEndpointServiceName(ctx, c, resource)
	}

	return false, nil
}

func (h *Handler) handleCreateCluster(ctx context.Context, c *echo.Context, body []byte) error {
	var req createClusterRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return h.writeInvalidBody(c)
		}
	}

	region := regionFromContext(ctx, h.DefaultRegion)

	in := CreateClusterInput{
		DeletionProtectionEnabled: req.DeletionProtectionEnabled,
		KmsEncryptionKey:          req.KmsEncryptionKey,
		MultiRegion:               multiRegionFromDTO(req.MultiRegionProperties),
		Policy:                    req.Policy,
		Tags:                      req.Tags,
	}

	cluster, err := h.Backend.CreateCluster(h.AccountID, region, in)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, clusterResponseFromCluster(cluster, false))
}

func (h *Handler) handleGetCluster(c *echo.Context, identifier string) error {
	cluster, err := h.Backend.GetCluster(identifier)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, clusterResponseFromCluster(cluster, true))
}

func (h *Handler) handleListClusters(c *echo.Context) error {
	q := c.Request().URL.Query()

	maxResults := 0
	if v := q.Get("max-results"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			maxResults = n
		}
	}

	clusters, next, err := h.Backend.ListClusters(q.Get("next-token"), maxResults)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	summaries := make([]clusterSummaryDTO, 0, len(clusters))
	for _, cl := range clusters {
		summaries = append(summaries, clusterSummaryDTO{Arn: cl.ARN, Identifier: cl.Identifier})
	}

	return c.JSON(http.StatusOK, listClustersResponse{Clusters: summaries, NextToken: next})
}

func (h *Handler) handleUpdateCluster(c *echo.Context, identifier string, body []byte) error {
	var req updateClusterRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return h.writeInvalidBody(c)
		}
	}

	in := UpdateClusterInput{
		DeletionProtectionEnabled: req.DeletionProtectionEnabled,
		KmsEncryptionKey:          req.KmsEncryptionKey,
		MultiRegion:               multiRegionFromDTO(req.MultiRegionProperties),
	}

	cluster, err := h.Backend.UpdateCluster(identifier, in)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, updateOrDeleteResponseFromCluster(cluster))
}

func (h *Handler) handleDeleteCluster(c *echo.Context, identifier string) error {
	cluster, err := h.Backend.DeleteCluster(identifier)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, updateOrDeleteResponseFromCluster(cluster))
}

func (h *Handler) handleGetVpcEndpointServiceName(ctx context.Context, c *echo.Context, identifier string) error {
	region := regionFromContext(ctx, h.DefaultRegion)

	serviceName, clusterVpcEndpoint, err := h.Backend.GetVpcEndpointServiceName(identifier, region)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, getVpcEndpointServiceNameResponse{
		ClusterVpcEndpoint: clusterVpcEndpoint,
		ServiceName:        serviceName,
	})
}
