package kafka

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

type createVpcConnectionInput struct {
	Tags             map[string]string `json:"tags,omitempty"`
	TargetClusterArn string            `json:"targetClusterArn"`
	VpcID            string            `json:"vpcId"`
	Authentication   string            `json:"authentication,omitempty"`
}

type createVpcConnectionOutput struct {
	VpcConnectionArn string `json:"vpcConnectionArn"`
	TargetClusterArn string `json:"targetClusterArn"`
	VpcID            string `json:"vpcId"`
	State            string `json:"state"`
}

func (h *Handler) handleCreateVpcConnection(
	ctx context.Context,
	c *echo.Context,
	body []byte,
) error {
	var in createVpcConnectionInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	conn, err := h.Backend.CreateVpcConnection(ctx,
		in.TargetClusterArn,
		in.VpcID,
		in.Authentication,
		in.Tags,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createVpcConnectionOutput{
		VpcConnectionArn: conn.VpcConnectionArn,
		TargetClusterArn: conn.TargetClusterArn,
		VpcID:            conn.VpcID,
		State:            conn.State,
	})
}

func (h *Handler) handleDeleteVpcConnection(
	ctx context.Context,
	c *echo.Context,
	vpcConnectionArn string,
) error {
	if err := h.Backend.DeleteVpcConnection(ctx, vpcConnectionArn); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

type listVpcConnectionsOutput struct {
	VpcConnections []*VpcConnection `json:"vpcConnections"`
}

func (h *Handler) handleDescribeVpcConnection(
	ctx context.Context,
	c *echo.Context,
	vpcConnectionArn string,
) error {
	v, err := h.Backend.DescribeVpcConnection(ctx, vpcConnectionArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, v)
}

func (h *Handler) handleListVpcConnections(ctx context.Context, c *echo.Context) error {
	conns := h.Backend.ListVpcConnections(ctx)

	return c.JSON(http.StatusOK, listVpcConnectionsOutput{VpcConnections: conns})
}

func (h *Handler) handleListClientVpcConnections(
	ctx context.Context,
	c *echo.Context,
	clusterArn string,
) error {
	conns, err := h.Backend.ListClientVpcConnections(ctx, clusterArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listVpcConnectionsOutput{VpcConnections: conns})
}

type rejectClientVpcConnectionInput struct {
	VpcConnectionArn string `json:"vpcConnectionArn"`
}

// handleRejectClientVpcConnection serves PUT /v1/clusters/{ClusterArn}/client-vpc-connection.
// The path carries the owning cluster ARN; the real API carries the target VPC
// connection ARN in the JSON body (vpcConnectionArn), not the path.
func (h *Handler) handleRejectClientVpcConnection(
	ctx context.Context,
	c *echo.Context,
	body []byte,
) error {
	var in rejectClientVpcConnectionInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	if in.VpcConnectionArn == "" {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"vpcConnectionArn is required",
		)
	}

	if err := h.Backend.RejectClientVpcConnection(ctx, in.VpcConnectionArn); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}
