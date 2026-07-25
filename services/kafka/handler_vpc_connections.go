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
	ClientSubnets    []string          `json:"clientSubnets,omitempty"`
	SecurityGroups   []string          `json:"securityGroups,omitempty"`
}

// createVpcConnectionOutput mirrors CreateVpcConnectionOutput: notably it
// carries clientSubnets/securityGroups/creationTime/tags but -- unlike
// DescribeVpcConnectionOutput -- no targetClusterArn.
type createVpcConnectionOutput struct {
	Tags             map[string]string `json:"tags,omitempty"`
	VpcConnectionArn string            `json:"vpcConnectionArn"`
	VpcID            string            `json:"vpcId"`
	State            string            `json:"state"`
	Authentication   string            `json:"authentication,omitempty"`
	CreationTime     string            `json:"creationTime,omitempty"`
	ClientSubnets    []string          `json:"clientSubnets,omitempty"`
	SecurityGroups   []string          `json:"securityGroups,omitempty"`
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
		in.ClientSubnets,
		in.SecurityGroups,
		in.Tags,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createVpcConnectionOutput{
		VpcConnectionArn: conn.VpcConnectionArn,
		VpcID:            conn.VpcID,
		State:            conn.State,
		Authentication:   conn.Authentication,
		CreationTime:     conn.CreationTime,
		ClientSubnets:    conn.SubnetIDs,
		SecurityGroups:   conn.SecurityGroupIDs,
		Tags:             in.Tags,
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

// listVpcConnectionsOutput mirrors ListVpcConnectionsOutput; its element
// shape is the wider types.VpcConnection (adds targetClusterArn/vpcId versus
// the narrower ClientVpcConnection ListClientVpcConnections returns -- see
// clientVpcConnectionOutput below, a DISTINCT shape under a DISTINCT
// "clientVpcConnections" JSON key, not a naming variant of this one).
type listVpcConnectionsOutput struct {
	VpcConnections []*VpcConnection `json:"vpcConnections"`
}

// describeVpcConnectionOutput mirrors DescribeVpcConnectionOutput, which adds
// securityGroups/subnets/tags on top of the ListVpcConnections item shape.
type describeVpcConnectionOutput struct {
	Tags             map[string]string `json:"tags,omitempty"`
	Authentication   string            `json:"authentication,omitempty"`
	CreationTime     string            `json:"creationTime,omitempty"`
	State            string            `json:"state"`
	TargetClusterArn string            `json:"targetClusterArn"`
	VpcConnectionArn string            `json:"vpcConnectionArn"`
	VpcID            string            `json:"vpcId"`
	SecurityGroups   []string          `json:"securityGroups,omitempty"`
	Subnets          []string          `json:"subnets,omitempty"`
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

	return c.JSON(http.StatusOK, describeVpcConnectionOutput{
		Authentication:   v.Authentication,
		CreationTime:     v.CreationTime,
		State:            v.State,
		TargetClusterArn: v.TargetClusterArn,
		VpcConnectionArn: v.VpcConnectionArn,
		VpcID:            v.VpcID,
		SecurityGroups:   v.SecurityGroupIDs,
		Subnets:          v.SubnetIDs,
		Tags:             v.Tags,
	})
}

func (h *Handler) handleListVpcConnections(ctx context.Context, c *echo.Context) error {
	conns := h.Backend.ListVpcConnections(ctx)

	return c.JSON(http.StatusOK, listVpcConnectionsOutput{VpcConnections: conns})
}

// clientVpcConnectionOutput mirrors types.ClientVpcConnection, the
// ListClientVpcConnections element shape: vpcConnectionArn/authentication/
// creationTime/owner/state only -- no targetClusterArn (redundant with the
// path) and no vpcId.
type clientVpcConnectionOutput struct {
	Authentication   string `json:"authentication,omitempty"`
	CreationTime     string `json:"creationTime,omitempty"`
	Owner            string `json:"owner,omitempty"`
	State            string `json:"state"`
	VpcConnectionArn string `json:"vpcConnectionArn"`
}

// listClientVpcConnectionsOutput mirrors ListClientVpcConnectionsOutput. The
// real JSON key is "clientVpcConnections", a DISTINCT key from
// listVpcConnectionsOutput's "vpcConnections" -- ListClientVpcConnections and
// ListVpcConnections are different operations with different response
// shapes, not the same shape reused across two paths.
type listClientVpcConnectionsOutput struct {
	ClientVpcConnections []clientVpcConnectionOutput `json:"clientVpcConnections"`
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

	out := make([]clientVpcConnectionOutput, len(conns))
	for i, v := range conns {
		out[i] = clientVpcConnectionOutput{
			VpcConnectionArn: v.VpcConnectionArn,
			Authentication:   v.Authentication,
			CreationTime:     v.CreationTime,
			Owner:            h.Backend.AccountID(),
			State:            v.State,
		}
	}

	return c.JSON(http.StatusOK, listClientVpcConnectionsOutput{ClientVpcConnections: out})
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
