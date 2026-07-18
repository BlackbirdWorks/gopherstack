package kafka

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

type createReplicatorInput struct {
	Tags                    map[string]string `json:"tags,omitempty"`
	ReplicatorName          string            `json:"replicatorName"`
	Description             string            `json:"description,omitempty"`
	ServiceExecutionRoleArn string            `json:"serviceExecutionRoleArn"`
}

type createReplicatorOutput struct {
	ReplicatorArn   string `json:"replicatorArn"`
	ReplicatorName  string `json:"replicatorName"`
	ReplicatorState string `json:"replicatorState"`
}

func (h *Handler) handleCreateReplicator(ctx context.Context, c *echo.Context, body []byte) error {
	var in createReplicatorInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	replicator, err := h.Backend.CreateReplicator(ctx,
		in.ReplicatorName,
		in.Description,
		in.ServiceExecutionRoleArn,
		in.Tags,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createReplicatorOutput{
		ReplicatorArn:   replicator.ReplicatorArn,
		ReplicatorName:  replicator.ReplicatorName,
		ReplicatorState: replicator.ReplicatorState,
	})
}

func (h *Handler) handleDeleteReplicator(
	ctx context.Context,
	c *echo.Context,
	replicatorArn string,
) error {
	if err := h.Backend.DeleteReplicator(ctx, replicatorArn); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

type listReplicatorsOutput struct {
	NextToken   string        `json:"nextToken,omitempty"`
	Replicators []*Replicator `json:"replicators"`
}

type updateReplicationInfoInput struct {
	Description string `json:"description,omitempty"`
}

type updateReplicationInfoOutput struct {
	ReplicatorArn   string `json:"replicatorArn"`
	ReplicatorState string `json:"replicatorState"`
}

func (h *Handler) handleDescribeReplicator(
	ctx context.Context,
	c *echo.Context,
	replicatorArn string,
) error {
	r, err := h.Backend.DescribeReplicator(ctx, replicatorArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, r)
}

func (h *Handler) handleListReplicators(ctx context.Context, c *echo.Context) error {
	all := h.Backend.ListReplicators(ctx)

	token := c.Request().URL.Query().Get("nextToken")
	offset := decodeKafkaPageToken(token)

	offset = min(offset, len(all))

	page := all[offset:]
	pageSize := kafkaPageSize(c)

	var nextToken string

	if len(page) > pageSize {
		page = page[:pageSize]
		nextToken = encodeKafkaPageToken(offset + pageSize)
	}

	return c.JSON(http.StatusOK, listReplicatorsOutput{Replicators: page, NextToken: nextToken})
}

func (h *Handler) handleUpdateReplicationInfo(
	ctx context.Context,
	c *echo.Context,
	replicatorArn string,
	body []byte,
) error {
	var in updateReplicationInfoInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	r, err := h.Backend.UpdateReplicationInfo(ctx, replicatorArn, in.Description)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, updateReplicationInfoOutput{
		ReplicatorArn:   r.ReplicatorArn,
		ReplicatorState: r.ReplicatorState,
	})
}
