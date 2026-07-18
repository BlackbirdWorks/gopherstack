package apprunner

import (
	"context"
	"fmt"
)

type createConnectionInput struct {
	ConnectionName string     `json:"ConnectionName"`
	ProviderType   string     `json:"ProviderType"`
	Tags           []tagInput `json:"Tags"`
}

type connectionOutput struct {
	ConnectionArn  string `json:"ConnectionArn"`
	ConnectionName string `json:"ConnectionName"`
	ProviderType   string `json:"ProviderType"`
	Status         string `json:"Status"`
	CreatedAt      int64  `json:"CreatedAt"`
}

type createConnectionOutput struct {
	Connection connectionOutput `json:"Connection"`
}

func toConnectionOutput(c *Connection) connectionOutput {
	return connectionOutput{
		ConnectionArn:  c.ConnectionArn,
		ConnectionName: c.ConnectionName,
		ProviderType:   c.ProviderType,
		Status:         c.Status,
		CreatedAt:      c.CreatedAt.Unix(),
	}
}

func (h *Handler) handleCreateConnection(
	_ context.Context,
	in *createConnectionInput,
) (*createConnectionOutput, error) {
	if in.ConnectionName == "" {
		return nil, fmt.Errorf("%w: ConnectionName is required", errInvalidRequest)
	}

	if in.ProviderType == "" {
		return nil, fmt.Errorf("%w: ProviderType is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)
	conn, err := h.Backend.CreateConnection(in.ConnectionName, in.ProviderType, tags)
	if err != nil {
		return nil, err
	}

	return &createConnectionOutput{Connection: toConnectionOutput(conn)}, nil
}

type deleteConnectionInput struct {
	ConnectionArn string `json:"ConnectionArn"`
}

type deleteConnectionOutput struct {
	Connection connectionOutput `json:"Connection"`
}

func (h *Handler) handleDeleteConnection(
	_ context.Context,
	in *deleteConnectionInput,
) (*deleteConnectionOutput, error) {
	if in.ConnectionArn == "" {
		return nil, fmt.Errorf("%w: ConnectionArn is required", errInvalidRequest)
	}

	conn, err := h.Backend.DeleteConnection(in.ConnectionArn)
	if err != nil {
		return nil, err
	}

	return &deleteConnectionOutput{Connection: toConnectionOutput(conn)}, nil
}

type listConnectionsInput struct {
	ConnectionName string `json:"ConnectionName"`
	NextToken      string `json:"NextToken"`
	MaxResults     int32  `json:"MaxResults"`
}

type connectionSummaryOutput struct {
	ConnectionArn  string `json:"ConnectionArn"`
	ConnectionName string `json:"ConnectionName"`
	ProviderType   string `json:"ProviderType"`
	Status         string `json:"Status"`
	CreatedAt      int64  `json:"CreatedAt"`
}

type listConnectionsOutput struct {
	NextToken             string                    `json:"NextToken,omitempty"`
	ConnectionSummaryList []connectionSummaryOutput `json:"ConnectionSummaryList"`
}

func (h *Handler) handleListConnections(
	_ context.Context,
	in *listConnectionsInput,
) (*listConnectionsOutput, error) {
	conns, nextToken, err := h.Backend.ListConnections(in.ConnectionName, in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}

	out := make([]connectionSummaryOutput, 0, len(conns))
	for _, c := range conns {
		out = append(out, connectionSummaryOutput{
			ConnectionArn:  c.ConnectionArn,
			ConnectionName: c.ConnectionName,
			ProviderType:   c.ProviderType,
			Status:         c.Status,
			CreatedAt:      c.CreatedAt.Unix(),
		})
	}

	return &listConnectionsOutput{ConnectionSummaryList: out, NextToken: nextToken}, nil
}
