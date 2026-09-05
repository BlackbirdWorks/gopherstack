package codestarconnections

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type createConnectionInput struct {
	ConnectionName string     `json:"ConnectionName"`
	ProviderType   string     `json:"ProviderType"`
	HostArn        string     `json:"HostArn"`
	Tags           []tagEntry `json:"Tags"`
}

type createConnectionOutput struct {
	ConnectionArn string     `json:"ConnectionArn"`
	Tags          []tagEntry `json:"Tags,omitempty"`
}

func (h *Handler) handleCreateConnection(
	ctx context.Context,
	in *createConnectionInput,
) (*createConnectionOutput, error) {
	if in.ConnectionName == "" {
		return nil, fmt.Errorf("%w: ConnectionName is required", errInvalidRequest)
	}

	conn, err := h.Backend.CreateConnection(
		ctx, in.ConnectionName, in.ProviderType, in.HostArn, tagsFromArray(in.Tags),
	)
	if err != nil {
		return nil, err
	}

	return &createConnectionOutput{
		ConnectionArn: conn.ConnectionArn,
		Tags:          tagsToSortedArray(conn.Tags),
	}, nil
}

type getConnectionInput struct {
	ConnectionArn string `json:"ConnectionArn"`
}

type connectionView struct {
	ConnectionName   string `json:"ConnectionName"`
	ConnectionArn    string `json:"ConnectionArn"`
	ConnectionStatus string `json:"ConnectionStatus"`
	OwnerAccountID   string `json:"OwnerAccountId"`
	ProviderType     string `json:"ProviderType"`
	HostArn          string `json:"HostArn,omitempty"`
}

type getConnectionOutput struct {
	Connection connectionView `json:"Connection"`
}

func connectionToView(c *Connection) connectionView {
	return connectionView{
		ConnectionName:   c.ConnectionName,
		ConnectionArn:    c.ConnectionArn,
		ConnectionStatus: c.ConnectionStatus,
		OwnerAccountID:   c.OwnerAccountID,
		ProviderType:     c.ProviderType,
		HostArn:          c.HostArn,
	}
}

// handleGetConnection does not pre-check for an empty ConnectionArn:
// GetConnection's own deserializeOpErrorGetConnection switch
// (codestarconnections@v1.38.4 deserializers.go) declares
// ResourceNotFoundException/ResourceUnavailableException, not
// InvalidInputException -- an ARN that matches nothing (including "")
// already answers ResourceNotFoundException through the ordinary
// lookup-miss path below (gopherstack-6flj/uox6 error-envelope sweep).
func (h *Handler) handleGetConnection(
	ctx context.Context,
	in *getConnectionInput,
) (*getConnectionOutput, error) {
	conn, err := h.Backend.GetConnection(ctx, in.ConnectionArn)
	if err != nil {
		return nil, err
	}

	return &getConnectionOutput{Connection: connectionToView(conn)}, nil
}

type listConnectionsInput struct {
	ProviderTypeFilter string `json:"ProviderTypeFilter"`
	HostArnFilter      string `json:"HostArnFilter"`
	NextToken          string `json:"NextToken"`
	MaxResults         int    `json:"MaxResults"`
}

type listConnectionsOutput struct {
	NextToken   string           `json:"NextToken,omitempty"`
	Connections []connectionView `json:"Connections"`
}

func (h *Handler) handleListConnections(
	ctx context.Context,
	in *listConnectionsInput,
) (*listConnectionsOutput, error) {
	connections := h.Backend.ListConnections(ctx, in.ProviderTypeFilter, in.HostArnFilter)

	views := make([]connectionView, len(connections))
	for i, c := range connections {
		views[i] = connectionToView(c)
	}

	p := page.New(views, in.NextToken, in.MaxResults, defaultCSCMaxResults)

	return &listConnectionsOutput{Connections: p.Data, NextToken: p.Next}, nil
}

type deleteConnectionInput struct {
	ConnectionArn string `json:"ConnectionArn"`
}

type deleteConnectionOutput struct{}

// handleDeleteConnection does not pre-check for an empty ConnectionArn: see
// handleGetConnection's doc comment -- DeleteConnection's own switch
// declares only ResourceNotFoundException, no InvalidInputException
// (gopherstack-6flj/uox6 error-envelope sweep).
func (h *Handler) handleDeleteConnection(
	ctx context.Context,
	in *deleteConnectionInput,
) (*deleteConnectionOutput, error) {
	if err := h.Backend.DeleteConnection(ctx, in.ConnectionArn); err != nil {
		return nil, err
	}

	return &deleteConnectionOutput{}, nil
}
