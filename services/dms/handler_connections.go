package dms

import (
	"context"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type deleteConnectionInput struct {
	EndpointArn            *string `json:"EndpointArn"`
	ReplicationInstanceArn *string `json:"ReplicationInstanceArn"`
}

type deleteConnectionOutput struct {
	Connection connectionJSON `json:"Connection"`
}

func (h *Handler) handleDeleteConnection(
	ctx context.Context, in *deleteConnectionInput,
) (*deleteConnectionOutput, error) {
	conn, err := h.Backend.DeleteConnection(
		ctx,
		ptrconv.String(in.ReplicationInstanceArn),
		ptrconv.String(in.EndpointArn),
	)
	if err != nil {
		return nil, err
	}

	return &deleteConnectionOutput{Connection: connToJSON(conn)}, nil
}

type describeConnectionsInput struct {
	Marker     *string       `json:"Marker"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeConnectionsOutput struct {
	Marker      *string          `json:"Marker,omitempty"`
	Connections []connectionJSON `json:"Connections"`
}

func (h *Handler) handleDescribeConnections(
	ctx context.Context, in *describeConnectionsInput,
) (*describeConnectionsOutput, error) {
	riArn := extractFilterValue(in.Filters, "replication-instance-id")
	epArn := extractFilterValue(in.Filters, "endpoint-id")

	list, err := h.Backend.DescribeConnections(ctx, riArn, epArn)
	if err != nil {
		return nil, err
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].EndpointIdentifier < list[j].EndpointIdentifier
	})

	all := make([]connectionJSON, 0, len(list))
	for _, conn := range list {
		all = append(all, connToJSON(conn))
	}

	return &describeConnectionsOutput{Connections: all}, nil
}

type testConnectionInput struct {
	ReplicationInstanceArn *string `json:"ReplicationInstanceArn"`
	EndpointArn            *string `json:"EndpointArn"`
}

type connectionJSON struct {
	ReplicationInstanceArn        string `json:"ReplicationInstanceArn,omitempty"`
	ReplicationInstanceIdentifier string `json:"ReplicationInstanceIdentifier,omitempty"`
	EndpointArn                   string `json:"EndpointArn,omitempty"`
	EndpointIdentifier            string `json:"EndpointIdentifier,omitempty"`
	Status                        string `json:"Status"`
	LastFailureMessage            string `json:"LastFailureMessage,omitempty"`
}

type testConnectionOutput struct {
	Connection connectionJSON `json:"Connection"`
}

func connToJSON(c *Connection) connectionJSON {
	return connectionJSON{
		ReplicationInstanceArn:        c.ReplicationInstanceArn,
		ReplicationInstanceIdentifier: c.ReplicationInstanceIdentifier,
		EndpointArn:                   c.EndpointArn,
		EndpointIdentifier:            c.EndpointIdentifier,
		Status:                        c.Status,
		LastFailureMessage:            c.LastFailureMessage,
	}
}

func (h *Handler) handleTestConnection(
	ctx context.Context, in *testConnectionInput,
) (*testConnectionOutput, error) {
	conn, err := h.Backend.TestConnection(
		ctx,
		ptrconv.String(in.ReplicationInstanceArn),
		ptrconv.String(in.EndpointArn),
	)
	if err != nil {
		return nil, err
	}

	return &testConnectionOutput{Connection: connToJSON(conn)}, nil
}

// opsConnections returns the dispatch-table entries for the connections operation family.
func (h *Handler) opsConnections() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opDeleteConnection:    service.WrapOp(h.handleDeleteConnection),
		opDescribeConnections: service.WrapOp(h.handleDescribeConnections),
		opTestConnection:      service.WrapOp(h.handleTestConnection),
	}
}
