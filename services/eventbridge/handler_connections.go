package eventbridge

import (
	"context"
	"encoding/json"
)

type createConnectionOutput struct {
	ConnectionArn    string  `json:"ConnectionArn"`
	ConnectionState  string  `json:"ConnectionState"`
	CreationTime     float64 `json:"CreationTime"`
	LastModifiedTime float64 `json:"LastModifiedTime"`
}

type deauthorizeConnectionOutput struct {
	ConnectionArn    string  `json:"ConnectionArn"`
	ConnectionState  string  `json:"ConnectionState"`
	LastModifiedTime float64 `json:"LastModifiedTime"`
}

// connectionResponse is the handler-level DTO for Connection objects.
type connectionResponse struct {
	AuthParameters     *ConnectionAuthParameters `json:"AuthParameters,omitempty"`
	ConnectionArn      string                    `json:"ConnectionArn"`
	AuthorizationType  string                    `json:"AuthorizationType"`
	ConnectionState    string                    `json:"ConnectionState"`
	Description        string                    `json:"Description,omitempty"`
	Name               string                    `json:"Name"`
	SecretArn          string                    `json:"SecretArn,omitempty"`
	StateReason        string                    `json:"StateReason,omitempty"`
	CreationTime       float64                   `json:"CreationTime"`
	LastAuthorizedTime float64                   `json:"LastAuthorizedTime,omitempty"`
	LastModifiedTime   float64                   `json:"LastModifiedTime"`
}

func connectionToResponse(c *Connection) *connectionResponse {
	if c == nil {
		return nil
	}

	r := &connectionResponse{
		AuthParameters:    c.AuthParameters,
		ConnectionArn:     c.ConnectionArn,
		AuthorizationType: c.AuthorizationType,
		ConnectionState:   c.ConnectionState,
		CreationTime:      timeToEpochSeconds(c.CreationTime),
		Description:       c.Description,
		LastModifiedTime:  timeToEpochSeconds(c.LastModifiedTime),
		Name:              c.Name,
		SecretArn:         c.SecretArn,
		StateReason:       c.StateReason,
	}

	if !c.LastAuthorizedTime.IsZero() {
		r.LastAuthorizedTime = timeToEpochSeconds(c.LastAuthorizedTime)
	}

	return r
}

// connectionActions returns the CreateConnection and DeauthorizeConnection actions.
func (h *Handler) connectionActions() map[string]actionFn {
	return map[string]actionFn{
		"CreateConnection": func(ctx context.Context, b []byte) (any, error) {
			var input CreateConnectionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			conn, err := h.Backend.CreateConnection(ctx, input)
			if err != nil {
				return nil, err
			}

			return &createConnectionOutput{
				ConnectionArn:    conn.ConnectionArn,
				ConnectionState:  conn.ConnectionState,
				CreationTime:     timeToEpochSeconds(conn.CreationTime),
				LastModifiedTime: timeToEpochSeconds(conn.LastModifiedTime),
			}, nil
		},
		"DeauthorizeConnection": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Name string `json:"Name"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			conn, err := h.Backend.DeauthorizeConnection(ctx, input.Name)
			if err != nil {
				return nil, err
			}

			return &deauthorizeConnectionOutput{
				ConnectionArn:    conn.ConnectionArn,
				ConnectionState:  conn.ConnectionState,
				LastModifiedTime: timeToEpochSeconds(conn.LastModifiedTime),
			}, nil
		},
	}
}

// extendedConnectionActions returns CRUD actions for connections beyond Create/Deauthorize.
func (h *Handler) extendedConnectionActions() map[string]actionFn {
	return map[string]actionFn{
		"DeleteConnection": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Name string `json:"Name"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return &struct{}{}, h.Backend.DeleteConnection(ctx, input.Name)
		},
		"DescribeConnection": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Name string `json:"Name"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			conn, err := h.Backend.DescribeConnection(ctx, input.Name)
			if err != nil {
				return nil, err
			}

			return connectionToResponse(conn), nil
		},
		"ListConnections": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				NamePrefix string `json:"NamePrefix"`
				NextToken  string `json:"NextToken"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			conns, next, err := h.Backend.ListConnections(ctx, input.NamePrefix, input.NextToken)
			if err != nil {
				return nil, err
			}

			connResponses := make([]connectionResponse, len(conns))
			for i, c := range conns {
				connResponses[i] = *connectionToResponse(&c)
			}

			return &struct {
				NextToken   string               `json:"NextToken,omitempty"`
				Connections []connectionResponse `json:"Connections"`
			}{Connections: connResponses, NextToken: next}, nil
		},
		"UpdateConnection": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateConnectionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			conn, err := h.Backend.UpdateConnection(ctx, input)
			if err != nil {
				return nil, err
			}

			return &struct {
				ConnectionArn    string  `json:"ConnectionArn"`
				ConnectionState  string  `json:"ConnectionState"`
				CreationTime     float64 `json:"CreationTime"`
				LastModifiedTime float64 `json:"LastModifiedTime"`
			}{
				ConnectionArn:    conn.ConnectionArn,
				ConnectionState:  conn.ConnectionState,
				CreationTime:     timeToEpochSeconds(conn.CreationTime),
				LastModifiedTime: timeToEpochSeconds(conn.LastModifiedTime),
			}, nil
		},
	}
}
