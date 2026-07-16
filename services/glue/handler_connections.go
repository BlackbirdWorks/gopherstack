package glue

import (
	"context"
	"fmt"
)

type batchDeleteConnectionInput struct {
	ConnectionNameList []string `json:"ConnectionNameList"`
}

type batchDeleteConnectionOutput struct {
	Errors    []ErrorDetail `json:"Errors"`
	Succeeded []string      `json:"Succeeded"`
}

func (h *Handler) handleBatchDeleteConnection(
	_ context.Context,
	in *batchDeleteConnectionInput,
) (*batchDeleteConnectionOutput, error) {
	succeeded, errs := h.Backend.BatchDeleteConnection(in.ConnectionNameList)

	return &batchDeleteConnectionOutput{Succeeded: succeeded, Errors: errs}, nil
}

type createConnectionInput struct {
	Tags            map[string]string `json:"Tags,omitempty"`
	ConnectionInput connectionInput   `json:"ConnectionInput"`
}

type connectionInput struct {
	ConnectionProperties map[string]string `json:"ConnectionProperties,omitempty"`
	Name                 string            `json:"Name"`
	ConnectionType       string            `json:"ConnectionType,omitempty"`
}

type createConnectionOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleCreateConnection(
	_ context.Context,
	in *createConnectionInput,
) (*createConnectionOutput, error) {
	c, err := h.Backend.CreateConnection(
		in.ConnectionInput.Name,
		in.ConnectionInput.ConnectionType,
		in.ConnectionInput.ConnectionProperties,
		in.Tags,
	)
	if err != nil {
		return nil, err
	}

	return &createConnectionOutput{Name: c.Name}, nil
}

type getConnectionInput struct {
	Name string `json:"Name"`
}

type getConnectionOutput struct {
	Connection *Connection `json:"Connection"`
}

func (h *Handler) handleGetConnection(
	_ context.Context,
	in *getConnectionInput,
) (*getConnectionOutput, error) {
	c, err := h.Backend.GetConnection(in.Name)
	if err != nil {
		return nil, err
	}

	return &getConnectionOutput{Connection: c}, nil
}

type getConnectionsInput struct{}

type getConnectionsOutput struct {
	ConnectionList []*Connection `json:"ConnectionList"`
}

func (h *Handler) handleGetConnections(
	_ context.Context,
	_ *getConnectionsInput,
) (*getConnectionsOutput, error) {
	conns := h.Backend.GetConnections()

	return &getConnectionsOutput{ConnectionList: conns}, nil
}

type deleteConnectionInput struct {
	ConnectionName string `json:"ConnectionName"`
}

func (h *Handler) handleDeleteConnection(
	_ context.Context,
	in *deleteConnectionInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeleteConnection(in.ConnectionName); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// adHocTestConnectionInput holds an inline (not-yet-created) connection to test.
type adHocTestConnectionInput struct {
	ConnectionProperties map[string]string `json:"ConnectionProperties,omitempty"`
	ConnectionType       string            `json:"ConnectionType,omitempty"`
}

// testConnectionInput holds input for TestConnection.
type testConnectionInput struct {
	TestConnectionInput *adHocTestConnectionInput `json:"TestConnectionInput,omitempty"`
	CatalogID           string                    `json:"CatalogId,omitempty"`
	ConnectionName      string                    `json:"ConnectionName,omitempty"`
}

// handleTestConnection validates the named connection exists, or that an
// ad-hoc connection description is well-formed, and returns success. Real AWS
// returns an empty 200 response on success; the actual dial happens
// asynchronously and can't be modeled here.
func (h *Handler) handleTestConnection(_ context.Context, in *testConnectionInput) (*emptyOutput, error) {
	hasName := in.ConnectionName != ""
	hasAdHoc := in.TestConnectionInput != nil

	switch {
	case hasName && hasAdHoc:
		return nil, fmt.Errorf(
			"%w: specify either ConnectionName or TestConnectionInput, not both", ErrValidation,
		)
	case hasName:
		if _, err := h.Backend.GetConnection(in.ConnectionName); err != nil {
			return nil, err
		}
	case hasAdHoc:
		if in.TestConnectionInput.ConnectionType == "" || len(in.TestConnectionInput.ConnectionProperties) == 0 {
			return nil, fmt.Errorf(
				"%w: TestConnectionInput requires ConnectionType and ConnectionProperties", ErrValidation,
			)
		}
	default:
		return nil, fmt.Errorf("%w: must specify ConnectionName or TestConnectionInput", ErrValidation)
	}

	return &emptyOutput{}, nil
}

// updateConnectionInput holds input for UpdateConnection.
type updateConnectionInput struct {
	Name            string          `json:"Name"`
	ConnectionInput connectionInput `json:"ConnectionInput"`
}

func (h *Handler) handleUpdateConnection(
	_ context.Context,
	in *updateConnectionInput,
) (*emptyOutput, error) {
	if err := h.Backend.UpdateConnection(
		in.Name,
		in.ConnectionInput.ConnectionType,
		in.ConnectionInput.ConnectionProperties,
	); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}
