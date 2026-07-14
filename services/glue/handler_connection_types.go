package glue

import (
	"context"
	"fmt"
)

// deleteConnectionTypeInput holds input for DeleteConnectionType.
type deleteConnectionTypeInput struct {
	ConnectionType string `json:"ConnectionType"`
}

func (h *Handler) handleDeleteConnectionType(
	_ context.Context,
	in *deleteConnectionTypeInput,
) (*emptyOutput, error) {
	if in.ConnectionType == "" {
		return nil, fmt.Errorf("%w: ConnectionType is required", ErrValidation)
	}

	if err := h.Backend.DeleteConnectionType(in.ConnectionType); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// describeConnectionTypeInput holds input for DescribeConnectionType.
type describeConnectionTypeInput struct {
	ConnectionType string `json:"ConnectionType"`
}

// describeConnectionTypeOutput holds the result for DescribeConnectionType.
type describeConnectionTypeOutput struct {
	ConnectionType string   `json:"ConnectionType"`
	Description    string   `json:"Description,omitempty"`
	Category       string   `json:"Category,omitempty"`
	Capabilities   []string `json:"Capabilities,omitempty"`
}

func (h *Handler) handleDescribeConnectionType(
	_ context.Context,
	in *describeConnectionTypeInput,
) (*describeConnectionTypeOutput, error) {
	if in.ConnectionType == "" {
		return nil, fmt.Errorf("%w: ConnectionType is required", ErrValidation)
	}

	info, err := h.Backend.DescribeConnectionType(in.ConnectionType)
	if err != nil {
		return nil, err
	}

	return &describeConnectionTypeOutput{
		ConnectionType: info.ConnectionType,
		Description:    info.Description,
		Category:       info.Category,
		Capabilities:   info.Capabilities,
	}, nil
}

// listConnectionTypesInput holds input for ListConnectionTypes.
type listConnectionTypesInput struct{}

// connectionTypeBrief is the per-type summary returned by ListConnectionTypes.
type connectionTypeBrief struct {
	ConnectionType string   `json:"ConnectionType"`
	Description    string   `json:"Description,omitempty"`
	Category       string   `json:"Category,omitempty"`
	Capabilities   []string `json:"Capabilities,omitempty"`
}

// listConnectionTypesOutput holds the result for ListConnectionTypes.
type listConnectionTypesOutput struct {
	ConnectionTypes []connectionTypeBrief `json:"ConnectionTypes"`
}

func (h *Handler) handleListConnectionTypes(
	_ context.Context,
	_ *listConnectionTypesInput,
) (*listConnectionTypesOutput, error) {
	infos := h.Backend.ListConnectionTypes()

	out := make([]connectionTypeBrief, 0, len(infos))
	for _, info := range infos {
		out = append(out, connectionTypeBrief{
			ConnectionType: info.ConnectionType,
			Description:    info.Description,
			Category:       info.Category,
			Capabilities:   info.Capabilities,
		})
	}

	return &listConnectionTypesOutput{ConnectionTypes: out}, nil
}

// registerConnectionTypeInput holds input for RegisterConnectionType.
type registerConnectionTypeInput struct {
	ConnectionType string `json:"ConnectionType"`
	Description    string `json:"Description,omitempty"`
}

// registerConnectionTypeOutput holds the result for RegisterConnectionType.
type registerConnectionTypeOutput struct {
	ConnectionType string `json:"ConnectionType"`
	Status         string `json:"Status"`
}

func (h *Handler) handleRegisterConnectionType(
	_ context.Context,
	in *registerConnectionTypeInput,
) (*registerConnectionTypeOutput, error) {
	if in.ConnectionType == "" {
		return nil, fmt.Errorf("%w: ConnectionType is required", ErrValidation)
	}

	info, err := h.Backend.RegisterConnectionType(in.ConnectionType, in.Description)
	if err != nil {
		return nil, err
	}

	return &registerConnectionTypeOutput{ConnectionType: info.ConnectionType, Status: stateReady}, nil
}
