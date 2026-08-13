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
// RestConfiguration is the one field RegisterConnectionType's input and this
// op's real output share verbatim (glue@v1.152.0
// api_op_DescribeConnectionType.go:76-79); ConnectionProperties and
// ConnectorAuthenticationConfiguration are NOT echoed here even though
// RegisterConnectionType requires them -- see RegisterConnectionTypeSpec's
// doc comment for why. Category/Capabilities-as-[]string predate this fix
// and are already a known mismatch against the real ConnectionType/
// *types.Capabilities shapes -- not touched here (out of scope for the
// required-member fix; tracked in PARITY.md).
type describeConnectionTypeOutput struct {
	RestConfiguration map[string]any `json:"RestConfiguration,omitempty"`
	ConnectionType    string         `json:"ConnectionType"`
	Description       string         `json:"Description,omitempty"`
	Category          string         `json:"Category,omitempty"`
	Capabilities      []string       `json:"Capabilities,omitempty"`
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
		RestConfiguration: info.RestConfiguration,
		ConnectionType:    info.ConnectionType,
		Description:       info.Description,
		Category:          info.Category,
		Capabilities:      info.Capabilities,
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
// ConnectionProperties/ConnectorAuthenticationConfiguration/RestConfiguration
// are decoded as raw documents rather than fully typed structs -- see
// RegisterConnectionTypeSpec's doc comment for why neither has an echo
// target on the real API surface this backend exposes.
type registerConnectionTypeInput struct {
	ConnectionProperties                 map[string]any `json:"ConnectionProperties"`
	ConnectorAuthenticationConfiguration map[string]any `json:"ConnectorAuthenticationConfiguration"`
	RestConfiguration                    map[string]any `json:"RestConfiguration"`
	ConnectionType                       string         `json:"ConnectionType"`
	Description                          string         `json:"Description,omitempty"`
	IntegrationType                      string         `json:"IntegrationType"`
}

// registerConnectionTypeOutput holds the result for RegisterConnectionType.
// The real RegisterConnectionTypeOutput carries only ConnectionTypeArn
// (glue@v1.152.0 api_op_RegisterConnectionType.go:79-84).
type registerConnectionTypeOutput struct {
	ConnectionTypeArn string `json:"ConnectionTypeArn"`
}

func (h *Handler) handleRegisterConnectionType(
	_ context.Context,
	in *registerConnectionTypeInput,
) (*registerConnectionTypeOutput, error) {
	if in.ConnectionType == "" {
		return nil, fmt.Errorf("%w: ConnectionType is required", ErrValidation)
	}

	info, err := h.Backend.RegisterConnectionType(in.ConnectionType, in.Description, RegisterConnectionTypeSpec{
		IntegrationType:                      in.IntegrationType,
		ConnectionProperties:                 in.ConnectionProperties,
		ConnectorAuthenticationConfiguration: in.ConnectorAuthenticationConfiguration,
		RestConfiguration:                    in.RestConfiguration,
	})
	if err != nil {
		return nil, err
	}

	return &registerConnectionTypeOutput{ConnectionTypeArn: info.ConnectionTypeArn}, nil
}
