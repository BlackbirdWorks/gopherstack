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

// connectionCapabilities mirrors the real *types.Capabilities shape
// (glue@v1.152.0 types/types.go:872-890): SupportedAuthenticationTypes/
// SupportedComputeEnvironments/SupportedDataOperations are all "This member
// is required" there. This backend hand-rolls JSON wire structs rather than
// importing SDK types at runtime (repo convention), so the shape is
// reproduced here instead of referencing types.Capabilities directly.
//
// Previously both DescribeConnectionType and ListConnectionTypes emitted
// Capabilities as a bare []string of "READ"/"WRITE" -- a real
// aws-sdk-go-v2 client's deserializer rejects the whole response body on
// that mismatch (it expects an object, not an array), so DescribeConnectionType
// and ListConnectionTypes were entirely undecodable by a real client. This
// backend's existing per-type capability data (rwCaps/readCaps, "READ"/
// "WRITE") is real state that maps exactly onto SupportedDataOperations
// (confirmed: types.DataOperation's only two enum values are literally
// "READ"/"WRITE", types/enums.go:979-986) -- it is threaded through here,
// not discarded. SupportedAuthenticationTypes/SupportedComputeEnvironments
// have no corresponding backing state anywhere in this backend (no
// per-connector auth-type/compute-environment tracking exists), so they are
// modeled as real, present, empty slices rather than fabricated -- see
// PARITY.md.
type connectionCapabilities struct {
	SupportedAuthenticationTypes []string `json:"SupportedAuthenticationTypes"`
	SupportedComputeEnvironments []string `json:"SupportedComputeEnvironments"`
	SupportedDataOperations      []string `json:"SupportedDataOperations"`
}

// toConnectionCapabilities builds the wire Capabilities object from this
// backend's stored DataOperations list. Capabilities itself is NOT a
// required member on DescribeConnectionTypeOutput/ConnectionTypeBrief (no
// "This member is required" on either field's doc comment), so a type with
// no tracked data operations (dataOps nil -- e.g. the NETWORK/MARKETPLACE/
// CUSTOM built-in categories, see builtInConnectionTypes) omits the whole
// object rather than emitting an empty-but-present one.
func toConnectionCapabilities(dataOps []string) *connectionCapabilities {
	if dataOps == nil {
		return nil
	}

	return &connectionCapabilities{
		SupportedAuthenticationTypes: []string{},
		SupportedComputeEnvironments: []string{},
		SupportedDataOperations:      dataOps,
	}
}

// describeConnectionTypeOutput holds the result for DescribeConnectionType.
// RestConfiguration is the one field RegisterConnectionType's input and this
// op's real output share verbatim (glue@v1.152.0
// api_op_DescribeConnectionType.go:76-79); ConnectionProperties and
// ConnectorAuthenticationConfiguration are NOT echoed here even though
// RegisterConnectionType requires them -- see RegisterConnectionTypeSpec's
// doc comment for why. There is no Category field anywhere on the real
// DescribeConnectionTypeOutput (confirmed against api_op_DescribeConnectionType.go)
// -- previously fabricated here, now removed.
type describeConnectionTypeOutput struct {
	RestConfiguration map[string]any          `json:"RestConfiguration,omitempty"`
	Capabilities      *connectionCapabilities `json:"Capabilities,omitempty"`
	ConnectionType    string                  `json:"ConnectionType"`
	Description       string                  `json:"Description,omitempty"`
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
		Capabilities:      toConnectionCapabilities(info.Capabilities),
	}, nil
}

// defaultListConnectionTypesLimit is used when ListConnectionTypesInput.MaxResults is unset.
const defaultListConnectionTypesLimit = 100

// listConnectionTypesInput holds input for ListConnectionTypes.
type listConnectionTypesInput struct {
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int32  `json:"MaxResults,omitempty"`
}

// connectionTypeBrief is the per-type summary returned by ListConnectionTypes.
// The real types.ConnectionTypeBrief (glue@v1.152.0 types/types.go:2533-2564)
// carries Categories as a []string (plural), not a single Category string --
// a second, distinct wire-shape bug found while fixing Capabilities below
// (this backend's ConnectionTypeInfo only ever models one category per type,
// so it is echoed as the one-element list that shape implies, not fabricated
// into several). DisplayName/LogoUrl/Vendor/ConnectionTypeVariants are also
// real ConnectionTypeBrief members with no backing state anywhere in this
// backend -- deliberately left absent rather than invented; see PARITY.md.
type connectionTypeBrief struct {
	Capabilities   *connectionCapabilities `json:"Capabilities,omitempty"`
	ConnectionType string                  `json:"ConnectionType"`
	Description    string                  `json:"Description,omitempty"`
	Categories     []string                `json:"Categories,omitempty"`
}

// listConnectionTypesOutput holds the result for ListConnectionTypes.
type listConnectionTypesOutput struct {
	NextToken       string                `json:"NextToken,omitempty"`
	ConnectionTypes []connectionTypeBrief `json:"ConnectionTypes"`
}

func (h *Handler) handleListConnectionTypes(
	_ context.Context,
	in *listConnectionTypesInput,
) (*listConnectionTypesOutput, error) {
	infos := h.Backend.ListConnectionTypes()

	limit := int(in.MaxResults)
	if limit <= 0 {
		limit = defaultListConnectionTypesLimit
	}

	page, next := paginateSlice(infos, in.NextToken, limit)

	out := make([]connectionTypeBrief, 0, len(page))
	for _, info := range page {
		var categories []string
		if info.Category != "" {
			categories = []string{info.Category}
		}

		out = append(out, connectionTypeBrief{
			ConnectionType: info.ConnectionType,
			Description:    info.Description,
			Categories:     categories,
			Capabilities:   toConnectionCapabilities(info.Capabilities),
		})
	}

	return &listConnectionTypesOutput{ConnectionTypes: out, NextToken: next}, nil
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
