package glue

import (
	"context"
	"fmt"
)

// createUserDefinedFunctionInput holds input for CreateUserDefinedFunction.
// CatalogId is a top-level member (glue@v1.157.0
// api_op_CreateUserDefinedFunction.go), not part of FunctionInput.
type createUserDefinedFunctionInput struct {
	Tags          map[string]string   `json:"Tags,omitempty"`
	DatabaseName  string              `json:"DatabaseName"`
	FunctionInput UserDefinedFunction `json:"FunctionInput"`
	CatalogID     string              `json:"CatalogId,omitempty"`
}

func (h *Handler) handleCreateUserDefinedFunction(
	_ context.Context,
	in *createUserDefinedFunctionInput,
) (*emptyOutput, error) {
	fnInput := in.FunctionInput
	fnInput.CatalogID = in.CatalogID

	_, err := h.Backend.CreateUserDefinedFunction(in.DatabaseName, fnInput, in.Tags)

	return &emptyOutput{}, err
}

// deleteUserDefinedFunctionInput holds input for DeleteUserDefinedFunction.
type deleteUserDefinedFunctionInput struct {
	DatabaseName string `json:"DatabaseName"`
	FunctionName string `json:"FunctionName"`
	CatalogID    string `json:"CatalogId,omitempty"`
}

func (h *Handler) handleDeleteUserDefinedFunction(
	_ context.Context,
	in *deleteUserDefinedFunctionInput,
) (*emptyOutput, error) {
	if in.CatalogID != "" {
		existing, err := h.Backend.GetUserDefinedFunction(in.DatabaseName, in.FunctionName)
		if err != nil {
			return nil, err
		}

		if catalogIDMismatch(in.CatalogID, existing.CatalogID) {
			return nil, ErrNotFound
		}
	}

	return &emptyOutput{}, h.Backend.DeleteUserDefinedFunction(in.DatabaseName, in.FunctionName)
}

// getUserDefinedFunctionInput holds input for GetUserDefinedFunction.
type getUserDefinedFunctionInput struct {
	DatabaseName string `json:"DatabaseName"`
	FunctionName string `json:"FunctionName"`
	CatalogID    string `json:"CatalogId,omitempty"`
}

// getUserDefinedFunctionOutput holds the result for GetUserDefinedFunction.
type getUserDefinedFunctionOutput struct {
	UserDefinedFunction *UserDefinedFunction `json:"UserDefinedFunction"`
}

func (h *Handler) handleGetUserDefinedFunction(
	_ context.Context,
	in *getUserDefinedFunctionInput,
) (*getUserDefinedFunctionOutput, error) {
	u, err := h.Backend.GetUserDefinedFunction(in.DatabaseName, in.FunctionName)
	if err != nil {
		return nil, err
	}

	if catalogIDMismatch(in.CatalogID, u.CatalogID) {
		return nil, ErrNotFound
	}

	return &getUserDefinedFunctionOutput{UserDefinedFunction: u}, nil
}

// getUserDefinedFunctionsInput holds input for GetUserDefinedFunctions.
// Pattern is marked "This member is required" by the pinned SDK
// (api_op_GetUserDefinedFunctions.go, validators.go:validateOpGetUserDefinedFunctionsInput)
// despite its own doc comment calling it "optional" -- the client-side
// validator enforces it regardless, so a real SDK call never omits it.
type getUserDefinedFunctionsInput struct {
	DatabaseName string `json:"DatabaseName,omitempty"`
	Pattern      string `json:"Pattern"`
	CatalogID    string `json:"CatalogId,omitempty"`
}

// getUserDefinedFunctionsOutput holds the result for GetUserDefinedFunctions.
type getUserDefinedFunctionsOutput struct {
	UserDefinedFunctions []*UserDefinedFunction `json:"UserDefinedFunctions"`
}

func (h *Handler) handleGetUserDefinedFunctions(
	_ context.Context,
	in *getUserDefinedFunctionsInput,
) (*getUserDefinedFunctionsOutput, error) {
	if in.Pattern == "" {
		return nil, fmt.Errorf("%w: Pattern is required", ErrValidation)
	}

	re, err := tableNameRegexp(in.Pattern)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid Pattern: %w", ErrValidation, err)
	}

	udfs := h.Backend.GetUserDefinedFunctions(in.DatabaseName)

	filtered := make([]*UserDefinedFunction, 0, len(udfs))
	for _, u := range udfs {
		if re.MatchString(u.FunctionName) && (in.CatalogID == "" || u.CatalogID == in.CatalogID) {
			filtered = append(filtered, u)
		}
	}

	return &getUserDefinedFunctionsOutput{UserDefinedFunctions: filtered}, nil
}

// updateUserDefinedFunctionInput holds input for UpdateUserDefinedFunction.
type updateUserDefinedFunctionInput struct {
	DatabaseName  string              `json:"DatabaseName"`
	FunctionName  string              `json:"FunctionName"`
	FunctionInput UserDefinedFunction `json:"FunctionInput"`
	CatalogID     string              `json:"CatalogId,omitempty"`
}

func (h *Handler) handleUpdateUserDefinedFunction(
	_ context.Context,
	in *updateUserDefinedFunctionInput,
) (*emptyOutput, error) {
	if in.CatalogID != "" {
		existing, err := h.Backend.GetUserDefinedFunction(in.DatabaseName, in.FunctionName)
		if err != nil {
			return nil, err
		}

		if catalogIDMismatch(in.CatalogID, existing.CatalogID) {
			return nil, ErrNotFound
		}
	}

	return &emptyOutput{}, h.Backend.UpdateUserDefinedFunction(
		in.DatabaseName,
		in.FunctionName,
		in.FunctionInput,
	)
}
