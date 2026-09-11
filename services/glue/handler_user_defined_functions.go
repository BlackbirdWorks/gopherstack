package glue

import (
	"context"
	"fmt"
)

// createUserDefinedFunctionInput holds input for CreateUserDefinedFunction.
type createUserDefinedFunctionInput struct {
	Tags          map[string]string   `json:"Tags,omitempty"`
	DatabaseName  string              `json:"DatabaseName"`
	FunctionInput UserDefinedFunction `json:"FunctionInput"`
}

func (h *Handler) handleCreateUserDefinedFunction(
	_ context.Context,
	in *createUserDefinedFunctionInput,
) (*emptyOutput, error) {
	_, err := h.Backend.CreateUserDefinedFunction(in.DatabaseName, in.FunctionInput, in.Tags)

	return &emptyOutput{}, err
}

// deleteUserDefinedFunctionInput holds input for DeleteUserDefinedFunction.
type deleteUserDefinedFunctionInput struct {
	DatabaseName string `json:"DatabaseName"`
	FunctionName string `json:"FunctionName"`
}

func (h *Handler) handleDeleteUserDefinedFunction(
	_ context.Context,
	in *deleteUserDefinedFunctionInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteUserDefinedFunction(in.DatabaseName, in.FunctionName)
}

// getUserDefinedFunctionInput holds input for GetUserDefinedFunction.
type getUserDefinedFunctionInput struct {
	DatabaseName string `json:"DatabaseName"`
	FunctionName string `json:"FunctionName"`
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
		if re.MatchString(u.FunctionName) {
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
}

func (h *Handler) handleUpdateUserDefinedFunction(
	_ context.Context,
	in *updateUserDefinedFunctionInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.UpdateUserDefinedFunction(
		in.DatabaseName,
		in.FunctionName,
		in.FunctionInput,
	)
}
