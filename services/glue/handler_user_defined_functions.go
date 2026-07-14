package glue

import (
	"context"
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
type getUserDefinedFunctionsInput struct {
	DatabaseName string `json:"DatabaseName,omitempty"`
}

// getUserDefinedFunctionsOutput holds the result for GetUserDefinedFunctions.
type getUserDefinedFunctionsOutput struct {
	UserDefinedFunctions []*UserDefinedFunction `json:"UserDefinedFunctions"`
}

func (h *Handler) handleGetUserDefinedFunctions(
	_ context.Context,
	in *getUserDefinedFunctionsInput,
) (*getUserDefinedFunctionsOutput, error) {
	udfs := h.Backend.GetUserDefinedFunctions(in.DatabaseName)
	if udfs == nil {
		udfs = []*UserDefinedFunction{}
	}

	return &getUserDefinedFunctionsOutput{UserDefinedFunctions: udfs}, nil
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
