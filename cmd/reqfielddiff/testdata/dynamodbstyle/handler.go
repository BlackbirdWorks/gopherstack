// Package dynamodbstyle is a fixture reproducing dynamodb's real
// handleOp[WireIn,...] + models subpackage shape for
// TestResolveOp_CrossPackageGenericCallback. It is never built or
// type-checked -- only parsed -- so signatures need only be syntactically
// valid, not sound.
package dynamodbstyle

import (
	"context"

	"github.com/blackbirdworks/gopherstack/services/dynamodbstyle/models"
)

type Handler struct {
	Backend Backend
}

type Backend interface {
	CreateTable(ctx context.Context, input *models.SDKCreateTableInput) (*models.SDKCreateTableOutput, error)
}

// handleOp mirrors dynamodb's real generic dispatch wrapper: its own
// toSDK parameter's LAST parameter type (*WireIn) is what this scan must
// resolve to find the request struct.
func handleOp[WireIn any, SDKIn any, SDKOut any, WireOut any](
	ctx context.Context,
	action string,
	body []byte,
	toSDK func(*WireIn) *SDKIn,
	doOp func(context.Context, *SDKIn) (*SDKOut, error),
	fromSDK func(*SDKOut) *WireOut,
) (any, error) {
	return nil, nil
}

func (h *Handler) dispatchTableOps(ctx context.Context, action string, body []byte) (any, error) {
	switch action {
	case "CreateTable":
		return handleOp(
			ctx, action, body,
			models.ToSDKCreateTableInput, h.Backend.CreateTable, models.FromSDKCreateTableOutput,
		)
	}

	return nil, nil
}
