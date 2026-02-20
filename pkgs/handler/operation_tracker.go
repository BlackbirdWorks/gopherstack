package handler

import "context"

// OperationKey is a type-safe context key for storing operation metadata.
type OperationKey struct{}

// operationData stores operation-related metadata in context.
type operationData struct {
	operation string
	resource  string
}

var operationCtxKey OperationKey

// withOperation creates a new context with operation metadata.
func withOperation(ctx context.Context, operation, resource string) context.Context {
	return context.WithValue(ctx, operationCtxKey, &operationData{
		operation: operation,
		resource:  resource,
	})
}

// GetOperation retrieves the operation name from context, or "Unknown" if not set.
func GetOperation(ctx context.Context) string {
	if data, ok := ctx.Value(operationCtxKey).(*operationData); ok && data != nil {
		return data.operation
	}

	return "Unknown"
}

// SetOperation returns a new context with the operation name updated.
// This follows the idiomatic context pattern - immutable values.
func SetOperation(ctx context.Context, operation string) context.Context {
	if data, ok := ctx.Value(operationCtxKey).(*operationData); ok && data != nil {
		return context.WithValue(ctx, operationCtxKey, &operationData{
			operation: operation,
			resource:  data.resource,
		})
	}

	return ctx
}

// GetResource retrieves the resource identifier from context, or "" if not set.
func GetResource(ctx context.Context) string {
	if data, ok := ctx.Value(operationCtxKey).(*operationData); ok && data != nil {
		return data.resource
	}

	return ""
}

// NewOperationContext creates a new context with operation metadata.
func NewOperationContext(ctx context.Context, operation, resource string) context.Context {
	return withOperation(ctx, operation, resource)
}
