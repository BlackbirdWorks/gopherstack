package dms

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// resolveProjectARN returns the project ARN for a name-or-ARN identifier.
// Callers must hold b.mu.
func (b *InMemoryBackend) resolveProjectARN(region, identifier string) string {
	if strings.HasPrefix(identifier, "arn:") {
		return identifier
	}

	if mp, ok := b.migrationProjects.Get(regionKey(region, identifier)); ok {
		return mp.MigrationProjectArn
	}

	return identifier
}

// cancelMetadataModelRequest marks a tracked request as cancelling and
// returns a copy of its current state. Callers must hold b.mu.
//
// If no request with this identifier was ever started, AWS's Cancel
// operations still succeed (fire-and-forget cancellation semantics): a
// minimal envelope echoing the caller's identifiers is returned rather than
// a ResourceNotFoundFault.
func (b *InMemoryBackend) cancelMetadataModelRequest(
	region, projectARN, requestIdentifier string,
) *MetadataModelRequest {
	if req, ok := b.metadataModelRequests.Get(metadataModelRequestKey(region, projectARN, requestIdentifier)); ok {
		req.Status = statusCancelling
		cp := *req

		return &cp
	}

	return &MetadataModelRequest{
		RequestIdentifier:          requestIdentifier,
		MigrationProjectIdentifier: projectARN,
		Status:                     statusCancelling,
		Region:                     region,
	}
}

// CancelMetadataModelConversion cancels a pending metadata model conversion task.
func (b *InMemoryBackend) CancelMetadataModelConversion(
	ctx context.Context,
	migrationProjectIdentifier, requestIdentifier string,
) (*MetadataModelRequest, error) {
	if migrationProjectIdentifier == "" {
		return nil, fmt.Errorf("%w: MigrationProjectIdentifier is required", ErrValidation)
	}

	if requestIdentifier == "" {
		return nil, fmt.Errorf("%w: RequestIdentifier is required", ErrValidation)
	}

	b.mu.Lock("CancelMetadataModelConversion")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	projectARN := b.resolveProjectARN(region, migrationProjectIdentifier)

	return b.cancelMetadataModelRequest(region, projectARN, requestIdentifier), nil
}

// CancelMetadataModelCreation cancels a pending metadata model creation task.
func (b *InMemoryBackend) CancelMetadataModelCreation(
	ctx context.Context,
	migrationProjectIdentifier, requestIdentifier string,
) (*MetadataModelRequest, error) {
	if migrationProjectIdentifier == "" {
		return nil, fmt.Errorf("%w: MigrationProjectIdentifier is required", ErrValidation)
	}

	if requestIdentifier == "" {
		return nil, fmt.Errorf("%w: RequestIdentifier is required", ErrValidation)
	}

	b.mu.Lock("CancelMetadataModelCreation")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	projectARN := b.resolveProjectARN(region, migrationProjectIdentifier)

	return b.cancelMetadataModelRequest(region, projectARN, requestIdentifier), nil
}

// StartMetadataModelRequest persists a metadata model operation request and returns its ID.
func (b *InMemoryBackend) StartMetadataModelRequest(
	ctx context.Context,
	projectIdentifier, reqType, selectionRules string,
) (string, error) {
	b.mu.Lock("StartMetadataModelRequest")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	projectARN := b.resolveProjectARN(region, projectIdentifier)
	reqID := uuid.NewString()
	b.metadataModelRequests.Put(&MetadataModelRequest{
		RequestIdentifier:          reqID,
		MigrationProjectIdentifier: projectARN,
		Status:                     "running",
		RequestType:                reqType,
		SelectionRules:             selectionRules,
		Region:                     region,
	})

	return reqID, nil
}

// ListMetadataModelRequests returns all requests of a given type for a migration project.
func (b *InMemoryBackend) ListMetadataModelRequests(
	ctx context.Context,
	projectIdentifier, reqType string,
) ([]*MetadataModelRequest, error) {
	b.mu.RLock("ListMetadataModelRequests")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	projectARN := b.resolveProjectARN(region, projectIdentifier)
	items := b.metadataModelRequestsByProject.Get(metadataModelRequestProjectKey(region, projectARN))
	result := make([]*MetadataModelRequest, 0)

	for _, req := range items {
		if req.RequestType == reqType {
			cp := *req
			result = append(result, &cp)
		}
	}

	return result, nil
}
