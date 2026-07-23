package kinesisanalyticsv2

import (
	"context"
	"strconv"
	"time"
)

// DescribeApplicationOperation returns a single operation by ID.
func (b *InMemoryBackend) DescribeApplicationOperation(
	ctx context.Context,
	name, operationID string,
) (*ApplicationOperation, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("DescribeApplicationOperation")
	defer b.mu.RUnlock()

	if !b.applications.Has(applicationKey(region, name)) {
		return nil, ErrNotFound
	}

	for _, op := range b.operations[region][name] {
		if op.OperationID == operationID {
			cp := *op

			return &cp, nil
		}
	}

	return nil, ErrNotFound
}

// ListApplicationOperations returns operations for an application with optional pagination.
func (b *InMemoryBackend) ListApplicationOperations(
	ctx context.Context,
	name, nextToken string,
) ([]*ApplicationOperation, string, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListApplicationOperations")
	defer b.mu.RUnlock()

	if !b.applications.Has(applicationKey(region, name)) {
		return nil, "", ErrNotFound
	}

	ops := b.operations[region][name]
	out := make([]*ApplicationOperation, len(ops))
	copy(out, ops)

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(out) {
		return []*ApplicationOperation{}, "", nil
	}
	end := startIdx + kav2DefaultPageSize
	var outToken string

	if end < len(out) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(out)
	}

	return out[startIdx:end], outToken, nil
}

// DescribeApplicationVersion returns the application state at a specific version ID.
func (b *InMemoryBackend) DescribeApplicationVersion(
	ctx context.Context,
	name string,
	versionID int64,
) (*Application, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("DescribeApplicationVersion")
	defer b.mu.RUnlock()

	if !b.applications.Has(applicationKey(region, name)) {
		return nil, ErrNotFound
	}

	for _, v := range b.versions[region][name] {
		if v.ApplicationVersionID == versionID {
			return appCopy(v), nil
		}
	}

	return nil, ErrNotFound
}

// ListApplicationVersions returns version summaries for an application.
func (b *InMemoryBackend) ListApplicationVersions(
	ctx context.Context,
	name, nextToken string,
) ([]*ApplicationVersionSummary, string, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListApplicationVersions")
	defer b.mu.RUnlock()

	if !b.applications.Has(applicationKey(region, name)) {
		return nil, "", ErrNotFound
	}

	vers := b.versions[region][name]
	summaries := make([]*ApplicationVersionSummary, 0, len(vers))

	for _, v := range vers {
		summaries = append(summaries, &ApplicationVersionSummary{
			ApplicationVersionID: v.ApplicationVersionID,
			ApplicationStatus:    v.ApplicationStatus,
		})
	}

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(summaries) {
		return []*ApplicationVersionSummary{}, "", nil
	}
	end := startIdx + kav2DefaultPageSize
	var outToken string

	if end < len(summaries) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(summaries)
	}

	return summaries[startIdx:end], outToken, nil
}

// RollbackApplication rolls back an application to its previous version,
// returning the OperationID of the recorded RollbackApplication operation
// (see recordOperation).
func (b *InMemoryBackend) RollbackApplication(
	ctx context.Context,
	name string,
	currentVersionID int64,
) (*Application, string, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("RollbackApplication")
	defer b.mu.Unlock()

	app, ok := b.findApplication(region, name)
	if !ok {
		return nil, "", ErrNotFound
	}

	if currentVersionID > 0 && app.ApplicationVersionID != currentVersionID {
		return nil, "", ErrConcurrentModification
	}

	const minVersionsForRollback = 2
	vers := b.versions[region][name]
	if len(vers) < minVersionsForRollback {
		return nil, "", ErrValidation
	}

	// Roll back to the second-to-last stored version.
	rolledFrom := app.ApplicationVersionID
	rolledTo := vers[len(vers)-2].ApplicationVersionID
	prev := appCopy(vers[len(vers)-2])
	prev.ApplicationVersionID = app.ApplicationVersionID + 1
	prev.ApplicationVersionCreateTimestamp = time.Now().UTC()
	prev.LastUpdateTimestamp = prev.ApplicationVersionCreateTimestamp
	prev.ApplicationVersionUpdatedFrom = &rolledFrom
	prev.ApplicationVersionRolledBackFrom = &rolledFrom
	prev.ApplicationVersionRolledBackTo = &rolledTo
	b.applications.Put(prev)
	b.versions[region][name] = append(b.versions[region][name], appCopy(prev))

	opID := b.recordOperation(region, name, "RollbackApplication")

	return appCopy(prev), opID, nil
}
