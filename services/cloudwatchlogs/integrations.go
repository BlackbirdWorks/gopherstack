package cloudwatchlogs

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// AssociateSourceToS3TableIntegration associates a data source with an S3 table integration.
// Returns a unique identifier for the association.
func (b *InMemoryBackend) AssociateSourceToS3TableIntegration(
	integrationArn, dataSourceName, dataSourceType string,
) (string, error) {
	if integrationArn == "" {
		return "", fmt.Errorf("%w: integrationArn is required", ErrValidation)
	}

	id := uuid.New().String()

	b.mu.Lock("AssociateSourceToS3TableIntegration")
	defer b.mu.Unlock()

	b.s3TableIntegrations.Put(&s3TableIntegrationEntry{
		ID:               id,
		IntegrationArn:   integrationArn,
		DataSourceName:   dataSourceName,
		DataSourceType:   dataSourceType,
		CreatedTimeStamp: time.Now().UnixMilli(),
	})

	return id, nil
}

// AddS3TableIntegrationSourceInternal seeds an S3 table integration source
// association directly into the store for testing, with a caller-controlled
// createdTimeStamp -- AssociateSourceToS3TableIntegration always stamps
// time.Now(), so tests that need two entries with an identical timestamp
// (to exercise ListSourcesForS3TableIntegration's sort) must go through here.
func (b *InMemoryBackend) AddS3TableIntegrationSourceInternal(
	id, integrationArn, dataSourceName, dataSourceType string, createdTimeStamp int64,
) {
	b.mu.Lock("AddS3TableIntegrationSourceInternal")
	defer b.mu.Unlock()

	b.s3TableIntegrations.Put(&s3TableIntegrationEntry{
		ID:               id,
		IntegrationArn:   integrationArn,
		DataSourceName:   dataSourceName,
		DataSourceType:   dataSourceType,
		CreatedTimeStamp: createdTimeStamp,
	})
}

// DisassociateSourceFromS3TableIntegration removes a source association by
// its identifier (the ID AssociateSourceToS3TableIntegration returned).
func (b *InMemoryBackend) DisassociateSourceFromS3TableIntegration(identifier string) error {
	if identifier == "" {
		return fmt.Errorf("%w: identifier is required", ErrValidation)
	}

	b.mu.Lock("DisassociateSourceFromS3TableIntegration")
	defer b.mu.Unlock()

	if !b.s3TableIntegrations.Delete(identifier) {
		return fmt.Errorf("%w: S3 table integration source %q not found", ErrS3TableIntegrationNotFound, identifier)
	}

	return nil
}

// s3TableIntegrationSourceLimit is ListSourcesForS3TableIntegrationInput's
// documented "Valid range is 1 to 100" (api_op_ListSourcesForS3TableIntegration.go:39).
const s3TableIntegrationSourceLimit = 100

// ListSourcesForS3TableIntegration returns data source associations for the
// given integration ARN, oldest first, with maxResults/nextToken pagination
// (api_op_ListSourcesForS3TableIntegration.go).
func (b *InMemoryBackend) ListSourcesForS3TableIntegration(
	integrationArn, nextToken string, maxResults int,
) ([]s3TableIntegrationEntry, string, error) {
	if integrationArn == "" {
		return nil, "", fmt.Errorf("%w: integrationArn is required", ErrValidation)
	}

	b.mu.RLock("ListSourcesForS3TableIntegration")
	defer b.mu.RUnlock()

	var all []s3TableIntegrationEntry

	for _, e := range b.s3TableIntegrations.All() {
		if e.IntegrationArn == integrationArn {
			all = append(all, *e)
		}
	}

	sort.Slice(all, func(i, j int) bool {
		if all[i].CreatedTimeStamp != all[j].CreatedTimeStamp {
			return all[i].CreatedTimeStamp < all[j].CreatedTimeStamp
		}

		return all[i].ID < all[j].ID
	})

	if maxResults <= 0 || maxResults > s3TableIntegrationSourceLimit {
		maxResults = defaultDescribeLimit
	}

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []s3TableIntegrationEntry{}, "", nil
	}

	end := startIdx + maxResults

	var outToken string
	if end < len(all) {
		outToken = encodeNextToken(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken, nil
}

// PutIntegration creates or updates an integration. resourceConfig is
// required (PutIntegrationInput.ResourceConfig, verified against
// validateOpPutIntegrationInput, validators.go); its own required members
// (DataSourceRoleArn/DashboardViewerPrincipals/RetentionDays) are validated
// by the caller (handlePutIntegration) before this is invoked, matching
// nested-required validation elsewhere in this package.
func (b *InMemoryBackend) PutIntegration(
	name, integrationType string, resourceConfig *OpenSearchResourceConfig,
) (*CWLIntegration, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: integrationName is required", ErrValidation)
	}

	if resourceConfig == nil {
		return nil, fmt.Errorf("%w: resourceConfig is required", ErrValidation)
	}

	b.mu.Lock("PutIntegration")
	defer b.mu.Unlock()

	ig := CWLIntegration{
		Name:                     name,
		Type:                     integrationType,
		Status:                   completenessStatusActive,
		CreatedAt:                time.Now().UTC(),
		OpenSearchResourceConfig: resourceConfig,
	}
	stored := ig
	b.integrations.Put(&stored)

	return &ig, nil
}

// GetIntegration returns an integration by name.
func (b *InMemoryBackend) GetIntegration(name string) (*CWLIntegration, error) {
	b.mu.RLock("GetIntegration")
	defer b.mu.RUnlock()

	ig, ok := b.integrations.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: integration %q not found", ErrIntegrationNotFound, name)
	}
	cp := *ig

	return &cp, nil
}

// ListIntegrations returns all integrations sorted by name.
func (b *InMemoryBackend) ListIntegrations() []CWLIntegration {
	b.mu.RLock("ListIntegrations")
	defer b.mu.RUnlock()

	out := make([]CWLIntegration, 0, b.integrations.Len())
	for _, ig := range b.integrations.All() {
		out = append(out, *ig)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

// DeleteIntegration removes an integration by name.
func (b *InMemoryBackend) DeleteIntegration(name string) error {
	b.mu.Lock("DeleteIntegration")
	defer b.mu.Unlock()

	if !b.integrations.Delete(name) {
		return fmt.Errorf("%w: integration %q not found", ErrIntegrationNotFound, name)
	}

	return nil
}
