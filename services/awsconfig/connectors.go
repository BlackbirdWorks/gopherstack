package awsconfig

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

// connectorNamePrefix names every connector this backend creates. Real AWS
// Config does not document its exact connector naming scheme (PutConnector's
// request has no caller-supplied Name field -- only ConnectorConfiguration
// and Tags), so this is a best-effort deterministic-enough name, mirroring
// this package's existing serviceLinkedRecorderName caveat for the same
// reason.
const connectorNamePrefix = "connector-"

// connectorArn builds the ARN for a connector owned by this backend, matching
// this file's existing recorderArn/aggregator-ARN convention (hardcoded
// "arn:aws:config:..." rather than pkgs/arn, for consistency with the rest of
// this package).
func (b *InMemoryBackend) connectorArn(name string) string {
	return fmt.Sprintf("arn:aws:config:%s:%s:connector/%s", b.region, b.accountID, name)
}

// connectorConfigurationsMatch reports whether a and b describe the same
// third-party provider connection, used by PutConnector to enforce the real
// API's declared ConflictException ("you cannot create a connector because a
// connector already exists for the specified connector configuration" --
// verified against the AWS Config API reference's PutConnector error list).
// Only Azure is modeled since it's the only provider AWS Config currently
// documents (types.Provider's sole enum value).
func connectorConfigurationsMatch(a, b *ConnectorConfiguration) bool {
	if a == nil || b == nil || a.Azure == nil || b.Azure == nil {
		return false
	}

	return a.Azure.ClientIdentifier == b.Azure.ClientIdentifier &&
		a.Azure.TenantIdentifier == b.Azure.TenantIdentifier
}

// PutConnector creates a connector to a third-party cloud service provider.
// Connectors cannot be updated once created (verified against the real
// PutConnector doc comment: "Connectors cannot be updated -- To update the
// connector configuration, you must delete all associated configuration
// recorders, delete the connector, and recreate it with the updated
// configuration"), so unlike PutServiceLinkedConfigurationRecorder this is
// NOT an upsert: a repeat call with a ConnectorConfiguration matching an
// existing connector errors ConflictException instead of returning the
// existing connector. ConnectorConfiguration must specify exactly one
// provider (Azure, with both ClientIdentifier and TenantIdentifier set) --
// ValidationException otherwise, matching the "You must specify exactly one
// provider configuration" doc comment (the SDK's client-side validators.go
// doesn't itself enforce this, since it's a server-side rule).
func (b *InMemoryBackend) PutConnector(config *ConnectorConfiguration, tags []Tag) (string, error) {
	if config == nil || config.Azure == nil {
		return "", fmt.Errorf("%w: ConnectorConfiguration must specify exactly one provider", ErrValidation)
	}

	clientSet := strings.TrimSpace(config.Azure.ClientIdentifier) != ""
	tenantSet := strings.TrimSpace(config.Azure.TenantIdentifier) != ""

	if !clientSet || !tenantSet {
		return "", fmt.Errorf(
			"%w: Azure connector configuration requires ClientIdentifier and TenantIdentifier", ErrValidation,
		)
	}

	b.mu.Lock("PutConnector")
	defer b.mu.Unlock()

	for _, existing := range b.connectors.All() {
		if connectorConfigurationsMatch(existing.ConnectorConfiguration, config) {
			return "", fmt.Errorf(
				"%w: a connector already exists for the specified connector configuration", ErrConflict,
			)
		}
	}

	name := connectorNamePrefix + uuid.NewString()[:8]
	arn := b.connectorArn(name)
	azureCopy := *config.Azure

	b.connectors.Put(&Connector{
		Arn:                    arn,
		Name:                   name,
		ConnectorConfiguration: &ConnectorConfiguration{Azure: &azureCopy},
		CreatedTime:            float64(time.Now().Unix()),
	})
	b.setResourceTagsLocked(arn, tags)

	return arn, nil
}

// GetConnector returns a copy of the connector identified by arn.
// ResourceNotFoundException/ValidationException are the only errors the real
// GetConnector op declares (verified against its deserializer's error
// switch), so an unknown arn errors ErrResourceNotFound, not the
// NoSuchConfigurationRecorderException-style ErrNotFound this package uses
// for configuration recorders.
func (b *InMemoryBackend) GetConnector(arn string) (*Connector, error) {
	if arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", ErrValidation)
	}

	b.mu.RLock("GetConnector")
	defer b.mu.RUnlock()

	c, ok := b.connectors.Get(arn)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrResourceNotFound, arn)
	}

	cp := *c

	return &cp, nil
}

// connectorProvider returns the wire "provider" enum value for a connector's
// configuration. Currently always "AZURE", the only provider AWS Config
// documents, but derived from the configuration (rather than hardcoded) so a
// future second provider doesn't silently mislabel it.
func connectorProvider(config *ConnectorConfiguration) string {
	if config != nil && config.Azure != nil {
		return "AZURE"
	}

	return ""
}

// connectorTenantIdentifier returns the provider tenant identifier
// denormalized onto ConnectorSummary (verified against the real
// ListConnectors deserializer, which flattens azure.tenantIdentifier up to
// the summary's top-level tenantIdentifier field).
func connectorTenantIdentifier(config *ConnectorConfiguration) string {
	if config != nil && config.Azure != nil {
		return config.Azure.TenantIdentifier
	}

	return ""
}

// connectorMatchesFilters reports whether c satisfies every filter in
// filters. Real AWS Config currently only defines the "provider" FilterName
// (verified against types.ConnectorFilterName's sole enum value); any other
// FilterName is ignored rather than rejected, since ListConnectors's declared
// error model (ValidationException only, no documented per-filter-name
// validation) doesn't specify unknown-filter-name behavior.
func connectorMatchesFilters(c *Connector, filters []ConnectorFilter) bool {
	for _, f := range filters {
		if f.FilterName != "provider" {
			continue
		}

		if !slices.Contains(f.FilterValues, connectorProvider(c.ConnectorConfiguration)) {
			return false
		}
	}

	return true
}

// ListConnectors returns connector summaries matching filters, sorted by ARN
// for deterministic pagination (real AWS Config's own ordering is
// unspecified; a fixed order is what makes this backend's pagination stable
// across calls).
func (b *InMemoryBackend) ListConnectors(filters []ConnectorFilter) []ConnectorSummary {
	b.mu.RLock("ListConnectors")
	defer b.mu.RUnlock()

	all := b.connectors.All()
	out := make([]ConnectorSummary, 0, len(all))

	for _, c := range all {
		if !connectorMatchesFilters(c, filters) {
			continue
		}

		out = append(out, ConnectorSummary{
			Arn:              c.Arn,
			Name:             c.Name,
			Provider:         connectorProvider(c.ConnectorConfiguration),
			TenantIdentifier: connectorTenantIdentifier(c.ConnectorConfiguration),
			CreatedTime:      c.CreatedTime,
		})
	}

	slices.SortFunc(out, func(a, b ConnectorSummary) int { return strings.Compare(a.Arn, b.Arn) })

	return out
}

// DeleteConnector deletes the connector identified by arn.
// ResourceNotFoundException/ValidationException are the only errors the real
// DeleteConnector op declares (verified against its deserializer's error
// switch); it does not declare a ConflictException for "still referenced by
// a configuration recorder", so this backend doesn't invent one either.
func (b *InMemoryBackend) DeleteConnector(arn string) error {
	if arn == "" {
		return fmt.Errorf("%w: Arn is required", ErrValidation)
	}

	b.mu.Lock("DeleteConnector")
	defer b.mu.Unlock()

	if !b.connectors.Has(arn) {
		return fmt.Errorf("%w: %s", ErrResourceNotFound, arn)
	}

	b.connectors.Delete(arn)

	return nil
}
