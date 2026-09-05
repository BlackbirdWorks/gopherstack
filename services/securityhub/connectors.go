package securityhub

import (
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	cspmEnablementStatusPendingEnablement = "PENDING_ENABLEMENT"
	cspmEnablementStatusPendingUpdate     = "PENDING_UPDATE"
	cspmEnablementStatusPendingDeletion   = "PENDING_DELETION"
	cspmConnectorStatusUnknown            = "UNKNOWN"

	msgConnectorAwaitingAuth = "Awaiting external authorization with the cloud provider account; " +
		"connectivity cannot be established until the provider-side authorization step is completed out-of-band."
)

func (b *InMemoryBackend) cspmConnectorARN(id string) string {
	return arn.Build("securityhub", b.region, b.accountID, fmt.Sprintf("connector/%s", id))
}

// extractCspmProviderTag returns the single tag key and detail map of a CSPM
// provider tagged-union value (e.g. {"Azure": {...}} -> "Azure", {...}). The
// real CspmProviderConfiguration/CspmProviderUpdateConfiguration/
// CspmProviderDetail types are all Smithy tagged unions with exactly one
// member ever set; this mirrors that by returning the first (and, for a
// valid request, only) key present whose value is itself an object.
func extractCspmProviderTag(m map[string]any) (string, map[string]any) {
	for k, v := range m {
		if dm, ok := v.(map[string]any); ok {
			return k, dm
		}
	}

	return "", nil
}

// clone deep-copies c's map fields. Tags is created aliased to
// b.tags[ConnectorArn] (same map object, see CreateConnector), and
// TagResource/UntagResource mutate that map in place under lock -- a
// shallow "cp := *c" leaves the returned copy's Tags field pointing at that
// live, mutable map.
func (c *CspmConnector) clone() *CspmConnector {
	cp := *c
	cp.Provider = maps.Clone(c.Provider)
	cp.Tags = maps.Clone(c.Tags)

	return &cp
}

func (b *InMemoryBackend) resolveCspmConnector(identifier string) (*CspmConnector, bool) {
	if c, ok := b.cspmConnectors.Get(identifier); ok {
		return c, true
	}

	for _, c := range b.cspmConnectors.All() {
		if c.ConnectorArn == identifier {
			return c, true
		}
	}

	return nil, false
}

// CreateConnector creates a CSPM connector to a third-party cloud provider
// (currently Azure only -- the real CspmProviderConfiguration union has a
// single Azure member).
//
// Unlike Connectors V2 (which has a dedicated RegisterConnectorV2 operation
// to complete an out-of-band OAuth-style handshake), the real CreateConnector
// surface has NO companion "complete authorization" operation at all.
// Establishing connectivity to the provider account requires a purely
// external, provider-side step (e.g. granting the AWSConfigConnectorArn role
// access in the Azure portal) that this mock has no API-observable signal
// for. A newly created connector is therefore left at
// EnablementStatus=PENDING_ENABLEMENT with health ConnectorStatus=UNKNOWN
// permanently -- it is never auto-advanced to CONNECTED/ENABLED, since doing
// so would fabricate a transition no real client-visible action caused. See
// PARITY.md for the documented gap this leaves (GetConnector/ListConnectors
// can never observe a CONNECTED connector against this backend).
func (b *InMemoryBackend) CreateConnector(
	name, description string,
	provider map[string]any,
	tags map[string]string,
) (*CspmConnector, error) {
	b.mu.Lock("CreateConnector")
	defer b.mu.Unlock()

	b.cspmConnectorSeq++
	id := fmt.Sprintf("connector-%d", b.cspmConnectorSeq)
	connArn := b.cspmConnectorARN(id)
	now := time.Now().UTC().Format(time.RFC3339)

	tag, _ := extractCspmProviderTag(provider)

	c := &CspmConnector{
		ConnectorId:      id,
		ConnectorArn:     connArn,
		Name:             name,
		Description:      description,
		CreatedAt:        now,
		LastUpdatedAt:    now,
		CreatedBy:        b.accountID,
		EnablementStatus: cspmEnablementStatusPendingEnablement,
		ConnectorStatus:  cspmConnectorStatusUnknown,
		HealthMessage:    msgConnectorAwaitingAuth,
		HealthCheckedAt:  now,
		ProviderName:     strings.ToUpper(tag),
		Provider:         provider,
		Tags:             tags,
	}
	b.cspmConnectors.Put(c)

	if len(tags) > 0 {
		b.tags[connArn] = tags
	}

	return c.clone(), nil
}

// GetConnector retrieves a connector by ID or ARN.
func (b *InMemoryBackend) GetConnector(connectorID string) (*CspmConnector, error) {
	b.mu.RLock("GetConnector")
	defer b.mu.RUnlock()

	c, ok := b.resolveCspmConnector(connectorID)
	if !ok {
		return nil, ErrNotFound
	}

	return c.clone(), nil
}

// ListConnectors lists connectors, optionally filtered by connectivity
// status, enablement status, and/or provider name -- the same three filters
// the real ListConnectorsInput exposes as query parameters.
func (b *InMemoryBackend) ListConnectors(
	connectorStatus, enablementStatus, providerName, nextToken string,
	maxResults int,
) ([]*CspmConnector, string) {
	b.mu.RLock("ListConnectors")
	defer b.mu.RUnlock()

	snap := b.cspmConnectors.Snapshot()
	all := make([]*CspmConnector, 0, len(snap))

	for _, c := range snap {
		if connectorStatus != "" && c.ConnectorStatus != connectorStatus {
			continue
		}

		if enablementStatus != "" && c.EnablementStatus != enablementStatus {
			continue
		}

		if providerName != "" && c.ProviderName != providerName {
			continue
		}

		all = append(all, c.clone())
	}

	return paginateSlice(all, nextToken, maxResults, maxDefaultResults)
}

// UpdateConnector updates a connector's description and/or provider scope
// configuration (AzureRegions/ScopeConfiguration -- the real
// AzureUpdateConfiguration shape has no AWSConfigConnectorArn field, so that
// value is always preserved from the original CreateConnector call, merged
// with the new fields rather than replaced wholesale).
//
// A configuration change requires re-validation against the provider, so the
// connector's EnablementStatus moves to PENDING_UPDATE. As with
// CreateConnector, there is no out-of-band signal this mock can observe to
// advance it back to ENABLED, so it is left at PENDING_UPDATE -- see
// CreateConnector's doc comment for the same honest-lifecycle rationale.
func (b *InMemoryBackend) UpdateConnector(
	connectorID, description string,
	provider map[string]any,
) (*CspmConnector, error) {
	b.mu.Lock("UpdateConnector")
	defer b.mu.Unlock()

	c, ok := b.resolveCspmConnector(connectorID)
	if !ok {
		return nil, ErrNotFound
	}

	if description != "" {
		c.Description = description
	}

	if _, detail := extractCspmProviderTag(provider); detail != nil {
		c.Provider = mergeCspmProvider(c.Provider, provider)
		c.EnablementStatus = cspmEnablementStatusPendingUpdate
	}

	c.LastUpdatedAt = time.Now().UTC().Format(time.RFC3339)

	return c.clone(), nil
}

// mergeCspmProvider merges an update's provider detail fields (e.g.
// AzureRegions/ScopeConfiguration) onto the existing stored provider detail,
// preserving any fields the update doesn't carry (notably
// AWSConfigConnectorArn, which AzureUpdateConfiguration never sends).
func mergeCspmProvider(existing, update map[string]any) map[string]any {
	existingTag, existingDetail := extractCspmProviderTag(existing)
	updateTag, updateDetail := extractCspmProviderTag(update)

	if existingDetail == nil {
		return update
	}

	merged := make(map[string]any, len(existingDetail)+len(updateDetail))
	maps.Copy(merged, existingDetail)
	maps.Copy(merged, updateDetail)

	tag := existingTag
	if tag == "" {
		tag = updateTag
	}

	return map[string]any{tag: merged}
}

// DeleteConnector removes a connector. The real DeleteConnectorOutput
// reports the connector's EnablementStatus (PENDING_DELETION -- AWS may take
// time to tear down the provider-side connection) rather than confirming
// full removal. This mock has no background worker to model that async
// deletion window, so the record is removed immediately, but the response
// still reports PENDING_DELETION for wire fidelity with the real (eventually
// consistent) API.
func (b *InMemoryBackend) DeleteConnector(connectorID string) (string, error) {
	b.mu.Lock("DeleteConnector")
	defer b.mu.Unlock()

	c, ok := b.resolveCspmConnector(connectorID)
	if !ok {
		return "", ErrNotFound
	}

	b.cspmConnectors.Delete(c.ConnectorId)

	if len(c.Tags) > 0 {
		delete(b.tags, c.ConnectorArn)
	}

	return cspmEnablementStatusPendingDeletion, nil
}
