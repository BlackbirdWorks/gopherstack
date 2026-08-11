package mgn

import (
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// A Connector represents an SSM Managed Instance bridging an on-prem vCenter to
// the AWS control plane. This repo has no SSM Managed Instance concept to
// validate SsmInstanceID against, so it's accepted unvalidated (PARITY.md). No
// AccountID field exists on any Connector op (confirmed by direct SDK read) --
// Connectors are not delegated-account-scoped, unlike SourceServers/Applications/Waves.

// CreateConnectorInput mirrors CreateConnectorInput.
type CreateConnectorInput struct {
	SsmCommandConfig *ConnectorSsmCommandConfig
	Tags             map[string]string
	Name             string
	SsmInstanceID    string
}

// CreateConnector creates a new Connector.
func (b *InMemoryBackend) CreateConnector(in CreateConnectorInput) (*Connector, error) {
	b.mu.Lock("CreateConnector")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return nil, err
	}

	if in.Name == "" {
		return nil, validationError("name is required")
	}

	if in.SsmInstanceID == "" {
		return nil, validationError("ssmInstanceID is required")
	}

	id := newConnectorID()
	t := tags.New("mgn.connector." + id + ".tags")
	t.Merge(in.Tags)

	c := &Connector{
		ConnectorID:      id,
		Arn:              b.connectorARN(id),
		Name:             in.Name,
		SsmInstanceID:    in.SsmInstanceID,
		SsmCommandConfig: in.SsmCommandConfig,
		Tags:             t,
	}
	b.connectors.Put(c)

	return c.clone(), nil
}

// UpdateConnectorInput mirrors UpdateConnectorInput -- everything but
// ConnectorID is optional.
type UpdateConnectorInput struct {
	SsmCommandConfig *ConnectorSsmCommandConfig
	Name             *string
	SsmInstanceID    *string
}

// UpdateConnector applies a partial update to a Connector.
func (b *InMemoryBackend) UpdateConnector(id string, in UpdateConnectorInput) (*Connector, error) {
	b.mu.Lock("UpdateConnector")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return nil, err
	}

	c, ok := b.connectors.Get(id)
	if !ok {
		return nil, notFoundError(resourceConnector, id)
	}

	if in.Name != nil {
		c.Name = *in.Name
	}

	if in.SsmInstanceID != nil {
		c.SsmInstanceID = *in.SsmInstanceID
	}

	if in.SsmCommandConfig != nil {
		c.SsmCommandConfig = in.SsmCommandConfig
	}

	return c.clone(), nil
}

// DeleteConnector deletes a Connector.
func (b *InMemoryBackend) DeleteConnector(id string) error {
	b.mu.Lock("DeleteConnector")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return err
	}

	c, ok := b.connectors.Get(id)
	if !ok {
		return notFoundError(resourceConnector, id)
	}

	if c.Tags != nil {
		c.Tags.Close()
	}

	b.connectors.Delete(id)

	return nil
}

// ListConnectorsFilters mirrors types.ListConnectorsRequestFilters.
type ListConnectorsFilters struct {
	ConnectorIDs []string
}

// ListConnectors returns a page of Connectors matching f.
func (b *InMemoryBackend) ListConnectors(
	f ListConnectorsFilters,
	token string,
	limit int,
) (page.Page[*Connector], error) {
	b.mu.RLock("ListConnectors")
	defer b.mu.RUnlock()

	if err := b.requireInitializedLocked(); err != nil {
		return page.Page[*Connector]{}, err
	}

	all := b.connectors.Snapshot()
	filtered := make([]*Connector, 0, len(all))

	for _, c := range all {
		if len(f.ConnectorIDs) == 0 || containsStr(f.ConnectorIDs, c.ConnectorID) {
			filtered = append(filtered, c.clone())
		}
	}

	return page.New(filtered, token, limit, defaultPageLimit), nil
}
