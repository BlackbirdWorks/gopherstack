package glue

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

var ErrIntegrationNotFound = fmt.Errorf("integration not found: %w", ErrNotFound)

// integrationTransitionDelay is the async delay before a CREATING integration
// reaches ACTIVE, on the same 100-200ms scale as crawlerTransitionDelay/
// jobTransitionDelay. Applied via b.integrationReadyAt and reconciled by
// reconcileLocked (services/glue/reconciler.go), mirroring the crawler
// RUNNING->READY mechanism exactly.
const integrationTransitionDelay = 150 * time.Millisecond

// integrationARN returns the ARN for a Glue Zero-ETL integration, following
// this service's established "<resourceType>/<name>" convention (see
// blueprintARN, connectionARN, etc.).
func (b *InMemoryBackend) integrationARN(name string) string {
	return arn.Build("glue", b.region, b.accountID, "integration/"+name)
}

// resolveIntegrationName resolves an IntegrationIdentifier to the
// integration's name. Real CreateIntegration/ModifyIntegration/
// DeleteIntegration's own SDK doc comments describe IntegrationIdentifier as
// "The Amazon Resource Name (ARN) for the integration", so a real client
// passes the ARN this backend itself generated, not the bare name; accept
// either since this backend's store is keyed by name.
func (b *InMemoryBackend) resolveIntegrationName(identifier string) string {
	if _, name, ok := strings.Cut(identifier, "integration/"); ok {
		return name
	}

	return identifier
}

// CreateIntegration stores a new integration.
func (b *InMemoryBackend) CreateIntegration(
	name, sourceArn, targetArn string,
	tags map[string]string,
) (*Integration, error) {
	b.mu.Lock("CreateIntegration")
	defer b.mu.Unlock()

	if sourceArn == "" {
		return nil, fmt.Errorf("%w: SourceArn is required", ErrValidation)
	}

	if targetArn == "" {
		return nil, fmt.Errorf("%w: TargetArn is required", ErrValidation)
	}

	now := time.Now().UTC()
	ig := &Integration{
		IntegrationName: name,
		IntegrationArn:  b.integrationARN(name),
		SourceArn:       sourceArn,
		TargetArn:       targetArn,
		Status:          "CREATING",
		Tags:            tags,
		CreatedAt:       now,
	}
	b.integrations.Put(ig)

	// Nothing previously advanced a CREATING integration -- no ticker, no later
	// call. Schedule the async CREATING -> ACTIVE transition, reconciled by
	// reconcileLocked exactly like crawlerReadyAt's RUNNING -> READY.
	b.integrationReadyAt[name] = now.Add(integrationTransitionDelay)

	cp := *ig

	return &cp, nil
}

// DeleteIntegration removes an integration, identified by name or ARN
// (see resolveIntegrationName), and returns the record as it stood right
// before deletion so the caller can echo the real required response fields.
func (b *InMemoryBackend) DeleteIntegration(identifier string) (*Integration, error) {
	b.mu.Lock("DeleteIntegration")
	defer b.mu.Unlock()

	name := b.resolveIntegrationName(identifier)

	ig, ok := b.integrations.Get(name)
	if !ok {
		return nil, fmt.Errorf("integration %q not found: %w", identifier, ErrNotFound)
	}

	cp := *ig
	b.integrations.Delete(name)

	return &cp, nil
}

// ListIntegrations returns all integrations.
func (b *InMemoryBackend) ListIntegrations() []*Integration {
	b.advanceStates(time.Now())

	b.mu.RLock("ListIntegrations")
	defer b.mu.RUnlock()

	src := b.integrations.All()
	list := make([]*Integration, 0, len(src))
	for _, ig := range src {
		cp := *ig
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, k int) bool {
		return list[i].IntegrationName < list[k].IntegrationName
	})

	return list
}

// ModifyIntegration updates an integration, identified by name or ARN (see
// resolveIntegrationName), and returns the current record so the caller can
// echo the real required response fields.
func (b *InMemoryBackend) ModifyIntegration(identifier string) (*Integration, error) {
	b.mu.Lock("ModifyIntegration")
	defer b.mu.Unlock()

	name := b.resolveIntegrationName(identifier)

	ig, ok := b.integrations.Get(name)
	if !ok {
		return nil, ErrIntegrationNotFound
	}

	cp := *ig

	return &cp, nil
}

// cloneIntegrationResourceProperty returns a copy of p with cloned maps, so callers
// can't mutate live backend state through the returned pointer (and readers outside
// the lock can't race with UpdateIntegrationResourceProperty mutating the original).
func cloneIntegrationResourceProperty(p *IntegrationResourceProperty) *IntegrationResourceProperty {
	cp := *p
	cp.SourceProperties = maps.Clone(p.SourceProperties)
	cp.TargetProperties = maps.Clone(p.TargetProperties)

	return &cp
}

// CreateIntegrationResourceProperty stores properties for an integration resource.
func (b *InMemoryBackend) CreateIntegrationResourceProperty(
	resourceArn string,
	sourceProps, targetProps map[string]string,
) (*IntegrationResourceProperty, error) {
	if resourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrValidation)
	}

	b.mu.Lock("CreateIntegrationResourceProperty")
	defer b.mu.Unlock()

	prop := &IntegrationResourceProperty{
		CreatedAt:        time.Now(),
		ResourceArn:      resourceArn,
		SourceProperties: sourceProps,
		TargetProperties: targetProps,
	}
	b.integrationResourceProps.Put(prop)

	return cloneIntegrationResourceProperty(prop), nil
}

// GetIntegrationResourceProperty retrieves stored resource properties.
func (b *InMemoryBackend) GetIntegrationResourceProperty(resourceArn string) (*IntegrationResourceProperty, error) {
	if resourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrValidation)
	}

	b.mu.RLock("GetIntegrationResourceProperty")
	defer b.mu.RUnlock()

	prop, ok := b.integrationResourceProps.Get(resourceArn)
	if !ok {
		return nil, fmt.Errorf("resource property for %q not found: %w", resourceArn, ErrNotFound)
	}

	return cloneIntegrationResourceProperty(prop), nil
}

// UpdateIntegrationResourceProperty updates a previously created resource property.
func (b *InMemoryBackend) UpdateIntegrationResourceProperty(
	resourceArn string,
	sourceProps, targetProps map[string]string,
) (*IntegrationResourceProperty, error) {
	if resourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrValidation)
	}

	b.mu.Lock("UpdateIntegrationResourceProperty")
	defer b.mu.Unlock()

	prop, ok := b.integrationResourceProps.Get(resourceArn)
	if !ok {
		return nil, fmt.Errorf("resource property for %q not found: %w", resourceArn, ErrNotFound)
	}

	if sourceProps != nil {
		prop.SourceProperties = sourceProps
	}

	if targetProps != nil {
		prop.TargetProperties = targetProps
	}

	return cloneIntegrationResourceProperty(prop), nil
}

// ListIntegrationResourceProperties returns all stored integration resource
// properties, sorted by resource ARN for a deterministic response.
func (b *InMemoryBackend) ListIntegrationResourceProperties() []*IntegrationResourceProperty {
	b.mu.RLock("ListIntegrationResourceProperties")
	defer b.mu.RUnlock()

	src := b.integrationResourceProps.Snapshot()
	out := make([]*IntegrationResourceProperty, 0, len(src))
	for _, p := range src {
		out = append(out, cloneIntegrationResourceProperty(p))
	}

	return out
}

// CreateIntegrationTableProperties stores properties for an integration table.
func (b *InMemoryBackend) CreateIntegrationTableProperties(
	resourceArn, tableName string,
	sourceConfig, targetConfig map[string]any,
) error {
	if resourceArn == "" || tableName == "" {
		return fmt.Errorf("%w: ResourceArn and TableName are required", ErrValidation)
	}

	b.mu.Lock("CreateIntegrationTableProperties")
	defer b.mu.Unlock()

	b.integrationTableProps.Put(&IntegrationTableProperties{
		ResourceArn:       resourceArn,
		TableName:         tableName,
		SourceTableConfig: sourceConfig,
		TargetTableConfig: targetConfig,
	})

	return nil
}

// GetIntegrationTableProperties retrieves stored table properties.
func (b *InMemoryBackend) GetIntegrationTableProperties(
	resourceArn, tableName string,
) (*IntegrationTableProperties, error) {
	if resourceArn == "" || tableName == "" {
		return nil, fmt.Errorf("%w: ResourceArn and TableName are required", ErrValidation)
	}

	b.mu.RLock("GetIntegrationTableProperties")
	defer b.mu.RUnlock()

	key := resourceArn + "|" + tableName

	prop, ok := b.integrationTableProps.Get(key)
	if !ok {
		return nil, fmt.Errorf("table property for %q/%q not found: %w", resourceArn, tableName, ErrNotFound)
	}

	cp := *prop
	cp.SourceTableConfig = maps.Clone(prop.SourceTableConfig)
	cp.TargetTableConfig = maps.Clone(prop.TargetTableConfig)

	return &cp, nil
}

// UpdateIntegrationTableProperties updates a previously created table property.
func (b *InMemoryBackend) UpdateIntegrationTableProperties(
	resourceArn, tableName string,
	sourceConfig, targetConfig map[string]any,
) error {
	if resourceArn == "" || tableName == "" {
		return fmt.Errorf("%w: ResourceArn and TableName are required", ErrValidation)
	}

	b.mu.Lock("UpdateIntegrationTableProperties")
	defer b.mu.Unlock()

	key := resourceArn + "|" + tableName

	prop, ok := b.integrationTableProps.Get(key)
	if !ok {
		return fmt.Errorf("table property for %q/%q not found: %w", resourceArn, tableName, ErrNotFound)
	}

	if sourceConfig != nil {
		prop.SourceTableConfig = sourceConfig
	}

	if targetConfig != nil {
		prop.TargetTableConfig = targetConfig
	}

	return nil
}

// DeleteIntegrationResourceProperty removes stored integration resource
// properties for resourceArn.  Returns ErrNotFound if none exist.
func (b *InMemoryBackend) DeleteIntegrationResourceProperty(resourceArn string) error {
	if resourceArn == "" {
		return fmt.Errorf("%w: ResourceArn is required", ErrValidation)
	}

	b.mu.Lock("DeleteIntegrationResourceProperty")
	defer b.mu.Unlock()

	if !b.integrationResourceProps.Has(resourceArn) {
		return fmt.Errorf("resource property for %q not found: %w", resourceArn, ErrNotFound)
	}

	b.integrationResourceProps.Delete(resourceArn)

	return nil
}

// DeleteIntegrationTableProperties removes stored integration table properties
// for the given resource ARN and table name.  Returns ErrNotFound if none exist.
func (b *InMemoryBackend) DeleteIntegrationTableProperties(resourceArn, tableName string) error {
	if resourceArn == "" || tableName == "" {
		return fmt.Errorf("%w: ResourceArn and TableName are required", ErrValidation)
	}

	b.mu.Lock("DeleteIntegrationTableProperties")
	defer b.mu.Unlock()

	key := resourceArn + "|" + tableName
	if !b.integrationTableProps.Has(key) {
		return fmt.Errorf(
			"table property for %q/%q not found: %w",
			resourceArn,
			tableName,
			ErrNotFound,
		)
	}

	b.integrationTableProps.Delete(key)

	return nil
}
