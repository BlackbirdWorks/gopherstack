package appstream

import "time"

type storedEntitlement struct {
	CreatedTime    time.Time              `json:"createdTime"`
	LastModifiedAt time.Time              `json:"lastModifiedAt"`
	Name           string                 `json:"name"`
	StackName      string                 `json:"stackName"`
	Description    string                 `json:"description"`
	AppVisibility  string                 `json:"appVisibility"`
	Attributes     []EntitlementAttribute `json:"attributes"`
}

func (e *storedEntitlement) toEntitlement() *Entitlement {
	attrs := make([]EntitlementAttribute, len(e.Attributes))
	copy(attrs, e.Attributes)

	return &Entitlement{
		CreatedTime:    e.CreatedTime,
		LastModifiedAt: e.LastModifiedAt,
		Attributes:     attrs,
		Name:           e.Name,
		StackName:      e.StackName,
		Description:    e.Description,
		AppVisibility:  e.AppVisibility,
	}
}

func entitlementKey(name, stackName string) string { return name + "\x00" + stackName }

// CreateEntitlement creates a new entitlement.
func (b *InMemoryBackend) CreateEntitlement(
	name, stackName, description, appVisibility string,
	attributes []EntitlementAttribute,
) (*Entitlement, error) {
	b.mu.Lock("CreateEntitlement")
	defer b.mu.Unlock()

	key := entitlementKey(name, stackName)
	if b.entitlements.Has(key) {
		return nil, ErrEntitlementAlreadyExists
	}

	attrs := make([]EntitlementAttribute, len(attributes))
	copy(attrs, attributes)

	now := time.Now().UTC()
	ent := &storedEntitlement{
		CreatedTime:    now,
		LastModifiedAt: now,
		Attributes:     attrs,
		Name:           name,
		StackName:      stackName,
		Description:    description,
		AppVisibility:  appVisibility,
	}
	b.entitlements.Put(ent)

	return ent.toEntitlement(), nil
}

// DeleteEntitlement removes an entitlement.
func (b *InMemoryBackend) DeleteEntitlement(name, stackName string) error {
	b.mu.Lock("DeleteEntitlement")
	defer b.mu.Unlock()

	key := entitlementKey(name, stackName)
	if !b.entitlements.Has(key) {
		return ErrNotFound
	}

	b.entitlements.Delete(key)
	delete(b.entitlementApps, key)

	return nil
}

// DescribeEntitlements returns entitlements for a stack, optionally filtered by name.
func (b *InMemoryBackend) DescribeEntitlements(name, stackName string) ([]*Entitlement, error) {
	b.mu.RLock("DescribeEntitlements")
	defer b.mu.RUnlock()

	if name != "" {
		key := entitlementKey(name, stackName)
		ent, ok := b.entitlements.Get(key)
		if !ok {
			return nil, ErrNotFound
		}

		return []*Entitlement{ent.toEntitlement()}, nil
	}

	var result []*Entitlement

	for _, ent := range b.entitlements.All() {
		if stackName != "" && ent.StackName != stackName {
			continue
		}

		result = append(result, ent.toEntitlement())
	}

	return result, nil
}

// UpdateEntitlement updates mutable entitlement fields.
func (b *InMemoryBackend) UpdateEntitlement(
	name, stackName, description, appVisibility string,
	attributes []EntitlementAttribute,
) (*Entitlement, error) {
	b.mu.Lock("UpdateEntitlement")
	defer b.mu.Unlock()

	key := entitlementKey(name, stackName)
	ent, ok := b.entitlements.Get(key)
	if !ok {
		return nil, ErrNotFound
	}

	if description != "" {
		ent.Description = description
	}

	if appVisibility != "" {
		ent.AppVisibility = appVisibility
	}

	if len(attributes) > 0 {
		attrs := make([]EntitlementAttribute, len(attributes))
		copy(attrs, attributes)
		ent.Attributes = attrs
	}

	ent.LastModifiedAt = time.Now().UTC()

	return ent.toEntitlement(), nil
}

// AssociateApplicationToEntitlement links an application to an entitlement.
func (b *InMemoryBackend) AssociateApplicationToEntitlement(appID, entitlementName, stackName string) error {
	b.mu.Lock("AssociateApplicationToEntitlement")
	defer b.mu.Unlock()

	key := entitlementKey(entitlementName, stackName)
	if !b.entitlements.Has(key) {
		return ErrNotFound
	}

	if b.entitlementApps[key] == nil {
		b.entitlementApps[key] = make(map[string]bool)
	}

	b.entitlementApps[key][appID] = true

	return nil
}

// DisassociateApplicationFromEntitlement removes an application-entitlement link.
func (b *InMemoryBackend) DisassociateApplicationFromEntitlement(appID, entitlementName, stackName string) error {
	b.mu.Lock("DisassociateApplicationFromEntitlement")
	defer b.mu.Unlock()

	key := entitlementKey(entitlementName, stackName)
	if !b.entitlements.Has(key) {
		return ErrNotFound
	}

	if b.entitlementApps[key] != nil {
		delete(b.entitlementApps[key], appID)
	}

	return nil
}

// ListEntitledApplications returns applications associated with an entitlement.
func (b *InMemoryBackend) ListEntitledApplications(entitlementName, stackName string) ([]*EntitledApplication, error) {
	b.mu.RLock("ListEntitledApplications")
	defer b.mu.RUnlock()

	key := entitlementKey(entitlementName, stackName)
	if !b.entitlements.Has(key) {
		return nil, ErrNotFound
	}

	apps := b.entitlementApps[key]
	result := make([]*EntitledApplication, 0, len(apps))

	for appID := range apps {
		result = append(result, &EntitledApplication{ApplicationIdentifier: appID})
	}

	return result, nil
}
