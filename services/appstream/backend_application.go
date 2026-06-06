package appstream

import (
	"fmt"
	"maps"
	"time"
)

type storedApplication struct {
	CreatedTime time.Time         `json:"createdTime"`
	Tags        map[string]string `json:"tags"`
	Platforms   []string          `json:"platforms"`
	Name        string            `json:"name"`
	Arn         string            `json:"arn"`
	DisplayName string            `json:"displayName"`
	Description string            `json:"description"`
	LaunchPath  string            `json:"launchPath"`
	AppBlockArn string            `json:"appBlockArn"`
}

func (a *storedApplication) toApplication() *Application {
	tags := make(map[string]string)
	maps.Copy(tags, a.Tags)

	platforms := make([]string, len(a.Platforms))
	copy(platforms, a.Platforms)

	return &Application{
		CreatedTime: a.CreatedTime,
		Tags:        tags,
		Platforms:   platforms,
		Name:        a.Name,
		Arn:         a.Arn,
		DisplayName: a.DisplayName,
		Description: a.Description,
		LaunchPath:  a.LaunchPath,
		AppBlockArn: a.AppBlockArn,
	}
}

type storedEntitlement struct {
	CreatedTime    time.Time              `json:"createdTime"`
	LastModifiedAt time.Time              `json:"lastModifiedAt"`
	Attributes     []EntitlementAttribute `json:"attributes"`
	Name           string                 `json:"name"`
	StackName      string                 `json:"stackName"`
	Description    string                 `json:"description"`
	AppVisibility  string                 `json:"appVisibility"`
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

type storedDirectoryConfig struct {
	CreatedTime                          time.Time `json:"createdTime"`
	OrganizationalUnitDistinguishedNames []string  `json:"ouDNs"`
	DirectoryName                        string    `json:"directoryName"`
	Arn                                  string    `json:"arn"`
}

func (d *storedDirectoryConfig) toDirectoryConfig() *DirectoryConfig {
	ouDNs := make([]string, len(d.OrganizationalUnitDistinguishedNames))
	copy(ouDNs, d.OrganizationalUnitDistinguishedNames)

	return &DirectoryConfig{
		CreatedTime:                          d.CreatedTime,
		OrganizationalUnitDistinguishedNames: ouDNs,
		DirectoryName:                        d.DirectoryName,
		Arn:                                  d.Arn,
	}
}

func (b *InMemoryBackend) applicationARN(name string) string {
	return fmt.Sprintf("arn:aws:appstream:%s:%s:application/%s", b.region, b.accountID, name)
}

func (b *InMemoryBackend) directoryConfigARN(name string) string {
	return fmt.Sprintf("arn:aws:appstream:%s:%s:directory-config/%s", b.region, b.accountID, name)
}

func entitlementKey(name, stackName string) string { return name + "\x00" + stackName }

// CreateApplication creates a new application.
func (b *InMemoryBackend) CreateApplication(
	name, displayName, description, launchPath, appBlockArn string,
	platforms []string,
	tags map[string]string,
) (*Application, error) {
	b.mu.Lock("CreateApplication")
	defer b.mu.Unlock()

	if _, ok := b.applications[name]; ok {
		return nil, ErrAlreadyExists
	}

	arn := b.applicationARN(name)
	storedTags := make(map[string]string)
	maps.Copy(storedTags, tags)

	ps := make([]string, len(platforms))
	copy(ps, platforms)

	app := &storedApplication{
		CreatedTime: time.Now().UTC(),
		Tags:        storedTags,
		Platforms:   ps,
		Name:        name,
		Arn:         arn,
		DisplayName: displayName,
		Description: description,
		LaunchPath:  launchPath,
		AppBlockArn: appBlockArn,
	}
	b.applications[name] = app
	b.tags[arn] = storedTags

	return app.toApplication(), nil
}

// DeleteApplication removes an application.
func (b *InMemoryBackend) DeleteApplication(name string) error {
	b.mu.Lock("DeleteApplication")
	defer b.mu.Unlock()

	app, ok := b.applications[name]
	if !ok {
		return ErrNotFound
	}

	delete(b.tags, app.Arn)
	delete(b.applications, name)
	delete(b.appFleetAssoc, name)

	return nil
}

// DescribeApplications returns applications, optionally filtered by name.
func (b *InMemoryBackend) DescribeApplications(names []string) ([]*Application, error) {
	b.mu.RLock("DescribeApplications")
	defer b.mu.RUnlock()

	if len(names) > 0 {
		var result []*Application

		for _, name := range names {
			app, ok := b.applications[name]
			if !ok {
				return nil, ErrNotFound
			}

			result = append(result, app.toApplication())
		}

		return result, nil
	}

	result := make([]*Application, 0, len(b.applications))
	for _, app := range b.applications {
		result = append(result, app.toApplication())
	}

	return result, nil
}

// UpdateApplication updates mutable application fields.
func (b *InMemoryBackend) UpdateApplication(name, displayName, description, launchPath string) (*Application, error) {
	b.mu.Lock("UpdateApplication")
	defer b.mu.Unlock()

	app, ok := b.applications[name]
	if !ok {
		return nil, ErrNotFound
	}

	if displayName != "" {
		app.DisplayName = displayName
	}

	if description != "" {
		app.Description = description
	}

	if launchPath != "" {
		app.LaunchPath = launchPath
	}

	return app.toApplication(), nil
}

// DescribeAppLicenseUsage returns license usage (stub — always empty).
func (b *InMemoryBackend) DescribeAppLicenseUsage() ([]map[string]string, error) {
	return []map[string]string{}, nil
}

// AssociateApplicationFleet links an application to a fleet.
func (b *InMemoryBackend) AssociateApplicationFleet(appName, fleetName string) error {
	b.mu.Lock("AssociateApplicationFleet")
	defer b.mu.Unlock()

	if _, ok := b.applications[appName]; !ok {
		return ErrNotFound
	}

	if _, ok := b.fleets[fleetName]; !ok {
		return ErrNotFound
	}

	if b.appFleetAssoc[appName] == nil {
		b.appFleetAssoc[appName] = make(map[string]bool)
	}

	b.appFleetAssoc[appName][fleetName] = true

	return nil
}

// DisassociateApplicationFleet removes an application-fleet link.
func (b *InMemoryBackend) DisassociateApplicationFleet(appName, fleetName string) error {
	b.mu.Lock("DisassociateApplicationFleet")
	defer b.mu.Unlock()

	if _, ok := b.applications[appName]; !ok {
		return ErrNotFound
	}

	if _, ok := b.fleets[fleetName]; !ok {
		return ErrNotFound
	}

	if b.appFleetAssoc[appName] != nil {
		delete(b.appFleetAssoc[appName], fleetName)
	}

	return nil
}

// DescribeApplicationFleetAssociations returns application-fleet links.
func (b *InMemoryBackend) DescribeApplicationFleetAssociations(
	appName, fleetName string,
) ([]*ApplicationFleetAssociation, error) {
	b.mu.RLock("DescribeApplicationFleetAssociations")
	defer b.mu.RUnlock()

	var result []*ApplicationFleetAssociation

	for aName, fleets := range b.appFleetAssoc {
		if appName != "" && aName != appName {
			continue
		}

		app, ok := b.applications[aName]
		if !ok {
			continue
		}

		for fName := range fleets {
			if fleetName != "" && fName != fleetName {
				continue
			}

			result = append(result, &ApplicationFleetAssociation{
				ApplicationArn: app.Arn,
				FleetName:      fName,
				State:          "ASSOCIATED",
			})
		}
	}

	return result, nil
}

// CreateEntitlement creates a new entitlement.
func (b *InMemoryBackend) CreateEntitlement(
	name, stackName, description, appVisibility string,
	attributes []EntitlementAttribute,
) (*Entitlement, error) {
	b.mu.Lock("CreateEntitlement")
	defer b.mu.Unlock()

	key := entitlementKey(name, stackName)
	if _, ok := b.entitlements[key]; ok {
		return nil, ErrAlreadyExists
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
	b.entitlements[key] = ent

	return ent.toEntitlement(), nil
}

// DeleteEntitlement removes an entitlement.
func (b *InMemoryBackend) DeleteEntitlement(name, stackName string) error {
	b.mu.Lock("DeleteEntitlement")
	defer b.mu.Unlock()

	key := entitlementKey(name, stackName)
	if _, ok := b.entitlements[key]; !ok {
		return ErrNotFound
	}

	delete(b.entitlements, key)
	delete(b.entitlementApps, key)

	return nil
}

// DescribeEntitlements returns entitlements for a stack, optionally filtered by name.
func (b *InMemoryBackend) DescribeEntitlements(name, stackName string) ([]*Entitlement, error) {
	b.mu.RLock("DescribeEntitlements")
	defer b.mu.RUnlock()

	if name != "" {
		key := entitlementKey(name, stackName)
		ent, ok := b.entitlements[key]
		if !ok {
			return nil, ErrNotFound
		}

		return []*Entitlement{ent.toEntitlement()}, nil
	}

	var result []*Entitlement

	for _, ent := range b.entitlements {
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
	ent, ok := b.entitlements[key]
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
	if _, ok := b.entitlements[key]; !ok {
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
	if _, ok := b.entitlements[key]; !ok {
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
	if _, ok := b.entitlements[key]; !ok {
		return nil, ErrNotFound
	}

	apps := b.entitlementApps[key]
	result := make([]*EntitledApplication, 0, len(apps))

	for appID := range apps {
		result = append(result, &EntitledApplication{ApplicationIdentifier: appID})
	}

	return result, nil
}

// CreateDirectoryConfig creates a new directory configuration.
func (b *InMemoryBackend) CreateDirectoryConfig(name string, ouDNs []string) (*DirectoryConfig, error) {
	b.mu.Lock("CreateDirectoryConfig")
	defer b.mu.Unlock()

	if _, ok := b.directoryConfigs[name]; ok {
		return nil, ErrAlreadyExists
	}

	arn := b.directoryConfigARN(name)
	dns := make([]string, len(ouDNs))
	copy(dns, ouDNs)

	dc := &storedDirectoryConfig{
		CreatedTime:                          time.Now().UTC(),
		OrganizationalUnitDistinguishedNames: dns,
		DirectoryName:                        name,
		Arn:                                  arn,
	}
	b.directoryConfigs[name] = dc

	return dc.toDirectoryConfig(), nil
}

// DeleteDirectoryConfig removes a directory configuration.
func (b *InMemoryBackend) DeleteDirectoryConfig(name string) error {
	b.mu.Lock("DeleteDirectoryConfig")
	defer b.mu.Unlock()

	if _, ok := b.directoryConfigs[name]; !ok {
		return ErrNotFound
	}

	delete(b.directoryConfigs, name)

	return nil
}

// DescribeDirectoryConfigs returns directory configs, optionally filtered by name.
func (b *InMemoryBackend) DescribeDirectoryConfigs(names []string) ([]*DirectoryConfig, error) {
	b.mu.RLock("DescribeDirectoryConfigs")
	defer b.mu.RUnlock()

	if len(names) > 0 {
		var result []*DirectoryConfig

		for _, name := range names {
			dc, ok := b.directoryConfigs[name]
			if !ok {
				return nil, ErrNotFound
			}

			result = append(result, dc.toDirectoryConfig())
		}

		return result, nil
	}

	result := make([]*DirectoryConfig, 0, len(b.directoryConfigs))
	for _, dc := range b.directoryConfigs {
		result = append(result, dc.toDirectoryConfig())
	}

	return result, nil
}

// UpdateDirectoryConfig updates the OUs of a directory configuration.
func (b *InMemoryBackend) UpdateDirectoryConfig(name string, ouDNs []string) (*DirectoryConfig, error) {
	b.mu.Lock("UpdateDirectoryConfig")
	defer b.mu.Unlock()

	dc, ok := b.directoryConfigs[name]
	if !ok {
		return nil, ErrNotFound
	}

	if len(ouDNs) > 0 {
		dns := make([]string, len(ouDNs))
		copy(dns, ouDNs)
		dc.OrganizationalUnitDistinguishedNames = dns
	}

	return dc.toDirectoryConfig(), nil
}
