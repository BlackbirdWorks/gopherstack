package appstream

import (
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

type storedApplication struct {
	CreatedTime time.Time         `json:"createdTime"`
	Tags        map[string]string `json:"tags"`
	Name        string            `json:"name"`
	Arn         string            `json:"arn"`
	DisplayName string            `json:"displayName"`
	Description string            `json:"description"`
	LaunchPath  string            `json:"launchPath"`
	AppBlockArn string            `json:"appBlockArn"`
	Platforms   []string          `json:"platforms"`
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

func (b *InMemoryBackend) applicationARN(name string) string {
	return arn.Build("appstream", b.region, b.accountID, fmt.Sprintf("application/%s", name))
}

// CreateApplication creates a new application.
func (b *InMemoryBackend) CreateApplication(
	name, displayName, description, launchPath, appBlockArn string,
	platforms []string,
	tags map[string]string,
) (*Application, error) {
	b.mu.Lock("CreateApplication")
	defer b.mu.Unlock()

	if b.applications.Has(name) {
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
	b.applications.Put(app)
	b.tags[arn] = storedTags

	return app.toApplication(), nil
}

// DeleteApplication removes an application.
func (b *InMemoryBackend) DeleteApplication(name string) error {
	b.mu.Lock("DeleteApplication")
	defer b.mu.Unlock()

	app, ok := b.applications.Get(name)
	if !ok {
		return ErrNotFound
	}

	delete(b.tags, app.Arn)
	b.applications.Delete(name)
	delete(b.appFleetAssoc, name)

	return nil
}

// findApplication resolves id against Name (the primary key used by
// CreateApplication/DeleteApplication/UpdateApplication) or Arn. Real AWS
// identifies applications by ARN in DescribeApplications and in the
// ApplicationArn member of the Application-Fleet association operations, so
// callers on those wire paths must resolve through this helper rather than
// indexing b.applications directly with the caller-supplied identifier.
func (b *InMemoryBackend) findApplication(id string) (*storedApplication, bool) {
	if app, ok := b.applications.Get(id); ok {
		return app, true
	}

	for _, app := range b.applications.All() {
		if app.Arn == id {
			return app, true
		}
	}

	return nil, false
}

// DescribeApplications returns applications, optionally filtered by ARN.
func (b *InMemoryBackend) DescribeApplications(arns []string) ([]*Application, error) {
	b.mu.RLock("DescribeApplications")
	defer b.mu.RUnlock()

	if len(arns) > 0 {
		var result []*Application

		for _, id := range arns {
			app, ok := b.findApplication(id)
			if !ok {
				return nil, ErrNotFound
			}

			result = append(result, app.toApplication())
		}

		return result, nil
	}

	result := make([]*Application, 0, b.applications.Len())
	for _, app := range b.applications.All() {
		result = append(result, app.toApplication())
	}

	return result, nil
}

// UpdateApplication updates mutable application fields.
func (b *InMemoryBackend) UpdateApplication(name, displayName, description, launchPath string) (*Application, error) {
	b.mu.Lock("UpdateApplication")
	defer b.mu.Unlock()

	app, ok := b.applications.Get(name)
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

// AssociateApplicationFleet links an application to a fleet. appID accepts
// either the application Name or its Arn -- real AWS's
// AssociateApplicationFleet request carries the ApplicationArn.
func (b *InMemoryBackend) AssociateApplicationFleet(appID, fleetName string) error {
	b.mu.Lock("AssociateApplicationFleet")
	defer b.mu.Unlock()

	app, ok := b.findApplication(appID)
	if !ok {
		return ErrNotFound
	}

	if !b.fleets.Has(fleetName) {
		return ErrNotFound
	}

	if b.appFleetAssoc[app.Name] == nil {
		b.appFleetAssoc[app.Name] = make(map[string]bool)
	}

	b.appFleetAssoc[app.Name][fleetName] = true

	return nil
}

// DisassociateApplicationFleet removes an application-fleet link. appID
// accepts either the application Name or its Arn, matching
// AssociateApplicationFleet.
func (b *InMemoryBackend) DisassociateApplicationFleet(appID, fleetName string) error {
	b.mu.Lock("DisassociateApplicationFleet")
	defer b.mu.Unlock()

	app, ok := b.findApplication(appID)
	if !ok {
		return ErrNotFound
	}

	if !b.fleets.Has(fleetName) {
		return ErrNotFound
	}

	if b.appFleetAssoc[app.Name] != nil {
		delete(b.appFleetAssoc[app.Name], fleetName)
	}

	return nil
}

// DescribeApplicationFleetAssociations returns application-fleet links.
// appID accepts either the application Name or its Arn, matching
// AssociateApplicationFleet. A non-matching filter yields an empty result
// (real AWS's Describe op has no ResourceNotFoundException).
func (b *InMemoryBackend) DescribeApplicationFleetAssociations(
	appID, fleetName string,
) ([]*ApplicationFleetAssociation, error) {
	b.mu.RLock("DescribeApplicationFleetAssociations")
	defer b.mu.RUnlock()

	targetName := ""

	if appID != "" {
		app, ok := b.findApplication(appID)
		if !ok {
			return nil, nil
		}

		targetName = app.Name
	}

	var result []*ApplicationFleetAssociation

	for aName, fleets := range b.appFleetAssoc {
		if targetName != "" && aName != targetName {
			continue
		}

		app, ok := b.applications.Get(aName)
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
