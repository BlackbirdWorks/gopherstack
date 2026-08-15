package appstream

import (
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

type storedApplication struct {
	CreatedTime      time.Time         `json:"createdTime"`
	Tags             map[string]string `json:"tags"`
	Name             string            `json:"name"`
	Arn              string            `json:"arn"`
	DisplayName      string            `json:"displayName"`
	Description      string            `json:"description"`
	LaunchPath       string            `json:"launchPath"`
	AppBlockArn      string            `json:"appBlockArn"`
	Platforms        []string          `json:"platforms"`
	IconS3Location   S3Location        `json:"iconS3Location"`
	InstanceFamilies []string          `json:"instanceFamilies"`
}

func (a *storedApplication) toApplication() *Application {
	tags := make(map[string]string)
	maps.Copy(tags, a.Tags)

	platforms := make([]string, len(a.Platforms))
	copy(platforms, a.Platforms)

	instanceFamilies := make([]string, len(a.InstanceFamilies))
	copy(instanceFamilies, a.InstanceFamilies)

	return &Application{
		CreatedTime:      a.CreatedTime,
		Tags:             tags,
		Platforms:        platforms,
		Name:             a.Name,
		Arn:              a.Arn,
		DisplayName:      a.DisplayName,
		Description:      a.Description,
		LaunchPath:       a.LaunchPath,
		AppBlockArn:      a.AppBlockArn,
		IconS3Location:   a.IconS3Location,
		InstanceFamilies: instanceFamilies,
	}
}

func (b *InMemoryBackend) applicationARN(name string) string {
	return arn.Build("appstream", b.region, b.accountID, fmt.Sprintf("application/%s", name))
}

// CreateApplication creates a new application. iconS3Location and
// instanceFamilies are required members (api_op_CreateApplication.go:47,53).
// IconS3Location.S3Key is only conditionally required by the shared
// S3Location shape, but that condition is satisfied here: it is required
// specifically "for IconS3Location (Actions: CreateApplication and
// UpdateApplication)" (appstream@v1.64.5 types/types.go:1434-1451).
func (b *InMemoryBackend) CreateApplication(
	name, displayName, description, launchPath, appBlockArn string,
	platforms []string, iconS3Location S3Location, instanceFamilies []string,
	tags map[string]string,
) (*Application, error) {
	if iconS3Location.S3Bucket == "" || iconS3Location.S3Key == "" {
		return nil, fmt.Errorf("%w: IconS3Location is required", ErrSerialization)
	}

	if len(instanceFamilies) == 0 {
		return nil, fmt.Errorf("%w: InstanceFamilies is required", ErrSerialization)
	}

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

	families := make([]string, len(instanceFamilies))
	copy(families, instanceFamilies)

	app := &storedApplication{
		CreatedTime:      time.Now().UTC(),
		Tags:             storedTags,
		Platforms:        ps,
		Name:             name,
		Arn:              arn,
		DisplayName:      displayName,
		Description:      description,
		LaunchPath:       launchPath,
		AppBlockArn:      appBlockArn,
		IconS3Location:   iconS3Location,
		InstanceFamilies: families,
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

// DescribeAppLicenseUsage returns license usage records. This backend tracks
// no BYOL/license-included application state, so the real (not fabricated)
// answer is always an empty list -- matching what real AWS returns for an
// account with no licensed application usage to report.
func (b *InMemoryBackend) DescribeAppLicenseUsage() ([]map[string]string, error) {
	return []map[string]string{}, nil
}

// AssociateApplicationFleet links an application to a fleet and returns the
// association. appID accepts either the application Name or its Arn -- real
// AWS's AssociateApplicationFleet request carries the ApplicationArn. The
// real AssociateApplicationFleetOutput carries the ApplicationFleetAssociation
// itself (deserializeCBOR_AssociateApplicationFleetOutput in the pinned
// appstream SDK's deserializers.go), not an empty envelope.
func (b *InMemoryBackend) AssociateApplicationFleet(appID, fleetName string) (*ApplicationFleetAssociation, error) {
	b.mu.Lock("AssociateApplicationFleet")
	defer b.mu.Unlock()

	app, ok := b.findApplication(appID)
	if !ok {
		return nil, ErrNotFound
	}

	if !b.fleets.Has(fleetName) {
		return nil, ErrNotFound
	}

	if b.appFleetAssoc[app.Name] == nil {
		b.appFleetAssoc[app.Name] = make(map[string]bool)
	}

	b.appFleetAssoc[app.Name][fleetName] = true

	return &ApplicationFleetAssociation{
		ApplicationArn: app.Arn,
		FleetName:      fleetName,
		State:          associationStateActive,
	}, nil
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
				State:          associationStateActive,
			})
		}
	}

	return result, nil
}
