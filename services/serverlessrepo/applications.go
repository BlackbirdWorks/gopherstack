package serverlessrepo

import (
	"fmt"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// cloneApplication returns a deep copy of a, including its Labels slice.
func cloneApplication(a *Application) *Application {
	cp := *a
	cp.Labels = cloneStringSlice(a.Labels)

	return &cp
}

// AddApplicationInternal creates an application directly in the backend, bypassing
// validation and HTTP. Useful for seeding test state.
func (b *InMemoryBackend) AddApplicationInternal(name, description, author string) *Application {
	b.mu.Lock("AddApplicationInternal")
	defer b.mu.Unlock()

	appARN := arn.Build("serverlessrepo", b.region, b.accountID, "applications/"+name)
	a := &Application{
		ApplicationID: appARN,
		Name:          name,
		Description:   description,
		Author:        author,
		CreationTime:  time.Now(),
	}
	b.applications.Put(a)

	return cloneApplication(a)
}

// CreateApplication creates a new application.
func (b *InMemoryBackend) CreateApplication(
	name string,
	description string,
	author string,
	sourceCodeURL string,
	semanticVersion string,
	labels []string,
	homePageURL string,
	licenseURL string,
	spdxLicenseID string,
) (*Application, error) {
	b.mu.Lock("CreateApplication")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	if !validNameRe.MatchString(name) {
		return nil, fmt.Errorf("%w: name must contain only alphanumeric characters and hyphens", ErrValidation)
	}

	if len(name) > maxNameLength {
		return nil, fmt.Errorf("%w: name must be at most %d characters", ErrValidation, maxNameLength)
	}

	if author == "" {
		return nil, fmt.Errorf("%w: author is required", ErrValidation)
	}

	if len(author) > maxAuthorLength {
		return nil, fmt.Errorf("%w: author must be at most %d characters", ErrValidation, maxAuthorLength)
	}

	if description == "" {
		return nil, fmt.Errorf("%w: description is required", ErrValidation)
	}

	if len(description) > maxDescriptionLength {
		return nil, fmt.Errorf("%w: description must be at most %d characters", ErrValidation, maxDescriptionLength)
	}

	if semanticVersion != "" && !isValidSemanticVersion(semanticVersion) {
		return nil, fmt.Errorf("%w: semanticVersion must be a valid semantic version (e.g. 1.0.0)", ErrValidation)
	}

	if err := validateLabels(labels); err != nil {
		return nil, err
	}

	if b.applications.Has(name) {
		return nil, fmt.Errorf("%w: application %s already exists", ErrApplicationAlreadyExists, name)
	}

	appARN := arn.Build("serverlessrepo", b.region, b.accountID, "applications/"+name)

	a := &Application{
		ApplicationID:   appARN,
		Name:            name,
		Description:     description,
		Author:          author,
		SourceCodeURL:   sourceCodeURL,
		SemanticVersion: semanticVersion,
		HomePageURL:     homePageURL,
		LicenseURL:      licenseURL,
		SpdxLicenseID:   spdxLicenseID,
		CreationTime:    time.Now(),
		Labels:          nonNilStringSlice(cloneStringSlice(labels)),
	}
	b.applications.Put(a)

	return cloneApplication(a), nil
}

// SetApplicationReadmeURL stores readme metadata supplied during application creation.
func (b *InMemoryBackend) SetApplicationReadmeURL(name, readmeURL string) (*Application, error) {
	b.mu.Lock("SetApplicationReadmeURL")
	defer b.mu.Unlock()

	a, ok := b.applications.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, name)
	}

	a.ReadmeURL = readmeURL

	return cloneApplication(a), nil
}

// GetApplication returns an application by name.
func (b *InMemoryBackend) GetApplication(name string) (*Application, error) {
	b.mu.RLock("GetApplication")
	defer b.mu.RUnlock()

	a, ok := b.applications.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, name)
	}

	return cloneApplication(a), nil
}

// ListApplications returns all applications sorted by name.
func (b *InMemoryBackend) ListApplications() []*Application {
	b.mu.RLock("ListApplications")
	defer b.mu.RUnlock()

	// Table.Snapshot returns entries ordered by key ascending; the table is
	// keyed by Application.Name (see store_setup.go), which is exactly the
	// sort order this method has always returned.
	snap := b.applications.Snapshot()
	list := make([]*Application, 0, len(snap))

	for _, a := range snap {
		list = append(list, cloneApplication(a))
	}

	return list
}

// UpdateApplication updates the description or author of an existing application.
func (b *InMemoryBackend) UpdateApplication(
	name, description, author, homePageURL, readmeURL string,
) (*Application, error) {
	b.mu.Lock("UpdateApplication")
	defer b.mu.Unlock()

	a, ok := b.applications.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, name)
	}

	if description != "" {
		if len(description) > maxDescriptionLength {
			return nil, fmt.Errorf("%w: description must be at most %d characters", ErrValidation, maxDescriptionLength)
		}

		a.Description = description
	}

	if author != "" {
		if len(author) > maxAuthorLength {
			return nil, fmt.Errorf("%w: author must be at most %d characters", ErrValidation, maxAuthorLength)
		}

		a.Author = author
	}

	if homePageURL != "" {
		a.HomePageURL = homePageURL
	}

	if readmeURL != "" {
		a.ReadmeURL = readmeURL
	}

	return cloneApplication(a), nil
}

// UpdateApplicationLabels replaces labels supplied by UpdateApplication.
func (b *InMemoryBackend) UpdateApplicationLabels(name string, labels []string) (*Application, error) {
	b.mu.Lock("UpdateApplicationLabels")
	defer b.mu.Unlock()

	a, ok := b.applications.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, name)
	}

	if err := validateLabels(labels); err != nil {
		return nil, err
	}

	a.Labels = nonNilStringSlice(cloneStringSlice(labels))

	return cloneApplication(a), nil
}

// DeleteApplication deletes an application by name, cascading to every
// version, CloudFormation template, and CloudFormation change set owned by
// it. The byApp index results are cloned before the delete loops since
// deleting from the underlying table mutates the index's backing slice.
func (b *InMemoryBackend) DeleteApplication(name string) error {
	b.mu.Lock("DeleteApplication")
	defer b.mu.Unlock()

	if !b.applications.Has(name) {
		return fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, name)
	}

	b.applications.Delete(name)

	for _, v := range slices.Clone(b.appVersionsByApp.Get(name)) {
		b.appVersions.Delete(versionKey(v.AppName, v.SemanticVersion))
	}

	for _, t := range slices.Clone(b.cfTemplatesByApp.Get(name)) {
		b.cfTemplates.Delete(t.TemplateID)
	}

	for _, cs := range slices.Clone(b.cfChangeSetsByApp.Get(name)) {
		b.cfChangeSets.Delete(changeSetKey(cs.AppName, cs.ChangeSetID))
	}

	delete(b.appPolicies, name)
	delete(b.appDependencies, name)

	return nil
}

// UnshareApplication removes an application from an AWS Organization.
func (b *InMemoryBackend) UnshareApplication(appName, _ string) error {
	b.mu.Lock("UnshareApplication")
	defer b.mu.Unlock()

	if !b.applications.Has(appName) {
		return fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, appName)
	}

	return nil
}
