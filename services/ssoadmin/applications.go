package ssoadmin

import (
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// validateApplicationProviderArn validates an application provider ARN.
// Accepts AWS-managed (arn:aws:sso::aws:applicationProvider/...) or
// account-scoped (arn:aws:sso::{accountId}:applicationProvider/...) ARNs.
func validateApplicationProviderArn(arn string) error {
	if arn == "" {
		return fmt.Errorf("%w: ApplicationProviderArn is required", awserr.ErrInvalidParameter)
	}
	if !strings.HasPrefix(arn, "arn:aws:sso::") {
		return fmt.Errorf("%w: ApplicationProviderArn must be a valid SSO ARN", awserr.ErrInvalidParameter)
	}
	if !strings.Contains(arn, ":applicationProvider/") {
		return fmt.Errorf("%w: ApplicationProviderArn must reference an applicationProvider resource",
			awserr.ErrInvalidParameter)
	}

	return nil
}

// copyApplication returns a deep copy of an Application. Must be called with mu held.
func copyApplication(app *Application) *Application {
	cp := *app
	cp.Tags = make(map[string]string, len(app.Tags))
	maps.Copy(cp.Tags, app.Tags)
	if app.PortalOptions != nil {
		po := *app.PortalOptions
		cp.PortalOptions = &po
	}

	return &cp
}

// AddApplicationInternal adds a pre-built Application directly to the backend for test seeding.
func (b *InMemoryBackend) AddApplicationInternal(instanceArn, name string) *Application {
	b.mu.Lock("AddApplicationInternal")
	defer b.mu.Unlock()

	id := uuid.NewString()[:uuidShortLen]
	instanceID := instanceARNToID(instanceArn)
	appArn := arn.Build("sso", "", b.accountID, fmt.Sprintf("application/%s/apl-%s", instanceID, id))
	app := &Application{
		ApplicationArn:         appArn,
		ApplicationProviderArn: appProviderCustom,
		InstanceArn:            instanceArn,
		Name:                   name,
		Status:                 appStatusEnabled,
		CreatedDate:            time.Now().UTC(),
		Tags:                   make(map[string]string),
		ApplicationAccount:     b.accountID,
		CreatedFrom:            b.region,
		IdentityStoreArn:       b.identityStoreArn(instanceArn),
	}
	b.applications.Put(app)

	return copyApplication(app)
}

// CreateApplication creates a new application within an SSO instance.
func (b *InMemoryBackend) CreateApplication(
	instanceArn, applicationProviderArn, name, description string,
	tags map[string]string,
	portalOptions *PortalOptions,
) (*Application, error) {
	b.mu.Lock("CreateApplication")
	defer b.mu.Unlock()

	if err := validateApplicationProviderArn(applicationProviderArn); err != nil {
		return nil, err
	}

	if !b.instances.Has(instanceArn) {
		return nil, ErrInstanceNotFound
	}
	for _, app := range b.applicationsByInstance.Get(instanceArn) {
		if app.Name == name {
			return nil, ErrApplicationAlreadyExists
		}
	}

	if portalOptions != nil {
		if portalOptions.Visibility != "" &&
			portalOptions.Visibility != portalVisibilityEnabled &&
			portalOptions.Visibility != portalVisibilityDisabled {
			return nil, fmt.Errorf(
				"%w: PortalOptions.Visibility must be ENABLED or DISABLED",
				awserr.ErrInvalidParameter,
			)
		}
		if portalOptions.SignInOptions.Origin != "" &&
			portalOptions.SignInOptions.Origin != signInOriginApplication &&
			portalOptions.SignInOptions.Origin != signInOriginIdentityCenter {
			return nil, fmt.Errorf("%w: SignInOptions.Origin must be APPLICATION or IDENTITY_CENTER",
				awserr.ErrInvalidParameter)
		}
		if portalOptions.SignInOptions.Origin == signInOriginApplication &&
			portalOptions.SignInOptions.ApplicationURL == "" {
			return nil, fmt.Errorf("%w: SignInOptions.ApplicationUrl is required when Origin is APPLICATION",
				awserr.ErrInvalidParameter)
		}
	}

	id := uuid.NewString()[:uuidShortLen]
	instanceID := instanceARNToID(instanceArn)
	appArn := arn.Build("sso", "", b.accountID, fmt.Sprintf("application/%s/apl-%s", instanceID, id))
	app := &Application{
		ApplicationArn:         appArn,
		ApplicationProviderArn: applicationProviderArn,
		CreatedDate:            time.Now().UTC(),
		Description:            description,
		InstanceArn:            instanceArn,
		Name:                   name,
		Status:                 appStatusEnabled,
		Tags:                   make(map[string]string),
		PortalOptions:          portalOptions,
		ApplicationAccount:     b.accountID,
		CreatedFrom:            b.region,
		IdentityStoreArn:       b.identityStoreArn(instanceArn),
	}
	maps.Copy(app.Tags, tags)
	b.applications.Put(app)

	return copyApplication(app), nil
}

// identityStoreArn builds the ARN of the identity store connected to the
// given instance, matching the real ssoadmin.types.Application /
// DescribeApplicationOutput "IdentityStoreArn" wire field. Returns "" if the
// instance can't be found (should not happen for a validated instanceArn).
func (b *InMemoryBackend) identityStoreArn(instanceArn string) string {
	inst, ok := b.instances.Get(instanceArn)
	if !ok || inst.IdentityStoreID == "" {
		return ""
	}

	return arn.Build("identitystore", "", b.accountID, "identitystore/"+inst.IdentityStoreID)
}

// DescribeApplication returns an application by ARN.
func (b *InMemoryBackend) DescribeApplication(applicationArn string) (*Application, error) {
	b.mu.RLock("DescribeApplication")
	defer b.mu.RUnlock()

	app, ok := b.applications.Get(applicationArn)
	if !ok {
		return nil, ErrApplicationNotFound
	}

	return copyApplication(app), nil
}

// ListApplications returns applications for an instance.
func (b *InMemoryBackend) ListApplications(instanceArn string) []*Application {
	b.mu.RLock("ListApplications")
	defer b.mu.RUnlock()

	if instanceArn != "" {
		grouped := b.applicationsByInstance.Get(instanceArn)
		result := make([]*Application, 0, len(grouped))
		for _, app := range grouped {
			result = append(result, copyApplication(app))
		}

		return result
	}

	result := make([]*Application, 0, b.applications.Len())
	for _, app := range b.applications.All() {
		result = append(result, copyApplication(app))
	}

	return result
}

// UpdateApplication updates mutable fields on an application.
//
// UpdateApplicationInput.PortalOptions is types.UpdateApplicationPortalOptions,
// which declares only SignInOptions -- unlike CreateApplicationInput's
// types.PortalOptions, it has no Visibility. A real client's Update payload
// can therefore never carry Visibility, so only SignInOptions is merged;
// Visibility (and anything else already on the application's PortalOptions)
// survives untouched.
func (b *InMemoryBackend) UpdateApplication(
	applicationArn, name, description, status string,
	signInOptions *SignInOptions,
) (*Application, error) {
	b.mu.Lock("UpdateApplication")
	defer b.mu.Unlock()

	if status != "" && status != appStatusEnabled && status != appStatusDisabled {
		return nil, fmt.Errorf("%w: Application Status must be ENABLED or DISABLED", awserr.ErrInvalidParameter)
	}

	app, ok := b.applications.Get(applicationArn)
	if !ok {
		return nil, ErrApplicationNotFound
	}
	if name != "" {
		app.Name = name
	}
	if description != "" {
		app.Description = description
	}
	if status != "" {
		app.Status = status
	}
	if signInOptions != nil {
		if app.PortalOptions == nil {
			app.PortalOptions = &PortalOptions{}
		}

		app.PortalOptions.SignInOptions = *signInOptions
	}

	return copyApplication(app), nil
}

// DeleteApplication deletes an application.
func (b *InMemoryBackend) DeleteApplication(applicationArn string) error {
	b.mu.Lock("DeleteApplication")
	defer b.mu.Unlock()

	if !b.applications.Has(applicationArn) {
		return ErrApplicationNotFound
	}
	b.applications.Delete(applicationArn)
	delete(b.applicationAssignments, applicationArn)
	delete(b.applicationScopes, applicationArn)
	delete(b.applicationAuthMethods, applicationArn)
	delete(b.applicationGrants, applicationArn)
	delete(b.applicationAssignConfig, applicationArn)
	delete(b.applicationSessions, applicationArn)

	return nil
}
