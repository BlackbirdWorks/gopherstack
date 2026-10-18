package appintegrations

import (
	"context"
	"maps"
	"slices"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const (
	appTypeStandard = "STANDARD"
	maxPermissions  = 100
)

var ()

// CreateApplicationInput is the CreateApplication request body.
type CreateApplicationInput struct {
	Tags                    map[string]string
	ApplicationSourceConfig *ApplicationSourceConfig
	ApplicationConfig       *ApplicationConfig
	IframeConfig            *IframeConfig
	AuthConfig              *AuthConfig
	InitializationTimeout   *int32
	Name                    string
	Namespace               string
	Description             string
	ApplicationType         string
	ClientToken             string
	Permissions             []string
	Publications            []Publication
	Subscriptions           []Subscription
	IsService               bool
}

// UpdateApplicationInput is the UpdateApplication request.
type UpdateApplicationInput struct {
	ApplicationSourceConfig *ApplicationSourceConfig
	ApplicationConfig       *ApplicationConfig
	IframeConfig            *IframeConfig
	AuthConfig              *AuthConfig
	InitializationTimeout   *int32
	IsService               *bool
	Name                    *string
	Description             *string
	ApplicationType         *string
	Arn                     string `json:"-" path:"Arn"`
	Permissions             []string
	Publications            []Publication
	Subscriptions           []Subscription
}

func cloneApp(a *Application) *Application {
	cp := *a
	cp.Tags = maps.Clone(a.Tags)

	return &cp
}

func checkApplicationParts(
	src *ApplicationSourceConfig, cfg *ApplicationConfig, auth *AuthConfig, perms []string,
) error {
	if src != nil && src.ExternalURLConfig != nil && src.ExternalURLConfig.AccessURL == "" {
		return invalidErr("ApplicationSourceConfig.ExternalUrlConfig.AccessUrl is required")
	}

	if auth != nil && auth.AuthType != "" && !slices.Contains(authTypes(), auth.AuthType) {
		return invalidErr("AuthConfig.AuthType must be one of %s", strings.Join(authTypes(), ", "))
	}

	if cfg != nil && cfg.ContactHandling != nil && !slices.Contains(scopes(), cfg.ContactHandling.Scope) {
		return invalidErr("ApplicationConfig.ContactHandling.Scope must be one of %s", strings.Join(scopes(), ", "))
	}

	if len(perms) > maxPermissions {
		return invalidErr("Permissions can hold at most %d entries", maxPermissions)
	}

	return nil
}

// CreateApplication registers an application under a unique namespace.
func (b *InMemoryBackend) CreateApplication(ctx context.Context, in *CreateApplicationInput) (*Application, error) {
	if err := checkName("Namespace", in.Namespace); err != nil {
		return nil, err
	}

	if in.Name == "" || len(in.Name) > maxNameLen {
		return nil, invalidErr("Name must be 1-%d characters", maxNameLen)
	}

	if in.ApplicationSourceConfig == nil {
		return nil, invalidErr("ApplicationSourceConfig is required")
	}

	if in.ApplicationType == "" {
		in.ApplicationType = appTypeStandard
	}

	if !slices.Contains(appTypes(), in.ApplicationType) {
		return nil, invalidErr("ApplicationType must be one of %s", strings.Join(appTypes(), ", "))
	}

	if err := checkApplicationParts(
		in.ApplicationSourceConfig,
		in.ApplicationConfig,
		in.AuthConfig,
		in.Permissions,
	); err != nil {
		return nil, err
	}

	if err := checkDescription(in.Description); err != nil {
		return nil, err
	}

	if err := checkTags(in.Tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateApplication")
	defer b.mu.Unlock()

	for _, a := range b.apps.All() {
		if a.Namespace == in.Namespace {
			return nil, duplicateErr("Application namespace", in.Namespace)
		}
	}

	now := awstime.NewEpochTime(b.now())
	id := uuid.NewString()
	a := &Application{
		CreatedTime:             now,
		LastModifiedTime:        now,
		ApplicationSourceConfig: in.ApplicationSourceConfig,
		ApplicationConfig:       in.ApplicationConfig,
		IframeConfig:            in.IframeConfig,
		AuthConfig:              in.AuthConfig,
		InitializationTimeout:   in.InitializationTimeout,
		Tags:                    maps.Clone(in.Tags),
		Arn:                     b.buildARN(ctx, "application/"+id),
		ID:                      id,
		Name:                    in.Name,
		Namespace:               in.Namespace,
		Description:             in.Description,
		ApplicationType:         in.ApplicationType,
		Permissions:             in.Permissions,
		Publications:            in.Publications,
		Subscriptions:           in.Subscriptions,
		IsService:               in.IsService,
	}
	b.apps.Put(a)

	return cloneApp(a), nil
}

func (b *InMemoryBackend) appLocked(identifier string) (*Application, error) {
	if a, ok := b.apps.Get(identifier); ok {
		return a, nil
	}

	for _, a := range b.apps.All() {
		if a.Arn == identifier {
			return a, nil
		}
	}

	return nil, notFoundErr("Application", identifier)
}

// GetApplication resolves an application ID or ARN.
func (b *InMemoryBackend) GetApplication(identifier string) (*Application, error) {
	b.mu.RLock("GetApplication")
	defer b.mu.RUnlock()

	a, err := b.appLocked(identifier)
	if err != nil {
		return nil, err
	}

	return cloneApp(a), nil
}

// ListApplications pages through applications ordered by namespace, optionally filtered by type.
func (b *InMemoryBackend) ListApplications(
	appType, token string,
	maxResults int32,
) ([]ApplicationSummary, string, error) {
	if appType != "" && !slices.Contains(appTypes(), appType) {
		return nil, "", invalidErr("applicationType must be one of %s", strings.Join(appTypes(), ", "))
	}

	b.mu.RLock("ListApplications")
	var all []ApplicationSummary

	b.apps.Range(func(a *Application) bool {
		if appType == "" || a.ApplicationType == appType {
			all = append(all, ApplicationSummary{
				CreatedTime: a.CreatedTime, LastModifiedTime: a.LastModifiedTime, ApplicationType: a.ApplicationType,
				Arn: a.Arn, ID: a.ID, Name: a.Name, Namespace: a.Namespace, IsService: a.IsService,
			})
		}

		return true
	})
	b.mu.RUnlock()

	slices.SortFunc(all, func(a, c ApplicationSummary) int { return strings.Compare(a.Namespace, c.Namespace) })

	return page(all, token, maxResults)
}

// UpdateApplication replaces the supplied settings of an application.
func (b *InMemoryBackend) UpdateApplication(in *UpdateApplicationInput) error {
	if err := checkApplicationParts(
		in.ApplicationSourceConfig,
		in.ApplicationConfig,
		in.AuthConfig,
		in.Permissions,
	); err != nil {
		return err
	}

	if in.ApplicationType != nil && !slices.Contains(appTypes(), *in.ApplicationType) {
		return invalidErr("ApplicationType must be one of %s", strings.Join(appTypes(), ", "))
	}

	if in.Description != nil {
		if err := checkDescription(*in.Description); err != nil {
			return err
		}
	}

	b.mu.Lock("UpdateApplication")
	defer b.mu.Unlock()

	a, err := b.appLocked(in.Arn)
	if err != nil {
		return err
	}

	setIf(&a.Name, in.Name)
	setIf(&a.Description, in.Description)
	setIf(&a.ApplicationType, in.ApplicationType)
	setIf(&a.IsService, in.IsService)

	if in.ApplicationSourceConfig != nil {
		a.ApplicationSourceConfig = in.ApplicationSourceConfig
	}

	if in.ApplicationConfig != nil {
		a.ApplicationConfig = in.ApplicationConfig
	}

	if in.IframeConfig != nil {
		a.IframeConfig = in.IframeConfig
	}

	if in.AuthConfig != nil {
		a.AuthConfig = in.AuthConfig
	}

	if in.InitializationTimeout != nil {
		a.InitializationTimeout = in.InitializationTimeout
	}

	if in.Permissions != nil {
		a.Permissions = in.Permissions
	}

	if in.Publications != nil {
		a.Publications = in.Publications
	}

	if in.Subscriptions != nil {
		a.Subscriptions = in.Subscriptions
	}

	a.LastModifiedTime = awstime.NewEpochTime(b.now())

	return nil
}

func setIf[T any](dst *T, src *T) {
	if src != nil {
		*dst = *src
	}
}

// DeleteApplication removes an application; force also removes its associations.
func (b *InMemoryBackend) DeleteApplication(identifier string, force bool) error {
	b.mu.Lock("DeleteApplication")
	defer b.mu.Unlock()

	a, err := b.appLocked(identifier)
	if err != nil {
		return err
	}

	var assocs []string

	for _, as := range b.appAssocs.All() {
		if as.ApplicationID == a.ID {
			assocs = append(assocs, as.AssociationID)
		}
	}

	if len(assocs) > 0 && !force {
		return conflictErr("Application %q has associations; delete them or use force", identifier)
	}

	for _, id := range assocs {
		b.appAssocs.Delete(id)
	}

	b.apps.Delete(a.ID)

	return nil
}

// CreateApplicationAssociation links an application to a client such as a Connect instance.
func (b *InMemoryBackend) CreateApplicationAssociation(
	ctx context.Context, applicationIdentifier, clientID string,
) (*ApplicationAssociation, error) {
	b.mu.Lock("CreateApplicationAssociation")
	defer b.mu.Unlock()

	app, err := b.appLocked(applicationIdentifier)
	if err != nil {
		return nil, err
	}

	for _, as := range b.appAssocs.All() {
		if as.ApplicationID == app.ID && as.ClientID == clientID {
			return nil, duplicateErr("ApplicationAssociation", clientID)
		}
	}

	id := uuid.NewString()
	as := &ApplicationAssociation{
		ApplicationArn:            app.Arn,
		ApplicationAssociationArn: b.buildARN(ctx, "application/"+app.ID+"/association/"+id),
		ApplicationID:             app.ID,
		ClientID:                  clientID,
		AssociationID:             id,
	}
	b.appAssocs.Put(as)

	cp := *as

	return &cp, nil
}

// DeleteApplicationAssociation removes an application association.
func (b *InMemoryBackend) DeleteApplicationAssociation(id string) error {
	b.mu.Lock("DeleteApplicationAssociation")
	defer b.mu.Unlock()

	if !b.appAssocs.Delete(id) {
		return notFoundErr("ApplicationAssociation", id)
	}

	return nil
}

// ListApplicationAssociations pages through the associations of one application.
func (b *InMemoryBackend) ListApplicationAssociations(
	identifier, token string, maxResults int32,
) ([]ApplicationAssociationSummary, string, error) {
	b.mu.RLock("ListApplicationAssociations")

	app, err := b.appLocked(identifier)
	if err != nil {
		b.mu.RUnlock()

		return nil, "", err
	}

	var all []ApplicationAssociationSummary

	for _, as := range b.appAssocs.All() {
		if as.ApplicationID == app.ID {
			all = append(all, ApplicationAssociationSummary{
				ApplicationArn:            as.ApplicationArn,
				ApplicationAssociationArn: as.ApplicationAssociationArn,
				ClientID:                  as.ClientID,
			})
		}
	}

	b.mu.RUnlock()

	slices.SortFunc(all, func(a, c ApplicationAssociationSummary) int {
		return strings.Compare(a.ApplicationAssociationArn, c.ApplicationAssociationArn)
	})

	return page(all, token, maxResults)
}

func appTypes() []string {
	return []string{
		appTypeStandard,
		"SERVICE",
		"MCP_SERVER",
		"A2A_SERVER",
	}
}

func authTypes() []string {
	return []string{
		"API_KEY",
		"CONNECT_JWT",
	}
}

func scopes() []string {
	return []string{
		"CROSS_CONTACTS",
		"PER_CONTACT",
	}
}
