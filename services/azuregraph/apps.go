package azuregraph

import (
	"encoding/json"
	"net/url"
	"slices"
	"strings"
)

var (
	signInAudiences = []string{ //nolint:gochecknoglobals // fixed enum
		"AzureADMyOrg", "AzureADMultipleOrgs", "AzureADandPersonalMicrosoftAccount", "PersonalMicrosoftAccount",
	}
	appReadOnly = []string{ //nolint:gochecknoglobals // fixed list
		"id", "appId", "createdDateTime", "passwordCredentials", "keyCredentials", "publisherDomain", "certification",
	}
)

// Ref addresses an object by ID or by an alternate key such as applications(appId='...').
type Ref struct {
	ID    string
	Field string
	Value string
}

func (b *Backend) findApp(r Ref) (*Application, error) {
	switch r.Field {
	case "":
		if !isGUID(r.ID) {
			return nil, badRequest("Invalid object identifier '%s'.", r.ID)
		}

		if a, ok := b.apps.get(lc(r.ID)); ok {
			return a, nil
		}

		return nil, notFound(r.ID)
	case "appId", "uniqueName":
		for _, a := range b.apps.all() {
			if (r.Field == "appId" && strings.EqualFold(a.AppID, r.Value)) ||
				(r.Field == "uniqueName" && a.UniqueName != "" && strings.EqualFold(a.UniqueName, r.Value)) {
				return a, nil
			}
		}

		return nil, notFound(r.Value)
	}

	return nil, badRequest("The alternate key '%s' is not supported on applications.", r.Field)
}

func (b *Backend) validateApp(a *Application, self string) error {
	if strings.TrimSpace(a.DisplayName) == "" {
		return badRequest("A value is required for property 'displayName' of resource 'Application'.")
	}

	if !slices.Contains(signInAudiences, a.SignInAudience) {
		return badRequest("Invalid value specified for property 'signInAudience' of resource 'Application'.")
	}

	for _, u := range a.IdentifierURIs {
		if p, err := url.Parse(u); err != nil || p.Scheme == "" || p.Host == "" {
			return badRequest("Invalid value specified for property 'identifierUris' of resource 'Application'.")
		}
	}

	for _, o := range b.apps.all() {
		if o.ID == self {
			continue
		}

		for _, u := range a.IdentifierURIs {
			if slices.ContainsFunc(o.IdentifierURIs, func(x string) bool { return strings.EqualFold(x, u) }) {
				return badRequest("Another object with the same value for property identifierUris already exists.")
			}
		}

		if a.UniqueName != "" && strings.EqualFold(o.UniqueName, a.UniqueName) {
			return badRequest("Another object with the same value for property uniqueName already exists.")
		}
	}

	return validateAppRoles(a.AppRoles, a.API.OAuth2PermissionScopes)
}

func validateAppRoles(roles []AppRole, scopes []PermissionScope) error {
	seen := map[string]bool{}

	for _, r := range roles {
		if !isGUID(r.ID) || seen[lc(r.ID)] {
			return badRequest("Property id value '%s' is invalid or not unique for resource 'AppRole'.", r.ID)
		}

		seen[lc(r.ID)] = true

		for _, t := range r.AllowedMemberTypes {
			if t != "User" && t != "Application" {
				return badRequest("Invalid value specified for property 'allowedMemberTypes' of resource 'AppRole'.")
			}
		}
	}

	for _, s := range scopes {
		if !isGUID(s.ID) || seen[lc(s.ID)] {
			return badRequest("Property id value '%s' is invalid or not unique for resource 'PermissionScope'.", s.ID)
		}

		seen[lc(s.ID)] = true

		if s.Type != "" && s.Type != "User" && s.Type != "Admin" {
			return badRequest("Invalid value specified for property 'type' of resource 'PermissionScope'.")
		}
	}

	return nil
}

// CreateApplication registers an application.
func (b *Backend) CreateApplication(body map[string]json.RawMessage) (Application, error) {
	b.mu.Lock("CreateApplication")
	defer b.mu.Unlock()

	seed := &Application{
		ID: newID(), AppID: newID(), CreatedDateTime: b.stamp(), SignInAudience: "AzureADMyOrg",
		PublisherDomain: b.domains[0].Name,
	}
	seed.normalize()

	a, err := applyBody(seed, body, appReadOnly...)
	if err != nil {
		return Application{}, err
	}

	if err = b.validateApp(a, a.ID); err != nil {
		return Application{}, err
	}

	b.apps.put(a.ID, a)

	return *a, nil
}

// GetApplication returns an application.
func (b *Backend) GetApplication(r Ref) (Application, error) {
	b.mu.RLock("GetApplication")
	defer b.mu.RUnlock()

	a, err := b.findApp(r)
	if err != nil {
		return Application{}, err
	}

	return *a, nil
}

// ListApplications returns every application in creation order.
func (b *Backend) ListApplications() []Application {
	b.mu.RLock("ListApplications")
	defer b.mu.RUnlock()

	return b.apps.slice()
}

// UpdateApplication applies a PATCH body.
func (b *Backend) UpdateApplication(r Ref, body map[string]json.RawMessage) error {
	b.mu.Lock("UpdateApplication")
	defer b.mu.Unlock()

	cur, err := b.findApp(r)
	if err != nil {
		return err
	}

	next, err := applyBody(cur, body, appReadOnly...)
	if err != nil {
		return err
	}

	if err = b.validateApp(next, cur.ID); err != nil {
		return err
	}

	b.apps.put(cur.ID, next)
	b.syncServicePrincipal(next)

	return nil
}

// DeleteApplication removes an application and its service principal.
func (b *Backend) DeleteApplication(r Ref) error {
	b.mu.Lock("DeleteApplication")
	defer b.mu.Unlock()

	a, err := b.findApp(r)
	if err != nil {
		return err
	}

	for _, sp := range b.sps.all() {
		if strings.EqualFold(sp.AppID, a.AppID) && !isBuiltin(sp.ID) {
			b.removeObject(sp.ID)
		}
	}

	b.removeObject(a.ID)

	return nil
}

// syncServicePrincipal propagates the application's published roles, scopes and names to its service principal.
func (b *Backend) syncServicePrincipal(a *Application) {
	for _, sp := range b.sps.all() {
		if !strings.EqualFold(sp.AppID, a.AppID) {
			continue
		}

		next := *sp
		next.AppRoles, next.OAuth2PermissionScopes = a.AppRoles, a.API.OAuth2PermissionScopes
		next.AppDisplayName, next.SignInAudience = a.DisplayName, a.SignInAudience
		next.ServicePrincipalNames = append([]string{a.AppID}, a.IdentifierURIs...)
		b.sps.put(sp.ID, &next)
	}
}

// removeObject deletes id and every reference to it. Callers hold b.mu.
func (b *Backend) removeObject(id string) {
	id = lc(id)

	for _, c := range b.credentialsOf(id) {
		delete(b.secrets, c)
	}

	b.apps.del(id)
	b.sps.del(id)
	b.users.del(id)
	b.groups.del(id)
	delete(b.owners, id)
	delete(b.members, id)

	for k, v := range b.owners {
		b.owners[k] = without(v, id)
	}

	for k, v := range b.members {
		b.members[k] = without(v, id)
	}

	for _, a := range b.assigns.all() {
		if lc(a.PrincipalID) == id || lc(a.ResourceID) == id {
			b.assigns.del(lc(a.ID))
		}
	}

	for _, g := range b.grants.all() {
		if lc(g.ClientID) == id || lc(g.ResourceID) == id || lc(g.PrincipalID) == id {
			b.grants.del(lc(g.ID))
		}
	}
}

func without(ids []string, id string) []string {
	return slices.DeleteFunc(slices.Clone(ids), func(x string) bool { return lc(x) == id })
}
