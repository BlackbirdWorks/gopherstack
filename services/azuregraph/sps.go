package azuregraph

import (
	"encoding/json"
	"strings"
)

var spReadOnly = []string{"id", "createdDateTime", "passwordCredentials", "keyCredentials"} //nolint:gochecknoglobals // fixed list

func (b *Backend) findSP(r Ref) (*ServicePrincipal, error) {
	switch r.Field {
	case "":
		if !isGUID(r.ID) {
			return nil, badRequest("Invalid object identifier '%s'.", r.ID)
		}

		if s, ok := b.sps.get(lc(r.ID)); ok {
			return s, nil
		}

		return nil, notFound(r.ID)
	case "appId":
		for _, s := range b.sps.all() {
			if strings.EqualFold(s.AppID, r.Value) {
				return s, nil
			}
		}

		return nil, notFound(r.Value)
	}

	return nil, badRequest("The alternate key '%s' is not supported on servicePrincipals.", r.Field)
}

// CreateServicePrincipal instantiates an application in the tenant; body.appId must name a registered application.
func (b *Backend) CreateServicePrincipal(body map[string]json.RawMessage) (ServicePrincipal, error) {
	b.mu.Lock("CreateServicePrincipal")
	defer b.mu.Unlock()

	sp := &ServicePrincipal{
		ID: newID(), CreatedDateTime: b.stamp(), AccountEnabled: true, ServicePrincipalType: "Application",
		AppOwnerOrganizationID: b.cfg.TenantID, SignInAudience: "AzureADMyOrg",
	}
	sp.normalize()

	sp, err := applyBody(sp, body, spReadOnly...)
	if err != nil {
		return ServicePrincipal{}, err
	}

	if sp.AppID == "" {
		return ServicePrincipal{}, badRequest("A value is required for property 'appId' of resource 'ServicePrincipal'.")
	}

	for _, o := range b.sps.all() {
		if strings.EqualFold(o.AppID, sp.AppID) {
			return ServicePrincipal{}, badRequest(
				"Another object with the same value for property servicePrincipalNames already exists.")
		}
	}

	app, err := b.findApp(Ref{Field: "appId", Value: sp.AppID})
	if err != nil {
		return ServicePrincipal{}, badRequest(
			"The appId '%s' of the service principal does not reference a valid application object.", sp.AppID)
	}

	sp.AppID, sp.DisplayName, sp.AppDisplayName = app.AppID, orDefault(sp.DisplayName, app.DisplayName), app.DisplayName
	sp.AppRoles, sp.OAuth2PermissionScopes, sp.SignInAudience = app.AppRoles, app.API.OAuth2PermissionScopes, app.SignInAudience
	sp.ServicePrincipalNames = append([]string{app.AppID}, app.IdentifierURIs...)
	b.sps.put(sp.ID, sp)

	return *sp, nil
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}

	return v
}

// GetServicePrincipal returns a service principal.
func (b *Backend) GetServicePrincipal(r Ref) (ServicePrincipal, error) {
	b.mu.RLock("GetServicePrincipal")
	defer b.mu.RUnlock()

	s, err := b.findSP(r)
	if err != nil {
		return ServicePrincipal{}, err
	}

	return *s, nil
}

// ListServicePrincipals returns every service principal in creation order.
func (b *Backend) ListServicePrincipals() []ServicePrincipal {
	b.mu.RLock("ListServicePrincipals")
	defer b.mu.RUnlock()

	return b.sps.slice()
}

// UpdateServicePrincipal applies a PATCH body.
func (b *Backend) UpdateServicePrincipal(r Ref, body map[string]json.RawMessage) error {
	b.mu.Lock("UpdateServicePrincipal")
	defer b.mu.Unlock()

	cur, err := b.findSP(r)
	if err != nil {
		return err
	}

	if isBuiltin(cur.ID) {
		return denied()
	}

	next, err := applyBody(cur, body, append([]string{"appId", "appOwnerOrganizationId"}, spReadOnly...)...)
	if err != nil {
		return err
	}

	b.sps.put(cur.ID, next)

	return nil
}

// DeleteServicePrincipal removes a service principal.
func (b *Backend) DeleteServicePrincipal(r Ref) error {
	b.mu.Lock("DeleteServicePrincipal")
	defer b.mu.Unlock()

	s, err := b.findSP(r)
	if err != nil {
		return err
	}

	if isBuiltin(s.ID) {
		return denied()
	}

	b.removeObject(s.ID)

	return nil
}
