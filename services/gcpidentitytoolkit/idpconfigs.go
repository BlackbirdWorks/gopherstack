package gcpidentitytoolkit

import (
	"net/http"
	"slices"
	"strings"

	idtkv2 "google.golang.org/api/identitytoolkit/v2"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	collDefaultIdp  = "defaultSupportedIdpConfigs"
	collOAuthIdp    = "oauthIdpConfigs"
	collSamlIdp     = "inboundSamlConfigs"
	idpPageSize     = 20
	opIdpConfigs    = "IdpConfigs"
	opSignInWithIdp = "signInWithIdp"
	oidcPrefix      = "oidc."
	samlPrefix      = "saml."
)

type (
	defaultIdp = idtkv2.GoogleCloudIdentitytoolkitAdminV2DefaultSupportedIdpConfig
	oauthIdp   = idtkv2.GoogleCloudIdentitytoolkitAdminV2OAuthIdpConfig
	samlIdp    = idtkv2.GoogleCloudIdentitytoolkitAdminV2InboundSamlConfig
)

// idpProviders are the ids accepted by defaultSupportedIdpConfigs.
//
//nolint:gochecknoglobals // immutable catalog
var idpProviders = []string{
	"apple.com", "facebook.com", "gc.apple.com", "github.com", "google.com", "linkedin.com", "microsoft.com",
	"playgames.google.com", "twitter.com", "yahoo.com",
}

func idpKey(tenant, id string) string { return tenant + "/" + id }

func idpName(project, tenant, coll, id string) string {
	if tenant == rootTenantKey {
		return "projects/" + project + "/" + coll + "/" + id
	}

	return "projects/" + project + "/tenants/" + tenant + "/" + coll + "/" + id
}

type idpSpec[T any] struct {
	store    func(*projectRec) *map[string]*T
	name     func(*T) *string
	validate func(id string, in *T) error
	coll     string
}

func idpScope(b *InMemoryBackend, project, tenant string) (*projectRec, error) {
	p := b.projectLocked(project)
	if tenant != rootTenantKey {
		if _, ok := p.Tenants[tenant]; !ok {
			return nil, errNotFound("TENANT_NOT_FOUND")
		}
	}

	return p, nil
}

func idpCreate[T any](
	b *InMemoryBackend, sp idpSpec[T], project, tenant, id string, in *T,
) (*T, error) {
	if err := sp.validate(id, in); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateIdpConfig")
	defer b.mu.Unlock()

	p, err := idpScope(b, project, tenant)
	if err != nil {
		return nil, err
	}

	m := sp.store(p)
	if *m == nil {
		*m = map[string]*T{}
	}

	if _, taken := (*m)[idpKey(tenant, id)]; taken {
		return nil, &APIError{HTTP: http.StatusConflict, Status: "ALREADY_EXISTS", Message: "CONFIGURATION_EXISTS"}
	}

	rec := cloneJSON(in)
	*sp.name(rec) = idpName(project, tenant, sp.coll, id)
	(*m)[idpKey(tenant, id)] = rec

	return cloneJSON(rec), nil
}

func idpGet[T any](b *InMemoryBackend, sp idpSpec[T], project, tenant, id string) (*T, error) {
	b.mu.Lock("GetIdpConfig")
	defer b.mu.Unlock()

	p, err := idpScope(b, project, tenant)
	if err != nil {
		return nil, err
	}

	rec, ok := (*sp.store(p))[idpKey(tenant, id)]
	if !ok {
		return nil, errNotFound("CONFIGURATION_NOT_FOUND")
	}

	return cloneJSON(rec), nil
}

func idpUpdate[T any](
	b *InMemoryBackend, sp idpSpec[T], project, tenant, id string, patch map[string]any, mask string,
) (*T, error) {
	b.mu.Lock("UpdateIdpConfig")
	defer b.mu.Unlock()

	p, err := idpScope(b, project, tenant)
	if err != nil {
		return nil, err
	}

	m := sp.store(p)

	cur, ok := (*m)[idpKey(tenant, id)]
	if !ok {
		return nil, errNotFound("CONFIGURATION_NOT_FOUND")
	}

	next, err := patched(cur, maskedPatch(patch, mask))
	if err != nil {
		return nil, err
	}

	if err = sp.validate(id, next); err != nil {
		return nil, err
	}

	*sp.name(next) = *sp.name(cur)
	(*m)[idpKey(tenant, id)] = next

	return cloneJSON(next), nil
}

func idpDelete[T any](b *InMemoryBackend, sp idpSpec[T], project, tenant, id string) error {
	b.mu.Lock("DeleteIdpConfig")
	defer b.mu.Unlock()

	p, err := idpScope(b, project, tenant)
	if err != nil {
		return err
	}

	m := sp.store(p)
	if _, ok := (*m)[idpKey(tenant, id)]; !ok {
		return errNotFound("CONFIGURATION_NOT_FOUND")
	}

	delete(*m, idpKey(tenant, id))

	return nil
}

func idpList[T any](
	b *InMemoryBackend, sp idpSpec[T], project, tenant, token string, size int,
) (page.Page[*T], error) {
	if err := page.ValidateToken(token); err != nil {
		return page.Page[*T]{}, errInvalidArg("Invalid page token.")
	}

	b.mu.Lock("ListIdpConfigs")
	defer b.mu.Unlock()

	p, err := idpScope(b, project, tenant)
	if err != nil {
		return page.Page[*T]{}, err
	}

	m := *sp.store(p)
	keys := make([]string, 0, len(m))

	for k := range m {
		if strings.HasPrefix(k, tenant+"/") {
			keys = append(keys, k)
		}
	}

	slices.Sort(keys)

	all := make([]*T, 0, len(keys))
	for _, k := range keys {
		all = append(all, cloneJSON(m[k]))
	}

	return page.New(all, token, size, idpPageSize), nil
}

func requireField(name, v string) error {
	if v == "" {
		return errInvalidArg("%s is required.", name)
	}

	return nil
}

func firstErr(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}

	return nil
}

//nolint:gochecknoglobals // immutable specs
var (
	defaultIdpSpec = idpSpec[defaultIdp]{
		coll:  collDefaultIdp,
		store: func(p *projectRec) *map[string]*defaultIdp { return &p.DefaultIdps },
		name:  func(c *defaultIdp) *string { return &c.Name },
		validate: func(id string, in *defaultIdp) error {
			if !slices.Contains(idpProviders, id) {
				return errInvalidArg("Invalid default supported IdP id %q.", id)
			}

			return requireField("clientId", in.ClientId)
		},
	}
	oauthIdpSpec = idpSpec[oauthIdp]{
		coll:  collOAuthIdp,
		store: func(p *projectRec) *map[string]*oauthIdp { return &p.OAuthIdps },
		name:  func(c *oauthIdp) *string { return &c.Name },
		validate: func(id string, in *oauthIdp) error {
			if !strings.HasPrefix(id, oidcPrefix) || len(id) == len(oidcPrefix) {
				return errInvalidArg("oauthIdpConfigId must start with %q.", oidcPrefix)
			}

			return firstErr(requireField("clientId", in.ClientId), requireField("issuer", in.Issuer))
		},
	}
	samlIdpSpec = idpSpec[samlIdp]{
		coll:  collSamlIdp,
		store: func(p *projectRec) *map[string]*samlIdp { return &p.SamlIdps },
		name:  func(c *samlIdp) *string { return &c.Name },
		validate: func(id string, in *samlIdp) error {
			if !strings.HasPrefix(id, samlPrefix) || len(id) == len(samlPrefix) {
				return errInvalidArg("inboundSamlConfigId must start with %q.", samlPrefix)
			}

			if in.IdpConfig == nil || in.SpConfig == nil {
				return errInvalidArg("idpConfig and spConfig are required.")
			}

			return firstErr(
				requireField("idpConfig.idpEntityId", in.IdpConfig.IdpEntityId),
				requireField("idpConfig.ssoUrl", in.IdpConfig.SsoUrl),
				requireField("spConfig.spEntityId", in.SpConfig.SpEntityId),
				requireField("spConfig.callbackUri", in.SpConfig.CallbackUri),
			)
		},
	}
)

func deleteTenantIdps(p *projectRec, tenant string) {
	prefix := tenant + "/"

	dropPrefix(p.DefaultIdps, prefix)
	dropPrefix(p.OAuthIdps, prefix)
	dropPrefix(p.SamlIdps, prefix)
}

func dropPrefix[T any](m map[string]*T, prefix string) {
	for k := range m {
		if strings.HasPrefix(k, prefix) {
			delete(m, k)
		}
	}
}
