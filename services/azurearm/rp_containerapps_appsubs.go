package azurearm

import (
	"net/url"
	"strings"
)

const (
	acaAuthType       = "authConfigs"
	acaSourceType     = "sourcecontrols"
	acaCurrent        = "current"
	fieldLogin        = "login"
	acaActionRedirect = "RedirectToLoginPage"

	acaFieldRegistry   = "registryPassword"
	acaFieldClientSec  = "clientSecret"
	acaFieldActionCfg  = "githubActionConfiguration"
	acaFieldAzureCreds = "azureCredentials"
	acaFieldRegInfo    = "registryInfo"
)

//nolint:gochecknoglobals // static enums from the Container Apps spec
var (
	acaAuthActions   = []string{acaActionRedirect, "AllowAnonymous", "Return401", "Return403"}
	acaProxyConvents = []string{"NoProxy", skuTierStandard, "Custom"}
)

func isACAAppSubKind(k acaKind) bool { return k == acaKindAuth || k == acaKindSource }

func acaAppSubType(k acaKind) string {
	if k == acaKindAuth {
		return acaAuthType
	}

	return acaSourceType
}

func acaCheckAuthConfig(props map[string]any) error {
	if act := strOf(mapOf(props["globalValidation"]), "unauthenticatedClientAction"); act != "" {
		if _, ok := canonicalIn(acaAuthActions, act); !ok {
			return acaBad("globalValidation.unauthenticatedClientAction '%s' is not valid; allowed values are %s.", act,
				strings.Join(acaAuthActions, ", "))
		}
	}

	if conv := strOf(mapOf(mapOf(props["httpSettings"])["forwardProxy"]), "convention"); conv != "" {
		if _, ok := canonicalIn(acaProxyConvents, conv); !ok {
			return acaBad("httpSettings.forwardProxy.convention '%s' is not valid; allowed values are %s.", conv,
				strings.Join(acaProxyConvents, ", "))
		}
	}

	for _, k := range []string{"platform", "globalValidation", "identityProviders", fieldLogin, "httpSettings",
		"encryptionSettings"} {
		if v, set := props[k]; set && v != nil && mapOf(v) == nil {
			return acaBad("properties.%s must be an object.", k)
		}
	}

	return nil
}

func acaCheckSourceControl(props map[string]any) error {
	repo := strOf(props, "repoUrl")
	if repo == "" {
		return acaBad("properties.repoUrl is required.")
	}

	if u, err := url.Parse(repo); err != nil || !strings.HasPrefix(u.Scheme, "http") || u.Host == "" {
		return acaBad("properties.repoUrl '%s' is not a valid repository URL.", repo)
	}

	cfg := mapOf(props[acaFieldActionCfg])
	delete(mapOf(cfg[acaFieldRegInfo]), acaFieldRegistry)
	delete(mapOf(cfg[acaFieldAzureCreds]), acaFieldClientSec)

	return nil
}

func (p *ContainerAppsProvider) putAppSub(id ResourceID, body map[string]any) (map[string]any, error) {
	p.opMu.Lock("putAppSub")
	defer p.opMu.Unlock()

	t := acaTargetOf(id)
	typ, name := acaAppSubType(t.kind), t.names[1]

	if !strings.EqualFold(name, acaCurrent) {
		return nil, acaBad("The name of a container app %s resource must be '%s'.", typ, acaCurrent)
	}

	props := acaClone(mapOf(body[fieldProperties]))
	if props == nil {
		props = map[string]any{}
	}

	var err error
	if t.kind == acaKindAuth {
		err = acaCheckAuthConfig(props)
	} else {
		err = acaCheckSourceControl(props)
	}

	if err != nil {
		return nil, err
	}

	p.mu.Lock("putAppSub")
	defer p.mu.Unlock()

	app, ok := p.apps[acaKey(id.ResourceGroup, t.names[0])]
	if !ok {
		return nil, ErrResourceNotFound
	}

	now := p.now().UTC()
	rec := &storedACASub{Name: acaCurrent, Type: typ, Created: now, Modified: now, Props: props}

	if old, exists := app.Subs[acaSubKey(typ, acaCurrent)]; exists {
		rec.Created = old.Created
	}

	if app.Subs == nil {
		app.Subs = map[string]*storedACASub{}
	}

	app.Subs[acaSubKey(typ, acaCurrent)] = rec

	return p.appSubBody(id, app, rec), nil
}

func (p *ContainerAppsProvider) appSubBody(id ResourceID, app *storedACAApp, s *storedACASub) map[string]any {
	props := acaClone(s.Props)
	props[fieldProvisioningState] = provisioningStateSucceeded

	if s.Type == acaSourceType {
		props["operationState"] = provisioningStateSucceeded
	}

	sid := acaResourceID(id.SubscriptionID, id.ResourceGroup, []string{acaAppType, s.Type}, []string{app.Name, s.Name})

	return map[string]any{
		"id": sid.ARMID(), fieldName: s.Name, fieldType: namespaceMicrosoftApp + "/" + acaAppType + "/" + s.Type,
		fieldProperties: props, acaFieldSystemData: acaSystemData(s.Created, s.Modified),
	}
}

func (p *ContainerAppsProvider) appSubGet(id ResourceID, t acaTarget) (map[string]any, error) {
	app, ok := p.apps[acaKey(id.ResourceGroup, t.names[0])]
	if !ok {
		return nil, ErrResourceNotFound
	}

	s, ok := app.Subs[acaSubKey(acaAppSubType(t.kind), t.names[1])]
	if !ok {
		return nil, ErrResourceNotFound
	}

	return p.appSubBody(id, app, s), nil
}

func (p *ContainerAppsProvider) appSubList(id ResourceID, t acaTarget) ([]map[string]any, error) {
	app, ok := p.apps[acaKey(id.ResourceGroup, id.Names[0])]
	if !ok {
		return nil, ErrResourceNotFound
	}

	out := []map[string]any{}

	if s, found := app.Subs[acaSubKey(acaAppSubType(t.kind), acaCurrent)]; found {
		out = append(out, p.appSubBody(id, app, s))
	}

	return out, nil
}

func (p *ContainerAppsProvider) appSubDelete(id ResourceID, t acaTarget) error {
	p.opMu.Lock("appSubDelete")
	defer p.opMu.Unlock()

	p.mu.Lock("appSubDelete")
	defer p.mu.Unlock()

	app, ok := p.apps[acaKey(id.ResourceGroup, t.names[0])]
	if !ok {
		return ErrResourceNotFound
	}

	key := acaSubKey(acaAppSubType(t.kind), t.names[1])
	if _, found := app.Subs[key]; !found {
		return ErrResourceNotFound
	}

	delete(app.Subs, key)

	return nil
}
