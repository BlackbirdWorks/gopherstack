package azurearm

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
)

const (
	apimTagsType          = "tags"
	apimGroupsType        = "groups"
	apimLoggersType       = "loggers"
	apimCachesType        = "caches"
	apimIdentityProviders = "identityProviders"
	apimAuthServersType   = "authorizationServers"
	apimOIDCType          = "openidConnectProviders"
	apimVersionSetsType   = "apiVersionSets"
	apimFragmentsType     = "policyFragments"
	apimUsersType         = "users"
	apimPortalType        = "portalsettings"
	apimTenantType        = "tenant"
	apimTenantAccess      = "access"
	apimPortalEnabled     = acrWebhookEnabled
	apimMaxDisplay        = 300
	apimMaxTagName        = 160
	apimMaxText           = 1000
	apimGroupCustom       = flexRuntimeCustom
	apimFormatXML         = xmlPrefix
)

// apimGenericKinds are plain CRUD collections of a service (armapimanagement v2.1.0 contracts), kept per service in
// Generic, keyed by collection name.
func apimGenericKinds() []apimKind {
	mk := func(typ string, norm func(*apimService, string, string, map[string]any, *apimEntity) (map[string]any, error),
		view func(map[string]any) map[string]any,
	) apimKind {
		return apimKind{typ: typ, norm: norm, view: view, store: func(s *apimService) map[string]*apimEntity {
			if s.Generic == nil {
				s.Generic = map[string]map[string]*apimEntity{}
			}

			if s.Generic[typ] == nil {
				s.Generic[typ] = map[string]*apimEntity{}
			}

			return s.Generic[typ]
		}}
	}

	return []apimKind{
		mk(apimTagsType, normTag, nil), mk(apimGroupsType, normGroup, nil),
		mk(apimLoggersType, normLogger, viewLogger), mk(apimCachesType, normCache, nil),
		mk(apimIdentityProviders, normIdentityProvider, dropSecrets("clientSecret")),
		mk(apimAuthServersType, normAuthServer, dropSecrets("clientSecret")),
		mk(apimOIDCType, normOIDC, dropSecrets("clientSecret")),
		mk(apimVersionSetsType, normVersionSet, nil), mk(apimFragmentsType, normFragment, nil),
		mk(apimUsersType, normUser, dropSecrets("password")),
		mk(apimPortalType, normPortalSetting, dropSecrets("validationKey")),
		mk(apimTenantType, normTenantAccess, dropSecrets(redisPropPrimaryKey, redisPropSecondaryKey)),
	}
}

func apimGenericKind(segment string) (apimKind, bool) {
	for _, k := range apimGenericKinds() {
		if strings.EqualFold(k.typ, segment) {
			return k, true
		}
	}

	return apimKind{}, false
}

func dropSecrets(keys ...string) func(map[string]any) map[string]any {
	return func(in map[string]any) map[string]any {
		out := cloneAnyMap(in)
		for _, k := range keys {
			delete(out, k)
		}

		return out
	}
}

func apimOneOf(in map[string]any, key, what string, allowed ...string) (string, error) {
	v := strOf(in, key)
	if v == "" {
		return "", apimInvalid("The %s is required.", what)
	}

	if !slices.Contains(allowed, v) {
		return "", apimInvalid("The %s '%s' is not valid; allowed values are %s.", what, v, strings.Join(allowed, ", "))
	}

	return v, nil
}

func normTag(_ *apimService, _, _ string, in map[string]any, _ *apimEntity) (map[string]any, error) {
	out := cloneAnyMap(in)
	if _, err := optString(out, apimCDisplayName, 1, apimMaxTagName, true, "tag displayName"); err != nil {
		return nil, err
	}

	return out, nil
}

func normGroup(_ *apimService, _, _ string, in map[string]any, prev *apimEntity) (map[string]any, error) {
	out := cloneAnyMap(in)
	if _, err := optString(out, apimCDisplayName, 1, apimMaxDisplay, true, "group displayName"); err != nil {
		return nil, err
	}

	if _, err := optString(out, webDescription, 0, apimMaxText, false, "group description"); err != nil {
		return nil, err
	}

	typ := strOf(out, fieldType)

	switch {
	case typ == "" && prev != nil:
		out[fieldType] = prev.Props[fieldType]
	case typ == "":
		out[fieldType] = apimGroupCustom
	case typ != apimGroupCustom && typ != "external":
		return nil, apimInvalid("The group type '%s' is not valid; allowed values are custom and external.", typ)
	}

	if strOf(out, "externalId") != "" && strOf(out, fieldType) != "external" {
		return nil, apimInvalid("The externalId applies to external groups only.")
	}

	return out, nil
}

func normLogger(_ *apimService, _, _ string, in map[string]any, _ *apimEntity) (map[string]any, error) {
	out := cloneAnyMap(in)

	if _, err := apimOneOf(
		out,
		"loggerType",
		"logger type",
		"applicationInsights",
		"azureEventHub",
		"azureMonitor",
	); err != nil {
		return nil, err
	}

	creds := mapOf(out["credentials"])
	if strOf(out, "loggerType") != "azureMonitor" && len(creds) == 0 {
		return nil, apimInvalid("The logger credentials are required.")
	}

	for k, v := range creds {
		if _, ok := v.(string); !ok {
			return nil, apimInvalid("The logger credential '%s' must be a string.", k)
		}
	}

	if _, err := optString(out, webDescription, 0, apimMaxText, false, "logger description"); err != nil {
		return nil, err
	}

	return out, nil
}

// viewLogger replaces credential values with the named-value placeholder the service returns.
func viewLogger(in map[string]any) map[string]any {
	out := cloneAnyMap(in)

	masked := map[string]any{}
	for k := range mapOf(in["credentials"]) {
		masked[k] = "{{Logger-Credentials--" + k + "}}"
	}

	if len(masked) > 0 {
		out["credentials"] = masked
	}

	return out
}

func normCache(_ *apimService, _, _ string, in map[string]any, _ *apimEntity) (map[string]any, error) {
	out := cloneAnyMap(in)

	if _, err := optString(out, "connectionString", 1, apimMaxURL, true, "cache connectionString"); err != nil {
		return nil, err
	}

	if _, err := optString(out, "useFromLocation", 1, apimMaxName, true, "cache useFromLocation"); err != nil {
		return nil, err
	}

	return out, nil
}

func normIdentityProvider(_ *apimService, _, id string, in map[string]any, prev *apimEntity) (map[string]any, error) {
	out := cloneAnyMap(in)

	known := []string{"aad", "aadB2C", "facebook", "google", "microsoft", "twitter"}
	if !slices.ContainsFunc(known, func(k string) bool { return strings.EqualFold(k, id) }) {
		return nil, apimInvalid(
			"The identity provider '%s' is not valid; allowed values are %s.",
			id,
			strings.Join(known, ", "),
		)
	}

	if _, err := optString(out, "clientId", 1, apimMaxName, true, "identity provider clientId"); err != nil {
		return nil, err
	}

	if prev == nil || out["clientSecret"] != nil {
		if _, err := optString(out, "clientSecret", 1, apimMaxURL, true, "identity provider clientSecret"); err != nil {
			return nil, err
		}
	} else {
		out["clientSecret"] = prev.Props["clientSecret"]
	}

	out[fieldType] = id

	return out, nil
}

func normAuthServer(_ *apimService, _, _ string, in map[string]any, _ *apimEntity) (map[string]any, error) {
	out := cloneAnyMap(in)

	if _, err := optString(
		out,
		apimCDisplayName,
		1,
		apimMaxDisplay,
		true,
		"authorization server displayName",
	); err != nil {
		return nil, err
	}

	for _, k := range []string{"clientRegistrationEndpoint", "authorizationEndpoint"} {
		u, err := optString(out, k, 1, apimMaxURL, true, "authorization server "+k)
		if err != nil {
			return nil, err
		}

		if !validAbsURL(u, acaScaleHTTP, acaSchemeHTTPS) {
			return nil, apimInvalid("The %s must be an absolute http or https URL.", k)
		}
	}

	grants := anyList(out["grantTypes"])
	if len(grants) == 0 {
		return nil, apimInvalid("At least one grant type is required.")
	}

	for _, g := range grants {
		if !slices.Contains(
			[]string{"authorizationCode", "implicit", "resourceOwnerPassword", "clientCredentials"},
			strOfAny(g),
		) {
			return nil, apimInvalid("The grant type '%v' is not valid.", g)
		}
	}

	for _, m := range anyList(out["authorizationMethods"]) {
		if !slices.Contains(
			[]string{
				http.MethodHead,
				http.MethodOptions,
				http.MethodTrace,
				http.MethodGet,
				http.MethodPost,
				http.MethodPut,
				http.MethodPatch,
				http.MethodDelete,
			},
			strOfAny(m),
		) {
			return nil, apimInvalid("The authorization method '%v' is not valid.", m)
		}
	}

	if m := strOf(out, "clientAuthenticationMethod"); m != "" && m != agwRuleBasic && m != "Body" {
		return nil, apimInvalid("The client authentication method '%s' is not valid.", m)
	}

	if _, err := optString(out, "clientId", 1, apimMaxName, true, "authorization server clientId"); err != nil {
		return nil, err
	}

	return out, nil
}

func normOIDC(_ *apimService, _, _ string, in map[string]any, _ *apimEntity) (map[string]any, error) {
	out := cloneAnyMap(in)

	if _, err := optString(
		out,
		apimCDisplayName,
		1,
		apimMaxDisplay,
		true,
		"OpenID Connect provider displayName",
	); err != nil {
		return nil, err
	}

	u, err := optString(out, "metadataEndpoint", 1, apimMaxURL, true, "OpenID Connect provider metadataEndpoint")
	if err != nil {
		return nil, err
	}

	if !validAbsURL(u, acaScaleHTTP, acaSchemeHTTPS) {
		return nil, apimInvalid("The metadataEndpoint must be an absolute http or https URL.")
	}

	if _, err = optString(out, "clientId", 1, apimMaxName, true, "OpenID Connect provider clientId"); err != nil {
		return nil, err
	}

	return out, nil
}

func normVersionSet(_ *apimService, _, _ string, in map[string]any, _ *apimEntity) (map[string]any, error) {
	out := cloneAnyMap(in)

	if _, err := optString(out, apimCDisplayName, 1, apimMaxDisplay, true, "API version set displayName"); err != nil {
		return nil, err
	}

	scheme, err := apimOneOf(out, "versioningScheme", "versioning scheme", "Segment", "Query", "Header")
	if err != nil {
		return nil, err
	}

	switch scheme {
	case "Query":
		if strOf(out, "versionQueryName") == "" {
			return nil, apimInvalid("The versionQueryName is required for the Query versioning scheme.")
		}
	case "Header":
		if strOf(out, "versionHeaderName") == "" {
			return nil, apimInvalid("The versionHeaderName is required for the Header versioning scheme.")
		}
	}

	return out, nil
}

func normFragment(_ *apimService, _, _ string, in map[string]any, _ *apimEntity) (map[string]any, error) {
	out := cloneAnyMap(in)

	format := strOf(out, apimCFormat)
	if format == "" {
		format = apimFormatXML
	}

	if format != apimFormatXML && format != apimCRawxml {
		return nil, apimInvalid("The policy fragment format must be xml or rawxml.")
	}

	doc, err := optString(out, fieldValue, 1, apimPolicyBytes, true, "policy fragment value")
	if err != nil {
		return nil, err
	}

	if err = wellFormedXML(doc); err != nil {
		return nil, apimInvalid("The policy fragment is not well-formed XML: %v.", err)
	}

	out[apimCFormat] = format

	return out, nil
}

func wellFormedXML(doc string) error {
	dec := xml.NewDecoder(strings.NewReader(doc))
	dec.Strict = true

	root := ""

	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return err
		}

		if se, ok := tok.(xml.StartElement); ok && root == "" {
			root = se.Name.Local
		}
	}

	if root != "fragment" {
		return errAPIMFragmentRoot
	}

	return nil
}

func normUser(_ *apimService, _, _ string, in map[string]any, prev *apimEntity) (map[string]any, error) {
	out := cloneAnyMap(in)

	email, err := optString(out, "email", 1, apimMaxName, true, "user email")
	if err != nil {
		return nil, err
	}

	if at := strings.Index(email, "@"); at < 1 || at == len(email)-1 {
		return nil, apimInvalid("The user email '%s' is not valid.", email)
	}

	for _, k := range []string{"firstName", "lastName"} {
		if _, err = optString(out, k, 1, apimMaxName, true, "user "+k); err != nil {
			return nil, err
		}
	}

	switch state := strOf(out, "state"); {
	case state == "" && prev != nil:
		out["state"] = prev.Props["state"]
	case state == "":
		out["state"] = apimStateActive
	case !slices.Contains([]string{apimStateActive, "blocked", "pending", "deleted"}, state):
		return nil, apimInvalid("The user state '%s' is not valid.", state)
	}

	return out, nil
}

var errAPIMFragmentRoot = errors.New("the root element must be fragment")

// apimPortalDefaults are the developer portal settings every service has from creation
// (armapimanagement PortalSigninSettings, PortalSignupSettings, PortalDelegationSettings).
func apimPortalDefaults() map[string]map[string]any {
	return map[string]map[string]any{
		"signin": {apimPortalEnabled: false},
		"signup": {
			apimPortalEnabled: false,
			"termsOfService":  map[string]any{apimPortalEnabled: false, "consentRequired": false},
		},
		"delegation": {
			"subscriptions": map[string]any{
				apimPortalEnabled: false,
			},
			"userRegistration": map[string]any{apimPortalEnabled: false},
		},
	}
}

func seedAPIMPortal(s *apimService) {
	seedAPIMTenant(s)

	if s.Generic == nil {
		s.Generic = map[string]map[string]*apimEntity{}
	}

	if s.Generic[apimPortalType] == nil {
		s.Generic[apimPortalType] = map[string]*apimEntity{}
	}

	for id, props := range apimPortalDefaults() {
		if s.Generic[apimPortalType][entKey("", id)] == nil {
			s.Generic[apimPortalType][entKey("", id)] = &apimEntity{ID: id, Props: props, Etag: 1}

			if id == "delegation" {
				props["validationKey"] = newAPIMKey() + newAPIMKey()
			}
		}
	}
}

func normPortalSetting(_ *apimService, _, id string, in map[string]any, prev *apimEntity) (map[string]any, error) {
	if _, known := apimPortalDefaults()[strings.ToLower(id)]; !known || prev == nil {
		return nil, apimInvalid(
			"The portal setting '%s' is not valid; allowed values are signin, signup and delegation.",
			id,
		)
	}

	out := cloneAnyMap(prev.Props)

	for k, v := range in {
		switch k {
		case apimPortalEnabled:
			if _, ok := v.(bool); !ok {
				return nil, apimInvalid("The portal setting property enabled must be a boolean.")
			}
		case "termsOfService", "subscriptions", "userRegistration", "url", "validationKey":
		default:
			return nil, apimInvalid("The portal setting property '%s' is not valid.", k)
		}

		out[k] = v
	}

	return out, nil
}

// seedAPIMTenant creates the tenant access and git access settings every service has, disabled with generated keys.
func seedAPIMTenant(s *apimService) {
	if s.Generic == nil {
		s.Generic = map[string]map[string]*apimEntity{}
	}

	if s.Generic[apimTenantType] == nil {
		s.Generic[apimTenantType] = map[string]*apimEntity{}
	}

	for _, id := range []string{apimTenantAccess, "git"} {
		if s.Generic[apimTenantType][entKey("", id)] == nil {
			s.Generic[apimTenantType][entKey("", id)] = &apimEntity{ID: id, Etag: 1, Props: map[string]any{
				apimPortalEnabled: false, redisPropPrimaryKey: newAPIMKey(), redisPropSecondaryKey: newAPIMKey(),
			}}
		}
	}
}

func normTenantAccess(_ *apimService, _, id string, in map[string]any, prev *apimEntity) (map[string]any, error) {
	if prev == nil || (!strings.EqualFold(id, apimTenantAccess) && !strings.EqualFold(id, "git")) {
		return nil, apimInvalid("The tenant setting '%s' is not valid; allowed values are access and git.", id)
	}

	out := cloneAnyMap(prev.Props)

	if v, has := in[apimPortalEnabled]; has {
		enabled, ok := v.(bool)
		if !ok {
			return nil, apimInvalid("The tenant access property enabled must be a boolean.")
		}

		out[apimPortalEnabled] = enabled
	}

	return out, nil
}

func isAPIMSettingsSecrets(k apimKind, id, verb string) bool {
	if !strings.EqualFold(verb, "listSecrets") {
		return false
	}

	return k.typ == apimTenantType || (k.typ == apimPortalType && strings.EqualFold(id, "delegation"))
}

// apimSettingsSecrets answers listSecrets of tenant access (keys) and of the delegation setting (validation key).
func apimSettingsSecrets(k apimKind, ent *apimEntity) apimResp {
	if k.typ == apimPortalType {
		return apimResp{Status: http.StatusOK, Etag: apimEtag(ent.Etag), Body: map[string]any{
			"validationKey": ent.Props["validationKey"],
		}}
	}

	return apimResp{Status: http.StatusOK, Etag: apimEtag(ent.Etag), Body: map[string]any{
		"id":                  ent.ID,
		apimPortalEnabled:     ent.Props[apimPortalEnabled],
		redisPropPrimaryKey:   ent.Props[redisPropPrimaryKey],
		redisPropSecondaryKey: ent.Props[redisPropSecondaryKey],
	}}
}
