package azuregraph

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
)

type normalizer[T any] interface {
	*T
	normalize()
}

// jsonFields returns the JSON property names declared on the struct type t.
func jsonFields(t reflect.Type) map[string]bool {
	out := map[string]bool{}

	for i := range t.NumField() {
		name, _, _ := strings.Cut(t.Field(i).Tag.Get("json"), ",")
		if name != "" && name != "-" {
			out[name] = true
		}
	}

	return out
}

// mergeJSON overlays patch onto base: objects merge recursively, null deletes, anything else replaces.
func mergeJSON(base, patch map[string]json.RawMessage) {
	for k, v := range patch {
		if bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			delete(base, k)

			continue
		}

		var pm, bm map[string]json.RawMessage
		if json.Unmarshal(v, &pm) == nil && pm != nil && json.Unmarshal(base[k], &bm) == nil && bm != nil {
			mergeJSON(bm, pm)

			if merged, err := json.Marshal(bm); err == nil {
				base[k] = merged

				continue
			}
		}

		base[k] = v
	}
}

// applyBody merges a create or PATCH body onto cur and returns the resulting object.
// Unknown or read-only properties are rejected the way Graph does.
func applyBody[T any, P normalizer[T]](cur *T, body map[string]json.RawMessage, readOnly ...string) (*T, error) {
	known := jsonFields(reflect.TypeFor[T]())
	patch := make(map[string]json.RawMessage, len(body))

	for k, v := range body {
		if strings.Contains(k, "@") {
			continue
		}

		if !known[k] {
			return nil, badRequest("Property '%s' does not exist as a declared property or extension property.", k)
		}

		for _, ro := range readOnly {
			if k == ro {
				return nil, badRequest("Property '%s' is read-only and cannot be set.", k)
			}
		}

		patch[k] = v
	}

	raw, err := json.Marshal(cur)
	if err != nil {
		return nil, err //nolint:wrapcheck // marshal of an owned struct cannot fail on input
	}

	base := map[string]json.RawMessage{}
	if err = json.Unmarshal(raw, &base); err != nil {
		return nil, err //nolint:wrapcheck // as above
	}

	mergeJSON(base, patch)

	merged, err := json.Marshal(base)
	if err != nil {
		return nil, err //nolint:wrapcheck // as above
	}

	out := new(T)
	if err = json.Unmarshal(merged, out); err != nil {
		return nil, badRequest(
			"Unable to read JSON request payload. Please ensure Content-Type header is set and payload is of valid JSON format.")
	}

	P(out).normalize()

	return out, nil
}

func nonNil[S ~[]E, E any](s S) S {
	if s == nil {
		return S{}
	}

	return s
}

func (a *Application) normalize() {
	a.IdentifierURIs, a.Tags = nonNil(a.IdentifierURIs), nonNil(a.Tags)
	a.AppRoles, a.RequiredResourceAccess = nonNil(a.AppRoles), nonNil(a.RequiredResourceAccess)
	a.PasswordCredentials, a.KeyCredentials = nonNil(a.PasswordCredentials), nonNil(a.KeyCredentials)
	a.API.KnownClientApplications = nonNil(a.API.KnownClientApplications)
	a.API.OAuth2PermissionScopes = nonNil(a.API.OAuth2PermissionScopes)
	a.API.PreAuthorizedApplications = nonNil(a.API.PreAuthorizedApplications)
	a.Web.RedirectURIs, a.SPA.RedirectURIs = nonNil(a.Web.RedirectURIs), nonNil(a.SPA.RedirectURIs)
	a.PublicClient.RedirectURIs = nonNil(a.PublicClient.RedirectURIs)

	for i := range a.AppRoles {
		a.AppRoles[i].AllowedMemberTypes = nonNil(a.AppRoles[i].AllowedMemberTypes)
	}

	for i := range a.RequiredResourceAccess {
		a.RequiredResourceAccess[i].ResourceAccess = nonNil(a.RequiredResourceAccess[i].ResourceAccess)
	}

	for i := range a.API.PreAuthorizedApplications {
		p := &a.API.PreAuthorizedApplications[i]
		p.DelegatedPermissionIDs = nonNil(p.DelegatedPermissionIDs)
	}
}

func (s *ServicePrincipal) normalize() {
	s.AlternativeNames, s.NotificationEmailAddresses = nonNil(s.AlternativeNames), nonNil(s.NotificationEmailAddresses)
	s.ReplyURLs, s.ServicePrincipalNames, s.Tags = nonNil(s.ReplyURLs), nonNil(s.ServicePrincipalNames), nonNil(s.Tags)
	s.AppRoles, s.OAuth2PermissionScopes = nonNil(s.AppRoles), nonNil(s.OAuth2PermissionScopes)
	s.PasswordCredentials, s.KeyCredentials = nonNil(s.PasswordCredentials), nonNil(s.KeyCredentials)

	for i := range s.AppRoles {
		s.AppRoles[i].AllowedMemberTypes = nonNil(s.AppRoles[i].AllowedMemberTypes)
	}
}

func (u *User) normalize() {
	u.BusinessPhones, u.OtherMails = nonNil(u.BusinessPhones), nonNil(u.OtherMails)
}

func (g *Group) normalize() {
	g.GroupTypes, g.ResourceBehaviorOptions = nonNil(g.GroupTypes), nonNil(g.ResourceBehaviorOptions)
	g.ResourceProvisioningOptions = nonNil(g.ResourceProvisioningOptions)
}

func (*AppRoleAssignment) normalize()     {}
func (*OAuth2PermissionGrant) normalize() {}
