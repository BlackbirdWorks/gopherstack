package azurearm

import (
	"net/http"
	"slices"
	"strings"

	"github.com/google/uuid"
)

const (
	identityNone        = "None"
	identitySystem      = "SystemAssigned"
	identityUser        = "UserAssigned"
	identityBoth        = "SystemAssigned, UserAssigned"
	identityCodeInvalid = "InvalidParameter"
	identityCodeFailed  = "FailedIdentityOperation"
	fieldIdentity       = "identity"
	fieldPrincipalID    = "principalId"
	fieldClientID       = "clientId"
	fieldUserAssigned   = "userAssignedIdentities"
)

// resourceIdentity is the managed identity block of a VM, web site or container group.
type resourceIdentity struct {
	Type         string   `json:"type"`
	PrincipalID  string   `json:"principalId,omitempty"`
	TenantID     string   `json:"tenantId,omitempty"`
	UserAssigned []string `json:"userAssigned,omitempty"`
}

func (i resourceIdentity) hasSystem() bool {
	return i.Type == identitySystem || i.Type == identityBoth
}

func (i resourceIdentity) isSet() bool { return i.Type != "" }

// identityService validates identity blocks and resolves user-assigned identities against the ManagedIdentity
// provider. A nil service issues system-assigned identities in the default tenant and resolves no user-assigned ones.
type identityService struct {
	msi    *ManagedIdentityProvider
	tenant string
}

func newIdentityService(msi *ManagedIdentityProvider, tenant string) *identityService {
	if tenant == "" {
		tenant = DefaultTenantID
	}

	return &identityService{msi: msi, tenant: tenant}
}

func (s *identityService) tenantID() string {
	if s == nil || s.tenant == "" {
		return DefaultTenantID
	}

	return s.tenant
}

func identityErr(code, format string, args ...any) error {
	return netErr(http.StatusBadRequest, code, format, args...)
}

func normIdentityType(t string) (string, bool) {
	parts := strings.Split(t, ",")
	sys, usr := false, false

	for _, p := range parts {
		switch strings.ToLower(strings.TrimSpace(p)) {
		case "none":
			if len(parts) > 1 {
				return "", false
			}

			return identityNone, true
		case "systemassigned":
			sys = true
		case "userassigned":
			usr = true
		default:
			return "", false
		}
	}

	switch {
	case sys && usr:
		return identityBoth, true
	case sys:
		return identitySystem, true
	case usr:
		return identityUser, true
	}

	return "", false
}

// apply computes the identity a PUT/PATCH leaves behind. present=false (no identity key) keeps prev; type None clears
// it; a system-assigned principal survives re-application and is regenerated only after removal.
func (s *identityService) apply(raw any, present bool, prev resourceIdentity) (resourceIdentity, error) {
	if !present {
		return prev, nil
	}

	m := mapOf(raw)

	typ, ok := normIdentityType(strOf(m, fieldType))
	if !ok {
		return resourceIdentity{}, identityErr(identityCodeInvalid,
			"The value '%s' of parameter 'identity.type' is not allowed. Allowed values are: None, SystemAssigned, "+
				"UserAssigned, SystemAssigned, UserAssigned.", strOf(m, fieldType))
	}

	if typ == identityNone {
		return resourceIdentity{}, nil
	}

	ua, err := s.userAssigned(typ, m[fieldUserAssigned])
	if err != nil {
		return resourceIdentity{}, err
	}

	out := resourceIdentity{Type: typ, UserAssigned: ua}

	if out.hasSystem() {
		out.TenantID = s.tenantID()
		out.PrincipalID = uuid.NewString()

		if prev.hasSystem() {
			out.PrincipalID = prev.PrincipalID
		}
	}

	return out, nil
}

func (s *identityService) userAssigned(typ string, raw any) ([]string, error) {
	uai, _ := raw.(map[string]any)
	wantsUser := typ == identityUser || typ == identityBoth

	switch {
	case wantsUser && len(uai) == 0:
		return nil, identityErr(identityCodeInvalid,
			"The parameter 'identity.userAssignedIdentities' must contain at least one identity when the identity "+
				"type is '%s'.", typ)
	case !wantsUser && len(uai) > 0:
		return nil, identityErr(identityCodeInvalid,
			"The parameter 'identity.userAssignedIdentities' is not allowed when the identity type is '%s'.", typ)
	}

	ids := make([]string, 0, len(uai))

	for id := range uai {
		if _, _, found := s.lookup(id); !found {
			return nil, identityErr(identityCodeFailed,
				"Identity operation for the resource failed: the user assigned identity '%s' was not found.", id)
		}

		ids = append(ids, id)
	}

	slices.SortFunc(ids, func(a, b string) int { return strings.Compare(strings.ToLower(a), strings.ToLower(b)) })

	return slices.CompactFunc(ids, strings.EqualFold), nil
}

func (s *identityService) lookup(id string) (string, string, bool) {
	if s == nil || s.msi == nil {
		return "", "", false
	}

	return s.msi.lookupUserAssigned(id)
}

// body renders the identity as the resource's GET returns it; nil renders nothing.
func (s *identityService) body(i resourceIdentity) map[string]any {
	if !i.isSet() {
		return nil
	}

	out := map[string]any{fieldType: i.Type}

	if i.hasSystem() {
		out[fieldPrincipalID], out["tenantId"] = i.PrincipalID, i.TenantID
	}

	if len(i.UserAssigned) > 0 {
		uai := make(map[string]any, len(i.UserAssigned))

		for _, id := range i.UserAssigned {
			principal, client, _ := s.lookup(id)
			uai[id] = map[string]any{fieldPrincipalID: principal, fieldClientID: client}
		}

		out[fieldUserAssigned] = uai
	}

	return out
}
