package eks

import "encoding/json"

// Wire-shape request/response bodies and validation/conversion for
// Capability.Configuration -- gopherstack-wf8f item 1. Every field/key here
// is verified against aws-sdk-go-v2/service/eks@v1.98.0's serializers.go
// (awsRestjson1_serializeDocumentArgoCd*/CapabilityConfigurationRequest/
// UpdateCapabilityConfiguration) and deserializers.go
// (awsRestjson1_deserializeDocumentArgoCd*/CapabilityConfigurationResponse),
// and the request-side member requirements against validators.go's
// validateArgoCd*/validateCapabilityConfigurationRequest/
// validateUpdateCapabilityConfiguration/validateSsoIdentity.
//
// Capability.Configuration's json keys (argoCd/awsIdc/idcInstanceArn/
// idcRegion/idcManagedApplicationArn/namespace/networkAccess/vpceIds/
// rbacRoleMappings/role/identities/id/type/serverUrl, all above) are
// exactly the keys the pre-gopherstack-wf8f untyped map[string]any held --
// that map was always the raw request body re-echoed, which the real SDK
// client itself serializes under these same camelCase names, and every
// struct field here reuses that name verbatim in its own json tag. So a
// version-2 snapshot's Configuration decodes natively into
// *CapabilityConfiguration with no translation needed, EXCEPT that no
// released version of this backend ever actually populated
// Capability.Configuration on any code path (CreateCapability never wired
// a Configuration parameter before this pass) -- so no real snapshot has a
// non-null value there regardless. Capability.UnmarshalJSON below still
// treats a Configuration shape this pass cannot represent (chosen case: a
// map that fails to decode as *CapabilityConfiguration at all, e.g. a
// non-ARGOCD-typed blob) as Configuration: nil, not a decode failure, for
// two reasons: (1) honesty -- an unrepresentable shape has no correct typed
// value to invent, so nil is the truthful answer, matching this pass's
// no-fabrication rule elsewhere; (2) mechanics -- pkgs/store's
// restoreJSON unmarshals a whole table as one []*Capability before calling
// Table.Restore at all (table.go:268-277), so ANY single Capability's
// json.Unmarshal error aborts restoring every OTHER capability in the
// snapshot too, and eks's own Restore treats a table restore error as
// fatal to the entire eks snapshot (persistence.go) -- letting one
// unrepresentable Configuration blow up unrelated resources would be far
// worse than dropping that one field.

type ssoIdentityBody struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

type argoCdRoleMappingBody struct {
	Role       string            `json:"role"`
	Identities []ssoIdentityBody `json:"identities"`
}

type argoCdAwsIdcConfigRequestBody struct {
	IdcInstanceArn string `json:"idcInstanceArn"`
	IdcRegion      string `json:"idcRegion"`
}

type argoCdNetworkAccessConfigRequestBody struct {
	VpceIDs []string `json:"vpceIds"`
}

// argoCdConfigRequestBody mirrors types.ArgoCdConfigRequest
// (types.go:386) -- AwsIdc is required (validators.go's
// validateArgoCdConfigRequest); Namespace/NetworkAccess/RbacRoleMappings are
// optional.
type argoCdConfigRequestBody struct {
	AwsIdc           *argoCdAwsIdcConfigRequestBody        `json:"awsIdc"`
	NetworkAccess    *argoCdNetworkAccessConfigRequestBody `json:"networkAccess"`
	Namespace        string                                `json:"namespace"`
	RbacRoleMappings []argoCdRoleMappingBody               `json:"rbacRoleMappings"`
}

// capabilityConfigurationRequestBody mirrors types.CapabilityConfigurationRequest
// (types.go:645) -- ArgoCd is its only member in this pinned SDK version;
// see CapabilityConfiguration's doc comment in models.go.
type capabilityConfigurationRequestBody struct {
	ArgoCd *argoCdConfigRequestBody `json:"argoCd"`
}

// validateSsoIdentityBody enforces validateSsoIdentity (validators.go):
// both Id and Type are required.
func validateSsoIdentityBody(id ssoIdentityBody) error {
	if id.ID == "" {
		return ErrValidation
	}

	if id.Type == "" {
		return ErrValidation
	}

	return nil
}

// validateArgoCdRoleMappingBody enforces validateArgoCdRoleMapping
// (validators.go): Role and Identities are both required, and every
// identity must itself be valid.
func validateArgoCdRoleMappingBody(m argoCdRoleMappingBody) error {
	if m.Role == "" {
		return ErrValidation
	}

	if m.Identities == nil {
		return ErrValidation
	}

	for _, id := range m.Identities {
		if err := validateSsoIdentityBody(id); err != nil {
			return err
		}
	}

	return nil
}

// validateCapabilityConfigurationRequestBody enforces
// validateCapabilityConfigurationRequest/validateArgoCdConfigRequest/
// validateArgoCdAwsIdcConfigRequest (validators.go): when ArgoCd is present,
// AwsIdc is required and AwsIdc.IdcInstanceArn is required; RbacRoleMappings,
// when present, must each be a valid role mapping.
func validateCapabilityConfigurationRequestBody(cfg *capabilityConfigurationRequestBody) error {
	if cfg == nil || cfg.ArgoCd == nil {
		return nil
	}

	if cfg.ArgoCd.AwsIdc == nil {
		return ErrValidation
	}

	if cfg.ArgoCd.AwsIdc.IdcInstanceArn == "" {
		return ErrValidation
	}

	for _, m := range cfg.ArgoCd.RbacRoleMappings {
		if err := validateArgoCdRoleMappingBody(m); err != nil {
			return err
		}
	}

	return nil
}

func ssoIdentitiesFromBody(in []ssoIdentityBody) []SsoIdentity {
	if in == nil {
		return nil
	}

	out := make([]SsoIdentity, len(in))
	for i, id := range in {
		out[i] = SsoIdentity(id)
	}

	return out
}

func argoCdRoleMappingsFromBody(in []argoCdRoleMappingBody) []ArgoCdRoleMapping {
	if in == nil {
		return nil
	}

	out := make([]ArgoCdRoleMapping, len(in))
	for i, m := range in {
		out[i] = ArgoCdRoleMapping{Role: m.Role, Identities: ssoIdentitiesFromBody(m.Identities)}
	}

	return out
}

// capabilityConfigurationFromRequest converts a validated request body into
// this backend's stored CapabilityConfiguration. Server-computed response
// members (ArgoCdAwsIdcConfig.IdcManagedApplicationArn, ArgoCdConfig.ServerURL)
// are intentionally left zero -- see their doc comments in models.go.
func capabilityConfigurationFromRequest(cfg *capabilityConfigurationRequestBody) *CapabilityConfiguration {
	if cfg == nil || cfg.ArgoCd == nil {
		return nil
	}

	out := &ArgoCdConfig{
		Namespace:        cfg.ArgoCd.Namespace,
		RbacRoleMappings: argoCdRoleMappingsFromBody(cfg.ArgoCd.RbacRoleMappings),
	}

	if cfg.ArgoCd.AwsIdc != nil {
		out.AwsIdc = &ArgoCdAwsIdcConfig{
			IdcInstanceArn: cfg.ArgoCd.AwsIdc.IdcInstanceArn,
			IdcRegion:      cfg.ArgoCd.AwsIdc.IdcRegion,
		}
	}

	if cfg.ArgoCd.NetworkAccess != nil {
		out.NetworkAccess = &ArgoCdNetworkAccessConfig{VpceIDs: cloneStrings(cfg.ArgoCd.NetworkAccess.VpceIDs)}
	}

	return &CapabilityConfiguration{ArgoCd: out}
}

// updateArgoCdConfigBody mirrors types.UpdateArgoCdConfig (types.go:3295),
// whose doc says only the fields you want to update need to be specified.
type updateArgoCdConfigBody struct {
	NetworkAccess    *argoCdNetworkAccessConfigRequestBody `json:"networkAccess"`
	RbacRoleMappings *updateRoleMappingsBody               `json:"rbacRoleMappings"`
}

// updateRoleMappingsBody mirrors types.UpdateRoleMappings (types.go:3346).
type updateRoleMappingsBody struct {
	AddOrUpdateRoleMappings []argoCdRoleMappingBody `json:"addOrUpdateRoleMappings"`
	RemoveRoleMappings      []argoCdRoleMappingBody `json:"removeRoleMappings"`
}

// updateCapabilityConfigurationBody mirrors types.UpdateCapabilityConfiguration
// (types.go:3311).
type updateCapabilityConfigurationBody struct {
	ArgoCd *updateArgoCdConfigBody `json:"argoCd"`
}

func validateUpdateCapabilityConfigurationBody(cfg *updateCapabilityConfigurationBody) error {
	if cfg == nil || cfg.ArgoCd == nil || cfg.ArgoCd.RbacRoleMappings == nil {
		return nil
	}

	for _, m := range cfg.ArgoCd.RbacRoleMappings.AddOrUpdateRoleMappings {
		if err := validateArgoCdRoleMappingBody(m); err != nil {
			return err
		}
	}

	for _, m := range cfg.ArgoCd.RbacRoleMappings.RemoveRoleMappings {
		if err := validateArgoCdRoleMappingBody(m); err != nil {
			return err
		}
	}

	return nil
}

// identityEqual compares two SsoIdentity values by Id+Type, the natural key
// real AWS's role-mapping remove operation matches against (RemoveRoleMappings'
// doc: "a list of role mappings to remove ... and the identities to remove
// from that role").
func identityEqual(a, b SsoIdentity) bool { return a.ID == b.ID && a.Type == b.Type }

// applyRoleMappingRemovals removes, from current, every identity named by
// any entry in removals whose Role matches. A role mapping whose Identities
// list becomes empty is dropped entirely.
func applyRoleMappingRemovals(current []ArgoCdRoleMapping, removals []ArgoCdRoleMapping) []ArgoCdRoleMapping {
	toRemove := make(map[string][]SsoIdentity, len(removals))
	for _, r := range removals {
		toRemove[r.Role] = append(toRemove[r.Role], r.Identities...)
	}

	out := make([]ArgoCdRoleMapping, 0, len(current))

	for _, m := range current {
		remove, ok := toRemove[m.Role]
		if !ok {
			out = append(out, m)

			continue
		}

		kept := make([]SsoIdentity, 0, len(m.Identities))

		for _, id := range m.Identities {
			drop := false

			for _, r := range remove {
				if identityEqual(id, r) {
					drop = true

					break
				}
			}

			if !drop {
				kept = append(kept, id)
			}
		}

		if len(kept) > 0 {
			out = append(out, ArgoCdRoleMapping{Role: m.Role, Identities: kept})
		}
	}

	return out
}

// applyRoleMappingAddOrUpdate replaces the identities of an existing role's
// mapping or appends a new one, per UpdateRoleMappings.AddOrUpdateRoleMappings'
// doc ("If a mapping for the specified role already exists, it will be
// updated with the new identities. If it doesn't exist, a new mapping will
// be created.").
func applyRoleMappingAddOrUpdate(current []ArgoCdRoleMapping, updates []ArgoCdRoleMapping) []ArgoCdRoleMapping {
	out := make([]ArgoCdRoleMapping, 0, len(current)+len(updates))
	out = append(out, current...)

	for _, u := range updates {
		found := false

		for i := range out {
			if out[i].Role == u.Role {
				out[i].Identities = u.Identities
				found = true

				break
			}
		}

		if !found {
			out = append(out, u)
		}
	}

	return out
}

// applyUpdateCapabilityConfiguration merges an UpdateCapabilityConfiguration
// request onto the capability's current Configuration and returns the
// result. Nil fields on upd are left untouched (types.UpdateArgoCdConfig's
// doc: "You only need to specify the fields you want to update.").
// Returns the unmodified current configuration when upd is nil/empty, and a
// fresh ArgoCd-only configuration (matching capabilities.go's Type-mismatch
// guard, which rejects an ArgoCd Configuration on a non-ArgoCd capability
// before this is ever reached) when current has none yet.
func applyUpdateCapabilityConfiguration(
	current *CapabilityConfiguration, upd *updateCapabilityConfigurationBody,
) *CapabilityConfiguration {
	if upd == nil || upd.ArgoCd == nil {
		return current
	}

	out := &ArgoCdConfig{}
	if current != nil && current.ArgoCd != nil {
		cp := *current.ArgoCd
		out = &cp
	}

	if upd.ArgoCd.NetworkAccess != nil {
		out.NetworkAccess = &ArgoCdNetworkAccessConfig{VpceIDs: cloneStrings(upd.ArgoCd.NetworkAccess.VpceIDs)}
	}

	if upd.ArgoCd.RbacRoleMappings != nil {
		removals := argoCdRoleMappingsFromBody(upd.ArgoCd.RbacRoleMappings.RemoveRoleMappings)
		addOrUpdates := argoCdRoleMappingsFromBody(upd.ArgoCd.RbacRoleMappings.AddOrUpdateRoleMappings)

		mappings := applyRoleMappingRemovals(out.RbacRoleMappings, removals)
		out.RbacRoleMappings = applyRoleMappingAddOrUpdate(mappings, addOrUpdates)
	}

	return &CapabilityConfiguration{ArgoCd: out}
}

func ssoIdentitiesToJSON(in []SsoIdentity) []map[string]any {
	out := make([]map[string]any, len(in))
	for i, id := range in {
		out[i] = map[string]any{"id": id.ID, "type": id.Type}
	}

	return out
}

func argoCdRoleMappingsToJSON(in []ArgoCdRoleMapping) []map[string]any {
	out := make([]map[string]any, len(in))
	for i, m := range in {
		out[i] = map[string]any{"role": m.Role, "identities": ssoIdentitiesToJSON(m.Identities)}
	}

	return out
}

// capabilityConfigurationToJSON mirrors types.CapabilityConfigurationResponse
// (types.go:655). Absent optional members are omitted rather than emitted
// as null/empty, matching this file's other *ToJSON conventions.
func capabilityConfigurationToJSON(cfg *CapabilityConfiguration) map[string]any {
	if cfg == nil || cfg.ArgoCd == nil {
		return nil
	}

	a := cfg.ArgoCd
	argoCd := map[string]any{}

	if a.AwsIdc != nil {
		idc := map[string]any{}
		if a.AwsIdc.IdcInstanceArn != "" {
			idc["idcInstanceArn"] = a.AwsIdc.IdcInstanceArn
		}

		if a.AwsIdc.IdcManagedApplicationArn != "" {
			idc["idcManagedApplicationArn"] = a.AwsIdc.IdcManagedApplicationArn
		}

		if a.AwsIdc.IdcRegion != "" {
			idc["idcRegion"] = a.AwsIdc.IdcRegion
		}

		argoCd["awsIdc"] = idc
	}

	if a.Namespace != "" {
		argoCd["namespace"] = a.Namespace
	}

	if a.NetworkAccess != nil {
		argoCd["networkAccess"] = map[string]any{"vpceIds": a.NetworkAccess.VpceIDs}
	}

	if len(a.RbacRoleMappings) > 0 {
		argoCd["rbacRoleMappings"] = argoCdRoleMappingsToJSON(a.RbacRoleMappings)
	}

	if a.ServerURL != "" {
		argoCd["serverUrl"] = a.ServerURL
	}

	return map[string]any{"argoCd": argoCd}
}

// UnmarshalJSON degrades a persisted Configuration this pass's typed
// *CapabilityConfiguration cannot represent to nil instead of failing this
// Capability's decode -- see this file's top-of-file doc comment for why.
// Every other field decodes exactly as the plain Capability struct tags
// already specify (via the capabilityAlias trick, which shadows only
// "configuration" so encoding/json still promotes every other field
// normally).
func (c *Capability) UnmarshalJSON(data []byte) error {
	type capabilityAlias Capability

	aux := struct {
		*capabilityAlias
		Configuration json.RawMessage `json:"configuration"`
	}{
		capabilityAlias: (*capabilityAlias)(c),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	c.Configuration = nil

	if len(aux.Configuration) == 0 || string(aux.Configuration) == "null" {
		return nil
	}

	var cfg CapabilityConfiguration
	if err := json.Unmarshal(aux.Configuration, &cfg); err == nil {
		c.Configuration = &cfg
	}

	return nil
}
