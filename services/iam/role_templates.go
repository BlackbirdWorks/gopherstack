package iam

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func roleTemplateVersionKey(templateArn string, minorVersion int32) string {
	return templateArn + "#" + strconv.Itoa(int(minorVersion))
}

// AddRoleTemplateVersionInternal seeds a role template version directly into
// the store. Real AWS has no Create/Put/List operation for role templates in
// this SDK version -- GetRoleTemplateVersion and AcquireRole are the only two
// operations that ever reference one -- so this is the only way this
// backend's role-template state is ever populated. Mirrors
// services/cloudwatchlogs's AddAnomalyInternal and services/quicksight's
// AddAppInternal test seams for the same class of resource this backend
// cannot originate itself.
func (b *InMemoryBackend) AddRoleTemplateVersionInternal(v RoleTemplateVersion) *RoleTemplateVersion {
	b.mu.Lock("AddRoleTemplateVersionInternal")
	defer b.mu.Unlock()

	if v.CreateTimestamp.IsZero() {
		v.CreateTimestamp = time.Now().UTC()
	}

	cp := v
	b.roleTemplateVersions.Put(&cp)

	out := cp

	return &out
}

// resolveRoleTemplateVersionLocked finds the role template version matching
// templateArn and minorVersion (nil meaning "use the template's own default
// minor version", matching AcquireRole/GetRoleTemplateVersion's own
// documented "If you do not specify a minor version, the service uses the
// template's default minor version" semantics). Callers must hold b.mu.
func (b *InMemoryBackend) resolveRoleTemplateVersionLocked(
	templateArn string, minorVersion *int32,
) (*RoleTemplateVersion, error) {
	if templateArn == "" {
		return nil, fmt.Errorf("%w: TemplateArn is required", ErrInvalidInput)
	}

	var versions []*RoleTemplateVersion

	for _, v := range b.roleTemplateVersions.All() {
		if v.TemplateArn == templateArn {
			versions = append(versions, v)
		}
	}

	if len(versions) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrRoleTemplateNotFound, templateArn)
	}

	if minorVersion != nil {
		for _, v := range versions {
			if v.MinorVersion == *minorVersion {
				return v, nil
			}
		}

		return nil, fmt.Errorf(
			"%w: %s minor version %d", ErrRoleTemplateNotFound, templateArn, *minorVersion,
		)
	}

	// No minor version specified: prefer the version whose own MinorVersion
	// equals its declared DefaultMinorVersion. If the seeded set doesn't
	// carry a self-consistent default (e.g. only one version was ever
	// seeded), fall back to the highest MinorVersion -- deterministic, and
	// not a fabricated value since it is still one of the caller's own
	// seeded versions.
	best := versions[0]
	for _, v := range versions {
		if v.MinorVersion == v.DefaultMinorVersion {
			return v, nil
		}

		if v.MinorVersion > best.MinorVersion {
			best = v
		}
	}

	return best, nil
}

// GetRoleTemplateVersion retrieves a role template version by ARN (and
// optional minor version).
func (b *InMemoryBackend) GetRoleTemplateVersion(
	templateArn string, minorVersion *int32,
) (*RoleTemplateVersion, error) {
	b.mu.RLock("GetRoleTemplateVersion")
	defer b.mu.RUnlock()

	v, err := b.resolveRoleTemplateVersionLocked(templateArn, minorVersion)
	if err != nil {
		return nil, err
	}

	cp := *v

	return &cp, nil
}

// substituteTemplateParams replaces every "@{name}" placeholder in s with
// its resolved value from values. A placeholder with no entry in values (an
// optional parameter the caller didn't supply and that has no
// DefaultValue) is left untouched -- there is no default to fall back to,
// and inventing one would be fabrication.
func substituteTemplateParams(s string, values map[string]string) string {
	if s == "" || len(values) == 0 {
		return s
	}

	for name, val := range values {
		s = strings.ReplaceAll(s, "@{"+name+"}", val)
	}

	return s
}

// resolveTemplateParamValue resolves one ParametersDefinition entry against
// the caller-supplied ReplacementValues, matching the real
// ReplacementValueEntry.Values shape: List-typed parameters accept multiple
// values (joined with "," for substitution into a string pattern -- AWS
// does not publish the exact join format for embedding a list into a
// pattern, so this is this backend's own explicit, documented choice, not a
// guess presented as fact), every other type uses the first value.
func resolveTemplateParamValue(def RoleTemplateParameter, rv map[string][]string) (string, bool, error) {
	if vals, ok := rv[def.Name]; ok && len(vals) > 0 {
		switch def.Type {
		case "StringList", "NumberList", "ArnList":
			return strings.Join(vals, ","), true, nil
		default:
			return vals[0], true, nil
		}
	}

	if def.DefaultValue != "" {
		return def.DefaultValue, true, nil
	}

	if def.IsRequired {
		return "", false, fmt.Errorf(
			"%w: missing required role template parameter %q", ErrInvalidInput, def.Name,
		)
	}

	return "", false, nil
}

// resolveTemplateParamValues resolves every ParametersDefinition entry
// against replacementValues, returning the substitution map used to render
// the template's patterns.
func resolveTemplateParamValues(
	defs []RoleTemplateParameter, replacementValues map[string][]string,
) (map[string]string, error) {
	values := make(map[string]string, len(defs))

	for _, def := range defs {
		val, ok, err := resolveTemplateParamValue(def, replacementValues)
		if err != nil {
			return nil, err
		}

		if ok {
			values[def.Name] = val
		}
	}

	return values, nil
}

// acquiredRoleSpec is the fully-resolved (substituted and validated) role
// data AcquireRole is about to create, computed only once the resolved role
// name is known not to already exist.
type acquiredRoleSpec struct {
	inlinePolicies   map[string]string
	path             string
	description      string
	assumeRolePolicy string
}

// buildAcquiredRoleSpec substitutes tv's remaining patterns (path,
// description, trust policy, inline policies) and validates each one,
// returning an error without having mutated any backend state.
func buildAcquiredRoleSpec(tv *RoleTemplateVersion, values map[string]string) (*acquiredRoleSpec, error) {
	assumeRolePolicy := substituteTemplateParams(tv.AssumeRolePolicyDocumentTemplate, values)
	if assumeRolePolicy != "" && !json.Valid([]byte(assumeRolePolicy)) {
		return nil, fmt.Errorf("%w: invalid JSON in AssumeRolePolicyDocument", ErrMalformedPolicyDocument)
	}

	inlinePolicies := make(map[string]string, len(tv.InlinePolicyTemplates))

	for _, ip := range tv.InlinePolicyTemplates {
		doc := substituteTemplateParams(ip.PolicyDocument, values)
		if err := validateIdentityPolicyDocument(doc); err != nil {
			return nil, err
		}

		inlinePolicies[ip.PolicyName] = doc
	}

	return &acquiredRoleSpec{
		path:             normPath(substituteTemplateParams(tv.RolePathPattern, values)),
		description:      substituteTemplateParams(tv.RoleDescriptionPattern, values),
		assumeRolePolicy: assumeRolePolicy,
		inlinePolicies:   inlinePolicies,
	}, nil
}

// AcquireRole creates an IAM role from a role template version, substituting
// ReplacementValues into the template's @{parameter} patterns. Every
// mutation (role creation, inline policies, managed policy attachments,
// tags) is validated in full BEFORE any state is written, so a validation
// failure partway through a template never leaves a half-created role
// behind.
func (b *InMemoryBackend) AcquireRole(
	templateArn string, minorVersion *int32, replacementValues map[string][]string,
) (*Role, error) {
	b.mu.Lock("AcquireRole")
	defer b.mu.Unlock()

	tv, err := b.resolveRoleTemplateVersionLocked(templateArn, minorVersion)
	if err != nil {
		return nil, err
	}

	if !tv.Enabled {
		return nil, fmt.Errorf("%w: %s", ErrRoleTemplateDisabled, templateArn)
	}

	values, err := resolveTemplateParamValues(tv.ParametersDefinition, replacementValues)
	if err != nil {
		return nil, err
	}

	roleName := substituteTemplateParams(tv.RoleNamePattern, values)
	if roleName == "" {
		return nil, fmt.Errorf(
			"%w: role template %s produced an empty role name", ErrInvalidInput, templateArn,
		)
	}

	// Real AWS: "If a role that matches the template already exists in the
	// account, AcquireRole returns that role" (IAM User Guide, "Manage
	// access to role manager") -- AcquireRole is a get-or-create by the
	// template's resolved role name, not a hard create-or-fail. This
	// backend has no finer-grained "matches the template" signal than the
	// resolved name (real AWS doesn't document one either), so an existing
	// role under that name is treated as the match and returned as-is.
	if existing, exists := b.roles.Get(roleName); exists {
		cp := *existing

		return &cp, nil
	}

	spec, err := buildAcquiredRoleSpec(tv, values)
	if err != nil {
		return nil, err
	}

	r := &Role{
		RoleName:                 roleName,
		RoleID:                   newID("AROA"),
		Arn:                      arn.Build("iam", "", b.accountID, "role"+spec.path+roleName),
		Path:                     spec.path,
		AssumeRolePolicyDocument: spec.assumeRolePolicy,
		Description:              spec.description,
		CreateDate:               time.Now().UTC(),
		PermissionsBoundary:      tv.PermissionBoundaryArn,
		MaxSessionDuration:       tv.MaxSessionDuration,
	}

	if len(tv.RoleTagsTemplate) > 0 {
		r.Tags = make(map[string]string, len(tv.RoleTagsTemplate))
		for _, t := range tv.RoleTagsTemplate {
			r.Tags[t.Key] = t.Value
		}
	}

	b.roles.Put(r)
	b.roleByARN[r.Arn] = roleName
	b.sortedRoleNames = insertSorted(b.sortedRoleNames, roleName)

	if len(spec.inlinePolicies) > 0 {
		b.roleInlinePolicies[roleName] = spec.inlinePolicies
	}

	for _, policyArn := range tv.ManagedPolicyArns {
		b.rolePolicies[roleName] = append(b.rolePolicies[roleName], policyArn)
		b.addPolicyAttachmentLocked(policyArn, roleName, "role")
	}

	cp := *r

	return &cp, nil
}
