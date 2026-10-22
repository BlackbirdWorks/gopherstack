package connect

import (
	"context"
	"encoding/json"
	"slices"

	"github.com/google/uuid"
)

const maxPermissions = 500

// CreateSecurityProfileInput is the CreateSecurityProfile request.
type CreateSecurityProfileInput struct {
	AllowedAccessControlTags map[string]string
	Tags                     map[string]string
	SecurityProfileName      string
	instPath
	AllowedAccessControlHierarchyGroupID string `json:"AllowedAccessControlHierarchyGroupId"`
	Description                          string
	AllowedAIAgents                      json.RawMessage
	Applications                         json.RawMessage
	AllowedFlowModules                   json.RawMessage
	HierarchyRestrictedResources         []string
	Permissions                          []string
	TagRestrictedResources               []string
	GranularAccessControlConfiguration   json.RawMessage
}

// UpdateSecurityProfileInput is the UpdateSecurityProfile request.
type UpdateSecurityProfileInput struct {
	AllowedAccessControlTags             map[string]string
	Description                          *string
	AllowedAccessControlHierarchyGroupID *string `json:"AllowedAccessControlHierarchyGroupId"`
	ProfileID                            string  `json:"-"                                    path:"SecurityProfileId"`
	instPath
	GranularAccessControlConfiguration json.RawMessage
	AllowedAIAgents                    json.RawMessage
	AllowedFlowModules                 json.RawMessage
	Applications                       json.RawMessage
	HierarchyRestrictedResources       []string
	Permissions                        []string
	TagRestrictedResources             []string
}

func (b *Backend) checkHierarchyRef(inst, groupID string) error {
	if groupID != "" && !b.hierarchyGroups.has(inst, groupID) {
		return notFoundErr("HierarchyGroup", groupID)
	}

	return nil
}

// CreateSecurityProfile creates a security profile.
func (b *Backend) CreateSecurityProfile(
	ctx context.Context,
	in *CreateSecurityProfileInput,
) (*securityProfileOut, error) {
	if err := checkName("SecurityProfileName", in.SecurityProfileName, maxNameLen); err != nil {
		return nil, err
	}

	if len(in.Permissions) > maxPermissions {
		return nil, invalidParamErr("Permissions can hold at most %d entries", maxPermissions)
	}

	if err := firstError(
		checkDescription(in.Description),
		checkTags(in.Tags),
	); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateSecurityProfile")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	inst := r.Instance.ID

	if err = b.checkHierarchyRef(inst, in.AllowedAccessControlHierarchyGroupID); err != nil {
		return nil, err
	}

	if nameExists(
		b.securityProfiles,
		inst,
		func(p *securityProfileRec) string { return p.Profile.SecurityProfileName },
		in.SecurityProfileName,
		"",
	) {
		return nil, duplicateErr("SecurityProfile", in.SecurityProfileName)
	}

	extras, err := b.checkSecurityExtras(inst, in.AllowedAIAgents, in.Applications, in.AllowedFlowModules)
	if err != nil {
		return nil, err
	}

	rec := b.newSecurityProfile(ctx, inst, in.SecurityProfileName, in.Description, in.Permissions)
	rec.Extras = extras
	rec.Profile.Tags = cloneTags(in.Tags)
	rec.Profile.AllowedAccessControlTags = cloneTags(in.AllowedAccessControlTags)
	rec.Profile.AllowedAccessControlHierarchyGroupID = in.AllowedAccessControlHierarchyGroupID
	rec.Profile.HierarchyRestrictedResources = in.HierarchyRestrictedResources
	rec.Profile.TagRestrictedResources = in.TagRestrictedResources
	rec.Profile.GranularAccessControlConfiguration = in.GranularAccessControlConfiguration
	b.securityProfiles.put(rec)

	return &securityProfileOut{SecurityProfileArn: rec.Profile.Arn, SecurityProfileID: rec.Profile.ID}, nil
}

func (b *Backend) newSecurityProfile(ctx context.Context, inst, name, desc string, perms []string) *securityProfileRec {
	id := uuid.NewString()

	return &securityProfileRec{InstanceID: inst, Permissions: slices.Clone(perms), Profile: SecurityProfile{
		Stamp: b.stamp(ctx), Arn: b.buildARN(ctx, "instance/"+inst+"/security-profile/"+id), Description: desc,
		ID: id, SecurityProfileName: name,
	}}
}

// DescribeSecurityProfile returns a security profile.
func (b *Backend) DescribeSecurityProfile(inst, id string) (*SecurityProfile, error) {
	b.mu.RLock("DescribeSecurityProfile")
	defer b.mu.RUnlock()

	r, err := b.instanceLocked(inst)
	if err != nil {
		return nil, err
	}

	p, err := b.securityProfiles.get(r.Instance.ID, id)
	if err != nil {
		return nil, err
	}

	out := p.Profile
	out.Tags = cloneTags(out.Tags)

	return &out, nil
}

// ListSecurityProfiles pages through security profiles.
func (b *Backend) ListSecurityProfiles(inst, token string, maxResults int32) ([]Summary, string, error) {
	b.mu.RLock("ListSecurityProfiles")

	r, err := b.instanceLocked(inst)
	if err != nil {
		b.mu.RUnlock()

		return nil, "", err
	}

	var all []Summary
	for _, p := range b.securityProfiles.in(r.Instance.ID) {
		all = append(
			all,
			Summary{Stamp: p.Profile.Stamp, Arn: p.Profile.Arn, ID: p.Profile.ID, Name: p.Profile.SecurityProfileName},
		)
	}

	b.mu.RUnlock()

	return paged(all, token, maxResults)
}

// ListSecurityProfilePermissions pages through a profile's permissions.
func (b *Backend) ListSecurityProfilePermissions(inst, id, token string, maxResults int32) ([]string, string, error) {
	b.mu.RLock("ListSecurityProfilePermissions")
	defer b.mu.RUnlock()

	r, err := b.instanceLocked(inst)
	if err != nil {
		return nil, "", err
	}

	p, err := b.securityProfiles.get(r.Instance.ID, id)
	if err != nil {
		return nil, "", err
	}

	return paged(slices.Clone(p.Permissions), token, maxResults)
}

// UpdateSecurityProfile changes the supplied settings of a security profile.
func (b *Backend) UpdateSecurityProfile(ctx context.Context, in *UpdateSecurityProfileInput) (*empty, error) {
	if len(in.Permissions) > maxPermissions {
		return nil, invalidParamErr("Permissions can hold at most %d entries", maxPermissions)
	}

	if in.Description != nil {
		if err := checkDescription(*in.Description); err != nil {
			return nil, err
		}
	}

	b.mu.Lock("UpdateSecurityProfile")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	p, err := b.securityProfiles.get(r.Instance.ID, in.ProfileID)
	if err != nil {
		return nil, err
	}

	extras, err := b.checkSecurityExtras(p.InstanceID, in.AllowedAIAgents, in.Applications, in.AllowedFlowModules)
	if err != nil {
		return nil, err
	}

	if in.AllowedAccessControlHierarchyGroupID != nil {
		if err = b.checkHierarchyRef(p.InstanceID, *in.AllowedAccessControlHierarchyGroupID); err != nil {
			return nil, err
		}

		p.Profile.AllowedAccessControlHierarchyGroupID = *in.AllowedAccessControlHierarchyGroupID
	}

	p.Extras.merge(extras)
	setIf(&p.Profile.Description, in.Description)

	if in.Permissions != nil {
		p.Permissions = slices.Clone(in.Permissions)
	}

	if in.AllowedAccessControlTags != nil {
		p.Profile.AllowedAccessControlTags = cloneTags(in.AllowedAccessControlTags)
	}

	if in.HierarchyRestrictedResources != nil {
		p.Profile.HierarchyRestrictedResources = in.HierarchyRestrictedResources
	}

	if in.TagRestrictedResources != nil {
		p.Profile.TagRestrictedResources = in.TagRestrictedResources
	}

	if in.GranularAccessControlConfiguration != nil {
		p.Profile.GranularAccessControlConfiguration = in.GranularAccessControlConfiguration
	}

	p.Profile.Stamp = b.stamp(ctx)

	return &empty{}, nil
}

// DeleteSecurityProfile removes a security profile no user holds.
func (b *Backend) DeleteSecurityProfile(inst, id string) (*empty, error) {
	b.mu.Lock("DeleteSecurityProfile")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(inst)
	if err != nil {
		return nil, err
	}

	if slices.ContainsFunc(
		b.users.in(r.Instance.ID),
		func(u *userRec) bool { return slices.Contains(u.User.SecurityProfileIDs, id) },
	) {
		return nil, inUseErr("SecurityProfile %q is assigned to a user", id)
	}

	return nil, b.securityProfiles.del(r.Instance.ID, id)
}
