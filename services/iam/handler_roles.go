package iam

import (
	"fmt"
	"net/url"
	"strconv"
)

// handleCreateRole implements CreateRole, including the optional
// MaxSessionDuration validation/update and Tags.member.N tagging-at-creation
// (real AWS: "A list of tags that you want to attach to the new role").
func (h *Handler) handleCreateRole(vals url.Values, reqID string) (any, error) {
	r, err := h.Backend.CreateRole(
		vals.Get("RoleName"),
		vals.Get("Path"),
		vals.Get("AssumeRolePolicyDocument"),
		vals.Get("PermissionsBoundary"),
	)
	if err != nil {
		return nil, err
	}

	if msd := vals.Get("MaxSessionDuration"); msd != "" {
		d, parseErr := strconv.ParseInt(msd, 10, 32)
		if parseErr != nil || d < minMaxSessionDuration || d > maxMaxSessionDuration {
			return nil, fmt.Errorf(
				"%w: MaxSessionDuration must be between %d and %d",
				ErrValidationError, minMaxSessionDuration, maxMaxSessionDuration,
			)
		}

		if updateErr := h.Backend.UpdateRoleMaxSessionDuration(r.RoleName, int32(d)); updateErr != nil {
			return nil, fmt.Errorf("updating max session duration for role %s: %w", r.RoleName, updateErr)
		}

		r.MaxSessionDuration = int32(d)
	}

	if tags := parseIAMTags(vals); len(tags) > 0 {
		if tagErr := h.Backend.TagRole(r.RoleName, tags); tagErr != nil {
			return nil, tagErr
		}

		r.Tags = tags
	}

	return &CreateRoleResponse{
		Xmlns:            iamXMLNS,
		CreateRoleResult: CreateRoleResult{Role: toRoleXML(r)},
		ResponseMetadata: ResponseMetadata{RequestID: reqID},
	}, nil
}

func (h *Handler) iamRoleDispatchTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"CreateRole": h.handleCreateRole,
		"GetRole": func(vals url.Values, reqID string) (any, error) {
			r, err := h.Backend.GetRole(vals.Get("RoleName"))
			if err != nil {
				return nil, err
			}

			return &GetRoleResponse{
				Xmlns:            iamXMLNS,
				GetRoleResult:    GetRoleResult{Role: toRoleXML(r)},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteRole": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteRole(vals.Get("RoleName")); err != nil {
				return nil, err
			}

			return &DeleteRoleResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil
		},
		opListRoles: func(vals url.Values, reqID string) (any, error) {
			p, err := h.Backend.ListRoles(vals.Get("Marker"), parseMaxItems(vals.Get("MaxItems")))
			if err != nil {
				return nil, err
			}

			xmlRoles := make([]RoleXML, 0, len(p.Data))
			for i := range p.Data {
				xmlRoles = append(xmlRoles, toRoleXML(&p.Data[i]))
			}

			return &ListRolesResponse{
				Xmlns: iamXMLNS,
				ListRolesResult: ListRolesResult{
					Roles:       xmlRoles,
					IsTruncated: p.Next != "",
					Marker:      p.Next,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

func (h *Handler) iamOtherOperationsDispatchTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"UpdateAssumeRolePolicy": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.UpdateAssumeRolePolicy(vals.Get("RoleName"), vals.Get("PolicyDocument")); err != nil {
				return nil, err
			}

			return &UpdateAssumeRolePolicyResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

func toRoleXML(r *Role) RoleXML {
	x := RoleXML{
		Path:                     r.Path,
		RoleName:                 r.RoleName,
		RoleID:                   r.RoleID,
		Arn:                      r.Arn,
		CreateDate:               isoTime(r.CreateDate),
		AssumeRolePolicyDocument: encodePolicyDocument(r.AssumeRolePolicyDocument),
		MaxSessionDuration:       r.MaxSessionDuration,
		Description:              r.Description,
		Tags:                     tagsToXML(r.Tags),
	}

	if r.PermissionsBoundary != "" {
		x.PermissionsBoundary = &PermissionsBoundaryXML{
			PermissionsBoundaryArn:  r.PermissionsBoundary,
			PermissionsBoundaryType: xmlElemPolicy,
		}
	}

	return x
}

// toRoleDetailXML converts a RoleDetail to its XML shape for
// GetAccountAuthorizationDetails, including the role's instance profiles
// (each with its own resolved Roles list, matching ListInstanceProfilesForRole
// and GetInstanceProfile). It is a Handler method (not a free function) because
// resolving each instance profile's member roles requires backend lookups —
// safe here since the backend's read lock, held only inside
// InMemoryBackend.GetAccountAuthorizationDetails, has already been released by
// the time the handler converts the result to XML.
func (h *Handler) toRoleDetailXML(r RoleDetail) RoleDetailXML {
	profiles := make([]InstanceProfileXML, 0, len(r.InstanceProfiles))

	for i := range r.InstanceProfiles {
		ip := &r.InstanceProfiles[i]
		profiles = append(profiles, toInstanceProfileXML(ip, h.resolveInstanceProfileRoles(ip)))
	}

	x := RoleDetailXML{
		Path:                     r.Path,
		RoleName:                 r.RoleName,
		RoleID:                   r.RoleID,
		Arn:                      r.Arn,
		CreateDate:               isoTime(r.CreateDate),
		AssumeRolePolicyDocument: encodePolicyDocument(r.AssumeRolePolicyDocument),
		RolePolicyList:           toInlinePolicyEntriesXML(r.InlinePolicies),
		AttachedManagedPolicies:  toAttachedPoliciesXML(r.AttachedPolicies),
		InstanceProfileList:      profiles,
		Tags:                     tagsToXML(r.Tags),
	}

	if r.PermissionsBoundary != "" {
		x.PermissionsBoundary = &PermissionsBoundaryXML{
			PermissionsBoundaryArn:  r.PermissionsBoundary,
			PermissionsBoundaryType: xmlElemPolicy,
		}
	}

	return x
}
