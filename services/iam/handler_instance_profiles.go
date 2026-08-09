package iam

import (
	"net/url"
)

// handleListInstanceProfilesForRole implements ListInstanceProfilesForRole,
// returning every instance profile that actually contains the given role
// (via StorageBackend.ListInstanceProfilesForRole). Previously this action
// ignored the RoleName parameter and always returned an empty list.
func (h *Handler) handleListInstanceProfilesForRole(vals url.Values, reqID string) (any, error) {
	profiles, err := h.Backend.ListInstanceProfilesForRole(vals.Get("RoleName"))
	if err != nil {
		return nil, err
	}

	xmlProfiles := make([]InstanceProfileXML, 0, len(profiles))

	for i := range profiles {
		roles := h.resolveInstanceProfileRoles(&profiles[i])
		xmlProfiles = append(xmlProfiles, toInstanceProfileXML(&profiles[i], roles))
	}

	return &ListInstanceProfilesForRoleResponse{
		Xmlns:                             iamXMLNS,
		ListInstanceProfilesForRoleResult: ListInstanceProfilesForRoleResult{InstanceProfiles: xmlProfiles},
		ResponseMetadata:                  ResponseMetadata{RequestID: reqID},
	}, nil
}

func (h *Handler) iamInstanceProfileDispatchTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"CreateInstanceProfile": func(vals url.Values, reqID string) (any, error) {
			ip, err := h.Backend.CreateInstanceProfile(vals.Get("InstanceProfileName"), vals.Get("Path"))
			if err != nil {
				return nil, err
			}

			if tags := parseIAMTags(vals); len(tags) > 0 {
				h.setTags("ip:"+ip.InstanceProfileName, tags)
			}

			return &CreateInstanceProfileResponse{
				Xmlns: iamXMLNS,
				CreateInstanceProfileResult: CreateInstanceProfileResult{
					InstanceProfile: toInstanceProfileXML(ip, h.resolveInstanceProfileRoles(ip)),
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteInstanceProfile": func(vals url.Values, reqID string) (any, error) {
			name := vals.Get("InstanceProfileName")
			if err := h.Backend.DeleteInstanceProfile(name); err != nil {
				return nil, err
			}

			h.deleteTags("ip:" + name)

			return &DeleteInstanceProfileResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		opListInstanceProfiles: func(vals url.Values, reqID string) (any, error) {
			p, err := h.Backend.ListInstanceProfiles(vals.Get("Marker"), parseMaxItems(vals.Get("MaxItems")))
			if err != nil {
				return nil, err
			}

			xmlProfiles := make([]InstanceProfileXML, 0, len(p.Data))
			for i := range p.Data {
				xmlProfiles = append(
					xmlProfiles,
					toInstanceProfileXML(&p.Data[i], h.resolveInstanceProfileRoles(&p.Data[i])),
				)
			}

			return &ListInstanceProfilesResponse{
				Xmlns: iamXMLNS,
				ListInstanceProfilesResult: ListInstanceProfilesResult{
					InstanceProfiles: xmlProfiles,
					IsTruncated:      p.Next != "",
					Marker:           p.Next,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"AddRoleToInstanceProfile": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.AddRoleToInstanceProfile(
				vals.Get("InstanceProfileName"), vals.Get("RoleName"),
			); err != nil {
				return nil, err
			}

			return &AddRoleToInstanceProfileResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"RemoveRoleFromInstanceProfile": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.RemoveRoleFromInstanceProfile(
				vals.Get("InstanceProfileName"), vals.Get("RoleName"),
			); err != nil {
				return nil, err
			}

			return &RemoveRoleFromInstanceProfileResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

func toInstanceProfileXML(ip *InstanceProfile, roles []RoleXML) InstanceProfileXML {
	if roles == nil {
		roles = []RoleXML{}
	}

	return InstanceProfileXML{
		Path:                ip.Path,
		InstanceProfileName: ip.InstanceProfileName,
		InstanceProfileID:   ip.InstanceProfileID,
		Arn:                 ip.Arn,
		CreateDate:          isoTime(ip.CreateDate),
		Roles:               roles,
	}
}

// resolveInstanceProfileRoles looks up the full Role details for each role name
// in the instance profile, returning RoleXML entries. If a role no longer exists
// (deleted after the profile was created), a minimal entry with just the name is used.
func (h *Handler) resolveInstanceProfileRoles(ip *InstanceProfile) []RoleXML {
	roles := make([]RoleXML, 0, len(ip.Roles))

	for _, roleName := range ip.Roles {
		if r, err := h.Backend.GetRole(roleName); err == nil {
			roles = append(roles, toRoleXML(r))
		} else {
			roles = append(roles, RoleXML{RoleName: roleName})
		}
	}

	return roles
}

// iamInstanceProfileRefinementDispatch replaces the stub ListInstanceProfilesForRole
// with a real implementation and adds GetInstanceProfile.
func (h *Handler) iamInstanceProfileRefinementDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"GetInstanceProfile": func(vals url.Values, reqID string) (any, error) {
			ip, err := h.Backend.GetInstanceProfile(vals.Get("InstanceProfileName"))
			if err != nil {
				return nil, err
			}

			roles := h.resolveInstanceProfileRoles(ip)

			return &GetInstanceProfileResponse{
				Xmlns:                    iamXMLNS,
				GetInstanceProfileResult: GetInstanceProfileResult{InstanceProfile: toInstanceProfileXML(ip, roles)},
				ResponseMetadata:         ResponseMetadata{RequestID: reqID},
			}, nil
		},
		opListInstanceProfilesForRole: func(vals url.Values, reqID string) (any, error) {
			profiles, err := h.Backend.ListInstanceProfilesForRole(vals.Get("RoleName"))
			if err != nil {
				return nil, err
			}

			xmlProfiles := make([]InstanceProfileXML, 0, len(profiles))
			for i := range profiles {
				ip := &profiles[i]
				roles := h.resolveInstanceProfileRoles(ip)
				xmlProfiles = append(xmlProfiles, toInstanceProfileXML(ip, roles))
			}

			return &ListInstanceProfilesForRoleResponse{
				Xmlns: iamXMLNS,
				ListInstanceProfilesForRoleResult: ListInstanceProfilesForRoleResult{
					InstanceProfiles: xmlProfiles,
					IsTruncated:      false,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamInstanceProfileTagDispatch returns the tag dispatch entries for instance profiles.
func (h *Handler) iamInstanceProfileTagDispatch() map[string]iamActionFn {
	return h.resourceTagDispatch("InstanceProfile", "ip:", "InstanceProfileName")
}
