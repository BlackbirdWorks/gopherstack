package iam

import (
	"fmt"
	"maps"
	"net/url"
	"strconv"
)

// iamRefinementDispatchTable returns the dispatch entries for all refinement-pass operations.
func (h *Handler) iamRefinementDispatchTable() map[string]iamActionFn {
	combined := make(map[string]iamActionFn)

	tables := []map[string]iamActionFn{
		h.iamAccessKeyRefinementDispatch(),
		h.iamAccountAliasRefinementDispatch(),
		h.iamGroupRefinementDispatch(),
		h.iamVirtualMFADispatch(),
		h.iamPolicyVersionMgmtDispatch(),
		h.iamPasswordPolicyDispatch(),
		h.iamEntitiesForPolicyDispatch(),
		h.iamServiceSpecificCredDispatch(),
		h.iamEntityUpdateDispatch(),
		h.iamOIDCRefinementDispatch(),
		h.iamInstanceProfileRefinementDispatch(),
		h.iamSimulateCustomPolicyDispatch(),
		h.iamServiceLinkedRoleStatusDispatch(),
		h.iamGroupTagsDispatch(),
	}

	for _, t := range tables {
		maps.Copy(combined, t)
	}

	return combined
}

// iamAccessKeyRefinementDispatch adds UpdateAccessKey and GetAccessKeyLastUsed.
func (h *Handler) iamAccessKeyRefinementDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"UpdateAccessKey": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.UpdateAccessKey(
				vals.Get("UserName"), vals.Get("AccessKeyId"), vals.Get("Status"),
			); err != nil {
				return nil, err
			}

			return &UpdateAccessKeyResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"GetAccessKeyLastUsed": func(vals url.Values, reqID string) (any, error) {
			info, err := h.Backend.GetAccessKeyLastUsed(vals.Get("AccessKeyId"))
			if err != nil {
				return nil, err
			}

			return &GetAccessKeyLastUsedResponse{
				Xmlns: iamXMLNS,
				GetAccessKeyLastUsedResult: GetAccessKeyLastUsedResult{
					UserName: info.UserName,
					AccessKeyLastUsed: AccessKeyLastUsedXML{
						LastUsedDate: info.LastUsedDate,
						ServiceName:  info.ServiceName,
						Region:       info.Region,
					},
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamAccountAliasRefinementDispatch adds ListAccountAliases and DeleteAccountAlias.
func (h *Handler) iamAccountAliasRefinementDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"ListAccountAliases": func(_ url.Values, reqID string) (any, error) {
			aliases := h.Backend.ListAccountAliases()

			return &ListAccountAliasesResponse{
				Xmlns: iamXMLNS,
				ListAccountAliasesResult: ListAccountAliasesResult{
					AccountAliases: aliases,
					IsTruncated:    false,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteAccountAlias": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteAccountAlias(vals.Get("AccountAlias")); err != nil {
				return nil, err
			}

			return &DeleteAccountAliasResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamGroupRefinementDispatch adds ListGroupsForUser and UpdateGroup.
func (h *Handler) iamGroupRefinementDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"ListGroupsForUser": func(vals url.Values, reqID string) (any, error) {
			groups, err := h.Backend.ListGroupsForUser(vals.Get("UserName"))
			if err != nil {
				return nil, err
			}

			xmlGroups := make([]ListGroupsForUserXML, 0, len(groups))
			for i := range groups {
				g := &groups[i]
				xmlGroups = append(xmlGroups, ListGroupsForUserXML{
					GroupName:  g.GroupName,
					GroupID:    g.GroupID,
					Arn:        g.Arn,
					Path:       g.Path,
					CreateDate: isoTime(g.CreateDate),
				})
			}

			return &ListGroupsForUserResponse{
				Xmlns: iamXMLNS,
				ListGroupsForUserResult: ListGroupsForUserResult{
					Groups:      xmlGroups,
					IsTruncated: false,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UpdateGroup": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.UpdateGroup(
				vals.Get("GroupName"), vals.Get("NewPath"), vals.Get("NewGroupName"),
			); err != nil {
				return nil, err
			}

			return &UpdateGroupResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamVirtualMFADispatch adds ListVirtualMFADevices and DeleteVirtualMFADevice.
func (h *Handler) iamVirtualMFADispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"ListVirtualMFADevices": func(vals url.Values, reqID string) (any, error) {
			p, err := h.Backend.ListVirtualMFADevices(
				vals.Get("Marker"), parseMaxItems(vals.Get("MaxItems")),
			)
			if err != nil {
				return nil, err
			}

			xmlDevices := make([]VirtualMFADeviceXML, 0, len(p.Data))
			for i := range p.Data {
				xmlDevices = append(xmlDevices, VirtualMFADeviceXML{
					SerialNumber: p.Data[i].SerialNumber,
				})
			}

			return &ListVirtualMFADevicesResponse{
				Xmlns: iamXMLNS,
				ListVirtualMFADevicesResult: ListVirtualMFADevicesResult{
					VirtualMFADevices: xmlDevices,
					IsTruncated:       p.Next != "",
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteVirtualMFADevice": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteVirtualMFADevice(vals.Get("SerialNumber")); err != nil {
				return nil, err
			}

			return &DeleteVirtualMFADeviceResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamPolicyVersionMgmtDispatch adds SetDefaultPolicyVersion and DeletePolicyVersion.
func (h *Handler) iamPolicyVersionMgmtDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"SetDefaultPolicyVersion": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.SetDefaultPolicyVersion(
				vals.Get("PolicyArn"), vals.Get("VersionId"),
			); err != nil {
				return nil, err
			}

			return &SetDefaultPolicyVersionResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeletePolicyVersion": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeletePolicyVersion(
				vals.Get("PolicyArn"), vals.Get("VersionId"),
			); err != nil {
				return nil, err
			}

			return &DeletePolicyVersionResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamPasswordPolicyDispatch adds password policy handlers:
// GetAccountPasswordPolicy, UpdateAccountPasswordPolicy, and DeleteAccountPasswordPolicy.
func (h *Handler) iamPasswordPolicyDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"GetAccountPasswordPolicy": func(_ url.Values, reqID string) (any, error) {
			pp := h.Backend.GetAccountPasswordPolicy()

			return &GetAccountPasswordPolicyResponse{
				Xmlns: iamXMLNS,
				GetAccountPasswordPolicyResult: GetAccountPasswordPolicyResult{
					PasswordPolicy: PasswordPolicyXML{
						MinimumPasswordLength:      pp.MinimumPasswordLength,
						MaxPasswordAge:             pp.MaxPasswordAge,
						PasswordReusePrevention:    pp.PasswordReusePrevention,
						RequireUppercaseCharacters: pp.RequireUppercaseCharacters,
						RequireLowercaseCharacters: pp.RequireLowercaseCharacters,
						RequireNumbers:             pp.RequireNumbers,
						RequireSymbols:             pp.RequireSymbols,
						AllowUsersToChangePassword: pp.AllowUsersToChangePassword,
						HardExpiry:                 pp.HardExpiry,
						ExpirePasswords:            pp.MaxPasswordAge > 0,
					},
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UpdateAccountPasswordPolicy": func(vals url.Values, reqID string) (any, error) {
			minLen, _ := strconv.Atoi(vals.Get("MinimumPasswordLength"))
			maxAge, _ := strconv.Atoi(vals.Get("MaxPasswordAge"))
			reusePrev, _ := strconv.Atoi(vals.Get("PasswordReusePrevention"))

			pp := PasswordPolicy{
				MinimumPasswordLength:      minLen,
				MaxPasswordAge:             maxAge,
				PasswordReusePrevention:    reusePrev,
				RequireUppercaseCharacters: vals.Get("RequireUppercaseCharacters") == formValueTrue,
				RequireLowercaseCharacters: vals.Get("RequireLowercaseCharacters") == formValueTrue,
				RequireNumbers:             vals.Get("RequireNumbers") == formValueTrue,
				RequireSymbols:             vals.Get("RequireSymbols") == formValueTrue,
				AllowUsersToChangePassword: vals.Get("AllowUsersToChangePassword") != "false",
				HardExpiry:                 vals.Get("HardExpiry") == formValueTrue,
			}
			if err := h.Backend.UpdateAccountPasswordPolicy(pp); err != nil {
				return nil, err
			}

			return &UpdateAccountPasswordPolicyResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteAccountPasswordPolicy": func(_ url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteAccountPasswordPolicy(); err != nil {
				return nil, err
			}

			return &DeleteAccountPasswordPolicyResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamEntitiesForPolicyDispatch adds ListEntitiesForPolicy.
func (h *Handler) iamEntitiesForPolicyDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"ListEntitiesForPolicy": func(vals url.Values, reqID string) (any, error) {
			entities, err := h.Backend.ListEntitiesForPolicy(
				vals.Get("PolicyArn"), vals.Get("EntityFilter"),
			)
			if err != nil {
				return nil, err
			}

			return &ListEntitiesForPolicyResponse{
				Xmlns: iamXMLNS,
				ListEntitiesForPolicyResult: ListEntitiesForPolicyResult{
					PolicyUsers:  entities.PolicyUsers,
					PolicyGroups: entities.PolicyGroups,
					PolicyRoles:  entities.PolicyRoles,
					IsTruncated:  false,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamServiceSpecificCredDispatch adds ListServiceSpecificCredentials and DeleteServiceSpecificCredential.
func (h *Handler) iamServiceSpecificCredDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"ListServiceSpecificCredentials": func(vals url.Values, reqID string) (any, error) {
			creds, err := h.Backend.ListServiceSpecificCredentials(
				vals.Get("UserName"), vals.Get("ServiceName"),
			)
			if err != nil {
				return nil, err
			}

			xmlCreds := make([]ServiceSpecificCredentialMetadataXML, 0, len(creds))
			for i := range creds {
				c := &creds[i]
				xmlCreds = append(xmlCreds, ServiceSpecificCredentialMetadataXML{
					UserName:                    c.UserName,
					ServiceName:                 c.ServiceName,
					ServiceUserName:             c.ServiceUserName,
					ServiceSpecificCredentialID: c.ServiceSpecificCredentialID,
					Status:                      c.Status,
					CreateDate:                  isoTime(c.CreateDate),
				})
			}

			return &ListServiceSpecificCredentialsResponse{
				Xmlns: iamXMLNS,
				Result: ListServiceSpecificCredentialsResult{
					ServiceSpecificCredentials: xmlCreds,
				},
				Meta: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteServiceSpecificCredential": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteServiceSpecificCredential(
				vals.Get("UserName"), vals.Get("ServiceSpecificCredentialId"),
			); err != nil {
				return nil, err
			}

			return &DeleteServiceSpecificCredentialResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamEntityUpdateDispatch adds UpdateUser, UpdateRole, and UpdateRoleDescription.
func (h *Handler) iamEntityUpdateDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"UpdateUser": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.UpdateUser(
				vals.Get("UserName"), vals.Get("NewPath"), vals.Get("NewUserName"),
			); err != nil {
				return nil, err
			}

			return &UpdateUserResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UpdateRole": func(vals url.Values, reqID string) (any, error) {
			roleName := vals.Get("RoleName")
			if path := vals.Get("Path"); path != "" {
				return nil, fmt.Errorf(
					"%w: role Path is immutable; use a new role to change the path",
					ErrInvalidAction,
				)
			}

			if err := h.Backend.UpdateRole(roleName, vals.Get("Description")); err != nil {
				return nil, err
			}

			r, err := h.Backend.GetRole(roleName)
			if err != nil {
				return nil, err
			}

			return &UpdateRoleResponse{
				Xmlns:            iamXMLNS,
				UpdateRoleResult: UpdateRoleResult{Role: toRoleXML(r)},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UpdateRoleDescription": func(vals url.Values, reqID string) (any, error) {
			roleName := vals.Get("RoleName")
			if err := h.Backend.UpdateRole(roleName, vals.Get("Description")); err != nil {
				return nil, err
			}

			r, err := h.Backend.GetRole(roleName)
			if err != nil {
				return nil, err
			}

			return &UpdateRoleDescriptionResponse{
				Xmlns:                       iamXMLNS,
				UpdateRoleDescriptionResult: UpdateRoleDescriptionResult{Role: toRoleXML(r)},
				ResponseMetadata:            ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamOIDCRefinementDispatch adds RemoveClientIDFromOpenIDConnectProvider.
func (h *Handler) iamOIDCRefinementDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"RemoveClientIDFromOpenIDConnectProvider": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.RemoveClientIDFromOpenIDConnectProvider(
				vals.Get("OpenIDConnectProviderArn"), vals.Get("ClientID"),
			); err != nil {
				return nil, err
			}

			return &RemoveClientIDFromOpenIDConnectProviderResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
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

// iamSimulateCustomPolicyDispatch adds SimulateCustomPolicy.
func (h *Handler) iamSimulateCustomPolicyDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"SimulateCustomPolicy": func(vals url.Values, reqID string) (any, error) {
			actionNames := parseIndexedValues(vals, "ActionNames.member.")
			resourceArns := parseIndexedValues(vals, "ResourceArns.member.")
			policyInputList := parseIndexedValues(vals, "PolicyInputList.member.")

			results, err := h.Backend.SimulateCustomPolicy(
				policyInputList, actionNames, resourceArns, parseConditionContext(vals),
			)
			if err != nil {
				return nil, err
			}

			return &SimulateCustomPolicyResponse{
				Xmlns: iamXMLNS,
				SimulateCustomPolicyResult: SimulateCustomPolicyResult{
					EvaluationResults: simResultsToXML(results),
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamServiceLinkedRoleStatusDispatch adds GetServiceLinkedRoleDeletionStatus.
func (h *Handler) iamServiceLinkedRoleStatusDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"GetServiceLinkedRoleDeletionStatus": func(vals url.Values, reqID string) (any, error) {
			status, err := h.Backend.GetServiceLinkedRoleDeletionStatus(vals.Get("DeletionTaskId"))
			if err != nil {
				return nil, err
			}

			return &GetServiceLinkedRoleDeletionStatusResponse{
				Xmlns: iamXMLNS,
				GetServiceLinkedRoleDeletionStatusResult: GetServiceLinkedRoleDeletionStatusResult{
					Status: status,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamGroupTagsDispatch returns an empty map; group tag operations are now handled
// in iamTagDispatchTable via the backend (TagGroup/UntagGroup/ListGroupTags).
func (h *Handler) iamGroupTagsDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{}
}
