package iam

import "net/url"

// handleCreatePolicy implements CreatePolicy, including Tags.member.N
// tagging-at-creation (real AWS: "A list of tags that you want to attach to
// the new IAM customer managed policy"). The Policy wire type also lacked a
// Tags field entirely (real AWS's Policy type carries Tags), so even an
// out-of-band TagPolicy call was previously invisible on GetPolicy/ListPolicies.
func (h *Handler) handleCreatePolicy(vals url.Values, reqID string) (any, error) {
	pol, err := h.Backend.CreatePolicy(
		vals.Get("PolicyName"), vals.Get("Path"), vals.Get("PolicyDocument"),
	)
	if err != nil {
		return nil, err
	}

	if tags := parseIAMTags(vals); len(tags) > 0 {
		if tagErr := h.Backend.TagPolicy(pol.Arn, tags); tagErr != nil {
			return nil, tagErr
		}

		pol.Tags = tags
	}

	return &CreatePolicyResponse{
		Xmlns:              iamXMLNS,
		CreatePolicyResult: CreatePolicyResult{Policy: toPolicyXML(pol)},
		ResponseMetadata:   ResponseMetadata{RequestID: reqID},
	}, nil
}

func (h *Handler) iamPolicyBasicDispatchTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"CreatePolicy": h.handleCreatePolicy,
		"DeletePolicy": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeletePolicy(vals.Get("PolicyArn")); err != nil {
				return nil, err
			}

			return &DeletePolicyResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil
		},
		opListPolicies: func(vals url.Values, reqID string) (any, error) {
			p, err := h.Backend.ListPolicies(vals.Get("Marker"), parseMaxItems(vals.Get("MaxItems")))
			if err != nil {
				return nil, err
			}

			xmlPolicies := make([]PolicyXML, 0, len(p.Data))
			for i := range p.Data {
				xmlPolicies = append(xmlPolicies, toPolicyXML(&p.Data[i]))
			}

			return &ListPoliciesResponse{
				Xmlns: iamXMLNS,
				ListPoliciesResult: ListPoliciesResult{
					Policies:    xmlPolicies,
					IsTruncated: p.Next != "",
					Marker:      p.Next,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"GetPolicy": func(vals url.Values, reqID string) (any, error) {
			pol, err := h.Backend.GetPolicy(vals.Get("PolicyArn"))
			if err != nil {
				return nil, err
			}

			return &GetPolicyResponse{
				Xmlns:            iamXMLNS,
				GetPolicyResult:  GetPolicyResult{Policy: toPolicyXML(pol)},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"GetPolicyVersion": func(vals url.Values, reqID string) (any, error) {
			pv, err := h.Backend.GetPolicyVersion(vals.Get("PolicyArn"), vals.Get("VersionId"))
			if err != nil {
				return nil, err
			}

			return &GetPolicyVersionResponse{
				Xmlns: iamXMLNS,
				GetPolicyVersionResult: GetPolicyVersionResult{PolicyVersion: PolicyVersionXML{
					Document:         encodePolicyDocument(pv.PolicyDocument),
					VersionID:        pv.VersionID,
					IsDefaultVersion: pv.IsDefaultVersion,
					CreateDate:       isoTime(pv.CreateDate),
				}},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"ListPolicyVersions": func(vals url.Values, reqID string) (any, error) {
			versions, err := h.Backend.ListPolicyVersions(vals.Get("PolicyArn"))
			if err != nil {
				return nil, err
			}

			xmlVersions := make([]PolicyVersionXML, 0, len(versions))
			for i := range versions {
				xmlVersions = append(xmlVersions, PolicyVersionXML{
					VersionID:        versions[i].VersionID,
					CreateDate:       isoTime(versions[i].CreateDate),
					IsDefaultVersion: versions[i].IsDefaultVersion,
				})
			}

			return &ListPolicyVersionsResponse{
				Xmlns: iamXMLNS,
				ListPolicyVersionsResult: ListPolicyVersionsResult{
					Versions: xmlVersions,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

func (h *Handler) iamPolicyAttachDispatchTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"AttachUserPolicy": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.AttachUserPolicy(vals.Get("UserName"), vals.Get("PolicyArn")); err != nil {
				return nil, err
			}

			return &AttachUserPolicyResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil
		},
		"DetachUserPolicy": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DetachUserPolicy(vals.Get("UserName"), vals.Get("PolicyArn")); err != nil {
				return nil, err
			}

			return &DetachUserPolicyResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil
		},
		"AttachRolePolicy": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.AttachRolePolicy(vals.Get("RoleName"), vals.Get("PolicyArn")); err != nil {
				return nil, err
			}

			return &AttachRolePolicyResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil
		},
		"DetachRolePolicy": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DetachRolePolicy(vals.Get("RoleName"), vals.Get("PolicyArn")); err != nil {
				return nil, err
			}

			return &DetachRolePolicyResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil
		},
		"ListAttachedUserPolicies": h.handleListAttachedUserPolicies,
		"ListAttachedRolePolicies": h.handleListAttachedRolePolicies,
		"ListRolePolicies": func(vals url.Values, reqID string) (any, error) {
			names, err := h.Backend.ListRolePolicies(vals.Get("RoleName"))
			if err != nil {
				return nil, err
			}

			return &ListRolePoliciesResponse{
				Xmlns:                  iamXMLNS,
				ListRolePoliciesResult: ListRolePoliciesResult{PolicyNames: names},
				ResponseMetadata:       ResponseMetadata{RequestID: reqID},
			}, nil
		},
		opListInstanceProfilesForRole: h.handleListInstanceProfilesForRole,
	}
}

func (h *Handler) handleListAttachedUserPolicies(vals url.Values, reqID string) (any, error) {
	policies, err := h.Backend.ListAttachedUserPolicies(vals.Get("UserName"))
	if err != nil {
		return nil, err
	}

	pg, err := h.listAttachedPoliciesFiltered(policies, vals)
	if err != nil {
		return nil, err
	}

	xmlPolicies := make([]AttachedPolicyXML, 0, len(pg.Data))
	for _, p := range pg.Data {
		xmlPolicies = append(xmlPolicies, AttachedPolicyXML(p))
	}

	return &ListAttachedUserPoliciesResponse{
		Xmlns: iamXMLNS,
		ListAttachedUserPoliciesResult: ListAttachedUserPoliciesResult{
			AttachedPolicies: xmlPolicies,
			IsTruncated:      pg.Next != "",
			Marker:           pg.Next,
		},
		ResponseMetadata: ResponseMetadata{RequestID: reqID},
	}, nil
}

func (h *Handler) handleListAttachedRolePolicies(vals url.Values, reqID string) (any, error) {
	policies, err := h.Backend.ListAttachedRolePolicies(vals.Get("RoleName"))
	if err != nil {
		return nil, err
	}

	pg, err := h.listAttachedPoliciesFiltered(policies, vals)
	if err != nil {
		return nil, err
	}

	xmlPolicies := make([]AttachedPolicyXML, 0, len(pg.Data))
	for _, p := range pg.Data {
		xmlPolicies = append(xmlPolicies, AttachedPolicyXML(p))
	}

	return &ListAttachedRolePoliciesResponse{
		Xmlns: iamXMLNS,
		ListAttachedRolePoliciesResult: ListAttachedRolePoliciesResult{
			AttachedPolicies: xmlPolicies,
			IsTruncated:      pg.Next != "",
			Marker:           pg.Next,
		},
		ResponseMetadata: ResponseMetadata{RequestID: reqID},
	}, nil
}

func toPolicyXML(p *Policy) PolicyXML {
	defaultVersionID := p.DefaultVersionID
	if defaultVersionID == "" {
		defaultVersionID = "v1"
	}

	updateDate := p.UpdateDate
	if updateDate.IsZero() {
		updateDate = p.CreateDate
	}

	return PolicyXML{
		PolicyName:       p.PolicyName,
		PolicyID:         p.PolicyID,
		Arn:              p.Arn,
		Path:             p.Path,
		CreateDate:       isoTime(p.CreateDate),
		UpdateDate:       isoTime(updateDate),
		DefaultVersionID: defaultVersionID,
		Tags:             tagsToXML(p.Tags),
		AttachmentCount:  p.AttachmentCount,
		IsAttachable:     p.IsAttachable,
	}
}

// toManagedPolicyDetailXML builds the ManagedPolicyDetail XML element for
// GetAccountAuthorizationDetails. versions is the full, real version list for
// the policy (as returned by StorageBackend.ListPolicyVersions) — real AWS
// includes every stored version here, not just the default, and each
// version's Document is URL-encoded like GetPolicyVersion.
func toManagedPolicyDetailXML(p *Policy, versions []StoredPolicyVersion) ManagedPolicyDetailXML {
	xmlVersions := make([]PolicyVersionXML, 0, len(versions))
	for _, v := range versions {
		xmlVersions = append(xmlVersions, PolicyVersionXML{
			Document:         encodePolicyDocument(v.PolicyDocument),
			VersionID:        v.VersionID,
			IsDefaultVersion: v.IsDefaultVersion,
			CreateDate:       isoTime(v.CreateDate),
		})
	}

	return ManagedPolicyDetailXML{
		PolicyName:        p.PolicyName,
		PolicyID:          p.PolicyID,
		Arn:               p.Arn,
		Path:              p.Path,
		CreateDate:        isoTime(p.CreateDate),
		PolicyVersionList: xmlVersions,
	}
}

// iamNewOpsPolicyActions returns dispatch entries for policy version new operations.
func (h *Handler) iamNewOpsPolicyActions() map[string]iamActionFn {
	return map[string]iamActionFn{
		"CreatePolicyVersion": func(vals url.Values, reqID string) (any, error) {
			setAsDefault := vals.Get("SetAsDefault") == formValueTrue

			pv, err := h.Backend.CreatePolicyVersion(
				vals.Get("PolicyArn"),
				vals.Get("PolicyDocument"),
				setAsDefault,
			)
			if err != nil {
				return nil, err
			}

			return &CreatePolicyVersionResponse{
				Xmlns: iamXMLNS,
				CreatePolicyVersionResult: CreatePolicyVersionResult{
					PolicyVersion: PolicyVersionXML{
						VersionID:        pv.VersionID,
						IsDefaultVersion: pv.IsDefaultVersion,
						CreateDate:       isoTime(pv.CreateDate),
					},
				},
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

// iamSimulateCustomPolicyDispatch adds SimulateCustomPolicy.
func (h *Handler) iamSimulateCustomPolicyDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"SimulateCustomPolicy": func(vals url.Values, reqID string) (any, error) {
			actionNames := parseIndexedValues(vals, "ActionNames.member.")
			resourceArns := parseIndexedValues(vals, "ResourceArns.member.")
			policyInputList := parseIndexedValues(vals, "PolicyInputList.member.")
			permissionsBoundaryPolicyInputList := parseIndexedValues(vals, "PermissionsBoundaryPolicyInputList.member.")

			results, err := h.Backend.SimulateCustomPolicy(
				policyInputList,
				permissionsBoundaryPolicyInputList,
				actionNames,
				resourceArns,
				parseConditionContext(vals),
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
