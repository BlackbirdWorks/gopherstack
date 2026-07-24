package iam

import "net/url"

func (h *Handler) iamGroupAttachedPolicyDispatchTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"AttachGroupPolicy": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.AttachGroupPolicy(vals.Get("GroupName"), vals.Get("PolicyArn")); err != nil {
				return nil, err
			}

			return &AttachGroupPolicyResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DetachGroupPolicy": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DetachGroupPolicy(vals.Get("GroupName"), vals.Get("PolicyArn")); err != nil {
				return nil, err
			}

			return &DetachGroupPolicyResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"ListAttachedGroupPolicies": func(vals url.Values, reqID string) (any, error) {
			policies, err := h.Backend.ListAttachedGroupPolicies(vals.Get("GroupName"))
			if err != nil {
				return nil, err
			}

			xmlPolicies := make([]AttachedPolicyXML, 0, len(policies))
			for _, p := range policies {
				xmlPolicies = append(xmlPolicies, AttachedPolicyXML(p))
			}

			return &ListAttachedGroupPoliciesResponse{
				Xmlns:                           iamXMLNS,
				ListAttachedGroupPoliciesResult: ListAttachedGroupPoliciesResult{AttachedPolicies: xmlPolicies},
				ResponseMetadata:                ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

func (h *Handler) iamGroupDispatchTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"CreateGroup": func(vals url.Values, reqID string) (any, error) {
			g, err := h.Backend.CreateGroup(vals.Get("GroupName"), vals.Get("Path"))
			if err != nil {
				return nil, err
			}

			return &CreateGroupResponse{
				Xmlns:             iamXMLNS,
				CreateGroupResult: CreateGroupResult{Group: toGroupXML(g)},
				ResponseMetadata:  ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteGroup": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteGroup(vals.Get("GroupName")); err != nil {
				return nil, err
			}

			return &DeleteGroupResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil
		},
		"AddUserToGroup": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.AddUserToGroup(vals.Get("GroupName"), vals.Get("UserName")); err != nil {
				return nil, err
			}

			return &AddUserToGroupResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil
		},
		"RemoveUserFromGroup": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.RemoveUserFromGroup(vals.Get("GroupName"), vals.Get("UserName")); err != nil {
				return nil, err
			}

			return &RemoveUserFromGroupResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"GetGroup": func(vals url.Values, reqID string) (any, error) {
			g, err := h.Backend.GetGroup(vals.Get("GroupName"))
			if err != nil {
				return nil, err
			}

			members, _ := h.Backend.GetGroupUsers(vals.Get("GroupName"))
			xmlUsers := make([]UserXML, 0, len(members))
			for i := range members {
				xmlUsers = append(xmlUsers, toUserXML(&members[i]))
			}

			return &GetGroupResponse{
				Xmlns: iamXMLNS,
				GetGroupResult: GetGroupResult{
					Group: toGroupXML(g),
					Users: xmlUsers,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		opListGroups: func(vals url.Values, reqID string) (any, error) {
			p, err := h.Backend.ListGroups(vals.Get("Marker"), parseMaxItems(vals.Get("MaxItems")))
			if err != nil {
				return nil, err
			}

			xmlGroups := make([]GroupXML, 0, len(p.Data))
			for i := range p.Data {
				xmlGroups = append(xmlGroups, toGroupXML(&p.Data[i]))
			}

			return &ListGroupsResponse{
				Xmlns: iamXMLNS,
				ListGroupsResult: ListGroupsResult{
					Groups:      xmlGroups,
					IsTruncated: p.Next != "",
					Marker:      p.Next,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

func toGroupXML(g *Group) GroupXML {
	return GroupXML{
		Path:       g.Path,
		GroupName:  g.GroupName,
		GroupID:    g.GroupID,
		Arn:        g.Arn,
		CreateDate: isoTime(g.CreateDate),
	}
}

func toGroupDetailXML(g GroupDetail) GroupDetailXML {
	return GroupDetailXML{
		Path:                    g.Path,
		GroupName:               g.GroupName,
		GroupID:                 g.GroupID,
		Arn:                     g.Arn,
		CreateDate:              isoTime(g.CreateDate),
		GroupPolicyList:         toInlinePolicyEntriesXML(g.InlinePolicies),
		AttachedManagedPolicies: toAttachedPoliciesXML(g.AttachedPolicies),
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
