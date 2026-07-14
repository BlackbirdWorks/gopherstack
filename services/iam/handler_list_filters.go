package iam

import (
	"net/url"
	"strings"
)

// iamRefinement2ListTable provides PathPrefix-filtered overrides for ListUsers, ListRoles, ListGroups.
func (h *Handler) iamRefinement2ListTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		opListUsers: func(vals url.Values, reqID string) (any, error) {
			p, err := h.Backend.ListUsers(vals.Get("Marker"), parseMaxItems(vals.Get("MaxItems")))
			if err != nil {
				return nil, err
			}

			prefix := normPath(vals.Get("PathPrefix"))
			filtered := filterByPath(p.Data, prefix, func(u User) string { return u.Path })

			xmlUsers := make([]UserXML, 0, len(filtered))
			for i := range filtered {
				xmlUsers = append(xmlUsers, toUserXML(&filtered[i]))
			}

			return &ListUsersResponse{
				Xmlns: iamXMLNS,
				ListUsersResult: ListUsersResult{
					Users:       xmlUsers,
					IsTruncated: p.Next != "" && prefix == "/",
					Marker:      p.Next,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},

		opListRoles: func(vals url.Values, reqID string) (any, error) {
			p, err := h.Backend.ListRoles(vals.Get("Marker"), parseMaxItems(vals.Get("MaxItems")))
			if err != nil {
				return nil, err
			}

			prefix := normPath(vals.Get("PathPrefix"))
			filtered := filterByPath(p.Data, prefix, func(r Role) string { return r.Path })

			xmlRoles := make([]RoleXML, 0, len(filtered))
			for i := range filtered {
				xmlRoles = append(xmlRoles, toRoleXML(&filtered[i]))
			}

			return &ListRolesResponse{
				Xmlns: iamXMLNS,
				ListRolesResult: ListRolesResult{
					Roles:       xmlRoles,
					IsTruncated: p.Next != "" && prefix == "/",
					Marker:      p.Next,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},

		opListGroups: func(vals url.Values, reqID string) (any, error) {
			p, err := h.Backend.ListGroups(vals.Get("Marker"), parseMaxItems(vals.Get("MaxItems")))
			if err != nil {
				return nil, err
			}

			prefix := normPath(vals.Get("PathPrefix"))
			filtered := filterByPath(p.Data, prefix, func(g Group) string { return g.Path })

			xmlGroups := make([]GroupXML, 0, len(filtered))
			for i := range filtered {
				xmlGroups = append(xmlGroups, toGroupXML(&filtered[i]))
			}

			return &ListGroupsResponse{
				Xmlns: iamXMLNS,
				ListGroupsResult: ListGroupsResult{
					Groups:      xmlGroups,
					IsTruncated: p.Next != "" && prefix == "/",
					Marker:      p.Next,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamRefinement2ListTable2 provides PathPrefix-filtered overrides for ListPolicies and ListInstanceProfiles.
func (h *Handler) iamRefinement2ListTable2() map[string]iamActionFn {
	return map[string]iamActionFn{
		opListPolicies: func(vals url.Values, reqID string) (any, error) {
			return h.listPoliciesFiltered(vals, reqID)
		},

		opListInstanceProfiles: func(vals url.Values, reqID string) (any, error) {
			p, err := h.Backend.ListInstanceProfiles(vals.Get("Marker"), parseMaxItems(vals.Get("MaxItems")))
			if err != nil {
				return nil, err
			}

			prefix := normPath(vals.Get("PathPrefix"))
			filtered := filterByPath(p.Data, prefix, func(ip InstanceProfile) string { return ip.Path })

			xmlIPs := make([]InstanceProfileXML, 0, len(filtered))
			for i := range filtered {
				roles := h.resolveInstanceProfileRoles(&filtered[i])
				xmlIPs = append(xmlIPs, toInstanceProfileXML(&filtered[i], roles))
			}

			return &ListInstanceProfilesResponse{
				Xmlns: iamXMLNS,
				ListInstanceProfilesResult: ListInstanceProfilesResult{
					InstanceProfiles: xmlIPs,
					IsTruncated:      p.Next != "" && prefix == "/",
					Marker:           p.Next,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// listPoliciesFiltered handles ListPolicies with PathPrefix and Scope filtering.
func (h *Handler) listPoliciesFiltered(vals url.Values, reqID string) (any, error) {
	p, err := h.Backend.ListPolicies(vals.Get("Marker"), parseMaxItems(vals.Get("MaxItems")))
	if err != nil {
		return nil, err
	}

	prefix := normPath(vals.Get("PathPrefix"))
	scope := vals.Get("Scope")

	if scope == "" {
		scope = "Local"
	}

	policies := filterByPath(p.Data, prefix, func(pol Policy) string { return pol.Path })

	if scope != "All" {
		var scoped []Policy

		for _, pol := range policies {
			isAWS := strings.Contains(pol.Arn, ":aws:policy")
			if (scope == "AWS" && isAWS) || (scope == "Local" && !isAWS) {
				scoped = append(scoped, pol)
			}
		}

		policies = scoped
	}

	xmlPolicies := make([]PolicyXML, 0, len(policies))
	for i := range policies {
		xmlPolicies = append(xmlPolicies, toPolicyXML(&policies[i]))
	}

	return &ListPoliciesResponse{
		Xmlns: iamXMLNS,
		ListPoliciesResult: ListPoliciesResult{
			Policies:    xmlPolicies,
			IsTruncated: p.Next != "" && prefix == "/",
			Marker:      p.Next,
		},
		ResponseMetadata: ResponseMetadata{RequestID: reqID},
	}, nil
}

// filterByPath filters a slice of items to those whose path starts with prefix.
// When prefix is "/" (default) all items are returned.
func filterByPath[T any](items []T, prefix string, getPath func(T) string) []T {
	if prefix == "/" || prefix == "" {
		return items
	}

	out := make([]T, 0, len(items))
	for _, item := range items {
		if strings.HasPrefix(getPath(item), prefix) {
			out = append(out, item)
		}
	}

	return out
}
