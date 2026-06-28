package iam

import (
	"encoding/json"
	"maps"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

// iamRefinement2DispatchTable merges PathPrefix-filtered list overrides,
// permissions-boundary getters, context-key stubs, and credential management
// operations.  Entries here override earlier dispatch tables.
func (h *Handler) iamRefinement2DispatchTable() map[string]iamActionFn {
	combined := make(map[string]iamActionFn)

	maps.Copy(combined, h.iamRefinement2ListTable())
	maps.Copy(combined, h.iamRefinement2ListTable2())
	maps.Copy(combined, h.iamRefinement2PermsBoundaryTable())
	maps.Copy(combined, h.iamRefinement2CredTable())

	return combined
}

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

// iamRefinement2PermsBoundaryTable handles permissions boundary getters and context key stubs.
func (h *Handler) iamRefinement2PermsBoundaryTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"GetUserPermissionsBoundary": func(vals url.Values, reqID string) (any, error) {
			u, err := h.Backend.GetUser(vals.Get("UserName"))
			if err != nil {
				return nil, err
			}

			var pb *PermissionsBoundaryXML
			if u.PermissionsBoundary != "" {
				pb = &PermissionsBoundaryXML{
					PermissionsBoundaryArn:  u.PermissionsBoundary,
					PermissionsBoundaryType: xmlElemPolicy,
				}
			}

			return &GetUserResponse{
				Xmlns:            iamXMLNS,
				GetUserResult:    GetUserResult{User: UserXML{PermissionsBoundary: pb}},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},

		"GetRolePermissionsBoundary": func(vals url.Values, reqID string) (any, error) {
			r, err := h.Backend.GetRole(vals.Get("RoleName"))
			if err != nil {
				return nil, err
			}

			var pb *PermissionsBoundaryXML
			if r.PermissionsBoundary != "" {
				pb = &PermissionsBoundaryXML{
					PermissionsBoundaryArn:  r.PermissionsBoundary,
					PermissionsBoundaryType: xmlElemPolicy,
				}
			}

			return &GetRoleResponse{
				Xmlns:            iamXMLNS,
				GetRoleResult:    GetRoleResult{Role: RoleXML{PermissionsBoundary: pb}},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},

		"GetContextKeysForCustomPolicy": func(vals url.Values, reqID string) (any, error) {
			keys := contextKeysFromPolicyDocuments(collectPolicyInputList(vals))

			return &GetContextKeysResponse{
				Xmlns:                iamXMLNS,
				GetContextKeysResult: GetContextKeysResult{ContextKeyNames: keys},
				ResponseMetadata:     ResponseMetadata{RequestID: reqID},
			}, nil
		},

		"GetContextKeysForPrincipalPolicy": func(_ url.Values, reqID string) (any, error) {
			return &GetContextKeysResponse{
				Xmlns:                iamXMLNS,
				GetContextKeysResult: GetContextKeysResult{ContextKeyNames: []string{}},
				ResponseMetadata:     ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamRefinement2CredTable handles credential management operations.
func (h *Handler) iamRefinement2CredTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"UpdateServiceSpecificCredential": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.UpdateServiceSpecificCredential(
				vals.Get("UserName"),
				vals.Get("ServiceSpecificCredentialId"),
				vals.Get("Status"),
			); err != nil {
				return nil, err
			}

			return &UpdateServiceSpecificCredentialResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},

		"GetMFADevice": func(vals url.Values, reqID string) (any, error) {
			serial := vals.Get("SerialNumber")

			dev, owner, err := h.Backend.GetVirtualMFADevice(serial)
			if err != nil {
				return nil, err
			}

			userName := owner
			if userName == "" {
				// Fall back to the UserName supplied in the request when the
				// device is not yet associated with a user.
				userName = vals.Get("UserName")
			}

			return &GetMFADeviceResponse{
				Xmlns: iamXMLNS,
				GetMFADeviceResult: GetMFADeviceResult{
					UserName:     userName,
					SerialNumber: dev.SerialNumber,
					EnableDate:   isoTime(dev.CreateDate),
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
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

// collectPolicyInputList gathers the policy documents supplied via the
// PolicyInputList.member.N indexed query parameters (used by
// GetContextKeysForCustomPolicy). A bare PolicyDocument parameter is also
// accepted as a convenience.
func collectPolicyInputList(vals url.Values) []string {
	var docs []string

	for i := 1; ; i++ {
		key := "PolicyInputList.member." + strconv.Itoa(i)

		doc := vals.Get(key)
		if doc == "" {
			break
		}

		docs = append(docs, doc)
	}

	if doc := vals.Get("PolicyDocument"); doc != "" {
		docs = append(docs, doc)
	}

	return docs
}

// contextKeysFromPolicyDocuments parses each supplied IAM policy document and
// extracts the distinct condition context keys (e.g. aws:username,
// aws:SourceIp) referenced under any statement's Condition block. The returned
// slice is sorted for deterministic output.
func contextKeysFromPolicyDocuments(docs []string) []string {
	seen := make(map[string]struct{})

	for _, doc := range docs {
		if doc == "" {
			continue
		}

		var parsed struct {
			Statement json.RawMessage `json:"Statement"`
		}

		if err := json.Unmarshal([]byte(doc), &parsed); err != nil {
			// Malformed documents are skipped; GetContextKeys is best-effort.
			continue
		}

		if len(parsed.Statement) == 0 {
			continue
		}

		stmts, err := decodeStatements(parsed.Statement)
		if err != nil {
			continue
		}

		for _, stmt := range stmts {
			// Condition operators map to context-key/value maps, e.g.
			// {"StringEquals": {"aws:username": "bob"}}.
			for _, ctxKeys := range stmt.Condition {
				for ctxKey := range ctxKeys {
					seen[ctxKey] = struct{}{}
				}
			}
		}
	}

	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}
