package iam

import (
	"encoding/json"
	"maps"
	"net/url"
	"sort"
	"strconv"
)

func (h *Handler) iamInlinePolicyDispatchTable() map[string]iamActionFn {
	combined := make(map[string]iamActionFn)
	maps.Copy(combined, h.iamUserRoleInlinePolicyDispatchTable())
	maps.Copy(combined, h.iamGroupInlinePolicyDispatchTable())

	return combined
}

// iamUserRoleInlinePolicyDispatchTable returns dispatch entries for user and role inline policies.
func (h *Handler) iamUserRoleInlinePolicyDispatchTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"PutUserPolicy": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.PutUserPolicy(
				vals.Get("UserName"), vals.Get("PolicyName"), vals.Get("PolicyDocument"),
			); err != nil {
				return nil, err
			}

			return &PutUserPolicyResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil
		},
		"GetUserPolicy": func(vals url.Values, reqID string) (any, error) {
			doc, err := h.Backend.GetUserPolicy(vals.Get("UserName"), vals.Get("PolicyName"))
			if err != nil {
				return nil, err
			}

			return &GetUserPolicyResponse{
				Xmlns: iamXMLNS,
				GetUserPolicyResult: GetUserPolicyResult{
					UserName:       vals.Get("UserName"),
					PolicyName:     vals.Get("PolicyName"),
					PolicyDocument: encodePolicyDocument(doc),
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteUserPolicy": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteUserPolicy(vals.Get("UserName"), vals.Get("PolicyName")); err != nil {
				return nil, err
			}

			return &DeleteUserPolicyResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil
		},
		"ListUserPolicies": func(vals url.Values, reqID string) (any, error) {
			names, err := h.Backend.ListUserPolicies(vals.Get("UserName"))
			if err != nil {
				return nil, err
			}

			return &ListUserPoliciesResponse{
				Xmlns:                  iamXMLNS,
				ListUserPoliciesResult: ListUserPoliciesResult{PolicyNames: names},
				ResponseMetadata:       ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"PutRolePolicy": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.PutRolePolicy(
				vals.Get("RoleName"), vals.Get("PolicyName"), vals.Get("PolicyDocument"),
			); err != nil {
				return nil, err
			}

			return &PutRolePolicyResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil
		},
		"GetRolePolicy": func(vals url.Values, reqID string) (any, error) {
			doc, err := h.Backend.GetRolePolicy(vals.Get("RoleName"), vals.Get("PolicyName"))
			if err != nil {
				return nil, err
			}

			return &GetRolePolicyResponse{
				Xmlns: iamXMLNS,
				GetRolePolicyResult: GetRolePolicyResult{
					RoleName:       vals.Get("RoleName"),
					PolicyName:     vals.Get("PolicyName"),
					PolicyDocument: encodePolicyDocument(doc),
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteRolePolicy": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteRolePolicy(vals.Get("RoleName"), vals.Get("PolicyName")); err != nil {
				return nil, err
			}

			return &DeleteRolePolicyResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil
		},
	}
}

// iamGroupInlinePolicyDispatchTable returns dispatch entries for group inline policies.
func (h *Handler) iamGroupInlinePolicyDispatchTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"PutGroupPolicy": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.PutGroupPolicy(
				vals.Get("GroupName"), vals.Get("PolicyName"), vals.Get("PolicyDocument"),
			); err != nil {
				return nil, err
			}

			return &PutGroupPolicyResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil
		},
		"GetGroupPolicy": func(vals url.Values, reqID string) (any, error) {
			doc, err := h.Backend.GetGroupPolicy(vals.Get("GroupName"), vals.Get("PolicyName"))
			if err != nil {
				return nil, err
			}

			return &GetGroupPolicyResponse{
				Xmlns: iamXMLNS,
				GetGroupPolicyResult: GetGroupPolicyResult{
					GroupName:      vals.Get("GroupName"),
					PolicyName:     vals.Get("PolicyName"),
					PolicyDocument: encodePolicyDocument(doc),
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteGroupPolicy": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteGroupPolicy(vals.Get("GroupName"), vals.Get("PolicyName")); err != nil {
				return nil, err
			}

			return &DeleteGroupPolicyResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"ListGroupPolicies": func(vals url.Values, reqID string) (any, error) {
			names, err := h.Backend.ListGroupPolicies(vals.Get("GroupName"))
			if err != nil {
				return nil, err
			}

			return &ListGroupPoliciesResponse{
				Xmlns:                   iamXMLNS,
				ListGroupPoliciesResult: ListGroupPoliciesResult{PolicyNames: names},
				ResponseMetadata:        ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

func (h *Handler) iamPermissionBoundaryDispatchTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"PutUserPermissionsBoundary": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.PutUserPermissionsBoundary(
				vals.Get("UserName"),
				vals.Get("PermissionsBoundary"),
			); err != nil {
				return nil, err
			}

			return &PutUserPermissionsBoundaryResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteUserPermissionsBoundary": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteUserPermissionsBoundary(vals.Get("UserName")); err != nil {
				return nil, err
			}

			return &DeleteUserPermissionsBoundaryResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"PutRolePermissionsBoundary": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.PutRolePermissionsBoundary(
				vals.Get("RoleName"),
				vals.Get("PermissionsBoundary"),
			); err != nil {
				return nil, err
			}

			return &PutRolePermissionsBoundaryResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteRolePermissionsBoundary": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteRolePermissionsBoundary(vals.Get("RoleName")); err != nil {
				return nil, err
			}

			return &DeleteRolePermissionsBoundaryResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamRefinement2PermsBoundaryTable handles context key stub operations.
//
// NOTE: real IAM has no GetUserPermissionsBoundary / GetRolePermissionsBoundary
// actions — permissions-boundary info is returned as part of GetUser/GetRole
// (see UserXML.PermissionsBoundary / RoleXML.PermissionsBoundary in
// handler_users.go / handler_roles.go). A gopherstack-invented pair of getters
// with those exact response shapes previously lived here and has been removed.
func (h *Handler) iamRefinement2PermsBoundaryTable() map[string]iamActionFn {
	return map[string]iamActionFn{
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
