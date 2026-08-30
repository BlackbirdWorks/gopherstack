package iam

import (
	"math"
	"net/url"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// fetchAllMaxItems is passed to a backend List* method when this file needs
// every item in one call to filter and re-paginate locally: the backend's own
// Marker/MaxItems window is over the UNFILTERED sorted-name order, so a
// PathPrefix/OnlyAttached/PolicyUsageFilter match can straddle backend pages
// in a way that would silently drop results (or falsely report IsTruncated)
// if filtering ran after that window was already cut.
const fetchAllMaxItems = math.MaxInt32

// iamRefinement2ListTable provides PathPrefix-filtered overrides for ListUsers, ListRoles, ListGroups.
func (h *Handler) iamRefinement2ListTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		opListUsers: func(vals url.Values, reqID string) (any, error) {
			prefix := normPath(vals.Get("PathPrefix"))

			pg, err := filteredPage(h.Backend.ListUsers, prefix, vals, func(u User) string { return u.Path })
			if err != nil {
				return nil, err
			}

			xmlUsers := make([]UserXML, 0, len(pg.Data))
			for i := range pg.Data {
				xmlUsers = append(xmlUsers, toUserXML(&pg.Data[i]))
			}

			return &ListUsersResponse{
				Xmlns: iamXMLNS,
				ListUsersResult: ListUsersResult{
					Users:       xmlUsers,
					IsTruncated: pg.Next != "",
					Marker:      pg.Next,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},

		opListRoles: func(vals url.Values, reqID string) (any, error) {
			prefix := normPath(vals.Get("PathPrefix"))

			pg, err := filteredPage(h.Backend.ListRoles, prefix, vals, func(r Role) string { return r.Path })
			if err != nil {
				return nil, err
			}

			xmlRoles := make([]RoleXML, 0, len(pg.Data))
			for i := range pg.Data {
				xmlRoles = append(xmlRoles, toRoleXML(&pg.Data[i]))
			}

			return &ListRolesResponse{
				Xmlns: iamXMLNS,
				ListRolesResult: ListRolesResult{
					Roles:       xmlRoles,
					IsTruncated: pg.Next != "",
					Marker:      pg.Next,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},

		opListGroups: func(vals url.Values, reqID string) (any, error) {
			prefix := normPath(vals.Get("PathPrefix"))

			pg, err := filteredPage(h.Backend.ListGroups, prefix, vals, func(g Group) string { return g.Path })
			if err != nil {
				return nil, err
			}

			xmlGroups := make([]GroupXML, 0, len(pg.Data))
			for i := range pg.Data {
				xmlGroups = append(xmlGroups, toGroupXML(&pg.Data[i]))
			}

			return &ListGroupsResponse{
				Xmlns: iamXMLNS,
				ListGroupsResult: ListGroupsResult{
					Groups:      xmlGroups,
					IsTruncated: pg.Next != "",
					Marker:      pg.Next,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// filteredPage returns a correctly paginated page for a PathPrefix-filtered
// listing. When prefix is the default "/" it passes the backend's own
// Marker/MaxItems window through unchanged (matching prior behavior exactly).
// Otherwise it fetches every item, filters by path, and re-paginates the
// filtered slice with pkgs/page so Marker/IsTruncated reflect the filtered
// result set rather than the backend's unfiltered window.
func filteredPage[T any](
	list func(marker string, maxItems int) (page.Page[T], error),
	prefix string,
	vals url.Values,
	getPath func(T) string,
) (page.Page[T], error) {
	if prefix == "/" {
		return list(vals.Get("Marker"), parseMaxItems(vals.Get("MaxItems")))
	}

	full, err := list("", fetchAllMaxItems)
	if err != nil {
		return page.Page[T]{}, err
	}

	filtered := filterByPath(full.Data, prefix, getPath)

	return page.New(filtered, vals.Get("Marker"), parseMaxItems(vals.Get("MaxItems")), iamDefaultMaxItems), nil
}

// iamRefinement2ListTable2 provides PathPrefix-filtered overrides for ListPolicies and ListInstanceProfiles.
func (h *Handler) iamRefinement2ListTable2() map[string]iamActionFn {
	return map[string]iamActionFn{
		opListPolicies: func(vals url.Values, reqID string) (any, error) {
			return h.listPoliciesFiltered(vals, reqID)
		},

		opListInstanceProfiles: func(vals url.Values, reqID string) (any, error) {
			prefix := normPath(vals.Get("PathPrefix"))

			pg, err := filteredPage(
				h.Backend.ListInstanceProfiles, prefix, vals, func(ip InstanceProfile) string { return ip.Path },
			)
			if err != nil {
				return nil, err
			}

			xmlIPs := make([]InstanceProfileXML, 0, len(pg.Data))
			for i := range pg.Data {
				roles := h.resolveInstanceProfileRoles(&pg.Data[i])
				xmlIPs = append(xmlIPs, toInstanceProfileXML(&pg.Data[i], roles))
			}

			return &ListInstanceProfilesResponse{
				Xmlns: iamXMLNS,
				ListInstanceProfilesResult: ListInstanceProfilesResult{
					InstanceProfiles: xmlIPs,
					IsTruncated:      pg.Next != "",
					Marker:           pg.Next,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// listPoliciesFiltered handles ListPolicies with PathPrefix, Scope,
// OnlyAttached and PolicyUsageFilter (api_op_ListPolicies.go).
func (h *Handler) listPoliciesFiltered(vals url.Values, reqID string) (any, error) {
	prefix := normPath(vals.Get("PathPrefix"))

	scope := vals.Get("Scope")
	if scope == "" {
		scope = "Local"
	}

	if scope == "AWS" {
		// gopherstack never seeds or creates AWS-managed policies (every stored
		// Policy comes from CreatePolicy), so Scope=AWS genuinely has zero
		// matches rather than being an unhandled filter.
		return &ListPoliciesResponse{
			Xmlns:              iamXMLNS,
			ListPoliciesResult: ListPoliciesResult{Policies: []PolicyXML{}},
			ResponseMetadata:   ResponseMetadata{RequestID: reqID},
		}, nil
	}

	onlyAttached := vals.Get("OnlyAttached") == formValueTrue
	usageFilter := vals.Get("PolicyUsageFilter")

	pg, err := h.listPoliciesFilteredPage(vals, prefix, onlyAttached, usageFilter)
	if err != nil {
		return nil, err
	}

	xmlPolicies := make([]PolicyXML, 0, len(pg.Data))
	for i := range pg.Data {
		xmlPolicies = append(xmlPolicies, toPolicyXML(&pg.Data[i]))
	}

	return &ListPoliciesResponse{
		Xmlns: iamXMLNS,
		ListPoliciesResult: ListPoliciesResult{
			Policies:    xmlPolicies,
			IsTruncated: pg.Next != "",
			Marker:      pg.Next,
		},
		ResponseMetadata: ResponseMetadata{RequestID: reqID},
	}, nil
}

// listPoliciesFilteredPage fetches and paginates the policies matching
// prefix/onlyAttached/usageFilter. When none of the three narrow the result
// it passes the backend's own Marker/MaxItems window through unchanged.
func (h *Handler) listPoliciesFilteredPage(
	vals url.Values,
	prefix string,
	onlyAttached bool,
	usageFilter string,
) (page.Page[Policy], error) {
	if prefix == "/" && !onlyAttached && usageFilter == "" {
		return h.Backend.ListPolicies(vals.Get("Marker"), parseMaxItems(vals.Get("MaxItems")))
	}

	full, err := h.Backend.ListPolicies("", fetchAllMaxItems)
	if err != nil {
		return page.Page[Policy]{}, err
	}

	boundaryARNs := h.Backend.PermissionsBoundaryARNs()
	filtered := make([]Policy, 0, len(full.Data))

	for _, pol := range full.Data {
		if policyMatchesListFilters(pol, prefix, onlyAttached, usageFilter, boundaryARNs) {
			filtered = append(filtered, pol)
		}
	}

	return page.New(filtered, vals.Get("Marker"), parseMaxItems(vals.Get("MaxItems")), iamDefaultMaxItems), nil
}

// policyMatchesListFilters applies ListPolicies' PathPrefix, OnlyAttached and
// PolicyUsageFilter to a single policy. PermissionsPolicy excludes only
// policies used exclusively as a boundary (a policy can be both).
func policyMatchesListFilters(
	pol Policy,
	prefix string,
	onlyAttached bool,
	usageFilter string,
	boundaryARNs map[string]bool,
) bool {
	if prefix != "/" && !strings.HasPrefix(pol.Path, prefix) {
		return false
	}

	if onlyAttached && pol.AttachmentCount == 0 {
		return false
	}

	switch usageFilter {
	case "PermissionsBoundary":
		return boundaryARNs[pol.Arn]
	case "PermissionsPolicy":
		return !boundaryARNs[pol.Arn] || pol.AttachmentCount > 0
	default:
		return true
	}
}

// listAttachedPoliciesFiltered applies PathPrefix and Marker/MaxItems to a
// ListAttached{User,Group,Role}Policies result (api_op_ListAttachedUserPolicies.go
// et al: all three take PathPrefix, Marker, MaxItems). AttachedPolicy itself
// carries no Path, so PathPrefix is resolved through GetPolicy per entry.
func (h *Handler) listAttachedPoliciesFiltered(
	all []AttachedPolicy, vals url.Values,
) (page.Page[AttachedPolicy], error) {
	prefix := normPath(vals.Get("PathPrefix"))

	filtered := all
	if prefix != "/" {
		filtered = make([]AttachedPolicy, 0, len(all))
		for _, ap := range all {
			pol, err := h.Backend.GetPolicy(ap.PolicyArn)
			if err != nil {
				return page.Page[AttachedPolicy]{}, err
			}
			if strings.HasPrefix(pol.Path, prefix) {
				filtered = append(filtered, ap)
			}
		}
	}

	return page.New(filtered, vals.Get("Marker"), parseMaxItems(vals.Get("MaxItems")), iamDefaultMaxItems), nil
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
