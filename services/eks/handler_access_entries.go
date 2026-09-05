package eks

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// dispatchAccessEntryOps handles access entry CRUD and access-policy association operations.
func (h *Handler) dispatchAccessEntryOps(c *echo.Context, route eksRoute, body []byte) (bool, error) {
	switch route.operation {
	case opCreateAccessEntry:
		return true, h.handleCreateAccessEntry(c, route.clusterName, body)
	case opDeleteAccessEntry:
		return true, h.handleDeleteAccessEntry(c, route.clusterName, route.principalARN)
	case opDescribeAccessEntry:
		return true, h.handleDescribeAccessEntry(c, route.clusterName, route.principalARN)
	case opListAccessEntries:
		return true, h.handleListAccessEntries(c, route.clusterName)
	case opUpdateAccessEntry:
		return true, h.handleUpdateAccessEntry(c, route.clusterName, route.principalARN, body)
	case opAssociateAccessPolicy:
		return true, h.handleAssociateAccessPolicy(c, route.clusterName, route.principalARN, body)
	case opListAccessPolicies:
		return true, h.handleListAccessPolicies(c)
	case opListAssociatedAccessPolicies:
		return true, h.handleListAssociatedAccessPolicies(c, route.clusterName, route.principalARN)
	case opDisassociateAccessPolicy:
		return true, h.handleDisassociateAccessPolicy(c, route.clusterName, route.principalARN, route.resourceARN)
	}

	return false, nil
}

// parseAccessEntryRoute returns the route for access entry paths:
// /clusters/{name}/access-entries[/{principalArn}[/access-policies[/{policyArn}]]].
func parseAccessEntryRoute(method, clusterName string, parts []string) eksRoute {
	const accessEntryParts = 2

	if len(parts) == accessEntryParts {
		switch method {
		case http.MethodPost:
			return eksRoute{operation: opCreateAccessEntry, clusterName: clusterName}
		case http.MethodGet:
			return eksRoute{operation: opListAccessEntries, clusterName: clusterName}
		}

		return eksRoute{operation: opUnknown}
	}

	// parts[2] may be principalArn, principalArn/access-policies, or principalArn/access-policies/{policyArn}
	tail := parts[2]

	// Check for /access-policies/{policyArn} suffix (DELETE for disassociate)
	if before, after, ok := strings.Cut(tail, "/access-policies/"); ok {
		principalARN := before
		policyARN := after

		if method == http.MethodDelete {
			return eksRoute{
				operation:    opDisassociateAccessPolicy,
				clusterName:  clusterName,
				principalARN: principalARN,
				resourceARN:  policyARN,
			}
		}

		return eksRoute{operation: opUnknown}
	}

	if before, ok := strings.CutSuffix(tail, "/access-policies"); ok {
		principalARN := before

		switch method {
		case http.MethodPost:
			return eksRoute{operation: opAssociateAccessPolicy, clusterName: clusterName, principalARN: principalARN}
		case http.MethodGet:
			return eksRoute{
				operation:    opListAssociatedAccessPolicies,
				clusterName:  clusterName,
				principalARN: principalARN,
			}
		}

		return eksRoute{operation: opUnknown}
	}

	// plain principalArn. UpdateAccessEntry is POST to this same path (not PUT)
	// -- verified against the SDK serializer.
	switch method {
	case http.MethodDelete:
		return eksRoute{operation: opDeleteAccessEntry, clusterName: clusterName, principalARN: tail}
	case http.MethodGet:
		return eksRoute{operation: opDescribeAccessEntry, clusterName: clusterName, principalARN: tail}
	case http.MethodPost:
		return eksRoute{operation: opUpdateAccessEntry, clusterName: clusterName, principalARN: tail}
	}

	return eksRoute{operation: opUnknown}
}

func accessEntryToJSON(entry *AccessEntry) map[string]any {
	modifiedAt := entry.ModifiedAt
	if modifiedAt.IsZero() {
		modifiedAt = entry.CreatedAt
	}

	m := map[string]any{
		keyClusterName:    entry.ClusterName,
		keyPrincipalArn:   entry.PrincipalARN,
		keyAccessEntryArn: entry.ARN,
		keyType:           entry.Type,
		keyUsername:       entry.Username,
		keyCreatedAt:      entry.CreatedAt.Unix(),
		keyModifiedAt:     modifiedAt.Unix(),
	}

	if len(entry.KubernetesGroups) > 0 {
		m["kubernetesGroups"] = entry.KubernetesGroups
	} else {
		m["kubernetesGroups"] = []string{}
	}

	if entry.Tags != nil {
		m[keyTags] = entry.Tags.Clone()
	} else {
		m[keyTags] = map[string]string{}
	}

	return m
}

type createAccessEntryBody struct {
	Tags             map[string]string `json:"tags"`
	PrincipalArn     string            `json:"principalArn"`
	Type             string            `json:"type"`
	Username         string            `json:"username"`
	KubernetesGroups []string          `json:"kubernetesGroups"`
}

func (h *Handler) handleCreateAccessEntry(c *echo.Context, clusterName string, body []byte) error {
	var in createAccessEntryBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid request body"))
	}

	if in.PrincipalArn == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "principalArn is required"))
	}

	entry, err := h.Backend.CreateAccessEntry(
		clusterName,
		in.PrincipalArn,
		in.Type,
		in.Username,
		in.KubernetesGroups,
		in.Tags,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyAccessEntry: accessEntryToJSON(entry),
	})
}

func (h *Handler) handleDeleteAccessEntry(c *echo.Context, clusterName, principalARN string) error {
	if err := h.Backend.DeleteAccessEntry(clusterName, principalARN); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDescribeAccessEntry(c *echo.Context, clusterName, principalARN string) error {
	entry, err := h.Backend.DescribeAccessEntry(clusterName, principalARN)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"accessEntry": accessEntryToJSON(entry),
	})
}

func (h *Handler) handleListAccessEntries(c *echo.Context, clusterName string) error {
	arns, err := h.Backend.ListAccessEntries(clusterName)
	if err != nil {
		return h.handleError(c, err)
	}

	if policyARN := c.Request().URL.Query().Get("associatedPolicyArn"); policyARN != "" {
		arns = slices.DeleteFunc(arns, func(principalARN string) bool {
			policies, lookupErr := h.Backend.ListAssociatedAccessPolicies(clusterName, principalARN)
			if lookupErr != nil {
				return true
			}

			return !slices.ContainsFunc(policies, func(p *AccessPolicyAssociation) bool {
				return p.PolicyARN == policyARN
			})
		})
	}

	maxResults, nextToken := eksPaginationParams(c)
	p := page.New(arns, nextToken, maxResults, eksDefaultPageSize)

	return c.JSON(http.StatusOK, eksPageResponse("accessEntries", p))
}

type updateAccessEntryBody struct {
	Username         string   `json:"username"`
	KubernetesGroups []string `json:"kubernetesGroups"`
}

func (h *Handler) handleUpdateAccessEntry(c *echo.Context, clusterName, principalARN string, body []byte) error {
	var in updateAccessEntryBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", err.Error()))
		}
	}

	entry, err := h.Backend.UpdateAccessEntry(clusterName, principalARN, AccessEntryUpdate(in))
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"accessEntry": accessEntryToJSON(entry),
	})
}

type associateAccessPolicyBody struct {
	AccessScope map[string]any `json:"accessScope"`
	PolicyArn   string         `json:"policyArn"`
}

func (h *Handler) handleAssociateAccessPolicy(c *echo.Context, clusterName, principalARN string, body []byte) error {
	var in associateAccessPolicyBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid request body"))
	}

	if in.PolicyArn == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "policyArn is required"))
	}

	assoc, err := h.Backend.AssociateAccessPolicy(clusterName, principalARN, in.PolicyArn, in.AccessScope)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"associatedAccessPolicy": map[string]any{
			keyClusterName:  assoc.ClusterName,
			keyPrincipalArn: assoc.PrincipalARN,
			keyPolicyArn:    assoc.PolicyARN,
			"associatedAt":  assoc.AssociatedAt.Unix(),
			"accessScope":   assoc.AccessScope,
		},
	})
}

func (h *Handler) handleListAccessPolicies(c *echo.Context) error {
	policies := h.Backend.ListAccessPolicies()

	maxResults, nextToken := eksPaginationParams(c)
	p := page.New(policies, nextToken, maxResults, eksDefaultPageSize)

	return c.JSON(http.StatusOK, eksPageResponse("accessPolicies", p))
}

func (h *Handler) handleListAssociatedAccessPolicies(c *echo.Context, clusterName, principalARN string) error {
	policies, err := h.Backend.ListAssociatedAccessPolicies(clusterName, principalARN)
	if err != nil {
		return h.handleError(c, err)
	}

	result := make([]map[string]any, len(policies))
	for i, p := range policies {
		result[i] = map[string]any{
			keyPolicyArn:   p.PolicyARN,
			"accessScope":  p.AccessScope,
			"associatedAt": p.AssociatedAt.Unix(),
		}
	}

	maxResults, nextToken := eksPaginationParams(c)
	pg := page.New(result, nextToken, maxResults, eksDefaultPageSize)

	resp := eksPageResponse("associatedAccessPolicies", pg)
	resp[keyClusterName] = clusterName
	resp[keyPrincipalArn] = principalARN

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDisassociateAccessPolicy(c *echo.Context, clusterName, principalARN, policyARN string) error {
	if err := h.Backend.DisassociateAccessPolicy(clusterName, principalARN, policyARN); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}
