package eks

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// dispatchFargateOps handles Fargate profile CRUD operations.
func (h *Handler) dispatchFargateOps(c *echo.Context, route eksRoute, body []byte) (bool, error) {
	switch route.operation {
	case opCreateFargateProfile:
		return true, h.handleCreateFargateProfile(c, route.clusterName, body)
	case opDeleteFargateProfile:
		return true, h.handleDeleteFargateProfile(c, route.clusterName, route.nodegroupName)
	case opDescribeFargateProfile:
		return true, h.handleDescribeFargateProfile(c, route.clusterName, route.nodegroupName)
	case opListFargateProfiles:
		return true, h.handleListFargateProfiles(c, route.clusterName)
	}

	return false, nil
}

// parseFargateProfileRoute returns the route for /clusters/{name}/fargate-profiles[/{profileName}] paths.
func parseFargateProfileRoute(method, clusterName string, parts []string) eksRoute {
	const fargateProfileParts = 2

	if len(parts) == fargateProfileParts {
		switch method {
		case http.MethodPost:
			return eksRoute{operation: opCreateFargateProfile, clusterName: clusterName}
		case http.MethodGet:
			return eksRoute{operation: opListFargateProfiles, clusterName: clusterName}
		}

		return eksRoute{operation: opUnknown}
	}

	profileName := parts[2]

	switch method {
	case http.MethodGet:
		return eksRoute{operation: opDescribeFargateProfile, clusterName: clusterName, nodegroupName: profileName}
	case http.MethodDelete:
		return eksRoute{operation: opDeleteFargateProfile, clusterName: clusterName, nodegroupName: profileName}
	}

	return eksRoute{operation: opUnknown}
}

func fargateProfileToJSON(p *FargateProfile) map[string]any {
	health := p.Health
	if health == nil {
		health = &FargateProfileHealth{Issues: []FargateProfileIssue{}}
	} else if health.Issues == nil {
		health = &FargateProfileHealth{Issues: []FargateProfileIssue{}}
	}

	m := map[string]any{
		keyClusterName:        p.ClusterName,
		"fargateProfileName":  p.FargateProfileName,
		"fargateProfileArn":   p.ARN,
		"podExecutionRoleArn": p.PodExecutionRoleARN,
		keyStatusField:        p.Status,
		"selectors":           p.Selectors,
		keyCreatedAt:          p.CreatedAt.Unix(),
		keyHealth:             health,
	}

	if len(p.Subnets) > 0 {
		m["subnets"] = p.Subnets
	} else {
		m["subnets"] = []string{}
	}

	if p.Tags != nil {
		m["tags"] = p.Tags.Clone()
	} else {
		m["tags"] = map[string]string{}
	}

	return m
}

type fargateProfileSelectorJSON struct {
	Labels    map[string]string `json:"labels,omitempty"`
	Namespace string            `json:"namespace"`
}

type createFargateProfileBody struct {
	Tags                map[string]string            `json:"tags"`
	Subnets             []string                     `json:"subnets"`
	FargateProfileName  string                       `json:"fargateProfileName"`
	PodExecutionRoleArn string                       `json:"podExecutionRoleArn"`
	Selectors           []fargateProfileSelectorJSON `json:"selectors"`
}

func (h *Handler) handleCreateFargateProfile(c *echo.Context, clusterName string, body []byte) error {
	var in createFargateProfileBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid request body"))
	}

	if in.FargateProfileName == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "fargateProfileName is required"))
	}

	selectors := make([]FargateProfileSelector, len(in.Selectors))
	for i, s := range in.Selectors {
		selectors[i] = FargateProfileSelector(s)
	}

	profile, err := h.Backend.CreateFargateProfile(
		clusterName,
		in.FargateProfileName,
		in.PodExecutionRoleArn,
		selectors,
		in.Subnets,
		in.Tags,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyFargateProfile: fargateProfileToJSON(profile),
	})
}

func (h *Handler) handleDeleteFargateProfile(c *echo.Context, clusterName, profileName string) error {
	profile, err := h.Backend.DeleteFargateProfile(clusterName, profileName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"fargateProfile": fargateProfileToJSON(profile),
	})
}

func (h *Handler) handleDescribeFargateProfile(c *echo.Context, clusterName, profileName string) error {
	profile, err := h.Backend.DescribeFargateProfile(clusterName, profileName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"fargateProfile": fargateProfileToJSON(profile),
	})
}

func (h *Handler) handleListFargateProfiles(c *echo.Context, clusterName string) error {
	names, err := h.Backend.ListFargateProfiles(clusterName)
	if err != nil {
		return h.handleError(c, err)
	}

	maxResults, nextToken := eksPaginationParams(c)
	p := page.New(names, nextToken, maxResults, eksDefaultPageSize)

	return c.JSON(http.StatusOK, eksPageResponse("fargateProfileNames", p))
}
