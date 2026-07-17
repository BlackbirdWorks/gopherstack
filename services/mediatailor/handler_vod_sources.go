package mediatailor //nolint:dupl // VodSource/LiveSource CRUD handlers are structurally identical by AWS API design

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- VodSource handlers ---

func (h *Handler) handleCreateVodSource(
	c *echo.Context,
	sourceLocationName, vodSourceName string,
	body map[string]any,
) error {
	cfgs := extractHTTPPackageConfigurations(body)
	tags := extractTags(body)

	vs, err := h.Backend.CreateVodSource(sourceLocationName, vodSourceName, cfgs, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toVodSourceOutput(vs))
}

func (h *Handler) handleDescribeVodSource(c *echo.Context, sourceLocationName, vodSourceName string) error {
	vs, err := h.Backend.DescribeVodSource(sourceLocationName, vodSourceName)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toVodSourceOutput(vs))
}

func (h *Handler) handleUpdateVodSource(
	c *echo.Context,
	sourceLocationName, vodSourceName string,
	body map[string]any,
) error {
	cfgs := extractHTTPPackageConfigurations(body)

	vs, err := h.Backend.UpdateVodSource(sourceLocationName, vodSourceName, cfgs)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toVodSourceOutput(vs))
}

func (h *Handler) handleDeleteVodSource(c *echo.Context, sourceLocationName, vodSourceName string) error {
	if err := h.Backend.DeleteVodSource(sourceLocationName, vodSourceName); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListVodSources(c *echo.Context, sourceLocationName string) error {
	maxResults, nextToken := extractPaginationParams(c)
	summaries, nextToken, err := h.Backend.ListVodSources(sourceLocationName, maxResults, nextToken)
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(summaries))
	for _, s := range summaries {
		out = append(out, map[string]any{
			keyVodSourceName:      s.VodSourceName,
			keySourceLocationName: s.SourceLocationName,
			keyArn:                s.ARN,
			keyTags:               nilToEmpty(s.Tags),
		})
	}

	resp := map[string]any{keyItems: out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func toVodSourceOutput(vs *VodSource) map[string]any {
	cfgs := make([]map[string]any, 0, len(vs.HTTPPackageConfigurations))
	for _, cfg := range vs.HTTPPackageConfigurations {
		cfgs = append(cfgs, map[string]any{
			"Path":         cfg.Path,
			keySourceGroup: cfg.SourceGroup,
			"Type":         cfg.Type,
		})
	}

	return map[string]any{
		keyVodSourceName:            vs.VodSourceName,
		keySourceLocationName:       vs.SourceLocationName,
		keyArn:                      vs.ARN,
		"HttpPackageConfigurations": cfgs,
		keyTags:                     nilToEmpty(vs.Tags),
	}
}
