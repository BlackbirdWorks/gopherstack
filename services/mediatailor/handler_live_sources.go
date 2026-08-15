package mediatailor

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- LiveSource handlers ---

func (h *Handler) handleCreateLiveSource(
	c *echo.Context,
	sourceLocationName, liveSourceName string,
	body map[string]any,
) error {
	cfgs := extractHTTPPackageConfigurations(body)
	tags := extractTags(body)

	ls, err := h.Backend.CreateLiveSource(sourceLocationName, liveSourceName, cfgs, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toLiveSourceOutput(ls))
}

func (h *Handler) handleDescribeLiveSource(c *echo.Context, sourceLocationName, liveSourceName string) error {
	ls, err := h.Backend.DescribeLiveSource(sourceLocationName, liveSourceName)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toLiveSourceOutput(ls))
}

func (h *Handler) handleUpdateLiveSource(
	c *echo.Context,
	sourceLocationName, liveSourceName string,
	body map[string]any,
) error {
	cfgs := extractHTTPPackageConfigurations(body)

	ls, err := h.Backend.UpdateLiveSource(sourceLocationName, liveSourceName, cfgs)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toLiveSourceOutput(ls))
}

func (h *Handler) handleDeleteLiveSource(c *echo.Context, sourceLocationName, liveSourceName string) error {
	if err := h.Backend.DeleteLiveSource(sourceLocationName, liveSourceName); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListLiveSources(c *echo.Context, sourceLocationName string) error {
	maxResults, nextToken := extractPaginationParams(c)
	summaries, nextToken, err := h.Backend.ListLiveSources(sourceLocationName, maxResults, nextToken)
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(summaries))
	for _, s := range summaries {
		item := map[string]any{
			keyLiveSourceName:     s.LiveSourceName,
			keySourceLocationName: s.SourceLocationName,
			keyArn:                s.ARN,
			keyHTTPPackageConfigs: httpPackageConfigurationsWire(s.HTTPPackageConfigurations),
			keyTags:               nilToEmpty(s.Tags),
		}
		addTimestamps(item, s.CreationTime, s.LastModified)
		out = append(out, item)
	}

	resp := map[string]any{keyItems: out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func toLiveSourceOutput(ls *LiveSource) map[string]any {
	out := map[string]any{
		keyLiveSourceName:     ls.LiveSourceName,
		keySourceLocationName: ls.SourceLocationName,
		keyArn:                ls.ARN,
		keyHTTPPackageConfigs: httpPackageConfigurationsWire(ls.HTTPPackageConfigurations),
		keyTags:               nilToEmpty(ls.Tags),
	}
	addTimestamps(out, ls.CreationTime, ls.LastModified)

	return out
}
