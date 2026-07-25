package mediatailor

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- SourceLocation handlers ---

func (h *Handler) handleCreateSourceLocation(c *echo.Context, name string, body map[string]any) error {
	baseURL := extractBaseURL(body)
	tags := extractTags(body)

	sl, err := h.Backend.CreateSourceLocation(name, baseURL, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toSourceLocationOutput(sl))
}

func (h *Handler) handleDescribeSourceLocation(c *echo.Context, name string) error {
	sl, err := h.Backend.DescribeSourceLocation(name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toSourceLocationOutput(sl))
}

func (h *Handler) handleUpdateSourceLocation(c *echo.Context, name string, body map[string]any) error {
	baseURL := extractBaseURL(body)

	sl, err := h.Backend.UpdateSourceLocation(name, baseURL)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toSourceLocationOutput(sl))
}

func (h *Handler) handleDeleteSourceLocation(c *echo.Context, name string) error {
	if err := h.Backend.DeleteSourceLocation(name); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListSourceLocations(c *echo.Context) error {
	maxResults, nextToken := extractPaginationParams(c)
	summaries, nextToken, err := h.Backend.ListSourceLocations(maxResults, nextToken)
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(summaries))
	for _, s := range summaries {
		item := map[string]any{
			keySourceLocationName: s.Name,
			keyArn:                s.ARN,
			"HttpConfiguration": map[string]any{
				"BaseUrl": s.HTTPConfigurationURL,
			},
			keyTags: nilToEmpty(s.Tags),
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

func toSourceLocationOutput(sl *SourceLocation) map[string]any {
	out := map[string]any{
		keySourceLocationName: sl.Name,
		keyArn:                sl.ARN,
		"HttpConfiguration": map[string]any{
			"BaseUrl": sl.HTTPConfigurationURL,
		},
		keyTags: nilToEmpty(sl.Tags),
	}
	addTimestamps(out, sl.CreationTime, sl.LastModified)

	return out
}
