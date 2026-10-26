package azureblob

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) setBlobTags(c *echo.Context, container, blob string) error {
	r := c.Request()

	body, err := httputils.ReadBody(r)
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "InternalError", "Failed to read request body.")
	}

	if apiErr := verifyTransactional(r, body, "Content-Md5", headerContentCRC64); apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	tags, apiErr := tagsFromBody(body)
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	pre, apiErr := precondFromRequest(r)
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	if err = h.Backend.SetBlobTags(container, blob, c.QueryParam(querySnapshot), tags, pre); err != nil {
		return h.writeBlobNotFoundError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) getBlobTags(c *echo.Context, container, blob string) error {
	pre, apiErr := precondFromRequest(c.Request())
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	tags, err := h.Backend.GetBlobTags(container, blob, c.QueryParam(querySnapshot), pre)
	if err != nil {
		return h.writeBlobNotFoundError(c, err)
	}

	return h.writeXML(c, http.StatusOK, tagsToXML(tags))
}

func (h *Handler) findBlobsByTags(c *echo.Context, container string) error {
	q := c.QueryParams()
	where := q.Get("where")

	filter, err := parseTagFilter(where, container == "")
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidQueryParameterValue",
			"Value for one of the query parameters specified in the request URI is invalid.")
	}

	opts := FindBlobsOptions{Filter: filter, Marker: q.Get("marker"), Container: container, MaxResults: maxListResults}

	if raw := q.Get("maxresults"); raw != "" {
		n, convErr := strconv.Atoi(raw)
		if convErr != nil || n <= 0 {
			return h.writeError(c, http.StatusBadRequest, "InvalidQueryParameterValue",
				"Value for one of the query parameters specified in the request URI is invalid.")
		}

		opts.MaxResults = min(n, maxListResults)
	}

	page, err := h.Backend.FindBlobsByTags(container, opts)
	if errors.Is(err, ErrInvalidMarker) {
		return h.writeError(c, http.StatusBadRequest, "InvalidQueryParameterValue",
			"Value for one of the query parameters specified in the request URI is invalid.")
	}

	if err != nil {
		return h.writeContainerNotFound(c)
	}

	out := filterBlobsXML{
		ServiceEndpoint: h.serviceEndpoint(),
		Where:           where,
		NextMarker:      page.NextMarker,
		Blobs:           make([]filterBlobXML, 0, len(page.Blobs)),
	}

	for _, fb := range page.Blobs {
		out.Blobs = append(out.Blobs, filterBlobXML{Name: fb.Name, ContainerName: fb.Container, Tags: tagsToXML(fb.Tags)})
	}

	return h.writeXML(c, http.StatusOK, out)
}
