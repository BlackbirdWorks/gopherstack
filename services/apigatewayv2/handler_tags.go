package apigatewayv2

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func extractTagsOp(path, method string) string {
	suffix := strings.Trim(strings.TrimPrefix(path, tagsPrefix), "/")
	if suffix == "" {
		return opUnknown
	}

	switch method {
	case http.MethodGet:
		return "GetTags"
	case http.MethodPost:
		return "TagResource"
	case http.MethodDelete:
		return "UntagResource"
	}

	return opUnknown
}

func (h *Handler) handleTagsPath(c *echo.Context, method, path string) error {
	resourceARN := strings.TrimPrefix(path, tagsPrefix+"/")
	if resourceARN == "" {
		return writeErr(c, http.StatusNotFound, msgNotFound)
	}

	switch method {
	case http.MethodGet:
		return h.handleGetTags(c, resourceARN)
	case http.MethodPost:
		return h.handleTagResource(c, resourceARN)
	case http.MethodDelete:
		return h.handleUntagResource(c, resourceARN)
	default:
		return writeErr(c, http.StatusMethodNotAllowed, msgMethodNotAllowed)
	}
}

func (h *Handler) handleGetTags(c *echo.Context, resourceARN string) error {
	log := logger.Load(c.Request().Context())

	tags, err := h.Backend.GetTags(resourceARN)
	if err != nil {
		log.Error("apigatewayv2: get tags failed", "resourceArn", resourceARN, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrVpcLinkNotFound) ||
			errors.Is(err, ErrDomainNameNotFound) || errors.Is(err, ErrStageNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	if tags == nil {
		tags = map[string]string{}
	}

	return c.JSON(http.StatusOK, getTagsOutput{Tags: tags})
}

func (h *Handler) handleTagResource(c *echo.Context, resourceARN string) error {
	log := logger.Load(c.Request().Context())

	var input tagResourceInput
	if err := json.NewDecoder(c.Request().Body).Decode(&input); err != nil {
		return writeErr(c, http.StatusBadRequest, msgInvalidBody)
	}

	if err := h.Backend.TagResource(resourceARN, input.Tags); err != nil {
		log.Error("apigatewayv2: tag resource failed", "resourceArn", resourceARN, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrVpcLinkNotFound) ||
			errors.Is(err, ErrDomainNameNotFound) || errors.Is(err, ErrStageNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.NoContent(http.StatusCreated)
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceARN string) error {
	log := logger.Load(c.Request().Context())

	// AWS SDK sends tagKeys as repeated query parameters: ?tagKeys=key1&tagKeys=key2
	tagKeys := c.Request().URL.Query()["tagKeys"]

	if err := h.Backend.UntagResource(resourceARN, tagKeys); err != nil {
		log.Error("apigatewayv2: untag resource failed", "resourceArn", resourceARN, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrVpcLinkNotFound) ||
			errors.Is(err, ErrDomainNameNotFound) || errors.Is(err, ErrStageNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}
