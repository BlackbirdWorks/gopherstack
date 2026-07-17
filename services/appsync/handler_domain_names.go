package appsync

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/labstack/echo/v5"
)

// handleDomainNames handles /v1/domainnames[/{domainName}[/apiassociation]].
func (h *Handler) handleDomainNames(ctx context.Context, c *echo.Context, segs []string) error {
	switch len(segs) {
	case pathSegsAPIs:
		return h.handleDomainNamesCollection(ctx, c)
	case pathSegsAPIID:
		return h.handleDomainName(ctx, c, segs[2])
	case pathSegsAPISubresource:
		if segs[3] == "apiassociation" {
			return h.handleAPIAssociation(ctx, c, segs[2])
		}
	}

	return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
}

// handleDomainNamesCollection handles /v1/domainnames.
func (h *Handler) handleDomainNamesCollection(ctx context.Context, c *echo.Context) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.createDomainName(ctx, c)
	case http.MethodGet:
		return h.listDomainNames(ctx, c)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// handleDomainName handles /v1/domainnames/{domainName}.
func (h *Handler) handleDomainName(ctx context.Context, c *echo.Context, domainName string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.getDomainName(ctx, c, domainName)
	case http.MethodDelete:
		return h.deleteDomainName(ctx, c, domainName)
	case http.MethodPost, http.MethodPut, http.MethodPatch:
		return h.updateDomainName(ctx, c, domainName)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// handleAPIAssociation handles /v1/domainnames/{domainName}/apiassociation.
func (h *Handler) handleAPIAssociation(ctx context.Context, c *echo.Context, domainName string) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.associateAPI(ctx, c, domainName)
	case http.MethodGet:
		return h.getAPIAssociation(ctx, c, domainName)
	case http.MethodDelete:
		return h.disassociateAPI(ctx, c, domainName)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// createDomainName handles POST /v1/domainnames.
func (h *Handler) createDomainName(ctx context.Context, c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		Tags           map[string]string `json:"tags"`
		DomainName     string            `json:"domainName"`
		CertificateArn string            `json:"certificateArn"`
		Description    string            `json:"description"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if input.DomainName == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "domainName is required"))
	}

	if input.CertificateArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "certificateArn is required"))
	}

	dn, createErr := h.Backend.CreateDomainName(input.DomainName, input.CertificateArn, input.Description, input.Tags)
	if createErr != nil {
		return h.handleError(ctx, c, "CreateDomainName", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyDomainNameConfig: dn})
}

// associateAPI handles POST /v1/domainnames/{domainName}/apiassociation.
func (h *Handler) associateAPI(ctx context.Context, c *echo.Context, domainName string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		APIID string `json:"apiId"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if input.APIID == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "apiId is required"))
	}

	assoc, createErr := h.Backend.AssociateAPI(domainName, input.APIID)
	if createErr != nil {
		return h.handleError(ctx, c, "AssociateApi", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{"apiAssociation": assoc})
}

// getDomainName handles GET /v1/domainnames/{domainName}.
func (h *Handler) getDomainName(ctx context.Context, c *echo.Context, domainName string) error {
	dn, err := h.Backend.GetDomainName(domainName)
	if err != nil {
		return h.handleError(ctx, c, "GetDomainName", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyDomainNameConfig: dn})
}

// listDomainNames handles GET /v1/domainnames.
func (h *Handler) listDomainNames(ctx context.Context, c *echo.Context) error {
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")
	maxResults, _ := strconv.Atoi(q.Get("maxResults"))

	dns, err := h.Backend.ListDomainNames()
	if err != nil {
		return h.handleError(ctx, c, "ListDomainNames", err)
	}

	page, tok := appsyncPaginate(dns, nextToken, maxResults)
	out := map[string]any{"domainNameConfigs": page}
	if tok != "" {
		out["nextToken"] = tok
	}

	return c.JSON(http.StatusOK, out)
}

// deleteDomainName handles DELETE /v1/domainnames/{domainName}.
func (h *Handler) deleteDomainName(ctx context.Context, c *echo.Context, domainName string) error {
	if err := h.Backend.DeleteDomainName(domainName); err != nil {
		return h.handleError(ctx, c, "DeleteDomainName", err)
	}

	return c.NoContent(http.StatusNoContent)
}

// getAPIAssociation handles GET /v1/domainnames/{domainName}/apiassociation.
func (h *Handler) getAPIAssociation(ctx context.Context, c *echo.Context, domainName string) error {
	assoc, err := h.Backend.GetAPIAssociation(domainName)
	if err != nil {
		return h.handleError(ctx, c, "GetApiAssociation", err)
	}

	return c.JSON(http.StatusOK, map[string]any{"apiAssociation": assoc})
}

// updateDomainName handles PUT/PATCH /v1/domainnames/{domainName}.
func (h *Handler) updateDomainName(ctx context.Context, c *echo.Context, domainName string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		Description    string `json:"description"`
		CertificateARN string `json:"certificateArn"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	dn, updateErr := h.Backend.UpdateDomainName(domainName, input.Description, input.CertificateARN)
	if updateErr != nil {
		return h.handleError(ctx, c, "UpdateDomainName", updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyDomainNameConfig: dn})
}

// disassociateAPI handles DELETE /v1/domainnames/{domainName}/apiassociation.
func (h *Handler) disassociateAPI(ctx context.Context, c *echo.Context, domainName string) error {
	if err := h.Backend.DisassociateAPI(domainName); err != nil {
		return h.handleError(ctx, c, "DisassociateApi", err)
	}

	return c.NoContent(http.StatusNoContent)
}
