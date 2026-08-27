package sns

import (
	"net/http"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreatePlatformEndpoint(c *echo.Context) error {
	platformApplicationArn := c.Request().FormValue("PlatformApplicationArn")
	token := c.Request().FormValue("Token")

	if platformApplicationArn == "" || token == "" {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameter",
			"PlatformApplicationArn and Token are required",
		)
	}

	attrs := extractFormAttributes(c)

	// CustomUserData is sent as a top-level form field by the AWS SDK, not under Attributes.entry.*.
	if customData := c.Request().FormValue("CustomUserData"); customData != "" {
		attrs["CustomUserData"] = customData
	}

	ep, err := h.Backend.CreatePlatformEndpoint(platformApplicationArn, token, attrs)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, CreatePlatformEndpointResponse{
		CreatePlatformEndpointResult: CreatePlatformEndpointResult{EndpointArn: ep.EndpointArn},
		ResponseMetadata:             ResponseMetadata{RequestID: uuid.NewString()},
	})
}

func (h *Handler) handleGetEndpointAttributes(c *echo.Context) error {
	endpointArn := c.Request().FormValue("EndpointArn")
	if endpointArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "EndpointArn is required")
	}

	attrs, err := h.Backend.GetEndpointAttributes(endpointArn)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, GetEndpointAttributesResponse{
		GetEndpointAttributesResult: GetEndpointAttributesResult{Attributes: attrsToEntries(attrs)},
		ResponseMetadata:            ResponseMetadata{RequestID: uuid.NewString()},
	})
}

func (h *Handler) handleSetEndpointAttributes(c *echo.Context) error {
	endpointArn := c.Request().FormValue("EndpointArn")
	if endpointArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "EndpointArn is required")
	}

	attrs := extractFormAttributes(c)

	if err := h.Backend.SetEndpointAttributes(endpointArn, attrs); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, SetEndpointAttributesResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	})
}

func (h *Handler) handleListEndpointsByPlatformApplication(c *echo.Context) error {
	platformApplicationArn := c.Request().FormValue("PlatformApplicationArn")
	if platformApplicationArn == "" {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameter",
			"PlatformApplicationArn is required",
		)
	}

	nextToken := c.Request().FormValue("NextToken")

	eps, token, err := h.Backend.ListEndpointsByPlatformApplication(
		platformApplicationArn,
		nextToken,
	)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	members := make([]XMLPlatformEndpoint, len(eps))
	for i, ep := range eps {
		members[i] = XMLPlatformEndpoint{
			EndpointArn: ep.EndpointArn,
			Attributes:  attrsToEntries(ep.Attributes),
		}
	}

	return h.writeXML(c, ListEndpointsByPlatformApplicationResponse{
		ListEndpointsByPlatformApplicationResult: ListEndpointsByPlatformApplicationResult{
			Endpoints: members,
			NextToken: token,
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	})
}

func (h *Handler) handleDeleteEndpoint(c *echo.Context) error {
	endpointArn := c.Request().FormValue("EndpointArn")
	if endpointArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "EndpointArn is required")
	}

	if err := h.Backend.DeleteEndpoint(endpointArn); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, DeleteEndpointResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	})
}
