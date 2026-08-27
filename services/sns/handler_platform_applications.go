package sns

import (
	"net/http"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) handleCreatePlatformApplication(c *echo.Context) error {
	name := c.Request().FormValue("Name")
	platform := c.Request().FormValue("Platform")

	if name == "" || platform == "" {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameter",
			"Name and Platform are required",
		)
	}

	attrs := extractFormAttributes(c)
	region := httputils.ExtractRegionFromRequest(c.Request(), h.DefaultRegion)

	app, err := h.Backend.CreatePlatformApplicationInRegion(name, platform, region, attrs)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, CreatePlatformApplicationResponse{
		CreatePlatformApplicationResult: CreatePlatformApplicationResult{
			PlatformApplicationArn: app.PlatformApplicationArn,
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	})
}

func (h *Handler) handleGetPlatformApplicationAttributes(c *echo.Context) error {
	platformApplicationArn := c.Request().FormValue("PlatformApplicationArn")
	if platformApplicationArn == "" {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameter",
			"PlatformApplicationArn is required",
		)
	}

	attrs, err := h.Backend.GetPlatformApplicationAttributes(platformApplicationArn)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, GetPlatformApplicationAttributesResponse{
		GetPlatformApplicationAttributesResult: GetPlatformApplicationAttributesResult{
			Attributes: attrsToEntries(attrs),
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	})
}

func (h *Handler) handleSetPlatformApplicationAttributes(c *echo.Context) error {
	platformApplicationArn := c.Request().FormValue("PlatformApplicationArn")
	if platformApplicationArn == "" {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameter",
			"PlatformApplicationArn is required",
		)
	}

	attrs := extractFormAttributes(c)

	if err := h.Backend.SetPlatformApplicationAttributes(platformApplicationArn, attrs); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, SetPlatformApplicationAttributesResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	})
}

func (h *Handler) handleListPlatformApplications(c *echo.Context) error {
	nextToken := c.Request().FormValue("NextToken")

	apps, token, err := h.Backend.ListPlatformApplications(nextToken)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	members := make([]XMLPlatformApplication, len(apps))
	for i, a := range apps {
		members[i] = XMLPlatformApplication{
			PlatformApplicationArn: a.PlatformApplicationArn,
			Attributes:             attrsToEntries(a.Attributes),
		}
	}

	return h.writeXML(c, ListPlatformApplicationsResponse{
		ListPlatformApplicationsResult: ListPlatformApplicationsResult{
			PlatformApplications: members,
			NextToken:            token,
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	})
}

func (h *Handler) handleDeletePlatformApplication(c *echo.Context) error {
	platformApplicationArn := c.Request().FormValue("PlatformApplicationArn")
	if platformApplicationArn == "" {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameter",
			"PlatformApplicationArn is required",
		)
	}

	if err := h.Backend.DeletePlatformApplication(platformApplicationArn); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, DeletePlatformApplicationResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	})
}
