package apigatewayv2

import (
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func extractVpcLinksOp(path, method string) string {
	suffix := strings.Trim(strings.TrimPrefix(path, vpcLinksPrefix), "/")
	if suffix == "" {
		switch method {
		case http.MethodPost:
			return "CreateVpcLink"
		case http.MethodGet:
			return "GetVpcLinks"
		}

		return opUnknown
	}

	switch method {
	case http.MethodGet:
		return "GetVpcLink"
	case http.MethodDelete:
		return "DeleteVpcLink"
	case http.MethodPatch:
		return "UpdateVpcLink"
	default:
		return opUnknown
	}
}

func (h *Handler) handleVpcLinksPath(c *echo.Context, method, path string) error {
	suffix := strings.Trim(strings.TrimPrefix(path, vpcLinksPrefix), "/")
	if suffix == "" {
		switch method {
		case http.MethodPost:
			return handleCreateNoParent(c, "vpc link", func(input CreateVpcLinkInput) (*VpcLink, error) {
				return h.Backend.CreateVpcLink(input)
			})
		case http.MethodGet:
			links, err := h.Backend.GetVpcLinks()
			if err != nil {
				return writeErr(c, http.StatusInternalServerError, err.Error())
			}

			maxResults, nextToken := apigwPaginationParams(c)
			p := page.New(links, nextToken, maxResults, apigwDefaultPageSize)

			return c.JSON(http.StatusOK, listVpcLinksOutput{Items: p.Data, NextToken: p.Next})
		default:
			return writeErr(c, http.StatusMethodNotAllowed, msgMethodNotAllowed)
		}
	}

	switch method {
	case http.MethodGet:
		vpcLink, err := h.Backend.GetVpcLink(suffix)
		if err != nil {
			if errors.Is(err, ErrVpcLinkNotFound) {
				return writeErr(c, http.StatusNotFound, msgNotFound)
			}

			return writeErr(c, http.StatusInternalServerError, err.Error())
		}

		return c.JSON(http.StatusOK, vpcLink)
	case http.MethodPatch:
		return handleUpdate(c, suffix, "", "vpc link",
			func(input UpdateVpcLinkInput) (*VpcLink, error) { return h.Backend.UpdateVpcLink(suffix, input) },
			ErrVpcLinkNotFound)
	case http.MethodDelete:
		if err := h.Backend.DeleteVpcLink(suffix); err != nil {
			if errors.Is(err, ErrVpcLinkNotFound) {
				return writeErr(c, http.StatusNotFound, msgNotFound)
			}

			return writeErr(c, http.StatusInternalServerError, err.Error())
		}

		return c.NoContent(http.StatusNoContent)
	default:
		return writeErr(c, http.StatusMethodNotAllowed, msgMethodNotAllowed)
	}
}
