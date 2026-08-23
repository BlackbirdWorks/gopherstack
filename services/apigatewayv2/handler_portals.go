package apigatewayv2

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func extractPortalsOp(path, method string) string {
	suffix := strings.Trim(strings.TrimPrefix(path, portalsPrefix), "/")

	if suffix == "" {
		switch method {
		case http.MethodPost:
			return "CreatePortal"
		case http.MethodGet:
			return "ListPortals"
		}

		return opUnknown
	}

	parts := strings.SplitN(suffix, "/", maxPathParts)
	if len(parts) == 1 {
		switch method {
		case http.MethodGet:
			return "GetPortal"
		case http.MethodPatch:
			return "UpdatePortal"
		case http.MethodDelete:
			return "DeletePortal"
		}
	}
	if len(parts) == pathPartTwo && parts[1] == collPreview && method == http.MethodPost {
		return "PreviewPortal"
	}
	if len(parts) == pathPartTwo && parts[1] == collPublish {
		switch method {
		case http.MethodPost:
			return "PublishPortal"
		case http.MethodDelete:
			return "DisablePortal"
		}
	}

	return opUnknown
}

func extractPortalProductsOp(path, method string) string {
	suffix := strings.Trim(strings.TrimPrefix(path, portalProductsPrefix), "/")

	if suffix == "" {
		switch method {
		case http.MethodPost:
			return "CreatePortalProduct"
		case http.MethodGet:
			return "ListPortalProducts"
		}

		return opUnknown
	}

	parts := strings.SplitN(suffix, "/", maxPathParts)

	switch len(parts) {
	case pathPartOne:
		return extractPortalProductByIDOp(method)
	case pathPartTwo:
		return extractPortalProductSubCollOp(parts[1], method)
	case maxPathParts:
		return extractPortalProductPageOp(parts[1], method)
	}

	return opUnknown
}

func extractPortalProductByIDOp(method string) string {
	switch method {
	case http.MethodGet:
		return "GetPortalProduct"
	case http.MethodPatch:
		return "UpdatePortalProduct"
	case http.MethodDelete:
		return "DeletePortalProduct"
	}

	return opUnknown
}

func extractPortalProductSubCollOp(collection, method string) string {
	switch collection {
	case collProductPages:
		switch method {
		case http.MethodPost:
			return "CreateProductPage"
		case http.MethodGet:
			return "ListProductPages"
		}
	case collProductREPages:
		switch method {
		case http.MethodPost:
			return "CreateProductRestEndpointPage"
		case http.MethodGet:
			return "ListProductRestEndpointPages"
		}
	case collSharingPolicy:
		switch method {
		case http.MethodGet:
			return "GetPortalProductSharingPolicy"
		case http.MethodPut:
			return "PutPortalProductSharingPolicy"
		case http.MethodDelete:
			return "DeletePortalProductSharingPolicy"
		}
	}

	return opUnknown
}

func extractPortalProductPageOp(collection, method string) string {
	switch collection {
	case collProductPages:
		switch method {
		case http.MethodGet:
			return "GetProductPage"
		case http.MethodDelete:
			return "DeleteProductPage"
		case http.MethodPatch:
			return "UpdateProductPage"
		}
	case collProductREPages:
		switch method {
		case http.MethodGet:
			return "GetProductRestEndpointPage"
		case http.MethodDelete:
			return "DeleteProductRestEndpointPage"
		case http.MethodPatch:
			return "UpdateProductRestEndpointPage"
		}
	}

	return opUnknown
}

// handlePortalsPath handles requests for /v2/portals and /v2/portals/{portalId}/...
func (h *Handler) handlePortalsPath(c *echo.Context, method, path string) error {
	suffix := strings.TrimPrefix(path, portalsPrefix)
	suffix = strings.Trim(suffix, "/")

	if suffix == "" {
		switch method {
		case http.MethodPost:
			return handleCreateNoParent(c, "portal", func(input CreatePortalInput) (*Portal, error) {
				return h.Backend.CreatePortal(input)
			})
		case http.MethodGet:
			return h.handleListPortals(c)
		default:
			return writeErr(c, http.StatusMethodNotAllowed, msgMethodNotAllowed)
		}
	}

	parts := strings.Split(suffix, "/")
	if len(parts) == 1 {
		switch method {
		case http.MethodGet:
			return h.handleGetPortal(c, parts[0])
		case http.MethodPatch:
			return h.handleUpdatePortal(c, parts[0])
		case http.MethodDelete:
			return h.handleDeletePortal(c, parts[0])
		}
	}
	if len(parts) == pathPartTwo {
		return h.handlePortalSubAction(c, method, parts[0], parts[1])
	}

	return writeErr(c, http.StatusNotFound, msgNotFound)
}

func (h *Handler) handlePortalSubAction(c *echo.Context, method, portalID, action string) error {
	switch {
	case action == collPreview && method == http.MethodPost:
		p, err := h.Backend.GetPortal(portalID)
		if err != nil {
			if errors.Is(err, ErrPortalNotFound) {
				return writeErr(c, http.StatusNotFound, msgNotFound)
			}

			return writeErr(c, http.StatusInternalServerError, err.Error())
		}

		return c.JSON(http.StatusOK, p)
	case action == collPublish && method == http.MethodPost:
		return h.handlePublishPortal(c, portalID)
	case action == collPublish && method == http.MethodDelete:
		return h.handleDisablePortal(c, portalID)
	default:
		return writeErr(c, http.StatusNotFound, msgNotFound)
	}
}

// handlePortalProductsPath handles requests for /v2/portalproducts and nested paths.
func (h *Handler) handlePortalProductsPath(c *echo.Context, method, path string) error {
	suffix := strings.TrimPrefix(path, portalProductsPrefix)
	suffix = strings.Trim(suffix, "/")

	if suffix == "" {
		switch method {
		case http.MethodPost:
			return handleCreateNoParent(
				c,
				"portal product",
				func(input CreatePortalProductInput) (*PortalProduct, error) {
					return h.Backend.CreatePortalProduct(input)
				},
			)
		case http.MethodGet:
			return h.handleListPortalProducts(c)
		default:
			return writeErr(c, http.StatusMethodNotAllowed, msgMethodNotAllowed)
		}
	}

	parts := strings.Split(suffix, "/")

	switch len(parts) {
	case pathPartOne:
		switch method {
		case http.MethodGet:
			return h.handleGetPortalProduct(c, parts[0])
		case http.MethodPatch:
			return h.handleUpdatePortalProduct(c, parts[0])
		case http.MethodDelete:
			return h.handleDeletePortalProduct(c, parts[0])
		}
	case pathPartTwo:
		return h.handlePortalProductSubCollection(c, method, parts[0], parts[1])
	case maxPathParts:
		return h.handlePortalProductPage(c, method, parts[0], parts[1], parts[pathPartTwo])
	}

	return writeErr(c, http.StatusNotFound, msgNotFound)
}

func (h *Handler) handlePortalProductSubCollection(
	c *echo.Context, method, portalProductID, subColl string,
) error {
	switch method {
	case http.MethodPost:
		switch subColl {
		case collProductPages:
			return handleCreate(c, portalProductID, "product page", ErrPortalProductNotFound,
				func(input CreateProductPageInput) (*ProductPage, error) {
					return h.Backend.CreateProductPage(portalProductID, input)
				})
		case collProductREPages:
			return handleCreate(c, portalProductID, "product rest endpoint page", ErrPortalProductNotFound,
				func(input CreateProductRestEndpointPageInput) (*ProductRestEndpointPage, error) {
					return h.Backend.CreateProductRestEndpointPage(portalProductID, input)
				})
		}
	case http.MethodGet:
		switch subColl {
		case collProductPages:
			return h.handleListProductPages(c, portalProductID)
		case collProductREPages:
			return h.handleListProductRestEndpointPages(c, portalProductID)
		case collSharingPolicy:
			return h.handleGetPortalProductSharingPolicy(c, portalProductID)
		}
	case http.MethodPut:
		if subColl == collSharingPolicy {
			return h.handlePutPortalProductSharingPolicy(c, portalProductID)
		}
	case http.MethodDelete:
		if subColl == collSharingPolicy {
			return h.handleDeletePortalProductSharingPolicy(c, portalProductID)
		}
	}

	return writeErr(c, http.StatusNotFound, msgNotFound)
}

func (h *Handler) handlePortalProductPage(
	c *echo.Context, method, portalProductID, collection, pageID string,
) error {
	switch collection {
	case collProductPages:
		switch method {
		case http.MethodGet:
			return h.handleGetProductPage(c, portalProductID, pageID)
		case http.MethodDelete:
			return h.handleDeleteProductPage(c, portalProductID, pageID)
		case http.MethodPatch:
			return h.handleUpdateProductPage(c, portalProductID, pageID)
		}
	case collProductREPages:
		switch method {
		case http.MethodGet:
			return h.handleGetProductRestEndpointPage(c, portalProductID, pageID)
		case http.MethodDelete:
			return h.handleDeleteProductRestEndpointPage(c, portalProductID, pageID)
		case http.MethodPatch:
			return h.handleUpdateProductRestEndpointPage(c, portalProductID, pageID)
		}
	}

	return writeErr(c, http.StatusNotFound, msgNotFound)
}

func (h *Handler) handleListPortals(c *echo.Context) error {
	log := logger.Load(c.Request().Context())

	items, err := h.Backend.ListPortals()
	if err != nil {
		log.Error("apigatewayv2: list portals failed", "error", err)

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, listPortalsOutput{Items: items})
}

func (h *Handler) handleGetPortal(c *echo.Context, portalID string) error {
	log := logger.Load(c.Request().Context())

	p, err := h.Backend.GetPortal(portalID)
	if err != nil {
		log.Error("apigatewayv2: get portal failed", "portalId", portalID, "error", err)

		if errors.Is(err, ErrPortalNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, p)
}

func (h *Handler) handleListPortalProducts(c *echo.Context) error {
	log := logger.Load(c.Request().Context())

	items, err := h.Backend.ListPortalProducts()
	if err != nil {
		log.Error("apigatewayv2: list portal products failed", "error", err)

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, listPortalProductsOutput{Items: items})
}

func (h *Handler) handleGetPortalProduct(c *echo.Context, portalProductID string) error {
	log := logger.Load(c.Request().Context())

	pp, err := h.Backend.GetPortalProduct(portalProductID)
	if err != nil {
		log.Error("apigatewayv2: get portal product failed", "portalProductId", portalProductID, "error", err)

		if errors.Is(err, ErrPortalProductNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, pp)
}

func (h *Handler) handleListProductPages(c *echo.Context, portalProductID string) error {
	log := logger.Load(c.Request().Context())

	items, err := h.Backend.ListProductPages(portalProductID)
	if err != nil {
		log.Error("apigatewayv2: list product pages failed", "portalProductId", portalProductID, "error", err)

		if errors.Is(err, ErrPortalProductNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, listProductPagesOutput{Items: items})
}

func (h *Handler) handleListProductRestEndpointPages(c *echo.Context, portalProductID string) error {
	log := logger.Load(c.Request().Context())

	items, err := h.Backend.ListProductRestEndpointPages(portalProductID)
	if err != nil {
		log.Error("apigatewayv2: list product rest endpoint pages failed",
			"portalProductId", portalProductID, "error", err)

		if errors.Is(err, ErrPortalProductNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, listProductREPagesOutput{Items: items})
}

func (h *Handler) handleUpdatePortal(c *echo.Context, portalID string) error {
	return handleUpdate(c, portalID, "", "portal",
		func(input UpdatePortalInput) (*Portal, error) {
			return h.Backend.UpdatePortal(portalID, input)
		},
		ErrPortalNotFound)
}

func (h *Handler) handlePublishPortal(c *echo.Context, portalID string) error {
	var input PublishPortalInput
	if err := json.NewDecoder(c.Request().Body).Decode(&input); err != nil && !errors.Is(err, io.EOF) {
		return writeErr(c, http.StatusBadRequest, msgInvalidBody)
	}

	p, err := h.Backend.UpdatePortal(portalID, UpdatePortalInput{
		Status:             "PUBLISHED",
		PublishDescription: input.Description,
	})
	if err != nil {
		if errors.Is(err, ErrPortalNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, p)
}

func (h *Handler) handleDisablePortal(c *echo.Context, portalID string) error {
	p, err := h.Backend.UpdatePortal(portalID, UpdatePortalInput{Status: "DISABLED"})
	if err != nil {
		if errors.Is(err, ErrPortalNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, p)
}

func (h *Handler) handleDeletePortal(c *echo.Context, portalID string) error {
	log := logger.Load(c.Request().Context())

	if err := h.Backend.DeletePortal(portalID); err != nil {
		log.Error("apigatewayv2: delete portal failed", "portalId", portalID, "error", err)

		if errors.Is(err, ErrPortalNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUpdatePortalProduct(c *echo.Context, portalProductID string) error {
	return handleUpdate(c, portalProductID, "", "portal product",
		func(input UpdatePortalProductInput) (*PortalProduct, error) {
			return h.Backend.UpdatePortalProduct(portalProductID, input)
		},
		ErrPortalProductNotFound)
}

func (h *Handler) handleGetPortalProductSharingPolicy(c *echo.Context, portalProductID string) error {
	policy, err := h.Backend.GetPortalProductSharingPolicy(portalProductID)
	if err != nil {
		if errors.Is(err, ErrPortalProductNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, policy)
}

func (h *Handler) handlePutPortalProductSharingPolicy(c *echo.Context, portalProductID string) error {
	var input PortalProductSharingPolicy
	if err := json.NewDecoder(c.Request().Body).Decode(&input); err != nil {
		return writeErr(c, http.StatusBadRequest, msgInvalidBody)
	}

	policy, err := h.Backend.PutPortalProductSharingPolicy(portalProductID, input.PolicyDocument)
	if err != nil {
		if errors.Is(err, ErrPortalProductNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, policy)
}

func (h *Handler) handleDeletePortalProductSharingPolicy(c *echo.Context, portalProductID string) error {
	if err := h.Backend.DeletePortalProductSharingPolicy(portalProductID); err != nil {
		if errors.Is(err, ErrPortalProductNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUpdateProductPage(c *echo.Context, portalProductID, pageID string) error {
	return handleUpdate(c, portalProductID, pageID, "product page",
		func(input UpdateProductPageInput) (*ProductPage, error) {
			return h.Backend.UpdateProductPage(portalProductID, pageID, input)
		},
		ErrPortalProductNotFound, ErrProductPageNotFound)
}

func (h *Handler) handleUpdateProductRestEndpointPage(c *echo.Context, portalProductID, pageID string) error {
	return handleUpdate(c, portalProductID, pageID, "product rest endpoint page",
		func(input UpdateProductRestEndpointPageInput) (*ProductRestEndpointPage, error) {
			return h.Backend.UpdateProductRestEndpointPage(portalProductID, pageID, input)
		},
		ErrPortalProductNotFound, ErrProductREPageNotFound)
}

func (h *Handler) handleDeletePortalProduct(c *echo.Context, portalProductID string) error {
	log := logger.Load(c.Request().Context())

	if err := h.Backend.DeletePortalProduct(portalProductID); err != nil {
		log.Error("apigatewayv2: delete portal product failed", "portalProductId", portalProductID, "error", err)

		if errors.Is(err, ErrPortalProductNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleGetProductPage(c *echo.Context, portalProductID, pageID string) error {
	log := logger.Load(c.Request().Context())

	p, err := h.Backend.GetProductPage(portalProductID, pageID)
	if err != nil {
		log.Error("apigatewayv2: get product page failed",
			"portalProductId", portalProductID, "pageId", pageID, "error", err)

		if errors.Is(err, ErrPortalProductNotFound) || errors.Is(err, ErrProductPageNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, p)
}

func (h *Handler) handleDeleteProductPage(c *echo.Context, portalProductID, pageID string) error {
	log := logger.Load(c.Request().Context())

	if err := h.Backend.DeleteProductPage(portalProductID, pageID); err != nil {
		log.Error("apigatewayv2: delete product page failed",
			"portalProductId", portalProductID, "pageId", pageID, "error", err)

		if errors.Is(err, ErrPortalProductNotFound) || errors.Is(err, ErrProductPageNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleGetProductRestEndpointPage(c *echo.Context, portalProductID, pageID string) error {
	log := logger.Load(c.Request().Context())

	p, err := h.Backend.GetProductRestEndpointPage(portalProductID, pageID)
	if err != nil {
		log.Error("apigatewayv2: get product rest endpoint page failed",
			"portalProductId", portalProductID, "pageId", pageID, "error", err)

		if errors.Is(err, ErrPortalProductNotFound) || errors.Is(err, ErrProductREPageNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, p)
}

func (h *Handler) handleDeleteProductRestEndpointPage(c *echo.Context, portalProductID, pageID string) error {
	log := logger.Load(c.Request().Context())

	if err := h.Backend.DeleteProductRestEndpointPage(portalProductID, pageID); err != nil {
		log.Error("apigatewayv2: delete product rest endpoint page failed",
			"portalProductId", portalProductID, "pageId", pageID, "error", err)

		if errors.Is(err, ErrPortalProductNotFound) || errors.Is(err, ErrProductREPageNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}
