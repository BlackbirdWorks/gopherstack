package securityhub

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

func classifyProductsPath(method, path string) (string, string) {
	switch {
	case method == http.MethodGet && path == "/products":
		return opDescribeProducts, ""
	case method == http.MethodGet && path == pathProductSubscriptions:
		return opListEnabledProductsForImport, ""
	case method == http.MethodPost && path == pathProductSubscriptions:
		return opEnableImportFindingsForProduct, ""
	case strings.HasPrefix(path, "/productSubscriptions/") && method == http.MethodDelete:
		return opDisableImportFindingsForProduct, strings.TrimPrefix(path, "/productSubscriptions/")
	}

	return opUnknown, ""
}

func (h *Handler) handleDescribeProducts(c *echo.Context) error {
	productArn := c.QueryParam("ProductArn")
	nextToken := c.QueryParam("NextToken")
	maxResults := queryInt(c)

	products, nextOut := h.Backend.DescribeProducts(productArn, nextToken, maxResults)
	items := make([]map[string]any, len(products))

	for i, p := range products {
		items[i] = map[string]any{
			"ProductArn":       p.ProductArn, //nolint:goconst // existing issue.
			"ProductName":      p.ProductName,
			"CompanyName":      p.CompanyName,
			keyDescription:     p.Description,
			"Categories":       p.Categories,
			"IntegrationTypes": p.IntegrationTypes,
			"MarketplaceUrl":   p.MarketplaceURL,
			"ActivationUrl":    p.ActivationURL,
		}
	}

	resp := map[string]any{"Products": items}
	if nextOut != "" {
		resp["NextToken"] = nextOut
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleListEnabledProductsForImport(c *echo.Context) error {
	nextToken := c.QueryParam("NextToken")
	maxResults := queryInt(c)

	arns, nextOut := h.Backend.ListEnabledProductsForImport(nextToken, maxResults)

	resp := map[string]any{"ProductSubscriptions": arns}
	if nextOut != "" {
		resp["NextToken"] = nextOut
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleEnableImportFindingsForProduct(c *echo.Context, body map[string]any) error {
	productArn, _ := body["ProductArn"].(string)

	// ErrHubNotEnabled is left unheadered: EnableImportFindingsForProduct's
	// error list also carries InvalidAccessException (securityhub@v1.75.4
	// deserializers.go), same ambiguity as handler_hub.go's V1 handlers.
	subArn, err := h.Backend.EnableImportFindingsForProduct(productArn)
	if err != nil {
		if errors.Is(err, ErrHubNotEnabled) {
			return c.JSON(http.StatusBadRequest, map[string]any{keyMessage: msgHubNotEnabled})
		}

		if errors.Is(err, ErrAlreadyExists) {
			return typedErrorResponse(c, http.StatusConflict, "ResourceConflictException", err.Error())
		}

		return typedErrorResponse(c, http.StatusInternalServerError, "InternalException", err.Error())
	}

	return c.JSON(http.StatusOK, map[string]any{"ProductSubscriptionArn": subArn})
}

func (h *Handler) handleDisableImportFindingsForProduct(c *echo.Context, productSubscriptionArn string) error {
	if err := h.Backend.DisableImportFindingsForProduct(productSubscriptionArn); err != nil {
		if errors.Is(err, ErrNotFound) {
			return typedErrorResponse(
				c,
				http.StatusNotFound,
				"ResourceNotFoundException",
				"Product subscription not found",
			)
		}

		return typedErrorResponse(c, http.StatusInternalServerError, "InternalException", err.Error())
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func classifyProductsV2Path(method, path string) (string, string) {
	if method == http.MethodGet && path == "/productsV2" {
		return opDescribeProductsV2, ""
	}

	return opUnknown, ""
}

func (h *Handler) handleDescribeProductsV2(c *echo.Context) error {
	nextToken := c.QueryParam("NextToken")
	maxResults := 0

	if v := c.QueryParam("MaxResults"); v != "" {
		maxResults, _ = strconv.Atoi(v)
	}

	products, next := h.Backend.DescribeProductsV2(nextToken, maxResults)

	var out []map[string]any //nolint:prealloc // existing issue.

	for _, p := range products {
		out = append(out, map[string]any{
			"ProductArn":       p.ProductArn,
			"ProductName":      p.ProductName,
			"CompanyName":      p.CompanyName,
			keyDescription:     p.Description,
			"Categories":       p.Categories,
			"IntegrationTypes": p.IntegrationTypes,
			"MarketplaceUrl":   p.MarketplaceURL,
			"ActivationUrl":    p.ActivationURL,
		})
	}

	if out == nil {
		out = []map[string]any{}
	}

	resp := map[string]any{"Products": out}

	if next != "" {
		resp["NextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

func classifyRecommendedPolicyV2Path(method, path string) (string, string) {
	prefix := "/recommendedPolicyV2/"
	switch {
	case method == http.MethodPost && strings.HasPrefix(path, prefix):
		return opGenerateRecommendedPolicyV2, strings.TrimPrefix(path, prefix)
	case method == http.MethodGet && strings.HasPrefix(path, prefix):
		return opGetRecommendedPolicyV2, strings.TrimPrefix(path, prefix)
	}

	return opUnknown, ""
}

func (h *Handler) handleGenerateRecommendedPolicyV2(c *echo.Context, metadataUID string, _ map[string]any) error {
	rec, err := h.Backend.GenerateRecommendedPolicyV2(metadataUID)
	if err != nil {
		return typedErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	return c.JSON(http.StatusOK, map[string]any{
		"MetadataUid":    rec.MetadataUid,
		"Policy":         rec.Policy,
		"GenerationTime": rec.GenerationTime,
	})
}

func (h *Handler) handleGetRecommendedPolicyV2(c *echo.Context, metadataUID string) error {
	rec, err := h.Backend.GetRecommendedPolicyV2(metadataUID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return typedErrorResponse(
				c,
				http.StatusNotFound,
				"ResourceNotFoundException",
				"Recommended policy not found",
			)
		}

		return typedErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	return c.JSON(http.StatusOK, map[string]any{
		"MetadataUid":    rec.MetadataUid,
		"Policy":         rec.Policy,
		"GenerationTime": rec.GenerationTime,
	})
}

// productsOpHandlers returns the Products (V1 + V2 + Recommended Policy)
// operation dispatch table for handleREST.
func (h *Handler) productsOpHandlers(
	c *echo.Context,
	resource string,
	body map[string]any,
) map[string]func() error {
	return map[string]func() error{
		opDescribeProducts:                func() error { return h.handleDescribeProducts(c) },
		opListEnabledProductsForImport:    func() error { return h.handleListEnabledProductsForImport(c) },
		opEnableImportFindingsForProduct:  func() error { return h.handleEnableImportFindingsForProduct(c, body) },
		opDisableImportFindingsForProduct: func() error { return h.handleDisableImportFindingsForProduct(c, resource) },
		opDescribeProductsV2:              func() error { return h.handleDescribeProductsV2(c) },
		opGenerateRecommendedPolicyV2:     func() error { return h.handleGenerateRecommendedPolicyV2(c, resource, body) },
		opGetRecommendedPolicyV2:          func() error { return h.handleGetRecommendedPolicyV2(c, resource) },
	}
}
