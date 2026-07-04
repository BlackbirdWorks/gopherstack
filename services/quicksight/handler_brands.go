package quicksight

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"
)

// JSON response keys used only by brand operations.
const (
	keyBrandID         = "BrandId"
	keyBrand           = "Brand"
	keyBrandDetail     = "BrandDetail"
	keyBrandDefinition = "BrandDefinition"
	keyBrandArn        = "BrandArn"
	keyBrands          = "Brands"
	keyBrandStatus     = "BrandStatus"
	keyVersionID       = "VersionId"
	keyVersionStatus   = "VersionStatus"
	keyBrandName       = "BrandName"
)

func isBrandOp(op string) bool {
	switch op {
	case opCreateBrand, opDescribeBrand, opUpdateBrand, opDeleteBrand, opListBrands,
		opDescribeBrandAssignment, opUpdateBrandAssignment, opDeleteBrandAssignment,
		opDescribeBrandPublishedVer, opUpdateBrandPublishedVer:
		return true
	}

	return false
}

func (h *Handler) dispatchBrand(c *echo.Context, op string) error {
	switch op {
	case opCreateBrand:
		return h.handleCreateBrand(c)
	case opDescribeBrand:
		return h.handleDescribeBrand(c)
	case opUpdateBrand:
		return h.handleUpdateBrand(c)
	case opDeleteBrand:
		return h.handleDeleteBrand(c)
	case opListBrands:
		return h.handleListBrands(c)
	case opDescribeBrandAssignment:
		return h.handleDescribeBrandAssignment(c)
	case opUpdateBrandAssignment:
		return h.handleUpdateBrandAssignment(c)
	case opDeleteBrandAssignment:
		return h.handleDeleteBrandAssignment(c)
	case opDescribeBrandPublishedVer:
		return h.handleDescribeBrandPublishedVersion(c)
	case opUpdateBrandPublishedVer:
		return h.handleUpdateBrandPublishedVersion(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		"operation not implemented: "+op,
	)
}

func brandToMap(brand *Brand) map[string]any {
	return map[string]any{
		keyBrandID:         brand.BrandID,
		keyArn:             brand.Arn,
		keyBrandStatus:     brand.Status,
		keyVersionID:       brand.CurrentVersionID,
		keyCreatedTime:     brand.CreatedTime.Unix(),
		keyLastUpdatedTime: brand.LastUpdatedTime.Unix(),
	}
}

func (h *Handler) handleCreateBrand(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	brandID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	brand, err := h.Backend.CreateBrand(accountID, brandID, mapField(body, keyBrandDefinition))
	if err != nil {
		if errors.Is(err, ErrBrandAlreadyExists) {
			return writeError(c, http.StatusConflict, errResourceExistsCode, err.Error())
		}

		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyBrandDetail:     brandToMap(brand),
		keyBrandDefinition: brand.Definition,
		keyRequestID:       reqIDPlaceholder,
		keyStatus:          http.StatusOK,
	})
}

func (h *Handler) handleDescribeBrand(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	brandID := seg(segs, segResID)
	versionID := queryParam(c, "version-id")

	brand, err := h.Backend.DescribeBrand(accountID, brandID, versionID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyBrandDetail:     brandToMap(brand),
		keyBrandDefinition: brand.Definition,
		keyRequestID:       reqIDPlaceholder,
		keyStatus:          http.StatusOK,
	})
}

func (h *Handler) handleUpdateBrand(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	brandID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	brand, err := h.Backend.UpdateBrand(accountID, brandID, mapField(body, keyBrandDefinition))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyBrandDetail:     brandToMap(brand),
		keyBrandDefinition: brand.Definition,
		keyRequestID:       reqIDPlaceholder,
		keyStatus:          http.StatusOK,
	})
}

func (h *Handler) handleDeleteBrand(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	brandID := seg(segs, segResID)

	if err := h.Backend.DeleteBrand(accountID, brandID); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListBrands(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	brands, next, err := h.Backend.ListBrands(accountID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(brands))
	for _, brand := range brands {
		name, _ := brand.Definition[keyBrandName].(string)
		items = append(items, map[string]any{
			keyBrandID:         brand.BrandID,
			keyArn:             brand.Arn,
			keyBrandName:       name,
			keyBrandStatus:     brand.Status,
			keyCreatedTime:     brand.CreatedTime.Unix(),
			keyLastUpdatedTime: brand.LastUpdatedTime.Unix(),
		})
	}

	resp := map[string]any{
		keyBrands:    items,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleDescribeBrandPublishedVersion(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	brandID := seg(segs, segResID)

	brand, err := h.Backend.DescribeBrandPublishedVersion(accountID, brandID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyBrandDetail:     brandToMap(brand),
		keyBrandDefinition: brand.Definition,
		keyRequestID:       reqIDPlaceholder,
		keyStatus:          http.StatusOK,
	})
}

func (h *Handler) handleUpdateBrandPublishedVersion(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	brandID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	versionID := strField(body, keyVersionID)
	if err = h.Backend.UpdateBrandPublishedVersion(accountID, brandID, versionID); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyVersionID: versionID,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDescribeBrandAssignment(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	brandArn, err := h.Backend.DescribeBrandAssignment(accountID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyBrandArn:  brandArn,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleUpdateBrandAssignment(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	brandArn, err := h.Backend.UpdateBrandAssignment(accountID, strField(body, keyBrandArn))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyBrandArn:  brandArn,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDeleteBrandAssignment(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	if err := h.Backend.DeleteBrandAssignment(accountID); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}
