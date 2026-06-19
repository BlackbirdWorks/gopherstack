package quicksight

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// Response keys for the stateful Appendix-A resource families. Declared as
// constants to keep the handlers DRY and satisfy goconst.
const (
	keyFolderID        = "FolderId"
	keyFolder          = "Folder"
	keyFolderSummaries = "FolderSummaryList"

	keyTemplateID        = "TemplateId"
	keyTemplate          = "Template"
	keyTemplateSummaries = "TemplateSummaryList"

	keyThemeID        = "ThemeId"
	keyTheme          = "Theme"
	keyThemeSummaries = "ThemeSummaryList"

	keyVPCConnectionID  = "VPCConnectionId"
	keyVPCConnection    = "VPCConnection"
	keyVPCConnSummaries = "VPCConnectionSummaries"

	keyBrandID  = "BrandId"
	keyBrand    = "Brand"
	keyBrands   = "Brands"
	keyBrandArn = "BrandArn"

	keyUpdateStatus = "UpdateStatus"
)

// ---- Folder handlers ----

func (h *Handler) handleCreateFolder(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	folderID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	f, err := h.Backend.CreateFolder(accountID, folderID, strField(body, "Name"), strField(body, "FolderType"))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:       f.Arn,
		keyFolderID:  f.FolderID,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDescribeFolder(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	folderID := seg(segs, segResID)

	f, err := h.Backend.DescribeFolder(accountID, folderID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyFolder:    folderToMap(f),
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleUpdateFolder(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	folderID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	f, err := h.Backend.UpdateFolder(accountID, folderID, strField(body, "Name"))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:       f.Arn,
		keyFolderID:  f.FolderID,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDeleteFolder(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	folderID := seg(segs, segResID)

	if err := h.Backend.DeleteFolder(accountID, folderID); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:       h.buildResourceARN("folder", folderID),
		keyFolderID:  folderID,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListFolders(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	folders, err := h.Backend.ListFolders(accountID)
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(folders))
	for _, f := range folders {
		items = append(items, folderToMap(f))
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyFolderSummaries: items,
		keyRequestID:       reqIDPlaceholder,
		keyStatus:          http.StatusOK,
	})
}

func folderToMap(f *Folder) map[string]any {
	return map[string]any{
		keyArn:             f.Arn,
		keyCreatedTime:     f.CreatedTime.Format(timeFormat),
		keyFolderID:        f.FolderID,
		keyLastUpdatedTime: f.LastUpdatedTime.Format(timeFormat),
		keyName:            f.Name,
		"FolderType":       f.FolderType,
	}
}

// ---- Template handlers ----

func (h *Handler) handleCreateTemplate(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	templateID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	t, err := h.Backend.CreateTemplate(accountID, templateID, strField(body, "Name"))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:            t.Arn,
		keyTemplateID:     t.TemplateID,
		keyCreationStatus: t.Status,
		keyRequestID:      reqIDPlaceholder,
		keyStatus:         http.StatusOK,
	})
}

func (h *Handler) handleDescribeTemplate(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	templateID := seg(segs, segResID)

	t, err := h.Backend.DescribeTemplate(accountID, templateID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyTemplate:  templateToMap(t),
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleUpdateTemplate(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	templateID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	t, err := h.Backend.UpdateTemplate(accountID, templateID, strField(body, "Name"))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:            t.Arn,
		keyTemplateID:     t.TemplateID,
		keyCreationStatus: t.Status,
		keyRequestID:      reqIDPlaceholder,
		keyStatus:         http.StatusOK,
	})
}

func (h *Handler) handleDeleteTemplate(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	templateID := seg(segs, segResID)

	if err := h.Backend.DeleteTemplate(accountID, templateID); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:        h.buildResourceARN("template", templateID),
		keyTemplateID: templateID,
		keyRequestID:  reqIDPlaceholder,
		keyStatus:     http.StatusOK,
	})
}

func (h *Handler) handleListTemplates(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	templates, err := h.Backend.ListTemplates(accountID)
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(templates))
	for _, t := range templates {
		items = append(items, templateToMap(t))
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyTemplateSummaries: items,
		keyRequestID:         reqIDPlaceholder,
		keyStatus:            http.StatusOK,
	})
}

func templateToMap(t *Template) map[string]any {
	return map[string]any{
		keyArn:                t.Arn,
		keyCreatedTime:        t.CreatedTime.Format(timeFormat),
		keyTemplateID:         t.TemplateID,
		keyLastUpdatedTime:    t.LastUpdatedTime.Format(timeFormat),
		keyName:               t.Name,
		"LatestVersionNumber": t.VersionNumber,
	}
}

// ---- Theme handlers ----

func (h *Handler) handleCreateTheme(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	themeID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	t, err := h.Backend.CreateTheme(accountID, themeID, strField(body, "Name"))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:            t.Arn,
		keyThemeID:        t.ThemeID,
		keyCreationStatus: t.Status,
		keyRequestID:      reqIDPlaceholder,
		keyStatus:         http.StatusOK,
	})
}

func (h *Handler) handleDescribeTheme(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	themeID := seg(segs, segResID)

	t, err := h.Backend.DescribeTheme(accountID, themeID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyTheme:     themeToMap(t),
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleUpdateTheme(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	themeID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	t, err := h.Backend.UpdateTheme(accountID, themeID, strField(body, "Name"))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:            t.Arn,
		keyThemeID:        t.ThemeID,
		keyCreationStatus: t.Status,
		keyRequestID:      reqIDPlaceholder,
		keyStatus:         http.StatusOK,
	})
}

func (h *Handler) handleDeleteTheme(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	themeID := seg(segs, segResID)

	if err := h.Backend.DeleteTheme(accountID, themeID); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:       h.buildResourceARN("theme", themeID),
		keyThemeID:   themeID,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListThemes(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	themes, err := h.Backend.ListThemes(accountID)
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(themes))
	for _, t := range themes {
		items = append(items, themeToMap(t))
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyThemeSummaries: items,
		keyRequestID:      reqIDPlaceholder,
		keyStatus:         http.StatusOK,
	})
}

func themeToMap(t *Theme) map[string]any {
	return map[string]any{
		keyArn:                t.Arn,
		keyCreatedTime:        t.CreatedTime.Format(timeFormat),
		keyThemeID:            t.ThemeID,
		keyLastUpdatedTime:    t.LastUpdatedTime.Format(timeFormat),
		keyName:               t.Name,
		"LatestVersionNumber": t.VersionNumber,
	}
}

// ---- VPC Connection handlers ----

func (h *Handler) handleCreateVPCConnection(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	v, err := h.Backend.CreateVPCConnection(accountID, strField(body, keyVPCConnectionID), strField(body, "Name"))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:               v.Arn,
		keyVPCConnectionID:   v.VPCConnectionID,
		keyCreationStatus:    v.Status,
		"AvailabilityStatus": v.AvailabilityState,
		keyRequestID:         reqIDPlaceholder,
		keyStatus:            http.StatusOK,
	})
}

func (h *Handler) handleDescribeVPCConnection(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	vpcID := seg(segs, segResID)

	v, err := h.Backend.DescribeVPCConnection(accountID, vpcID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyVPCConnection: vpcConnectionToMap(v),
		keyRequestID:     reqIDPlaceholder,
		keyStatus:        http.StatusOK,
	})
}

func (h *Handler) handleUpdateVPCConnection(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	vpcID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	v, err := h.Backend.UpdateVPCConnection(accountID, vpcID, strField(body, "Name"))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:             v.Arn,
		keyVPCConnectionID: v.VPCConnectionID,
		keyUpdateStatus:    v.Status,
		keyRequestID:       reqIDPlaceholder,
		keyStatus:          http.StatusOK,
	})
}

func (h *Handler) handleDeleteVPCConnection(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	vpcID := seg(segs, segResID)

	v, err := h.Backend.DeleteVPCConnection(accountID, vpcID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:             v.Arn,
		keyVPCConnectionID: v.VPCConnectionID,
		"DeletionStatus":   v.Status,
		keyRequestID:       reqIDPlaceholder,
		keyStatus:          http.StatusOK,
	})
}

func (h *Handler) handleListVPCConnections(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	conns, err := h.Backend.ListVPCConnections(accountID)
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(conns))
	for _, v := range conns {
		items = append(items, vpcConnectionToMap(v))
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyVPCConnSummaries: items,
		keyRequestID:        reqIDPlaceholder,
		keyStatus:           http.StatusOK,
	})
}

func vpcConnectionToMap(v *VPCConnection) map[string]any {
	return map[string]any{
		keyArn:               v.Arn,
		keyCreatedTime:       v.CreatedTime.Format(timeFormat),
		keyVPCConnectionID:   v.VPCConnectionID,
		keyLastUpdatedTime:   v.LastUpdatedTime.Format(timeFormat),
		keyName:              v.Name,
		keyStatus:            v.Status,
		"AvailabilityStatus": v.AvailabilityState,
	}
}

// ---- Brand handlers ----

func (h *Handler) handleCreateBrand(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	brandID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	br, err := h.Backend.CreateBrand(accountID, brandID, strField(body, "BrandName"))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyBrandArn:  br.Arn,
		keyBrandID:   br.BrandID,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDescribeBrand(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	brandID := seg(segs, segResID)

	br, err := h.Backend.DescribeBrand(accountID, brandID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyBrand:     brandToMap(br),
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
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

	br, err := h.Backend.UpdateBrand(accountID, brandID, strField(body, "BrandName"))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyBrandArn:  br.Arn,
		keyBrandID:   br.BrandID,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
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
		keyBrandID:   brandID,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListBrands(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	brands, err := h.Backend.ListBrands(accountID)
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(brands))
	for _, br := range brands {
		items = append(items, brandToMap(br))
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyBrands:    items,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func brandToMap(br *Brand) map[string]any {
	return map[string]any{
		keyBrandArn:        br.Arn,
		keyBrandID:         br.BrandID,
		keyCreatedTime:     br.CreatedTime.Format(timeFormat),
		keyLastUpdatedTime: br.LastUpdatedTime.Format(timeFormat),
		keyName:            br.Name,
		keyStatus:          br.Status,
	}
}

// buildResourceARN builds an ARN for a resource using the handler's region/account.
func (h *Handler) buildResourceARN(resourceType, resourceID string) string {
	return "arn:aws:quicksight:" + h.region + ":" + h.accountID + ":" + resourceType + "/" + resourceID
}
