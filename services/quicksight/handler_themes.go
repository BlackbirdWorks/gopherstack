package quicksight

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"
)

// JSON response keys used only by theme operations.
const (
	keyThemeID             = "ThemeId"
	keyTheme               = "Theme"
	keyThemeArn            = "ThemeArn"
	keyThemeSummaryList    = "ThemeSummaryList"
	keyThemeVersionSummary = "ThemeVersionSummaryList"
	keyThemeAlias          = "ThemeAlias"
	keyThemeAliasList      = "ThemeAliasList"
	keyThemeType           = "Type"
	keyBaseThemeID         = "BaseThemeId"
	keyConfiguration       = "Configuration"
)

func isThemeOp(op string) bool {
	switch op {
	case opCreateTheme, opDescribeTheme, opUpdateTheme, opDeleteTheme,
		opListThemes, opListThemeVersions,
		opDescribeThemePerms, opUpdateThemePerms,
		opCreateThemeAlias, opDescribeThemeAlias, opUpdateThemeAlias,
		opDeleteThemeAlias, opListThemeAliases:
		return true
	}

	return false
}

func (h *Handler) dispatchTheme(c *echo.Context, op string) error {
	switch op {
	case opCreateTheme:
		return h.handleCreateTheme(c)
	case opDescribeTheme:
		return h.handleDescribeTheme(c)
	case opUpdateTheme:
		return h.handleUpdateTheme(c)
	case opDeleteTheme:
		return h.handleDeleteTheme(c)
	case opListThemes:
		return h.handleListThemes(c)
	case opListThemeVersions:
		return h.handleListThemeVersions(c)
	case opDescribeThemePerms:
		return h.handleDescribeThemePermissions(c)
	case opUpdateThemePerms:
		return h.handleUpdateThemePermissions(c)
	case opCreateThemeAlias:
		return h.handleCreateThemeAlias(c)
	case opDescribeThemeAlias:
		return h.handleDescribeThemeAlias(c)
	case opUpdateThemeAlias:
		return h.handleUpdateThemeAlias(c)
	case opDeleteThemeAlias:
		return h.handleDeleteThemeAlias(c)
	case opListThemeAliases:
		return h.handleListThemeAliases(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		"operation not implemented: "+op,
	)
}

func (h *Handler) handleCreateTheme(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	themeID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	t, err := h.Backend.CreateTheme(
		accountID,
		themeID,
		strField(body, keyName),
		strField(body, keyBaseThemeID),
		strField(body, keyVersionDescription),
		mapField(body, keyConfiguration),
		permissionsField(body, keyPermissions),
		tagsFromBody(body),
	)
	if err != nil {
		if errors.Is(err, ErrThemeAlreadyExists) {
			return writeError(c, http.StatusConflict, errResourceExistsCode, err.Error())
		}

		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:             t.Arn,
		keyVersionArn:      t.Version.Arn,
		keyThemeID:         t.ThemeID,
		keyCreationStatus2: t.Version.Status,
		keyRequestID:       reqIDPlaceholder,
		keyStatus:          http.StatusOK,
	})
}

func (h *Handler) handleDescribeTheme(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	themeID := seg(segs, segResID)

	t, err := h.Backend.DescribeTheme(accountID, themeID, versionNumberParam(c))
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

	t, err := h.Backend.UpdateTheme(
		accountID,
		themeID,
		strField(body, keyName),
		strField(body, keyBaseThemeID),
		strField(body, keyVersionDescription),
		mapField(body, keyConfiguration),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyThemeID:         t.ThemeID,
		keyArn:             t.Arn,
		keyVersionArn:      t.Version.Arn,
		keyCreationStatus2: t.Version.Status,
		keyRequestID:       reqIDPlaceholder,
		keyStatus:          http.StatusOK,
	})
}

func (h *Handler) handleDeleteTheme(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	themeID := seg(segs, segResID)

	if err := h.Backend.DeleteTheme(accountID, themeID, versionNumberParam(c)); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyThemeID:   themeID,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListThemes(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	themes, next, err := h.Backend.ListThemes(accountID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(themes))
	for _, t := range themes {
		items = append(items, map[string]any{
			keyArn:                 t.Arn,
			keyThemeID:             t.ThemeID,
			keyName:                t.Name,
			keyThemeType:           t.Type,
			keyCreatedTime:         t.CreatedTime.Unix(),
			keyLastUpdatedTime:     t.LastUpdatedTime.Unix(),
			keyLatestVersionNumber: t.Version.VersionNumber,
		})
	}

	resp := map[string]any{
		keyThemeSummaryList: items,
		keyRequestID:        reqIDPlaceholder,
		keyStatus:           http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleListThemeVersions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	themeID := seg(segs, segResID)

	versions, next, err := h.Backend.ListThemeVersions(accountID, themeID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(versions))
	for _, v := range versions {
		items = append(items, themeVersionToMap(v))
	}

	resp := map[string]any{
		keyThemeVersionSummary: items,
		keyRequestID:           reqIDPlaceholder,
		keyStatus:              http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleDescribeThemePermissions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	themeID := seg(segs, segResID)

	t, perms, err := h.Backend.DescribeThemePermissions(accountID, themeID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyThemeID:     themeID,
		keyThemeArn:    t.Arn,
		keyPermissions: permissionsToMaps(perms),
		keyRequestID:   reqIDPlaceholder,
		keyStatus:      http.StatusOK,
	})
}

func (h *Handler) handleUpdateThemePermissions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	themeID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	t, perms, err := h.Backend.UpdateThemePermissions(
		accountID,
		themeID,
		permissionsField(body, "GrantPermissions"),
		permissionsField(body, "RevokePermissions"),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyThemeID:     themeID,
		keyThemeArn:    t.Arn,
		keyPermissions: permissionsToMaps(perms),
		keyRequestID:   reqIDPlaceholder,
		keyStatus:      http.StatusOK,
	})
}

func (h *Handler) handleCreateThemeAlias(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	themeID := seg(segs, segResID)
	aliasName := seg(segs, segSubResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	alias, err := h.Backend.CreateThemeAlias(
		accountID, themeID, aliasName, int64Field(body, "ThemeVersionNumber"),
	)
	if err != nil {
		if errors.Is(err, ErrThemeAliasAlreadyExists) {
			return writeError(c, http.StatusConflict, errResourceExistsCode, err.Error())
		}

		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyThemeAlias: themeAliasToMap(alias),
		keyRequestID:  reqIDPlaceholder,
		keyStatus:     http.StatusOK,
	})
}

func (h *Handler) handleDescribeThemeAlias(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	themeID := seg(segs, segResID)
	aliasName := seg(segs, segSubResID)

	alias, err := h.Backend.DescribeThemeAlias(accountID, themeID, aliasName)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyThemeAlias: themeAliasToMap(alias),
		keyRequestID:  reqIDPlaceholder,
		keyStatus:     http.StatusOK,
	})
}

func (h *Handler) handleUpdateThemeAlias(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	themeID := seg(segs, segResID)
	aliasName := seg(segs, segSubResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	alias, err := h.Backend.UpdateThemeAlias(
		accountID, themeID, aliasName, int64Field(body, "ThemeVersionNumber"),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyThemeAlias: themeAliasToMap(alias),
		keyRequestID:  reqIDPlaceholder,
		keyStatus:     http.StatusOK,
	})
}

func (h *Handler) handleDeleteThemeAlias(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	themeID := seg(segs, segResID)
	aliasName := seg(segs, segSubResID)

	if err := h.Backend.DeleteThemeAlias(accountID, themeID, aliasName); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyThemeID:   themeID,
		keyAliasName: aliasName,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListThemeAliases(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	themeID := seg(segs, segResID)

	aliases, next, err := h.Backend.ListThemeAliases(accountID, themeID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(aliases))
	for _, a := range aliases {
		items = append(items, themeAliasToMap(a))
	}

	resp := map[string]any{
		keyThemeAliasList: items,
		keyRequestID:      reqIDPlaceholder,
		keyStatus:         http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

// ---- shared helpers ----

func themeToMap(t *Theme) map[string]any {
	return map[string]any{
		keyArn:             t.Arn,
		keyThemeID:         t.ThemeID,
		keyName:            t.Name,
		keyThemeType:       t.Type,
		keyCreatedTime:     t.CreatedTime.Unix(),
		keyLastUpdatedTime: t.LastUpdatedTime.Unix(),
		keyVersion:         themeVersionToMap(t.Version),
	}
}

func themeVersionToMap(v *ThemeVersion) map[string]any {
	m := map[string]any{
		keyVersionNumber: v.VersionNumber,
		keyStatus:        v.Status,
		keyArn:           v.Arn,
		keyCreatedTime:   v.CreatedTime.Unix(),
	}
	if v.BaseThemeID != "" {
		m[keyBaseThemeID] = v.BaseThemeID
	}
	if v.Description != "" {
		m[keyDescription] = v.Description
	}
	if v.Configuration != nil {
		m[keyConfiguration] = v.Configuration
	}

	return m
}

func themeAliasToMap(a *ThemeAlias) map[string]any {
	return map[string]any{
		keyAliasName:         a.AliasName,
		keyArn:               a.Arn,
		"ThemeVersionNumber": a.ThemeVersionNumber,
	}
}

// classifyThemePaths routes /accounts/{id}/themes/... paths.
func classifyThemePaths(method string, segs []string, n int) (string, string) {
	switch n {
	case nSegsAccountRes:
		if method == http.MethodGet {
			return opListThemes, seg(segs, segAccountID)
		}
	case nSegsAccountResID:
		return classifyThemeByID(method, segs)
	case nSegsSubRes:
		return classifyThemeSubRes(method, segs)
	case nSegsSubResID:
		return classifyThemeAlias(method, segs)
	}

	return opUnknown, ""
}

func classifyThemeByID(method string, segs []string) (string, string) {
	id := seg(segs, segResID)
	switch method {
	case http.MethodPost:
		return opCreateTheme, id
	case http.MethodGet:
		return opDescribeTheme, id
	case http.MethodPut:
		return opUpdateTheme, id
	case http.MethodDelete:
		return opDeleteTheme, id
	}

	return opUnknown, ""
}

func classifyThemeSubRes(method string, segs []string) (string, string) {
	id := seg(segs, segResID)
	sub := seg(segs, segSubRes)

	switch sub {
	case pathSegPermissions:
		switch method {
		case http.MethodGet:
			return opDescribeThemePerms, id
		case http.MethodPut:
			return opUpdateThemePerms, id
		}
	case pathSegAliases:
		if method == http.MethodGet {
			return opListThemeAliases, id
		}
	case pathSegVersions:
		if method == http.MethodGet {
			return opListThemeVersions, id
		}
	}

	return opUnknown, ""
}

// classifyThemeAlias routes /accounts/{id}/themes/{id}/aliases/{alias}. Every
// method but DELETE identifies the resource by the alias name itself;
// DeleteThemeAlias's op-extraction historically used the theme id instead --
// preserved verbatim here (mirrors classifyTemplateAlias).
func classifyThemeAlias(method string, segs []string) (string, string) {
	if seg(segs, segSubRes) != pathSegAliases {
		return opUnknown, ""
	}

	id := seg(segs, segResID)
	alias := seg(segs, segSubResID)

	switch method {
	case http.MethodPost:
		return opCreateThemeAlias, alias
	case http.MethodGet:
		return opDescribeThemeAlias, alias
	case http.MethodPut:
		return opUpdateThemeAlias, alias
	case http.MethodDelete:
		return opDeleteThemeAlias, id
	}

	return opUnknown, ""
}
