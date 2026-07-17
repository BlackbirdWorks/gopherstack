package quicksight

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"
)

// JSON response keys used only by template operations.
const (
	keyTemplateID             = "TemplateId"
	keyTemplate               = "Template"
	keyTemplateArn            = "TemplateArn"
	keyTemplateSummaryList    = "TemplateSummaryList"
	keyTemplateVersionSummary = "TemplateVersionSummaryList"
	keyTemplateAlias          = "TemplateAlias"
	keyTemplateAliasList      = "TemplateAliasList"
	keyAliasName              = "AliasName"
	keyVersion                = "Version"
	keyVersionNumber          = "VersionNumber"
	keyVersionArn             = "VersionArn"
	keyCreationStatus2        = "CreationStatus"
	keyLatestVersionNumber    = "LatestVersionNumber"
	keySourceEntityArn        = "SourceEntityArn"
	keyDefinition             = "Definition"
	keyDescription            = "Description"
	keyResourceStatus         = "ResourceStatus"

	queryParamVersionNumber = "version-number"
	queryParamAliasName     = "alias-name"
)

func isTemplateOp(op string) bool {
	switch op {
	case opCreateTemplate, opDescribeTemplate, opUpdateTemplate, opDeleteTemplate,
		opListTemplates, opListTemplateVersions, opDescribeTemplateDefinition,
		opDescribeTemplatePerms, opUpdateTemplatePerms,
		opCreateTemplateAlias, opDescribeTemplateAlias, opUpdateTemplateAlias,
		opDeleteTemplateAlias, opListTemplateAliases:
		return true
	}

	return false
}

func (h *Handler) dispatchTemplate(c *echo.Context, op string) error {
	switch op {
	case opCreateTemplate:
		return h.handleCreateTemplate(c)
	case opDescribeTemplate:
		return h.handleDescribeTemplate(c)
	case opUpdateTemplate:
		return h.handleUpdateTemplate(c)
	case opDeleteTemplate:
		return h.handleDeleteTemplate(c)
	case opListTemplates:
		return h.handleListTemplates(c)
	case opListTemplateVersions:
		return h.handleListTemplateVersions(c)
	case opDescribeTemplateDefinition:
		return h.handleDescribeTemplateDefinition(c)
	case opDescribeTemplatePerms:
		return h.handleDescribeTemplatePermissions(c)
	case opUpdateTemplatePerms:
		return h.handleUpdateTemplatePermissions(c)
	case opCreateTemplateAlias:
		return h.handleCreateTemplateAlias(c)
	case opDescribeTemplateAlias:
		return h.handleDescribeTemplateAlias(c)
	case opUpdateTemplateAlias:
		return h.handleUpdateTemplateAlias(c)
	case opDeleteTemplateAlias:
		return h.handleDeleteTemplateAlias(c)
	case opListTemplateAliases:
		return h.handleListTemplateAliases(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		"operation not implemented: "+op,
	)
}

func (h *Handler) handleCreateTemplate(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	templateID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	t, err := h.Backend.CreateTemplate(
		accountID,
		templateID,
		strField(body, keyName),
		sourceEntityArnFromBody(body),
		mapField(body, keyDefinition),
		permissionsField(body, keyPermissions),
		tagsFromBody(body),
	)
	if err != nil {
		if errors.Is(err, ErrTemplateAlreadyExists) {
			return writeError(c, http.StatusConflict, errResourceExistsCode, err.Error())
		}

		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:             t.Arn,
		keyVersionArn:      t.Version.Arn,
		keyTemplateID:      t.TemplateID,
		keyCreationStatus2: t.Version.Status,
		keyRequestID:       reqIDPlaceholder,
		keyStatus:          http.StatusOK,
	})
}

func (h *Handler) handleDescribeTemplate(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	templateID := seg(segs, segResID)

	t, err := h.Backend.DescribeTemplate(accountID, templateID, versionNumberParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyTemplate:  templateToMap(t),
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDescribeTemplateDefinition(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	templateID := seg(segs, segResID)

	versionNumber := versionNumberParam(c)
	if aliasName := queryParam(c, queryParamAliasName); aliasName != "" {
		alias, err := h.Backend.DescribeTemplateAlias(accountID, templateID, aliasName)
		if err != nil {
			return httpErr(c, err)
		}
		versionNumber = alias.TemplateVersionNumber
	}

	t, err := h.Backend.DescribeTemplate(accountID, templateID, versionNumber)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyName:           t.Name,
		keyTemplateID:     t.TemplateID,
		keyResourceStatus: t.Version.Status,
		keyDefinition:     t.Version.Definition,
		keyRequestID:      reqIDPlaceholder,
		keyStatus:         http.StatusOK,
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

	t, err := h.Backend.UpdateTemplate(
		accountID,
		templateID,
		strField(body, keyName),
		sourceEntityArnFromBody(body),
		mapField(body, keyDefinition),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyTemplateID:      t.TemplateID,
		keyArn:             t.Arn,
		keyVersionArn:      t.Version.Arn,
		keyCreationStatus2: t.Version.Status,
		keyRequestID:       reqIDPlaceholder,
		keyStatus:          http.StatusOK,
	})
}

func (h *Handler) handleDeleteTemplate(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	templateID := seg(segs, segResID)

	if err := h.Backend.DeleteTemplate(accountID, templateID, versionNumberParam(c)); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyTemplateID: templateID,
		keyRequestID:  reqIDPlaceholder,
		keyStatus:     http.StatusOK,
	})
}

func (h *Handler) handleListTemplates(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	templates, next, err := h.Backend.ListTemplates(accountID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(templates))
	for _, t := range templates {
		items = append(items, map[string]any{
			keyArn:                 t.Arn,
			keyTemplateID:          t.TemplateID,
			keyName:                t.Name,
			keyCreatedTime:         t.CreatedTime.Unix(),
			keyLastUpdatedTime:     t.LastUpdatedTime.Unix(),
			keyLatestVersionNumber: t.Version.VersionNumber,
		})
	}

	resp := map[string]any{
		keyTemplateSummaryList: items,
		keyRequestID:           reqIDPlaceholder,
		keyStatus:              http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleListTemplateVersions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	templateID := seg(segs, segResID)

	versions, next, err := h.Backend.ListTemplateVersions(
		accountID,
		templateID,
		maxResultsParam(c),
		nextTokenParam(c),
	)
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(versions))
	for _, v := range versions {
		items = append(items, templateVersionToMap(v))
	}

	resp := map[string]any{
		keyTemplateVersionSummary: items,
		keyRequestID:              reqIDPlaceholder,
		keyStatus:                 http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleDescribeTemplatePermissions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	templateID := seg(segs, segResID)

	t, perms, err := h.Backend.DescribeTemplatePermissions(accountID, templateID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyTemplateID:  templateID,
		keyTemplateArn: t.Arn,
		keyPermissions: permissionsToMaps(perms),
		keyRequestID:   reqIDPlaceholder,
		keyStatus:      http.StatusOK,
	})
}

func (h *Handler) handleUpdateTemplatePermissions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	templateID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	t, perms, err := h.Backend.UpdateTemplatePermissions(
		accountID,
		templateID,
		permissionsField(body, "GrantPermissions"),
		permissionsField(body, "RevokePermissions"),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyTemplateID:  templateID,
		keyTemplateArn: t.Arn,
		keyPermissions: permissionsToMaps(perms),
		keyRequestID:   reqIDPlaceholder,
		keyStatus:      http.StatusOK,
	})
}

func (h *Handler) handleCreateTemplateAlias(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	templateID := seg(segs, segResID)
	aliasName := seg(segs, segSubResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	alias, err := h.Backend.CreateTemplateAlias(
		accountID, templateID, aliasName, int64Field(body, "TemplateVersionNumber"),
	)
	if err != nil {
		if errors.Is(err, ErrTemplateAliasAlreadyExists) {
			return writeError(c, http.StatusConflict, errResourceExistsCode, err.Error())
		}

		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyTemplateAlias: templateAliasToMap(alias),
		keyRequestID:     reqIDPlaceholder,
		keyStatus:        http.StatusOK,
	})
}

func (h *Handler) handleDescribeTemplateAlias(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	templateID := seg(segs, segResID)
	aliasName := seg(segs, segSubResID)

	alias, err := h.Backend.DescribeTemplateAlias(accountID, templateID, aliasName)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyTemplateAlias: templateAliasToMap(alias),
		keyRequestID:     reqIDPlaceholder,
		keyStatus:        http.StatusOK,
	})
}

func (h *Handler) handleUpdateTemplateAlias(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	templateID := seg(segs, segResID)
	aliasName := seg(segs, segSubResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	alias, err := h.Backend.UpdateTemplateAlias(
		accountID, templateID, aliasName, int64Field(body, "TemplateVersionNumber"),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyTemplateAlias: templateAliasToMap(alias),
		keyRequestID:     reqIDPlaceholder,
		keyStatus:        http.StatusOK,
	})
}

func (h *Handler) handleDeleteTemplateAlias(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	templateID := seg(segs, segResID)
	aliasName := seg(segs, segSubResID)

	if err := h.Backend.DeleteTemplateAlias(accountID, templateID, aliasName); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyTemplateID: templateID,
		keyAliasName:  aliasName,
		keyRequestID:  reqIDPlaceholder,
		keyStatus:     http.StatusOK,
	})
}

func (h *Handler) handleListTemplateAliases(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	templateID := seg(segs, segResID)

	aliases, next, err := h.Backend.ListTemplateAliases(accountID, templateID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(aliases))
	for _, a := range aliases {
		items = append(items, templateAliasToMap(a))
	}

	resp := map[string]any{
		keyTemplateAliasList: items,
		keyRequestID:         reqIDPlaceholder,
		keyStatus:            http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

// ---- shared helpers ----

func templateToMap(t *Template) map[string]any {
	return map[string]any{
		keyArn:             t.Arn,
		keyTemplateID:      t.TemplateID,
		keyName:            t.Name,
		keyCreatedTime:     t.CreatedTime.Unix(),
		keyLastUpdatedTime: t.LastUpdatedTime.Unix(),
		keyVersion:         templateVersionToMap(t.Version),
	}
}

func templateVersionToMap(v *TemplateVersion) map[string]any {
	m := map[string]any{
		keyVersionNumber: v.VersionNumber,
		keyStatus:        v.Status,
		keyArn:           v.Arn,
		keyCreatedTime:   v.CreatedTime.Unix(),
	}
	if v.SourceEntityArn != "" {
		m[keySourceEntityArn] = v.SourceEntityArn
	}
	if v.Description != "" {
		m[keyDescription] = v.Description
	}
	if v.Definition != nil {
		m[keyDefinition] = v.Definition
	}

	return m
}

func templateAliasToMap(a *TemplateAlias) map[string]any {
	return map[string]any{
		keyAliasName:            a.AliasName,
		keyArn:                  a.Arn,
		"TemplateVersionNumber": a.TemplateVersionNumber,
	}
}

func sourceEntityArnFromBody(body map[string]any) string {
	se, _ := body["SourceEntity"].(map[string]any)
	if se == nil {
		return ""
	}
	if st, ok := se["SourceTemplate"].(map[string]any); ok {
		return strField(st, keyArn)
	}
	if sa, ok := se["SourceAnalysis"].(map[string]any); ok {
		return strField(sa, keyArn)
	}

	return ""
}

func mapField(body map[string]any, key string) map[string]any {
	m, _ := body[key].(map[string]any)

	return m
}

func int64Field(body map[string]any, key string) int64 {
	switch v := body[key].(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	}

	return 0
}

func versionNumberParam(c *echo.Context) int64 {
	s := queryParam(c, queryParamVersionNumber)
	if s == "" {
		return 0
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}

	return n
}

// classifyTemplatePaths routes /accounts/{id}/templates/... paths.
func classifyTemplatePaths( //nolint:gocognit,cyclop // existing issue.
	method string,
	segs []string,
	n int,
) (string, string) {
	accountID := seg(segs, segAccountID)
	switch n {
	case nSegsAccountRes:
		if method == http.MethodGet {
			return opListTemplates, accountID
		}
	case nSegsAccountResID:
		id := seg(segs, segResID)
		switch method {
		case http.MethodPost:
			return opCreateTemplate, id
		case http.MethodGet:
			return opDescribeTemplate, id
		case http.MethodPut:
			return opUpdateTemplate, id
		case http.MethodDelete:
			return opDeleteTemplate, id
		}
	case nSegsSubRes:
		id := seg(segs, segResID)
		sub := seg(segs, segSubRes)
		switch sub {
		case pathSegDefinition:
			if method == http.MethodGet {
				return opDescribeTemplateDefinition, id
			}
		case pathSegPermissions:
			switch method {
			case http.MethodGet:
				return opDescribeTemplatePerms, id
			case http.MethodPut:
				return opUpdateTemplatePerms, id
			}
		case pathSegAliases:
			if method == http.MethodGet {
				return opListTemplateAliases, id
			}
		case pathSegVersions:
			if method == http.MethodGet {
				return opListTemplateVersions, id
			}
		}
	case nSegsSubResID:
		id := seg(segs, segResID)
		sub := seg(segs, segSubRes)
		alias := seg(segs, segSubResID)
		if sub == pathSegAliases {
			switch method {
			case http.MethodPost:
				return opCreateTemplateAlias, alias
			case http.MethodGet:
				return opDescribeTemplateAlias, alias
			case http.MethodPut:
				return opUpdateTemplateAlias, alias
			case http.MethodDelete:
				return opDeleteTemplateAlias, id
			}
		}
	}

	return opUnknown, ""
}
