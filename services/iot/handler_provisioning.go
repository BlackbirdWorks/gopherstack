package iot

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func resolveRoleAliasOps(path, method string) string {
	switch {
	case path == "/role-aliases" && method == http.MethodGet:
		return opListRoleAliases
	case strings.HasPrefix(path, "/role-aliases/") && method == http.MethodPost:
		return opCreateRoleAlias
	case strings.HasPrefix(path, "/role-aliases/") && method == http.MethodGet:
		return opDescribeRoleAlias
	case strings.HasPrefix(path, "/role-aliases/") && method == http.MethodPut:
		return opUpdateRoleAlias
	case strings.HasPrefix(path, "/role-aliases/") && method == http.MethodDelete:
		return opDeleteRoleAlias
	}

	return unknownOperation
}

func resolveDomainConfigOps(path, method string) string {
	switch {
	case path == "/domainConfigurations" && method == http.MethodGet:
		return opListDomainConfigurations
	case strings.HasPrefix(path, "/domainConfigurations/") && method == http.MethodPost:
		return opCreateDomainConfiguration
	case strings.HasPrefix(path, "/domainConfigurations/") && method == http.MethodGet:
		return opDescribeDomainConfiguration
	case strings.HasPrefix(path, "/domainConfigurations/") && method == http.MethodPut:
		return opUpdateDomainConfiguration
	case strings.HasPrefix(path, "/domainConfigurations/") && method == http.MethodDelete:
		return opDeleteDomainConfiguration
	}

	return unknownOperation
}

func resolveProvisioningTemplateOps(path, method string) string {
	if op := resolveProvisioningTemplateVersionOps(path, method); op != unknownOperation {
		return op
	}

	return resolveProvisioningTemplateCrudOps(path, method)
}

// resolveProvisioningTemplateVersionOps resolves the /provisioning-templates/{name}/versions...
// sub-routes, which must be checked before the generic per-template CRUD routing.
func resolveProvisioningTemplateVersionOps(path, method string) string {
	switch {
	// POST /provisioning-templates/{templateName}/versions → CreateProvisioningTemplateVersion
	case strings.HasPrefix(path, "/provisioning-templates/") &&
		strings.HasSuffix(path, "/versions") &&
		method == http.MethodPost:
		return opCreateProvisioningTemplateVersion
	// GET /provisioning-templates/{templateName}/versions → ListProvisioningTemplateVersions
	case strings.HasPrefix(path, "/provisioning-templates/") &&
		strings.HasSuffix(path, "/versions") &&
		method == http.MethodGet:
		return opListProvisioningTemplateVersions
	// DELETE /provisioning-templates/{templateName}/versions/{versionId} → DeleteProvisioningTemplateVersion
	case strings.HasPrefix(path, "/provisioning-templates/") &&
		strings.Contains(path, "/versions/") &&
		method == http.MethodDelete:
		return opDeleteProvisioningTemplateVersion
	// GET /provisioning-templates/{templateName}/versions/{versionId} → DescribeProvisioningTemplateVersion
	case strings.HasPrefix(path, "/provisioning-templates/") &&
		strings.Contains(path, "/versions/") &&
		!strings.HasSuffix(path, "/versions") &&
		method == http.MethodGet:
		return opDescribeProvisioningTemplateVersion
	}

	return unknownOperation
}

// resolveProvisioningTemplateCrudOps resolves the plain /provisioning-templates and
// /provisioning-templates/{name} CRUD routes.
func resolveProvisioningTemplateCrudOps(path, method string) string {
	switch {
	// POST /provisioning-templates → CreateProvisioningTemplate
	case path == "/provisioning-templates" && method == http.MethodPost:
		return opCreateProvisioningTemplate
	// GET /provisioning-templates → ListProvisioningTemplates
	case path == "/provisioning-templates" && method == http.MethodGet:
		return opListProvisioningTemplates
	// GET /provisioning-templates/{templateName} → DescribeProvisioningTemplate
	case strings.HasPrefix(path, "/provisioning-templates/") && method == http.MethodGet:
		return opDescribeProvisioningTemplate
	// PATCH /provisioning-templates/{templateName} → UpdateProvisioningTemplate
	case strings.HasPrefix(path, "/provisioning-templates/") && method == http.MethodPatch:
		return opUpdateProvisioningTemplate
	// DELETE /provisioning-templates/{templateName} → DeleteProvisioningTemplate
	case strings.HasPrefix(path, "/provisioning-templates/") && method == http.MethodDelete:
		return opDeleteProvisioningTemplate
	}

	return unknownOperation
}

func (h *Handler) handleCreateRoleAlias(c *echo.Context) error {
	alias := strings.TrimPrefix(c.Request().URL.Path, "/role-aliases/")
	var input CreateRoleAliasInput
	if err := readBody(c, &input); err != nil {
		return err
	}
	input.RoleAlias = alias
	ra, err := h.Backend.CreateRoleAlias(&input)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"roleAlias":    ra.RoleAlias,
		"roleAliasArn": ra.RoleAliasARN,
	})
}

func (h *Handler) handleDescribeRoleAlias(c *echo.Context) error {
	alias := strings.TrimPrefix(c.Request().URL.Path, "/role-aliases/")
	ra, err := h.Backend.DescribeRoleAlias(alias)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"roleAliasDescription": ra})
}

func (h *Handler) handleListRoleAliases(c *echo.Context) error {
	aliases := h.Backend.ListRoleAliases()
	names := make([]string, len(aliases))
	for i, ra := range aliases {
		names[i] = ra.RoleAlias
	}

	return c.JSON(http.StatusOK, map[string]any{"roleAliases": names})
}

func (h *Handler) handleUpdateRoleAlias(c *echo.Context) error {
	alias := strings.TrimPrefix(c.Request().URL.Path, "/role-aliases/")
	var req struct {
		RoleARN                   string `json:"roleArn"`
		CredentialDurationSeconds int    `json:"credentialDurationSeconds"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	ra, err := h.Backend.UpdateRoleAlias(alias, req.RoleARN, req.CredentialDurationSeconds)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"roleAlias":    ra.RoleAlias,
		"roleAliasArn": ra.RoleAliasARN,
	})
}

func (h *Handler) handleDeleteRoleAlias(c *echo.Context) error {
	alias := strings.TrimPrefix(c.Request().URL.Path, "/role-aliases/")
	if err := h.Backend.DeleteRoleAlias(alias); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleCreateDomainConfiguration(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/domainConfigurations/")
	var input CreateDomainConfigurationInput
	if err := readBody(c, &input); err != nil {
		return err
	}
	input.DomainConfigurationName = name
	dc, err := h.Backend.CreateDomainConfiguration(&input)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyDomainConfigName: dc.DomainConfigurationName,
		keyDomainConfigARN:  dc.DomainConfigurationARN,
	})
}

func (h *Handler) handleDescribeDomainConfiguration(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/domainConfigurations/")
	dc, err := h.Backend.DescribeDomainConfiguration(name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, dc)
}

func (h *Handler) handleListDomainConfigurations(c *echo.Context) error {
	configs := h.Backend.ListDomainConfigurations()
	summaries := make([]map[string]any, len(configs))
	for i, dc := range configs {
		summaries[i] = map[string]any{
			keyDomainConfigName:         dc.DomainConfigurationName,
			keyDomainConfigARN:          dc.DomainConfigurationARN,
			"domainConfigurationStatus": dc.DomainConfigurationStatus,
			"serviceType":               dc.ServiceType,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{"domainConfigurations": summaries})
}

func (h *Handler) handleUpdateDomainConfiguration(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/domainConfigurations/")
	var req struct {
		DomainConfigurationStatus string `json:"domainConfigurationStatus"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	dc, err := h.Backend.UpdateDomainConfiguration(name, req.DomainConfigurationStatus)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyDomainConfigName: dc.DomainConfigurationName,
		keyDomainConfigARN:  dc.DomainConfigurationARN,
	})
}

func (h *Handler) handleDeleteDomainConfiguration(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/domainConfigurations/")
	if err := h.Backend.DeleteDomainConfiguration(name); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleCreateProvisioningTemplate(c *echo.Context) error {
	var input CreateProvisioningTemplateInput
	if err := readBody(c, &input); err != nil {
		return err
	}
	pt, err := h.Backend.CreateProvisioningTemplate(&input)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"templateArn":   pt.TemplateARN,
		keyTemplateName: pt.TemplateName,
	})
}

func (h *Handler) handleDescribeProvisioningTemplate(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/provisioning-templates/")
	pt, err := h.Backend.DescribeProvisioningTemplate(name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, pt)
}

func (h *Handler) handleListProvisioningTemplates(c *echo.Context) error {
	templates := h.Backend.ListProvisioningTemplates()
	summaries := make([]map[string]any, len(templates))
	for i, pt := range templates {
		summaries[i] = map[string]any{
			"templateArn":      pt.TemplateARN,
			keyTemplateName:    pt.TemplateName,
			keyDescription:     pt.Description,
			"enabled":          pt.Enabled,
			"creationDate":     pt.CreationDate,
			"lastModifiedDate": pt.LastModifiedDate,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{"templates": summaries})
}

func (h *Handler) handleUpdateProvisioningTemplate(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/provisioning-templates/")
	var req struct {
		Enabled             *bool  `json:"enabled"`
		Description         string `json:"description"`
		ProvisioningRoleARN string `json:"provisioningRoleArn"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	if err := h.Backend.UpdateProvisioningTemplate(
		name, req.Description, req.Enabled, req.ProvisioningRoleARN,
	); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteProvisioningTemplate(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/provisioning-templates/")
	if err := h.Backend.DeleteProvisioningTemplate(name); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleCreateProvisioningTemplateVersion(c *echo.Context) error {
	// POST /provisioning-templates/{templateName}/versions
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/provisioning-templates/")
	name := strings.TrimSuffix(trimmed, "/versions")
	var req struct {
		TemplateBody string `json:"templateBody"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	v, err := h.Backend.CreateProvisioningTemplateVersion(name, req.TemplateBody)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyTemplateName: name,
		"versionId":     v.VersionID,
	})
}

func (h *Handler) handleListProvisioningTemplateVersions(c *echo.Context) error {
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/provisioning-templates/")
	name := strings.TrimSuffix(trimmed, "/versions")
	versions, err := h.Backend.ListProvisioningTemplateVersions(name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{pathSegmentVersions: versions})
}

func (h *Handler) handleDeleteProvisioningTemplateVersion(c *echo.Context) error {
	// DELETE /provisioning-templates/{templateName}/versions/{versionId}
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/provisioning-templates/")
	parts := strings.SplitN(trimmed, "/versions/", twoparts)
	if len(parts) != twoparts {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: "invalid path"})
	}
	name := parts[0]
	var versionID int32
	if err := parseInt32(parts[1], &versionID); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: "invalid versionId"})
	}
	if err := h.Backend.DeleteProvisioningTemplateVersion(name, versionID); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDescribeProvisioningTemplateVersion(c *echo.Context) error {
	// GET /provisioning-templates/{templateName}/versions/{versionId}
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/provisioning-templates/")
	parts := strings.SplitN(trimmed, "/versions/", pathSplitTwo)

	if len(parts) != pathSplitTwo {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: keyInvalidPath})
	}

	var versionID int32
	if err := parseInt32(parts[1], &versionID); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: "invalid versionId"})
	}

	v, err := h.Backend.DescribeProvisioningTemplateVersion(parts[0], versionID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, v)
}

func (h *Handler) dispatchRoleAliasOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opCreateRoleAlias:
		return true, h.handleCreateRoleAlias(c)
	case opDescribeRoleAlias:
		return true, h.handleDescribeRoleAlias(c)
	case opListRoleAliases:
		return true, h.handleListRoleAliases(c)
	case opUpdateRoleAlias:
		return true, h.handleUpdateRoleAlias(c)
	case opDeleteRoleAlias:
		return true, h.handleDeleteRoleAlias(c)
	}

	return false, nil
}

func (h *Handler) dispatchDomainConfigOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opCreateDomainConfiguration:
		return true, h.handleCreateDomainConfiguration(c)
	case opDescribeDomainConfiguration:
		return true, h.handleDescribeDomainConfiguration(c)
	case opListDomainConfigurations:
		return true, h.handleListDomainConfigurations(c)
	case opUpdateDomainConfiguration:
		return true, h.handleUpdateDomainConfiguration(c)
	case opDeleteDomainConfiguration:
		return true, h.handleDeleteDomainConfiguration(c)
	}

	return false, nil
}

func (h *Handler) dispatchProvisioningTemplateOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opCreateProvisioningTemplate:
		return true, h.handleCreateProvisioningTemplate(c)
	case opDescribeProvisioningTemplate:
		return true, h.handleDescribeProvisioningTemplate(c)
	case opListProvisioningTemplates:
		return true, h.handleListProvisioningTemplates(c)
	case opUpdateProvisioningTemplate:
		return true, h.handleUpdateProvisioningTemplate(c)
	case opDeleteProvisioningTemplate:
		return true, h.handleDeleteProvisioningTemplate(c)
	case opCreateProvisioningTemplateVersion:
		return true, h.handleCreateProvisioningTemplateVersion(c)
	case opListProvisioningTemplateVersions:
		return true, h.handleListProvisioningTemplateVersions(c)
	case opDeleteProvisioningTemplateVersion:
		return true, h.handleDeleteProvisioningTemplateVersion(c)
	}

	return false, nil
}
