package iot

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleAttachSecurityProfile(c *echo.Context) error {
	// Path: /security-profiles/{securityProfileName}/targets
	after := strings.TrimPrefix(c.Request().URL.Path, "/security-profiles/")
	profileName := strings.SplitN(after, "/", maxPathSegments)[0]
	targetArn := c.QueryParam("securityProfileTargetArn")

	if err := h.Backend.AttachSecurityProfile(&AttachSecurityProfileInput{
		SecurityProfileName:      profileName,
		SecurityProfileTargetArn: targetArn,
	}); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDetachSecurityProfile(c *echo.Context) error {
	// DELETE /security-profiles/{name}/targets?securityProfileTargetArn=...
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/security-profiles/")
	profileName := strings.TrimSuffix(trimmed, "/targets")
	targetARN := c.Request().URL.Query().Get("securityProfileTargetArn")
	if err := h.Backend.DetachSecurityProfile(profileName, targetARN); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListTargetsForSecurityProfile(c *echo.Context) error {
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/security-profiles/")
	profileName := strings.TrimSuffix(trimmed, "/targets")
	targets := h.Backend.ListTargetsForSecurityProfile(profileName)
	summaries := make([]map[string]any, len(targets))
	for i, t := range targets {
		summaries[i] = map[string]any{"securityProfileTargetArn": t}
	}

	return c.JSON(http.StatusOK, map[string]any{"securityProfileTargets": summaries})
}

func (h *Handler) handleListSecurityProfilesForTarget(c *echo.Context) error {
	targetARN := c.Request().URL.Query().Get("securityProfileTargetArn")
	profiles := h.Backend.ListSecurityProfilesForTarget(targetARN)
	summaries := make([]map[string]any, len(profiles))
	for i, p := range profiles {
		summaries[i] = map[string]any{"securityProfileIdentifier": map[string]string{keyName: p}}
	}

	return c.JSON(http.StatusOK, map[string]any{"securityProfileTargetMappings": summaries})
}

// resolveSecurityProfileTargetOps handles /security-profiles/{name}/targets operations.
func resolveSecurityProfileTargetOps(path, method string) string {
	if !strings.HasPrefix(path, "/security-profiles/") || !strings.HasSuffix(path, "/targets") {
		return unknownOperation
	}
	switch method {
	case http.MethodDelete:
		return opDetachSecurityProfile
	case http.MethodGet:
		return opListTargetsForSecurityProfile
	case http.MethodPut:
		return opAttachSecurityProfile
	}

	return unknownOperation
}

func resolveSecurityProfileOps(path, method string) string {
	switch {
	case path == "/security-profiles" && method == http.MethodGet:
		return opListSecurityProfiles
	case path == "/security-profiles-for-target" && method == http.MethodGet:
		return opListSecurityProfilesForTarget
	}
	// /security-profiles/{name}/targets — must be before generic prefix cases
	if op := resolveSecurityProfileTargetOps(path, method); op != unknownOperation {
		return op
	}
	switch {
	case strings.HasPrefix(path, "/security-profiles/") && method == http.MethodPost:
		return opCreateSecurityProfile
	case strings.HasPrefix(path, "/security-profiles/") && method == http.MethodGet:
		return opDescribeSecurityProfile
	case strings.HasPrefix(path, "/security-profiles/") && method == http.MethodPatch:
		return opUpdateSecurityProfile
	case strings.HasPrefix(path, "/security-profiles/") && method == http.MethodDelete:
		return opDeleteSecurityProfile
	}

	return unknownOperation
}

func (h *Handler) handleCreateSecurityProfile(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/security-profiles/")
	var input CreateSecurityProfileInput
	if err := readBody(c, &input); err != nil {
		return err
	}
	input.SecurityProfileName = name
	sp, err := h.Backend.CreateSecurityProfile(&input)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keySecurityProfileName: sp.SecurityProfileName,
		keySecurityProfileARN:  sp.SecurityProfileARN,
	})
}

func (h *Handler) handleDescribeSecurityProfile(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/security-profiles/")
	sp, err := h.Backend.DescribeSecurityProfile(name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, sp)
}

func (h *Handler) handleListSecurityProfiles(c *echo.Context) error {
	profiles := h.Backend.ListSecurityProfiles()
	summaries := make([]map[string]any, len(profiles))
	for i, sp := range profiles {
		summaries[i] = map[string]any{
			keySecurityProfileName: sp.SecurityProfileName,
			keySecurityProfileARN:  sp.SecurityProfileARN,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{"securityProfileIdentifiers": summaries})
}

func (h *Handler) handleUpdateSecurityProfile(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/security-profiles/")
	var req struct {
		SecurityProfileDescription string `json:"securityProfileDescription"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	sp, err := h.Backend.UpdateSecurityProfile(name, req.SecurityProfileDescription)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keySecurityProfileName: sp.SecurityProfileName,
		keySecurityProfileARN:  sp.SecurityProfileARN,
		keyVersion:             sp.Version,
	})
}

func (h *Handler) handleDeleteSecurityProfile(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/security-profiles/")
	if err := h.Backend.DeleteSecurityProfile(name); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleValidateSecurityProfileBehaviors(c *echo.Context) error {
	var req struct {
		Behaviors []SecurityProfileBehavior `json:"behaviors"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}

	valid, validationErrors := h.Backend.ValidateSecurityProfileBehaviors(req.Behaviors)

	errs := make([]map[string]string, len(validationErrors))
	for i, msg := range validationErrors {
		errs[i] = map[string]string{"errorMessage": msg}
	}

	return c.JSON(http.StatusOK, map[string]any{
		"valid":            valid,
		"validationErrors": errs,
	})
}

func (h *Handler) dispatchSecurityProfileOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opCreateSecurityProfile:
		return true, h.handleCreateSecurityProfile(c)
	case opDescribeSecurityProfile:
		return true, h.handleDescribeSecurityProfile(c)
	case opListSecurityProfiles:
		return true, h.handleListSecurityProfiles(c)
	case opUpdateSecurityProfile:
		return true, h.handleUpdateSecurityProfile(c)
	case opDeleteSecurityProfile:
		return true, h.handleDeleteSecurityProfile(c)
	case opDetachSecurityProfile:
		return true, h.handleDetachSecurityProfile(c)
	case opListTargetsForSecurityProfile:
		return true, h.handleListTargetsForSecurityProfile(c)
	case opListSecurityProfilesForTarget:
		return true, h.handleListSecurityProfilesForTarget(c)
	}

	return false, nil
}
