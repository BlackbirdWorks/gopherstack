package iot

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// ---------------------------------------------------------------------------
// Route resolvers for batch 3 ops
// ---------------------------------------------------------------------------

func resolveOTAUpdateOps(path, method string) string {
	switch {
	case path == "/otaUpdates" && method == http.MethodGet:

		return opListOTAUpdates
	case strings.HasPrefix(path, "/otaUpdates/") && method == http.MethodPost:

		return opCreateOTAUpdate
	case strings.HasPrefix(path, "/otaUpdates/") && method == http.MethodGet:

		return opGetOTAUpdate
	case strings.HasPrefix(path, "/otaUpdates/") && method == http.MethodDelete:

		return opDeleteOTAUpdate
	}

	return unknownOperation
}

func resolvePackageOps(path, method string) string {
	switch {
	case path == "/packages" && method == http.MethodGet:

		return opListPackages
	case path == "/package-configuration" && method == http.MethodGet:

		return opGetPackageConfiguration
	case path == "/package-configuration" && method == http.MethodPatch:

		return opUpdatePackageConfiguration
	}

	// /packages/{name}
	if strings.HasPrefix(path, "/packages/") {
		rest := strings.TrimPrefix(path, "/packages/")
		parts := strings.SplitN(rest, "/", 3)
		switch len(parts) {
		case 1:
			switch method {
			case http.MethodPut:

				return opCreatePackage
			case http.MethodGet:

				return opGetPackage
			case http.MethodDelete:

				return opDeletePackage
			case http.MethodPatch:

				return opUpdatePackage
			}
		case 2:
			// /packages/{name}/versions
			if parts[1] == "versions" {
				if method == http.MethodGet {
					return opListPackageVersions
				}
			}
		case 3:
			// /packages/{name}/versions/{versionName}
			if parts[1] == "versions" {
				switch method {
				case http.MethodPut:

					return opCreatePackageVersion
				case http.MethodGet:

					return opGetPackageVersion
				case http.MethodDelete:

					return opDeletePackageVersion
				case http.MethodPatch:

					return opUpdatePackageVersion
				}
			}
		}
	}

	return unknownOperation
}

func resolveAuditSuppressionOps(path, method string) string {
	switch {
	case path == "/audit/suppressions" && method == http.MethodPost:

		return opCreateAuditSuppression
	case path == "/audit/suppressions/describe" && method == http.MethodGet:

		return opDescribeAuditSuppression
	case path == "/audit/suppressions/delete" && method == http.MethodPost:

		return opDeleteAuditSuppression
	case path == "/audit/suppressions/update" && method == http.MethodPatch:

		return opUpdateAuditSuppression
	case path == "/audit/suppressions/list" && method == http.MethodGet:

		return opListAuditSuppressions
	case strings.HasPrefix(path, "/audit/findings/") && method == http.MethodGet:

		return opDescribeAuditFinding
	case path == "/audit/findings" && method == http.MethodGet:

		return opListAuditFindings
	}

	return unknownOperation
}

func resolveV2LoggingOps(path, method string) string {
	switch {
	case path == "/v2LoggingOptions" && method == http.MethodGet:

		return opGetV2LoggingOptions
	case path == "/v2LoggingOptions" && method == http.MethodPost:

		return opSetV2LoggingOptions
	case path == "/v2LoggingLevel" && method == http.MethodPost:

		return opSetV2LoggingLevel
	case path == "/v2LoggingLevel" && method == http.MethodDelete:

		return opDeleteV2LoggingLevel
	case path == "/v2LoggingLevel" && method == http.MethodGet:

		return opListV2LoggingLevels
	case path == "/loggingOptions" && method == http.MethodGet:

		return opGetLoggingOptions
	case path == "/loggingOptions" && method == http.MethodPost:

		return opSetLoggingOptions
	}

	return unknownOperation
}

func resolveCommandOps(path, method string) string {
	switch {
	case path == "/commands" && method == http.MethodGet:

		return opListCommands
	}
	if strings.HasPrefix(path, "/commands/") {
		rest := strings.TrimPrefix(path, "/commands/")
		parts := strings.SplitN(rest, "/", 3)
		switch len(parts) {
		case 1:
			switch method {
			case http.MethodPut:

				return opCreateCommand
			case http.MethodGet:

				return opGetCommand
			case http.MethodDelete:

				return opDeleteCommand
			case http.MethodPatch:

				return opUpdateCommand
			}
		case 2:
			if parts[1] == "executions" && method == http.MethodGet {
				return opListCommandExecutions
			}
		case 3:
			if parts[1] == "executions" {
				if method == http.MethodGet {
					return opGetCommandExecution
				}
			}
		}
	}

	return unknownOperation
}

func resolveBatch3MiscOps(path, method string) string {
	switch {
	case strings.HasPrefix(path, "/provisioning-templates/") &&
		strings.HasSuffix(path, "/provisioning-claim") &&
		method == http.MethodPost:

		return opCreateProvisioningClaim
	case path == "/keys-and-certificate" && method == http.MethodPost:

		return opCreateKeysAndCertificate
	case strings.HasPrefix(path, "/certificates/") &&
		strings.HasSuffix(path, "/transfer") &&
		method == http.MethodPatch:

		return opTransferCertificate
	case strings.HasPrefix(path, "/reject-certificate-transfer/") && method == http.MethodPatch:

		return opRejectCertificateTransfer
	case path == "/event-configurations" && method == http.MethodGet:

		return opDescribeEventConfigurations
	case path == "/event-configurations" && method == http.MethodPatch:

		return opUpdateEventConfigurations
	case strings.HasPrefix(path, "/dynamic-thing-groups/") && method == http.MethodPost:

		return opCreateDynamicThingGroup
	case strings.HasPrefix(path, "/dynamic-thing-groups/") && method == http.MethodDelete:

		return opDeleteDynamicThingGroup
	case strings.HasPrefix(path, "/dynamic-thing-groups/") && method == http.MethodPatch:

		return opUpdateDynamicThingGroup
	}

	return unknownOperation
}

// resolveBatch3Op tries all batch-3 route resolvers and returns the op name.
func resolveBatch3Op(path, method string) string {
	if op := resolveOTAUpdateOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolvePackageOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolveAuditSuppressionOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolveV2LoggingOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolveCommandOps(path, method); op != unknownOperation {
		return op
	}

	return resolveBatch3MiscOps(path, method)
}

// ---------------------------------------------------------------------------
// Handler implementations
// ---------------------------------------------------------------------------

// --- OTA Updates ---

func (h *Handler) handleCreateOTAUpdate(c *echo.Context) error {
	id := strings.TrimPrefix(c.Request().URL.Path, "/otaUpdates/")
	var req struct {
		Description string   `json:"description"`
		RoleARN     string   `json:"roleArn"`
		Targets     []string `json:"targets"`
		Files       []any    `json:"otaUpdateFiles"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	o, err := h.Backend.CreateOTAUpdate(id, req.Description, req.RoleARN, req.Targets, req.Files)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"otaUpdateId":     o.OTAUpdateID,
		"otaUpdateArn":    o.OTAUpdateARN,
		"otaUpdateStatus": o.Status,
	})
}

func (h *Handler) handleGetOTAUpdate(c *echo.Context) error {
	id := strings.TrimPrefix(c.Request().URL.Path, "/otaUpdates/")
	o, err := h.Backend.GetOTAUpdate(id)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"otaUpdateInfo": o})
}

func (h *Handler) handleDeleteOTAUpdate(c *echo.Context) error {
	id := strings.TrimPrefix(c.Request().URL.Path, "/otaUpdates/")
	if err := h.Backend.DeleteOTAUpdate(id); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListOTAUpdates(c *echo.Context) error {
	items := h.Backend.ListOTAUpdates()
	summaries := make([]map[string]any, len(items))
	for i, o := range items {
		summaries[i] = map[string]any{
			"otaUpdateId":  o.OTAUpdateID,
			"otaUpdateArn": o.OTAUpdateARN,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{"otaUpdates": summaries})
}

// --- Packages ---

func (h *Handler) handleCreatePackage(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/packages/")
	var req struct {
		Tags        map[string]string `json:"tags"`
		Description string            `json:"description"`
	}
	_ = readBody(c, &req)
	p, err := h.Backend.CreateIoTPackage(name, req.Description, req.Tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, p)
}

func (h *Handler) handleGetPackage(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/packages/")
	p, err := h.Backend.GetIoTPackage(name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, p)
}

func (h *Handler) handleUpdatePackage(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/packages/")
	var req struct {
		Description        string `json:"description"`
		DefaultVersionName string `json:"defaultVersionName"`
	}
	_ = readBody(c, &req)
	if err := h.Backend.UpdateIoTPackage(name, req.Description, req.DefaultVersionName); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeletePackage(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/packages/")
	if err := h.Backend.DeleteIoTPackage(name); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListPackages(c *echo.Context) error {
	items := h.Backend.ListIoTPackages()

	return c.JSON(http.StatusOK, map[string]any{"packageList": items})
}

// --- Package Versions ---

func packageAndVersion(path string) (packageName, versionName string) {
	// /packages/{name}/versions/{versionName}
	trimmed := strings.TrimPrefix(path, "/packages/")
	parts := strings.SplitN(trimmed, "/versions/", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}

	return parts[0], ""
}

func (h *Handler) handleCreatePackageVersion(c *echo.Context) error {
	pkgName, versionName := packageAndVersion(c.Request().URL.Path)
	var req struct {
		Tags        map[string]string `json:"tags"`
		Description string            `json:"description"`
	}
	_ = readBody(c, &req)
	v, err := h.Backend.CreateIoTPackageVersion(pkgName, versionName, req.Description, req.Tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, v)
}

func (h *Handler) handleGetPackageVersion(c *echo.Context) error {
	pkgName, versionName := packageAndVersion(c.Request().URL.Path)
	v, err := h.Backend.GetIoTPackageVersion(pkgName, versionName)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, v)
}

func (h *Handler) handleUpdatePackageVersion(c *echo.Context) error {
	pkgName, versionName := packageAndVersion(c.Request().URL.Path)
	var req struct {
		Description string `json:"description"`
		Status      string `json:"status"`
	}
	_ = readBody(c, &req)
	if err := h.Backend.UpdateIoTPackageVersion(pkgName, versionName, req.Description, req.Status); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeletePackageVersion(c *echo.Context) error {
	pkgName, versionName := packageAndVersion(c.Request().URL.Path)
	if err := h.Backend.DeleteIoTPackageVersion(pkgName, versionName); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListPackageVersions(c *echo.Context) error {
	// /packages/{name}/versions
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/packages/")
	pkgName := strings.TrimSuffix(trimmed, "/versions")
	items := h.Backend.ListIoTPackageVersions(pkgName)

	return c.JSON(http.StatusOK, map[string]any{"packageVersionList": items})
}

// --- Package Configuration ---

func (h *Handler) handleGetPackageConfiguration(c *echo.Context) error {
	cfg := h.Backend.GetPackageConfiguration()

	return c.JSON(http.StatusOK, cfg)
}

func (h *Handler) handleUpdatePackageConfiguration(c *echo.Context) error {
	var req struct {
		VersionUpdateByJobsConfig map[string]any `json:"versionUpdateByJobsConfig"`
	}
	_ = readBody(c, &req)
	if err := h.Backend.UpdatePackageConfiguration(req.VersionUpdateByJobsConfig); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- Audit Suppressions ---

func (h *Handler) handleCreateAuditSuppression(c *echo.Context) error {
	var req struct {
		ResourceIdentifier   map[string]any `json:"resourceIdentifier"`
		CheckName            string         `json:"checkName"`
		Description          string         `json:"description"`
		SuppressIndefinitely bool           `json:"suppressIndefinitely"`
		ExpirationDate       float64        `json:"expirationDate"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	if err := h.Backend.CreateAuditSuppression(req.CheckName, req.ResourceIdentifier, req.Description, req.SuppressIndefinitely, req.ExpirationDate); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDescribeAuditSuppression(c *echo.Context) error {
	var req struct {
		ResourceIdentifier map[string]any `json:"resourceIdentifier"`
		CheckName          string         `json:"checkName"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	s, err := h.Backend.DescribeAuditSuppression(req.CheckName, req.ResourceIdentifier)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, s)
}

func (h *Handler) handleUpdateAuditSuppression(c *echo.Context) error {
	var req struct {
		ResourceIdentifier   map[string]any `json:"resourceIdentifier"`
		CheckName            string         `json:"checkName"`
		Description          string         `json:"description"`
		SuppressIndefinitely bool           `json:"suppressIndefinitely"`
		ExpirationDate       float64        `json:"expirationDate"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	if err := h.Backend.UpdateAuditSuppression(req.CheckName, req.ResourceIdentifier, req.Description, req.SuppressIndefinitely, req.ExpirationDate); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteAuditSuppression(c *echo.Context) error {
	var req struct {
		ResourceIdentifier map[string]any `json:"resourceIdentifier"`
		CheckName          string         `json:"checkName"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	if err := h.Backend.DeleteAuditSuppression(req.CheckName, req.ResourceIdentifier); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListAuditSuppressions(c *echo.Context) error {
	items := h.Backend.ListAuditSuppressions()

	return c.JSON(http.StatusOK, map[string]any{"suppressions": items})
}

// --- Audit Findings ---

func (h *Handler) handleDescribeAuditFinding(c *echo.Context) error {
	findingID := strings.TrimPrefix(c.Request().URL.Path, "/audit/findings/")
	f, err := h.Backend.DescribeAuditFinding(findingID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"finding": f})
}

func (h *Handler) handleListAuditFindings(c *echo.Context) error {
	items := h.Backend.ListAuditFindings()

	return c.JSON(http.StatusOK, map[string]any{"findings": items})
}

// --- V2 Logging ---

func (h *Handler) handleGetV2LoggingOptions(c *echo.Context) error {
	opts := h.Backend.GetV2LoggingOptions()

	return c.JSON(http.StatusOK, opts)
}

func (h *Handler) handleSetV2LoggingOptions(c *echo.Context) error {
	var req struct {
		RoleARN         string `json:"roleArn"`
		DefaultLogLevel string `json:"defaultLogLevel"`
		DisableAllLogs  bool   `json:"disableAllLogs"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	if err := h.Backend.SetV2LoggingOptions(req.RoleARN, req.DefaultLogLevel, req.DisableAllLogs); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleSetV2LoggingLevel(c *echo.Context) error {
	var req struct {
		LogTarget map[string]any `json:"logTarget"`
		LogLevel  string         `json:"logLevel"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	if err := h.Backend.SetV2LoggingLevel(req.LogTarget, req.LogLevel); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteV2LoggingLevel(c *echo.Context) error {
	targetType := c.Request().URL.Query().Get("targetType")
	targetName := c.Request().URL.Query().Get("targetName")
	target := map[string]any{"targetType": targetType, "targetName": targetName}
	if err := h.Backend.DeleteV2LoggingLevel(target); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListV2LoggingLevels(c *echo.Context) error {
	items := h.Backend.ListV2LoggingLevels()

	return c.JSON(http.StatusOK, map[string]any{"logTargetConfigurations": items})
}

// --- Logging Options ---

func (h *Handler) handleGetLoggingOptions(c *echo.Context) error {
	opts := h.Backend.GetLoggingOptions()

	return c.JSON(http.StatusOK, opts)
}

func (h *Handler) handleSetLoggingOptions(c *echo.Context) error {
	var req struct {
		RoleARN  string `json:"roleArn"`
		LogLevel string `json:"logLevel"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	if err := h.Backend.SetLoggingOptions(req.RoleARN, req.LogLevel); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- Provisioning Claim ---

func (h *Handler) handleCreateProvisioningClaim(c *echo.Context) error {
	// /provisioning-templates/{templateName}/provisioning-claim
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/provisioning-templates/")
	templateName := strings.TrimSuffix(trimmed, "/provisioning-claim")
	certPEM, publicKey, privateKey, err := h.Backend.CreateProvisioningClaim(templateName)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"certificatePem": certPEM,
		"keyPair": map[string]string{
			"PublicKey":  publicKey,
			"PrivateKey": privateKey,
		},
	})
}

// --- Keys and Certificate ---

func (h *Handler) handleCreateKeysAndCertificate(c *echo.Context) error {
	setAsActive := c.Request().URL.Query().Get("setAsActive") == "true"
	cert, publicKey, privateKey, err := h.Backend.CreateKeysAndCertificate(setAsActive)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"certificateArn": cert.ARN,
		"certificateId":  cert.CertificateID,
		"certificatePem": cert.PEM,
		"keyPair": map[string]string{
			"PublicKey":  publicKey,
			"PrivateKey": privateKey,
		},
	})
}

// --- Transfer / Reject Certificate ---

func (h *Handler) handleTransferCertificate(c *echo.Context) error {
	// PATCH /certificates/{certId}/transfer?targetAwsAccount=...
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/certificates/")
	certID := strings.TrimSuffix(trimmed, "/transfer")
	targetAccount := c.Request().URL.Query().Get("targetAwsAccount")
	if err := h.Backend.TransferCertificate(certID, targetAccount); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"transferredCertificateArn": certID})
}

func (h *Handler) handleRejectCertificateTransfer(c *echo.Context) error {
	// PATCH /reject-certificate-transfer/{certId}
	certID := strings.TrimPrefix(c.Request().URL.Path, "/reject-certificate-transfer/")
	if err := h.Backend.RejectCertificateTransfer(certID); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- Event Configurations ---

func (h *Handler) handleDescribeEventConfigurations(c *echo.Context) error {
	cfg := h.Backend.DescribeEventConfigurations()

	return c.JSON(http.StatusOK, cfg)
}

func (h *Handler) handleUpdateEventConfigurations(c *echo.Context) error {
	var req struct {
		EventConfigurations map[string]*EventConfigEntry `json:"eventConfigurations"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	if err := h.Backend.UpdateEventConfigurations(req.EventConfigurations); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- Security Profile Targets ---

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
		summaries[i] = map[string]any{"securityProfileIdentifier": map[string]string{"name": p}}
	}

	return c.JSON(http.StatusOK, map[string]any{"securityProfileTargetMappings": summaries})
}

// --- Dynamic Thing Groups ---

func (h *Handler) handleCreateDynamicThingGroup(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/dynamic-thing-groups/")
	var req struct {
		QueryString string `json:"queryString"`
		Description string `json:"description"`
	}
	_ = readBody(c, &req)
	tg, err := h.Backend.CreateDynamicThingGroup(&CreateThingGroupInput{
		ThingGroupName: name,
		Description:    req.Description,
		QueryString:    req.QueryString,
	})
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"thingGroupName": tg.ThingGroupName,
		"thingGroupArn":  tg.ThingGroupARN,
		"thingGroupId":   tg.ThingGroupID,
		"version":        tg.Version,
	})
}

func (h *Handler) handleDeleteDynamicThingGroup(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/dynamic-thing-groups/")
	if err := h.Backend.DeleteDynamicThingGroup(name); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleUpdateDynamicThingGroup(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/dynamic-thing-groups/")
	var req struct {
		QueryString string `json:"queryString"`
		Description string `json:"description"`
	}
	_ = readBody(c, &req)
	version, err := h.Backend.UpdateDynamicThingGroup(&UpdateThingGroupInput{
		ThingGroupName: name,
		Description:    req.Description,
		QueryString:    req.QueryString,
	})
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"version": version})
}

// --- Commands ---

func (h *Handler) handleCreateCommand(c *echo.Context) error {
	id := strings.TrimPrefix(c.Request().URL.Path, "/commands/")
	var req struct {
		Tags        map[string]string `json:"tags"`
		Payload     map[string]any    `json:"payload"`
		DisplayName string            `json:"displayName"`
		Description string            `json:"description"`
		Namespace   string            `json:"namespace"`
	}
	_ = readBody(c, &req)
	cmd, err := h.Backend.CreateCommand(id, req.DisplayName, req.Description, req.Namespace, req.Payload, req.Tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"commandId":  cmd.CommandID,
		"commandArn": cmd.CommandARN,
	})
}

func (h *Handler) handleGetCommand(c *echo.Context) error {
	id := strings.TrimPrefix(c.Request().URL.Path, "/commands/")
	cmd, err := h.Backend.GetCommand(id)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, cmd)
}

func (h *Handler) handleUpdateCommand(c *echo.Context) error {
	id := strings.TrimPrefix(c.Request().URL.Path, "/commands/")
	var req struct {
		DisplayName string `json:"displayName"`
		Description string `json:"description"`
		Deprecated  bool   `json:"deprecated"`
	}
	_ = readBody(c, &req)
	if err := h.Backend.UpdateCommand(id, req.DisplayName, req.Description, req.Deprecated); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteCommand(c *echo.Context) error {
	id := strings.TrimPrefix(c.Request().URL.Path, "/commands/")
	if err := h.Backend.DeleteCommand(id); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListCommands(c *echo.Context) error {
	items := h.Backend.ListCommands()

	return c.JSON(http.StatusOK, map[string]any{"commands": items})
}

func (h *Handler) handleGetCommandExecution(c *echo.Context) error {
	// /commands/{commandId}/executions/{executionId}
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/commands/")
	parts := strings.SplitN(trimmed, "/executions/", 2)
	if len(parts) != 2 {
		return respondNotFound(c, "command execution not found")
	}
	ex, err := h.Backend.GetCommandExecution(parts[0], parts[1])
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, ex)
}

func (h *Handler) handleListCommandExecutions(c *echo.Context) error {
	// /commands/{commandId}/executions
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/commands/")
	commandID := strings.TrimSuffix(trimmed, "/executions")
	items := h.Backend.ListCommandExecutions(commandID)

	return c.JSON(http.StatusOK, map[string]any{"executions": items})
}

// ---------------------------------------------------------------------------
// dispatchBatch3Ops dispatches batch-3 operations.
// ---------------------------------------------------------------------------

//nolint:funlen // mechanical routing switch
func (h *Handler) dispatchBatch3Ops(c *echo.Context, op string) (bool, error) {
	switch op {
	// OTA Updates
	case opCreateOTAUpdate:

		return true, h.handleCreateOTAUpdate(c)
	case opGetOTAUpdate:

		return true, h.handleGetOTAUpdate(c)
	case opDeleteOTAUpdate:

		return true, h.handleDeleteOTAUpdate(c)
	case opListOTAUpdates:

		return true, h.handleListOTAUpdates(c)
	// Packages
	case opCreatePackage:

		return true, h.handleCreatePackage(c)
	case opGetPackage:

		return true, h.handleGetPackage(c)
	case opUpdatePackage:

		return true, h.handleUpdatePackage(c)
	case opDeletePackage:

		return true, h.handleDeletePackage(c)
	case opListPackages:

		return true, h.handleListPackages(c)
	// Package Versions
	case opCreatePackageVersion:

		return true, h.handleCreatePackageVersion(c)
	case opGetPackageVersion:

		return true, h.handleGetPackageVersion(c)
	case opUpdatePackageVersion:

		return true, h.handleUpdatePackageVersion(c)
	case opDeletePackageVersion:

		return true, h.handleDeletePackageVersion(c)
	case opListPackageVersions:

		return true, h.handleListPackageVersions(c)
	// Package Configuration
	case opGetPackageConfiguration:

		return true, h.handleGetPackageConfiguration(c)
	case opUpdatePackageConfiguration:

		return true, h.handleUpdatePackageConfiguration(c)
	// Audit Suppressions
	case opCreateAuditSuppression:

		return true, h.handleCreateAuditSuppression(c)
	case opDescribeAuditSuppression:

		return true, h.handleDescribeAuditSuppression(c)
	case opUpdateAuditSuppression:

		return true, h.handleUpdateAuditSuppression(c)
	case opDeleteAuditSuppression:

		return true, h.handleDeleteAuditSuppression(c)
	case opListAuditSuppressions:

		return true, h.handleListAuditSuppressions(c)
	// Audit Findings
	case opDescribeAuditFinding:

		return true, h.handleDescribeAuditFinding(c)
	case opListAuditFindings:

		return true, h.handleListAuditFindings(c)
	// V2 Logging
	case opGetV2LoggingOptions:

		return true, h.handleGetV2LoggingOptions(c)
	case opSetV2LoggingOptions:

		return true, h.handleSetV2LoggingOptions(c)
	case opSetV2LoggingLevel:

		return true, h.handleSetV2LoggingLevel(c)
	case opDeleteV2LoggingLevel:

		return true, h.handleDeleteV2LoggingLevel(c)
	case opListV2LoggingLevels:

		return true, h.handleListV2LoggingLevels(c)
	// Logging Options
	case opGetLoggingOptions:

		return true, h.handleGetLoggingOptions(c)
	case opSetLoggingOptions:

		return true, h.handleSetLoggingOptions(c)
	// Provisioning Claim
	case opCreateProvisioningClaim:

		return true, h.handleCreateProvisioningClaim(c)
	// Keys and Certificate
	case opCreateKeysAndCertificate:

		return true, h.handleCreateKeysAndCertificate(c)
	// Transfer / Reject
	case opTransferCertificate:

		return true, h.handleTransferCertificate(c)
	case opRejectCertificateTransfer:

		return true, h.handleRejectCertificateTransfer(c)
	// Event Configurations
	case opDescribeEventConfigurations:

		return true, h.handleDescribeEventConfigurations(c)
	case opUpdateEventConfigurations:

		return true, h.handleUpdateEventConfigurations(c)
	// Security Profile Targets
	case opDetachSecurityProfile:

		return true, h.handleDetachSecurityProfile(c)
	case opListTargetsForSecurityProfile:

		return true, h.handleListTargetsForSecurityProfile(c)
	case opListSecurityProfilesForTarget:

		return true, h.handleListSecurityProfilesForTarget(c)
	// Dynamic Thing Groups
	case opCreateDynamicThingGroup:

		return true, h.handleCreateDynamicThingGroup(c)
	case opDeleteDynamicThingGroup:

		return true, h.handleDeleteDynamicThingGroup(c)
	case opUpdateDynamicThingGroup:

		return true, h.handleUpdateDynamicThingGroup(c)
	// Commands
	case opCreateCommand:

		return true, h.handleCreateCommand(c)
	case opGetCommand:

		return true, h.handleGetCommand(c)
	case opUpdateCommand:

		return true, h.handleUpdateCommand(c)
	case opDeleteCommand:

		return true, h.handleDeleteCommand(c)
	case opListCommands:

		return true, h.handleListCommands(c)
	case opGetCommandExecution:

		return true, h.handleGetCommandExecution(c)
	case opListCommandExecutions:

		return true, h.handleListCommandExecutions(c)
	}

	return false, nil
}
