package directoryservice

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	keyRemoteDomainName = "RemoteDomainName"
	keyTopicName        = "TopicName"

	opAcceptSharedDirectory                = "AcceptSharedDirectory"
	opAddIpRoutes                          = "AddIpRoutes" //nolint:revive,staticcheck // existing issue.
	opAddRegion                            = "AddRegion"
	opCancelSchemaExtension                = "CancelSchemaExtension"
	opConnectDirectory                     = "ConnectDirectory"
	opCreateComputer                       = "CreateComputer"
	opCreateConditionalForwarder           = "CreateConditionalForwarder"
	opCreateHybridAD                       = "CreateHybridAD"
	opCreateLogSubscription                = "CreateLogSubscription"
	opCreateTrust                          = "CreateTrust"
	opDeleteADAssessment                   = "DeleteADAssessment"
	opDeleteConditionalForwarder           = "DeleteConditionalForwarder"
	opDeleteLogSubscription                = "DeleteLogSubscription"
	opDeleteTrust                          = "DeleteTrust"
	opDeregisterCertificate                = "DeregisterCertificate"
	opDeregisterEventTopic                 = "DeregisterEventTopic"
	opDescribeADAssessment                 = "DescribeADAssessment"
	opDescribeCAEnrollmentPolicy           = "DescribeCAEnrollmentPolicy"
	opDescribeCertificate                  = "DescribeCertificate"
	opDescribeClientAuthenticationSettings = "DescribeClientAuthenticationSettings"
	opDescribeConditionalForwarders        = "DescribeConditionalForwarders"
	opDescribeDirectoryDataAccess          = "DescribeDirectoryDataAccess"
	opDescribeDomainControllers            = "DescribeDomainControllers"
	opDescribeEventTopics                  = "DescribeEventTopics"
	opDescribeHybridADUpdate               = "DescribeHybridADUpdate"
	opDescribeLDAPSSettings                = "DescribeLDAPSSettings"
	opDescribeRegions                      = "DescribeRegions"
	opDescribeSettings                     = "DescribeSettings"
	opDescribeSharedDirectories            = "DescribeSharedDirectories"
	opDescribeTrusts                       = "DescribeTrusts"
	opDescribeUpdateDirectory              = "DescribeUpdateDirectory"
	opDisableCAEnrollmentPolicy            = "DisableCAEnrollmentPolicy"
	opDisableClientAuthentication          = "DisableClientAuthentication"
	opDisableDirectoryDataAccess           = "DisableDirectoryDataAccess"
	opDisableLDAPS                         = "DisableLDAPS"
	opDisableRadius                        = "DisableRadius"
	opEnableCAEnrollmentPolicy             = "EnableCAEnrollmentPolicy"
	opEnableClientAuthentication           = "EnableClientAuthentication"
	opEnableDirectoryDataAccess            = "EnableDirectoryDataAccess"
	opEnableLDAPS                          = "EnableLDAPS"
	opEnableRadius                         = "EnableRadius"
	opListADAssessments                    = "ListADAssessments"
	opListCertificates                     = "ListCertificates"
	opListIpRoutes                         = "ListIpRoutes" //nolint:revive,staticcheck // existing issue.
	opListLogSubscriptions                 = "ListLogSubscriptions"
	opListSchemaExtensions                 = "ListSchemaExtensions"
	opRegisterCertificate                  = "RegisterCertificate"
	opRegisterEventTopic                   = "RegisterEventTopic"
	opRejectSharedDirectory                = "RejectSharedDirectory"
	opRemoveIpRoutes                       = "RemoveIpRoutes" //nolint:revive,staticcheck // existing issue.
	opRemoveRegion                         = "RemoveRegion"
	opResetUserPassword                    = "ResetUserPassword"
	opShareDirectory                       = "ShareDirectory"
	opStartADAssessment                    = "StartADAssessment"
	opStartSchemaExtension                 = "StartSchemaExtension"
	opUnshareDirectory                     = "UnshareDirectory"
	opUpdateConditionalForwarder           = "UpdateConditionalForwarder"
	opUpdateDirectorySetup                 = "UpdateDirectorySetup"
	opUpdateHybridAD                       = "UpdateHybridAD"
	opUpdateNumberOfDomainControllers      = "UpdateNumberOfDomainControllers"
	opUpdateRadius                         = "UpdateRadius"
	opUpdateSettings                       = "UpdateSettings"
	opUpdateTrust                          = "UpdateTrust"
	opVerifyTrust                          = "VerifyTrust"
)

// appendixAOps returns the Appendix A operation handlers for registration.
func (h *Handler) appendixAOps() map[string]echo.HandlerFunc {
	return map[string]echo.HandlerFunc{
		opAcceptSharedDirectory:                h.handleAcceptSharedDirectory,
		opAddIpRoutes:                          h.handleAddIpRoutes,
		opAddRegion:                            h.handleAddRegion,
		opCancelSchemaExtension:                h.handleCancelSchemaExtension,
		opConnectDirectory:                     h.handleConnectDirectory,
		opCreateComputer:                       h.handleCreateComputer,
		opCreateConditionalForwarder:           h.handleCreateConditionalForwarder,
		opCreateHybridAD:                       h.handleCreateHybridAD,
		opCreateLogSubscription:                h.handleCreateLogSubscription,
		opCreateTrust:                          h.handleCreateTrust,
		opDeleteADAssessment:                   h.handleDeleteADAssessment,
		opDeleteConditionalForwarder:           h.handleDeleteConditionalForwarder,
		opDeleteLogSubscription:                h.handleDeleteLogSubscription,
		opDeleteTrust:                          h.handleDeleteTrust,
		opDeregisterCertificate:                h.handleDeregisterCertificate,
		opDeregisterEventTopic:                 h.handleDeregisterEventTopic,
		opDescribeADAssessment:                 h.handleDescribeADAssessment,
		opDescribeCAEnrollmentPolicy:           h.handleDescribeCAEnrollmentPolicy,
		opDescribeCertificate:                  h.handleDescribeCertificate,
		opDescribeClientAuthenticationSettings: h.handleDescribeClientAuthenticationSettings,
		opDescribeConditionalForwarders:        h.handleDescribeConditionalForwarders,
		opDescribeDirectoryDataAccess:          h.handleDescribeDirectoryDataAccess,
		opDescribeDomainControllers:            h.handleDescribeDomainControllers,
		opDescribeEventTopics:                  h.handleDescribeEventTopics,
		opDescribeHybridADUpdate:               h.handleDescribeHybridADUpdate,
		opDescribeLDAPSSettings:                h.handleDescribeLDAPSSettings,
		opDescribeRegions:                      h.handleDescribeRegions,
		opDescribeSettings:                     h.handleDescribeSettings,
		opDescribeSharedDirectories:            h.handleDescribeSharedDirectories,
		opDescribeTrusts:                       h.handleDescribeTrusts,
		opDescribeUpdateDirectory:              h.handleDescribeUpdateDirectory,
		opDisableCAEnrollmentPolicy:            h.handleDisableCAEnrollmentPolicy,
		opDisableClientAuthentication:          h.handleDisableClientAuthentication,
		opDisableDirectoryDataAccess:           h.handleDisableDirectoryDataAccess,
		opDisableLDAPS:                         h.handleDisableLDAPS,
		opDisableRadius:                        h.handleDisableRadius,
		opEnableCAEnrollmentPolicy:             h.handleEnableCAEnrollmentPolicy,
		opEnableClientAuthentication:           h.handleEnableClientAuthentication,
		opEnableDirectoryDataAccess:            h.handleEnableDirectoryDataAccess,
		opEnableLDAPS:                          h.handleEnableLDAPS,
		opEnableRadius:                         h.handleEnableRadius,
		opListADAssessments:                    h.handleListADAssessments,
		opListCertificates:                     h.handleListCertificates,
		opListIpRoutes:                         h.handleListIpRoutes,
		opListLogSubscriptions:                 h.handleListLogSubscriptions,
		opListSchemaExtensions:                 h.handleListSchemaExtensions,
		opRegisterCertificate:                  h.handleRegisterCertificate,
		opRegisterEventTopic:                   h.handleRegisterEventTopic,
		opRejectSharedDirectory:                h.handleRejectSharedDirectory,
		opRemoveIpRoutes:                       h.handleRemoveIpRoutes,
		opRemoveRegion:                         h.handleRemoveRegion,
		opResetUserPassword:                    h.handleResetUserPassword,
		opShareDirectory:                       h.handleShareDirectory,
		opStartADAssessment:                    h.handleStartADAssessment,
		opStartSchemaExtension:                 h.handleStartSchemaExtension,
		opUnshareDirectory:                     h.handleUnshareDirectory,
		opUpdateConditionalForwarder:           h.handleUpdateConditionalForwarder,
		opUpdateDirectorySetup:                 h.handleUpdateDirectorySetup,
		opUpdateHybridAD:                       h.handleUpdateHybridAD,
		opUpdateNumberOfDomainControllers:      h.handleUpdateNumberOfDomainControllers,
		opUpdateRadius:                         h.handleUpdateRadius,
		opUpdateSettings:                       h.handleUpdateSettings,
		opUpdateTrust:                          h.handleUpdateTrust,
		opVerifyTrust:                          h.handleVerifyTrust,
	}
}

// appendixAOpsNames returns the Appendix A operation names for GetSupportedOperations.
func appendixAOpsNames() []string {
	return []string{
		opAcceptSharedDirectory,
		opAddIpRoutes,
		opAddRegion,
		opCancelSchemaExtension,
		opConnectDirectory,
		opCreateComputer,
		opCreateConditionalForwarder,
		opCreateHybridAD,
		opCreateLogSubscription,
		opCreateTrust,
		opDeleteADAssessment,
		opDeleteConditionalForwarder,
		opDeleteLogSubscription,
		opDeleteTrust,
		opDeregisterCertificate,
		opDeregisterEventTopic,
		opDescribeADAssessment,
		opDescribeCAEnrollmentPolicy,
		opDescribeCertificate,
		opDescribeClientAuthenticationSettings,
		opDescribeConditionalForwarders,
		opDescribeDirectoryDataAccess,
		opDescribeDomainControllers,
		opDescribeEventTopics,
		opDescribeHybridADUpdate,
		opDescribeLDAPSSettings,
		opDescribeRegions,
		opDescribeSettings,
		opDescribeSharedDirectories,
		opDescribeTrusts,
		opDescribeUpdateDirectory,
		opDisableCAEnrollmentPolicy,
		opDisableClientAuthentication,
		opDisableDirectoryDataAccess,
		opDisableLDAPS,
		opDisableRadius,
		opEnableCAEnrollmentPolicy,
		opEnableClientAuthentication,
		opEnableDirectoryDataAccess,
		opEnableLDAPS,
		opEnableRadius,
		opListADAssessments,
		opListCertificates,
		opListIpRoutes,
		opListLogSubscriptions,
		opListSchemaExtensions,
		opRegisterCertificate,
		opRegisterEventTopic,
		opRejectSharedDirectory,
		opRemoveIpRoutes,
		opRemoveRegion,
		opResetUserPassword,
		opShareDirectory,
		opStartADAssessment,
		opStartSchemaExtension,
		opUnshareDirectory,
		opUpdateConditionalForwarder,
		opUpdateDirectorySetup,
		opUpdateHybridAD,
		opUpdateNumberOfDomainControllers,
		opUpdateRadius,
		opUpdateSettings,
		opUpdateTrust,
		opVerifyTrust,
	}
}

// twoFieldOp describes an operation that takes a directory ID plus one secondary
// string field and returns only an error. It centralises the identical request
// parsing, validation and error mapping shared by several Appendix A handlers.
type twoFieldOp struct {
	invoke    func(ctx context.Context, dirID, second string) error
	secondKey string // JSON key (and human label) of the secondary field
}

// handleTwoFieldOp parses {DirectoryId, <secondKey>} from the request body,
// validates both are present, resolves the request region and invokes op.
func (h *Handler) handleTwoFieldOp(c *echo.Context, op twoFieldOp) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var raw map[string]json.RawMessage
	if jsonErr := json.Unmarshal(body, &raw); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	dirID := jsonString(raw, keyDirectoryID)
	second := jsonString(raw, op.secondKey)

	if dirID == "" || second == "" {
		msg := "DirectoryId and " + op.secondKey + " are required"

		return c.JSON(http.StatusBadRequest, errResp("ClientException", msg))
	}

	if opErr := op.invoke(h.contextWithRegion(c), dirID, second); opErr != nil {
		return h.mapError(c, opErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// jsonString returns the string value stored under key in raw, or "" if absent
// or not a JSON string.
func jsonString(raw map[string]json.RawMessage, key string) string {
	v, ok := raw[key]
	if !ok {
		return ""
	}

	var s string
	if err := json.Unmarshal(v, &s); err != nil {
		return ""
	}

	return s
}

// --- IP Routes ---

func (h *Handler) handleAddIpRoutes(c *echo.Context) error { //nolint:revive,staticcheck // existing issue.
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string     `json:"DirectoryId"`
		IpRoutes    []struct { //nolint:revive,staticcheck // existing issue.
			CidrIp      string `json:"CidrIp"` //nolint:revive,staticcheck // existing issue.
			Description string `json:"Description"`
		} `json:"IpRoutes"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	routes := make([]IpRoute, 0, len(req.IpRoutes))
	for _, r := range req.IpRoutes {
		routes = append(routes, IpRoute{CidrIP: r.CidrIp, Description: r.Description})
	}

	if addErr := h.Backend.AddIpRoutes(h.contextWithRegion(c), req.DirectoryID, routes); addErr != nil {
		return h.mapError(c, addErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleRemoveIpRoutes(c *echo.Context) error { //nolint:revive,staticcheck // existing issue.
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string   `json:"DirectoryId"`
		CidrIPs     []string `json:"CidrIps"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	if removeErr := h.Backend.RemoveIpRoutes(h.contextWithRegion(c), req.DirectoryID, req.CidrIPs); removeErr != nil {
		return h.mapError(c, removeErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListIpRoutes(c *echo.Context) error { //nolint:revive,staticcheck // existing issue.
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		NextToken   string `json:"NextToken"`
		Limit       int32  `json:"Limit"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	routes, nextToken, listErr := h.Backend.ListIpRoutes(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.Limit,
		req.NextToken,
	)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	routeList := make([]map[string]any, 0, len(routes))
	for _, r := range routes {
		routeList = append(routeList, map[string]any{
			keyDirectoryID:     r.DirectoryID,
			"CidrIp":           r.CidrIP,
			"Description":      r.Description, //nolint:goconst // existing issue.
			"AddedDateTime":    r.AddedTime.Format("2006-01-02T15:04:05.000Z"),
			"IpRouteStatusMsg": r.Status,
		})
	}

	resp := map[string]any{"IpRoutesInfo": routeList}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

// --- Regions ---

func (h *Handler) handleAddRegion(c *echo.Context) error {
	return h.handleTwoFieldOp(c, twoFieldOp{
		secondKey: "RegionName",
		invoke: func(ctx context.Context, dirID, second string) error {
			return h.Backend.AddRegion(ctx, dirID, second)
		},
	})
}

func (h *Handler) handleRemoveRegion(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	if removeErr := h.Backend.RemoveRegion(h.contextWithRegion(c), req.DirectoryID); removeErr != nil {
		return h.mapError(c, removeErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDescribeRegions(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		RegionName  string `json:"RegionName"`
		NextToken   string `json:"NextToken"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	regions, nextToken, descErr := h.Backend.DescribeRegions(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.RegionName,
		req.NextToken,
	)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	regionList := make([]map[string]any, 0, len(regions))
	for _, r := range regions {
		regionList = append(regionList, map[string]any{
			keyDirectoryID: r.DirectoryID,
			"RegionName":   r.RegionName,
			"RegionType":   r.RegionType,
			keyStatus:      r.Status,
			keyLaunchTime:  r.LaunchTime.Format("2006-01-02T15:04:05.000Z"),
		})
	}

	resp := map[string]any{"RegionsDescription": regionList}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

// --- Schema Extensions ---

func (h *Handler) handleStartSchemaExtension(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID                         string `json:"DirectoryId"`
		Description                         string `json:"Description"`
		LdifContent                         string `json:"LdifContent"`
		CreateSnapshotBeforeSchemaExtension bool   `json:"CreateSnapshotBeforeSchemaExtension"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	id, startErr := h.Backend.StartSchemaExtension(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.Description,
		req.LdifContent,
	)
	if startErr != nil {
		return h.mapError(c, startErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"SchemaExtensionId": id,
	})
}

func (h *Handler) handleCancelSchemaExtension(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID       string `json:"DirectoryId"`
		SchemaExtensionID string `json:"SchemaExtensionId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" || req.SchemaExtensionID == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ClientException", "DirectoryId and SchemaExtensionId are required"),
		)
	}

	cancelErr := h.Backend.CancelSchemaExtension(h.contextWithRegion(c), req.DirectoryID, req.SchemaExtensionID)
	if cancelErr != nil {
		return h.mapError(c, cancelErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListSchemaExtensions(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		NextToken   string `json:"NextToken"`
		Limit       int32  `json:"Limit"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	exts, nextToken, listErr := h.Backend.ListSchemaExtensions(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.Limit,
		req.NextToken,
	)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	extList := make([]map[string]any, 0, len(exts))
	for _, e := range exts {
		extList = append(extList, map[string]any{
			keyDirectoryID:          e.DirectoryID,
			"SchemaExtensionId":     e.ExtensionID,
			"Description":           e.Description,
			"SchemaExtensionStatus": e.Status,
			"StartDateTime":         e.StartTime.Format("2006-01-02T15:04:05.000Z"),
			"EndDateTime":           e.EndTime.Format("2006-01-02T15:04:05.000Z"),
		})
	}

	resp := map[string]any{"SchemaExtensionsInfo": extList}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

// --- Conditional Forwarders ---

func (h *Handler) handleCreateConditionalForwarder(c *echo.Context) error { //nolint:dupl // existing issue.
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID      string   `json:"DirectoryId"`
		RemoteDomainName string   `json:"RemoteDomainName"`
		DNSIpAddrs       []string `json:"DnsIpAddrs"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" || req.RemoteDomainName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ClientException", "DirectoryId and RemoteDomainName are required"),
		)
	}

	if createErr := h.Backend.CreateConditionalForwarder(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.RemoteDomainName,
		req.DNSIpAddrs,
	); createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleUpdateConditionalForwarder(c *echo.Context) error { //nolint:dupl // existing issue.
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID      string   `json:"DirectoryId"`
		RemoteDomainName string   `json:"RemoteDomainName"`
		DNSIpAddrs       []string `json:"DnsIpAddrs"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" || req.RemoteDomainName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ClientException", "DirectoryId and RemoteDomainName are required"),
		)
	}

	if updateErr := h.Backend.UpdateConditionalForwarder(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.RemoteDomainName,
		req.DNSIpAddrs,
	); updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDeleteConditionalForwarder(c *echo.Context) error {
	return h.handleTwoFieldOp(c, twoFieldOp{
		secondKey: keyRemoteDomainName,
		invoke: func(ctx context.Context, dirID, second string) error {
			return h.Backend.DeleteConditionalForwarder(ctx, dirID, second)
		},
	})
}

func (h *Handler) handleDescribeConditionalForwarders(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID       string   `json:"DirectoryId"`
		RemoteDomainNames []string `json:"RemoteDomainNames"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	fwds, descErr := h.Backend.DescribeConditionalForwarders(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.RemoteDomainNames,
	)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	fwdList := make([]map[string]any, 0, len(fwds))
	for _, f := range fwds {
		fwdList = append(fwdList, map[string]any{
			"RemoteDomainName": f.RemoteDomainName,
			"DnsIpAddrs":       f.DNSIPAddrs,
			"ReplicationScope": f.ReplicationScope,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"ConditionalForwarders": fwdList})
}

// --- Log Subscriptions ---

func (h *Handler) handleCreateLogSubscription(c *echo.Context) error {
	return h.handleTwoFieldOp(c, twoFieldOp{
		secondKey: "LogGroupName",
		invoke: func(ctx context.Context, dirID, second string) error {
			return h.Backend.CreateLogSubscription(ctx, dirID, second)
		},
	})
}

func (h *Handler) handleDeleteLogSubscription(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	if delErr := h.Backend.DeleteLogSubscription(h.contextWithRegion(c), req.DirectoryID); delErr != nil {
		return h.mapError(c, delErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListLogSubscriptions(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		NextToken   string `json:"NextToken"`
		Limit       int32  `json:"Limit"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	subs, nextToken, listErr := h.Backend.ListLogSubscriptions(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.Limit,
		req.NextToken,
	)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	subList := make([]map[string]any, 0, len(subs))
	for _, s := range subs {
		subList = append(subList, map[string]any{
			keyDirectoryID:                s.DirectoryID,
			"LogGroupName":                s.LogGroupName,
			"SubscriptionCreatedDateTime": s.CreatedTime.Format("2006-01-02T15:04:05.000Z"),
		})
	}

	resp := map[string]any{"LogSubscriptions": subList}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

// --- Event Topics ---

func (h *Handler) handleRegisterEventTopic(c *echo.Context) error {
	return h.handleTwoFieldOp(c, twoFieldOp{
		secondKey: keyTopicName,
		invoke: func(ctx context.Context, dirID, second string) error {
			return h.Backend.RegisterEventTopic(ctx, dirID, second)
		},
	})
}

func (h *Handler) handleDeregisterEventTopic(c *echo.Context) error {
	return h.handleTwoFieldOp(c, twoFieldOp{
		secondKey: keyTopicName,
		invoke: func(ctx context.Context, dirID, second string) error {
			return h.Backend.DeregisterEventTopic(ctx, dirID, second)
		},
	})
}

func (h *Handler) handleDescribeEventTopics(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string   `json:"DirectoryId"`
		TopicNames  []string `json:"TopicNames"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	topics, descErr := h.Backend.DescribeEventTopics(h.contextWithRegion(c), req.DirectoryID, req.TopicNames)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	topicList := make([]map[string]any, 0, len(topics))
	for _, t := range topics {
		topicList = append(topicList, map[string]any{
			keyDirectoryID:    t.DirectoryID,
			"TopicName":       t.TopicName,
			"TopicArn":        t.TopicARN,
			keyStatus:         t.Status,
			"CreatedDateTime": t.CreatedDateTime.Format("2006-01-02T15:04:05.000Z"), //nolint:goconst // existing issue.
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"EventTopics": topicList})
}

// --- Domain Controllers ---

func (h *Handler) handleDescribeDomainControllers(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID         string   `json:"DirectoryId"`
		NextToken           string   `json:"NextToken"`
		DomainControllerIDs []string `json:"DomainControllerIds"`
		Limit               int32    `json:"Limit"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	dcs, nextToken, descErr := h.Backend.DescribeDomainControllers(
		h.contextWithRegion(c),
		req.DirectoryID, req.DomainControllerIDs, req.Limit, req.NextToken,
	)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	dcList := make([]map[string]any, 0, len(dcs))
	for _, dc := range dcs {
		dcList = append(dcList, map[string]any{
			"DomainControllerId": dc.ControllerID,
			keyDirectoryID:       dc.DirectoryID,
			keyStatus:            dc.Status,
			"AvailabilityZone":   dc.AvailabilityZone,
			keyLaunchTime:        dc.LaunchTime.Format("2006-01-02T15:04:05.000Z"),
		})
	}

	resp := map[string]any{"DomainControllers": dcList}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateNumberOfDomainControllers(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID   string `json:"DirectoryId"`
		DesiredNumber int32  `json:"DesiredNumber"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	updateErr := h.Backend.UpdateNumberOfDomainControllers(h.contextWithRegion(c), req.DirectoryID, req.DesiredNumber)
	if updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- Trusts ---

func (h *Handler) handleCreateTrust(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID      string `json:"DirectoryId"`
		RemoteDomainName string `json:"RemoteDomainName"`
		TrustPassword    string `json:"TrustPassword"`
		TrustDirection   string `json:"TrustDirection"`
		TrustType        string `json:"TrustType"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" || req.RemoteDomainName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ClientException", "DirectoryId and RemoteDomainName are required"),
		)
	}

	trustType := req.TrustType
	if trustType == "" {
		trustType = "Forest"
	}

	trustID, createErr := h.Backend.CreateTrust(
		h.contextWithRegion(c),
		req.DirectoryID, req.RemoteDomainName, req.TrustPassword, req.TrustDirection, trustType,
	)
	if createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyDirectoryID: req.DirectoryID,
		"TrustId":      trustID, //nolint:goconst // existing issue.
	})
}

func (h *Handler) handleDeleteTrust(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		TrustID string `json:"TrustId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.TrustID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "TrustId is required"))
	}

	trustID, delErr := h.Backend.DeleteTrust(h.contextWithRegion(c), req.TrustID)
	if delErr != nil {
		return h.mapError(c, delErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"TrustId": trustID})
}

func (h *Handler) handleDescribeTrusts(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string   `json:"DirectoryId"`
		NextToken   string   `json:"NextToken"`
		TrustIDs    []string `json:"TrustIds"`
		Limit       int32    `json:"Limit"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	trusts, nextToken, descErr := h.Backend.DescribeTrusts(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.TrustIDs,
		req.Limit,
		req.NextToken,
	)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	trustList := make([]map[string]any, 0, len(trusts))
	for _, t := range trusts {
		trustList = append(trustList, map[string]any{
			keyDirectoryID:     t.DirectoryID,
			"TrustId":          t.TrustID,
			"RemoteDomainName": t.RemoteDomainName,
			"TrustDirection":   t.TrustDirection,
			"TrustType":        t.TrustType,
			"TrustState":       t.TrustState,
			"SelectiveAuth":    t.SelectiveAuth,
			"CreatedDateTime":  t.CreatedDateTime.Format("2006-01-02T15:04:05.000Z"),
			"LastUpdatedDateTime": t.LastUpdatedDateTime.Format( //nolint:goconst // existing issue.
				"2006-01-02T15:04:05.000Z",
			),
		})
	}

	resp := map[string]any{"Trusts": trustList}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateTrust(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		TrustID       string `json:"TrustId"`
		SelectiveAuth string `json:"SelectiveAuth"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.TrustID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "TrustId is required"))
	}

	trustID, updateErr := h.Backend.UpdateTrust(h.contextWithRegion(c), req.TrustID, req.SelectiveAuth)
	if updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"TrustId":   trustID,
		"RequestId": trustID, //nolint:goconst // existing issue.
	})
}

func (h *Handler) handleVerifyTrust(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		TrustID string `json:"TrustId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.TrustID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "TrustId is required"))
	}

	trustID, verifyErr := h.Backend.VerifyTrust(h.contextWithRegion(c), req.TrustID)
	if verifyErr != nil {
		return h.mapError(c, verifyErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"TrustId": trustID})
}

// --- Shared Directories ---

func (h *Handler) handleShareDirectory(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		ShareMethod string `json:"ShareMethod"`
		ShareNotes  string `json:"ShareNotes"`
		ShareTarget struct {
			ID   string `json:"Id"`
			Type string `json:"Type"`
		} `json:"ShareTarget"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	shareMethod := req.ShareMethod
	if shareMethod == "" {
		shareMethod = "HANDSHAKE"
	}

	sharedDirID, shareErr := h.Backend.ShareDirectory(
		h.contextWithRegion(c),
		req.DirectoryID,
		shareMethod,
		req.ShareNotes,
		req.ShareTarget.ID,
	)
	if shareErr != nil {
		return h.mapError(c, shareErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"SharedDirectoryId": sharedDirID}) //nolint:goconst // existing issue.
}

func (h *Handler) handleUnshareDirectory(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID   string `json:"DirectoryId"`
		UnshareTarget struct {
			ID   string `json:"Id"`
			Type string `json:"Type"`
		} `json:"UnshareTarget"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	sharedDirID, unshareErr := h.Backend.UnshareDirectory(h.contextWithRegion(c), req.DirectoryID, req.UnshareTarget.ID)
	if unshareErr != nil {
		return h.mapError(c, unshareErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"SharedDirectoryId": sharedDirID})
}

func (h *Handler) handleAcceptSharedDirectory(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		SharedDirectoryID string `json:"SharedDirectoryId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.SharedDirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "SharedDirectoryId is required"))
	}

	id, acceptErr := h.Backend.AcceptSharedDirectory(h.contextWithRegion(c), req.SharedDirectoryID)
	if acceptErr != nil {
		return h.mapError(c, acceptErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"SharedDirectory": map[string]any{"SharedDirectoryId": id},
	})
}

func (h *Handler) handleRejectSharedDirectory(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		SharedDirectoryID string `json:"SharedDirectoryId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.SharedDirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "SharedDirectoryId is required"))
	}

	id, rejectErr := h.Backend.RejectSharedDirectory(h.contextWithRegion(c), req.SharedDirectoryID)
	if rejectErr != nil {
		return h.mapError(c, rejectErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"SharedDirectoryId": id})
}

func (h *Handler) handleDescribeSharedDirectories(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		OwnerDirectoryID   string   `json:"OwnerDirectoryId"`
		NextToken          string   `json:"NextToken"`
		SharedDirectoryIDs []string `json:"SharedDirectoryIds"`
		Limit              int32    `json:"Limit"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	if req.OwnerDirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "OwnerDirectoryId is required"))
	}

	dirs, nextToken, descErr := h.Backend.DescribeSharedDirectories(
		h.contextWithRegion(c),
		req.OwnerDirectoryID, req.SharedDirectoryIDs, req.Limit, req.NextToken,
	)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	dirList := make([]map[string]any, 0, len(dirs))
	for _, d := range dirs {
		dirList = append(dirList, map[string]any{
			"SharedDirectoryId":   d.SharedDirectoryID,
			"OwnerDirectoryId":    d.OwnerDirectoryID,
			"OwnerAccountId":      d.OwnerAccountID,
			"SharedAccountId":     d.SharedAccountID,
			"ShareMethod":         d.ShareMethod,
			"ShareStatus":         d.ShareStatus,
			"ShareNotes":          d.ShareNotes,
			"CreatedDateTime":     d.CreatedDateTime.Format("2006-01-02T15:04:05.000Z"),
			"LastUpdatedDateTime": d.LastUpdatedDateTime.Format("2006-01-02T15:04:05.000Z"),
		})
	}

	resp := map[string]any{"SharedDirectories": dirList}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

// --- Certificates ---

func (h *Handler) handleRegisterCertificate(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID     string `json:"DirectoryId"`
		CertificateData string `json:"CertificateData"`
		Type            string `json:"Type"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	certType := req.Type
	if certType == "" {
		certType = "ClientLDAPS"
	}

	certID, regErr := h.Backend.RegisterCertificate(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.CertificateData,
		certType,
	)
	if regErr != nil {
		return h.mapError(c, regErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"CertificateId": certID}) //nolint:goconst // existing issue.
}

func (h *Handler) handleDeregisterCertificate(c *echo.Context) error {
	return h.handleTwoFieldOp(c, twoFieldOp{
		secondKey: "CertificateId",
		invoke: func(ctx context.Context, dirID, second string) error {
			return h.Backend.DeregisterCertificate(ctx, dirID, second)
		},
	})
}

func (h *Handler) handleListCertificates(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		NextToken   string `json:"NextToken"`
		PageSize    int32  `json:"PageSize"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	certs, nextToken, listErr := h.Backend.ListCertificates(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.PageSize,
		req.NextToken,
	)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	certList := make([]map[string]any, 0, len(certs))
	for _, cert := range certs {
		certList = append(certList, map[string]any{
			"CertificateId":  cert.CertificateID,
			"CommonName":     cert.CommonName,
			"Type":           cert.CertType, //nolint:goconst // existing issue.
			"State":          cert.State,
			"ExpiryDateTime": cert.ExpiryDateTime.Format("2006-01-02T15:04:05.000Z"),
		})
	}

	resp := map[string]any{"CertificatesInfo": certList}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDescribeCertificate(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID   string `json:"DirectoryId"`
		CertificateID string `json:"CertificateId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" || req.CertificateID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId and CertificateId are required"))
	}

	cert, descErr := h.Backend.DescribeCertificate(h.contextWithRegion(c), req.DirectoryID, req.CertificateID)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"Certificate": map[string]any{
			"CertificateId":      cert.CertificateID,
			"CommonName":         cert.CommonName,
			"Type":               cert.CertType,
			"State":              cert.State,
			"RegisteredDateTime": cert.RegisteredDateTime.Format("2006-01-02T15:04:05.000Z"),
			"ExpiryDateTime":     cert.ExpiryDateTime.Format("2006-01-02T15:04:05.000Z"),
		},
	})
}

// --- LDAPS ---

func (h *Handler) handleEnableLDAPS(c *echo.Context) error { //nolint:dupl // existing issue.
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		Type        string `json:"Type"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	ldapsType := req.Type
	if ldapsType == "" {
		ldapsType = "Client"
	}

	if enableErr := h.Backend.EnableLDAPS(h.contextWithRegion(c), req.DirectoryID, ldapsType); enableErr != nil {
		return h.mapError(c, enableErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDisableLDAPS(c *echo.Context) error { //nolint:dupl // existing issue.
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		Type        string `json:"Type"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	ldapsType := req.Type
	if ldapsType == "" {
		ldapsType = "Client"
	}

	if disableErr := h.Backend.DisableLDAPS(h.contextWithRegion(c), req.DirectoryID, ldapsType); disableErr != nil {
		return h.mapError(c, disableErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDescribeLDAPSSettings(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		Type        string `json:"Type"`
		NextToken   string `json:"NextToken"`
		Limit       int32  `json:"Limit"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	settings, nextToken, descErr := h.Backend.DescribeLDAPSSettings(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.Type,
		req.Limit,
		req.NextToken,
	)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	settingList := make([]map[string]any, 0, len(settings))
	for _, s := range settings {
		settingList = append(settingList, map[string]any{
			"LDAPSType":                 s.LDAPSType,
			"CertificateId":             s.CertificateID,
			"LDAPSStatus":               s.State,
			"LastUpdatedDateTime":       s.LastUpdatedDateTime.Format("2006-01-02T15:04:05.000Z"),
			"CertificateExpiryDateTime": s.CertificateExpiryDateTime.Format("2006-01-02T15:04:05.000Z"),
		})
	}

	resp := map[string]any{"LDAPSSettingsInfo": settingList}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

// --- Client Authentication ---

func (h *Handler) handleEnableClientAuthentication(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		Type        string `json:"Type"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	enableErr := h.Backend.EnableClientAuthentication(h.contextWithRegion(c), req.DirectoryID, req.Type)
	if enableErr != nil {
		return h.mapError(c, enableErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDisableClientAuthentication(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		Type        string `json:"Type"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	disableErr := h.Backend.DisableClientAuthentication(h.contextWithRegion(c), req.DirectoryID, req.Type)
	if disableErr != nil {
		return h.mapError(c, disableErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDescribeClientAuthenticationSettings(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		Type        string `json:"Type"`
		NextToken   string `json:"NextToken"`
		PageSize    int32  `json:"PageSize"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	settings, nextToken, descErr := h.Backend.DescribeClientAuthenticationSettings(
		h.contextWithRegion(c),
		req.DirectoryID, req.Type, req.PageSize, req.NextToken,
	)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	settingList := make([]map[string]any, 0, len(settings))
	for _, s := range settings {
		settingList = append(settingList, map[string]any{
			"Type":                s.AuthType,
			keyStatus:             s.Status,
			"LastUpdatedDateTime": s.LastUpdatedDateTime.Format("2006-01-02T15:04:05.000Z"),
		})
	}

	resp := map[string]any{"ClientAuthenticationSettingsInfo": settingList}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

// --- RADIUS ---

func (h *Handler) handleEnableRadius(c *echo.Context) error { //nolint:dupl // existing issue.
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID    string `json:"DirectoryId"`
		RadiusSettings struct {
			AuthenticationProtocol string   `json:"AuthenticationProtocol"`
			DisplayLabel           string   `json:"DisplayLabel"`
			SharedSecret           string   `json:"SharedSecret"`
			RadiusServers          []string `json:"RadiusServers"`
			RadiusPort             int32    `json:"RadiusPort"`
			RadiusRetries          int32    `json:"RadiusRetries"`
			RadiusTimeout          int32    `json:"RadiusTimeout"`
			UseSameUsername        bool     `json:"UseSameUsername"`
		} `json:"RadiusSettings"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	settings := RadiusSettingsInput{
		AuthenticationProtocol: req.RadiusSettings.AuthenticationProtocol,
		DisplayLabel:           req.RadiusSettings.DisplayLabel,
		RadiusServers:          req.RadiusSettings.RadiusServers,
		SharedSecret:           req.RadiusSettings.SharedSecret,
		RadiusPort:             req.RadiusSettings.RadiusPort,
		RadiusRetries:          req.RadiusSettings.RadiusRetries,
		RadiusTimeout:          req.RadiusSettings.RadiusTimeout,
		UseSameUsername:        req.RadiusSettings.UseSameUsername,
	}

	if enableErr := h.Backend.EnableRadius(h.contextWithRegion(c), req.DirectoryID, settings); enableErr != nil {
		return h.mapError(c, enableErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDisableRadius(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	if disableErr := h.Backend.DisableRadius(h.contextWithRegion(c), req.DirectoryID); disableErr != nil {
		return h.mapError(c, disableErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleUpdateRadius(c *echo.Context) error { //nolint:dupl // existing issue.
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID    string `json:"DirectoryId"`
		RadiusSettings struct {
			AuthenticationProtocol string   `json:"AuthenticationProtocol"`
			DisplayLabel           string   `json:"DisplayLabel"`
			SharedSecret           string   `json:"SharedSecret"`
			RadiusServers          []string `json:"RadiusServers"`
			RadiusPort             int32    `json:"RadiusPort"`
			RadiusRetries          int32    `json:"RadiusRetries"`
			RadiusTimeout          int32    `json:"RadiusTimeout"`
			UseSameUsername        bool     `json:"UseSameUsername"`
		} `json:"RadiusSettings"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	settings := RadiusSettingsInput{
		AuthenticationProtocol: req.RadiusSettings.AuthenticationProtocol,
		DisplayLabel:           req.RadiusSettings.DisplayLabel,
		RadiusServers:          req.RadiusSettings.RadiusServers,
		SharedSecret:           req.RadiusSettings.SharedSecret,
		RadiusPort:             req.RadiusSettings.RadiusPort,
		RadiusRetries:          req.RadiusSettings.RadiusRetries,
		RadiusTimeout:          req.RadiusSettings.RadiusTimeout,
		UseSameUsername:        req.RadiusSettings.UseSameUsername,
	}

	if updateErr := h.Backend.UpdateRadius(h.contextWithRegion(c), req.DirectoryID, settings); updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- Directory Data Access ---

func (h *Handler) handleEnableDirectoryDataAccess(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	if enableErr := h.Backend.EnableDirectoryDataAccess(h.contextWithRegion(c), req.DirectoryID); enableErr != nil {
		return h.mapError(c, enableErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDisableDirectoryDataAccess(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	if disableErr := h.Backend.DisableDirectoryDataAccess(h.contextWithRegion(c), req.DirectoryID); disableErr != nil {
		return h.mapError(c, disableErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDescribeDirectoryDataAccess(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	status, descErr := h.Backend.DescribeDirectoryDataAccess(h.contextWithRegion(c), req.DirectoryID)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	dataAccessStatus := "Disabled" //nolint:goconst // existing issue.
	if status.Enabled {
		dataAccessStatus = "Enabled" //nolint:goconst // existing issue.
	}

	return c.JSON(http.StatusOK, map[string]any{
		"DirectoryDataAccessStatus": dataAccessStatus,
	})
}

// --- CA Enrollment Policy ---

func (h *Handler) handleEnableCAEnrollmentPolicy(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	if enableErr := h.Backend.EnableCAEnrollmentPolicy(h.contextWithRegion(c), req.DirectoryID); enableErr != nil {
		return h.mapError(c, enableErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDisableCAEnrollmentPolicy(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	if disableErr := h.Backend.DisableCAEnrollmentPolicy(h.contextWithRegion(c), req.DirectoryID); disableErr != nil {
		return h.mapError(c, disableErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDescribeCAEnrollmentPolicy(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	policy, descErr := h.Backend.DescribeCAEnrollmentPolicy(h.contextWithRegion(c), req.DirectoryID)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	enrollmentStatus := "Disabled"
	if policy.Enabled {
		enrollmentStatus = "Enabled"
	}

	return c.JSON(http.StatusOK, map[string]any{
		"CAEnrollmentPolicy": map[string]any{
			"EnrollmentStatus": enrollmentStatus,
		},
	})
}

// --- AD Assessments ---

func (h *Handler) handleStartADAssessment(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	assessmentID, startErr := h.Backend.StartADAssessment(h.contextWithRegion(c), req.DirectoryID)
	if startErr != nil {
		return h.mapError(c, startErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"AssessmentId": assessmentID}) //nolint:goconst // existing issue.
}

func (h *Handler) handleDeleteADAssessment(c *echo.Context) error {
	return h.handleTwoFieldOp(c, twoFieldOp{
		secondKey: "AssessmentId",
		invoke: func(ctx context.Context, dirID, second string) error {
			return h.Backend.DeleteADAssessment(ctx, dirID, second)
		},
	})
}

func (h *Handler) handleDescribeADAssessment(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID  string `json:"DirectoryId"`
		AssessmentID string `json:"AssessmentId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" || req.AssessmentID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId and AssessmentId are required"))
	}

	a, descErr := h.Backend.DescribeADAssessment(h.contextWithRegion(c), req.DirectoryID, req.AssessmentID)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ADAssessment": map[string]any{
			"AssessmentId":   a.AssessmentID,
			keyDirectoryID:   a.DirectoryID,
			keyStatus:        a.Status,
			"AssessmentType": a.AssessType,
			keyRegion:        a.Region,
			keyStartTime:     a.StartTime.Format("2006-01-02T15:04:05.000Z"),
		},
	})
}

func (h *Handler) handleListADAssessments(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		NextToken   string `json:"NextToken"`
		PageSize    int32  `json:"PageSize"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	assessments, nextToken, listErr := h.Backend.ListADAssessments(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.PageSize,
		req.NextToken,
	)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	assessList := make([]map[string]any, 0, len(assessments))
	for _, a := range assessments {
		assessList = append(assessList, map[string]any{
			"AssessmentId":   a.AssessmentID,
			keyDirectoryID:   a.DirectoryID,
			keyStatus:        a.Status,
			"AssessmentType": a.AssessType,
			keyRegion:        a.Region,
			keyStartTime:     a.StartTime.Format("2006-01-02T15:04:05.000Z"),
		})
	}

	resp := map[string]any{"ADAssessments": assessList}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

// --- Hybrid AD ---

func (h *Handler) handleCreateHybridAD(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		Name        string `json:"Name"`
		ShortName   string `json:"ShortName"`
		Description string `json:"Description"`
		Password    string `json:"Password"`
		Edition     string `json:"Edition"`
		Tags        []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.Name == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "Name is required"))
	}

	edition := DirectoryEdition(req.Edition)
	if edition == "" {
		edition = DirectoryEditionEnterprise
	}

	tags := reqTagsToTags(req.Tags)
	d, requestID, createErr := h.Backend.CreateHybridAD(
		h.contextWithRegion(c),
		req.Name,
		req.ShortName,
		req.Description,
		req.Password,
		edition,
		tags,
	)
	if createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyDirectoryID: d.DirectoryID,
		"RequestId":    requestID,
	})
}

func (h *Handler) handleUpdateHybridAD(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	requestID, updateErr := h.Backend.UpdateHybridAD(h.contextWithRegion(c), req.DirectoryID)
	if updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"RequestId": requestID})
}

func (h *Handler) handleDescribeHybridADUpdate(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	updates, descErr := h.Backend.DescribeHybridADUpdate(h.contextWithRegion(c), req.DirectoryID)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	updateList := make([]map[string]any, 0, len(updates))
	for _, u := range updates {
		updateList = append(updateList, map[string]any{
			"RequestId":    u.RequestID,
			keyDirectoryID: u.DirectoryID,
			keyStatus:      u.Status,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"HybridADUpdateInfo": updateList})
}

// --- Computer ---

func (h *Handler) handleCreateComputer(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID  string `json:"DirectoryId"`
		ComputerName string `json:"ComputerName"`
		Password     string `json:"Password"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" || req.ComputerName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId and ComputerName are required"))
	}

	computer, createErr := h.Backend.CreateComputer(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.ComputerName,
		req.Password,
	)
	if createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"Computer": map[string]any{
			"ComputerId":   computer.ComputerID,
			"ComputerName": computer.ComputerName,
		},
	})
}

// --- Settings ---

func (h *Handler) handleUpdateSettings(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		Settings    []struct {
			Name  string `json:"Name"`
			Value string `json:"Value"`
		} `json:"Settings"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	settings := make([]DirectorySetting, 0, len(req.Settings))
	for _, s := range req.Settings {
		settings = append(settings, DirectorySetting{Name: s.Name, Value: s.Value})
	}

	directoryID, updateErr := h.Backend.UpdateSettings(h.contextWithRegion(c), req.DirectoryID, settings)
	if updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyDirectoryID: directoryID})
}

func (h *Handler) handleDescribeSettings(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		Status      string `json:"Status"`
		NextToken   string `json:"NextToken"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	settings, nextToken, descErr := h.Backend.DescribeSettings(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.Status,
		req.NextToken,
	)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	settingList := make([]map[string]any, 0, len(settings))
	for _, s := range settings {
		settingList = append(settingList, map[string]any{
			"Name":                s.Name, //nolint:goconst // existing issue.
			"AllowedValues":       s.AllowedValues,
			"AppliedValue":        s.AppliedValue,
			"RequestedValue":      s.RequestedValue,
			keyStatus:             s.Status,
			"LastUpdatedDateTime": s.LastUpdatedDateTime.Format("2006-01-02T15:04:05.000Z"),
		})
	}

	resp := map[string]any{
		keyDirectoryID:   req.DirectoryID,
		"SettingEntries": settingList,
	}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateDirectorySetup(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID                string `json:"DirectoryId"`
		UpdateType                 string `json:"UpdateType"`
		CreateSnapshotBeforeUpdate bool   `json:"CreateSnapshotBeforeUpdate"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	if updateErr := h.Backend.UpdateDirectorySetup(
		h.contextWithRegion(c),
		req.DirectoryID, req.UpdateType, req.CreateSnapshotBeforeUpdate,
	); updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDescribeUpdateDirectory(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		UpdateType  string `json:"UpdateType"`
		NextToken   string `json:"NextToken"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	entries, nextToken, descErr := h.Backend.DescribeUpdateDirectory(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.UpdateType,
		req.NextToken,
	)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	entryList := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		entryList = append(entryList, map[string]any{
			"UpdateType":          e.UpdateType,
			keyStatus:             e.Status,
			"NewValue":            e.NewValue,
			"PreviousValue":       e.PreviousValue,
			"InitiatedBy":         e.InitiatedBy,
			keyRegion:             e.Region,
			keyStartTime:          e.StartTime.Format("2006-01-02T15:04:05.000Z"),
			"LastUpdatedDateTime": e.LastUpdatedDateTime.Format("2006-01-02T15:04:05.000Z"),
		})
	}

	resp := map[string]any{"UpdateDirectoryInfo": entryList}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

// --- Password Reset ---

func (h *Handler) handleResetUserPassword(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		UserName    string `json:"UserName"`
		NewPassword string `json:"NewPassword"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" || req.UserName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId and UserName are required"))
	}

	resetErr := h.Backend.ResetUserPassword(h.contextWithRegion(c), req.DirectoryID, req.UserName, req.NewPassword)
	if resetErr != nil {
		return h.mapError(c, resetErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- ConnectDirectory ---

func (h *Handler) handleConnectDirectory(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		Name        string `json:"Name"`
		ShortName   string `json:"ShortName"`
		Description string `json:"Description"`
		Password    string `json:"Password"`
		Size        string `json:"Size"`
		Tags        []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.Name == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "Name is required"))
	}
	if req.Password == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "Password is required"))
	}
	if req.Size != string(DirectorySizeSmall) && req.Size != string(DirectorySizeLarge) {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "Size must be Small or Large"))
	}

	tags := reqTagsToTags(req.Tags)
	d, createErr := h.Backend.ConnectDirectory(
		h.contextWithRegion(c),
		req.Name,
		req.ShortName,
		req.Description,
		req.Password,
		DirectorySize(req.Size),
		tags,
	)
	if createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyDirectoryID: d.DirectoryID})
}
