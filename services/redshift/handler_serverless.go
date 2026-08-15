package redshift

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ---------------------------------------------------------------------------
// Serverless handler wiring
//
// Redshift Serverless is an awsJson1.1 RPC API: every operation is a POST to
// "/" with an X-Amz-Target: RedshiftServerless.<Op> header and all parameters
// (including resource identifiers like namespaceName) in the JSON body --
// there is no REST-style path/verb routing (confirmed against
// aws-sdk-go-v2/service/redshiftserverless@v1.38.5/serializers.go: every
// awsAwsjson11_serializeOp* sets request.URL.Path = "/", Method = "POST", and
// the X-Amz-Target header; resource identifiers are serialized as body
// members, e.g. awsAwsjson11_serializeOpDocumentGetNamespaceInput writes
// "namespaceName" into the JSON object, not the URL). This mirrors the
// dispatch style already used by redshiftdata.Handler in this package's
// sibling directory.
// ---------------------------------------------------------------------------

// ServerlessHandler is a separate Echo handler for Redshift Serverless APIs.
type ServerlessHandler struct {
	Backend *InMemoryBackend
}

// NewServerlessHandler creates a new Redshift Serverless handler.
func NewServerlessHandler(backend *InMemoryBackend) *ServerlessHandler {
	return &ServerlessHandler{Backend: backend}
}

// Name returns the service name.
func (h *ServerlessHandler) Name() string { return "RedshiftServerless" }

// GetSupportedOperations returns the list of supported operations.
func (h *ServerlessHandler) GetSupportedOperations() []string {
	return []string{
		"CreateNamespace",
		"DeleteNamespace",
		"GetNamespace",
		"ListNamespaces",
		"UpdateNamespace",
		"CreateWorkgroup",
		"DeleteWorkgroup",
		"GetWorkgroup",
		"ListWorkgroups",
		"UpdateWorkgroup",
		"GetCredentials",
		"CreateSnapshot",
		"DeleteSnapshot",
		"GetSnapshot",
		"ListSnapshots",
		"CreateUsageLimit",
		"DeleteUsageLimit",
		"GetUsageLimit",
		"ListUsageLimits",
		"UpdateUsageLimit",
		"CreateScheduledAction",
		"DeleteScheduledAction",
		"GetScheduledAction",
		"ListScheduledActions",
		"UpdateScheduledAction",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
		opCreateCustomDomainAssociation,
		opDeleteCustomDomainAssociation,
		"GetCustomDomainAssociation",
		"ListCustomDomainAssociations",
		"UpdateCustomDomainAssociation",
		opGetResourcePolicy,
		opPutResourcePolicy,
		opDeleteResourcePolicy,
		"CreateSnapshotCopyConfiguration",
		"UpdateSnapshotCopyConfiguration",
		"DeleteSnapshotCopyConfiguration",
		"ListSnapshotCopyConfigurations",
		"GetRecoveryPoint",
		"ListRecoveryPoints",
		"RestoreFromRecoveryPoint",
		"RestoreTableFromSnapshot",
		"RestoreTableFromRecoveryPoint",
		"GetTableRestoreStatus",
		"ListTableRestoreStatus",
		opCreateEndpointAccess,
		"GetEndpointAccess",
		"ListEndpointAccess",
		"UpdateEndpointAccess",
		opDeleteEndpointAccess,
		"ListManagedWorkgroups",
		"RestoreFromSnapshot",
		"ConvertRecoveryPointToSnapshot",
		"UpdateSnapshot",
		"GetTrack",
		"ListTracks",
		"UpdateLakehouseConfiguration",
		opGetIdentityCenterAuthToken,
	}
}

// ChaosServiceName returns the chaos service name.
func (h *ServerlessHandler) ChaosServiceName() string { return "redshift-serverless" }

// ChaosOperations returns all supported operations.
func (h *ServerlessHandler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns the regions for chaos.
func (h *ServerlessHandler) ChaosRegions() []string { return []string{h.Backend.region} }

// Reset clears all backend state.
func (h *ServerlessHandler) Reset() {
	h.Backend.mu.Lock("ServerlessHandler.Reset")
	defer h.Backend.mu.Unlock()

	h.Backend.slNamespaces.Reset()
	h.Backend.slWorkgroups.Reset()
	h.Backend.slSnapshots.Reset()
	h.Backend.slUsageLimits.Reset()
	h.Backend.slScheduledActions.Reset()
	h.Backend.slResourceTags.Reset()
	h.Backend.slCustomDomains.Reset()
	h.Backend.slResourcePolicies.Reset()
	h.Backend.slSnapshotCopyConfig.Reset()
	h.Backend.slRecoveryPoints.Reset()
	h.Backend.slTableRestoreStatuses.Reset()
	h.Backend.slEndpointAccesses.Reset()
	h.Backend.slLakehouseConfig.Reset()
	h.Backend.resetServerlessIndexes()
}

const slTargetPrefix = "RedshiftServerless."

// RouteMatcher returns a function that matches Redshift Serverless requests
// by their X-Amz-Target header, the real wire discriminator for this
// awsJson1.1 API (see package doc comment above).
func (h *ServerlessHandler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), slTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *ServerlessHandler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *ServerlessHandler) ExtractOperation(c *echo.Context) string {
	return strings.TrimPrefix(c.Request().Header.Get("X-Amz-Target"), slTargetPrefix)
}

// ExtractResource extracts a resource identifier from the request. Best-effort
// only (used for logging/chaos matching); the authoritative field varies per op.
func (h *ServerlessHandler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var probe struct {
		NamespaceName string `json:"namespaceName"`
		WorkgroupName string `json:"workgroupName"`
	}

	if json.Unmarshal(body, &probe) != nil {
		return ""
	}

	if probe.NamespaceName != "" {
		return probe.NamespaceName
	}

	return probe.WorkgroupName
}

// Handler returns the Echo handler function.
func (h *ServerlessHandler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		log := logger.Load(r.Context())

		body, err := httputils.ReadBody(r)
		if err != nil {
			log.ErrorContext(r.Context(), "redshift-serverless: failed to read body", "error", err)

			return c.JSON(
				http.StatusInternalServerError,
				slErrorResponse("InternalFailure", "internal server error"),
			)
		}

		op := h.ExtractOperation(c)

		fn, ok := slDispatchTable[op]
		if !ok {
			return c.JSON(
				http.StatusBadRequest,
				slErrorResponse("ValidationException", "unknown operation: "+op),
			)
		}

		return fn(h, c, body)
	}
}

// slDispatchTable maps each supported operation to its handler. A map keeps
// routing a flat lookup instead of one large switch, matching the pattern
// this file used before (dispatchNamespace/dispatchWorkgroup/...) but keyed
// by the real X-Amz-Target action name now that routing no longer derives
// the op from URL shape.
//
//nolint:gochecknoglobals // read-only dispatch table, same convention as pkgs/awscron's month/day-of-week name tables
var slDispatchTable = map[string]func(*ServerlessHandler, *echo.Context, []byte) error{
	"CreateNamespace":       (*ServerlessHandler).handleCreateNamespace,
	"GetNamespace":          (*ServerlessHandler).handleGetNamespace,
	"ListNamespaces":        (*ServerlessHandler).handleListNamespaces,
	"UpdateNamespace":       (*ServerlessHandler).handleUpdateNamespace,
	"DeleteNamespace":       (*ServerlessHandler).handleDeleteNamespace,
	"CreateWorkgroup":       (*ServerlessHandler).handleCreateWorkgroup,
	"GetWorkgroup":          (*ServerlessHandler).handleGetWorkgroup,
	"ListWorkgroups":        (*ServerlessHandler).handleListWorkgroups,
	"UpdateWorkgroup":       (*ServerlessHandler).handleUpdateWorkgroup,
	"DeleteWorkgroup":       (*ServerlessHandler).handleDeleteWorkgroup,
	"GetCredentials":        (*ServerlessHandler).handleGetCredentials,
	"CreateSnapshot":        (*ServerlessHandler).handleCreateSnapshot,
	"GetSnapshot":           (*ServerlessHandler).handleGetSnapshot,
	"ListSnapshots":         (*ServerlessHandler).handleListSnapshots,
	"DeleteSnapshot":        (*ServerlessHandler).handleDeleteSnapshot,
	"CreateUsageLimit":      (*ServerlessHandler).handleCreateUsageLimit,
	"GetUsageLimit":         (*ServerlessHandler).handleGetUsageLimit,
	"ListUsageLimits":       (*ServerlessHandler).handleListUsageLimits,
	"UpdateUsageLimit":      (*ServerlessHandler).handleUpdateUsageLimit,
	"DeleteUsageLimit":      (*ServerlessHandler).handleDeleteUsageLimit,
	"CreateScheduledAction": (*ServerlessHandler).handleCreateScheduledAction,
	"GetScheduledAction":    (*ServerlessHandler).handleGetScheduledAction,
	"ListScheduledActions":  (*ServerlessHandler).handleListScheduledActions,
	"UpdateScheduledAction": (*ServerlessHandler).handleUpdateScheduledAction,
	"DeleteScheduledAction": (*ServerlessHandler).handleDeleteScheduledAction,

	"TagResource":                   (*ServerlessHandler).handleTagResource,
	"UntagResource":                 (*ServerlessHandler).handleUntagResource,
	"ListTagsForResource":           (*ServerlessHandler).handleListTagsForResource,
	opCreateCustomDomainAssociation: (*ServerlessHandler).handleCreateCustomDomainAssociation,
	"GetCustomDomainAssociation":    (*ServerlessHandler).handleGetCustomDomainAssociation,
	"ListCustomDomainAssociations":  (*ServerlessHandler).handleListCustomDomainAssociations,
	"UpdateCustomDomainAssociation": (*ServerlessHandler).handleUpdateCustomDomainAssociation,
	opDeleteCustomDomainAssociation: (*ServerlessHandler).handleDeleteCustomDomainAssociation,

	opGetResourcePolicy:    (*ServerlessHandler).handleGetResourcePolicySL,
	opPutResourcePolicy:    (*ServerlessHandler).handlePutResourcePolicySL,
	opDeleteResourcePolicy: (*ServerlessHandler).handleDeleteResourcePolicySL,

	"CreateSnapshotCopyConfiguration": (*ServerlessHandler).handleCreateSnapshotCopyConfigurationSL,
	"UpdateSnapshotCopyConfiguration": (*ServerlessHandler).handleUpdateSnapshotCopyConfigurationSL,
	"DeleteSnapshotCopyConfiguration": (*ServerlessHandler).handleDeleteSnapshotCopyConfigurationSL,
	"ListSnapshotCopyConfigurations":  (*ServerlessHandler).handleListSnapshotCopyConfigurationsSL,

	"GetRecoveryPoint":         (*ServerlessHandler).handleGetRecoveryPoint,
	"ListRecoveryPoints":       (*ServerlessHandler).handleListRecoveryPoints,
	"RestoreFromRecoveryPoint": (*ServerlessHandler).handleRestoreFromRecoveryPoint,

	"RestoreTableFromSnapshot":      (*ServerlessHandler).handleRestoreTableFromSnapshot,
	"RestoreTableFromRecoveryPoint": (*ServerlessHandler).handleRestoreTableFromRecoveryPoint,
	"GetTableRestoreStatus":         (*ServerlessHandler).handleGetTableRestoreStatus,
	"ListTableRestoreStatus":        (*ServerlessHandler).handleListTableRestoreStatus,

	opCreateEndpointAccess: (*ServerlessHandler).handleCreateEndpointAccessSL,
	"GetEndpointAccess":    (*ServerlessHandler).handleGetEndpointAccessSL,
	"ListEndpointAccess":   (*ServerlessHandler).handleListEndpointAccessSL,
	"UpdateEndpointAccess": (*ServerlessHandler).handleUpdateEndpointAccessSL,
	opDeleteEndpointAccess: (*ServerlessHandler).handleDeleteEndpointAccessSL,

	"ListManagedWorkgroups": (*ServerlessHandler).handleListManagedWorkgroups,

	"RestoreFromSnapshot":            (*ServerlessHandler).handleRestoreFromSnapshot,
	"ConvertRecoveryPointToSnapshot": (*ServerlessHandler).handleConvertRecoveryPointToSnapshot,

	"UpdateSnapshot":               (*ServerlessHandler).handleUpdateSnapshot,
	"GetTrack":                     (*ServerlessHandler).handleGetTrack,
	"ListTracks":                   (*ServerlessHandler).handleListTracks,
	"UpdateLakehouseConfiguration": (*ServerlessHandler).handleUpdateLakehouseConfiguration,
	opGetIdentityCenterAuthToken:   (*ServerlessHandler).handleGetIdentityCenterAuthTokenSL,
}

// ---------------------------------------------------------------------------
// Namespace handlers
// ---------------------------------------------------------------------------

func (h *ServerlessHandler) handleCreateNamespace(c *echo.Context, body []byte) error {
	var req struct {
		NamespaceName               string      `json:"namespaceName"`
		AdminUsername               string      `json:"adminUsername"`
		AdminUserPassword           string      `json:"adminUserPassword"`
		DBName                      string      `json:"dbName"`
		KmsKeyID                    string      `json:"kmsKeyId"`
		DefaultIamRoleArn           string      `json:"defaultIamRoleArn"`
		AdminPasswordSecretKmsKeyID string      `json:"adminPasswordSecretKmsKeyId"`
		RedshiftIdcApplicationArn   string      `json:"redshiftIdcApplicationArn"`
		IamRoles                    []string    `json:"iamRoles"`
		LogExports                  []string    `json:"logExports"`
		Tags                        []slTagWire `json:"tags"`
		ManageAdminPassword         bool        `json:"manageAdminPassword"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	if req.NamespaceName == "" {
		return slBadRequest(c, "namespaceName is required")
	}

	ns, err := h.Backend.CreateNamespace(CreateNamespaceParams{
		NamespaceName:               req.NamespaceName,
		AdminUsername:               req.AdminUsername,
		AdminUserPassword:           req.AdminUserPassword,
		DBName:                      req.DBName,
		KmsKeyID:                    req.KmsKeyID,
		DefaultIamRoleArn:           req.DefaultIamRoleArn,
		AdminPasswordSecretKmsKeyID: req.AdminPasswordSecretKmsKeyID,
		RedshiftIdcApplicationArn:   req.RedshiftIdcApplicationArn,
		ManageAdminPassword:         req.ManageAdminPassword,
		IamRoles:                    req.IamRoles,
		LogExports:                  req.LogExports,
		Tags:                        slTagsFromWire(req.Tags),
	})
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespNamespace: ns})
}

func (h *ServerlessHandler) handleGetNamespace(c *echo.Context, body []byte) error {
	var req struct {
		NamespaceName string `json:"namespaceName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	ns, err := h.Backend.GetNamespace(req.NamespaceName)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespNamespace: ns})
}

func (h *ServerlessHandler) handleListNamespaces(c *echo.Context, body []byte) error {
	var req struct {
		NextToken  string `json:"nextToken"`
		MaxResults int    `json:"maxResults"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return slBadRequest(c, "invalid request body")
		}
	}

	list, outToken := h.Backend.ListNamespaces(req.MaxResults, req.NextToken)
	resp := map[string]any{"namespaces": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *ServerlessHandler) handleUpdateNamespace(c *echo.Context, body []byte) error {
	var req struct {
		NamespaceName               string   `json:"namespaceName"`
		AdminUsername               string   `json:"adminUsername"`
		AdminUserPassword           string   `json:"adminUserPassword"`
		KmsKeyID                    string   `json:"kmsKeyId"`
		DefaultIamRoleArn           string   `json:"defaultIamRoleArn"`
		AdminPasswordSecretKmsKeyID string   `json:"adminPasswordSecretKmsKeyId"`
		ManageAdminPassword         *bool    `json:"manageAdminPassword"`
		IamRoles                    []string `json:"iamRoles"`
		LogExports                  []string `json:"logExports"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	ns, err := h.Backend.UpdateNamespace(req.NamespaceName, UpdateNamespaceParams{
		AdminUsername:               req.AdminUsername,
		AdminUserPassword:           req.AdminUserPassword,
		KmsKeyID:                    req.KmsKeyID,
		DefaultIamRoleArn:           req.DefaultIamRoleArn,
		AdminPasswordSecretKmsKeyID: req.AdminPasswordSecretKmsKeyID,
		ManageAdminPassword:         req.ManageAdminPassword,
		IamRoles:                    req.IamRoles,
		LogExports:                  req.LogExports,
	})
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespNamespace: ns})
}

func (h *ServerlessHandler) handleDeleteNamespace(c *echo.Context, body []byte) error {
	var req struct {
		NamespaceName                string `json:"namespaceName"`
		FinalSnapshotName            string `json:"finalSnapshotName"`
		FinalSnapshotRetentionPeriod int    `json:"finalSnapshotRetentionPeriod"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	ns, err := h.Backend.DeleteNamespace(req.NamespaceName, req.FinalSnapshotName, req.FinalSnapshotRetentionPeriod)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespNamespace: ns})
}

// ---------------------------------------------------------------------------
// Workgroup handlers
// ---------------------------------------------------------------------------

type slWorkgroupReq struct {
	PricePerformanceTarget *PerformanceTarget `json:"pricePerformanceTarget"`
	NamespaceName          string             `json:"namespaceName"`
	TrackName              string             `json:"trackName"`
	WorkgroupName          string             `json:"workgroupName"`
	IPAddressType          string             `json:"ipAddressType"`
	SecurityGroupIDs       []string           `json:"securityGroupIds"`
	ConfigParameters       []ConfigParameter  `json:"configParameters"`
	SubnetIDs              []string           `json:"subnetIds"`
	// Tags is CreateWorkgroupInput-only (UpdateWorkgroupRequest has no "tags"
	// field, confirmed absent in service-2.json) but parsed on this shared
	// struct for convenience; UpdateWorkgroup's handler simply never reads it.
	Tags                                 []slTagWire `json:"tags"`
	BaseCapacity                         int         `json:"baseCapacity"`
	MaxCapacity                          int         `json:"maxCapacity"`
	Port                                 int         `json:"port"`
	EnhancedVpcRouting                   bool        `json:"enhancedVpcRouting"`
	ExtraComputeForAutomaticOptimization bool        `json:"extraComputeForAutomaticOptimization"`
	PubliclyAccessible                   bool        `json:"publiclyAccessible"`
}

func (r slWorkgroupReq) toParams() WorkgroupParams {
	return WorkgroupParams{
		BaseCapacity:                         r.BaseCapacity,
		MaxCapacity:                          r.MaxCapacity,
		Port:                                 r.Port,
		IPAddressType:                        r.IPAddressType,
		TrackName:                            r.TrackName,
		ConfigParameters:                     r.ConfigParameters,
		PricePerformanceTarget:               r.PricePerformanceTarget,
		SubnetIDs:                            r.SubnetIDs,
		SecurityGroupIDs:                     r.SecurityGroupIDs,
		EnhancedVpcRouting:                   r.EnhancedVpcRouting,
		ExtraComputeForAutomaticOptimization: r.ExtraComputeForAutomaticOptimization,
		PubliclyAccessible:                   r.PubliclyAccessible,
	}
}

func (h *ServerlessHandler) handleCreateWorkgroup(c *echo.Context, body []byte) error {
	var req slWorkgroupReq

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	if req.WorkgroupName == "" || req.NamespaceName == "" {
		return slBadRequest(c, "workgroupName and namespaceName are required")
	}

	wg, err := h.Backend.CreateWorkgroup(req.WorkgroupName, req.NamespaceName, req.toParams(), slTagsFromWire(req.Tags))
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespWorkgroup: wg})
}

func (h *ServerlessHandler) handleGetWorkgroup(c *echo.Context, body []byte) error {
	var req struct {
		WorkgroupName string `json:"workgroupName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	wg, err := h.Backend.GetWorkgroup(req.WorkgroupName)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespWorkgroup: wg})
}

func (h *ServerlessHandler) handleListWorkgroups(c *echo.Context, body []byte) error {
	var req struct {
		OwnerAccount string `json:"ownerAccount"`
		NextToken    string `json:"nextToken"`
		MaxResults   int    `json:"maxResults"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return slBadRequest(c, "invalid request body")
		}
	}

	list, outToken := h.Backend.ListWorkgroups(req.OwnerAccount, req.MaxResults, req.NextToken)
	resp := map[string]any{"workgroups": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *ServerlessHandler) handleUpdateWorkgroup(c *echo.Context, body []byte) error {
	var req slWorkgroupReq

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	wg, err := h.Backend.UpdateWorkgroup(req.WorkgroupName, req.toParams())
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespWorkgroup: wg})
}

func (h *ServerlessHandler) handleDeleteWorkgroup(c *echo.Context, body []byte) error {
	var req struct {
		WorkgroupName string `json:"workgroupName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	wg, err := h.Backend.DeleteWorkgroup(req.WorkgroupName)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespWorkgroup: wg})
}

func (h *ServerlessHandler) handleGetCredentials(c *echo.Context, body []byte) error {
	var req struct {
		WorkgroupName    string `json:"workgroupName"`
		CustomDomainName string `json:"customDomainName"`
		DBName           string `json:"dbName"`
		DurationSeconds  int    `json:"durationSeconds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	workgroupName := req.WorkgroupName

	// GetCredentialsRequest accepts either field: "The custom domain name or
	// the workgroup name must be included in the request" (confirmed against
	// GetCredentialsRequest's documentation in service-2.json).
	if workgroupName == "" {
		if req.CustomDomainName == "" {
			return slBadRequest(c, "customDomainName or workgroupName is required")
		}

		resolved, err := h.Backend.WorkgroupNameByCustomDomainSL(req.CustomDomainName)
		if err != nil {
			return slHandleErr(c, err)
		}

		workgroupName = resolved
	}

	dbUser, dbPassword, expiration, nextRefresh, err := h.Backend.GetCredentials(
		workgroupName, req.DBName, req.DurationSeconds,
	)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"dbUser":          dbUser, // backend already includes "IAMR:" prefix
		"dbPassword":      dbPassword,
		"expiration":      awstime.Epoch(expiration),
		"nextRefreshTime": awstime.Epoch(nextRefresh),
	})
}

// ---------------------------------------------------------------------------
// Snapshot handlers
// ---------------------------------------------------------------------------

func (h *ServerlessHandler) handleCreateSnapshot(c *echo.Context, body []byte) error {
	var req struct {
		SnapshotName    string      `json:"snapshotName"`
		NamespaceName   string      `json:"namespaceName"`
		Tags            []slTagWire `json:"tags"`
		RetentionPeriod int         `json:"retentionPeriod"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	snap, err := h.Backend.CreateServerlessSnapshot(
		req.SnapshotName, req.NamespaceName, req.RetentionPeriod, slTagsFromWire(req.Tags),
	)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespSnapshot: snap})
}

func (h *ServerlessHandler) handleGetSnapshot(c *echo.Context, body []byte) error {
	var req struct {
		SnapshotName string `json:"snapshotName"`
		SnapshotArn  string `json:"snapshotArn"`
		OwnerAccount string `json:"ownerAccount"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	lookup := req.SnapshotName
	if lookup == "" {
		lookup = req.SnapshotArn
	}

	snap, err := h.Backend.GetServerlessSnapshot(lookup, req.OwnerAccount)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespSnapshot: snap})
}

func (h *ServerlessHandler) handleListSnapshots(c *echo.Context, body []byte) error {
	var req struct {
		StartTime     *float64 `json:"startTime"`
		EndTime       *float64 `json:"endTime"`
		NamespaceName string   `json:"namespaceName"`
		NamespaceArn  string   `json:"namespaceArn"`
		OwnerAccount  string   `json:"ownerAccount"`
		NextToken     string   `json:"nextToken"`
		MaxResults    int      `json:"maxResults"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return slBadRequest(c, "invalid request body")
		}
	}

	list, outToken := h.Backend.ListServerlessSnapshots(ListServerlessSnapshotsParams{
		NamespaceName: req.NamespaceName,
		NamespaceArn:  req.NamespaceArn,
		OwnerAccount:  req.OwnerAccount,
		StartTime:     slEpochFromPtr(req.StartTime),
		EndTime:       slEpochFromPtr(req.EndTime),
		MaxResults:    req.MaxResults,
		NextToken:     req.NextToken,
	})
	resp := map[string]any{"snapshots": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *ServerlessHandler) handleDeleteSnapshot(c *echo.Context, body []byte) error {
	var req struct {
		SnapshotName string `json:"snapshotName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	snap, err := h.Backend.DeleteServerlessSnapshot(req.SnapshotName)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespSnapshot: snap})
}

func (h *ServerlessHandler) handleUpdateSnapshot(c *echo.Context, body []byte) error {
	var req struct {
		RetentionPeriod *int   `json:"retentionPeriod"`
		SnapshotName    string `json:"snapshotName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	if req.SnapshotName == "" {
		return slBadRequest(c, "snapshotName is required")
	}

	snap, err := h.Backend.UpdateServerlessSnapshot(req.SnapshotName, req.RetentionPeriod)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespSnapshot: snap})
}

// ---------------------------------------------------------------------------
// Usage limit handlers
// ---------------------------------------------------------------------------

func (h *ServerlessHandler) handleCreateUsageLimit(c *echo.Context, body []byte) error {
	var req struct {
		ResourceArn  string `json:"resourceArn"`
		UsageType    string `json:"usageType"`
		Period       string `json:"period"`
		BreachAction string `json:"breachAction"`
		Amount       int64  `json:"amount"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	ul, err := h.Backend.CreateServerlessUsageLimit(
		req.ResourceArn,
		req.UsageType,
		req.Period,
		req.BreachAction,
		req.Amount,
	)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespUsageLimit: ul})
}

func (h *ServerlessHandler) handleGetUsageLimit(c *echo.Context, body []byte) error {
	var req struct {
		UsageLimitID string `json:"usageLimitId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	ul, err := h.Backend.GetServerlessUsageLimit(req.UsageLimitID)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespUsageLimit: ul})
}

func (h *ServerlessHandler) handleListUsageLimits(c *echo.Context, body []byte) error {
	var req struct {
		ResourceArn string `json:"resourceArn"`
		UsageType   string `json:"usageType"`
		NextToken   string `json:"nextToken"`
		MaxResults  int    `json:"maxResults"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return slBadRequest(c, "invalid request body")
		}
	}

	list, outToken := h.Backend.ListServerlessUsageLimits(req.ResourceArn, req.UsageType, req.MaxResults, req.NextToken)
	resp := map[string]any{"usageLimits": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *ServerlessHandler) handleUpdateUsageLimit(c *echo.Context, body []byte) error {
	var req struct {
		UsageLimitID string `json:"usageLimitId"`
		BreachAction string `json:"breachAction"`
		Amount       int64  `json:"amount"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	ul, err := h.Backend.UpdateServerlessUsageLimit(req.UsageLimitID, req.BreachAction, req.Amount)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespUsageLimit: ul})
}

func (h *ServerlessHandler) handleDeleteUsageLimit(c *echo.Context, body []byte) error {
	var req struct {
		UsageLimitID string `json:"usageLimitId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	ul, err := h.Backend.DeleteServerlessUsageLimit(req.UsageLimitID)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespUsageLimit: ul})
}

// ---------------------------------------------------------------------------
// Scheduled action handlers
// ---------------------------------------------------------------------------

// slScheduledActionWire is the wire shape for ServerlessScheduledAction
// responses. StartTime/EndTime are epoch-seconds numbers on the wire (see
// ScheduledActionResponse's "startTime"/"endTime" case in deserializers.go,
// which requires json.Number, unlike Namespace/Workgroup/Snapshot's
// date-time-string "creationDate"/"snapshotCreateTime") -- ServerlessScheduledAction
// itself stores them as time.Time (json:"-") so this wrapper does the epoch
// conversion at the response boundary, same pattern as
// applicationautoscaling's epochSecondsPtr.
type slScheduledActionWire struct {
	StartTime                  *float64        `json:"startTime,omitempty"`
	EndTime                    *float64        `json:"endTime,omitempty"`
	NamespaceName              string          `json:"namespaceName,omitempty"`
	RoleArn                    string          `json:"roleArn,omitempty"`
	ScheduledActionDescription string          `json:"scheduledActionDescription,omitempty"`
	ScheduledActionName        string          `json:"scheduledActionName"`
	ScheduledActionUUID        string          `json:"scheduledActionUuid,omitempty"`
	State                      string          `json:"state"`
	Schedule                   json.RawMessage `json:"schedule,omitempty"`
	TargetAction               json.RawMessage `json:"targetAction,omitempty"`
}

func slEpochPtr(t time.Time) *float64 {
	if t.IsZero() {
		return nil
	}

	v := awstime.Epoch(t)

	return &v
}

func toScheduledActionWire(sa *ServerlessScheduledAction) *slScheduledActionWire {
	return &slScheduledActionWire{
		NamespaceName:              sa.NamespaceName,
		RoleArn:                    sa.RoleArn,
		Schedule:                   sa.Schedule,
		ScheduledActionDescription: sa.ScheduledActionDescription,
		ScheduledActionName:        sa.ScheduledActionName,
		ScheduledActionUUID:        sa.ScheduledActionUUID,
		State:                      sa.State,
		TargetAction:               sa.TargetAction,
		StartTime:                  slEpochPtr(sa.StartTime),
		EndTime:                    slEpochPtr(sa.EndTime),
	}
}

func (h *ServerlessHandler) handleCreateScheduledAction(c *echo.Context, body []byte) error {
	var req struct {
		Enabled                    *bool           `json:"enabled"`
		StartTime                  *float64        `json:"startTime"`
		EndTime                    *float64        `json:"endTime"`
		ScheduledActionName        string          `json:"scheduledActionName"`
		NamespaceName              string          `json:"namespaceName"`
		RoleArn                    string          `json:"roleArn"`
		ScheduledActionDescription string          `json:"scheduledActionDescription"`
		Schedule                   json.RawMessage `json:"schedule"`
		TargetAction               json.RawMessage `json:"targetAction"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	if req.ScheduledActionName == "" || req.NamespaceName == "" || req.RoleArn == "" || len(req.Schedule) == 0 {
		return slBadRequest(c, "scheduledActionName, namespaceName, roleArn and schedule are required")
	}

	sa, err := h.Backend.CreateServerlessScheduledAction(CreateScheduledActionParams{
		ScheduledActionName:        req.ScheduledActionName,
		NamespaceName:              req.NamespaceName,
		RoleArn:                    req.RoleArn,
		Schedule:                   req.Schedule,
		TargetAction:               req.TargetAction,
		ScheduledActionDescription: req.ScheduledActionDescription,
		Enabled:                    req.Enabled,
		StartTime:                  slEpochFromPtr(req.StartTime),
		EndTime:                    slEpochFromPtr(req.EndTime),
	})
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespScheduledAction: toScheduledActionWire(sa)})
}

func slEpochFromPtr(v *float64) time.Time {
	if v == nil {
		return time.Time{}
	}

	return time.UnixMilli(int64(*v * 1000)).UTC() //nolint:mnd // ms-per-second conversion, not a magic threshold
}

func (h *ServerlessHandler) handleGetScheduledAction(c *echo.Context, body []byte) error {
	var req struct {
		ScheduledActionName string `json:"scheduledActionName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	sa, err := h.Backend.GetServerlessScheduledAction(req.ScheduledActionName)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespScheduledAction: toScheduledActionWire(sa)})
}

func (h *ServerlessHandler) handleListScheduledActions(c *echo.Context, body []byte) error {
	var req struct {
		NamespaceName string `json:"namespaceName"`
		NextToken     string `json:"nextToken"`
		MaxResults    int    `json:"maxResults"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return slBadRequest(c, "invalid request body")
		}
	}

	list, outToken := h.Backend.ListServerlessScheduledActions(req.NamespaceName, req.MaxResults, req.NextToken)

	wire := make([]*slScheduledActionWire, 0, len(list))
	for _, sa := range list {
		wire = append(wire, toScheduledActionWire(sa))
	}

	resp := map[string]any{"scheduledActions": wire}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *ServerlessHandler) handleUpdateScheduledAction(c *echo.Context, body []byte) error {
	var req struct {
		ScheduledActionDescription *string         `json:"scheduledActionDescription"`
		Enabled                    *bool           `json:"enabled"`
		StartTime                  *float64        `json:"startTime"`
		EndTime                    *float64        `json:"endTime"`
		ScheduledActionName        string          `json:"scheduledActionName"`
		RoleArn                    string          `json:"roleArn"`
		Schedule                   json.RawMessage `json:"schedule"`
		TargetAction               json.RawMessage `json:"targetAction"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	sa, err := h.Backend.UpdateServerlessScheduledAction(req.ScheduledActionName, UpdateScheduledActionParams{
		RoleArn:                    req.RoleArn,
		Schedule:                   req.Schedule,
		TargetAction:               req.TargetAction,
		ScheduledActionDescription: req.ScheduledActionDescription,
		Enabled:                    req.Enabled,
		StartTime:                  slEpochFromPtr(req.StartTime),
		EndTime:                    slEpochFromPtr(req.EndTime),
	})
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespScheduledAction: toScheduledActionWire(sa)})
}

func (h *ServerlessHandler) handleDeleteScheduledAction(c *echo.Context, body []byte) error {
	var req struct {
		ScheduledActionName string `json:"scheduledActionName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	sa, err := h.Backend.DeleteServerlessScheduledAction(req.ScheduledActionName)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespScheduledAction: toScheduledActionWire(sa)})
}

// ---------------------------------------------------------------------------
// Track handlers
// ---------------------------------------------------------------------------

func (h *ServerlessHandler) handleGetTrack(c *echo.Context, body []byte) error {
	var req struct {
		TrackName string `json:"trackName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	if req.TrackName == "" {
		return slBadRequest(c, "trackName is required")
	}

	track, err := h.Backend.GetServerlessTrack(req.TrackName)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"track": track})
}

func (h *ServerlessHandler) handleListTracks(c *echo.Context, body []byte) error {
	var req struct {
		NextToken  string `json:"nextToken"`
		MaxResults int    `json:"maxResults"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return slBadRequest(c, "invalid request body")
		}
	}

	list, outToken := h.Backend.ListServerlessTracks(req.MaxResults, req.NextToken)
	resp := map[string]any{"tracks": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

// ---------------------------------------------------------------------------
// Lakehouse configuration handler
// ---------------------------------------------------------------------------

func (h *ServerlessHandler) handleUpdateLakehouseConfiguration(c *echo.Context, body []byte) error {
	var req struct {
		NamespaceName              string `json:"namespaceName"`
		CatalogName                string `json:"catalogName"`
		LakehouseIdcApplicationArn string `json:"lakehouseIdcApplicationArn"`
		LakehouseIdcRegistration   string `json:"lakehouseIdcRegistration"`
		LakehouseRegistration      string `json:"lakehouseRegistration"`
		DryRun                     bool   `json:"dryRun"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	if req.NamespaceName == "" {
		return slBadRequest(c, "namespaceName is required")
	}

	result, err := h.Backend.UpdateLakehouseConfigurationSL(UpdateLakehouseConfigParams{
		NamespaceName:              req.NamespaceName,
		CatalogName:                req.CatalogName,
		LakehouseIdcApplicationArn: req.LakehouseIdcApplicationArn,
		LakehouseIdcRegistration:   req.LakehouseIdcRegistration,
		LakehouseRegistration:      req.LakehouseRegistration,
		DryRun:                     req.DryRun,
	})
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, result)
}

// ---------------------------------------------------------------------------
// Identity Center auth token handler
// ---------------------------------------------------------------------------

func (h *ServerlessHandler) handleGetIdentityCenterAuthTokenSL(c *echo.Context, body []byte) error {
	var req struct {
		WorkgroupNames []string `json:"workgroupNames"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	token, expiry, err := h.Backend.GetIdentityCenterAuthTokenSL(req.WorkgroupNames)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"token":          token,
		"expirationTime": expiry,
	})
}

// ---------------------------------------------------------------------------
// Response envelope keys and error helpers
// ---------------------------------------------------------------------------

const (
	slRespNamespace          = "namespace"
	slRespWorkgroup          = "workgroup"
	slRespSnapshot           = "snapshot"
	slRespUsageLimit         = "usageLimit"
	slRespScheduledAction    = "scheduledAction"
	slRespSnapshotCopyConfig = "snapshotCopyConfiguration"
	slRespRecoveryPoint      = "recoveryPoint"
	slRespTableRestoreStatus = "tableRestoreStatus"
	slRespEndpoint           = "endpoint"
)

func slErrorResponse(code, msg string) map[string]any {
	return map[string]any{
		"__type":  code,
		"message": msg,
	}
}

func slBadRequest(c *echo.Context, msg string) error {
	return c.JSON(http.StatusBadRequest, slErrorResponse("ValidationException", msg))
}

// slHandleErr maps a backend sentinel error to the AWS JSON1.1 error envelope.
// The real protocol returns HTTP 400 for every client-fault exception
// (ResourceNotFoundException/ConflictException/ValidationException alike) --
// confirmed by the absence of any http-status override in
// redshiftserverless/types/errors.go (only ErrorFault Client/Server, no
// per-exception status trait), unlike rest-json services.
func slHandleErr(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrNamespaceNotFound),
		errors.Is(err, ErrWorkgroupNotFound),
		errors.Is(err, ErrServerlessSnapshotNotFound),
		errors.Is(err, ErrUsageLimitSLNotFound),
		errors.Is(err, ErrScheduledActionSLNotFound),
		errors.Is(err, ErrServerlessResourceNotFound),
		errors.Is(err, ErrCustomDomainSLNotFound),
		errors.Is(err, ErrResourcePolicySLNotFound),
		errors.Is(err, ErrSnapshotCopyConfigSLNotFound),
		errors.Is(err, ErrRecoveryPointNotFound),
		errors.Is(err, ErrTableRestoreSLNotFound),
		errors.Is(err, ErrEndpointAccessSLNotFound),
		errors.Is(err, ErrServerlessTrackNotFound):
		return c.JSON(http.StatusBadRequest, slErrorResponse("ResourceNotFoundException", err.Error()))
	case errors.Is(err, ErrNamespaceAlreadyExists),
		errors.Is(err, ErrWorkgroupAlreadyExists),
		errors.Is(err, ErrServerlessConflict),
		errors.Is(err, ErrCustomDomainSLConflict),
		errors.Is(err, ErrEndpointAccessSLAlreadyExists):
		return c.JSON(http.StatusBadRequest, slErrorResponse("ConflictException", err.Error()))
	case errors.Is(err, ErrServerlessDryRun):
		return c.JSON(http.StatusBadRequest, slErrorResponse("DryRunException", err.Error()))
	default:
		return c.JSON(http.StatusBadRequest, slErrorResponse("ValidationException", err.Error()))
	}
}
