package appstream

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	appstreamTargetPrefix  = "PhotonAdminProxyService."
	appstreamContentType   = "application/x-amz-json-1.1"
	keyTags                = "Tags"
	keyStreamingURL        = "StreamingURL"
	keyExpires             = "Expires"
	keyStatus              = "Status"
	keyAppBlockArn         = "AppBlockArn"
	keyFleetName           = "FleetName"
	associationStateActive = "ASSOCIATED"
)

// Handler serves AppStream 2.0 JSON operations.
type Handler struct {
	Backend StorageBackend
	ops     opTable
}

// NewHandler creates an AppStream 2.0 handler backed by b.
func NewHandler(b StorageBackend) *Handler {
	h := &Handler{Backend: b}
	h.ops = h.buildOps()

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return "AppStream" }

// Reset clears backend state.
func (h *Handler) Reset() { h.Backend.Reset() }

// MatchPriority returns header matching priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// RouteMatcher matches AppStream 2.0 requests on either wire protocol: the
// legacy X-Amz-Target header (awsjson1.1, still used by older pinned SDKs,
// the Terraform provider, and gopherstack's own unit tests) or the
// rpc-v2-cbor path used by aws-sdk-go-v2/service/appstream >= v1.64.5.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()
		if isCBORRequest(r) {
			_, ok := h.ops[extractCBOROperation(r.URL.Path)]

			return ok
		}

		return strings.HasPrefix(r.Header.Get("X-Amz-Target"), appstreamTargetPrefix)
	}
}

// ExtractOperation extracts the AppStream action from the CBOR operation
// path or, for the legacy protocol, the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	r := c.Request()
	if isCBORRequest(r) {
		return extractCBOROperation(r.URL.Path)
	}

	target := r.Header.Get("X-Amz-Target")
	if !strings.HasPrefix(target, appstreamTargetPrefix) {
		return "Unknown"
	}

	return strings.TrimPrefix(target, appstreamTargetPrefix)
}

// ExtractResource returns an empty string (AppStream identifies resources by name in body).
func (h *Handler) ExtractResource(_ *echo.Context) string { return "" }

// GetSupportedOperations returns all implemented operation names.
func (h *Handler) GetSupportedOperations() []string {
	ops := make([]string, 0, len(h.ops))
	for name := range h.ops {
		ops = append(ops, name)
	}

	return ops
}

// Snapshot returns a serialized snapshot of the backend state.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore restores the backend state from a snapshot.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}

// Handler returns the Echo handler function. It dispatches rpc-v2-cbor
// requests to handleCBOR and everything else through the legacy
// X-Amz-Target/awsjson1.1 path.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		if isCBORRequest(c.Request()) {
			return h.handleCBOR(c)
		}

		return service.HandleTarget(
			c, logger.Load(c.Request().Context()), h.Name(), appstreamContentType,
			h.GetSupportedOperations(), h.dispatch, h.handleError,
		)
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
	if !ok {
		return nil, awserr.New("OperationNotPermitted", awserr.ErrInvalidParameter)
	}

	result, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

// errorCodeStatus maps a backend error to the AWS exception name and HTTP
// status used to shape it on the wire. Shared by the JSON (handleError) and
// rpc-v2-cbor (handleCBORError) response paths so both protocols report the
// same error for the same failure.
func (h *Handler) errorCodeStatus(err error) (string, int) {
	switch {
	// ErrFleetNotStopped, ErrAlreadyExists and ErrSerialization must precede
	// their wrapped sentinels.
	case errors.Is(err, ErrFleetNotStopped):
		return errFleetNotStopped, http.StatusBadRequest
	case errors.Is(err, ErrAlreadyExists):
		return errResourceExists, http.StatusBadRequest
	case errors.Is(err, ErrEntitlementAlreadyExists):
		return errEntitlementExists, http.StatusBadRequest
	case errors.Is(err, ErrSerialization):
		return errSerialization, http.StatusBadRequest
	case errors.Is(err, awserr.ErrConflict):
		return errResourceInUse, http.StatusBadRequest
	case errors.Is(err, awserr.ErrNotFound):
		return errResourceNotFound, http.StatusBadRequest
	case errors.Is(err, awserr.ErrInvalidParameter):
		return errInvalidParameter, http.StatusBadRequest
	default:
		return "InternalServiceError", http.StatusInternalServerError
	}
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	code, status := h.errorCodeStatus(err)

	return c.JSON(status, map[string]string{
		"__type":  code,
		"message": err.Error(),
	})
}

// opTable is the op-name -> handler-func map type shared by buildOps and its
// per-family helpers below.
type opTable = map[string]func(context.Context, []byte) (any, error)

// buildOps assembles the full operation-routing table by merging one small
// table per resource family (see the *Ops helpers below), keeping each
// family's registration independently readable/testable instead of one
// long function body.
func (h *Handler) buildOps() opTable {
	ops := make(opTable)

	maps.Copy(ops, h.stackFleetOps())
	maps.Copy(ops, h.appBlockOps())
	maps.Copy(ops, h.applicationOps())
	maps.Copy(ops, h.imageOps())
	maps.Copy(ops, h.miscOps())

	return ops
}

// stackFleetOps covers Stack, Fleet (incl. Fleet-Stack associations), and Tags.
func (h *Handler) stackFleetOps() opTable {
	return opTable{
		// Stack
		"CreateStack":    h.opCreateStack,
		"DescribeStacks": h.opDescribeStacks,
		"UpdateStack":    h.opUpdateStack,
		"DeleteStack":    h.opDeleteStack,

		// Fleet
		"CreateFleet":          h.opCreateFleet,
		"DescribeFleets":       h.opDescribeFleets,
		"UpdateFleet":          h.opUpdateFleet,
		"DeleteFleet":          h.opDeleteFleet,
		"StartFleet":           h.opStartFleet,
		"StopFleet":            h.opStopFleet,
		"AssociateFleet":       h.opAssociateFleet,
		"DisassociateFleet":    h.opDisassociateFleet,
		"ListAssociatedFleets": h.opListAssociatedFleets,
		"ListAssociatedStacks": h.opListAssociatedStacks,

		// Tags
		"TagResource":         h.opTagResource,
		"UntagResource":       h.opUntagResource,
		"ListTagsForResource": h.opListTagsForResource,
	}
}

// appBlockOps covers AppBlock, AppBlockBuilder, and their association ops.
func (h *Handler) appBlockOps() opTable {
	return opTable{
		// AppBlock
		"CreateAppBlock":    h.opCreateAppBlock,
		"DeleteAppBlock":    h.opDeleteAppBlock,
		"DescribeAppBlocks": h.opDescribeAppBlocks,

		// AppBlockBuilder
		"CreateAppBlockBuilder":             h.opCreateAppBlockBuilder,
		"DeleteAppBlockBuilder":             h.opDeleteAppBlockBuilder,
		"DescribeAppBlockBuilders":          h.opDescribeAppBlockBuilders,
		"StartAppBlockBuilder":              h.opStartAppBlockBuilder,
		"StopAppBlockBuilder":               h.opStopAppBlockBuilder,
		"UpdateAppBlockBuilder":             h.opUpdateAppBlockBuilder,
		"CreateAppBlockBuilderStreamingURL": h.opCreateAppBlockBuilderStreamingURL,

		// AppBlockBuilder-AppBlock associations
		"AssociateAppBlockBuilderAppBlock":            h.opAssociateAppBlockBuilderAppBlock,
		"DisassociateAppBlockBuilderAppBlock":         h.opDisassociateAppBlockBuilderAppBlock,
		"DescribeAppBlockBuilderAppBlockAssociations": h.opDescribeAppBlockBuilderAppBlockAssociations,
	}
}

// applicationOps covers Application, Application-Fleet associations,
// Entitlement, and DirectoryConfig.
func (h *Handler) applicationOps() opTable {
	return opTable{
		// Application
		"CreateApplication":       h.opCreateApplication,
		"DeleteApplication":       h.opDeleteApplication,
		"DescribeApplications":    h.opDescribeApplications,
		"UpdateApplication":       h.opUpdateApplication,
		"DescribeAppLicenseUsage": h.opDescribeAppLicenseUsage,

		// Application-Fleet associations
		"AssociateApplicationFleet":            h.opAssociateApplicationFleet,
		"DisassociateApplicationFleet":         h.opDisassociateApplicationFleet,
		"DescribeApplicationFleetAssociations": h.opDescribeApplicationFleetAssociations,

		// Entitlement
		"CreateEntitlement":                      h.opCreateEntitlement,
		"DeleteEntitlement":                      h.opDeleteEntitlement,
		"DescribeEntitlements":                   h.opDescribeEntitlements,
		"UpdateEntitlement":                      h.opUpdateEntitlement,
		"AssociateApplicationToEntitlement":      h.opAssociateApplicationToEntitlement,
		"DisassociateApplicationFromEntitlement": h.opDisassociateApplicationFromEntitlement,
		"ListEntitledApplications":               h.opListEntitledApplications,

		// DirectoryConfig
		"CreateDirectoryConfig":    h.opCreateDirectoryConfig,
		"DeleteDirectoryConfig":    h.opDeleteDirectoryConfig,
		"DescribeDirectoryConfigs": h.opDescribeDirectoryConfigs,
		"UpdateDirectoryConfig":    h.opUpdateDirectoryConfig,
	}
}

// imageOps covers Image, ImageBuilder, Software associations, and ExportImageTask.
func (h *Handler) imageOps() opTable {
	return opTable{
		// Image
		"CopyImage":                h.opCopyImage,
		"CreateImportedImage":      h.opCreateImportedImage,
		"CreateUpdatedImage":       h.opCreateUpdatedImage,
		"DeleteImage":              h.opDeleteImage,
		"DescribeImages":           h.opDescribeImages,
		"UpdateImagePermissions":   h.opUpdateImagePermissions,
		"DeleteImagePermissions":   h.opDeleteImagePermissions,
		"DescribeImagePermissions": h.opDescribeImagePermissions,

		// ImageBuilder
		"CreateImageBuilder":             h.opCreateImageBuilder,
		"DeleteImageBuilder":             h.opDeleteImageBuilder,
		"DescribeImageBuilders":          h.opDescribeImageBuilders,
		"StartImageBuilder":              h.opStartImageBuilder,
		"StopImageBuilder":               h.opStopImageBuilder,
		"CreateImageBuilderStreamingURL": h.opCreateImageBuilderStreamingURL,

		// Software associations
		"AssociateSoftwareToImageBuilder":       h.opAssociateSoftwareToImageBuilder,
		"DisassociateSoftwareFromImageBuilder":  h.opDisassociateSoftwareFromImageBuilder,
		"DescribeSoftwareAssociations":          h.opDescribeSoftwareAssociations,
		"StartSoftwareDeploymentToImageBuilder": h.opStartSoftwareDeploymentToImageBuilder,

		// ExportImageTask
		"CreateExportImageTask": h.opCreateExportImageTask,
		"GetExportImageTask":    h.opGetExportImageTask,
		"ListExportImageTasks":  h.opListExportImageTasks,
	}
}

// miscOps covers UsageReport, Theme, User, UserStack associations, and Session.
func (h *Handler) miscOps() opTable {
	return opTable{
		// UsageReport
		"CreateUsageReportSubscription":    h.opCreateUsageReportSubscription,
		"DeleteUsageReportSubscription":    h.opDeleteUsageReportSubscription,
		"DescribeUsageReportSubscriptions": h.opDescribeUsageReportSubscriptions,

		// Theme
		"CreateThemeForStack":   h.opCreateThemeForStack,
		"DeleteThemeForStack":   h.opDeleteThemeForStack,
		"DescribeThemeForStack": h.opDescribeThemeForStack,
		"UpdateThemeForStack":   h.opUpdateThemeForStack,

		// User
		"CreateUser":    h.opCreateUser,
		"DeleteUser":    h.opDeleteUser,
		"DescribeUsers": h.opDescribeUsers,
		"DisableUser":   h.opDisableUser,
		"EnableUser":    h.opEnableUser,

		// UserStack associations
		"BatchAssociateUserStack":       h.opBatchAssociateUserStack,
		"BatchDisassociateUserStack":    h.opBatchDisassociateUserStack,
		"DescribeUserStackAssociations": h.opDescribeUserStackAssociations,

		// Session
		"DescribeSessions":     h.opDescribeSessions,
		"DrainSessionInstance": h.opDrainSessionInstance,
		"ExpireSession":        h.opExpireSession,
		"CreateStreamingURL":   h.opCreateStreamingURL,
	}
}

// --- Stack handlers ---

// userSettingJSON mirrors appstream@v1.64.5 types.UserSetting's wire shape.
type userSettingJSON struct {
	Action        string `json:"Action"`
	Permission    string `json:"Permission"`
	MaximumLength int    `json:"MaximumLength"`
}

func toUserSettings(in []userSettingJSON) []UserSetting {
	out := make([]UserSetting, len(in))
	for i, u := range in {
		out[i] = UserSetting(u)
	}

	return out
}

// applicationSettingsJSON mirrors appstream@v1.64.5 types.ApplicationSettings's
// (request-side) wire shape.
type applicationSettingsJSON struct {
	SettingsGroup string `json:"SettingsGroup"`
	Enabled       bool   `json:"Enabled"`
}

func (j *applicationSettingsJSON) toModel() *ApplicationSettings {
	if j == nil {
		return nil
	}

	return &ApplicationSettings{Enabled: j.Enabled, SettingsGroup: j.SettingsGroup}
}

// accessEndpointJSON mirrors appstream@v1.64.5 types.AccessEndpoint's wire shape.
type accessEndpointJSON struct {
	EndpointType string `json:"EndpointType"`
	VpceId       string `json:"VpceId"` //nolint:revive,staticcheck // matches real SDK field name.
}

func toAccessEndpoints(in []accessEndpointJSON) []AccessEndpoint {
	out := make([]AccessEndpoint, len(in))
	for i, e := range in {
		out[i] = AccessEndpoint{EndpointType: e.EndpointType, VpceID: e.VpceId}
	}

	return out
}

func accessEndpointsToJSON(in []AccessEndpoint) []map[string]any {
	out := make([]map[string]any, len(in))
	for i, e := range in {
		out[i] = map[string]any{"EndpointType": e.EndpointType, "VpceId": e.VpceID}
	}

	return out
}

// storageConnectorJSON mirrors appstream@v1.64.5 types.StorageConnector's wire shape.
type storageConnectorJSON struct {
	ConnectorType              string   `json:"ConnectorType"`
	ResourceIdentifier         string   `json:"ResourceIdentifier"`
	Domains                    []string `json:"Domains"`
	DomainsRequireAdminConsent []string `json:"DomainsRequireAdminConsent"`
}

func toStorageConnectors(in []storageConnectorJSON) []StorageConnector {
	out := make([]StorageConnector, len(in))
	for i, c := range in {
		out[i] = StorageConnector(c)
	}

	return out
}

func storageConnectorsToJSON(in []StorageConnector) []map[string]any {
	out := make([]map[string]any, len(in))
	for i, c := range in {
		out[i] = map[string]any{
			"ConnectorType":              c.ConnectorType,
			"ResourceIdentifier":         c.ResourceIdentifier,
			"Domains":                    c.Domains,
			"DomainsRequireAdminConsent": c.DomainsRequireAdminConsent,
		}
	}

	return out
}

// streamingExperienceSettingsJSON mirrors appstream@v1.64.5
// types.StreamingExperienceSettings's wire shape.
type streamingExperienceSettingsJSON struct {
	PreferredProtocol string `json:"PreferredProtocol"`
}

func (j *streamingExperienceSettingsJSON) toModel() *StreamingExperienceSettings {
	if j == nil {
		return nil
	}

	return &StreamingExperienceSettings{PreferredProtocol: j.PreferredProtocol}
}

// urlRedirectionConfigJSON mirrors appstream@v1.64.5 types.UrlRedirectionConfig's
// wire shape.
type urlRedirectionConfigJSON struct {
	Enabled     *bool    `json:"Enabled"`
	AllowedUrls []string `json:"AllowedUrls"`
	DeniedUrls  []string `json:"DeniedUrls"`
}

// contentRedirectionJSON mirrors appstream@v1.64.5 types.ContentRedirection's
// wire shape.
type contentRedirectionJSON struct {
	HostToClient *urlRedirectionConfigJSON `json:"HostToClient"`
}

func (j *contentRedirectionJSON) toModel() *ContentRedirection {
	if j == nil {
		return nil
	}

	cr := &ContentRedirection{}
	if j.HostToClient != nil {
		cr.HostToClient = &UrlRedirectionConfig{
			Enabled:     j.HostToClient.Enabled,
			AllowedUrls: j.HostToClient.AllowedUrls,
			DeniedUrls:  j.HostToClient.DeniedUrls,
		}
	}

	return cr
}

type createStackInput struct {
	Tags                        map[string]string                `json:"Tags"`
	ApplicationSettings         *applicationSettingsJSON         `json:"ApplicationSettings"`
	ContentRedirection          *contentRedirectionJSON          `json:"ContentRedirection"`
	StreamingExperienceSettings *streamingExperienceSettingsJSON `json:"StreamingExperienceSettings"`
	Name                        string                           `json:"Name"`
	DisplayName                 string                           `json:"DisplayName"`
	Description                 string                           `json:"Description"`
	RedirectURL                 string                           `json:"RedirectURL"`
	FeedbackURL                 string                           `json:"FeedbackURL"`
	EmbedHostDomains            []string                         `json:"EmbedHostDomains"`
	UserSettings                []userSettingJSON                `json:"UserSettings"`
	StorageConnectors           []storageConnectorJSON           `json:"StorageConnectors"`
	AccessEndpoints             []accessEndpointJSON             `json:"AccessEndpoints"`
}

func (h *Handler) opCreateStack(_ context.Context, body []byte) (any, error) {
	var req createStackInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	stack, err := h.Backend.CreateStack(req.Name, CreateStackOptions{
		Tags:                        req.Tags,
		DisplayName:                 req.DisplayName,
		Description:                 req.Description,
		RedirectURL:                 req.RedirectURL,
		FeedbackURL:                 req.FeedbackURL,
		EmbedHostDomains:            req.EmbedHostDomains,
		UserSettings:                toUserSettings(req.UserSettings),
		StorageConnectors:           toStorageConnectors(req.StorageConnectors),
		AccessEndpoints:             toAccessEndpoints(req.AccessEndpoints),
		ApplicationSettings:         req.ApplicationSettings.toModel(),
		ContentRedirection:          req.ContentRedirection.toModel(),
		StreamingExperienceSettings: req.StreamingExperienceSettings.toModel(),
	})
	if err != nil {
		return nil, err
	}

	return map[string]any{"Stack": stackToResponse(stack)}, nil
}

type describeStacksInput struct {
	Names []string `json:"Names"`
}

func (h *Handler) opDescribeStacks(_ context.Context, body []byte) (any, error) {
	var req describeStacksInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
		}
	}

	stacks, err := h.Backend.DescribeStacks(req.Names)
	if err != nil {
		return nil, err
	}

	resp := make([]any, 0, len(stacks))
	for _, s := range stacks {
		resp = append(resp, stackToResponse(s))
	}

	return map[string]any{"Stacks": resp}, nil
}

type updateStackInput struct {
	ApplicationSettings         *applicationSettingsJSON         `json:"ApplicationSettings"`
	ContentRedirection          *contentRedirectionJSON          `json:"ContentRedirection"`
	StreamingExperienceSettings *streamingExperienceSettingsJSON `json:"StreamingExperienceSettings"`
	DeleteStorageConnectors     *bool                            `json:"DeleteStorageConnectors"`
	Name                        string                           `json:"Name"`
	DisplayName                 string                           `json:"DisplayName"`
	Description                 string                           `json:"Description"`
	RedirectURL                 string                           `json:"RedirectURL"`
	FeedbackURL                 string                           `json:"FeedbackURL"`
	EmbedHostDomains            []string                         `json:"EmbedHostDomains"`
	UserSettings                []userSettingJSON                `json:"UserSettings"`
	StorageConnectors           []storageConnectorJSON           `json:"StorageConnectors"`
	AccessEndpoints             []accessEndpointJSON             `json:"AccessEndpoints"`
	AttributesToDelete          []string                         `json:"AttributesToDelete"`
}

func (h *Handler) opUpdateStack(_ context.Context, body []byte) (any, error) {
	var req updateStackInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	stack, err := h.Backend.UpdateStack(req.Name, UpdateStackOptions{
		DeleteStorageConnectors:     req.DeleteStorageConnectors,
		RedirectURL:                 req.RedirectURL,
		FeedbackURL:                 req.FeedbackURL,
		DisplayName:                 req.DisplayName,
		Description:                 req.Description,
		EmbedHostDomains:            req.EmbedHostDomains,
		UserSettings:                toUserSettings(req.UserSettings),
		StorageConnectors:           toStorageConnectors(req.StorageConnectors),
		AccessEndpoints:             toAccessEndpoints(req.AccessEndpoints),
		AttributesToDelete:          req.AttributesToDelete,
		ApplicationSettings:         req.ApplicationSettings.toModel(),
		ContentRedirection:          req.ContentRedirection.toModel(),
		StreamingExperienceSettings: req.StreamingExperienceSettings.toModel(),
	})
	if err != nil {
		return nil, err
	}

	return map[string]any{"Stack": stackToResponse(stack)}, nil
}

type deleteStackInput struct {
	Name string `json:"Name"`
}

func (h *Handler) opDeleteStack(_ context.Context, body []byte) (any, error) {
	var req deleteStackInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.DeleteStack(req.Name); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// --- Fleet handlers ---

type computeCapacityInput struct {
	DesiredInstances int `json:"DesiredInstances"`
}

// vpcConfigJSON mirrors appstream@v1.64.5 types.VpcConfig's wire shape.
type vpcConfigJSON struct {
	SecurityGroupIDs []string `json:"SecurityGroupIds"`
	SubnetIDs        []string `json:"SubnetIds"`
}

func (j *vpcConfigJSON) toModel() VpcConfig {
	if j == nil {
		return VpcConfig{}
	}

	return VpcConfig{SecurityGroupIDs: j.SecurityGroupIDs, SubnetIDs: j.SubnetIDs}
}

func vpcConfigToJSON(v VpcConfig) map[string]any {
	return map[string]any{"SecurityGroupIds": v.SecurityGroupIDs, "SubnetIds": v.SubnetIDs}
}

// domainJoinInfoJSON mirrors appstream@v1.64.5 types.DomainJoinInfo's wire shape.
type domainJoinInfoJSON struct {
	DirectoryName                       string `json:"DirectoryName"`
	OrganizationalUnitDistinguishedName string `json:"OrganizationalUnitDistinguishedName"`
}

func (j *domainJoinInfoJSON) toModel() DomainJoinInfo {
	if j == nil {
		return DomainJoinInfo{}
	}

	return DomainJoinInfo(*j)
}

func domainJoinInfoToJSON(d DomainJoinInfo) map[string]any {
	return map[string]any{
		"DirectoryName":                       d.DirectoryName,
		"OrganizationalUnitDistinguishedName": d.OrganizationalUnitDistinguishedName,
	}
}

// volumeConfigJSON mirrors appstream@v1.64.5 types.VolumeConfig's wire shape.
type volumeConfigJSON struct {
	VolumeSizeInGb int `json:"VolumeSizeInGb"`
}

func (j *volumeConfigJSON) toModel() *VolumeConfig {
	if j == nil {
		return nil
	}

	return &VolumeConfig{VolumeSizeInGb: j.VolumeSizeInGb}
}

type createFleetInput struct {
	Tags                           map[string]string     `json:"Tags"`
	ComputeCapacity                *computeCapacityInput `json:"ComputeCapacity"`
	EnableDefaultInternetAccess    *bool                 `json:"EnableDefaultInternetAccess"`
	DisableIMDSV1                  *bool                 `json:"DisableIMDSV1"`
	VpcConfig                      *vpcConfigJSON        `json:"VpcConfig"`
	DomainJoinInfo                 *domainJoinInfoJSON   `json:"DomainJoinInfo"`
	RootVolumeConfig               *volumeConfigJSON     `json:"RootVolumeConfig"`
	SessionScriptS3Location        *s3LocationJSON       `json:"SessionScriptS3Location"`
	Name                           string                `json:"Name"`
	DisplayName                    string                `json:"DisplayName"`
	Description                    string                `json:"Description"`
	InstanceType                   string                `json:"InstanceType"`
	FleetType                      string                `json:"FleetType"`
	ImageName                      string                `json:"ImageName"`
	ImageArn                       string                `json:"ImageArn"`
	IamRoleArn                     string                `json:"IamRoleArn"`
	StreamView                     string                `json:"StreamView"`
	Platform                       string                `json:"Platform"`
	UsbDeviceFilterStrings         []string              `json:"UsbDeviceFilterStrings"`
	MaxUserDurationInSeconds       int                   `json:"MaxUserDurationInSeconds"`
	DisconnectTimeoutInSeconds     int                   `json:"DisconnectTimeoutInSeconds"`
	IdleDisconnectTimeoutInSeconds int                   `json:"IdleDisconnectTimeoutInSeconds"`
	MaxSessionsPerInstance         int                   `json:"MaxSessionsPerInstance"`
	MaxConcurrentSessions          int                   `json:"MaxConcurrentSessions"`
}

func (h *Handler) opCreateFleet(_ context.Context, body []byte) (any, error) {
	var req createFleetInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	desired := 0
	if req.ComputeCapacity != nil {
		desired = req.ComputeCapacity.DesiredInstances
	}

	fleet, err := h.Backend.CreateFleet(req.Name, CreateFleetOptions{
		Tags:                        req.Tags,
		EnableDefaultInternetAccess: req.EnableDefaultInternetAccess,
		DisableIMDSV1:               req.DisableIMDSV1,
		VpcConfig:                   req.VpcConfig.toModel(),
		DomainJoinInfo:              req.DomainJoinInfo.toModel(),
		RootVolumeConfig:            req.RootVolumeConfig.toModel(),
		SessionScriptS3Location:     req.SessionScriptS3Location.toModel(),
		UsbDeviceFilterStrings:      req.UsbDeviceFilterStrings,
		DisplayName:                 req.DisplayName,
		Description:                 req.Description,
		InstanceType:                req.InstanceType,
		FleetType:                   req.FleetType,
		ImageName:                   req.ImageName,
		ImageArn:                    req.ImageArn,
		IamRoleArn:                  req.IamRoleArn,
		StreamView:                  req.StreamView,
		Platform:                    req.Platform,
		DesiredInstances:            desired,
		MaxUserDurationSecs:         req.MaxUserDurationInSeconds,
		DisconnectTimeoutSecs:       req.DisconnectTimeoutInSeconds,
		IdleDisconnectTimeoutSecs:   req.IdleDisconnectTimeoutInSeconds,
		MaxSessionsPerInstance:      req.MaxSessionsPerInstance,
		MaxConcurrentSessions:       req.MaxConcurrentSessions,
	})
	if err != nil {
		return nil, err
	}

	return map[string]any{"Fleet": fleetToResponse(fleet)}, nil
}

type describeFleetsInput struct {
	Names []string `json:"Names"`
}

func (h *Handler) opDescribeFleets(_ context.Context, body []byte) (any, error) {
	var req describeFleetsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
		}
	}

	fleets, err := h.Backend.DescribeFleets(req.Names)
	if err != nil {
		return nil, err
	}

	resp := make([]any, 0, len(fleets))
	for _, f := range fleets {
		resp = append(resp, fleetToResponse(f))
	}

	return map[string]any{"Fleets": resp}, nil
}

type updateFleetInput struct {
	ComputeCapacity                *computeCapacityInput `json:"ComputeCapacity"`
	EnableDefaultInternetAccess    *bool                 `json:"EnableDefaultInternetAccess"`
	DisableIMDSV1                  *bool                 `json:"DisableIMDSV1"`
	VpcConfig                      *vpcConfigJSON        `json:"VpcConfig"`
	DomainJoinInfo                 *domainJoinInfoJSON   `json:"DomainJoinInfo"`
	RootVolumeConfig               *volumeConfigJSON     `json:"RootVolumeConfig"`
	SessionScriptS3Location        *s3LocationJSON       `json:"SessionScriptS3Location"`
	Name                           string                `json:"Name"`
	DisplayName                    string                `json:"DisplayName"`
	Description                    string                `json:"Description"`
	InstanceType                   string                `json:"InstanceType"`
	ImageName                      string                `json:"ImageName"`
	ImageArn                       string                `json:"ImageArn"`
	IamRoleArn                     string                `json:"IamRoleArn"`
	StreamView                     string                `json:"StreamView"`
	Platform                       string                `json:"Platform"`
	UsbDeviceFilterStrings         []string              `json:"UsbDeviceFilterStrings"`
	AttributesToDelete             []string              `json:"AttributesToDelete"`
	MaxUserDurationInSeconds       int                   `json:"MaxUserDurationInSeconds"`
	DisconnectTimeoutInSeconds     int                   `json:"DisconnectTimeoutInSeconds"`
	IdleDisconnectTimeoutInSeconds int                   `json:"IdleDisconnectTimeoutInSeconds"`
	MaxSessionsPerInstance         int                   `json:"MaxSessionsPerInstance"`
	MaxConcurrentSessions          int                   `json:"MaxConcurrentSessions"`
}

func (h *Handler) opUpdateFleet(_ context.Context, body []byte) (any, error) {
	var req updateFleetInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	desired := 0
	if req.ComputeCapacity != nil {
		desired = req.ComputeCapacity.DesiredInstances
	}

	fleet, err := h.Backend.UpdateFleet(req.Name, UpdateFleetOptions{
		EnableDefaultInternetAccess: req.EnableDefaultInternetAccess,
		DisableIMDSV1:               req.DisableIMDSV1,
		VpcConfig:                   req.VpcConfig.toModel(),
		DomainJoinInfo:              req.DomainJoinInfo.toModel(),
		RootVolumeConfig:            req.RootVolumeConfig.toModel(),
		SessionScriptS3Location:     req.SessionScriptS3Location.toModel(),
		UsbDeviceFilterStrings:      req.UsbDeviceFilterStrings,
		AttributesToDelete:          req.AttributesToDelete,
		DisplayName:                 req.DisplayName,
		Description:                 req.Description,
		InstanceType:                req.InstanceType,
		ImageName:                   req.ImageName,
		ImageArn:                    req.ImageArn,
		IamRoleArn:                  req.IamRoleArn,
		StreamView:                  req.StreamView,
		Platform:                    req.Platform,
		DesiredInstances:            desired,
		MaxUserDurationSecs:         req.MaxUserDurationInSeconds,
		DisconnectTimeoutSecs:       req.DisconnectTimeoutInSeconds,
		IdleDisconnectTimeoutSecs:   req.IdleDisconnectTimeoutInSeconds,
		MaxSessionsPerInstance:      req.MaxSessionsPerInstance,
		MaxConcurrentSessions:       req.MaxConcurrentSessions,
	})
	if err != nil {
		return nil, err
	}

	return map[string]any{"Fleet": fleetToResponse(fleet)}, nil
}

type deleteFleetInput struct {
	Name string `json:"Name"`
}

func (h *Handler) opDeleteFleet(_ context.Context, body []byte) (any, error) {
	var req deleteFleetInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.DeleteFleet(req.Name); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

type fleetNameInput struct {
	Name string `json:"Name"`
}

func (h *Handler) opStartFleet(_ context.Context, body []byte) (any, error) {
	var req fleetNameInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.StartFleet(req.Name); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) opStopFleet(_ context.Context, body []byte) (any, error) {
	var req fleetNameInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.StopFleet(req.Name); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// --- Association handlers ---

type associateFleetInput struct {
	FleetName string `json:"FleetName"`
	StackName string `json:"StackName"`
}

func (h *Handler) opAssociateFleet(_ context.Context, body []byte) (any, error) {
	var req associateFleetInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.AssociateFleet(req.FleetName, req.StackName); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) opDisassociateFleet(_ context.Context, body []byte) (any, error) {
	var req associateFleetInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.DisassociateFleet(req.FleetName, req.StackName); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

type listAssociatedFleetsInput struct {
	StackName string `json:"StackName"`
}

func (h *Handler) opListAssociatedFleets(_ context.Context, body []byte) (any, error) {
	var req listAssociatedFleetsInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	names, err := h.Backend.ListAssociatedFleets(req.StackName)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Names": names}, nil
}

type listAssociatedStacksInput struct {
	FleetName string `json:"FleetName"`
}

func (h *Handler) opListAssociatedStacks(_ context.Context, body []byte) (any, error) {
	var req listAssociatedStacksInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	names, err := h.Backend.ListAssociatedStacks(req.FleetName)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Names": names}, nil
}

// --- Tag handlers ---

type tagResourceInput struct {
	Tags        map[string]string `json:"Tags"`
	ResourceArn string            `json:"ResourceArn"`
}

func (h *Handler) opTagResource(_ context.Context, body []byte) (any, error) {
	var req tagResourceInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.TagResource(req.ResourceArn, req.Tags); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// untagResourceInput lists string before slice to satisfy fieldalignment.
type untagResourceInput struct {
	ResourceArn string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

func (h *Handler) opUntagResource(_ context.Context, body []byte) (any, error) {
	var req untagResourceInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.UntagResource(req.ResourceArn, req.TagKeys); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

type listTagsForResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) opListTagsForResource(_ context.Context, body []byte) (any, error) {
	var req listTagsForResourceInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	tags, err := h.Backend.ListTagsForResource(req.ResourceArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyTags: tags}, nil
}

// --- Response helpers ---

// stackToResponse builds the real Stack wire shape. Real deserializeCBOR_Stack
// (appstream@v1.64.5 deserializers.go) has no Tags key -- unlike its sibling
// members here, this backend never emitted one for Stack.
func stackToResponse(s *Stack) map[string]any {
	resp := map[string]any{
		"Name":        s.Name,               //nolint:goconst // existing issue.
		"Arn":         s.Arn,                //nolint:goconst // existing issue.
		"DisplayName": s.DisplayName,        //nolint:goconst // existing issue.
		"Description": s.Description,        //nolint:goconst // existing issue.
		"CreatedTime": s.CreatedTime.Unix(), //nolint:goconst // existing issue.
	}

	if s.RedirectURL != "" {
		resp["RedirectURL"] = s.RedirectURL
	}

	if s.FeedbackURL != "" {
		resp["FeedbackURL"] = s.FeedbackURL
	}

	if len(s.EmbedHostDomains) > 0 {
		resp["EmbedHostDomains"] = s.EmbedHostDomains
	}

	if len(s.UserSettings) > 0 {
		settings := make([]map[string]any, len(s.UserSettings))

		for i, us := range s.UserSettings {
			entry := map[string]any{"Action": us.Action, "Permission": us.Permission}
			if us.MaximumLength > 0 {
				entry["MaximumLength"] = us.MaximumLength
			}

			settings[i] = entry
		}

		resp["UserSettings"] = settings
	}

	if len(s.StorageConnectors) > 0 {
		resp["StorageConnectors"] = storageConnectorsToJSON(s.StorageConnectors)
	}

	if len(s.AccessEndpoints) > 0 {
		resp["AccessEndpoints"] = accessEndpointsToJSON(s.AccessEndpoints)
	}

	if s.ApplicationSettings != nil {
		resp["ApplicationSettings"] = map[string]any{
			"Enabled":       s.ApplicationSettings.Enabled, //nolint:goconst // existing issue.
			"SettingsGroup": s.ApplicationSettings.SettingsGroup,
			"S3BucketName":  s.ApplicationSettings.S3BucketName, //nolint:goconst // existing issue.
		}
	}

	if s.StreamingExperienceSettings != nil {
		resp["StreamingExperienceSettings"] = map[string]any{
			"PreferredProtocol": s.StreamingExperienceSettings.PreferredProtocol,
		}
	}

	if s.ContentRedirection != nil {
		resp["ContentRedirection"] = contentRedirectionToJSON(s.ContentRedirection)
	}

	return resp
}

func contentRedirectionToJSON(cr *ContentRedirection) map[string]any {
	out := map[string]any{}
	if cr.HostToClient == nil {
		return out
	}

	htc := map[string]any{
		"AllowedUrls": cr.HostToClient.AllowedUrls,
		"DeniedUrls":  cr.HostToClient.DeniedUrls,
	}
	if cr.HostToClient.Enabled != nil {
		htc["Enabled"] = *cr.HostToClient.Enabled
	}

	out["HostToClient"] = htc

	return out
}

func fleetToResponse(f *Fleet) map[string]any {
	resp := map[string]any{
		"Name":                           f.Name,
		"Arn":                            f.Arn,
		"DisplayName":                    f.DisplayName,
		"Description":                    f.Description,
		"InstanceType":                   f.InstanceType, //nolint:goconst // existing issue.
		"FleetType":                      f.FleetType,
		"State":                          f.State, //nolint:goconst // existing issue.
		"MaxUserDurationInSeconds":       f.MaxUserDurationSecs,
		"DisconnectTimeoutInSeconds":     f.DisconnectTimeoutSecs,
		"IdleDisconnectTimeoutInSeconds": f.IdleDisconnectTimeoutSecs,
		"CreatedTime":                    f.CreatedTime.Unix(),
		"ComputeCapacityStatus": map[string]any{
			"Desired":   f.DesiredInstances,
			"Running":   0,
			"InUse":     0,
			"Available": f.DesiredInstances,
		},
	}

	if f.ImageName != "" {
		resp["ImageName"] = f.ImageName
	}

	if f.ImageArn != "" {
		resp["ImageArn"] = f.ImageArn
	}

	if f.EnableDefaultInternetAccess != nil {
		resp["EnableDefaultInternetAccess"] = *f.EnableDefaultInternetAccess
	}

	addOptionalFleetFields(resp, f)

	return resp
}

// addOptionalFleetFields sets every Fleet response member gopherstack now
// wires through that real AWS models as optional (the deserializer's Nil
// check on that key) -- split out of fleetToResponse to keep both functions
// under this repo's cyclop/gocognit budgets.
func addOptionalFleetFields(resp map[string]any, f *Fleet) {
	if f.DisableIMDSV1 != nil {
		resp["DisableIMDSV1"] = *f.DisableIMDSV1
	}

	if len(f.VpcConfig.SecurityGroupIDs) > 0 || len(f.VpcConfig.SubnetIDs) > 0 {
		resp["VpcConfig"] = vpcConfigToJSON(f.VpcConfig)
	}

	if f.IamRoleArn != "" {
		resp["IamRoleArn"] = f.IamRoleArn
	}

	if f.StreamView != "" {
		resp["StreamView"] = f.StreamView
	}

	if f.Platform != "" {
		resp["Platform"] = f.Platform
	}

	if f.MaxConcurrentSessions > 0 {
		resp["MaxConcurrentSessions"] = f.MaxConcurrentSessions
	}

	if len(f.UsbDeviceFilterStrings) > 0 {
		resp["UsbDeviceFilterStrings"] = f.UsbDeviceFilterStrings
	}

	if f.SessionScriptS3Location.S3Bucket != "" {
		resp["SessionScriptS3Location"] = s3LocationToJSON(f.SessionScriptS3Location)
	}

	if f.MaxSessionsPerInstance > 0 {
		resp["MaxSessionsPerInstance"] = f.MaxSessionsPerInstance
	}

	if f.RootVolumeConfig != nil {
		resp["RootVolumeConfig"] = map[string]any{"VolumeSizeInGb": f.RootVolumeConfig.VolumeSizeInGb}
	}

	if f.DomainJoinInfo.DirectoryName != "" || f.DomainJoinInfo.OrganizationalUnitDistinguishedName != "" {
		resp["DomainJoinInfo"] = domainJoinInfoToJSON(f.DomainJoinInfo)
	}
}
