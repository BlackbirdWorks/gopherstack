package transfer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	keyTypeField    = "__type"
	keyMessageField = "message"
)

const transferTargetPrefix = "TransferService."

const (
	keyDescription      = "Description"
	keyStatus           = "Status"
	keyWorkflowID       = "WorkflowId"
	keyConnectorID      = "ConnectorId"
	keyURL              = "Url"
	keyTransferID       = "TransferId"
	keyStepType         = "Type"
	keyStepName         = "Name"
	keySourceFileLoc    = "SourceFileLocation"
	keyLocalProfileID   = "LocalProfileId"
	keyPartnerProfileID = "PartnerProfileId"
	keyArn              = "Arn"
	keyTags             = "Tags"
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the Echo HTTP handler for AWS Transfer Family operations.
type Handler struct {
	Backend StorageBackend
	ops     map[string]service.JSONOpFunc
}

// NewHandler creates a new Transfer handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// Reset clears the handler state and rebuilds the dispatch table.
func (h *Handler) Reset() {
	h.Backend.Reset()
	h.ops = h.buildOps()
}

// Name returns the service name.
func (h *Handler) Name() string { return "Transfer" }

// GetSupportedOperations returns the list of supported Transfer operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateAccess",
		"CreateAgreement",
		"CreateConnector",
		"CreateProfile",
		"CreateServer",
		"CreateUser",
		"CreateWebApp",
		"CreateWorkflow",
		"DeleteAccess",
		"DeleteAgreement",
		"DeleteCertificate",
		"DeleteConnector",
		"DeleteHostKey",
		"DeleteProfile",
		"DeleteServer",
		"DeleteSshPublicKey",
		"DeleteUser",
		"DeleteWebApp",
		"DeleteWebAppCustomization",
		"DeleteWorkflow",
		"DescribeAccess",
		"DescribeAgreement",
		"DescribeCertificate",
		"DescribeConnector",
		"DescribeExecution",
		"DescribeHostKey",
		"DescribeProfile",
		"DescribeSecurityPolicy",
		"DescribeServer",
		"DescribeUser",
		"DescribeWebApp",
		"DescribeWebAppCustomization",
		"DescribeWorkflow",
		"ImportCertificate",
		"ImportHostKey",
		"ImportSshPublicKey",
		"ListAccesses",
		"ListAgreements",
		"ListCertificates",
		"ListConnectors",
		"ListExecutions",
		"ListFileTransferResults",
		"ListHostKeys",
		"ListProfiles",
		"ListSecurityPolicies",
		"ListServers",
		"ListTagsForResource",
		"ListUsers",
		"ListWebApps",
		"ListWorkflows",
		"SendWorkflowStepState",
		"StartDirectoryListing",
		"StartFileTransfer",
		"StartRemoteDelete",
		"StartRemoteMove",
		"StartServer",
		"StopServer",
		"TagResource",
		"TestConnection",
		"TestIdentityProvider",
		"UntagResource",
		"UpdateAccess",
		"UpdateAgreement",
		"UpdateCertificate",
		"UpdateConnector",
		"UpdateHostKey",
		"UpdateProfile",
		"UpdateServer",
		"UpdateUser",
		"UpdateWebApp",
		"UpdateWebAppCustomization",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "transfer" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Transfer instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function that matches Transfer API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), transferTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the Transfer action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, transferTargetPrefix)

	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

// ExtractResource extracts the resource identifier from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req struct {
		ServerID string `json:"ServerId"`
		UserName string `json:"UserName"`
	}
	_ = json.Unmarshal(body, &req)

	if req.ServerID != "" && req.UserName != "" {
		return req.ServerID + "/" + req.UserName
	}

	return req.ServerID
}

// Handler returns the Echo handler function for Transfer requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"Transfer", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateServer":                service.WrapOp(h.handleCreateServer),
		"DescribeServer":              service.WrapOp(h.handleDescribeServer),
		"ListServers":                 service.WrapOp(h.handleListServers),
		"StartServer":                 service.WrapOp(h.handleStartServer),
		"StopServer":                  service.WrapOp(h.handleStopServer),
		"DeleteServer":                service.WrapOp(h.handleDeleteServer),
		"UpdateServer":                service.WrapOp(h.handleUpdateServer),
		"CreateUser":                  service.WrapOp(h.handleCreateUser),
		"DescribeUser":                service.WrapOp(h.handleDescribeUser),
		"ListUsers":                   service.WrapOp(h.handleListUsers),
		"DeleteUser":                  service.WrapOp(h.handleDeleteUser),
		"UpdateUser":                  service.WrapOp(h.handleUpdateUser),
		"CreateAccess":                service.WrapOp(h.handleCreateAccess),
		"DeleteAccess":                service.WrapOp(h.handleDeleteAccess),
		"DescribeAccess":              service.WrapOp(h.handleDescribeAccess),
		"ListAccesses":                service.WrapOp(h.handleListAccesses),
		"UpdateAccess":                service.WrapOp(h.handleUpdateAccess),
		"CreateAgreement":             service.WrapOp(h.handleCreateAgreement),
		"DeleteAgreement":             service.WrapOp(h.handleDeleteAgreement),
		"DescribeAgreement":           service.WrapOp(h.handleDescribeAgreement),
		"ListAgreements":              service.WrapOp(h.handleListAgreements),
		"UpdateAgreement":             service.WrapOp(h.handleUpdateAgreement),
		"CreateConnector":             service.WrapOp(h.handleCreateConnector),
		"DeleteConnector":             service.WrapOp(h.handleDeleteConnector),
		"DescribeConnector":           service.WrapOp(h.handleDescribeConnector),
		"ListConnectors":              service.WrapOp(h.handleListConnectors),
		"UpdateConnector":             service.WrapOp(h.handleUpdateConnector),
		"CreateProfile":               service.WrapOp(h.handleCreateProfile),
		"DeleteProfile":               service.WrapOp(h.handleDeleteProfile),
		"DescribeProfile":             service.WrapOp(h.handleDescribeProfile),
		"ListProfiles":                service.WrapOp(h.handleListProfiles),
		"UpdateProfile":               service.WrapOp(h.handleUpdateProfile),
		"CreateWebApp":                service.WrapOp(h.handleCreateWebApp),
		"DeleteWebApp":                service.WrapOp(h.handleDeleteWebApp),
		"DescribeWebApp":              service.WrapOp(h.handleDescribeWebApp),
		"ListWebApps":                 service.WrapOp(h.handleListWebApps),
		"UpdateWebApp":                service.WrapOp(h.handleUpdateWebApp),
		"DeleteWebAppCustomization":   service.WrapOp(h.handleDeleteWebAppCustomization),
		"DescribeWebAppCustomization": service.WrapOp(h.handleDescribeWebAppCustomization),
		"UpdateWebAppCustomization":   service.WrapOp(h.handleUpdateWebAppCustomization),
		"CreateWorkflow":              service.WrapOp(h.handleCreateWorkflow),
		"DeleteWorkflow":              service.WrapOp(h.handleDeleteWorkflow),
		"DescribeWorkflow":            service.WrapOp(h.handleDescribeWorkflow),
		"ListWorkflows":               service.WrapOp(h.handleListWorkflows),
		"DeleteCertificate":           service.WrapOp(h.handleDeleteCertificate),
		"ImportCertificate":           service.WrapOp(h.handleImportCertificate),
		"DescribeCertificate":         service.WrapOp(h.handleDescribeCertificate),
		"ListCertificates":            service.WrapOp(h.handleListCertificates),
		"UpdateCertificate":           service.WrapOp(h.handleUpdateCertificate),
		"ImportHostKey":               service.WrapOp(h.handleImportHostKey),
		"DeleteHostKey":               service.WrapOp(h.handleDeleteHostKey),
		"DescribeHostKey":             service.WrapOp(h.handleDescribeHostKey),
		"ListHostKeys":                service.WrapOp(h.handleListHostKeys),
		"UpdateHostKey":               service.WrapOp(h.handleUpdateHostKey),
		"ImportSshPublicKey":          service.WrapOp(h.handleImportSSHPublicKey),
		"DeleteSshPublicKey":          service.WrapOp(h.handleDeleteSSHPublicKey),
		"TagResource":                 service.WrapOp(h.handleTagResource),
		"UntagResource":               service.WrapOp(h.handleUntagResource),
		"ListTagsForResource":         service.WrapOp(h.handleListTagsForResource),
		"DescribeExecution":           service.WrapOp(h.handleDescribeExecution),
		"ListExecutions":              service.WrapOp(h.handleListExecutions),
		"ListFileTransferResults":     service.WrapOp(h.handleListFileTransferResults),
		"DescribeSecurityPolicy":      service.WrapOp(h.handleDescribeSecurityPolicy),
		"ListSecurityPolicies":        service.WrapOp(h.handleListSecurityPolicies),
		"SendWorkflowStepState":       service.WrapOp(h.handleSendWorkflowStepState),
		"StartDirectoryListing":       service.WrapOp(h.handleStartDirectoryListing),
		"StartFileTransfer":           service.WrapOp(h.handleStartFileTransfer),
		"StartRemoteDelete":           service.WrapOp(h.handleStartRemoteDelete),
		"StartRemoteMove":             service.WrapOp(h.handleStartRemoteMove),
		"TestConnection":              service.WrapOp(h.handleTestConnection),
		"TestIdentityProvider":        service.WrapOp(h.handleTestIdentityProvider),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
	}

	result, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    "ResourceNotFoundException",
			keyMessageField: err.Error(),
		})
	case errors.Is(err, awserr.ErrConflict):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    "ResourceExistsException",
			keyMessageField: err.Error(),
		})
	case errors.Is(err, awserr.ErrInvalidParameter),
		errors.Is(err, errInvalidRequest),
		errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr),
		errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    "InvalidRequestException",
			keyMessageField: err.Error(),
		})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{
			keyTypeField:    "InternalServiceError",
			keyMessageField: err.Error(),
		})
	}
}

// --- Server operations ---

type identityProviderDetailsInput struct {
	URL                       string `json:"Url,omitempty"`
	InvocationRole            string `json:"InvocationRole,omitempty"`
	DirectoryID               string `json:"DirectoryId,omitempty"`
	Function                  string `json:"Function,omitempty"`
	SftpAuthenticationMethods string `json:"SftpAuthenticationMethods,omitempty"`
}

type endpointDetailsInput struct {
	VpcEndpointID        string   `json:"VpcEndpointId,omitempty"`
	VpcID                string   `json:"VpcId,omitempty"`
	AddressAllocationIDs []string `json:"AddressAllocationIds,omitempty"`
	SubnetIDs            []string `json:"SubnetIds,omitempty"`
	SecurityGroupIDs     []string `json:"SecurityGroupIds,omitempty"`
}

type protocolDetailsInput struct {
	PassiveIP                string   `json:"PassiveIp,omitempty"`
	TLSSessionResumptionMode string   `json:"TlsSessionResumptionMode,omitempty"`
	SetStatOption            string   `json:"SetStatOption,omitempty"`
	As2Transports            []string `json:"As2Transports,omitempty"`
}

type workflowDetailInput struct {
	WorkflowID    string `json:"WorkflowId"`
	ExecutionRole string `json:"ExecutionRole"`
}

type workflowDetailsInput struct {
	OnUpload        []workflowDetailInput `json:"OnUpload,omitempty"`
	OnPartialUpload []workflowDetailInput `json:"OnPartialUpload,omitempty"`
}

type s3StorageOptionsInput struct {
	DirectoryListingOptimization string `json:"DirectoryListingOptimization,omitempty"`
}

func toIdentityProviderDetails(in *identityProviderDetailsInput) *IdentityProviderDetails {
	if in == nil {
		return nil
	}

	return &IdentityProviderDetails{
		URL:                       in.URL,
		InvocationRole:            in.InvocationRole,
		DirectoryID:               in.DirectoryID,
		Function:                  in.Function,
		SftpAuthenticationMethods: in.SftpAuthenticationMethods,
	}
}

func toEndpointDetails(in *endpointDetailsInput) *EndpointDetails {
	if in == nil {
		return nil
	}

	return &EndpointDetails{
		AddressAllocationIDs: in.AddressAllocationIDs,
		SubnetIDs:            in.SubnetIDs,
		SecurityGroupIDs:     in.SecurityGroupIDs,
		VpcEndpointID:        in.VpcEndpointID,
		VpcID:                in.VpcID,
	}
}

func toProtocolDetails(in *protocolDetailsInput) *ProtocolDetails {
	if in == nil {
		return nil
	}

	return &ProtocolDetails{
		PassiveIP:                in.PassiveIP,
		TLSSessionResumptionMode: in.TLSSessionResumptionMode,
		SetStatOption:            in.SetStatOption,
		As2Transports:            in.As2Transports,
	}
}

func toWorkflowDetails(in *workflowDetailsInput) *WorkflowDetails {
	if in == nil {
		return nil
	}

	toDetails := func(items []workflowDetailInput) []WorkflowDetail {
		if items == nil {
			return nil
		}
		out := make([]WorkflowDetail, len(items))
		for i, d := range items {
			out[i] = WorkflowDetail(d)
		}

		return out
	}

	return &WorkflowDetails{
		OnUpload:        toDetails(in.OnUpload),
		OnPartialUpload: toDetails(in.OnPartialUpload),
	}
}

func toS3StorageOptions(in *s3StorageOptionsInput) *S3StorageOptions {
	if in == nil {
		return nil
	}

	return &S3StorageOptions{
		DirectoryListingOptimization: in.DirectoryListingOptimization,
	}
}

type createServerInput struct {
	IdentityProviderDetails       *identityProviderDetailsInput `json:"IdentityProviderDetails,omitempty"`
	EndpointDetails               *endpointDetailsInput         `json:"EndpointDetails,omitempty"`
	ProtocolDetails               *protocolDetailsInput         `json:"ProtocolDetails,omitempty"`
	WorkflowDetails               *workflowDetailsInput         `json:"WorkflowDetails,omitempty"`
	S3StorageOptions              *s3StorageOptionsInput        `json:"S3StorageOptions,omitempty"`
	IdentityProviderType          string                        `json:"IdentityProviderType,omitempty"`
	EndpointType                  string                        `json:"EndpointType,omitempty"`
	LoggingRole                   string                        `json:"LoggingRole,omitempty"`
	PreAuthenticationLoginBanner  string                        `json:"PreAuthenticationLoginBanner,omitempty"`
	PostAuthenticationLoginBanner string                        `json:"PostAuthenticationLoginBanner,omitempty"`
	HostKey                       string                        `json:"HostKey,omitempty"`
	Certificate                   string                        `json:"Certificate,omitempty"`
	Domain                        string                        `json:"Domain,omitempty"`
	SecurityPolicyName            string                        `json:"SecurityPolicyName,omitempty"`
	IPAddressType                 string                        `json:"IpAddressType,omitempty"`
	StructuredLogDestinations     []string                      `json:"StructuredLogDestinations,omitempty"`
	Tags                          []map[string]string           `json:"Tags"`
	Protocols                     []string                      `json:"Protocols"`
}

type createServerOutput struct {
	ServerID string `json:"ServerId"`
}

func (h *Handler) handleCreateServer(
	_ context.Context,
	in *createServerInput,
) (*createServerOutput, error) {
	tags := tagsFromList(in.Tags)

	s, err := h.Backend.CreateServerFull(&CreateServerInput{
		Protocols:                     in.Protocols,
		Tags:                          tags,
		IdentityProviderType:          in.IdentityProviderType,
		EndpointType:                  in.EndpointType,
		LoggingRole:                   in.LoggingRole,
		PreAuthenticationLoginBanner:  in.PreAuthenticationLoginBanner,
		PostAuthenticationLoginBanner: in.PostAuthenticationLoginBanner,
		HostKey:                       in.HostKey,
		Certificate:                   in.Certificate,
		Domain:                        in.Domain,
		SecurityPolicyName:            in.SecurityPolicyName,
		IPAddressType:                 in.IPAddressType,
		StructuredLogDestinations:     in.StructuredLogDestinations,
		IdentityProviderDetails:       toIdentityProviderDetails(in.IdentityProviderDetails),
		EndpointDetails:               toEndpointDetails(in.EndpointDetails),
		ProtocolDetails:               toProtocolDetails(in.ProtocolDetails),
		WorkflowDetails:               toWorkflowDetails(in.WorkflowDetails),
		S3StorageOptions:              toS3StorageOptions(in.S3StorageOptions),
	})
	if err != nil {
		return nil, err
	}

	return &createServerOutput{ServerID: s.ServerID}, nil
}

type serverIDInput struct {
	ServerID string `json:"ServerId"`
}

type serverView struct {
	IdentityProviderDetails       *identityProviderDetailsView `json:"IdentityProviderDetails,omitempty"`
	EndpointDetails               *endpointDetailsView         `json:"EndpointDetails,omitempty"`
	ProtocolDetails               *protocolDetailsView         `json:"ProtocolDetails,omitempty"`
	WorkflowDetails               *workflowDetailsView         `json:"WorkflowDetails,omitempty"`
	S3StorageOptions              *s3StorageOptionsView        `json:"S3StorageOptions,omitempty"`
	PreAuthenticationLoginBanner  string                       `json:"PreAuthenticationLoginBanner,omitempty"`
	IdentityProviderType          string                       `json:"IdentityProviderType,omitempty"`
	IPAddressType                 string                       `json:"IpAddressType,omitempty"`
	Arn                           string                       `json:"Arn"`
	ServerID                      string                       `json:"ServerId"`
	State                         string                       `json:"State"`
	Domain                        string                       `json:"Domain"`
	SecurityPolicyName            string                       `json:"SecurityPolicyName,omitempty"`
	EndpointType                  string                       `json:"EndpointType,omitempty"`
	LoggingRole                   string                       `json:"LoggingRole,omitempty"`
	Certificate                   string                       `json:"Certificate,omitempty"`
	PostAuthenticationLoginBanner string                       `json:"PostAuthenticationLoginBanner,omitempty"`
	Tags                          []map[string]string          `json:"Tags"`
	Protocols                     []string                     `json:"Protocols"`
	StructuredLogDestinations     []string                     `json:"StructuredLogDestinations,omitempty"`
	UserCount                     int                          `json:"UserCount"`
}

type identityProviderDetailsView struct {
	URL                       string `json:"Url,omitempty"`
	InvocationRole            string `json:"InvocationRole,omitempty"`
	DirectoryID               string `json:"DirectoryId,omitempty"`
	Function                  string `json:"Function,omitempty"`
	SftpAuthenticationMethods string `json:"SftpAuthenticationMethods,omitempty"`
}

type endpointDetailsView struct {
	VpcEndpointID        string   `json:"VpcEndpointId,omitempty"`
	VpcID                string   `json:"VpcId,omitempty"`
	AddressAllocationIDs []string `json:"AddressAllocationIds,omitempty"`
	SubnetIDs            []string `json:"SubnetIds,omitempty"`
	SecurityGroupIDs     []string `json:"SecurityGroupIds,omitempty"`
}

type protocolDetailsView struct {
	PassiveIP                string   `json:"PassiveIp,omitempty"`
	TLSSessionResumptionMode string   `json:"TlsSessionResumptionMode,omitempty"`
	SetStatOption            string   `json:"SetStatOption,omitempty"`
	As2Transports            []string `json:"As2Transports,omitempty"`
}

type workflowDetailView struct {
	WorkflowID    string `json:"WorkflowId"`
	ExecutionRole string `json:"ExecutionRole"`
}

type workflowDetailsView struct {
	OnUpload        []workflowDetailView `json:"OnUpload,omitempty"`
	OnPartialUpload []workflowDetailView `json:"OnPartialUpload,omitempty"`
}

type s3StorageOptionsView struct {
	DirectoryListingOptimization string `json:"DirectoryListingOptimization,omitempty"`
}

type describeServerOutput struct {
	Server serverView `json:"Server"`
}

func (h *Handler) handleDescribeServer(
	_ context.Context,
	in *serverIDInput,
) (*describeServerOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	s, err := h.Backend.DescribeServer(in.ServerID)
	if err != nil {
		return nil, err
	}

	view := toServerView(s, serverARN(s.AccountID, s.Region, s.ServerID))
	view.UserCount = h.Backend.ServerUserCount(in.ServerID)

	return &describeServerOutput{Server: view}, nil
}

type listServersOutput struct {
	NextToken string           `json:"NextToken,omitempty"`
	Servers   []serverListItem `json:"Servers"`
}

type listServersInput struct {
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

type serverListItem struct {
	Arn                  string `json:"Arn"`
	ServerID             string `json:"ServerId"`
	State                string `json:"State"`
	Domain               string `json:"Domain"`
	EndpointType         string `json:"EndpointType,omitempty"`
	IdentityProviderType string `json:"IdentityProviderType,omitempty"`
	UserCount            int    `json:"UserCount"`
}

func (h *Handler) handleListServers(
	_ context.Context,
	in *listServersInput,
) (*listServersOutput, error) {
	servers := h.Backend.ListServers()
	items := make([]serverListItem, 0, len(servers))

	for i := range servers {
		s := &servers[i]
		items = append(items, serverListItem{
			Arn:                  serverARN(s.AccountID, s.Region, s.ServerID),
			ServerID:             s.ServerID,
			State:                s.State,
			Domain:               s.Domain,
			EndpointType:         s.EndpointType,
			IdentityProviderType: s.IdentityProviderType,
			UserCount:            h.Backend.ServerUserCount(s.ServerID),
		})
	}

	items, nextToken := applyNextTokenItems(items, in.NextToken, in.MaxResults)

	return &listServersOutput{Servers: items, NextToken: nextToken}, nil
}

func (h *Handler) handleStartServer(_ context.Context, in *serverIDInput) (*struct{}, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if err := h.Backend.StartServer(in.ServerID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleStopServer(_ context.Context, in *serverIDInput) (*struct{}, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if err := h.Backend.StopServer(in.ServerID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleDeleteServer(_ context.Context, in *serverIDInput) (*struct{}, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteServer(in.ServerID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type updateServerInput struct {
	IdentityProviderDetails       *identityProviderDetailsInput `json:"IdentityProviderDetails,omitempty"`
	EndpointDetails               *endpointDetailsInput         `json:"EndpointDetails,omitempty"`
	ProtocolDetails               *protocolDetailsInput         `json:"ProtocolDetails,omitempty"`
	WorkflowDetails               *workflowDetailsInput         `json:"WorkflowDetails,omitempty"`
	S3StorageOptions              *s3StorageOptionsInput        `json:"S3StorageOptions,omitempty"`
	Certificate                   string                        `json:"Certificate,omitempty"`
	ServerID                      string                        `json:"ServerId"`
	EndpointType                  string                        `json:"EndpointType,omitempty"`
	HostKey                       string                        `json:"HostKey,omitempty"`
	LoggingRole                   string                        `json:"LoggingRole,omitempty"`
	PreAuthenticationLoginBanner  string                        `json:"PreAuthenticationLoginBanner,omitempty"`
	PostAuthenticationLoginBanner string                        `json:"PostAuthenticationLoginBanner,omitempty"`
	SecurityPolicyName            string                        `json:"SecurityPolicyName,omitempty"`
	IPAddressType                 string                        `json:"IpAddressType,omitempty"`
	StructuredLogDestinations     []string                      `json:"StructuredLogDestinations,omitempty"`
	Protocols                     []string                      `json:"Protocols,omitempty"`
}

type updateServerOutput struct {
	ServerID string `json:"ServerId"`
}

func (h *Handler) handleUpdateServer(
	_ context.Context,
	in *updateServerInput,
) (*updateServerOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	s, err := h.Backend.UpdateServerFull(&UpdateServerInput{
		ServerID:                      in.ServerID,
		Protocols:                     in.Protocols,
		Certificate:                   in.Certificate,
		SetCertificate:                in.Certificate != "",
		EndpointType:                  in.EndpointType,
		SetEndpointType:               in.EndpointType != "",
		HostKey:                       in.HostKey,
		SetHostKey:                    in.HostKey != "",
		LoggingRole:                   in.LoggingRole,
		SetLoggingRole:                in.LoggingRole != "",
		PreAuthenticationLoginBanner:  in.PreAuthenticationLoginBanner,
		SetPreAuthBanner:              in.PreAuthenticationLoginBanner != "",
		PostAuthenticationLoginBanner: in.PostAuthenticationLoginBanner,
		SetPostAuthBanner:             in.PostAuthenticationLoginBanner != "",
		SecurityPolicyName:            in.SecurityPolicyName,
		SetSecurityPolicyName:         in.SecurityPolicyName != "",
		IPAddressType:                 in.IPAddressType,
		SetIPAddressType:              in.IPAddressType != "",
		IdentityProviderDetails:       toIdentityProviderDetails(in.IdentityProviderDetails),
		SetIdentityProviderDetails:    in.IdentityProviderDetails != nil,
		EndpointDetails:               toEndpointDetails(in.EndpointDetails),
		SetEndpointDetails:            in.EndpointDetails != nil,
		ProtocolDetails:               toProtocolDetails(in.ProtocolDetails),
		SetProtocolDetails:            in.ProtocolDetails != nil,
		WorkflowDetails:               toWorkflowDetails(in.WorkflowDetails),
		SetWorkflowDetails:            in.WorkflowDetails != nil,
		S3StorageOptions:              toS3StorageOptions(in.S3StorageOptions),
		SetS3StorageOptions:           in.S3StorageOptions != nil,
		StructuredLogDestinations:     in.StructuredLogDestinations,
		SetStructuredLogDestinations:  in.StructuredLogDestinations != nil,
	})
	if err != nil {
		return nil, err
	}

	return &updateServerOutput{ServerID: s.ServerID}, nil
}

// --- User operations ---

type posixProfileInput struct {
	SecondaryGids []int64 `json:"SecondaryGids,omitempty"`
	UID           int64   `json:"Uid"`
	GID           int64   `json:"Gid"`
}

type homeDirectoryMapEntryInput struct {
	Entry  string `json:"Entry"`
	Target string `json:"Target"`
	Type   string `json:"Type,omitempty"`
}

func toPosixProfile(in *posixProfileInput) *PosixProfile {
	if in == nil {
		return nil
	}

	return &PosixProfile{
		UID:           in.UID,
		GID:           in.GID,
		SecondaryGids: in.SecondaryGids,
	}
}

func toHomeDirectoryMappings(in []homeDirectoryMapEntryInput) []HomeDirectoryMapEntry {
	if in == nil {
		return nil
	}

	out := make([]HomeDirectoryMapEntry, len(in))
	for i, e := range in {
		out[i] = HomeDirectoryMapEntry(e)
	}

	return out
}

type createUserInput struct {
	PosixProfile          *posixProfileInput           `json:"PosixProfile,omitempty"`
	ServerID              string                       `json:"ServerId"`
	UserName              string                       `json:"UserName"`
	HomeDir               string                       `json:"HomeDirectory"`
	Role                  string                       `json:"Role"`
	HomeDirectoryType     string                       `json:"HomeDirectoryType,omitempty"`
	Policy                string                       `json:"Policy,omitempty"`
	HomeDirectoryMappings []homeDirectoryMapEntryInput `json:"HomeDirectoryMappings,omitempty"`
	Tags                  []map[string]string          `json:"Tags"`
}

type createUserOutput struct {
	ServerID string `json:"ServerId"`
	UserName string `json:"UserName"`
}

func (h *Handler) handleCreateUser(
	_ context.Context,
	in *createUserInput,
) (*createUserOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.UserName == "" {
		return nil, fmt.Errorf("%w: UserName is required", errInvalidRequest)
	}

	tags := tagsFromList(in.Tags)

	u, err := h.Backend.CreateUserFull(&CreateUserInput{
		ServerID:              in.ServerID,
		UserName:              in.UserName,
		HomeDir:               in.HomeDir,
		Role:                  in.Role,
		HomeDirectoryType:     in.HomeDirectoryType,
		HomeDirectoryMappings: toHomeDirectoryMappings(in.HomeDirectoryMappings),
		Policy:                in.Policy,
		PosixProfile:          toPosixProfile(in.PosixProfile),
		Tags:                  tags,
	})
	if err != nil {
		return nil, err
	}

	return &createUserOutput{ServerID: u.ServerID, UserName: u.UserName}, nil
}

type describeUserInput struct {
	ServerID string `json:"ServerId"`
	UserName string `json:"UserName"`
}

type sshKeyView struct {
	DateImported     string `json:"DateImported"`
	SSHPublicKeyID   string `json:"SshPublicKeyId"`
	SSHPublicKeyBody string `json:"SshPublicKeyBody"`
	KeyType          string `json:"KeyType,omitempty"`
}

type posixProfileView struct {
	SecondaryGids []int64 `json:"SecondaryGids,omitempty"`
	UID           int64   `json:"Uid"`
	GID           int64   `json:"Gid"`
}

type homeDirectoryMapEntryView struct {
	Entry  string `json:"Entry"`
	Target string `json:"Target"`
	Type   string `json:"Type,omitempty"`
}

type userView struct {
	PosixProfile          *posixProfileView           `json:"PosixProfile,omitempty"`
	Arn                   string                      `json:"Arn"`
	UserName              string                      `json:"UserName"`
	HomeDir               string                      `json:"HomeDirectory"`
	Role                  string                      `json:"Role"`
	HomeDirectoryType     string                      `json:"HomeDirectoryType,omitempty"`
	Policy                string                      `json:"Policy,omitempty"`
	SSHPublicKeys         []sshKeyView                `json:"SshPublicKeys,omitempty"`
	HomeDirectoryMappings []homeDirectoryMapEntryView `json:"HomeDirectoryMappings,omitempty"`
	Tags                  []map[string]string         `json:"Tags"`
}

type describeUserOutput struct {
	ServerID string   `json:"ServerId"`
	User     userView `json:"User"`
}

func (h *Handler) handleDescribeUser(
	_ context.Context,
	in *describeUserInput,
) (*describeUserOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.UserName == "" {
		return nil, fmt.Errorf("%w: UserName is required", errInvalidRequest)
	}

	u, err := h.Backend.DescribeUser(in.ServerID, in.UserName)
	if err != nil {
		return nil, err
	}

	// Include SSH public keys.
	sshKeys := h.Backend.ListSSHPublicKeys(in.ServerID, in.UserName)
	keyViews := make([]sshKeyView, len(sshKeys))
	for i, k := range sshKeys {
		keyViews[i] = sshKeyView{
			SSHPublicKeyID:   k.SSHPublicKeyID,
			SSHPublicKeyBody: k.SSHPublicKeyBody,
			DateImported:     k.DateImported.Format(time.RFC3339),
			KeyType:          k.KeyType,
		}
	}

	return &describeUserOutput{
		ServerID: u.ServerID,
		User:     toUserView(u, userARN(u.AccountID, u.Region, u.ServerID, u.UserName), keyViews),
	}, nil
}

type listUsersInput struct {
	ServerID   string `json:"ServerId"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

type userListItem struct {
	Arn      string `json:"Arn"`
	UserName string `json:"UserName"`
	HomeDir  string `json:"HomeDirectory"`
	Role     string `json:"Role"`
}

type listUsersOutput struct {
	ServerID  string         `json:"ServerId"`
	NextToken string         `json:"NextToken,omitempty"`
	Users     []userListItem `json:"Users"`
}

func (h *Handler) handleListUsers(_ context.Context, in *listUsersInput) (*listUsersOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	users, err := h.Backend.ListUsers(in.ServerID)
	if err != nil {
		return nil, err
	}

	items := make([]userListItem, 0, len(users))

	for i := range users {
		u := &users[i]
		items = append(items, userListItem{
			Arn:      userARN(u.AccountID, u.Region, u.ServerID, u.UserName),
			UserName: u.UserName,
			HomeDir:  u.HomeDir,
			Role:     u.Role,
		})
	}

	items, nextToken := applyNextTokenItems(items, in.NextToken, in.MaxResults)

	return &listUsersOutput{ServerID: in.ServerID, Users: items, NextToken: nextToken}, nil
}

func (h *Handler) handleDeleteUser(_ context.Context, in *describeUserInput) (*struct{}, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.UserName == "" {
		return nil, fmt.Errorf("%w: UserName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteUser(in.ServerID, in.UserName); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type updateUserInput struct {
	PosixProfile          *posixProfileInput           `json:"PosixProfile,omitempty"`
	ServerID              string                       `json:"ServerId"`
	UserName              string                       `json:"UserName"`
	HomeDir               string                       `json:"HomeDirectory"`
	Role                  string                       `json:"Role"`
	HomeDirectoryType     string                       `json:"HomeDirectoryType,omitempty"`
	Policy                string                       `json:"Policy,omitempty"`
	HomeDirectoryMappings []homeDirectoryMapEntryInput `json:"HomeDirectoryMappings,omitempty"`
}

type updateUserOutput struct {
	ServerID string `json:"ServerId"`
	UserName string `json:"UserName"`
}

func (h *Handler) handleUpdateUser(
	_ context.Context,
	in *updateUserInput,
) (*updateUserOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.UserName == "" {
		return nil, fmt.Errorf("%w: UserName is required", errInvalidRequest)
	}

	u, err := h.Backend.UpdateUserFull(&UpdateUserInput{
		ServerID:                 in.ServerID,
		UserName:                 in.UserName,
		HomeDir:                  in.HomeDir,
		Role:                     in.Role,
		HomeDirectoryType:        in.HomeDirectoryType,
		SetHomeDirectoryType:     in.HomeDirectoryType != "",
		Policy:                   in.Policy,
		SetPolicy:                in.Policy != "",
		PosixProfile:             toPosixProfile(in.PosixProfile),
		SetPosixProfile:          in.PosixProfile != nil,
		HomeDirectoryMappings:    toHomeDirectoryMappings(in.HomeDirectoryMappings),
		SetHomeDirectoryMappings: in.HomeDirectoryMappings != nil,
	})
	if err != nil {
		return nil, err
	}

	return &updateUserOutput{ServerID: u.ServerID, UserName: u.UserName}, nil
}

// --- View helpers ---

func toServerView(s *Server, arnStr string) serverView {
	v := serverView{
		Arn:                           arnStr,
		ServerID:                      s.ServerID,
		State:                         s.State,
		Protocols:                     s.Protocols,
		Domain:                        s.Domain,
		Tags:                          tagsToList(s.Tags),
		IdentityProviderType:          s.IdentityProviderType,
		EndpointType:                  s.EndpointType,
		LoggingRole:                   s.LoggingRole,
		PreAuthenticationLoginBanner:  s.PreAuthenticationLoginBanner,
		PostAuthenticationLoginBanner: s.PostAuthenticationLoginBanner,
		Certificate:                   s.Certificate,
		SecurityPolicyName:            s.SecurityPolicyName,
		IPAddressType:                 s.IPAddressType,
		StructuredLogDestinations:     s.StructuredLogDestinations,
	}

	if s.IdentityProviderDetails != nil {
		v.IdentityProviderDetails = &identityProviderDetailsView{
			URL:                       s.IdentityProviderDetails.URL,
			InvocationRole:            s.IdentityProviderDetails.InvocationRole,
			DirectoryID:               s.IdentityProviderDetails.DirectoryID,
			Function:                  s.IdentityProviderDetails.Function,
			SftpAuthenticationMethods: s.IdentityProviderDetails.SftpAuthenticationMethods,
		}
	}

	if s.EndpointDetails != nil {
		v.EndpointDetails = &endpointDetailsView{
			AddressAllocationIDs: s.EndpointDetails.AddressAllocationIDs,
			SubnetIDs:            s.EndpointDetails.SubnetIDs,
			SecurityGroupIDs:     s.EndpointDetails.SecurityGroupIDs,
			VpcEndpointID:        s.EndpointDetails.VpcEndpointID,
			VpcID:                s.EndpointDetails.VpcID,
		}
	}

	if s.ProtocolDetails != nil {
		v.ProtocolDetails = &protocolDetailsView{
			PassiveIP:                s.ProtocolDetails.PassiveIP,
			TLSSessionResumptionMode: s.ProtocolDetails.TLSSessionResumptionMode,
			SetStatOption:            s.ProtocolDetails.SetStatOption,
			As2Transports:            s.ProtocolDetails.As2Transports,
		}
	}

	if s.WorkflowDetails != nil {
		toViews := func(wds []WorkflowDetail) []workflowDetailView {
			if wds == nil {
				return nil
			}
			out := make([]workflowDetailView, len(wds))
			for i, d := range wds {
				out[i] = workflowDetailView(d)
			}

			return out
		}
		v.WorkflowDetails = &workflowDetailsView{
			OnUpload:        toViews(s.WorkflowDetails.OnUpload),
			OnPartialUpload: toViews(s.WorkflowDetails.OnPartialUpload),
		}
	}

	if s.S3StorageOptions != nil {
		v.S3StorageOptions = &s3StorageOptionsView{
			DirectoryListingOptimization: s.S3StorageOptions.DirectoryListingOptimization,
		}
	}

	return v
}

func toUserView(u *User, arnStr string, sshKeys []sshKeyView) userView {
	v := userView{
		Arn:               arnStr,
		UserName:          u.UserName,
		HomeDir:           u.HomeDir,
		Role:              u.Role,
		Tags:              tagsToList(u.Tags),
		HomeDirectoryType: u.HomeDirectoryType,
		Policy:            u.Policy,
		SSHPublicKeys:     sshKeys,
	}

	if u.PosixProfile != nil {
		v.PosixProfile = &posixProfileView{
			UID:           u.PosixProfile.UID,
			GID:           u.PosixProfile.GID,
			SecondaryGids: u.PosixProfile.SecondaryGids,
		}
	}

	if u.HomeDirectoryMappings != nil {
		v.HomeDirectoryMappings = make([]homeDirectoryMapEntryView, len(u.HomeDirectoryMappings))
		for i, m := range u.HomeDirectoryMappings {
			v.HomeDirectoryMappings[i] = homeDirectoryMapEntryView(m)
		}
	}

	return v
}

// --- Access operations ---

type createAccessInput struct {
	PosixProfile          *posixProfileInput           `json:"PosixProfile,omitempty"`
	ServerID              string                       `json:"ServerId"`
	ExternalID            string                       `json:"ExternalId"`
	Role                  string                       `json:"Role"`
	HomeDir               string                       `json:"HomeDirectory"`
	HomeDirectoryType     string                       `json:"HomeDirectoryType,omitempty"`
	Policy                string                       `json:"Policy,omitempty"`
	HomeDirectoryMappings []homeDirectoryMapEntryInput `json:"HomeDirectoryMappings,omitempty"`
	Tags                  []map[string]string          `json:"Tags"`
}

type createAccessOutput struct {
	ServerID   string `json:"ServerId"`
	ExternalID string `json:"ExternalId"`
}

func (h *Handler) handleCreateAccess(
	_ context.Context,
	in *createAccessInput,
) (*createAccessOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.ExternalID == "" {
		return nil, fmt.Errorf("%w: ExternalId is required", errInvalidRequest)
	}

	tags := tagsFromList(in.Tags)

	a, err := h.Backend.CreateAccessFull(&CreateAccessInput{
		ServerID:              in.ServerID,
		ExternalID:            in.ExternalID,
		Role:                  in.Role,
		HomeDir:               in.HomeDir,
		HomeDirectoryType:     in.HomeDirectoryType,
		HomeDirectoryMappings: toHomeDirectoryMappings(in.HomeDirectoryMappings),
		Policy:                in.Policy,
		PosixProfile:          toPosixProfile(in.PosixProfile),
		Tags:                  tags,
	})
	if err != nil {
		return nil, err
	}

	return &createAccessOutput{ServerID: a.ServerID, ExternalID: a.ExternalID}, nil
}

type deleteAccessInput struct {
	ServerID   string `json:"ServerId"`
	ExternalID string `json:"ExternalId"`
}

func (h *Handler) handleDeleteAccess(_ context.Context, in *deleteAccessInput) (*struct{}, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.ExternalID == "" {
		return nil, fmt.Errorf("%w: ExternalId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteAccess(in.ServerID, in.ExternalID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// --- Agreement operations ---

type createAgreementInput struct {
	ServerID         string              `json:"ServerId"`
	Description      string              `json:"Description"`
	LocalProfileID   string              `json:"LocalProfileId"`
	PartnerProfileID string              `json:"PartnerProfileId"`
	BaseDirectory    string              `json:"BaseDirectory"`
	AccessRole       string              `json:"AccessRole"`
	Status           string              `json:"Status,omitempty"`
	Tags             []map[string]string `json:"Tags"`
}

type createAgreementOutput struct {
	AgreementID string `json:"AgreementId"`
}

// agreementARN builds the ARN for a Transfer agreement.
func agreementARN(accountID, region, serverID, agreementID string) string {
	return arn.Build("transfer", region, accountID, "server/"+serverID+"/agreement/"+agreementID)
}

func (h *Handler) handleCreateAgreement(
	_ context.Context,
	in *createAgreementInput,
) (*createAgreementOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	tags := tagsFromList(in.Tags)

	ag, err := h.Backend.CreateAgreementFull(
		in.ServerID,
		in.Description,
		in.LocalProfileID,
		in.PartnerProfileID,
		in.BaseDirectory,
		in.AccessRole,
		in.Status,
		tags,
	)
	if err != nil {
		return nil, err
	}

	return &createAgreementOutput{AgreementID: ag.AgreementID}, nil
}

type deleteAgreementInput struct {
	ServerID    string `json:"ServerId"`
	AgreementID string `json:"AgreementId"`
}

func (h *Handler) handleDeleteAgreement(
	_ context.Context,
	in *deleteAgreementInput,
) (*struct{}, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.AgreementID == "" {
		return nil, fmt.Errorf("%w: AgreementId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteAgreement(in.ServerID, in.AgreementID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// --- Connector operations ---

type connectorSftpConfigInput struct {
	UserSecretID    string   `json:"UserSecretId,omitempty"`
	TrustedHostKeys []string `json:"TrustedHostKeys,omitempty"`
}

type connectorAs2ConfigInput struct {
	LocalProfileID      string `json:"LocalProfileId,omitempty"`
	PartnerProfileID    string `json:"PartnerProfileId,omitempty"`
	SigningAlgorithm    string `json:"SigningAlgorithm,omitempty"`
	EncryptionAlgorithm string `json:"EncryptionAlgorithm,omitempty"`
	MdnSigningAlgorithm string `json:"MdnSigningAlgorithm,omitempty"`
	MdnResponse         string `json:"MdnResponse,omitempty"`
	MessageSubject      string `json:"MessageSubject,omitempty"`
	Compression         string `json:"Compression,omitempty"`
}

func toConnectorSftpConfig(in *connectorSftpConfigInput) *ConnectorSftpConfig {
	if in == nil {
		return nil
	}

	return &ConnectorSftpConfig{
		TrustedHostKeys: in.TrustedHostKeys,
		UserSecretID:    in.UserSecretID,
	}
}

func toConnectorAs2Config(in *connectorAs2ConfigInput) *ConnectorAs2Config {
	if in == nil {
		return nil
	}

	return &ConnectorAs2Config{
		LocalProfileID:      in.LocalProfileID,
		PartnerProfileID:    in.PartnerProfileID,
		SigningAlgorithm:    in.SigningAlgorithm,
		EncryptionAlgorithm: in.EncryptionAlgorithm,
		MdnSigningAlgorithm: in.MdnSigningAlgorithm,
		MdnResponse:         in.MdnResponse,
		MessageSubject:      in.MessageSubject,
		Compression:         in.Compression,
	}
}

type createConnectorInput struct {
	SftpConfig         *connectorSftpConfigInput `json:"SftpConfig,omitempty"`
	As2Config          *connectorAs2ConfigInput  `json:"As2Config,omitempty"`
	URL                string                    `json:"Url"`
	AccessRole         string                    `json:"AccessRole"`
	LoggingRole        string                    `json:"LoggingRole,omitempty"`
	SecurityPolicyName string                    `json:"SecurityPolicyName,omitempty"`
	Tags               []map[string]string       `json:"Tags"`
}

type createConnectorOutput struct {
	ConnectorID string `json:"ConnectorId"`
}

func (h *Handler) handleCreateConnector(
	_ context.Context,
	in *createConnectorInput,
) (*createConnectorOutput, error) {
	if in.URL == "" {
		return nil, fmt.Errorf("%w: Url is required", errInvalidRequest)
	}

	tags := tagsFromList(in.Tags)

	c, err := h.Backend.CreateConnectorFull(&CreateConnectorInput{
		URL:                in.URL,
		AccessRole:         in.AccessRole,
		SftpConfig:         toConnectorSftpConfig(in.SftpConfig),
		As2Config:          toConnectorAs2Config(in.As2Config),
		LoggingRole:        in.LoggingRole,
		SecurityPolicyName: in.SecurityPolicyName,
		Tags:               tags,
	})
	if err != nil {
		return nil, err
	}

	return &createConnectorOutput{ConnectorID: c.ConnectorID}, nil
}

type deleteConnectorInput struct {
	ConnectorID string `json:"ConnectorId"`
}

func (h *Handler) handleDeleteConnector(
	_ context.Context,
	in *deleteConnectorInput,
) (*struct{}, error) {
	if in.ConnectorID == "" {
		return nil, fmt.Errorf("%w: ConnectorId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteConnector(in.ConnectorID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// --- Profile operations ---

type createProfileInput struct {
	ProfileType string              `json:"ProfileType"`
	As2ID       string              `json:"As2Id"`
	Tags        []map[string]string `json:"Tags"`
}

type createProfileOutput struct {
	ProfileID string `json:"ProfileId"`
}

func (h *Handler) handleCreateProfile(
	_ context.Context,
	in *createProfileInput,
) (*createProfileOutput, error) {
	if in.ProfileType == "" {
		return nil, fmt.Errorf("%w: ProfileType is required", errInvalidRequest)
	}

	tags := tagsFromList(in.Tags)

	p, err := h.Backend.CreateProfile(in.ProfileType, in.As2ID, tags)
	if err != nil {
		return nil, err
	}

	return &createProfileOutput{ProfileID: p.ProfileID}, nil
}

// --- WebApp operations ---

type createWebAppInput struct {
	Tags []map[string]string `json:"Tags"`
}

type createWebAppOutput struct {
	WebAppID string `json:"WebAppId"`
}

func (h *Handler) handleCreateWebApp(
	_ context.Context,
	in *createWebAppInput,
) (*createWebAppOutput, error) {
	tags := tagsFromList(in.Tags)

	w, err := h.Backend.CreateWebApp(tags)
	if err != nil {
		return nil, err
	}

	return &createWebAppOutput{WebAppID: w.WebAppID}, nil
}

// --- Workflow operations ---

type copyStepDetailsInput struct {
	DestinationFileLocation map[string]any `json:"DestinationFileLocation,omitempty"`
	Name                    string         `json:"Name,omitempty"`
	SourceFileLocation      string         `json:"SourceFileLocation,omitempty"`
	OverwriteExisting       string         `json:"OverwriteExisting,omitempty"`
}

type customStepDetailsInput struct {
	Name               string `json:"Name,omitempty"`
	Target             string `json:"Target,omitempty"`
	SourceFileLocation string `json:"SourceFileLocation,omitempty"`
	Timeout            int32  `json:"Timeout,omitempty"`
}

type deleteStepDetailsInput struct {
	Name               string `json:"Name,omitempty"`
	SourceFileLocation string `json:"SourceFileLocation,omitempty"`
}

type tagStepDetailsInput struct {
	Name               string           `json:"Name,omitempty"`
	SourceFileLocation string           `json:"SourceFileLocation,omitempty"`
	Tags               []map[string]any `json:"Tags,omitempty"`
}

type decryptStepDetailsInput struct {
	DestinationFileLocation map[string]any `json:"DestinationFileLocation,omitempty"`
	Name                    string         `json:"Name,omitempty"`
	Type                    string         `json:"Type,omitempty"`
	SourceFileLocation      string         `json:"SourceFileLocation,omitempty"`
	OverwriteExisting       string         `json:"OverwriteExisting,omitempty"`
}

type workflowStepInput struct {
	CopyStepDetails    *copyStepDetailsInput    `json:"CopyStepDetails,omitempty"`
	CustomStepDetails  *customStepDetailsInput  `json:"CustomStepDetails,omitempty"`
	DeleteStepDetails  *deleteStepDetailsInput  `json:"DeleteStepDetails,omitempty"`
	TagStepDetails     *tagStepDetailsInput     `json:"TagStepDetails,omitempty"`
	DecryptStepDetails *decryptStepDetailsInput `json:"DecryptStepDetails,omitempty"`
	Type               string                   `json:"Type"`
}

type createWorkflowInput struct {
	Description      string              `json:"Description"`
	Steps            []workflowStepInput `json:"Steps,omitempty"`
	OnExceptionSteps []workflowStepInput `json:"OnExceptionSteps,omitempty"`
	Tags             []map[string]string `json:"Tags"`
}

type createWorkflowOutput struct {
	WorkflowID string `json:"WorkflowId"`
}

func toWorkflowStep(s workflowStepInput) WorkflowStep {
	ws := WorkflowStep{Type: s.Type}

	if s.CopyStepDetails != nil {
		ws.CopyStepDetails = &CopyStepDetails{
			Name:                    s.CopyStepDetails.Name,
			SourceFileLocation:      s.CopyStepDetails.SourceFileLocation,
			OverwriteExisting:       s.CopyStepDetails.OverwriteExisting,
			DestinationFileLocation: s.CopyStepDetails.DestinationFileLocation,
		}
	}

	if s.CustomStepDetails != nil {
		ws.CustomStepDetails = &CustomStepDetails{
			Name:               s.CustomStepDetails.Name,
			Target:             s.CustomStepDetails.Target,
			SourceFileLocation: s.CustomStepDetails.SourceFileLocation,
			Timeout:            s.CustomStepDetails.Timeout,
		}
	}

	if s.DeleteStepDetails != nil {
		ws.DeleteStepDetails = &DeleteStepDetails{
			Name:               s.DeleteStepDetails.Name,
			SourceFileLocation: s.DeleteStepDetails.SourceFileLocation,
		}
	}

	if s.TagStepDetails != nil {
		ws.TagStepDetails = &TagStepDetails{
			Name:               s.TagStepDetails.Name,
			SourceFileLocation: s.TagStepDetails.SourceFileLocation,
			Tags:               s.TagStepDetails.Tags,
		}
	}

	if s.DecryptStepDetails != nil {
		ws.DecryptStepDetails = &DecryptStepDetails{
			Name:                    s.DecryptStepDetails.Name,
			Type:                    s.DecryptStepDetails.Type,
			SourceFileLocation:      s.DecryptStepDetails.SourceFileLocation,
			OverwriteExisting:       s.DecryptStepDetails.OverwriteExisting,
			DestinationFileLocation: s.DecryptStepDetails.DestinationFileLocation,
		}
	}

	return ws
}

func toWorkflowSteps(inputs []workflowStepInput) []WorkflowStep {
	if inputs == nil {
		return nil
	}

	out := make([]WorkflowStep, len(inputs))
	for i, s := range inputs {
		out[i] = toWorkflowStep(s)
	}

	return out
}

func workflowStepToMap(s WorkflowStep) map[string]any {
	m := map[string]any{keyStepType: s.Type}

	if s.CopyStepDetails != nil {
		m["CopyStepDetails"] = map[string]any{
			keyStepName:               s.CopyStepDetails.Name,
			keySourceFileLoc:          s.CopyStepDetails.SourceFileLocation,
			"OverwriteExisting":       s.CopyStepDetails.OverwriteExisting,
			"DestinationFileLocation": s.CopyStepDetails.DestinationFileLocation,
		}
	}

	if s.CustomStepDetails != nil {
		m["CustomStepDetails"] = map[string]any{
			keyStepName:      s.CustomStepDetails.Name,
			"Target":         s.CustomStepDetails.Target,
			keySourceFileLoc: s.CustomStepDetails.SourceFileLocation,
			"Timeout":        s.CustomStepDetails.Timeout,
		}
	}

	if s.DeleteStepDetails != nil {
		m["DeleteStepDetails"] = map[string]any{
			keyStepName:      s.DeleteStepDetails.Name,
			keySourceFileLoc: s.DeleteStepDetails.SourceFileLocation,
		}
	}

	if s.TagStepDetails != nil {
		m["TagStepDetails"] = map[string]any{
			keyStepName:      s.TagStepDetails.Name,
			keySourceFileLoc: s.TagStepDetails.SourceFileLocation,
			keyTags:          s.TagStepDetails.Tags,
		}
	}

	if s.DecryptStepDetails != nil {
		m["DecryptStepDetails"] = map[string]any{
			keyStepName:               s.DecryptStepDetails.Name,
			keyStepType:               s.DecryptStepDetails.Type,
			keySourceFileLoc:          s.DecryptStepDetails.SourceFileLocation,
			"OverwriteExisting":       s.DecryptStepDetails.OverwriteExisting,
			"DestinationFileLocation": s.DecryptStepDetails.DestinationFileLocation,
		}
	}

	return m
}

func (h *Handler) handleCreateWorkflow(
	_ context.Context,
	in *createWorkflowInput,
) (*createWorkflowOutput, error) {
	tags := tagsFromList(in.Tags)

	wf, err := h.Backend.CreateWorkflow(
		in.Description,
		toWorkflowSteps(in.Steps),
		toWorkflowSteps(in.OnExceptionSteps),
		tags,
	)
	if err != nil {
		return nil, err
	}

	return &createWorkflowOutput{WorkflowID: wf.WorkflowID}, nil
}

// --- Certificate operations ---

type deleteCertificateInput struct {
	CertificateID string `json:"CertificateId"`
}

func (h *Handler) handleDeleteCertificate(
	_ context.Context,
	in *deleteCertificateInput,
) (*struct{}, error) {
	if in.CertificateID == "" {
		return nil, fmt.Errorf("%w: CertificateId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteCertificate(in.CertificateID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// --- Extended Access operations ---

type describeAccessInput struct {
	ServerID   string `json:"ServerId"`
	ExternalID string `json:"ExternalId"`
}

type describeAccessOutput struct {
	Access   map[string]any `json:"Access"`
	ServerID string         `json:"ServerId"`
}

func (h *Handler) handleDescribeAccess(
	_ context.Context,
	in *describeAccessInput,
) (*describeAccessOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.ExternalID == "" {
		return nil, fmt.Errorf("%w: ExternalId is required", errInvalidRequest)
	}

	a, err := h.Backend.DescribeAccess(in.ServerID, in.ExternalID)
	if err != nil {
		return nil, err
	}

	accessMap := map[string]any{
		"ExternalId":        a.ExternalID,
		"ServerId":          a.ServerID,
		"Role":              a.Role,
		"HomeDirectory":     a.HomeDir,
		"HomeDirectoryType": a.HomeDirectoryType,
		"Policy":            a.Policy,
	}

	if a.PosixProfile != nil {
		accessMap["PosixProfile"] = map[string]any{
			"Uid":           a.PosixProfile.UID,
			"Gid":           a.PosixProfile.GID,
			"SecondaryGids": a.PosixProfile.SecondaryGids,
		}
	}

	if a.HomeDirectoryMappings != nil {
		mappings := make([]map[string]any, len(a.HomeDirectoryMappings))
		for i, m := range a.HomeDirectoryMappings {
			mappings[i] = map[string]any{"Entry": m.Entry, "Target": m.Target, keyStepType: m.Type}
		}
		accessMap["HomeDirectoryMappings"] = mappings
	}

	if a.Tags != nil {
		accessMap[keyTags] = tagsToList(a.Tags)
	}

	return &describeAccessOutput{
		ServerID: a.ServerID,
		Access:   accessMap,
	}, nil
}

type listAccessesInput struct {
	ServerID   string `json:"ServerId"`
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listAccessesOutput struct {
	NextToken string           `json:"NextToken,omitempty"`
	ServerID  string           `json:"ServerId"`
	Accesses  []map[string]any `json:"Accesses"`
}

func (h *Handler) handleListAccesses(
	_ context.Context,
	in *listAccessesInput,
) (*listAccessesOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	items, err := h.Backend.ListAccesses(in.ServerID)
	if err != nil {
		return nil, err
	}

	page, next := applyNextTokenItems(items, in.NextToken, in.MaxResults)
	out := make([]map[string]any, len(page))

	for i, a := range page {
		out[i] = map[string]any{
			"ExternalId":    a.ExternalID,
			"HomeDirectory": a.HomeDir,
			"Role":          a.Role,
		}
	}

	return &listAccessesOutput{Accesses: out, NextToken: next, ServerID: in.ServerID}, nil
}

type updateAccessInput struct {
	ServerID   string `json:"ServerId"`
	ExternalID string `json:"ExternalId"`
	Role       string `json:"Role"`
	HomeDir    string `json:"HomeDirectory"`
}

type updateAccessOutput struct {
	ServerID   string `json:"ServerId"`
	ExternalID string `json:"ExternalId"`
}

func (h *Handler) handleUpdateAccess(
	_ context.Context,
	in *updateAccessInput,
) (*updateAccessOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.ExternalID == "" {
		return nil, fmt.Errorf("%w: ExternalId is required", errInvalidRequest)
	}

	a, err := h.Backend.UpdateAccess(in.ServerID, in.ExternalID, in.Role, in.HomeDir)
	if err != nil {
		return nil, err
	}

	return &updateAccessOutput{ServerID: a.ServerID, ExternalID: a.ExternalID}, nil
}

// --- Extended Agreement operations ---

type describeAgreementInput struct {
	ServerID    string `json:"ServerId"`
	AgreementID string `json:"AgreementId"`
}

type describeAgreementOutput struct {
	Agreement map[string]any `json:"Agreement"`
}

func (h *Handler) handleDescribeAgreement(
	_ context.Context,
	in *describeAgreementInput,
) (*describeAgreementOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.AgreementID == "" {
		return nil, fmt.Errorf("%w: AgreementId is required", errInvalidRequest)
	}

	ag, err := h.Backend.DescribeAgreement(in.ServerID, in.AgreementID)
	if err != nil {
		return nil, err
	}

	return &describeAgreementOutput{
		Agreement: map[string]any{
			"AgreementId":       ag.AgreementID,
			"ServerId":          ag.ServerID,
			keyDescription:      ag.Description,
			keyStatus:           ag.Status,
			keyLocalProfileID:   ag.LocalProfileID,
			keyPartnerProfileID: ag.PartnerProfileID,
			"BaseDirectory":     ag.BaseDirectory,
			"AccessRole":        ag.AccessRole,
			keyArn:              agreementARN(ag.AccountID, ag.Region, ag.ServerID, ag.AgreementID),
			keyTags:             tagsToList(ag.Tags),
		},
	}, nil
}

type listAgreementsInput struct {
	ServerID   string `json:"ServerId"`
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listAgreementsOutput struct {
	NextToken  string           `json:"NextToken,omitempty"`
	Agreements []map[string]any `json:"Agreements"`
}

func (h *Handler) handleListAgreements(
	_ context.Context,
	in *listAgreementsInput,
) (*listAgreementsOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	items, err := h.Backend.ListAgreements(in.ServerID)
	if err != nil {
		return nil, err
	}

	page, next := applyNextTokenItems(items, in.NextToken, in.MaxResults)
	out := make([]map[string]any, len(page))

	for i, ag := range page {
		out[i] = map[string]any{
			"AgreementId":       ag.AgreementID,
			keyArn:              agreementARN(ag.AccountID, ag.Region, ag.ServerID, ag.AgreementID),
			keyDescription:      ag.Description,
			keyStatus:           ag.Status,
			keyLocalProfileID:   ag.LocalProfileID,
			keyPartnerProfileID: ag.PartnerProfileID,
		}
	}

	return &listAgreementsOutput{Agreements: out, NextToken: next}, nil
}

type updateAgreementInput struct {
	ServerID    string `json:"ServerId"`
	AgreementID string `json:"AgreementId"`
	Description string `json:"Description"`
	Status      string `json:"Status"`
}

type updateAgreementOutput struct {
	AgreementID string `json:"AgreementId"`
}

func (h *Handler) handleUpdateAgreement(
	_ context.Context,
	in *updateAgreementInput,
) (*updateAgreementOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.AgreementID == "" {
		return nil, fmt.Errorf("%w: AgreementId is required", errInvalidRequest)
	}

	ag, err := h.Backend.UpdateAgreement(in.ServerID, in.AgreementID, in.Description, in.Status)
	if err != nil {
		return nil, err
	}

	return &updateAgreementOutput{AgreementID: ag.AgreementID}, nil
}

// --- Extended Connector operations ---

type describeConnectorInput struct {
	ConnectorID string `json:"ConnectorId"`
}

type describeConnectorOutput struct {
	Connector map[string]any `json:"Connector"`
}

// connectorARN builds the ARN for a Transfer connector.
func connectorARN(accountID, region, connectorID string) string {
	return arn.Build("transfer", region, accountID, "connector/"+connectorID)
}

// profileARN builds the ARN for a Transfer AS2 profile.
func profileARN(accountID, region, profileID string) string {
	return arn.Build("transfer", region, accountID, "profile/"+profileID)
}

// workflowARN builds the ARN for a Transfer workflow.
func workflowARN(accountID, region, workflowID string) string {
	return arn.Build("transfer", region, accountID, "workflow/"+workflowID)
}

// certificateARN builds the ARN for a Transfer certificate.
func certificateARN(accountID, region, certificateID string) string {
	return arn.Build("transfer", region, accountID, "certificate/"+certificateID)
}

// hostKeyARN builds the ARN for a Transfer host key.
func hostKeyARN(accountID, region, serverID, hostKeyID string) string {
	return arn.Build("transfer", region, accountID, "host-key/"+serverID+"/"+hostKeyID)
}

func (h *Handler) handleDescribeConnector(
	_ context.Context,
	in *describeConnectorInput,
) (*describeConnectorOutput, error) {
	if in.ConnectorID == "" {
		return nil, fmt.Errorf("%w: ConnectorId is required", errInvalidRequest)
	}

	c, err := h.Backend.DescribeConnector(in.ConnectorID)
	if err != nil {
		return nil, err
	}

	connMap := map[string]any{
		keyConnectorID:       c.ConnectorID,
		keyURL:               c.URL,
		"AccessRole":         c.AccessRole,
		keyArn:               connectorARN(c.AccountID, c.Region, c.ConnectorID),
		keyTags:              tagsToList(c.Tags),
		"LoggingRole":        c.LoggingRole,
		"SecurityPolicyName": c.SecurityPolicyName,
	}

	if c.SftpConfig != nil {
		connMap["SftpConfig"] = map[string]any{
			"UserSecretId":    c.SftpConfig.UserSecretID,
			"TrustedHostKeys": c.SftpConfig.TrustedHostKeys,
		}
	}

	if c.As2Config != nil {
		connMap["As2Config"] = map[string]any{
			keyLocalProfileID:     c.As2Config.LocalProfileID,
			keyPartnerProfileID:   c.As2Config.PartnerProfileID,
			"SigningAlgorithm":    c.As2Config.SigningAlgorithm,
			"EncryptionAlgorithm": c.As2Config.EncryptionAlgorithm,
			"MdnSigningAlgorithm": c.As2Config.MdnSigningAlgorithm,
			"MdnResponse":         c.As2Config.MdnResponse,
			"MessageSubject":      c.As2Config.MessageSubject,
			"Compression":         c.As2Config.Compression,
		}
	}

	return &describeConnectorOutput{
		Connector: connMap,
	}, nil
}

type listConnectorsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listConnectorsOutput struct {
	NextToken  string           `json:"NextToken,omitempty"`
	Connectors []map[string]any `json:"Connectors"`
}

func (h *Handler) handleListConnectors(
	_ context.Context,
	in *listConnectorsInput,
) (*listConnectorsOutput, error) {
	items := h.Backend.ListConnectors()
	page, next := applyNextTokenItems(items, in.NextToken, in.MaxResults)
	out := make([]map[string]any, len(page))

	for i, c := range page {
		out[i] = map[string]any{
			keyConnectorID: c.ConnectorID,
			keyURL:         c.URL,
		}
	}

	return &listConnectorsOutput{Connectors: out, NextToken: next}, nil
}

type updateConnectorInput struct {
	SftpConfig  *connectorSftpConfigInput `json:"SftpConfig,omitempty"`
	As2Config   *connectorAs2ConfigInput  `json:"As2Config,omitempty"`
	ConnectorID string                    `json:"ConnectorId"`
	URL         string                    `json:"Url"`
	AccessRole  string                    `json:"AccessRole"`
}

type updateConnectorOutput struct {
	ConnectorID string `json:"ConnectorId"`
}

func (h *Handler) handleUpdateConnector(
	_ context.Context,
	in *updateConnectorInput,
) (*updateConnectorOutput, error) {
	if in.ConnectorID == "" {
		return nil, fmt.Errorf("%w: ConnectorId is required", errInvalidRequest)
	}

	c, err := h.Backend.UpdateConnector(
		in.ConnectorID,
		in.URL,
		in.AccessRole,
		toConnectorSftpConfig(in.SftpConfig),
		toConnectorAs2Config(in.As2Config),
	)
	if err != nil {
		return nil, err
	}

	return &updateConnectorOutput{ConnectorID: c.ConnectorID}, nil
}

// --- Extended Profile operations ---

type deleteProfileInput struct {
	ProfileID string `json:"ProfileId"`
}

func (h *Handler) handleDeleteProfile(
	_ context.Context,
	in *deleteProfileInput,
) (*struct{}, error) {
	if in.ProfileID == "" {
		return nil, fmt.Errorf("%w: ProfileId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteProfile(in.ProfileID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type describeProfileInput struct {
	ProfileID string `json:"ProfileId"`
}

type describeProfileOutput struct {
	Profile map[string]any `json:"Profile"`
}

func (h *Handler) handleDescribeProfile(
	_ context.Context,
	in *describeProfileInput,
) (*describeProfileOutput, error) {
	if in.ProfileID == "" {
		return nil, fmt.Errorf("%w: ProfileId is required", errInvalidRequest)
	}

	p, err := h.Backend.DescribeProfile(in.ProfileID)
	if err != nil {
		return nil, err
	}

	return &describeProfileOutput{
		Profile: map[string]any{
			"ProfileId":   p.ProfileID,
			"ProfileType": p.ProfileType,
			"As2Id":       p.As2ID,
			keyArn:        profileARN(p.AccountID, p.Region, p.ProfileID),
			keyTags:       tagsToList(p.Tags),
		},
	}, nil
}

type listProfilesInput struct {
	NextToken   string `json:"NextToken"`
	ProfileType string `json:"ProfileType"`
	MaxResults  int    `json:"MaxResults"`
}

type listProfilesOutput struct {
	NextToken string           `json:"NextToken,omitempty"`
	Profiles  []map[string]any `json:"Profiles"`
}

func (h *Handler) handleListProfiles(
	_ context.Context,
	in *listProfilesInput,
) (*listProfilesOutput, error) {
	items := h.Backend.ListProfiles()

	if in.ProfileType != "" {
		filtered := items[:0]
		for _, p := range items {
			if p.ProfileType == in.ProfileType {
				filtered = append(filtered, p)
			}
		}
		items = filtered
	}

	page, next := applyNextTokenItems(items, in.NextToken, in.MaxResults)
	out := make([]map[string]any, len(page))

	for i, p := range page {
		out[i] = map[string]any{
			"ProfileId":   p.ProfileID,
			"ProfileType": p.ProfileType,
			"As2Id":       p.As2ID,
			keyArn:        profileARN(p.AccountID, p.Region, p.ProfileID),
		}
	}

	return &listProfilesOutput{Profiles: out, NextToken: next}, nil
}

type updateProfileInput struct {
	ProfileID string `json:"ProfileId"`
	As2ID     string `json:"As2Id"`
}

type updateProfileOutput struct {
	ProfileID string `json:"ProfileId"`
}

func (h *Handler) handleUpdateProfile(
	_ context.Context,
	in *updateProfileInput,
) (*updateProfileOutput, error) {
	if in.ProfileID == "" {
		return nil, fmt.Errorf("%w: ProfileId is required", errInvalidRequest)
	}

	p, err := h.Backend.UpdateProfile(in.ProfileID, in.As2ID)
	if err != nil {
		return nil, err
	}

	return &updateProfileOutput{ProfileID: p.ProfileID}, nil
}

// --- Extended WebApp operations ---

type deleteWebAppInput struct {
	WebAppID string `json:"WebAppId"`
}

func (h *Handler) handleDeleteWebApp(_ context.Context, in *deleteWebAppInput) (*struct{}, error) {
	if in.WebAppID == "" {
		return nil, fmt.Errorf("%w: WebAppId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteWebApp(in.WebAppID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type describeWebAppInput struct {
	WebAppID string `json:"WebAppId"`
}

type describeWebAppOutput struct {
	WebApp map[string]any `json:"WebApp"`
}

func (h *Handler) handleDescribeWebApp(
	_ context.Context,
	in *describeWebAppInput,
) (*describeWebAppOutput, error) {
	if in.WebAppID == "" {
		return nil, fmt.Errorf("%w: WebAppId is required", errInvalidRequest)
	}

	w, err := h.Backend.DescribeWebApp(in.WebAppID)
	if err != nil {
		return nil, err
	}

	return &describeWebAppOutput{
		WebApp: map[string]any{
			"WebAppId": w.WebAppID,
		},
	}, nil
}

type listWebAppsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listWebAppsOutput struct {
	NextToken string           `json:"NextToken,omitempty"`
	WebApps   []map[string]any `json:"WebApps"`
}

func (h *Handler) handleListWebApps(
	_ context.Context,
	in *listWebAppsInput,
) (*listWebAppsOutput, error) {
	items := h.Backend.ListWebApps()
	page, next := applyNextTokenItems(items, in.NextToken, in.MaxResults)
	out := make([]map[string]any, len(page))

	for i, w := range page {
		out[i] = map[string]any{
			"WebAppId": w.WebAppID,
		}
	}

	return &listWebAppsOutput{WebApps: out, NextToken: next}, nil
}

type webAppIdentityProviderDetailsInput struct {
	IdentityProviderType string `json:"IdentityProviderType,omitempty"`
	InstanceArn          string `json:"InstanceArn,omitempty"`
	Role                 string `json:"Role,omitempty"`
	URL                  string `json:"Url,omitempty"`
	Directory            string `json:"Directory,omitempty"`
	Function             string `json:"Function,omitempty"`
}

type updateWebAppInput struct {
	IdentityProviderDetails *webAppIdentityProviderDetailsInput `json:"IdentityProviderDetails,omitempty"`
	WebAppID                string                              `json:"WebAppId"`
}

type updateWebAppOutput struct {
	WebAppID string `json:"WebAppId"`
}

func (h *Handler) handleUpdateWebApp(
	_ context.Context,
	in *updateWebAppInput,
) (*updateWebAppOutput, error) {
	if in.WebAppID == "" {
		return nil, fmt.Errorf("%w: WebAppId is required", errInvalidRequest)
	}

	var ipd *WebAppIdentityProviderDetails
	if in.IdentityProviderDetails != nil {
		ipd = &WebAppIdentityProviderDetails{
			IdentityProviderType: in.IdentityProviderDetails.IdentityProviderType,
			InstanceArn:          in.IdentityProviderDetails.InstanceArn,
			Role:                 in.IdentityProviderDetails.Role,
			URL:                  in.IdentityProviderDetails.URL,
			Directory:            in.IdentityProviderDetails.Directory,
			Function:             in.IdentityProviderDetails.Function,
		}
	}

	w, err := h.Backend.UpdateWebApp(in.WebAppID, ipd)
	if err != nil {
		return nil, err
	}

	return &updateWebAppOutput{WebAppID: w.WebAppID}, nil
}

// --- WebApp Customization stubs ---

func (h *Handler) handleDeleteWebAppCustomization(
	_ context.Context,
	_ *struct{},
) (*struct{}, error) {
	return &struct{}{}, nil
}

func (h *Handler) handleDescribeWebAppCustomization(
	_ context.Context,
	_ *struct{},
) (*map[string]any, error) {
	return &map[string]any{"WebAppCustomization": map[string]any{}}, nil
}

func (h *Handler) handleUpdateWebAppCustomization(
	_ context.Context,
	_ *struct{},
) (*struct{}, error) {
	return &struct{}{}, nil
}

// --- Extended Workflow operations ---

type deleteWorkflowInput struct {
	WorkflowID string `json:"WorkflowId"`
}

func (h *Handler) handleDeleteWorkflow(
	_ context.Context,
	in *deleteWorkflowInput,
) (*struct{}, error) {
	if in.WorkflowID == "" {
		return nil, fmt.Errorf("%w: WorkflowId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteWorkflow(in.WorkflowID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type describeWorkflowInput struct {
	WorkflowID string `json:"WorkflowId"`
}

type describeWorkflowOutput struct {
	Workflow map[string]any `json:"Workflow"`
}

func (h *Handler) handleDescribeWorkflow(
	_ context.Context,
	in *describeWorkflowInput,
) (*describeWorkflowOutput, error) {
	if in.WorkflowID == "" {
		return nil, fmt.Errorf("%w: WorkflowId is required", errInvalidRequest)
	}

	wf, err := h.Backend.DescribeWorkflow(in.WorkflowID)
	if err != nil {
		return nil, err
	}

	stepsToList := func(steps []WorkflowStep) []any {
		out := make([]any, len(steps))
		for i, s := range steps {
			out[i] = workflowStepToMap(s)
		}

		return out
	}

	return &describeWorkflowOutput{
		Workflow: map[string]any{
			keyWorkflowID:      wf.WorkflowID,
			keyDescription:     wf.Description,
			"Steps":            stepsToList(wf.Steps),
			"OnExceptionSteps": stepsToList(wf.OnExceptionSteps),
			keyArn:             workflowARN(wf.AccountID, wf.Region, wf.WorkflowID),
			keyTags:            tagsToList(wf.Tags),
		},
	}, nil
}

type listWorkflowsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listWorkflowsOutput struct {
	NextToken string           `json:"NextToken,omitempty"`
	Workflows []map[string]any `json:"Workflows"`
}

func (h *Handler) handleListWorkflows(
	_ context.Context,
	in *listWorkflowsInput,
) (*listWorkflowsOutput, error) {
	items := h.Backend.ListWorkflows()
	page, next := applyNextTokenItems(items, in.NextToken, in.MaxResults)
	out := make([]map[string]any, len(page))

	for i, wf := range page {
		out[i] = map[string]any{
			keyWorkflowID:  wf.WorkflowID,
			keyDescription: wf.Description,
			keyArn:         workflowARN(wf.AccountID, wf.Region, wf.WorkflowID),
		}
	}

	return &listWorkflowsOutput{Workflows: out, NextToken: next}, nil
}

// --- Extended Certificate operations ---

// parseCertDate parses an RFC3339 date string for use as a certificate date fallback.
// When the certificate body is non-empty, dates come from the PEM, so this returns zero.
func parseCertDate(body, dateStr string) time.Time {
	if body != "" || dateStr == "" {
		return time.Time{}
	}

	t, err := time.Parse(time.RFC3339, dateStr)
	if err != nil {
		return time.Time{}
	}

	return t
}

type importCertificateInput struct {
	Usage         string              `json:"Usage"`
	Body          string              `json:"Certificate"`
	Description   string              `json:"Description,omitempty"`
	NotBeforeDate string              `json:"NotBeforeDate,omitempty"` // RFC3339
	NotAfterDate  string              `json:"NotAfterDate,omitempty"`  // RFC3339
	Tags          []map[string]string `json:"Tags"`
}

type importCertificateOutput struct {
	CertificateID string `json:"CertificateId"`
}

func (h *Handler) handleImportCertificate(
	_ context.Context,
	in *importCertificateInput,
) (*importCertificateOutput, error) {
	if in.Usage == "" {
		return nil, fmt.Errorf("%w: Usage is required", errInvalidRequest)
	}

	switch in.Usage {
	case "SIGNING", "ENCRYPTION":
		// valid
	default:
		return nil, fmt.Errorf(
			"%w: Usage must be SIGNING or ENCRYPTION, got %q",
			errInvalidRequest,
			in.Usage,
		)
	}

	tags := tagsFromList(in.Tags)

	// If body is provided, the backend will parse PEM and extract dates.
	// Only use user-provided dates as fallback when no body.
	notBefore := parseCertDate(in.Body, in.NotBeforeDate)
	notAfter := parseCertDate(in.Body, in.NotAfterDate)

	c, err := h.Backend.ImportCertificate(
		in.Usage,
		in.Body,
		in.Description,
		notBefore,
		notAfter,
		tags,
	)
	if err != nil {
		return nil, err
	}

	return &importCertificateOutput{CertificateID: c.CertificateID}, nil
}

type describeCertificateInput struct {
	CertificateID string `json:"CertificateId"`
}

type describeCertificateOutput struct {
	Certificate map[string]any `json:"Certificate"`
}

func (h *Handler) handleDescribeCertificate(
	_ context.Context,
	in *describeCertificateInput,
) (*describeCertificateOutput, error) {
	if in.CertificateID == "" {
		return nil, fmt.Errorf("%w: CertificateId is required", errInvalidRequest)
	}

	c, err := h.Backend.DescribeCertificate(in.CertificateID)
	if err != nil {
		return nil, err
	}

	certMap := map[string]any{
		"CertificateId": c.CertificateID,
		"Usage":         c.Usage,
		keyDescription:  c.Description,
		keyStatus:       c.Status,
		keyArn:          certificateARN(c.AccountID, c.Region, c.CertificateID),
	}

	if !c.NotBeforeDate.IsZero() {
		certMap["NotBeforeDate"] = c.NotBeforeDate.Format(time.RFC3339)
	}

	if !c.NotAfterDate.IsZero() {
		certMap["NotAfterDate"] = c.NotAfterDate.Format(time.RFC3339)
	}

	return &describeCertificateOutput{
		Certificate: certMap,
	}, nil
}

type listCertificatesInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listCertificatesOutput struct {
	NextToken    string           `json:"NextToken,omitempty"`
	Certificates []map[string]any `json:"Certificates"`
}

func (h *Handler) handleListCertificates(
	_ context.Context,
	in *listCertificatesInput,
) (*listCertificatesOutput, error) {
	items := h.Backend.ListCertificates()
	page, next := applyNextTokenItems(items, in.NextToken, in.MaxResults)
	out := make([]map[string]any, len(page))

	for i, c := range page {
		out[i] = map[string]any{
			"CertificateId": c.CertificateID,
			"Usage":         c.Usage,
			keyStatus:       c.Status,
			keyArn:          certificateARN(c.AccountID, c.Region, c.CertificateID),
		}
	}

	return &listCertificatesOutput{Certificates: out, NextToken: next}, nil
}

type updateCertificateInput struct {
	CertificateID string `json:"CertificateId"`
	Description   string `json:"Description"`
}

type updateCertificateOutput struct {
	CertificateID string `json:"CertificateId"`
}

func (h *Handler) handleUpdateCertificate(
	_ context.Context,
	in *updateCertificateInput,
) (*updateCertificateOutput, error) {
	if in.CertificateID == "" {
		return nil, fmt.Errorf("%w: CertificateId is required", errInvalidRequest)
	}

	c, err := h.Backend.UpdateCertificate(in.CertificateID, in.Description)
	if err != nil {
		return nil, err
	}

	return &updateCertificateOutput{CertificateID: c.CertificateID}, nil
}

// --- HostKey operations ---

type importHostKeyInput struct {
	ServerID    string              `json:"ServerId"`
	HostKeyBody string              `json:"HostKeyBody"`
	Description string              `json:"Description"`
	Tags        []map[string]string `json:"Tags"`
}

type importHostKeyOutput struct {
	ServerID  string `json:"ServerId"`
	HostKeyID string `json:"HostKeyId"`
}

func (h *Handler) handleImportHostKey(
	_ context.Context,
	in *importHostKeyInput,
) (*importHostKeyOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	tags := tagsFromList(in.Tags)

	hk, err := h.Backend.ImportHostKey(in.ServerID, in.HostKeyBody, in.Description, tags)
	if err != nil {
		return nil, err
	}

	return &importHostKeyOutput{ServerID: hk.ServerID, HostKeyID: hk.HostKeyID}, nil
}

type deleteHostKeyInput struct {
	ServerID  string `json:"ServerId"`
	HostKeyID string `json:"HostKeyId"`
}

func (h *Handler) handleDeleteHostKey(
	_ context.Context,
	in *deleteHostKeyInput,
) (*struct{}, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.HostKeyID == "" {
		return nil, fmt.Errorf("%w: HostKeyId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteHostKey(in.ServerID, in.HostKeyID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type describeHostKeyInput struct {
	ServerID  string `json:"ServerId"`
	HostKeyID string `json:"HostKeyId"`
}

type describeHostKeyOutput struct {
	HostKey  map[string]any `json:"HostKey"`
	ServerID string         `json:"ServerId"`
}

func (h *Handler) handleDescribeHostKey(
	_ context.Context,
	in *describeHostKeyInput,
) (*describeHostKeyOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.HostKeyID == "" {
		return nil, fmt.Errorf("%w: HostKeyId is required", errInvalidRequest)
	}

	hk, err := h.Backend.DescribeHostKey(in.ServerID, in.HostKeyID)
	if err != nil {
		return nil, err
	}

	hkMap := map[string]any{
		"HostKeyId":    hk.HostKeyID,
		keyDescription: hk.Description,
		"Type":         hk.Type,
		"DateImported": hk.CreatedAt.Format(time.RFC3339),
		keyArn:         hostKeyARN(hk.AccountID, hk.Region, hk.ServerID, hk.HostKeyID),
		keyTags:        tagsToList(hk.Tags),
	}

	if hk.Fingerprint != "" {
		hkMap["HostKeyFingerprint"] = hk.Fingerprint
	}

	return &describeHostKeyOutput{
		ServerID: hk.ServerID,
		HostKey:  hkMap,
	}, nil
}

type listHostKeysInput struct {
	ServerID   string `json:"ServerId"`
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listHostKeysOutput struct {
	NextToken string           `json:"NextToken,omitempty"`
	ServerID  string           `json:"ServerId"`
	HostKeys  []map[string]any `json:"HostKeys"`
}

func (h *Handler) handleListHostKeys(
	_ context.Context,
	in *listHostKeysInput,
) (*listHostKeysOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	items, err := h.Backend.ListHostKeys(in.ServerID)
	if err != nil {
		return nil, err
	}

	page, next := applyNextTokenItems(items, in.NextToken, in.MaxResults)
	out := make([]map[string]any, len(page))

	for i, hk := range page {
		item := map[string]any{
			"HostKeyId":    hk.HostKeyID,
			keyDescription: hk.Description,
			"Type":         hk.Type,
			"DateImported": hk.CreatedAt.Format(time.RFC3339),
			keyArn:         hostKeyARN(hk.AccountID, hk.Region, hk.ServerID, hk.HostKeyID),
		}
		if hk.Fingerprint != "" {
			item["HostKeyFingerprint"] = hk.Fingerprint
		}
		out[i] = item
	}

	return &listHostKeysOutput{HostKeys: out, NextToken: next, ServerID: in.ServerID}, nil
}

type updateHostKeyInput struct {
	ServerID    string `json:"ServerId"`
	HostKeyID   string `json:"HostKeyId"`
	Description string `json:"Description"`
}

type updateHostKeyOutput struct {
	ServerID  string `json:"ServerId"`
	HostKeyID string `json:"HostKeyId"`
}

func (h *Handler) handleUpdateHostKey(
	_ context.Context,
	in *updateHostKeyInput,
) (*updateHostKeyOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.HostKeyID == "" {
		return nil, fmt.Errorf("%w: HostKeyId is required", errInvalidRequest)
	}

	hk, err := h.Backend.UpdateHostKey(in.ServerID, in.HostKeyID, in.Description)
	if err != nil {
		return nil, err
	}

	return &updateHostKeyOutput{ServerID: hk.ServerID, HostKeyID: hk.HostKeyID}, nil
}

// --- SSH public key operations ---

type importSSHPublicKeyInput struct {
	ServerID         string `json:"ServerId"`
	UserName         string `json:"UserName"`
	SSHPublicKeyBody string `json:"SshPublicKeyBody"`
}

type importSSHPublicKeyOutput struct {
	ServerID       string `json:"ServerId"`
	SSHPublicKeyID string `json:"SshPublicKeyId"`
	UserName       string `json:"UserName"`
}

func (h *Handler) handleImportSSHPublicKey(
	_ context.Context,
	in *importSSHPublicKeyInput,
) (*importSSHPublicKeyOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.UserName == "" {
		return nil, fmt.Errorf("%w: UserName is required", errInvalidRequest)
	}

	k, err := h.Backend.ImportSSHPublicKey(in.ServerID, in.UserName, in.SSHPublicKeyBody)
	if err != nil {
		return nil, err
	}

	return &importSSHPublicKeyOutput{
		ServerID:       k.ServerID,
		SSHPublicKeyID: k.SSHPublicKeyID,
		UserName:       k.UserName,
	}, nil
}

type deleteSSHPublicKeyInput struct {
	ServerID       string `json:"ServerId"`
	UserName       string `json:"UserName"`
	SSHPublicKeyID string `json:"SshPublicKeyId"`
}

func (h *Handler) handleDeleteSSHPublicKey(
	_ context.Context,
	in *deleteSSHPublicKeyInput,
) (*struct{}, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.UserName == "" {
		return nil, fmt.Errorf("%w: UserName is required", errInvalidRequest)
	}

	if in.SSHPublicKeyID == "" {
		return nil, fmt.Errorf("%w: SSHPublicKeyID is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteSSHPublicKey(in.ServerID, in.UserName, in.SSHPublicKeyID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// --- Tag operations ---

type tagResourceInput struct {
	Arn  string              `json:"Arn"`
	Tags []map[string]string `json:"Tags"`
}

func (h *Handler) handleTagResource(_ context.Context, in *tagResourceInput) (*struct{}, error) {
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", errInvalidRequest)
	}

	tags := tagsFromList(in.Tags)
	if err := h.Backend.TagResource(in.Arn, tags); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type untagResourceInput struct {
	Arn     string   `json:"Arn"`
	TagKeys []string `json:"TagKeys"`
}

func (h *Handler) handleUntagResource(
	_ context.Context,
	in *untagResourceInput,
) (*struct{}, error) {
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(in.Arn, in.TagKeys); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type listTagsForResourceInput struct {
	Arn        string `json:"Arn"`
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listTagsForResourceOutput struct {
	Arn       string              `json:"Arn"`
	NextToken string              `json:"NextToken,omitempty"`
	Tags      []map[string]string `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", errInvalidRequest)
	}

	tags := h.Backend.ListTagsForResource(in.Arn)

	return &listTagsForResourceOutput{
		Arn:  in.Arn,
		Tags: tagsToList(tags),
	}, nil
}

// --- Execution operations ---

type listExecutionsInput struct {
	WorkflowID string `json:"WorkflowId"`
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

func (h *Handler) handleListExecutions(
	_ context.Context,
	in *listExecutionsInput,
) (*map[string]any, error) {
	if in.WorkflowID == "" {
		return nil, fmt.Errorf("%w: WorkflowId is required", errInvalidRequest)
	}

	items, err := h.Backend.ListExecutions(in.WorkflowID)
	if err != nil {
		return nil, err
	}

	pageItems, next := applyNextTokenItems(items, in.NextToken, in.MaxResults)
	out := make([]any, len(pageItems))

	for i, e := range pageItems {
		out[i] = map[string]any{
			"ExecutionId": e.ExecutionID,
			keyWorkflowID: e.WorkflowID,
			keyStatus:     e.Status,
		}
	}

	return &map[string]any{"Executions": out, keyWorkflowID: in.WorkflowID, "NextToken": next}, nil
}

type describeExecutionInput struct {
	WorkflowID  string `json:"WorkflowId"`
	ExecutionID string `json:"ExecutionId"`
}

func (h *Handler) handleDescribeExecution(
	_ context.Context,
	in *describeExecutionInput,
) (*map[string]any, error) {
	if in.WorkflowID == "" {
		return nil, fmt.Errorf("%w: WorkflowId is required", errInvalidRequest)
	}

	if in.ExecutionID == "" {
		return nil, fmt.Errorf("%w: ExecutionId is required", errInvalidRequest)
	}

	e, err := h.Backend.DescribeExecution(in.WorkflowID, in.ExecutionID)
	if err != nil {
		return nil, err
	}

	return &map[string]any{
		"Execution": map[string]any{
			"ExecutionId": e.ExecutionID,
			keyWorkflowID: e.WorkflowID,
			keyStatus:     e.Status,
		},
		keyWorkflowID: in.WorkflowID,
	}, nil
}

// --- Other improved operations ---

type listFileTransferResultsInput struct {
	ConnectorID string `json:"ConnectorId"`
	TransferID  string `json:"TransferId,omitempty"`
	NextToken   string `json:"NextToken,omitempty"`
	MaxResults  int    `json:"MaxResults,omitempty"`
}

func (h *Handler) handleListFileTransferResults(
	_ context.Context,
	in *listFileTransferResultsInput,
) (*map[string]any, error) {
	connID := ""
	if in != nil {
		connID = in.ConnectorID
	}

	records := h.Backend.ListFileFileTransferResults(connID)
	results := make([]any, len(records))

	for i, r := range records {
		results[i] = map[string]any{
			keyTransferID:  r.TransferID,
			keyConnectorID: r.ConnectorID,
			keyStatus:      r.Status,
			"FilePaths":    r.Files,
		}
	}

	return &map[string]any{"FileTransferResults": results}, nil
}

type describeSecurityPolicyInput struct {
	SecurityPolicyName string `json:"SecurityPolicyName"`
}

func (h *Handler) handleDescribeSecurityPolicy(
	_ context.Context,
	in *describeSecurityPolicyInput,
) (*map[string]any, error) {
	name := in.SecurityPolicyName
	if name == "" {
		name = "TransferSecurityPolicy-2024-01"
	}

	isFIPS := strings.Contains(name, "FIPS")

	ciphers := []string{"aes128-gcm@openssh.com", "aes256-gcm@openssh.com", "aes256-ctr", "aes192-ctr", "aes128-ctr"}
	kexs := []string{"curve25519-sha256", "ecdh-sha2-nistp384", "ecdh-sha2-nistp256", "diffie-hellman-group16-sha512"}
	macs := []string{"hmac-sha2-256-etm@openssh.com", "hmac-sha2-512-etm@openssh.com", "hmac-sha2-256"}
	tlsCiphers := []string{"TLS_AES_256_GCM_SHA384", "TLS_AES_128_GCM_SHA256", "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"}

	if isFIPS {
		ciphers = []string{"aes256-ctr", "aes192-ctr", "aes128-ctr"}
		kexs = []string{"ecdh-sha2-nistp384", "ecdh-sha2-nistp256", "diffie-hellman-group16-sha512"}
		macs = []string{"hmac-sha2-256-etm@openssh.com", "hmac-sha2-512-etm@openssh.com"}
		tlsCiphers = []string{"TLS_AES_256_GCM_SHA384", "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"}
	}

	return &map[string]any{
		"SecurityPolicy": map[string]any{
			"SecurityPolicyName": name,
			"Protocols":          []string{"SFTP"},
			"SshCiphers":         ciphers,
			"SshKexs":            kexs,
			"SshMacs":            macs,
			"TlsCiphers":         tlsCiphers,
		},
	}, nil
}

func (h *Handler) handleListSecurityPolicies(
	_ context.Context,
	_ *struct{},
) (*map[string]any, error) {
	return &map[string]any{
		"SecurityPolicyNames": []string{
			"TransferSecurityPolicy-2024-01",
			"TransferSecurityPolicy-2023-05",
			"TransferSecurityPolicy-2022-03",
			"TransferSecurityPolicy-FIPS-2024-01",
		},
	}, nil
}

type sendWorkflowStepStateInput struct {
	WorkflowID  string `json:"WorkflowId"`
	ExecutionID string `json:"ExecutionId"`
	Token       string `json:"Token"`
	Status      string `json:"Status"`
}

func (h *Handler) handleSendWorkflowStepState(
	_ context.Context,
	in *sendWorkflowStepStateInput,
) (*struct{}, error) {
	if in.WorkflowID == "" {
		return nil, fmt.Errorf("%w: WorkflowId is required", errInvalidRequest)
	}

	if in.ExecutionID == "" {
		return nil, fmt.Errorf("%w: ExecutionId is required", errInvalidRequest)
	}

	if in.Token == "" {
		return nil, fmt.Errorf("%w: Token is required", errInvalidRequest)
	}

	if err := h.Backend.SendWorkflowStepStateRecord(in.WorkflowID, in.ExecutionID, in.Token, in.Status); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type startDirectoryListingInput struct {
	ConnectorID          string   `json:"ConnectorId"`
	OutputDirectoryPath  string   `json:"OutputDirectoryPath,omitempty"`
	RemoteDirectoryPaths []string `json:"RemoteDirectoryPaths"`
	MaxItems             int      `json:"MaxItems,omitempty"`
}

func (h *Handler) handleStartDirectoryListing(
	_ context.Context,
	in *startDirectoryListingInput,
) (*map[string]any, error) {
	if in.ConnectorID == "" {
		return nil, fmt.Errorf("%w: ConnectorId is required", errInvalidRequest)
	}

	opID := h.Backend.StartAsyncOperationRecord(in.ConnectorID, "DIRECTORY_LISTING")

	return &map[string]any{"DirectoryListingId": opID}, nil
}

type startFileTransferInput struct {
	ConnectorID         string   `json:"ConnectorId"`
	LocalDirectoryPath  string   `json:"LocalDirectoryPath,omitempty"`
	RemoteDirectoryPath string   `json:"RemoteDirectoryPath,omitempty"`
	SendFilePaths       []string `json:"SendFilePaths,omitempty"`
	RetrieveFilePaths   []string `json:"RetrieveFilePaths,omitempty"`
}

func (h *Handler) handleStartFileTransfer(
	_ context.Context,
	in *startFileTransferInput,
) (*map[string]any, error) {
	if in.ConnectorID == "" {
		return nil, fmt.Errorf("%w: ConnectorId is required", errInvalidRequest)
	}

	allFiles := append(in.SendFilePaths, in.RetrieveFilePaths...) //nolint:gocritic // intentional append to first slice

	transferID := h.Backend.StartFileFileTransferResult(in.ConnectorID, allFiles)

	return &map[string]any{keyTransferID: transferID}, nil
}

type startRemoteDeleteInput struct {
	ConnectorID string   `json:"ConnectorId"`
	DeletePaths []string `json:"DeletePaths,omitempty"`
}

func (h *Handler) handleStartRemoteDelete(_ context.Context, in *startRemoteDeleteInput) (*map[string]any, error) {
	connID := ""
	if in != nil {
		connID = in.ConnectorID
	}
	opID := h.Backend.StartAsyncOperationRecord(connID, "REMOTE_DELETE")

	return &map[string]any{keyTransferID: opID}, nil
}

type startRemoteMoveInput struct {
	ConnectorID string   `json:"ConnectorId"`
	TargetPath  string   `json:"TargetPath,omitempty"`
	SourcePaths []string `json:"SourcePaths,omitempty"`
}

func (h *Handler) handleStartRemoteMove(_ context.Context, in *startRemoteMoveInput) (*map[string]any, error) {
	connID := ""
	if in != nil {
		connID = in.ConnectorID
	}
	opID := h.Backend.StartAsyncOperationRecord(connID, "REMOTE_MOVE")

	return &map[string]any{keyTransferID: opID}, nil
}

type testConnectionInput struct {
	ConnectorID string `json:"ConnectorId"`
}

func (h *Handler) handleTestConnection(
	_ context.Context,
	in *testConnectionInput,
) (*map[string]any, error) {
	if in.ConnectorID == "" {
		return nil, fmt.Errorf("%w: ConnectorId is required", errInvalidRequest)
	}

	if _, err := h.Backend.DescribeConnector(in.ConnectorID); err != nil {
		return nil, err
	}

	return &map[string]any{
		keyConnectorID:  in.ConnectorID,
		keyStatus:       "OK",
		"StatusMessage": "Connection to remote server is successful",
	}, nil
}

type testIdentityProviderInput struct {
	ServerID       string `json:"ServerId"`
	UserName       string `json:"UserName"`
	UserPassword   string `json:"UserPassword,omitempty"`
	ServerProtocol string `json:"ServerProtocol,omitempty"`
	SourceIP       string `json:"SourceIp,omitempty"`
}

func (h *Handler) handleTestIdentityProvider(
	_ context.Context,
	in *testIdentityProviderInput,
) (*map[string]any, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.UserName == "" {
		return nil, fmt.Errorf("%w: UserName is required", errInvalidRequest)
	}

	statusCode, message := h.Backend.TestIdentityProvider(in.ServerID, in.UserName)

	response := ""
	if statusCode == http.StatusOK {
		response = `{"Role":"arn:aws:iam::000000000000:role/transfer-test-role","HomeDirectory":"/"}`
	}

	return &map[string]any{
		"StatusCode": statusCode,
		"Message":    message,
		"Response":   response,
		keyURL:       "",
	}, nil
}

// tagsToList converts a map of tags to the AWS list format sorted by key.
func tagsToList(tags map[string]string) []map[string]string {
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	list := make([]map[string]string, 0, len(tags))
	for _, k := range keys {
		list = append(list, map[string]string{"Key": k, "Value": tags[k]})
	}

	return list
}

// tagsFromList converts the AWS tag list format to a map.
func tagsFromList(tags []map[string]string) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t["Key"]] = t["Value"]
	}

	return m
}

const defaultTransferMaxResults = 1000

// applyNextTokenItems applies NextToken-based pagination to a slice using the
// shared pkgs/page opaque token format.
func applyNextTokenItems[T any](items []T, nextToken string, maxResults int) ([]T, string) {
	p := page.New(items, nextToken, maxResults, defaultTransferMaxResults)

	return p.Data, p.Next
}
