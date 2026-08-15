package transfer

import (
	"context"
	"fmt"
	"net/http"
)

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
	LoggingRole          string `json:"LoggingRole,omitempty"`
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
			LoggingRole:          s.LoggingRole,
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
