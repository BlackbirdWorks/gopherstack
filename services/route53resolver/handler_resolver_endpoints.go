package route53resolver

import (
	"context"
	"fmt"
	"slices"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	filterFieldDirection        = "Direction"
	filterFieldHostVPCID        = "HostVPCId"
	filterFieldIPAddressCount   = "IpAddressCount"
	filterFieldSecurityGroupIDs = "SecurityGroupIds"
)

// resolverEndpointFilterAliases canonicalizes Filter.Name for
// ListResolverEndpoints. Both forms are accepted per types.Filter's doc
// (aws-sdk-go-v2/service/route53resolver@v1.48.4 types/types.go): "In early
// versions of Resolver, values for Name were listed as uppercase, with
// underscore (_) delimiters ... Uppercase values for Name are still
// supported" -- nothing beyond these two documented forms per name.
//
//nolint:gochecknoglobals // immutable lookup table, same pattern as other services' dispatch/alias tables
var resolverEndpointFilterAliases = map[string]string{
	filterFieldCreatorRequestID:  filterFieldCreatorRequestID,
	legacyFilterCreatorRequestID: filterFieldCreatorRequestID,
	filterFieldDirection:         filterFieldDirection,
	"DIRECTION":                  filterFieldDirection,
	filterFieldHostVPCID:         filterFieldHostVPCID,
	"HOST_VPC_ID":                filterFieldHostVPCID,
	filterFieldIPAddressCount:    filterFieldIPAddressCount,
	"IP_ADDRESS_COUNT":           filterFieldIPAddressCount,
	filterFieldName:              filterFieldName,
	legacyFilterName:             filterFieldName,
	filterFieldSecurityGroupIDs:  filterFieldSecurityGroupIDs,
	"SECURITY_GROUP_IDS":         filterFieldSecurityGroupIDs,
	filterFieldStatus:            filterFieldStatus,
	legacyFilterStatus:           filterFieldStatus,
}

func matchResolverEndpointFilter(ep *ResolverEndpoint, name string, values []string) bool {
	switch name {
	case filterFieldCreatorRequestID:
		return slices.Contains(values, ep.CreatorRequestID)
	case filterFieldDirection:
		return slices.Contains(values, ep.Direction)
	case filterFieldHostVPCID:
		return slices.Contains(values, ep.HostVPCID)
	case filterFieldIPAddressCount:
		//nolint:gosec // conversion is safe: IP count is always small
		return slices.Contains(values, int32ToString(int32(len(ep.IPAddresses))))
	case filterFieldName:
		return slices.Contains(values, ep.Name)
	case filterFieldSecurityGroupIDs:
		return containsAny(ep.SecurityGroupIDs, values)
	case filterFieldStatus:
		return slices.Contains(values, ep.Status)
	default:
		return false
	}
}

type resolverEndpointIDInput struct {
	ResolverEndpointID string `json:"ResolverEndpointId"`
}

// resolverEndpointIPAddress holds the subnet and IP for a resolver endpoint IP address.
type resolverEndpointIPAddress struct {
	SubnetID string `json:"SubnetId"`
	IP       string `json:"Ip"`
	Ipv6     string `json:"Ipv6,omitempty"`
}

type resolverEndpointIPAddressDetail struct {
	IPID     string `json:"IpId"`
	SubnetID string `json:"SubnetId"`
	IP       string `json:"Ip"`
	Ipv6     string `json:"Ipv6,omitempty"`
	Status   string `json:"Status"`
}

type listResolverEndpointIPAddressesInput struct {
	NextToken          string `json:"NextToken"`
	ResolverEndpointID string `json:"ResolverEndpointId"`
	MaxResults         int32  `json:"MaxResults"`
}

type listResolverEndpointIPAddressesOutput struct {
	NextToken   *string                           `json:"NextToken,omitempty"`
	IPAddresses []resolverEndpointIPAddressDetail `json:"IpAddresses"`
}

type handleCreateResolverEndpointInput struct {
	RniEnhancedMetricsEnabled      *bool                       `json:"RniEnhancedMetricsEnabled,omitempty"`
	TargetNameServerMetricsEnabled *bool                       `json:"TargetNameServerMetricsEnabled,omitempty"`
	Direction                      string                      `json:"Direction"`
	VpcID                          string                      `json:"VpcId"`
	Name                           string                      `json:"Name"`
	ResolverEndpointType           string                      `json:"ResolverEndpointType"`
	OutpostArn                     string                      `json:"OutpostArn"`
	PreferredInstanceType          string                      `json:"PreferredInstanceType"`
	CreatorRequestID               string                      `json:"CreatorRequestId"`
	SecurityGroupIDs               []string                    `json:"SecurityGroupIds"`
	IPAddresses                    []resolverEndpointIPAddress `json:"IpAddresses"`
	Tags                           []svcTags.KV                `json:"Tags"`
	Protocols                      []string                    `json:"Protocols"`
}

// resolverEndpointOutput is the wire shape of types.ResolverEndpoint. Note: the
// real SDK type does NOT carry an IpAddresses list -- IPs are only obtainable
// via the separate ListResolverEndpointIpAddresses call. An earlier revision of
// this handler invented an IpAddresses field on this struct; it has been
// removed (see resolver_endpoints_test.go and PARITY.md for the fix note).
type resolverEndpointOutput struct {
	ID                             string   `json:"Id"`
	Arn                            string   `json:"Arn"`
	Name                           string   `json:"Name"`
	Direction                      string   `json:"Direction"`
	Status                         string   `json:"Status"`
	StatusMessage                  string   `json:"StatusMessage,omitempty"`
	VpcID                          string   `json:"VpcId"`
	HostVPCId                      string   `json:"HostVPCId"`
	ResolverEndpointType           string   `json:"ResolverEndpointType"`
	OutpostArn                     string   `json:"OutpostArn,omitempty"`
	PreferredInstanceType          string   `json:"PreferredInstanceType,omitempty"`
	CreatorRequestID               string   `json:"CreatorRequestId,omitempty"`
	CreationTime                   string   `json:"CreationTime,omitempty"`
	ModificationTime               string   `json:"ModificationTime,omitempty"`
	SecurityGroupIDs               []string `json:"SecurityGroupIds"`
	Protocols                      []string `json:"Protocols,omitempty"`
	IPAddressCount                 int32    `json:"IpAddressCount"`
	RniEnhancedMetricsEnabled      bool     `json:"RniEnhancedMetricsEnabled"`
	TargetNameServerMetricsEnabled bool     `json:"TargetNameServerMetricsEnabled"`
}

type createResolverEndpointOutput struct {
	ResolverEndpoint resolverEndpointOutput `json:"ResolverEndpoint"`
}

type deleteResolverEndpointOutput struct{}

type listResolverEndpointsInput struct {
	NextToken  string       `json:"NextToken"`
	Filters    []wireFilter `json:"Filters"`
	MaxResults int32        `json:"MaxResults"`
}

type listResolverEndpointsOutput struct {
	NextToken         *string                  `json:"NextToken,omitempty"`
	ResolverEndpoints []resolverEndpointOutput `json:"ResolverEndpoints"`
}

type getResolverEndpointOutput struct {
	ResolverEndpoint resolverEndpointOutput `json:"ResolverEndpoint"`
}

func endpointToOutput(ep *ResolverEndpoint) resolverEndpointOutput {
	sgIDs := ep.SecurityGroupIDs
	if sgIDs == nil {
		sgIDs = []string{}
	}

	epType := ep.ResolverEndpointType
	if epType == "" {
		epType = endpointTypeIPV4
	}

	//nolint:gosec // conversion is safe: IP count is always small
	ipCount := int32(len(ep.IPAddresses))

	return resolverEndpointOutput{
		ID:                             ep.ID,
		Arn:                            ep.ARN,
		Name:                           ep.Name,
		Direction:                      ep.Direction,
		Status:                         ep.Status,
		StatusMessage:                  ep.StatusMessage,
		VpcID:                          ep.VpcID,
		HostVPCId:                      ep.HostVPCID,
		ResolverEndpointType:           epType,
		IPAddressCount:                 ipCount,
		SecurityGroupIDs:               sgIDs,
		Protocols:                      ep.Protocols,
		OutpostArn:                     ep.OutpostArn,
		PreferredInstanceType:          ep.PreferredInstanceType,
		CreatorRequestID:               ep.CreatorRequestID,
		CreationTime:                   ep.CreationTime,
		ModificationTime:               ep.ModificationTime,
		RniEnhancedMetricsEnabled:      ep.RniEnhancedMetricsEnabled,
		TargetNameServerMetricsEnabled: ep.TargetNameServerMetricsEnabled,
	}
}

func (h *Handler) handleCreateResolverEndpoint(
	ctx context.Context,
	in *handleCreateResolverEndpointInput,
) (*createResolverEndpointOutput, error) {
	ips := make([]IPAddress, 0, len(in.IPAddresses))
	for _, ip := range in.IPAddresses {
		ips = append(ips, IPAddress{SubnetID: ip.SubnetID, IP: ip.IP, Ipv6: ip.Ipv6})
	}

	ep, err := h.Backend.CreateResolverEndpoint(
		ctx,
		in.Name, in.Direction, in.VpcID, ips, in.SecurityGroupIDs, in.ResolverEndpointType,
		in.Protocols, in.OutpostArn, in.PreferredInstanceType, in.CreatorRequestID,
		boolValue(in.RniEnhancedMetricsEnabled), boolValue(in.TargetNameServerMetricsEnabled),
	)
	if err != nil {
		return nil, err
	}

	// Store tags if provided.
	if len(in.Tags) > 0 {
		tagErr := h.Backend.TagResource(ctx, ep.ARN, in.Tags)
		if tagErr != nil {
			return nil, tagErr
		}
	}

	return &createResolverEndpointOutput{ResolverEndpoint: endpointToOutput(ep)}, nil
}

func (h *Handler) handleDeleteResolverEndpoint(
	ctx context.Context,
	in *resolverEndpointIDInput,
) (*deleteResolverEndpointOutput, error) {
	if err := h.Backend.DeleteResolverEndpoint(ctx, in.ResolverEndpointID); err != nil {
		return nil, err
	}

	return &deleteResolverEndpointOutput{}, nil
}

func (h *Handler) handleListResolverEndpoints(
	ctx context.Context,
	in *listResolverEndpointsInput,
) (*listResolverEndpointsOutput, error) {
	eps := h.Backend.ListResolverEndpoints(ctx)
	eps, err := applyFilters(eps, in.Filters, resolverEndpointFilterAliases, matchResolverEndpointFilter)
	if err != nil {
		return nil, err
	}
	items := make([]resolverEndpointOutput, 0, len(eps))
	for _, ep := range eps {
		items = append(items, endpointToOutput(ep))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	data, next := paginate(items, in.NextToken, in.MaxResults, defaultPageSizeSmall)

	return &listResolverEndpointsOutput{ResolverEndpoints: data, NextToken: next}, nil
}

func (h *Handler) handleGetResolverEndpoint(
	ctx context.Context,
	in *resolverEndpointIDInput,
) (*getResolverEndpointOutput, error) {
	ep, err := h.Backend.GetResolverEndpoint(ctx, in.ResolverEndpointID)
	if err != nil {
		return nil, err
	}

	return &getResolverEndpointOutput{ResolverEndpoint: endpointToOutput(ep)}, nil
}

func (h *Handler) handleListResolverEndpointIPAddresses(
	ctx context.Context,
	in *listResolverEndpointIPAddressesInput,
) (*listResolverEndpointIPAddressesOutput, error) {
	ips, err := h.Backend.ListResolverEndpointIPAddresses(ctx, in.ResolverEndpointID)
	if err != nil {
		return nil, err
	}
	items := make([]resolverEndpointIPAddressDetail, 0, len(ips))
	for _, ip := range ips {
		items = append(items, resolverEndpointIPAddressDetail{
			IPID:     ip.IPID,
			SubnetID: ip.SubnetID,
			IP:       ip.IP,
			Ipv6:     ip.Ipv6,
			Status:   "ATTACHED",
		})
	}
	data, next := paginate(items, in.NextToken, in.MaxResults, defaultPageSizeLarge)
	out := &listResolverEndpointIPAddressesOutput{IPAddresses: data, NextToken: next}

	return out, nil
}

type ipAddressUpdateInput struct {
	SubnetID string `json:"SubnetId"`
	IP       string `json:"Ip"`
	Ipv6     string `json:"Ipv6,omitempty"`
}

type associateResolverEndpointIPAddressInput struct {
	ResolverEndpointID string               `json:"ResolverEndpointId"`
	IPAddress          ipAddressUpdateInput `json:"IpAddress"`
}

type associateResolverEndpointIPAddressOutput struct {
	ResolverEndpoint resolverEndpointOutput `json:"ResolverEndpoint"`
}

func (h *Handler) handleAssociateResolverEndpointIPAddress(
	ctx context.Context,
	in *associateResolverEndpointIPAddressInput,
) (*associateResolverEndpointIPAddressOutput, error) {
	if in.ResolverEndpointID == "" {
		return nil, fmt.Errorf("%w: ResolverEndpointId is required", ErrValidation)
	}

	ep, err := h.Backend.AssociateResolverEndpointIPAddress(
		ctx, in.ResolverEndpointID, in.IPAddress.SubnetID, in.IPAddress.IP, in.IPAddress.Ipv6,
	)
	if err != nil {
		return nil, err
	}

	return &associateResolverEndpointIPAddressOutput{ResolverEndpoint: endpointToOutput(ep)}, nil
}

// --- CreateResolverQueryLogConfig ---

type updateResolverEndpointInput struct {
	RniEnhancedMetricsEnabled      *bool    `json:"RniEnhancedMetricsEnabled,omitempty"`
	TargetNameServerMetricsEnabled *bool    `json:"TargetNameServerMetricsEnabled,omitempty"`
	ResolverEndpointID             string   `json:"ResolverEndpointId"`
	Name                           string   `json:"Name"`
	ResolverEndpointType           string   `json:"ResolverEndpointType"`
	Protocols                      []string `json:"Protocols"`
}

type updateResolverEndpointOutput struct {
	ResolverEndpoint resolverEndpointOutput `json:"ResolverEndpoint"`
}

func (h *Handler) handleUpdateResolverEndpoint(
	ctx context.Context,
	in *updateResolverEndpointInput,
) (*updateResolverEndpointOutput, error) {
	if in.ResolverEndpointID == "" {
		return nil, fmt.Errorf("%w: ResolverEndpointId is required", ErrValidation)
	}
	ep, err := h.Backend.UpdateResolverEndpoint(
		ctx,
		in.ResolverEndpointID,
		in.Name,
		in.ResolverEndpointType,
		in.Protocols,
		in.RniEnhancedMetricsEnabled,
		in.TargetNameServerMetricsEnabled,
	)
	if err != nil {
		return nil, err
	}

	return &updateResolverEndpointOutput{ResolverEndpoint: endpointToOutput(ep)}, nil
}

// --- DisassociateResolverEndpointIpAddress ---

type disassociateResolverEndpointIPAddressInput struct {
	ResolverEndpointID string               `json:"ResolverEndpointId"`
	IPAddress          ipAddressRemoveInput `json:"IpAddress"`
}

type ipAddressRemoveInput struct {
	IPID     string `json:"IpId"`
	IP       string `json:"Ip"`
	SubnetID string `json:"SubnetId"`
}

type disassociateResolverEndpointIPAddressOutput struct {
	ResolverEndpoint resolverEndpointOutput `json:"ResolverEndpoint"`
}

func (h *Handler) handleDisassociateResolverEndpointIPAddress(
	ctx context.Context,
	in *disassociateResolverEndpointIPAddressInput,
) (*disassociateResolverEndpointIPAddressOutput, error) {
	if in.ResolverEndpointID == "" {
		return nil, fmt.Errorf("%w: ResolverEndpointId is required", ErrValidation)
	}
	if in.IPAddress.IPID == "" {
		return nil, fmt.Errorf("%w: IpAddress.IpId is required", ErrValidation)
	}
	ep, err := h.Backend.DisassociateResolverEndpointIPAddress(
		ctx,
		in.ResolverEndpointID,
		in.IPAddress.IPID,
	)
	if err != nil {
		return nil, err
	}

	return &disassociateResolverEndpointIPAddressOutput{ResolverEndpoint: endpointToOutput(ep)}, nil
}

// --- UpdateResolverRule ---

func (h *Handler) opsResolverEndpoints() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"AssociateResolverEndpointIpAddress": service.WrapOp(
			h.handleAssociateResolverEndpointIPAddress,
		),
		"CreateResolverEndpoint": service.WrapOp(h.handleCreateResolverEndpoint),
		"DeleteResolverEndpoint": service.WrapOp(h.handleDeleteResolverEndpoint),
		"DisassociateResolverEndpointIpAddress": service.WrapOp(
			h.handleDisassociateResolverEndpointIPAddress,
		),
		"GetResolverEndpoint": service.WrapOp(h.handleGetResolverEndpoint),
		"ListResolverEndpointIpAddresses": service.WrapOp(
			h.handleListResolverEndpointIPAddresses,
		),
		"ListResolverEndpoints":  service.WrapOp(h.handleListResolverEndpoints),
		"UpdateResolverEndpoint": service.WrapOp(h.handleUpdateResolverEndpoint),
	}
}
