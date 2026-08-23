package codeconnections

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// vpcConfigurationView is the wire shape of VpcConfiguration
// (aws-sdk-go-v2/service/codeconnections@v1.13.4 types.VpcConfiguration).
type vpcConfigurationView struct {
	VpcID            string   `json:"VpcId"`
	TLSCertificate   string   `json:"TlsCertificate,omitempty"`
	SubnetIDs        []string `json:"SubnetIds"`
	SecurityGroupIDs []string `json:"SecurityGroupIds"`
}

func vpcConfigFromView(v *vpcConfigurationView) *VpcConfiguration {
	if v == nil {
		return nil
	}

	return &VpcConfiguration{
		VpcID:            v.VpcID,
		TLSCertificate:   v.TLSCertificate,
		SubnetIDs:        v.SubnetIDs,
		SecurityGroupIDs: v.SecurityGroupIDs,
	}
}

func vpcConfigToView(v *VpcConfiguration) *vpcConfigurationView {
	if v == nil {
		return nil
	}

	return &vpcConfigurationView{
		VpcID:            v.VpcID,
		TLSCertificate:   v.TLSCertificate,
		SubnetIDs:        v.SubnetIDs,
		SecurityGroupIDs: v.SecurityGroupIDs,
	}
}

type createHostInput struct {
	VpcConfiguration *vpcConfigurationView `json:"VpcConfiguration"`
	Name             string                `json:"Name"`
	ProviderType     string                `json:"ProviderType"`
	ProviderEndpoint string                `json:"ProviderEndpoint"`
	Tags             []tag                 `json:"Tags"`
}

type createHostOutput struct {
	HostArn string `json:"HostArn"`
	Tags    []tag  `json:"Tags,omitempty"`
}

func (h *Handler) handleCreateHost(
	ctx context.Context,
	in *createHostInput,
) (*createHostOutput, error) {
	host, err := h.Backend.CreateHost(
		ctx,
		in.Name,
		in.ProviderType,
		in.ProviderEndpoint,
		vpcConfigFromView(in.VpcConfiguration),
		tagsFromArray(in.Tags),
	)
	if err != nil {
		return nil, err
	}

	return &createHostOutput{HostArn: host.HostArn, Tags: tagsToSortedArray(host.Tags)}, nil
}

type getHostInput struct {
	HostArn string `json:"HostArn"`
}

// getHostOutput is the GetHost response shape. Per aws-sdk-go-v2's
// GetHostOutput struct and its live deserializer
// (awsAwsjson10_deserializeOpDocumentGetHostOutput), the real GetHost
// response has exactly Name/ProviderEndpoint/ProviderType/Status/
// VpcConfiguration -- HostArn is NOT included (the caller already knows it)
// and neither is StatusMessage/Tags, even though the sibling Host type (used
// by ListHosts, see hostItem below) DOES have a StatusMessage field and
// CreateHostOutput DOES have Tags. A previous implementation fabricated
// HostArn/StatusMessage/Tags on this shape by incorrectly generalizing from
// the full Host type, and omitted VpcConfiguration entirely.
type getHostOutput struct {
	VpcConfiguration *vpcConfigurationView `json:"VpcConfiguration,omitempty"`
	Name             string                `json:"Name"`
	ProviderEndpoint string                `json:"ProviderEndpoint"`
	ProviderType     string                `json:"ProviderType"`
	Status           string                `json:"Status"`
}

func (h *Handler) handleGetHost(ctx context.Context, in *getHostInput) (*getHostOutput, error) {
	if in.HostArn == "" {
		return nil, fmt.Errorf("%w: HostArn is required", ErrValidation)
	}

	host, err := h.Backend.GetHost(ctx, in.HostArn)
	if err != nil {
		return nil, err
	}

	return &getHostOutput{
		Name:             host.Name,
		ProviderEndpoint: host.ProviderEndpoint,
		ProviderType:     host.ProviderType,
		Status:           host.Status,
		VpcConfiguration: vpcConfigToView(host.VpcConfiguration),
	}, nil
}

type deleteHostInput struct {
	HostArn string `json:"HostArn"`
}

func (h *Handler) handleDeleteHost(ctx context.Context, in *deleteHostInput) (*emptyOutput, error) {
	if err := h.Backend.DeleteHost(ctx, in.HostArn); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type listHostsInput struct {
	NextToken  *string `json:"NextToken"`
	MaxResults *int32  `json:"MaxResults"`
}

// hostItem is the wire shape of the full Host type (aws-sdk-go-v2/service/
// codeconnections@v1.13.4 types.Host), used by ListHosts. Unlike getHostOutput
// above, Host DOES carry HostArn and StatusMessage -- but it has no Tags
// member at all (confirmed against awsAwsjson10_deserializeDocumentHost's
// case switch), so Tags must not appear here either.
type hostItem struct {
	VpcConfiguration *vpcConfigurationView `json:"VpcConfiguration,omitempty"`
	HostArn          string                `json:"HostArn"`
	Name             string                `json:"Name"`
	ProviderEndpoint string                `json:"ProviderEndpoint"`
	ProviderType     string                `json:"ProviderType"`
	Status           string                `json:"Status"`
	StatusMessage    string                `json:"StatusMessage,omitempty"`
}

type listHostsOutput struct {
	NextToken *string    `json:"NextToken,omitempty"`
	Hosts     []hostItem `json:"Hosts"`
}

func (h *Handler) handleListHosts(
	ctx context.Context,
	in *listHostsInput,
) (*listHostsOutput, error) {
	hosts := h.Backend.ListHosts(ctx)
	items := make([]hostItem, len(hosts))

	for i, host := range hosts {
		items[i] = hostItem{
			HostArn:          host.HostArn,
			Name:             host.Name,
			ProviderEndpoint: host.ProviderEndpoint,
			ProviderType:     host.ProviderType,
			Status:           host.Status,
			StatusMessage:    host.StatusMessage,
			VpcConfiguration: vpcConfigToView(host.VpcConfiguration),
		}
	}

	var limit int
	if in.MaxResults != nil && *in.MaxResults > 0 {
		limit = int(*in.MaxResults)
	}

	token := ""
	if in.NextToken != nil {
		token = *in.NextToken
	}

	p := page.New(items, token, limit, ccDefaultPageSize)

	var nextToken *string
	if p.Next != "" {
		nextToken = &p.Next
	}

	return &listHostsOutput{Hosts: p.Data, NextToken: nextToken}, nil
}

type updateHostInput struct {
	VpcConfiguration *vpcConfigurationView `json:"VpcConfiguration"`
	HostArn          string                `json:"HostArn"`
	ProviderEndpoint string                `json:"ProviderEndpoint"`
}

func (h *Handler) handleUpdateHost(ctx context.Context, in *updateHostInput) (*emptyOutput, error) {
	if in.HostArn == "" {
		return nil, fmt.Errorf("%w: HostArn is required", ErrValidation)
	}

	vpcConfig := vpcConfigFromView(in.VpcConfiguration)
	if err := h.Backend.UpdateHost(ctx, in.HostArn, in.ProviderEndpoint, vpcConfig); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}
