package codestarconnections

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type vpcConfigurationView struct {
	VpcID            string   `json:"VpcId"`
	TLSCertificate   string   `json:"TlsCertificate,omitempty"`
	SubnetIDs        []string `json:"SubnetIds"`
	SecurityGroupIDs []string `json:"SecurityGroupIds"`
}

type createHostInput struct {
	Name             string                `json:"Name"`
	ProviderType     string                `json:"ProviderType"`
	ProviderEndpoint string                `json:"ProviderEndpoint"`
	VpcConfiguration *vpcConfigurationView `json:"VpcConfiguration"`
	Tags             []tagEntry            `json:"Tags"`
}

type createHostOutput struct {
	HostArn string     `json:"HostArn"`
	Tags    []tagEntry `json:"Tags,omitempty"`
}

func (h *Handler) handleCreateHost(
	ctx context.Context,
	in *createHostInput,
) (*createHostOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	host, err := h.Backend.CreateHost(
		ctx, in.Name, in.ProviderType, in.ProviderEndpoint,
		vpcConfigFromView(in.VpcConfiguration), tagsFromArray(in.Tags),
	)
	if err != nil {
		return nil, err
	}

	return &createHostOutput{
		HostArn: host.HostArn,
		Tags:    tagsToSortedArray(host.Tags),
	}, nil
}

type getHostInput struct {
	HostArn string `json:"HostArn"`
}

// getHostView is the GetHost response shape. Per aws-sdk-go-v2's
// GetHostOutput struct and its deserializer
// (awsAwsjson10_deserializeOpDocumentGetHostOutput), the real GetHost
// response has exactly Name/ProviderEndpoint/ProviderType/Status/
// VpcConfiguration -- HostArn is NOT included (caller already knows it) and,
// notably, neither is StatusMessage, even though the sibling Host type
// (used by ListHosts, see listHostView below) DOES have a StatusMessage
// field. This is a genuine asymmetry in the real API, not a gopherstack
// omission: a previous implementation incorrectly added StatusMessage to
// this shape by generalizing from the Host type.
type getHostView struct {
	VpcConfiguration *vpcConfigurationView `json:"VpcConfiguration,omitempty"`
	Name             string                `json:"Name"`
	ProviderType     string                `json:"ProviderType"`
	ProviderEndpoint string                `json:"ProviderEndpoint"`
	Status           string                `json:"Status"`
}

// listHostView is the ListHosts per-item shape — includes HostArn.
type listHostView struct {
	Name             string                `json:"Name"`
	HostArn          string                `json:"HostArn"`
	ProviderType     string                `json:"ProviderType"`
	ProviderEndpoint string                `json:"ProviderEndpoint"`
	Status           string                `json:"Status"`
	VpcConfiguration *vpcConfigurationView `json:"VpcConfiguration,omitempty"`
	StatusMessage    string                `json:"StatusMessage,omitempty"`
}

type getHostOutput struct {
	getHostView
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

func hostToGetView(h *Host) getHostView {
	return getHostView{
		Name:             h.Name,
		ProviderType:     h.ProviderType,
		ProviderEndpoint: h.ProviderEndpoint,
		Status:           h.Status,
		VpcConfiguration: vpcConfigToView(h.VpcConfiguration),
	}
}

func hostToListView(h *Host) listHostView {
	return listHostView{
		Name:             h.Name,
		HostArn:          h.HostArn,
		ProviderType:     h.ProviderType,
		ProviderEndpoint: h.ProviderEndpoint,
		Status:           h.Status,
		StatusMessage:    h.StatusMessage,
		VpcConfiguration: vpcConfigToView(h.VpcConfiguration),
	}
}

func (h *Handler) handleGetHost(
	ctx context.Context,
	in *getHostInput,
) (*getHostOutput, error) {
	if in.HostArn == "" {
		return nil, fmt.Errorf("%w: HostArn is required", errInvalidRequest)
	}

	host, err := h.Backend.GetHost(ctx, in.HostArn)
	if err != nil {
		return nil, err
	}

	return &getHostOutput{hostToGetView(host)}, nil
}

type listHostsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listHostsOutput struct {
	NextToken string         `json:"NextToken,omitempty"`
	Hosts     []listHostView `json:"Hosts"`
}

func (h *Handler) handleListHosts(
	ctx context.Context,
	in *listHostsInput,
) (*listHostsOutput, error) {
	hosts := h.Backend.ListHosts(ctx)

	views := make([]listHostView, len(hosts))
	for i, host := range hosts {
		views[i] = hostToListView(host)
	}

	p := page.New(views, in.NextToken, in.MaxResults, defaultCSCMaxResults)

	return &listHostsOutput{Hosts: p.Data, NextToken: p.Next}, nil
}

type deleteHostInput struct {
	HostArn string `json:"HostArn"`
}

type deleteHostOutput struct{}

func (h *Handler) handleDeleteHost(
	ctx context.Context,
	in *deleteHostInput,
) (*deleteHostOutput, error) {
	if in.HostArn == "" {
		return nil, fmt.Errorf("%w: HostArn is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteHost(ctx, in.HostArn); err != nil {
		return nil, err
	}

	return &deleteHostOutput{}, nil
}

type updateHostInput struct {
	VpcConfiguration *vpcConfigurationView `json:"VpcConfiguration"`
	HostArn          string                `json:"HostArn"`
	ProviderEndpoint string                `json:"ProviderEndpoint"`
}

type updateHostOutput struct{}

func (h *Handler) handleUpdateHost(
	ctx context.Context,
	in *updateHostInput,
) (*updateHostOutput, error) {
	if in.HostArn == "" {
		return nil, fmt.Errorf("%w: HostArn is required", errInvalidRequest)
	}

	vpcCfg := vpcConfigFromView(in.VpcConfiguration)
	if err := h.Backend.UpdateHost(ctx, in.HostArn, in.ProviderEndpoint, vpcCfg); err != nil {
		return nil, err
	}

	return &updateHostOutput{}, nil
}
