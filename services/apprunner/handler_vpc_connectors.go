package apprunner

import (
	"context"
	"fmt"
)

type createVpcConnectorInput struct {
	VpcConnectorName string     `json:"VpcConnectorName"`
	Subnets          []string   `json:"Subnets"`
	SecurityGroups   []string   `json:"SecurityGroups"`
	Tags             []tagInput `json:"Tags"`
}

type vpcConnectorOutput struct {
	VpcConnectorArn      string   `json:"VpcConnectorArn"`
	VpcConnectorName     string   `json:"VpcConnectorName"`
	Status               string   `json:"Status"`
	Subnets              []string `json:"Subnets"`
	SecurityGroups       []string `json:"SecurityGroups"`
	CreatedAt            int64    `json:"CreatedAt"`
	VpcConnectorRevision int32    `json:"VpcConnectorRevision"`
}

type createVpcConnectorOutput struct {
	VpcConnector vpcConnectorOutput `json:"VpcConnector"`
}

func toVpcConnectorOutput(vc *VpcConnector) vpcConnectorOutput {
	sg := vc.SecurityGroups
	if sg == nil {
		sg = []string{}
	}
	sn := vc.Subnets
	if sn == nil {
		sn = []string{}
	}

	return vpcConnectorOutput{
		VpcConnectorArn:      vc.VpcConnectorArn,
		VpcConnectorName:     vc.VpcConnectorName,
		VpcConnectorRevision: vc.VpcConnectorRevision,
		Status:               vc.Status,
		Subnets:              sn,
		SecurityGroups:       sg,
		CreatedAt:            vc.CreatedAt.Unix(),
	}
}

func (h *Handler) handleCreateVpcConnector(
	_ context.Context,
	in *createVpcConnectorInput,
) (*createVpcConnectorOutput, error) {
	if in.VpcConnectorName == "" {
		return nil, fmt.Errorf("%w: VpcConnectorName is required", errInvalidRequest)
	}

	if len(in.Subnets) == 0 {
		return nil, fmt.Errorf("%w: Subnets is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)
	vc, err := h.Backend.CreateVpcConnector(in.VpcConnectorName, in.Subnets, in.SecurityGroups, tags)
	if err != nil {
		return nil, err
	}

	return &createVpcConnectorOutput{VpcConnector: toVpcConnectorOutput(vc)}, nil
}

type describeVpcConnectorInput struct {
	VpcConnectorArn string `json:"VpcConnectorArn"`
}

type describeVpcConnectorOutput struct {
	VpcConnector vpcConnectorOutput `json:"VpcConnector"`
}

func (h *Handler) handleDescribeVpcConnector(
	_ context.Context,
	in *describeVpcConnectorInput,
) (*describeVpcConnectorOutput, error) {
	if in.VpcConnectorArn == "" {
		return nil, fmt.Errorf("%w: VpcConnectorArn is required", errInvalidRequest)
	}

	vc, err := h.Backend.DescribeVpcConnector(in.VpcConnectorArn)
	if err != nil {
		return nil, err
	}

	return &describeVpcConnectorOutput{VpcConnector: toVpcConnectorOutput(vc)}, nil
}

type deleteVpcConnectorInput struct {
	VpcConnectorArn string `json:"VpcConnectorArn"`
}

type deleteVpcConnectorOutput struct {
	VpcConnector vpcConnectorOutput `json:"VpcConnector"`
}

func (h *Handler) handleDeleteVpcConnector(
	_ context.Context,
	in *deleteVpcConnectorInput,
) (*deleteVpcConnectorOutput, error) {
	if in.VpcConnectorArn == "" {
		return nil, fmt.Errorf("%w: VpcConnectorArn is required", errInvalidRequest)
	}

	vc, err := h.Backend.DeleteVpcConnector(in.VpcConnectorArn)
	if err != nil {
		return nil, err
	}

	return &deleteVpcConnectorOutput{VpcConnector: toVpcConnectorOutput(vc)}, nil
}

type listVpcConnectorsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type listVpcConnectorsOutput struct {
	NextToken     string               `json:"NextToken,omitempty"`
	VpcConnectors []vpcConnectorOutput `json:"VpcConnectors"`
}

func (h *Handler) handleListVpcConnectors(
	_ context.Context,
	in *listVpcConnectorsInput,
) (*listVpcConnectorsOutput, error) {
	vcs, nextToken, err := h.Backend.ListVpcConnectors(in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}

	out := make([]vpcConnectorOutput, 0, len(vcs))
	for _, vc := range vcs {
		out = append(out, toVpcConnectorOutput(vc))
	}

	return &listVpcConnectorsOutput{VpcConnectors: out, NextToken: nextToken}, nil
}
