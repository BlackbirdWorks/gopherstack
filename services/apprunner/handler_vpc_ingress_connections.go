package apprunner

import (
	"context"
	"fmt"
)

type ingressVpcConfigurationInput struct {
	VpcID         string `json:"VpcId"`
	VpcEndpointID string `json:"VpcEndpointId"`
}

type createVpcIngressConnectionInput struct {
	VpcIngressConnectionName string                        `json:"VpcIngressConnectionName"`
	ServiceArn               string                        `json:"ServiceArn"`
	IngressVpcConfiguration  *ingressVpcConfigurationInput `json:"IngressVpcConfiguration"`
	Tags                     []tagInput                    `json:"Tags"`
}

type ingressVpcConfigurationOutput struct {
	VpcID         string `json:"VpcId"`
	VpcEndpointID string `json:"VpcEndpointId"`
}

type vpcIngressConnectionOutput struct {
	DeletedAt                *int64                        `json:"DeletedAt,omitempty"`
	IngressVpcConfiguration  ingressVpcConfigurationOutput `json:"IngressVpcConfiguration"`
	VpcIngressConnectionArn  string                        `json:"VpcIngressConnectionArn"`
	VpcIngressConnectionName string                        `json:"VpcIngressConnectionName"`
	ServiceArn               string                        `json:"ServiceArn"`
	AccountID                string                        `json:"AccountId"`
	DomainName               string                        `json:"DomainName"`
	Status                   string                        `json:"Status"`
	CreatedAt                int64                         `json:"CreatedAt"`
}

type createVpcIngressConnectionOutput struct {
	VpcIngressConnection vpcIngressConnectionOutput `json:"VpcIngressConnection"`
}

func toVpcIngressConnectionOutput(v *VpcIngressConnection) vpcIngressConnectionOutput {
	out := vpcIngressConnectionOutput{
		VpcIngressConnectionArn:  v.VpcIngressConnectionArn,
		VpcIngressConnectionName: v.VpcIngressConnectionName,
		ServiceArn:               v.ServiceArn,
		AccountID:                v.AccountID,
		DomainName:               v.DomainName,
		Status:                   v.Status,
		IngressVpcConfiguration: ingressVpcConfigurationOutput{
			VpcID:         v.VpcID,
			VpcEndpointID: v.VpcEndpointID,
		},
		CreatedAt: v.CreatedAt.Unix(),
	}

	if !v.DeletedAt.IsZero() {
		deletedAt := v.DeletedAt.Unix()
		out.DeletedAt = &deletedAt
	}

	return out
}

func (h *Handler) handleCreateVpcIngressConnection(
	_ context.Context,
	in *createVpcIngressConnectionInput,
) (*createVpcIngressConnectionOutput, error) {
	if in.VpcIngressConnectionName == "" {
		return nil, fmt.Errorf("%w: VpcIngressConnectionName is required", errInvalidRequest)
	}

	if in.ServiceArn == "" {
		return nil, fmt.Errorf("%w: ServiceArn is required", errInvalidRequest)
	}

	var vpcID, vpcEndpointID string
	if in.IngressVpcConfiguration != nil {
		vpcID = in.IngressVpcConfiguration.VpcID
		vpcEndpointID = in.IngressVpcConfiguration.VpcEndpointID
	}

	tags := tagsFromInput(in.Tags)
	vic, err := h.Backend.CreateVpcIngressConnection(
		in.VpcIngressConnectionName, in.ServiceArn, vpcID, vpcEndpointID, tags,
	)
	if err != nil {
		return nil, err
	}

	return &createVpcIngressConnectionOutput{VpcIngressConnection: toVpcIngressConnectionOutput(vic)}, nil
}

type describeVpcIngressConnectionInput struct {
	VpcIngressConnectionArn string `json:"VpcIngressConnectionArn"`
}

type describeVpcIngressConnectionOutput struct {
	VpcIngressConnection vpcIngressConnectionOutput `json:"VpcIngressConnection"`
}

func (h *Handler) handleDescribeVpcIngressConnection(
	_ context.Context,
	in *describeVpcIngressConnectionInput,
) (*describeVpcIngressConnectionOutput, error) {
	if in.VpcIngressConnectionArn == "" {
		return nil, fmt.Errorf("%w: VpcIngressConnectionArn is required", errInvalidRequest)
	}

	vic, err := h.Backend.DescribeVpcIngressConnection(in.VpcIngressConnectionArn)
	if err != nil {
		return nil, err
	}

	return &describeVpcIngressConnectionOutput{VpcIngressConnection: toVpcIngressConnectionOutput(vic)}, nil
}

type deleteVpcIngressConnectionInput struct {
	VpcIngressConnectionArn string `json:"VpcIngressConnectionArn"`
}

type deleteVpcIngressConnectionOutput struct {
	VpcIngressConnection vpcIngressConnectionOutput `json:"VpcIngressConnection"`
}

func (h *Handler) handleDeleteVpcIngressConnection(
	_ context.Context,
	in *deleteVpcIngressConnectionInput,
) (*deleteVpcIngressConnectionOutput, error) {
	if in.VpcIngressConnectionArn == "" {
		return nil, fmt.Errorf("%w: VpcIngressConnectionArn is required", errInvalidRequest)
	}

	vic, err := h.Backend.DeleteVpcIngressConnection(in.VpcIngressConnectionArn)
	if err != nil {
		return nil, err
	}

	return &deleteVpcIngressConnectionOutput{VpcIngressConnection: toVpcIngressConnectionOutput(vic)}, nil
}

type listVpcIngressConnectionsFilterInput struct {
	ServiceArn              string `json:"ServiceArn"`
	VpcIngressConnectionArn string `json:"VpcIngressConnectionArn"`
}

type listVpcIngressConnectionsInput struct {
	Filter     *listVpcIngressConnectionsFilterInput `json:"Filter"`
	NextToken  string                                `json:"NextToken"`
	MaxResults int32                                 `json:"MaxResults"`
}

type vpcIngressConnectionSummaryOutput struct {
	VpcIngressConnectionArn string `json:"VpcIngressConnectionArn"`
	ServiceArn              string `json:"ServiceArn"`
}

type listVpcIngressConnectionsOutput struct {
	NextToken                       string                              `json:"NextToken,omitempty"`
	VpcIngressConnectionSummaryList []vpcIngressConnectionSummaryOutput `json:"VpcIngressConnectionSummaryList"`
}

func (h *Handler) handleListVpcIngressConnections(
	_ context.Context,
	in *listVpcIngressConnectionsInput,
) (*listVpcIngressConnectionsOutput, error) {
	var serviceArnFilter, connArnFilter string
	if in.Filter != nil {
		serviceArnFilter = in.Filter.ServiceArn
		connArnFilter = in.Filter.VpcIngressConnectionArn
	}

	vics, nextToken, err := h.Backend.ListVpcIngressConnections(
		serviceArnFilter, connArnFilter, in.MaxResults, in.NextToken,
	)
	if err != nil {
		return nil, err
	}

	out := make([]vpcIngressConnectionSummaryOutput, 0, len(vics))
	for _, v := range vics {
		out = append(out, vpcIngressConnectionSummaryOutput{
			VpcIngressConnectionArn: v.VpcIngressConnectionArn,
			ServiceArn:              v.ServiceArn,
		})
	}

	return &listVpcIngressConnectionsOutput{
		VpcIngressConnectionSummaryList: out,
		NextToken:                       nextToken,
	}, nil
}

type updateVpcIngressConnectionInput struct {
	IngressVpcConfiguration *ingressVpcConfigurationInput `json:"IngressVpcConfiguration"`
	VpcIngressConnectionArn string                        `json:"VpcIngressConnectionArn"`
}

type updateVpcIngressConnectionOutput struct {
	VpcIngressConnection vpcIngressConnectionOutput `json:"VpcIngressConnection"`
}

func (h *Handler) handleUpdateVpcIngressConnection(
	_ context.Context,
	in *updateVpcIngressConnectionInput,
) (*updateVpcIngressConnectionOutput, error) {
	if in.VpcIngressConnectionArn == "" {
		return nil, fmt.Errorf("%w: VpcIngressConnectionArn is required", errInvalidRequest)
	}

	var vpcID, vpcEndpointID string
	if in.IngressVpcConfiguration != nil {
		vpcID = in.IngressVpcConfiguration.VpcID
		vpcEndpointID = in.IngressVpcConfiguration.VpcEndpointID
	}

	vic, err := h.Backend.UpdateVpcIngressConnection(in.VpcIngressConnectionArn, vpcID, vpcEndpointID)
	if err != nil {
		return nil, err
	}

	return &updateVpcIngressConnectionOutput{VpcIngressConnection: toVpcIngressConnectionOutput(vic)}, nil
}
