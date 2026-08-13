package glue

import (
	"context"
)

type batchGetDevEndpointsInput struct {
	DevEndpointNames []string `json:"DevEndpointNames"`
}

type batchGetDevEndpointsOutput struct {
	DevEndpoints         []*DevEndpoint `json:"DevEndpoints"`
	DevEndpointsNotFound []string       `json:"DevEndpointsNotFound"`
}

func (h *Handler) handleBatchGetDevEndpoints(
	_ context.Context,
	in *batchGetDevEndpointsInput,
) (*batchGetDevEndpointsOutput, error) {
	found, missing := h.Backend.BatchGetDevEndpoints(in.DevEndpointNames)

	return &batchGetDevEndpointsOutput{DevEndpoints: found, DevEndpointsNotFound: missing}, nil
}

// createDevEndpointInput holds input for CreateDevEndpoint.
type createDevEndpointInput struct {
	Arguments             map[string]string `json:"Arguments,omitempty"`
	Tags                  map[string]string `json:"Tags,omitempty"`
	GlueVersion           string            `json:"GlueVersion,omitempty"`
	EndpointName          string            `json:"EndpointName"`
	RoleArn               string            `json:"RoleArn"`
	SubnetID              string            `json:"SubnetId,omitempty"`
	PublicKey             string            `json:"PublicKey,omitempty"`
	WorkerType            string            `json:"WorkerType,omitempty"`
	ExtraPythonLibsS3Path string            `json:"ExtraPythonLibsS3Path,omitempty"`
	ExtraJarsS3Path       string            `json:"ExtraJarsS3Path,omitempty"`
	SecurityConfiguration string            `json:"SecurityConfiguration,omitempty"`
	PublicKeys            []string          `json:"PublicKeys,omitempty"`
	SecurityGroupIDs      []string          `json:"SecurityGroupIds,omitempty"`
	NumberOfNodes         int               `json:"NumberOfNodes,omitempty"`
	NumberOfWorkers       int               `json:"NumberOfWorkers,omitempty"`
}

// createDevEndpointOutput holds the result for CreateDevEndpoint.
type createDevEndpointOutput struct {
	Arguments             map[string]string `json:"Arguments,omitempty"`
	WorkerType            string            `json:"WorkerType,omitempty"`
	AvailabilityZone      string            `json:"AvailabilityZone,omitempty"`
	Status                string            `json:"Status"`
	RoleArn               string            `json:"RoleArn,omitempty"`
	SubnetID              string            `json:"SubnetId,omitempty"`
	SecurityConfiguration string            `json:"SecurityConfiguration,omitempty"`
	GlueVersion           string            `json:"GlueVersion,omitempty"`
	EndpointName          string            `json:"EndpointName"`
	VpcID                 string            `json:"VpcId,omitempty"`
	YarnEndpointAddress   string            `json:"YarnEndpointAddress,omitempty"`
	SecurityGroupIDs      []string          `json:"SecurityGroupIds,omitempty"`
	NumberOfNodes         int               `json:"NumberOfNodes,omitempty"`
	NumberOfWorkers       int               `json:"NumberOfWorkers,omitempty"`
	CreatedTimestamp      float64           `json:"CreatedTimestamp,omitempty"`
}

func (h *Handler) handleCreateDevEndpoint(
	_ context.Context,
	in *createDevEndpointInput,
) (*createDevEndpointOutput, error) {
	dep, err := h.Backend.CreateDevEndpoint(in.EndpointName, DevEndpointInput{
		Arguments:             in.Arguments,
		SecurityGroupIDs:      in.SecurityGroupIDs,
		PublicKeys:            in.PublicKeys,
		SubnetID:              in.SubnetID,
		PublicKey:             in.PublicKey,
		WorkerType:            in.WorkerType,
		GlueVersion:           in.GlueVersion,
		ExtraPythonLibsS3Path: in.ExtraPythonLibsS3Path,
		ExtraJarsS3Path:       in.ExtraJarsS3Path,
		SecurityConfiguration: in.SecurityConfiguration,
		NumberOfNodes:         in.NumberOfNodes,
		NumberOfWorkers:       in.NumberOfWorkers,
	}, in.RoleArn, in.Tags)
	if err != nil {
		return nil, err
	}

	return &createDevEndpointOutput{
		EndpointName:          dep.EndpointName,
		Status:                dep.Status,
		Arguments:             dep.Arguments,
		SecurityGroupIDs:      dep.SecurityGroupIDs,
		RoleArn:               dep.RoleArn,
		SubnetID:              dep.SubnetID,
		WorkerType:            dep.WorkerType,
		GlueVersion:           dep.GlueVersion,
		AvailabilityZone:      dep.AvailabilityZone,
		VpcID:                 dep.VpcID,
		YarnEndpointAddress:   dep.YarnEndpointAddress,
		SecurityConfiguration: dep.SecurityConfiguration,
		NumberOfNodes:         dep.NumberOfNodes,
		NumberOfWorkers:       dep.NumberOfWorkers,
		CreatedTimestamp:      dep.CreatedTimestamp,
	}, nil
}

// deleteDevEndpointInput holds input for DeleteDevEndpoint.
type deleteDevEndpointInput struct {
	EndpointName string `json:"EndpointName"`
}

func (h *Handler) handleDeleteDevEndpoint(
	_ context.Context,
	in *deleteDevEndpointInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeleteDevEndpoint(in.EndpointName); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// getDevEndpointInput holds input for GetDevEndpoint.
type getDevEndpointInput struct {
	EndpointName string `json:"EndpointName"`
}

// getDevEndpointOutput holds the result for GetDevEndpoint.
type getDevEndpointOutput struct {
	DevEndpoint *DevEndpoint `json:"DevEndpoint"`
}

func (h *Handler) handleGetDevEndpoint(
	_ context.Context,
	in *getDevEndpointInput,
) (*getDevEndpointOutput, error) {
	dep, err := h.Backend.GetDevEndpoint(in.EndpointName)
	if err != nil {
		return nil, err
	}

	return &getDevEndpointOutput{DevEndpoint: dep}, nil
}

// defaultGetDevEndpointsLimit is used when GetDevEndpointsInput.MaxResults is unset.
const defaultGetDevEndpointsLimit = 100

// getDevEndpointsInput holds input for GetDevEndpoints.
type getDevEndpointsInput struct {
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int32  `json:"MaxResults,omitempty"`
}

// getDevEndpointsOutput holds the result for GetDevEndpoints.
type getDevEndpointsOutput struct {
	NextToken    string         `json:"NextToken,omitempty"`
	DevEndpoints []*DevEndpoint `json:"DevEndpoints"`
}

func (h *Handler) handleGetDevEndpoints(
	_ context.Context,
	in *getDevEndpointsInput,
) (*getDevEndpointsOutput, error) {
	deps := h.Backend.GetAllDevEndpoints()

	limit := int(in.MaxResults)
	if limit <= 0 {
		limit = defaultGetDevEndpointsLimit
	}

	page, next := paginateSlice(deps, in.NextToken, limit)

	return &getDevEndpointsOutput{DevEndpoints: page, NextToken: next}, nil
}

// defaultListDevEndpointsLimit is used when ListDevEndpointsInput.MaxResults is unset.
const defaultListDevEndpointsLimit = 100

// listDevEndpointsInput holds input for ListDevEndpoints.
type listDevEndpointsInput struct {
	Tags       map[string]string `json:"Tags,omitempty"`
	NextToken  string            `json:"NextToken,omitempty"`
	MaxResults int32             `json:"MaxResults,omitempty"`
}

// listDevEndpointsOutput holds the result for ListDevEndpoints.
type listDevEndpointsOutput struct {
	NextToken        string   `json:"NextToken,omitempty"`
	DevEndpointNames []string `json:"DevEndpointNames"`
}

func (h *Handler) handleListDevEndpoints(
	_ context.Context,
	in *listDevEndpointsInput,
) (*listDevEndpointsOutput, error) {
	deps := h.Backend.GetAllDevEndpoints()

	if len(in.Tags) > 0 {
		filtered := make([]*DevEndpoint, 0, len(deps))

		for _, d := range deps {
			if matchesTagFilter(d.Tags, in.Tags) {
				filtered = append(filtered, d)
			}
		}

		deps = filtered
	}

	limit := int(in.MaxResults)
	if limit <= 0 {
		limit = defaultListDevEndpointsLimit
	}

	page, next := paginateSlice(deps, in.NextToken, limit)

	names := make([]string, 0, len(page))
	for _, d := range page {
		names = append(names, d.EndpointName)
	}

	return &listDevEndpointsOutput{DevEndpointNames: names, NextToken: next}, nil
}

// updateDevEndpointInput holds input for UpdateDevEndpoint.
type updateDevEndpointInput struct {
	AddArguments     map[string]string `json:"AddArguments,omitempty"`
	EndpointName     string            `json:"EndpointName"`
	PublicKey        string            `json:"PublicKey,omitempty"`
	AddPublicKeys    []string          `json:"AddPublicKeys,omitempty"`
	DeleteArguments  []string          `json:"DeleteArguments,omitempty"`
	DeletePublicKeys []string          `json:"DeletePublicKeys,omitempty"`
}

func (h *Handler) handleUpdateDevEndpoint(
	_ context.Context,
	in *updateDevEndpointInput,
) (*emptyOutput, error) {
	err := h.Backend.UpdateDevEndpoint(in.EndpointName, in.AddArguments, in.DeleteArguments, UpdateDevEndpointOptions{
		AddPublicKeys:    in.AddPublicKeys,
		DeletePublicKeys: in.DeletePublicKeys,
		PublicKey:        in.PublicKey,
	})

	return &emptyOutput{}, err
}
