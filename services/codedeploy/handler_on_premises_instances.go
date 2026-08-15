package codedeploy

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

type addTagsToOnPremisesInstancesInput struct {
	InstanceNames []string   `json:"instanceNames"`
	Tags          []tagEntry `json:"tags"`
}

type addTagsToOnPremisesInstancesOutput struct{}

func (h *Handler) handleAddTagsToOnPremisesInstances(
	_ context.Context,
	in *addTagsToOnPremisesInstancesInput,
) (*addTagsToOnPremisesInstancesOutput, error) {
	if len(in.InstanceNames) == 0 {
		return nil, fmt.Errorf("%w: instanceNames is required", errInvalidRequest)
	}

	if err := h.Backend.AddTagsToOnPremisesInstances(in.InstanceNames, tagEntriesToMap(in.Tags)); err != nil {
		return nil, err
	}

	return &addTagsToOnPremisesInstancesOutput{}, nil
}

type removeTagsFromOnPremisesInstancesInput struct {
	InstanceNames []string   `json:"instanceNames"`
	Tags          []tagEntry `json:"tags"`
}

type removeTagsFromOnPremisesInstancesOutput struct{}

func (h *Handler) handleRemoveTagsFromOnPremisesInstances(
	_ context.Context,
	in *removeTagsFromOnPremisesInstancesInput,
) (*removeTagsFromOnPremisesInstancesOutput, error) {
	if len(in.InstanceNames) == 0 {
		return nil, fmt.Errorf("%w: instanceNames is required", errInvalidRequest)
	}

	keys := make([]string, 0, len(in.Tags))
	for _, t := range in.Tags {
		keys = append(keys, t.Key)
	}

	if err := h.Backend.RemoveTagsFromOnPremisesInstances(in.InstanceNames, keys); err != nil {
		return nil, err
	}

	return &removeTagsFromOnPremisesInstancesOutput{}, nil
}

type registerOnPremisesInstanceInput struct {
	InstanceName  string `json:"instanceName"`
	IamSessionArn string `json:"iamSessionArn"`
	IamUserArn    string `json:"iamUserArn"`
}

type registerOnPremisesInstanceOutput struct{}

func (h *Handler) handleRegisterOnPremisesInstance(
	_ context.Context,
	in *registerOnPremisesInstanceInput,
) (*registerOnPremisesInstanceOutput, error) {
	if in.InstanceName == "" {
		return nil, fmt.Errorf("%w: instanceName is required", errInvalidRequest)
	}

	if err := h.Backend.RegisterOnPremisesInstance(in.InstanceName, in.IamSessionArn, in.IamUserArn); err != nil {
		return nil, err
	}

	return &registerOnPremisesInstanceOutput{}, nil
}

type deregisterOnPremisesInstanceInput struct {
	InstanceName string `json:"instanceName"`
}

type deregisterOnPremisesInstanceOutput struct{}

func (h *Handler) handleDeregisterOnPremisesInstance(
	_ context.Context,
	in *deregisterOnPremisesInstanceInput,
) (*deregisterOnPremisesInstanceOutput, error) {
	if in.InstanceName == "" {
		return nil, fmt.Errorf("%w: instanceName is required", errInvalidRequest)
	}

	if err := h.Backend.DeregisterOnPremisesInstance(in.InstanceName); err != nil {
		return nil, err
	}

	return &deregisterOnPremisesInstanceOutput{}, nil
}

type onPremisesInstanceInfo struct {
	DeregisterTime *float64   `json:"deregisterTime,omitempty"`
	InstanceName   string     `json:"instanceName"`
	InstanceArn    string     `json:"instanceArn,omitempty"`
	IamSessionArn  string     `json:"iamSessionArn,omitempty"`
	IamUserArn     string     `json:"iamUserArn,omitempty"`
	Tags           []tagEntry `json:"tags"`
	RegisterTime   float64    `json:"registerTime"`
}

type getOnPremisesInstanceInput struct {
	InstanceName string `json:"instanceName"`
}

type getOnPremisesInstanceOutput struct {
	InstanceInfo onPremisesInstanceInfo `json:"instanceInfo"`
}

func (h *Handler) handleGetOnPremisesInstance(
	_ context.Context,
	in *getOnPremisesInstanceInput,
) (*getOnPremisesInstanceOutput, error) {
	if in.InstanceName == "" {
		return nil, fmt.Errorf("%w: instanceName is required", errInvalidRequest)
	}

	inst, err := h.Backend.GetOnPremisesInstance(in.InstanceName)
	if err != nil {
		return nil, err
	}

	info := onPremisesInstanceInfo{
		InstanceName:  inst.InstanceName,
		InstanceArn:   h.Backend.OnPremisesInstanceARN(inst.InstanceName),
		RegisterTime:  awstime.Epoch(inst.RegisterTime),
		IamSessionArn: inst.IamSessionArn,
		IamUserArn:    inst.IamUserArn,
	}

	if inst.DeregisterTime != nil {
		dt := awstime.Epoch(*inst.DeregisterTime)
		info.DeregisterTime = &dt
	}

	if inst.Tags != nil {
		info.Tags = tagsToSortedSlice(inst.Tags.Clone())
	} else {
		info.Tags = []tagEntry{}
	}

	return &getOnPremisesInstanceOutput{InstanceInfo: info}, nil
}

type listOnPremisesInstancesInput struct {
	RegistrationStatus string           `json:"registrationStatus"`
	TagFilters         []tagFilterEntry `json:"tagFilters"`
}

type listOnPremisesInstancesOutput struct {
	InstanceNames []string `json:"instanceNames"`
}

func (h *Handler) handleListOnPremisesInstances(
	_ context.Context,
	in *listOnPremisesInstancesInput,
) (*listOnPremisesInstancesOutput, error) {
	filters := make([]TagFilter, 0, len(in.TagFilters))
	for _, f := range in.TagFilters {
		filters = append(filters, TagFilter(f))
	}

	return &listOnPremisesInstancesOutput{
		InstanceNames: h.Backend.ListOnPremisesInstances(in.RegistrationStatus, filters),
	}, nil
}

type batchGetOnPremisesInstancesInput struct {
	InstanceNames []string `json:"instanceNames"`
}

type batchGetOnPremisesInstancesOutput struct {
	InstanceInfos []onPremisesInstanceInfo `json:"instanceInfos"`
}

func (h *Handler) handleBatchGetOnPremisesInstances(
	_ context.Context,
	in *batchGetOnPremisesInstancesInput,
) (*batchGetOnPremisesInstancesOutput, error) {
	if len(in.InstanceNames) == 0 {
		return nil, fmt.Errorf("%w: instanceNames is required", errInvalidRequest)
	}

	instances := h.Backend.BatchGetOnPremisesInstances(in.InstanceNames)

	infos := make([]onPremisesInstanceInfo, 0, len(instances))
	for _, inst := range instances {
		info := onPremisesInstanceInfo{
			InstanceName:  inst.InstanceName,
			InstanceArn:   h.Backend.OnPremisesInstanceARN(inst.InstanceName),
			RegisterTime:  awstime.Epoch(inst.RegisterTime),
			IamSessionArn: inst.IamSessionArn,
			IamUserArn:    inst.IamUserArn,
		}

		if inst.DeregisterTime != nil {
			dt := awstime.Epoch(*inst.DeregisterTime)
			info.DeregisterTime = &dt
		}

		if inst.Tags != nil {
			info.Tags = tagsToSortedSlice(inst.Tags.Clone())
		} else {
			info.Tags = []tagEntry{}
		}

		infos = append(infos, info)
	}

	return &batchGetOnPremisesInstancesOutput{InstanceInfos: infos}, nil
}
