package networkfirewall

import (
	"context"
	"time"
)

const (
	containerCreating = "CREATING"
	containerActive   = "ACTIVE"
	containerUpdating = "UPDATING"
	containerDeleting = "DELETING"
)

// ContainerAttribute is one attribute filter.
type ContainerAttribute struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// ContainerMonitoringConfiguration selects the containers of one cluster.
type ContainerMonitoringConfiguration struct {
	ClusterArn       string               `json:"ClusterArn"`
	AttributeFilters []ContainerAttribute `json:"AttributeFilters,omitempty"`
}

// CreateContainerAssociationInput is the CreateContainerAssociation request.
type CreateContainerAssociationInput struct {
	ContainerAssociationName          string                             `json:"ContainerAssociationName"`
	Type                              string                             `json:"Type"`
	Description                       string                             `json:"Description"`
	ContainerMonitoringConfigurations []ContainerMonitoringConfiguration `json:"ContainerMonitoringConfigurations"`
	Tags                              []Tag                              `json:"Tags"`
}

// ContainerAssociationOutput is the Create/UpdateContainerAssociation response.
type ContainerAssociationOutput struct {
	ContainerAssociationArn           string                             `json:"ContainerAssociationArn"`
	ContainerAssociationName          string                             `json:"ContainerAssociationName"`
	Description                       string                             `json:"Description,omitempty"`
	Status                            string                             `json:"Status"`
	Type                              string                             `json:"Type"`
	UpdateToken                       string                             `json:"UpdateToken"`
	ContainerMonitoringConfigurations []ContainerMonitoringConfiguration `json:"ContainerMonitoringConfigurations"`
	Tags                              []Tag                              `json:"Tags,omitempty"`
}

// DescribeContainerAssociationInput is the DescribeContainerAssociation request.
type DescribeContainerAssociationInput struct {
	ContainerAssociationArn  string `json:"ContainerAssociationArn"`
	ContainerAssociationName string `json:"ContainerAssociationName"`
}

// DescribeContainerAssociationOutput is the DescribeContainerAssociation response.
type DescribeContainerAssociationOutput struct {
	LastUpdatedTime *float64 `json:"LastUpdatedTime,omitempty"`
	ContainerAssociationOutput
	ResolvedCidrCount int32 `json:"ResolvedCidrCount"`
}

// UpdateContainerAssociationInput is the UpdateContainerAssociation request.
type UpdateContainerAssociationInput struct {
	ContainerAssociationArn           string                             `json:"ContainerAssociationArn"`
	ContainerAssociationName          string                             `json:"ContainerAssociationName"`
	Type                              string                             `json:"Type"`
	UpdateToken                       string                             `json:"UpdateToken"`
	Description                       *string                            `json:"Description"`
	ContainerMonitoringConfigurations []ContainerMonitoringConfiguration `json:"ContainerMonitoringConfigurations"`
	Tags                              []Tag                              `json:"Tags"`
}

// DeleteContainerAssociationInput is the DeleteContainerAssociation request.
type DeleteContainerAssociationInput = DescribeContainerAssociationInput

// DeleteContainerAssociationOutput is the DeleteContainerAssociation response.
type DeleteContainerAssociationOutput struct {
	ContainerAssociationArn  string `json:"ContainerAssociationArn"`
	ContainerAssociationName string `json:"ContainerAssociationName"`
	Status                   string `json:"Status"`
}

// ListContainerAssociationsInput is the ListContainerAssociations request.
type ListContainerAssociationsInput = ListProxiesInput

// ContainerAssociationSummary is a ListContainerAssociations entry.
type ContainerAssociationSummary struct {
	Arn  string `json:"Arn"`
	Name string `json:"Name"`
}

// ListContainerAssociationsOutput is the ListContainerAssociations response.
type ListContainerAssociationsOutput struct {
	NextToken             string                        `json:"NextToken,omitempty"`
	ContainerAssociations []ContainerAssociationSummary `json:"ContainerAssociations"`
}

func (r *containerRecord) expired(now time.Time) bool {
	return !r.DeleteUntil.IsZero() && !now.Before(r.DeleteUntil)
}

func (r *containerRecord) statusAt(now time.Time) string {
	switch {
	case !r.DeleteUntil.IsZero():
		return containerDeleting
	case now.Before(r.SettleAt) && r.Updating:
		return containerUpdating
	case now.Before(r.SettleAt):
		return containerCreating
	default:
		return containerActive
	}
}

func (r *containerRecord) output(now time.Time) ContainerAssociationOutput {
	return ContainerAssociationOutput{
		ContainerAssociationArn:           r.ARN,
		ContainerAssociationName:          r.Name,
		Description:                       r.Description,
		Status:                            r.statusAt(now),
		Type:                              r.Type,
		UpdateToken:                       r.UpdateToken,
		ContainerMonitoringConfigurations: cloneConfigs(r.Configs),
		Tags:                              tagList(r.Tags),
	}
}

func cloneConfigs(c []ContainerMonitoringConfiguration) []ContainerMonitoringConfiguration {
	out := make([]ContainerMonitoringConfiguration, len(c))
	copy(out, c)

	return out
}

func validateContainerInputs(kind string, c []ContainerMonitoringConfiguration, desc string, t []Tag) error {
	if kind != "ECS" && kind != "EKS" {
		return invalid("Type %q is not valid", kind)
	}

	if c == nil {
		return invalid("ContainerMonitoringConfigurations is required")
	}

	for _, x := range c {
		if x.ClusterArn == "" {
			return invalid("ContainerMonitoringConfiguration.ClusterArn is required")
		}

		for _, f := range x.AttributeFilters {
			if f.Key == "" || f.Value == "" {
				return invalid("ContainerAttribute requires Key and Value")
			}
		}
	}

	if err := validateDescription(desc); err != nil {
		return err
	}

	return validateTags(tagMap(t))
}

func (b *InMemoryBackend) sweepContainersLocked(now time.Time) {
	for _, r := range b.containers.All() {
		if r.expired(now) {
			b.containers.Delete(r.ARN)
		}
	}
}

func (b *InMemoryBackend) findContainerLocked(id arnOrName, now time.Time) (*containerRecord, error) {
	r, err := lookup(b, b.containers, kindContainer, "container association", id)
	if err != nil {
		return nil, err
	}

	if r.expired(now) {
		return nil, notFound("container association", r.ARN)
	}

	return r, nil
}

// CreateContainerAssociation creates a container association.
func (b *InMemoryBackend) CreateContainerAssociation(
	_ context.Context, in *CreateContainerAssociationInput,
) (*ContainerAssociationOutput, error) {
	if err := validateName("ContainerAssociationName", in.ContainerAssociationName); err != nil {
		return nil, err
	}

	if err := validateContainerInputs(
		in.Type,
		in.ContainerMonitoringConfigurations,
		in.Description,
		in.Tags,
	); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateContainerAssociation")
	defer b.mu.Unlock()

	now := time.Now()
	b.sweepContainersLocked(now)

	key := b.arnFor(kindContainer, in.ContainerAssociationName)
	if b.containers.Has(key) {
		return nil, invalid("A container association named %s already exists", in.ContainerAssociationName)
	}

	r := &containerRecord{
		Name:        in.ContainerAssociationName,
		ARN:         key,
		Type:        in.Type,
		Description: in.Description,
		UpdateToken: newToken(),
		Configs:     cloneConfigs(in.ContainerMonitoringConfigurations),
		Tags:        tagMap(in.Tags),
		LastUpdated: now,
		SettleAt:    b.deadline(now),
	}

	b.containers.Put(r)

	out := r.output(now)

	return &out, nil
}

// DescribeContainerAssociation returns a container association.
func (b *InMemoryBackend) DescribeContainerAssociation(
	_ context.Context, in *DescribeContainerAssociationInput,
) (*DescribeContainerAssociationOutput, error) {
	b.mu.RLock("DescribeContainerAssociation")
	defer b.mu.RUnlock()

	now := time.Now()

	r, err := b.findContainerLocked(arnOrName{in.ContainerAssociationArn, in.ContainerAssociationName}, now)
	if err != nil {
		return nil, err
	}

	return &DescribeContainerAssociationOutput{
		ContainerAssociationOutput: r.output(now),
		LastUpdatedTime:            epoch(r.LastUpdated),
	}, nil
}

// ListContainerAssociations lists container associations.
func (b *InMemoryBackend) ListContainerAssociations(
	_ context.Context, in *ListContainerAssociationsInput,
) (*ListContainerAssociationsOutput, error) {
	b.mu.RLock("ListContainerAssociations")
	defer b.mu.RUnlock()

	now := time.Now()
	live := make([]*containerRecord, 0, b.containers.Len())

	for _, r := range b.containers.All() {
		if !r.expired(now) {
			live = append(live, r)
		}
	}

	data, next, err := listMetadata(live,
		func(r *containerRecord) ResourceMetadata { return ResourceMetadata{Arn: r.ARN, Name: r.Name} },
		in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	out := make([]ContainerAssociationSummary, len(data))
	for i, d := range data {
		out[i] = ContainerAssociationSummary(d)
	}

	return &ListContainerAssociationsOutput{ContainerAssociations: out, NextToken: next}, nil
}

// UpdateContainerAssociation replaces the monitoring configuration of an association.
func (b *InMemoryBackend) UpdateContainerAssociation(
	_ context.Context, in *UpdateContainerAssociationInput,
) (*ContainerAssociationOutput, error) {
	desc := ""
	if in.Description != nil {
		desc = *in.Description
	}

	if err := validateContainerInputs(in.Type, in.ContainerMonitoringConfigurations, desc, in.Tags); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateContainerAssociation")
	defer b.mu.Unlock()

	now := time.Now()
	b.sweepContainersLocked(now)

	r, err := b.findContainerLocked(arnOrName{in.ContainerAssociationArn, in.ContainerAssociationName}, now)
	if err != nil {
		return nil, err
	}

	if !r.DeleteUntil.IsZero() {
		return nil, invalidOp("The container association %s is being deleted", r.Name)
	}

	if err = requireToken(r.UpdateToken, in.UpdateToken); err != nil {
		return nil, err
	}

	r.Type = in.Type
	r.Configs = cloneConfigs(in.ContainerMonitoringConfigurations)

	if in.Description != nil {
		r.Description = *in.Description
	}

	if in.Tags != nil {
		r.Tags = tagMap(in.Tags)
	}

	r.UpdateToken = newToken()
	r.LastUpdated = now
	r.Updating = true
	r.SettleAt = b.deadline(now)

	out := r.output(now)

	return &out, nil
}

// DeleteContainerAssociation starts deletion; the association disappears once DELETING passes.
func (b *InMemoryBackend) DeleteContainerAssociation(
	_ context.Context, in *DeleteContainerAssociationInput,
) (*DeleteContainerAssociationOutput, error) {
	b.mu.Lock("DeleteContainerAssociation")
	defer b.mu.Unlock()

	now := time.Now()
	b.sweepContainersLocked(now)

	r, err := b.findContainerLocked(arnOrName{in.ContainerAssociationArn, in.ContainerAssociationName}, now)
	if err != nil {
		return nil, err
	}

	if r.DeleteUntil.IsZero() {
		r.DeleteUntil = b.deadline(now)
	}

	out := &DeleteContainerAssociationOutput{
		ContainerAssociationArn: r.ARN, ContainerAssociationName: r.Name, Status: containerDeleting,
	}

	if b.delay <= 0 {
		b.containers.Delete(r.ARN)
	}

	return out, nil
}
