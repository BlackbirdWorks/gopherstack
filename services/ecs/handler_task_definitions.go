package ecs

import (
	"context"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// ----- Task definition handlers -----

// placementConstraintInput/placementConstraintView and toPlacementConstraints
// live here because RegisterTaskDefinition is their first/primary caller;
// CreateService and UpdateService (handler_services.go) also consume them,
// cross-file.
type placementConstraintInput struct {
	Type       string `json:"type,omitempty"`
	Expression string `json:"expression,omitempty"`
}

type registerTaskDefinitionInput struct {
	RuntimePlatform         *RuntimePlatform           `json:"runtimePlatform,omitempty"`
	EphemeralStorage        *EphemeralStorage          `json:"ephemeralStorage,omitempty"`
	Family                  string                     `json:"family"`
	TaskRoleArn             string                     `json:"taskRoleArn,omitempty"`
	ExecutionRoleArn        string                     `json:"executionRoleArn,omitempty"`
	NetworkMode             string                     `json:"networkMode,omitempty"`
	CPU                     string                     `json:"cpu,omitempty"`
	Memory                  string                     `json:"memory,omitempty"`
	PlatformFamily          string                     `json:"platformFamily,omitempty"`
	Tags                    []Tag                      `json:"tags,omitempty"`
	ContainerDefinitions    []ContainerDefinition      `json:"containerDefinitions"`
	Volumes                 []Volume                   `json:"volumes,omitempty"`
	PlacementConstraints    []placementConstraintInput `json:"placementConstraints,omitempty"`
	RequiresCompatibilities []string                   `json:"requiresCompatibilities,omitempty"`
	InferenceAccelerators   []InferenceAccelerator     `json:"inferenceAccelerators,omitempty"`
}

type registerTaskDefinitionOutput struct {
	Tags           []Tag              `json:"tags,omitempty"`
	TaskDefinition taskDefinitionView `json:"taskDefinition"`
}

func (h *Handler) handleRegisterTaskDefinition(
	_ context.Context,
	in *registerTaskDefinitionInput,
) (*registerTaskDefinitionOutput, error) {
	td, err := h.Backend.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:                  in.Family,
		TaskRoleArn:             in.TaskRoleArn,
		ExecutionRoleArn:        in.ExecutionRoleArn,
		NetworkMode:             in.NetworkMode,
		CPU:                     in.CPU,
		Memory:                  in.Memory,
		PlatformFamily:          in.PlatformFamily,
		ContainerDefinitions:    in.ContainerDefinitions,
		Volumes:                 in.Volumes,
		PlacementConstraints:    toPlacementConstraints(in.PlacementConstraints),
		RequiresCompatibilities: in.RequiresCompatibilities,
		Tags:                    in.Tags,
		RuntimePlatform:         in.RuntimePlatform,
		EphemeralStorage:        in.EphemeralStorage,
		InferenceAccelerators:   in.InferenceAccelerators,
	})
	if err != nil {
		return nil, err
	}

	// RegisterTaskDefinitionOutput always echoes the supplied tags, unlike
	// DescribeTaskDefinition/ListTagsForResource which gate behind `include=TAGS`
	// (ecs@v1.90.0 deserializers.go:32428, awsAwsjson11_deserializeOpDocumentRegisterTaskDefinitionOutput).
	return &registerTaskDefinitionOutput{TaskDefinition: toTaskDefinitionView(*td), Tags: in.Tags}, nil
}

type describeTaskDefinitionInput struct {
	TaskDefinition string   `json:"taskDefinition"`
	Include        []string `json:"include,omitempty"`
}

type describeTaskDefinitionOutput struct {
	Tags           []Tag              `json:"tags,omitempty"`
	TaskDefinition taskDefinitionView `json:"taskDefinition"`
}

// describeTaskIncludeTags is the AWS-defined value for the `include` parameter
// that requests resource tags be returned alongside the TaskDefinition.
const describeTaskIncludeTags = "TAGS"

func (h *Handler) handleDescribeTaskDefinition(
	_ context.Context,
	in *describeTaskDefinitionInput,
) (*describeTaskDefinitionOutput, error) {
	td, err := h.Backend.DescribeTaskDefinition(in.TaskDefinition)
	if err != nil {
		return nil, err
	}

	out := &describeTaskDefinitionOutput{TaskDefinition: toTaskDefinitionView(*td)}

	for _, opt := range in.Include {
		if !strings.EqualFold(opt, describeTaskIncludeTags) {
			continue
		}

		tags, terr := h.Backend.ListTagsForResource(td.TaskDefinitionArn)
		if terr != nil {
			return nil, terr
		}

		out.Tags = tags

		break
	}

	return out, nil
}

type deregisterTaskDefinitionInput struct {
	TaskDefinition string `json:"taskDefinition"`
}

type deregisterTaskDefinitionOutput struct {
	TaskDefinition taskDefinitionView `json:"taskDefinition"`
}

func (h *Handler) handleDeregisterTaskDefinition(
	_ context.Context,
	in *deregisterTaskDefinitionInput,
) (*deregisterTaskDefinitionOutput, error) {
	td, err := h.Backend.DeregisterTaskDefinition(in.TaskDefinition)
	if err != nil {
		return nil, err
	}

	return &deregisterTaskDefinitionOutput{TaskDefinition: toTaskDefinitionView(*td)}, nil
}

type listTaskDefinitionsInput struct {
	FamilyPrefix string `json:"familyPrefix,omitempty"`
	Status       string `json:"status,omitempty"`
	NextToken    string `json:"nextToken,omitempty"`
	MaxResults   int    `json:"maxResults,omitempty"`
}

type listTaskDefinitionsOutput struct {
	NextToken          string   `json:"nextToken,omitempty"`
	TaskDefinitionArns []string `json:"taskDefinitionArns"`
}

func (h *Handler) handleListTaskDefinitions(
	_ context.Context,
	in *listTaskDefinitionsInput,
) (*listTaskDefinitionsOutput, error) {
	arns, err := h.Backend.ListTaskDefinitionsFiltered(ListTaskDefinitionsInput{
		FamilyPrefix: in.FamilyPrefix,
		Status:       in.Status,
	})
	if err != nil {
		return nil, err
	}

	if arns == nil {
		arns = []string{}
	}

	sort.Strings(arns)

	arns, nextToken := applyNextTokenSlice(arns, in.NextToken, in.MaxResults)

	return &listTaskDefinitionsOutput{TaskDefinitionArns: arns, NextToken: nextToken}, nil
}

// ----- View types -----

type placementConstraintView struct {
	Type       string `json:"type,omitempty"`
	Expression string `json:"expression,omitempty"`
}

type taskDefinitionView struct {
	RuntimePlatform         *RuntimePlatform          `json:"runtimePlatform,omitempty"`
	EphemeralStorage        *EphemeralStorage         `json:"ephemeralStorage,omitempty"`
	TaskDefinitionArn       string                    `json:"taskDefinitionArn"`
	Family                  string                    `json:"family"`
	TaskRoleArn             string                    `json:"taskRoleArn,omitempty"`
	ExecutionRoleArn        string                    `json:"executionRoleArn,omitempty"`
	NetworkMode             string                    `json:"networkMode,omitempty"`
	Status                  string                    `json:"status"`
	CPU                     string                    `json:"cpu,omitempty"`
	Memory                  string                    `json:"memory,omitempty"`
	PlatformFamily          string                    `json:"platformFamily,omitempty"`
	ContainerDefinitions    []ContainerDefinition     `json:"containerDefinitions"`
	Volumes                 []Volume                  `json:"volumes"`
	PlacementConstraints    []placementConstraintView `json:"placementConstraints,omitempty"`
	RequiresCompatibilities []string                  `json:"requiresCompatibilities,omitempty"`
	InferenceAccelerators   []InferenceAccelerator    `json:"inferenceAccelerators,omitempty"`
	RegisteredAt            float64                   `json:"registeredAt"`
	Revision                int                       `json:"revision"`
}

func toTaskDefinitionView(td TaskDefinition) taskDefinitionView {
	volumes := td.Volumes
	if volumes == nil {
		volumes = []Volume{}
	}

	v := taskDefinitionView{
		TaskDefinitionArn:       td.TaskDefinitionArn,
		Family:                  td.Family,
		TaskRoleArn:             td.TaskRoleArn,
		ExecutionRoleArn:        td.ExecutionRoleArn,
		NetworkMode:             td.NetworkMode,
		Status:                  td.Status,
		CPU:                     td.CPU,
		Memory:                  td.Memory,
		PlatformFamily:          td.PlatformFamily,
		ContainerDefinitions:    td.ContainerDefinitions,
		Volumes:                 volumes,
		RequiresCompatibilities: td.RequiresCompatibilities,
		RegisteredAt:            float64(td.RegisteredAt.Unix()),
		Revision:                td.Revision,
		RuntimePlatform:         td.RuntimePlatform,
		EphemeralStorage:        td.EphemeralStorage,
		InferenceAccelerators:   td.InferenceAccelerators,
	}

	for _, c := range td.PlacementConstraints {
		v.PlacementConstraints = append(v.PlacementConstraints, placementConstraintView(c))
	}

	return v
}

// toPlacementConstraints converts handler input to backend type.
func toPlacementConstraints(in []placementConstraintInput) []PlacementConstraint {
	if len(in) == 0 {
		return nil
	}

	out := make([]PlacementConstraint, len(in))
	for i, c := range in {
		out[i] = PlacementConstraint(c)
	}

	return out
}

// ----- ListTaskDefinitionFamilies -----

type listTaskDefinitionFamiliesInput struct {
	FamilyPrefix string `json:"familyPrefix,omitempty"`
	Status       string `json:"status,omitempty"`
	NextToken    string `json:"nextToken,omitempty"`
	MaxResults   int    `json:"maxResults,omitempty"`
}

type listTaskDefinitionFamiliesOutput struct {
	NextToken string   `json:"nextToken,omitempty"`
	Families  []string `json:"families"`
}

func (h *Handler) handleListTaskDefinitionFamilies(
	_ context.Context,
	in *listTaskDefinitionFamiliesInput,
) (*listTaskDefinitionFamiliesOutput, error) {
	families, err := h.Backend.ListTaskDefinitionFamilies(in.FamilyPrefix, in.Status)
	if err != nil {
		return nil, err
	}

	sort.Strings(families)

	p := page.New(families, in.NextToken, in.MaxResults, defaultECSMaxResults)

	return &listTaskDefinitionFamiliesOutput{Families: p.Data, NextToken: p.Next}, nil
}

// ----- Handler: DeleteTaskDefinitions -----

type deleteTaskDefinitionsInput struct {
	TaskDefinitions []string `json:"taskDefinitions"`
}

type deleteTaskDefinitionsOutput struct {
	TaskDefinitions []taskDefinitionView `json:"taskDefinitions"`
	Failures        []failureView        `json:"failures"`
}

func (h *Handler) handleDeleteTaskDefinitions(
	_ context.Context,
	in *deleteTaskDefinitionsInput,
) (*deleteTaskDefinitionsOutput, error) {
	deleted, failures, err := h.Backend.DeleteTaskDefinitions(in.TaskDefinitions)
	if err != nil {
		return nil, err
	}

	tdViews := make([]taskDefinitionView, 0, len(deleted))
	for _, td := range deleted {
		tdViews = append(tdViews, toTaskDefinitionView(td))
	}

	failViews := make([]failureView, 0, len(failures))
	for _, f := range failures {
		failViews = append(failViews, failureView(f))
	}

	return &deleteTaskDefinitionsOutput{
		TaskDefinitions: tdViews,
		Failures:        failViews,
	}, nil
}
