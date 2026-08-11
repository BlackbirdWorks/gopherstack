package ecs

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// describeContainerInstanceIncludeTags is the AWS-defined `include` value
// that requests resource tags be returned alongside each ContainerInstance
// (see also describeClusterIncludeTags for the equivalent DescribeClusters
// option). AWS's other documented value for this parameter,
// CONTAINER_INSTANCE_HEALTH, is not modeled by this backend (no health-check
// state is tracked for container instances).
const describeContainerInstanceIncludeTags = "TAGS"

// ----- Container instance handlers -----

// registerContainerInstanceInput mirrors the real RegisterContainerInstanceRequest.
// There is no ec2InstanceId field on the real request -- a real client (EC2
// launch-type container agent) can never send one -- so it cannot appear
// here either. Real ECS derives the EC2 instance ID from the signed EC2
// instance identity document (see ec2InstanceIDFromIdentityDocument).
// instanceIdentityDocumentSignature is accepted for wire-shape completeness
// but not cryptographically verified by this emulator.
type registerContainerInstanceInput struct {
	Cluster                           string `json:"cluster,omitempty"`
	InstanceIdentityDocument          string `json:"instanceIdentityDocument,omitempty"`
	InstanceIdentityDocumentSignature string `json:"instanceIdentityDocumentSignature,omitempty"`
}

type registerContainerInstanceOutput struct {
	ContainerInstance containerInstanceView `json:"containerInstance"`
}

// ec2InstanceIDFromIdentityDocument extracts the "instanceId" field from an EC2
// instance identity document (the JSON blob real EC2-launch-type container
// agents pass verbatim as instanceIdentityDocument). If doc is empty or doesn't
// parse as such a document, it returns "" rather than fabricating a
// plausible-looking ID -- Fargate-style/externally-registered instances simply
// get an empty EC2InstanceID, matching real ECS.
func ec2InstanceIDFromIdentityDocument(doc string) string {
	if doc == "" {
		return ""
	}

	var parsed struct {
		InstanceID string `json:"instanceId"`
	}

	if err := json.Unmarshal([]byte(doc), &parsed); err != nil {
		return ""
	}

	return parsed.InstanceID
}

func (h *Handler) handleRegisterContainerInstance(
	_ context.Context,
	in *registerContainerInstanceInput,
) (*registerContainerInstanceOutput, error) {
	ec2InstanceID := ec2InstanceIDFromIdentityDocument(in.InstanceIdentityDocument)

	ci, err := h.Backend.RegisterContainerInstance(in.Cluster, ec2InstanceID)
	if err != nil {
		return nil, err
	}

	return &registerContainerInstanceOutput{ContainerInstance: toContainerInstanceView(*ci)}, nil
}

type deregisterContainerInstanceInput struct {
	Cluster           string `json:"cluster,omitempty"`
	ContainerInstance string `json:"containerInstance"`
	Force             bool   `json:"force,omitempty"`
}

type deregisterContainerInstanceOutput struct {
	ContainerInstance containerInstanceView `json:"containerInstance"`
}

func (h *Handler) handleDeregisterContainerInstance(
	_ context.Context,
	in *deregisterContainerInstanceInput,
) (*deregisterContainerInstanceOutput, error) {
	ci, err := h.Backend.DeregisterContainerInstance(in.Cluster, in.ContainerInstance, in.Force)
	if err != nil {
		return nil, err
	}

	return &deregisterContainerInstanceOutput{ContainerInstance: toContainerInstanceView(*ci)}, nil
}

type describeContainerInstancesInput struct {
	Cluster            string   `json:"cluster,omitempty"`
	ContainerInstances []string `json:"containerInstances"`
	Include            []string `json:"include,omitempty"`
}

type describeContainerInstancesOutput struct {
	ContainerInstances []containerInstanceView `json:"containerInstances"`
	Failures           []failureView           `json:"failures"`
}

func (h *Handler) handleDescribeContainerInstances(
	_ context.Context,
	in *describeContainerInstancesInput,
) (*describeContainerInstancesOutput, error) {
	cis, failures, err := h.Backend.DescribeContainerInstances(in.Cluster, in.ContainerInstances)
	if err != nil {
		return nil, err
	}

	wantTags := wantsIncludeTag(in.Include, describeContainerInstanceIncludeTags)

	views, err := attachTagsIfWanted(
		h, cis,
		func(ci ContainerInstance) string { return ci.ContainerInstanceArn },
		toContainerInstanceView,
		func(v *containerInstanceView, tags []Tag) { v.Tags = tags },
		wantTags,
	)
	if err != nil {
		return nil, err
	}

	failViews := make([]failureView, 0, len(failures))
	for _, f := range failures {
		failViews = append(failViews, failureView(f))
	}

	return &describeContainerInstancesOutput{ContainerInstances: views, Failures: failViews}, nil
}

type listContainerInstancesInput struct {
	Cluster    string `json:"cluster,omitempty"`
	Filter     string `json:"filter,omitempty"`
	Status     string `json:"status,omitempty"`
	NextToken  string `json:"nextToken,omitempty"`
	MaxResults int    `json:"maxResults,omitempty"`
}

type listContainerInstancesOutput struct {
	NextToken             string   `json:"nextToken,omitempty"`
	ContainerInstanceArns []string `json:"containerInstanceArns"`
}

func (h *Handler) handleListContainerInstances(
	_ context.Context,
	in *listContainerInstancesInput,
) (*listContainerInstancesOutput, error) {
	arns, err := h.Backend.ListContainerInstances(in.Cluster, in.Status)
	if err != nil {
		return nil, err
	}

	if arns == nil {
		arns = []string{}
	}

	arns, nextToken := applyNextTokenSlice(arns, in.NextToken, in.MaxResults)

	return &listContainerInstancesOutput{ContainerInstanceArns: arns, NextToken: nextToken}, nil
}

type updateContainerInstancesStateInput struct {
	Cluster            string   `json:"cluster,omitempty"`
	Status             string   `json:"status"`
	ContainerInstances []string `json:"containerInstances"`
}

type updateContainerInstancesStateOutput struct {
	ContainerInstances []containerInstanceView `json:"containerInstances"`
}

func (h *Handler) handleUpdateContainerInstancesState(
	_ context.Context,
	in *updateContainerInstancesStateInput,
) (*updateContainerInstancesStateOutput, error) {
	cis, err := h.Backend.UpdateContainerInstancesState(
		in.Cluster,
		in.ContainerInstances,
		in.Status,
	)
	if err != nil {
		return nil, err
	}

	views := make([]containerInstanceView, 0, len(cis))
	for _, ci := range cis {
		views = append(views, toContainerInstanceView(ci))
	}

	return &updateContainerInstancesStateOutput{ContainerInstances: views}, nil
}

// ----- View types -----

type containerInstanceView struct {
	ContainerInstanceArn string  `json:"containerInstanceArn"`
	EC2InstanceID        string  `json:"ec2InstanceId"`
	ClusterArn           string  `json:"clusterArn"`
	Status               string  `json:"status"`
	AgentUpdateStatus    string  `json:"agentUpdateStatus,omitempty"`
	Tags                 []Tag   `json:"tags,omitempty"`
	RegisteredAt         float64 `json:"registeredAt"`
	Version              int64   `json:"version"`
	RunningTasksCount    int     `json:"runningTasksCount"`
	PendingTasksCount    int     `json:"pendingTasksCount"`
	AgentConnected       bool    `json:"agentConnected"`
}

func toContainerInstanceView(ci ContainerInstance) containerInstanceView {
	return containerInstanceView{
		ContainerInstanceArn: ci.ContainerInstanceArn,
		EC2InstanceID:        ci.EC2InstanceID,
		ClusterArn:           ci.ClusterArn,
		Status:               ci.Status,
		AgentUpdateStatus:    ci.AgentUpdateStatus,
		RegisteredAt:         float64(ci.RegisteredAt.Unix()),
		Version:              ci.Version,
		RunningTasksCount:    ci.RunningTasksCount,
		PendingTasksCount:    ci.PendingTasksCount,
		AgentConnected:       ci.AgentConnected,
	}
}

// ----- Handler: ListAttributes / PutAttributes / DeleteAttributes -----

type attributeInput struct {
	Name       string `json:"name"`
	Value      string `json:"value,omitempty"`
	TargetID   string `json:"targetId,omitempty"`
	TargetType string `json:"targetType,omitempty"`
}

type listAttributesInput struct {
	Cluster       string `json:"cluster,omitempty"`
	TargetType    string `json:"targetType,omitempty"`
	AttributeName string `json:"attributeName,omitempty"`
	TargetID      string `json:"targetId,omitempty"`
	NextToken     string `json:"nextToken,omitempty"`
	MaxResults    int    `json:"maxResults,omitempty"`
}

type listAttributesOutput struct {
	NextToken  string           `json:"nextToken,omitempty"`
	Attributes []attributeInput `json:"attributes"`
}

func (h *Handler) handleListAttributes(
	_ context.Context,
	in *listAttributesInput,
) (*listAttributesOutput, error) {
	attrs, err := h.Backend.ListAttributes(in.Cluster, in.TargetType, in.AttributeName, in.TargetID)
	if err != nil {
		return nil, err
	}

	views := make([]attributeInput, 0, len(attrs))
	for _, a := range attrs {
		views = append(views, attributeInput(a))
	}

	p := page.New(views, in.NextToken, in.MaxResults, defaultECSMaxResults)

	return &listAttributesOutput{Attributes: p.Data, NextToken: p.Next}, nil
}

type putAttributesInput struct {
	Cluster    string           `json:"cluster,omitempty"`
	Attributes []attributeInput `json:"attributes"`
}

type putAttributesOutput struct {
	Attributes []attributeInput `json:"attributes"`
}

func (h *Handler) handlePutAttributes(
	_ context.Context,
	in *putAttributesInput,
) (*putAttributesOutput, error) {
	attrs := make([]Attribute, 0, len(in.Attributes))
	for _, a := range in.Attributes {
		attrs = append(attrs, Attribute(a))
	}

	created, err := h.Backend.PutAttributes(in.Cluster, attrs)
	if err != nil {
		return nil, err
	}

	views := make([]attributeInput, 0, len(created))
	for _, a := range created {
		views = append(views, attributeInput(a))
	}

	return &putAttributesOutput{Attributes: views}, nil
}

type deleteAttributesInput struct {
	Cluster    string           `json:"cluster,omitempty"`
	Attributes []attributeInput `json:"attributes"`
}

type deleteAttributesOutput struct {
	Attributes []attributeInput `json:"attributes"`
}

func (h *Handler) handleDeleteAttributes(
	_ context.Context,
	in *deleteAttributesInput,
) (*deleteAttributesOutput, error) {
	attrs := make([]Attribute, 0, len(in.Attributes))
	for _, a := range in.Attributes {
		attrs = append(attrs, Attribute(a))
	}

	deleted, err := h.Backend.DeleteAttributes(in.Cluster, attrs)
	if err != nil {
		return nil, err
	}

	views := make([]attributeInput, 0, len(deleted))
	for _, a := range deleted {
		views = append(views, attributeInput(a))
	}

	return &deleteAttributesOutput{Attributes: views}, nil
}

// ----- Handler: UpdateContainerAgent -----

type updateContainerAgentInput struct {
	Cluster           string `json:"cluster,omitempty"`
	ContainerInstance string `json:"containerInstance"`
}

type updateContainerAgentOutput struct {
	ContainerInstance containerInstanceView `json:"containerInstance"`
}

func (h *Handler) handleUpdateContainerAgent(
	_ context.Context,
	in *updateContainerAgentInput,
) (*updateContainerAgentOutput, error) {
	ci, err := h.Backend.UpdateContainerAgent(in.Cluster, in.ContainerInstance)
	if err != nil {
		return nil, err
	}

	return &updateContainerAgentOutput{ContainerInstance: toContainerInstanceView(*ci)}, nil
}
