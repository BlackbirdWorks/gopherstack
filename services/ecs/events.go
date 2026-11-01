package ecs

import (
	"context"
	"encoding/json"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	eventSource = "aws.ecs"

	detailTypeTaskState       = "ECS Task State Change"
	detailTypeInstanceState   = "ECS Container Instance State Change"
	detailTypeDeploymentState = "ECS Deployment State Change"
	detailTypeServiceAction   = "ECS Service Action"

	eventTypeInfo  = "INFO"
	eventTypeError = "ERROR"

	eventDeploymentInProgress = "SERVICE_DEPLOYMENT_IN_PROGRESS"
	eventDeploymentCompleted  = "SERVICE_DEPLOYMENT_COMPLETED"
	eventDeploymentFailed     = "SERVICE_DEPLOYMENT_FAILED"
	eventServiceSteadyState   = "SERVICE_STEADY_STATE"

	eventTimeLayout = "2006-01-02T15:04:05.000Z"
)

// EventPublisher puts events on the default event bus.
type EventPublisher interface {
	PublishEvent(ctx context.Context, source, detailType string, resources []string, detail string)
}

// SetEventPublisher wires the default-bus publisher for ECS state-change events.
func (b *InMemoryBackend) SetEventPublisher(p EventPublisher) {
	b.mu.Lock("SetEventPublisher")
	defer b.mu.Unlock()

	b.events = p
}

// emitLocked publishes one event; b.mu must be held (EventBridge delivers asynchronously).
func (b *InMemoryBackend) emitLocked(detailType string, resources []string, detail any) {
	if b.events == nil {
		return
	}

	body, err := json.Marshal(detail)
	if err != nil {
		return
	}

	ctx := awsmeta.WithRegion(context.Background(), b.region)
	b.events.PublishEvent(ctx, eventSource, detailType, resources, string(body))

	logger.Load(ctx).Debug("ecs: event published", "detailType", detailType)
}

func eventTime(t time.Time) string { return t.UTC().Format(eventTimeLayout) }

func eventTimePtr(t *time.Time) string {
	if t == nil {
		return ""
	}

	return eventTime(*t)
}

type eventNetworkInterface struct {
	AttachmentID       string `json:"attachmentId,omitempty"`
	PrivateIpv4Address string `json:"privateIpv4Address,omitempty"`
}

type eventContainer struct {
	ExitCode          *int                    `json:"exitCode,omitempty"`
	ContainerArn      string                  `json:"containerArn,omitempty"`
	LastStatus        string                  `json:"lastStatus"`
	Name              string                  `json:"name"`
	Image             string                  `json:"image,omitempty"`
	ImageDigest       string                  `json:"imageDigest,omitempty"`
	RuntimeID         string                  `json:"runtimeId,omitempty"`
	TaskArn           string                  `json:"taskArn"`
	CPU               string                  `json:"cpu,omitempty"`
	Memory            string                  `json:"memory,omitempty"`
	Reason            string                  `json:"reason,omitempty"`
	NetworkInterfaces []eventNetworkInterface `json:"networkInterfaces,omitempty"`
}

type eventContainerOverride struct {
	Name string `json:"name"`
}

type eventOverrides struct {
	ContainerOverrides []eventContainerOverride `json:"containerOverrides"`
}

type eventAttachment struct {
	ID      string         `json:"id"`
	Type    string         `json:"type"`
	Status  string         `json:"status"`
	Details []KeyValuePair `json:"details,omitempty"`
}

type taskStateChangeDetail struct {
	Memory               string            `json:"memory,omitempty"`
	TaskDefinitionArn    string            `json:"taskDefinitionArn"`
	UpdatedAt            string            `json:"updatedAt"`
	ContainerInstanceArn string            `json:"containerInstanceArn,omitempty"`
	ClusterArn           string            `json:"clusterArn"`
	TaskArn              string            `json:"taskArn"`
	CPU                  string            `json:"cpu,omitempty"`
	LastStatus           string            `json:"lastStatus"`
	DesiredStatus        string            `json:"desiredStatus"`
	Group                string            `json:"group,omitempty"`
	PlatformVersion      string            `json:"platformVersion,omitempty"`
	LaunchType           string            `json:"launchType,omitempty"`
	StartedAt            string            `json:"startedAt,omitempty"`
	Connectivity         string            `json:"connectivity,omitempty"`
	ConnectivityAt       string            `json:"connectivityAt,omitempty"`
	CreatedAt            string            `json:"createdAt,omitempty"`
	StoppedReason        string            `json:"stoppedReason,omitempty"`
	StartedBy            string            `json:"startedBy,omitempty"`
	StoppedAt            string            `json:"stoppedAt,omitempty"`
	Overrides            eventOverrides    `json:"overrides"`
	Attachments          []eventAttachment `json:"attachments,omitempty"`
	Containers           []eventContainer  `json:"containers"`
	Version              int64             `json:"version"`
	EnableExecuteCommand bool              `json:"enableExecuteCommand"`
}

// emitTaskStateLocked publishes an "ECS Task State Change" for task's current state and bumps its event version.
func (b *InMemoryBackend) emitTaskStateLocked(task *Task) {
	task.version++

	if b.events == nil {
		return
	}

	d := taskStateChangeDetail{
		Version:              task.version,
		ClusterArn:           task.ClusterArn,
		TaskArn:              task.TaskArn,
		TaskDefinitionArn:    task.TaskDefinitionArn,
		LastStatus:           task.LastStatus,
		DesiredStatus:        task.DesiredStatus,
		Group:                task.Group,
		LaunchType:           task.LaunchType,
		Connectivity:         task.Connectivity,
		ConnectivityAt:       eventTimePtr(task.ConnectivityAt),
		CreatedAt:            eventTimePtr(task.CreatedAt),
		StartedBy:            task.StartedBy,
		StoppedAt:            eventTimePtr(task.StoppedAt),
		StoppedReason:        task.StoppedReason,
		PlatformVersion:      task.PlatformVersion,
		ContainerInstanceArn: task.ContainerInstanceArn,
		EnableExecuteCommand: task.EnableExecuteCommand,
		UpdatedAt:            eventTime(time.Now()),
		Overrides:            eventOverrides{ContainerOverrides: []eventContainerOverride{}},
		Containers:           make([]eventContainer, 0, len(task.Containers)),
	}

	if task.StartedAt != nil {
		d.StartedAt = eventTimePtr(task.StartedAt)
	}

	if td, err := b.findTaskDefinitionLocked(task.TaskDefinitionArn); err == nil {
		d.CPU, d.Memory = td.CPU, td.Memory
	}

	if task.Overrides != nil {
		for _, co := range task.Overrides.ContainerOverrides {
			d.Overrides.ContainerOverrides = append(
				d.Overrides.ContainerOverrides,
				eventContainerOverride{Name: co.Name},
			)
		}

		if task.Overrides.CPU != "" {
			d.CPU = task.Overrides.CPU
		}

		if task.Overrides.Memory != "" {
			d.Memory = task.Overrides.Memory
		}
	}

	for _, a := range task.Attachments {
		d.Attachments = append(d.Attachments, eventAttachment(a))
	}

	for _, c := range task.Containers {
		ec := eventContainer{
			ExitCode: c.ExitCode, ContainerArn: c.ContainerArn, LastStatus: c.LastStatus, Name: c.Name,
			Image: c.Image, ImageDigest: c.ImageDigest, RuntimeID: c.RuntimeID, TaskArn: task.TaskArn,
			CPU: c.CPU, Memory: c.Memory, Reason: c.Reason,
		}

		for _, ni := range c.NetworkInterfaces {
			ec.NetworkInterfaces = append(ec.NetworkInterfaces,
				eventNetworkInterface{AttachmentID: ni.AttachmentID, PrivateIpv4Address: ni.PrivateIpv4Address})
		}

		d.Containers = append(d.Containers, ec)
	}

	b.emitLocked(detailTypeTaskState, []string{task.TaskArn}, d)
}

type containerInstanceStateChangeDetail struct {
	VersionInfo          *VersionInfo `json:"versionInfo,omitempty"`
	ClusterArn           string       `json:"clusterArn"`
	ContainerInstanceArn string       `json:"containerInstanceArn"`
	EC2InstanceID        string       `json:"ec2InstanceId"`
	Status               string       `json:"status"`
	RegisteredAt         string       `json:"registeredAt"`
	UpdatedAt            string       `json:"updatedAt"`
	AgentConnected       bool         `json:"agentConnected"`
	Version              int64        `json:"version"`
}

// emitContainerInstanceStateLocked publishes an "ECS Container Instance State Change".
func (b *InMemoryBackend) emitContainerInstanceStateLocked(ci *ContainerInstance) {
	if b.events == nil {
		return
	}

	b.emitLocked(detailTypeInstanceState, []string{ci.ContainerInstanceArn}, containerInstanceStateChangeDetail{
		VersionInfo:          ci.VersionInfo,
		ClusterArn:           ci.ClusterArn,
		ContainerInstanceArn: ci.ContainerInstanceArn,
		EC2InstanceID:        ci.EC2InstanceID,
		Status:               ci.Status,
		RegisteredAt:         eventTime(ci.RegisteredAt),
		UpdatedAt:            eventTime(time.Now()),
		AgentConnected:       ci.AgentConnected,
		Version:              ci.Version,
	})
}

type deploymentStateChangeDetail struct {
	EventType    string `json:"eventType"`
	EventName    string `json:"eventName"`
	DeploymentID string `json:"deploymentId"`
	UpdatedAt    string `json:"updatedAt"`
	Reason       string `json:"reason,omitempty"`
}

// emitDeploymentStateLocked publishes an "ECS Deployment State Change" for dep.
func (b *InMemoryBackend) emitDeploymentStateLocked(svc *Service, depID, eventName, reason string) {
	if b.events == nil {
		return
	}

	eventType := eventTypeInfo
	if eventName == eventDeploymentFailed {
		eventType = eventTypeError
	}

	b.emitLocked(detailTypeDeploymentState, []string{svc.ServiceArn}, deploymentStateChangeDetail{
		EventType: eventType, EventName: eventName, DeploymentID: depID,
		UpdatedAt: eventTime(time.Now()), Reason: reason,
	})
}

type serviceActionDetail struct {
	EventType  string `json:"eventType"`
	EventName  string `json:"eventName"`
	ClusterArn string `json:"clusterArn"`
	CreatedAt  string `json:"createdAt"`
}

// emitServiceActionLocked publishes an "ECS Service Action".
func (b *InMemoryBackend) emitServiceActionLocked(svc *Service, eventName string) {
	if b.events == nil {
		return
	}

	b.emitLocked(detailTypeServiceAction, []string{svc.ServiceArn}, serviceActionDetail{
		EventType: eventTypeInfo, EventName: eventName, ClusterArn: svc.ClusterArn, CreatedAt: eventTime(time.Now()),
	})
}
