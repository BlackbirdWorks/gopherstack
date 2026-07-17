package ecs

import (
	"fmt"
	"strings"
	"time"
)

// agent.go implements the ECS-agent-facing state-submission
// operations (SubmitTaskStateChange, SubmitContainerStateChange,
// SubmitAttachmentStateChanges). These operations are used exclusively by the
// real Amazon ECS agent to report task/container/attachment lifecycle events
// back to the control plane; AWS documents them as "only used by the Amazon
// ECS agent, and not intended for use outside of the agent." Consistent with
// that eventually-consistent, agent-internal contract, unknown cluster/task/
// container references are tolerated as no-ops rather than surfaced as errors.

// AttachmentStateChange reports a status transition for a single task attachment
// (e.g. an ENI), identified by the attachment ARN previously handed out by ECS.
type AttachmentStateChange struct {
	AttachmentArn string `json:"attachmentArn"`
	Status        string `json:"status"`
}

// ContainerStateChangeItem reports a status transition for a single container
// within a SubmitTaskStateChange call.
type ContainerStateChangeItem struct {
	ExitCode        *int             `json:"exitCode,omitempty"`
	ContainerName   string           `json:"containerName,omitempty"`
	ImageDigest     string           `json:"imageDigest,omitempty"`
	Reason          string           `json:"reason,omitempty"`
	RuntimeID       string           `json:"runtimeId,omitempty"`
	NetworkBindings []NetworkBinding `json:"networkBindings,omitempty"`
}

// SubmitTaskStateChangeInput holds input for SubmitTaskStateChange.
type SubmitTaskStateChangeInput struct {
	Cluster     string                     `json:"cluster,omitempty"`
	Reason      string                     `json:"reason,omitempty"`
	Status      string                     `json:"status,omitempty"`
	Task        string                     `json:"task,omitempty"`
	Attachments []AttachmentStateChange    `json:"attachments,omitempty"`
	Containers  []ContainerStateChangeItem `json:"containers,omitempty"`
}

// SubmitContainerStateChangeInput holds input for SubmitContainerStateChange.
type SubmitContainerStateChangeInput struct {
	ExitCode        *int             `json:"exitCode,omitempty"`
	Cluster         string           `json:"cluster,omitempty"`
	ContainerName   string           `json:"containerName,omitempty"`
	Reason          string           `json:"reason,omitempty"`
	RuntimeID       string           `json:"runtimeId,omitempty"`
	Status          string           `json:"status,omitempty"`
	Task            string           `json:"task,omitempty"`
	NetworkBindings []NetworkBinding `json:"networkBindings,omitempty"`
}

// DiscoverPollEndpoint returns the ECS agent poll, Service Connect, and telemetry
// endpoints for the configured region. Real ECS agents use this to discover which
// regional endpoint to poll for task state updates.
func (b *InMemoryBackend) DiscoverPollEndpoint() (string, string, string) {
	b.mu.RLock("DiscoverPollEndpoint")
	defer b.mu.RUnlock()

	base := fmt.Sprintf("ecs-a-1.%s.amazonaws.com", b.region)

	return fmt.Sprintf("https://%s/", base),
		fmt.Sprintf("https://ecs-a-1.svc.%s.amazonaws.com/", b.region),
		fmt.Sprintf("https://ecs-t-1.%s.amazonaws.com/", b.region)
}

// resolveTaskInClusterLocked finds a task within clusterTasks by full ARN or by
// bare task ID (the trailing path segment of the ARN). Callers must hold b.mu.
func resolveTaskInClusterLocked(clusterTasks []*Task, ref string) *Task {
	if ref == "" {
		return nil
	}

	for _, t := range clusterTasks {
		if t.TaskArn == ref {
			return t
		}
	}

	if strings.HasPrefix(ref, "arn:") {
		return nil
	}

	suffix := "/" + ref
	for _, t := range clusterTasks {
		if strings.HasSuffix(t.TaskArn, suffix) {
			return t
		}
	}

	return nil
}

// applyContainerStateChangeLocked applies a single container's reported state
// change onto the matching container, if found. Callers must hold b.mu for writing.
func applyContainerStateChangeLocked(task *Task, c ContainerStateChangeItem) {
	for i := range task.Containers {
		if task.Containers[i].Name != c.ContainerName {
			continue
		}

		if c.ExitCode != nil {
			task.Containers[i].ExitCode = c.ExitCode
		}

		if c.Reason != "" {
			task.Containers[i].Reason = c.Reason
		}

		if c.RuntimeID != "" {
			task.Containers[i].RuntimeID = c.RuntimeID
		}

		if c.ImageDigest != "" {
			task.Containers[i].ImageDigest = c.ImageDigest
		}

		if c.NetworkBindings != nil {
			task.Containers[i].NetworkBindings = c.NetworkBindings
		}

		return
	}
}

// applyContainerStateChangesLocked applies per-container state updates reported
// alongside a task state change. Callers must hold b.mu for writing.
func applyContainerStateChangesLocked(task *Task, changes []ContainerStateChangeItem) {
	for _, c := range changes {
		applyContainerStateChangeLocked(task, c)
	}
}

// applyAttachmentStateChangesLocked applies attachment status updates to a task's
// attachments, matching by the attachment ID previously reported to the caller.
// Callers must hold b.mu for writing.
func applyAttachmentStateChangesLocked(task *Task, changes []AttachmentStateChange) {
	for _, c := range changes {
		for i := range task.Attachments {
			if task.Attachments[i].ID == c.AttachmentArn {
				task.Attachments[i].Status = c.Status
			}
		}
	}
}

// SubmitTaskStateChange records a task (and its containers'/attachments') status
// transition reported by the ECS agent. Unknown clusters or tasks are tolerated
// as no-ops, matching the tolerant, eventually-consistent contract of this
// agent-internal API.
func (b *InMemoryBackend) SubmitTaskStateChange(input SubmitTaskStateChangeInput) error {
	clusterName := clusterKey(b.resolveCluster(input.Cluster))

	b.mu.Lock("SubmitTaskStateChange")
	defer b.mu.Unlock()

	if !b.clusters.Has(clusterName) {
		return nil
	}

	task := resolveTaskInClusterLocked(b.tasksByCluster.Get(clusterName), input.Task)
	if task == nil {
		return nil
	}

	if input.Status != "" {
		task.LastStatus = input.Status

		if input.Status == statusStopped {
			task.StoppedReason = input.Reason

			if task.StoppedAt == nil {
				now := time.Now()
				task.StoppedAt = &now
			}
		}
	}

	applyContainerStateChangesLocked(task, input.Containers)
	applyAttachmentStateChangesLocked(task, input.Attachments)

	return nil
}

// SubmitContainerStateChange records a single container's status transition
// reported by the ECS agent. Unknown clusters, tasks, or containers are
// tolerated as no-ops.
func (b *InMemoryBackend) SubmitContainerStateChange(input SubmitContainerStateChangeInput) error {
	clusterName := clusterKey(b.resolveCluster(input.Cluster))

	b.mu.Lock("SubmitContainerStateChange")
	defer b.mu.Unlock()

	if !b.clusters.Has(clusterName) {
		return nil
	}

	task := resolveTaskInClusterLocked(b.tasksByCluster.Get(clusterName), input.Task)
	if task == nil {
		return nil
	}

	applyContainerStateChangeLocked(task, ContainerStateChangeItem{
		ContainerName:   input.ContainerName,
		ExitCode:        input.ExitCode,
		NetworkBindings: input.NetworkBindings,
		Reason:          input.Reason,
		RuntimeID:       input.RuntimeID,
	})

	if input.Status != "" {
		for i := range task.Containers {
			if task.Containers[i].Name == input.ContainerName {
				task.Containers[i].LastStatus = input.Status

				break
			}
		}
	}

	return nil
}

// SubmitAttachmentStateChanges records attachment status transitions reported by
// the ECS agent for one or more attachments within a cluster. Unknown clusters
// or attachment ARNs are tolerated as no-ops.
func (b *InMemoryBackend) SubmitAttachmentStateChanges(cluster string, attachments []AttachmentStateChange) error {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.Lock("SubmitAttachmentStateChanges")
	defer b.mu.Unlock()

	if !b.clusters.Has(clusterName) {
		return nil
	}

	for _, task := range b.tasksByCluster.Get(clusterName) {
		applyAttachmentStateChangesLocked(task, attachments)
	}

	return nil
}
