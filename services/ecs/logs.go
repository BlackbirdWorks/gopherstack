package ecs

import "strings"

const (
	logDriverAwslogs       = "awslogs"
	optAwslogsGroup        = "awslogs-group"
	optAwslogsStreamPrefix = "awslogs-stream-prefix"
)

// SetCWLogsBackend wires CloudWatch Logs so an awslogs-driver container's log
// group/stream become discoverable when its task starts. Passing nil restores
// the historical behavior of LogConfiguration being stored and echoed with no
// effect on CloudWatch Logs.
//
// This does not forward any container output: gopherstack's Docker client
// (services/ecs/docker_runner.go) has no ContainerLogs method, so there is no
// source of container stdout/stderr to push as log events yet
// (gopherstack-sv5q). Only the destination is made to exist.
func (b *InMemoryBackend) SetCWLogsBackend(cwl CWLogsBackend) {
	b.mu.Lock("SetCWLogsBackend")
	defer b.mu.Unlock()
	b.cwLogs = cwl
}

// ensureAwslogsStreams creates the CloudWatch Logs group/stream named by each
// awslogs-driver container in td, when a CWLogsBackend is wired. Must be
// called without the backend lock held: it calls out to another backend.
func (b *InMemoryBackend) ensureAwslogsStreams(task *Task, td *TaskDefinition) {
	if task == nil || td == nil {
		return
	}

	var cwl CWLogsBackend

	b.mu.RLock("ensureAwslogsStreams")
	cwl = b.cwLogs
	b.mu.RUnlock()

	if cwl == nil {
		return
	}

	taskID := taskIDFromARN(task.TaskArn)

	for _, cd := range td.ContainerDefinitions {
		lc := cd.LogConfiguration
		if lc == nil || lc.LogDriver != logDriverAwslogs {
			continue
		}

		group := lc.Options[optAwslogsGroup]
		if group == "" {
			continue
		}

		stream := awslogsStreamName(lc.Options[optAwslogsStreamPrefix], cd.Name, taskID)

		_ = cwl.EnsureLogGroupAndStream(group, stream)
	}
}

// taskIDFromARN extracts the task ID (the segment after the last "/") from an
// ECS task ARN of either format handled by clusterFromTaskARN.
func taskIDFromARN(taskARN string) string {
	if idx := strings.LastIndex(taskARN, "/"); idx != -1 {
		return taskARN[idx+1:]
	}

	return taskARN
}

// awslogsStreamName derives the log stream name for an awslogs-driver
// container. Per the aws-sdk-go-v2 doc for LogConfiguration's
// awslogs-stream-prefix option (service/ecs types/types.go): "If you specify
// a prefix with this option, then the log stream takes the format
// prefix-name/container-name/ecs-task-id." When no prefix is configured, real
// ECS instead names the stream after the Docker-assigned container ID, which
// gopherstack does not have available at this layer (no ContainerLogs
// plumbing yet); the task ID alone is used as an approximation.
func awslogsStreamName(prefix, containerName, taskID string) string {
	if prefix == "" {
		return taskID
	}

	return prefix + "/" + containerName + "/" + taskID
}
