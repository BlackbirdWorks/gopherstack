package ecs

import (
	"context"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// FISActions returns the FIS action definitions that the ECS service supports.
func (h *Handler) FISActions() []service.FISActionDefinition {
	return []service.FISActionDefinition{
		{
			ActionID:    "aws:ecs:stop-task",
			Description: "Stop running ECS tasks",
			TargetType:  "aws:ecs:task",
		},
	}
}

// ExecuteFISAction executes a FIS action against resolved ECS targets.
func (h *Handler) ExecuteFISAction(_ context.Context, action service.FISActionExecution) error {
	if action.ActionID != "aws:ecs:stop-task" {
		return nil
	}

	for _, taskARN := range action.Targets {
		cluster := clusterFromTaskARN(taskARN)
		// Ignore not-found errors — the task may have already stopped.
		_, _ = h.Backend.StopTask(cluster, taskARN, "FIS aws:ecs:stop-task")
	}

	return nil
}

// clusterFromTaskARN extracts the cluster name from an ECS task ARN.
// Task ARN format (new): arn:aws:ecs:{region}:{account}:task/{cluster}/{taskID}
// Task ARN format (old): arn:aws:ecs:{region}:{account}:task/{taskID}
// Returns an empty string (which resolves to "default") when the cluster cannot
// be determined.
func clusterFromTaskARN(taskARN string) string {
	// Find the "task/" prefix.
	const taskPrefix = ":task/"
	_, after, ok := strings.Cut(taskARN, taskPrefix)

	if !ok {
		return ""
	}

	// Everything after ":task/"
	rest := after

	// New-format ARN has "cluster/taskID" after "task/".
	if cluster, _, found := strings.Cut(rest, "/"); found {
		return cluster
	}

	// Old-format ARN — cluster is not encoded; return empty to use default.
	return ""
}
