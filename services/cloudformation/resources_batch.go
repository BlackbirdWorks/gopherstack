package cloudformation

import (
	"context"
	"fmt"
	"math"
	"strconv"

	batchbackend "github.com/blackbirdworks/gopherstack/services/batch"
)

// ---- Batch ----

func (rc *ResourceCreator) createBatchComputeEnvironment(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Batch == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "ComputeEnvironmentName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	ceType := strProp(props, "Type", params, physicalIDs)
	if ceType == "" {
		ceType = "MANAGED"
	}

	ce, err := rc.backends.Batch.Backend.CreateComputeEnvironment(
		ctx,
		name,
		ceType,
		"ENABLED",
		nil,
		"",
		nil,
		nil,
		nil,
		nil,
	)
	if err != nil {
		return "", fmt.Errorf("create Batch compute environment %s: %w", name, err)
	}

	return ce.ComputeEnvironmentArn, nil
}

func (rc *ResourceCreator) deleteBatchComputeEnvironment(ctx context.Context, arnOrName string) error {
	if rc.backends.Batch == nil {
		return nil
	}

	// AWS requires DISABLED state before deletion.
	_, err := rc.backends.Batch.Backend.UpdateComputeEnvironment(
		ctx, arnOrName, "DISABLED", "", nil, nil, nil,
	)
	if err != nil {
		return fmt.Errorf("disable Batch compute environment %s: %w", arnOrName, err)
	}

	return rc.backends.Batch.Backend.DeleteComputeEnvironment(ctx, arnOrName)
}

func (rc *ResourceCreator) createBatchJobQueue(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Batch == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "JobQueueName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	var priority int32 = 1
	if pStr := strProp(props, "Priority", params, physicalIDs); pStr != "" {
		if p, err := strconv.ParseInt(pStr, 10, 32); err == nil {
			priority = int32(p)
		}
	} else if pRaw, ok := props["Priority"].(float64); ok {
		priority = int32(pRaw)
	}

	var ceOrder []batchbackend.ComputeEnvironmentOrder
	if rawList, ok := props["ComputeEnvironmentOrder"].([]any); ok {
		for i, item := range rawList {
			if m, ok2 := item.(map[string]any); ok2 {
				order := int32(math.MaxInt32)
				if i <= math.MaxInt32 {
					order = int32(i)
				}

				ceOrder = append(ceOrder, batchbackend.ComputeEnvironmentOrder{
					ComputeEnvironment: resolve(m["ComputeEnvironment"], params, physicalIDs),
					Order:              order,
				})
			}
		}
	}

	jq, err := rc.backends.Batch.Backend.CreateJobQueue(
		ctx,
		name,
		priority,
		"ENABLED",
		ceOrder,
		nil,
		"",
		nil,
		"",
		nil,
	)
	if err != nil {
		return "", fmt.Errorf("create Batch job queue %s: %w", name, err)
	}

	return jq.JobQueueArn, nil
}

func (rc *ResourceCreator) deleteBatchJobQueue(ctx context.Context, arnOrName string) error {
	if rc.backends.Batch == nil {
		return nil
	}

	// AWS requires DISABLED state before deletion.
	disabled := "DISABLED"
	if _, err := rc.backends.Batch.Backend.UpdateJobQueue(
		ctx, arnOrName, nil, disabled, "", nil, nil, nil,
	); err != nil {
		return fmt.Errorf("disable Batch job queue %s: %w", arnOrName, err)
	}

	return rc.backends.Batch.Backend.DeleteJobQueue(ctx, arnOrName)
}

func (rc *ResourceCreator) createBatchJobDefinition(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Batch == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "JobDefinitionName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	defType := strProp(props, "Type", params, physicalIDs)
	if defType == "" {
		defType = "container"
	}

	jd, err := rc.backends.Batch.Backend.RegisterJobDefinition(
		ctx,
		name,
		defType,
		nil,
		nil,
		0,
		0,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		false,
		nil,
	)
	if err != nil {
		return "", fmt.Errorf("create Batch job definition %s: %w", name, err)
	}

	return jd.JobDefinitionArn, nil
}

func (rc *ResourceCreator) deleteBatchJobDefinition(ctx context.Context, arnOrNameRev string) error {
	if rc.backends.Batch == nil {
		return nil
	}

	return rc.backends.Batch.Backend.DeregisterJobDefinition(ctx, arnOrNameRev)
}
