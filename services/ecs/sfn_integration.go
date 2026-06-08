package ecs

import "context"

// SFNRunTask implements the Step Functions ECS RunTask service integration.
// It maps the SFN input map (ClusterArn, TaskDefinition, LaunchType, Overrides, etc.)
// to a RunTaskInput and returns the standard ECS RunTask response shape.
func (b *InMemoryBackend) SFNRunTask(_ context.Context, input map[string]any) (any, error) {
	rti := RunTaskInput{}
	rti.TaskDefinition, _ = input["TaskDefinition"].(string)
	rti.Cluster, _ = input["Cluster"].(string)
	rti.LaunchType, _ = input["LaunchType"].(string)

	if n, ok := toInt(input["Count"]); ok {
		rti.Count = n
	}

	tasks, err := b.RunTask(rti)
	if err != nil {
		return nil, err
	}

	taskAny := make([]any, len(tasks))
	for i, t := range tasks {
		taskAny[i] = t
	}

	return map[string]any{
		"Tasks":    taskAny,
		"Failures": []any{},
	}, nil
}

// toInt extracts an int from a map value (accepting float64 from JSON).
func toInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case float64:
		return int(n), true
	}

	return 0, false
}
