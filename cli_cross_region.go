package main

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/regionpeers"
	cwbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
	gluebackend "github.com/blackbirdworks/gopherstack/services/glue"
)

// originRegion returns the region of the first ARN that names one, else the context region.
func originRegion(ctx context.Context, arns ...string) string {
	for _, a := range arns {
		if r := arnRegion(a); r != "" {
			return r
		}
	}

	return awsmeta.Region(ctx)
}

// regionalMetricEmitter publishes a metric to the CloudWatch backend of the emitting resource's region.
func regionalMetricEmitter(
	cwH *cwbackend.Handler,
) func(region, namespace, name string, value float64, unit string) error {
	return func(region, namespace, name string, value float64, unit string) error {
		bk := cwH.BackendFor(region)

		return bk.PutMetricData(namespace, []cwbackend.MetricDatum{{
			MetricName: name,
			Value:      value,
			Unit:       unit,
			Timestamp:  time.Now(),
		}})
	}
}

// regionalECSBackend maps a region to its ECS backend, falling back to home.
func regionalECSBackend(
	h *ecsbackend.Handler, home *ecsbackend.InMemoryBackend,
) func(string) *ecsbackend.InMemoryBackend {
	return func(region string) *ecsbackend.InMemoryBackend {
		if bk, ok := regionpeers.Backend[*ecsbackend.InMemoryBackend](h.BackendFor, region); ok {
			return bk
		}

		return home
	}
}

// regionalGlueBackend maps a region to its Glue backend, falling back to home.
func regionalGlueBackend(
	h *gluebackend.Handler, home *gluebackend.InMemoryBackend,
) func(string) *gluebackend.InMemoryBackend {
	return func(region string) *gluebackend.InMemoryBackend {
		if bk, ok := regionpeers.Backend[*gluebackend.InMemoryBackend](h.BackendFor, region); ok {
			return bk
		}

		return home
	}
}

// sfnECSAdapter runs Step Functions ECS tasks in the region of the cluster, task definition or execution.
type sfnECSAdapter struct {
	handler *ecsbackend.Handler
	home    *ecsbackend.InMemoryBackend
}

func (a *sfnECSAdapter) SFNRunTask(ctx context.Context, input map[string]any) (any, error) {
	cluster, _ := input["Cluster"].(string)
	taskDef, _ := input["TaskDefinition"].(string)

	return regionalECSBackend(a.handler, a.home)(originRegion(ctx, cluster, taskDef)).SFNRunTask(ctx, input)
}

// sfnGlueAdapter starts Step Functions Glue job runs in the execution's region.
type sfnGlueAdapter struct {
	handler *gluebackend.Handler
	home    *gluebackend.InMemoryBackend
}

func (a *sfnGlueAdapter) SFNStartJobRun(
	ctx context.Context, jobName string, args map[string]string,
) (string, error) {
	return regionalGlueBackend(a.handler, a.home)(awsmeta.Region(ctx)).SFNStartJobRun(ctx, jobName, args)
}
