package main

import (
	"context"
	"errors"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
	"github.com/blackbirdworks/gopherstack/pkgs/regionpeers"
	cwbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
	elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"
	gluebackend "github.com/blackbirdworks/gopherstack/services/glue"
	mqbackend "github.com/blackbirdworks/gopherstack/services/mq"
	resourcegroupstaggingapibackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

var errOpenSearchRegionUnavailable = errors.New("opensearch backend unavailable for region")

// originRegion returns the region of the first ARN that names one, else the context region.
func originRegion(ctx context.Context, arns ...string) string {
	for _, a := range arns {
		if r := arnRegion(a); r != "" {
			return r
		}
	}

	return awsmeta.Region(ctx)
}

// inRegion returns ctx carrying region; an empty region leaves ctx unchanged.
func inRegion(ctx context.Context, region string) context.Context {
	if region == "" {
		return ctx
	}

	m := *awsmeta.Get(ctx)
	m.Region = region

	return awsmeta.Set(ctx, &m)
}

// regionalMetricEmitter publishes a metric to the CloudWatch backend of the emitting resource's region.
func regionalMetricEmitter(cwH *cwbackend.Handler) cwmetric.EmitterFunc {
	return func(p cwmetric.Point) error {
		dims := make([]cwbackend.Dimension, len(p.Dimensions))
		for i, d := range p.Dimensions {
			dims[i] = cwbackend.Dimension{Name: d.Name, Value: d.Value}
		}

		return cwH.BackendFor(p.Region).PutMetricData(p.Namespace, []cwbackend.MetricDatum{{
			MetricName: p.Name,
			Namespace:  p.Namespace,
			Value:      p.Value,
			HasValue:   true,
			Count:      1,
			Sum:        p.Value,
			Min:        p.Value,
			Max:        p.Value,
			Unit:       p.Unit,
			Dimensions: dims,
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

// regionalEC2Backend maps a region to its EC2 backend, falling back to home.
func regionalEC2Backend(
	h *ec2backend.Handler, home *ec2backend.InMemoryBackend,
) func(string) *ec2backend.InMemoryBackend {
	return func(region string) *ec2backend.InMemoryBackend {
		if bk, ok := regionpeers.Backend[*ec2backend.InMemoryBackend](h.BackendFor, region); ok {
			return bk
		}

		return home
	}
}

// regionalELBv2Backend maps a region to its ELBv2 backend, falling back to home.
func regionalELBv2Backend(
	h *elbv2backend.Handler, home *elbv2backend.InMemoryBackend,
) func(string) *elbv2backend.InMemoryBackend {
	return func(region string) *elbv2backend.InMemoryBackend {
		if bk, ok := regionpeers.Backend[*elbv2backend.InMemoryBackend](h.BackendFor, region); ok {
			return bk
		}

		return home
	}
}

// taggedEntries converts a service's TaggedEntry slice to the bridge's entry type.
func taggedEntries[T ~struct {
	Tags map[string]string
	ARN  string
}](items []T) []taggedARNEntry {
	out := make([]taggedARNEntry, len(items))
	for i, it := range items {
		out[i] = taggedARNEntry(it)
	}

	return out
}

// asBackend returns v as B, or home when v is not a B.
func asBackend[B any](v any, home B) B {
	if b, ok := v.(B); ok {
		return b
	}

	return home
}

// regionalTagSpec describes one region-isolated service's tagging surface.
type regionalTagSpec[B any] struct {
	backendFor     func(region string) B
	list           func(B) []taggedARNEntry
	tag            func(B, context.Context, string, map[string]string) error
	untag          func(B, context.Context, string, []string) error
	resourceTypeOf func(arn string) string
	arnService     string
}

// wireRegionalTagging lists the request region's resources and tags or untags in the ARN's region.
func wireRegionalTagging[B any](bk resourcegroupstaggingapibackend.StorageBackend, spec regionalTagSpec[B]) {
	registerTaggingService(
		bk,
		func(ctx context.Context) []resourcegroupstaggingapibackend.TaggedResource {
			items := spec.list(spec.backendFor(awsmeta.Region(ctx)))
			out := make([]resourcegroupstaggingapibackend.TaggedResource, 0, len(items))

			for _, item := range items {
				out = append(out, resourcegroupstaggingapibackend.TaggedResource{
					ResourceARN: item.ARN, ResourceType: spec.resourceTypeOf(item.ARN), Tags: item.Tags,
				})
			}

			return out
		},
		spec.arnService,
		func(ctx context.Context, arn string, newTags map[string]string) error {
			return spec.tag(spec.backendFor(originRegion(ctx, arn)), ctx, arn, newTags)
		},
		func(ctx context.Context, arn string, keys []string) error {
			return spec.untag(spec.backendFor(originRegion(ctx, arn)), ctx, arn, keys)
		},
	)
}

// taggedEntry is the TaggedEntry shape every bridged service shares.
type taggedEntry interface {
	~struct {
		Tags map[string]string
		ARN  string
	}
}

// tagSurface is the plain tagging surface of a bridged backend.
type tagSurface[E taggedEntry] interface {
	TaggedResources() []E
	TagResource(arn string, tags map[string]string) error
	UntagResource(arn string, keys []string) error
}

// wireStdRegionalTagging bridges a backend with the plain tagging surface, one backend per region.
func wireStdRegionalTagging[B tagSurface[E], E taggedEntry](
	bk resourcegroupstaggingapibackend.StorageBackend,
	arnService string,
	resourceTypeOf func(string) string,
	backendOf func(region string) any,
) {
	home, ok := backendOf("").(B)
	if !ok {
		return
	}

	wireRegionalTagging(bk, regionalTagSpec[B]{
		arnService:     arnService,
		resourceTypeOf: resourceTypeOf,
		backendFor:     func(region string) B { return asBackend(backendOf(region), home) },
		list:           func(b B) []taggedARNEntry { return taggedEntries(b.TaggedResources()) },
		tag: func(b B, _ context.Context, arn string, tags map[string]string) error {
			return b.TagResource(arn, tags)
		},
		untag: func(b B, _ context.Context, arn string, keys []string) error {
			return b.UntagResource(arn, keys)
		},
	})
}

// arnResourceType returns a resourceTypeOf that derives the type from the ARN for service.
func arnResourceType(service string) func(string) string {
	return func(arn string) string { return resourceTypeFromARN(arn, service) }
}

// regionalBackendFor resolves a region's backend as B through backendOf, falling back to the home one.
func regionalBackendFor[B any](backendOf func(region string) any) func(string) B {
	home, _ := backendOf("").(B)

	return func(region string) B { return asBackend(backendOf(region), home) }
}

// mqRegionResolver finds a broker's consumer endpoint in the region its ARN names.
type mqRegionResolver struct{ handler *mqbackend.Handler }

func (r *mqRegionResolver) MQConsumerEndpoint(brokerARN string) (string, string, bool) {
	bk, ok := r.handler.BackendFor(arnRegion(brokerARN)).(*mqbackend.InMemoryBackend)
	if !ok {
		return "", "", false
	}

	return bk.MQConsumerEndpoint(brokerARN)
}
