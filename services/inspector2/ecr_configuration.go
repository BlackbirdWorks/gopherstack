package inspector2

import (
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ecs"
)

// ecsSibling resolves the ECS handler lazily: handlers are wired only after every provider initialises.
type ecsSibling interface {
	GetECSHandler() service.Registerable
}

// SetAppConfig records the service.AppContext.Config for lazy sibling lookup.
func (b *InMemoryBackend) SetAppConfig(cfg any) {
	b.mu.Lock("SetAppConfig")
	defer b.mu.Unlock()

	b.appConfig = cfg
}

func (b *InMemoryBackend) ecsBackend() (ecs.Backend, bool) {
	b.mu.RLock("ecsBackend")
	cfg, region := b.appConfig, b.region
	b.mu.RUnlock()

	s, ok := cfg.(ecsSibling)
	if !ok {
		return nil, false
	}

	h, ok := s.GetECSHandler().(*ecs.Handler)
	if !ok || h == nil {
		return nil, false
	}

	return h.BackendFor(region), true
}

// clusterUse accumulates the tasks of one ECS task group running an image in one cluster.
type clusterUse struct {
	lastInUse      time.Time
	clusterArn     string
	group          string
	taskDefinition string
	running        int64
	stopped        int64
}

// containerRunsImage reports whether c runs the ECR image identified by digest.
func containerRunsImage(c ecs.Container, digest string) bool {
	return c.ImageDigest == digest || strings.HasSuffix(c.Image, "@"+digest)
}

func (u *clusterUse) add(t *ecs.Task) {
	stopped := t.LastStatus == "STOPPED"
	if stopped {
		u.stopped++
	} else {
		u.running++
	}

	for _, ts := range []*time.Time{t.StartedAt, t.StoppedAt} {
		if ts != nil && ts.After(u.lastInUse) {
			u.lastInUse = *ts
		}
	}
}

// clusterTasks returns the RUNNING and STOPPED tasks of cluster.
func clusterTasks(backend ecs.Backend, cluster string) []ecs.Task {
	var tasks []ecs.Task

	for _, status := range []string{"RUNNING", "STOPPED"} {
		arns, err := backend.ListTasksFiltered(ecs.ListTasksInput{Cluster: cluster, DesiredStatus: status})
		if err != nil || len(arns) == 0 {
			continue
		}

		described, _, descErr := backend.DescribeTasks(cluster, arns)
		if descErr == nil {
			tasks = append(tasks, described...)
		}
	}

	return tasks
}

// ecsUsesOfImage lists the ECS task groups running the image with the given digest.
func ecsUsesOfImage(backend ecs.Backend, digest string) []*clusterUse {
	clusters, err := backend.ListClusters()
	if err != nil {
		return nil
	}

	uses := map[string]*clusterUse{}

	for _, cl := range clusters {
		for _, t := range clusterTasks(backend, cl.ClusterName) {
			if !runsImage(&t, digest) {
				continue
			}

			key := cl.ClusterArn + "\x00" + t.Group + "\x00" + t.TaskDefinitionArn

			u, ok := uses[key]
			if !ok {
				u = &clusterUse{clusterArn: cl.ClusterArn, group: t.Group, taskDefinition: t.TaskDefinitionArn}
				uses[key] = u
			}

			u.add(&t)
		}
	}

	out := slices.Collect(maps.Values(uses))
	slices.SortFunc(out, func(x, y *clusterUse) int { return strings.Compare(x.sortKey(), y.sortKey()) })

	return out
}

func (u *clusterUse) sortKey() string {
	return u.clusterArn + "\x00" + u.group + "\x00" + u.taskDefinition
}

func runsImage(t *ecs.Task, digest string) bool {
	return slices.ContainsFunc(t.Containers, func(c ecs.Container) bool { return containerRunsImage(c, digest) })
}

func (u *clusterUse) detail() map[string]any {
	detail := map[string]any{
		"clusterMetadata": map[string]any{
			"awsEcsMetadataDetails": map[string]any{"detailsGroup": u.group, "taskDefinitionArn": u.taskDefinition},
		},
		"runningUnitCount": u.running,
		"stoppedUnitCount": u.stopped,
	}

	if !u.lastInUse.IsZero() {
		detail["lastInUse"] = awstime.Epoch(u.lastInUse)
	}

	return detail
}

// GetClustersForImage returns the ECS clusters running the ECR image with the given digest, one ClusterInformation
// per cluster. The required filter.resourceId is the image digest. EKS pods are not tracked.
func (b *InMemoryBackend) GetClustersForImage(resourceID string) (map[string]any, error) {
	if resourceID == "" {
		return nil, ErrValidation
	}

	backend, ok := b.ecsBackend()
	if !ok {
		return map[string]any{"cluster": []map[string]any{}}, nil
	}

	var (
		order    []string
		byArn    = map[string][]map[string]any{}
		clusters = []map[string]any{}
	)

	for _, u := range ecsUsesOfImage(backend, resourceID) {
		if _, seen := byArn[u.clusterArn]; !seen {
			order = append(order, u.clusterArn)
		}

		byArn[u.clusterArn] = append(byArn[u.clusterArn], u.detail())
	}

	for _, arn := range order {
		clusters = append(clusters, map[string]any{"clusterArn": arn, "clusterDetails": byArn[arn]})
	}

	return map[string]any{"cluster": clusters}, nil
}
