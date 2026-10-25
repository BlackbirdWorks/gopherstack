package gcpbigtable

import (
	"strings"
	"time"

	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

const (
	cpuTargetMin = 10
	cpuTargetMax = 80
)

func validateInstanceID(id string) error {
	if !instanceIDRe.MatchString(id) || len(id) > maxInstanceIDLen {
		return errInvalid("Invalid instance id %q: must match [a-z][a-z0-9-]+[a-z0-9] and be at most %d characters.",
			id, maxInstanceIDLen)
	}

	return nil
}

func validateClusterID(id string) error {
	if !clusterIDRe.MatchString(id) || len(id) > maxClusterIDLen {
		return errInvalid("Invalid cluster id %q: must match [a-z][-a-z0-9]* and be at most %d characters.",
			id, maxClusterIDLen)
	}

	return nil
}

func validateLabels(labels map[string]string) error {
	if len(labels) > maxLabels {
		return errInvalid("At most %d labels are allowed.", maxLabels)
	}

	for k, v := range labels {
		if k == "" || len(k) > maxLabelLen || !labelRe.MatchString(k) {
			return errInvalid("Invalid label key %q.", k)
		}

		if len(v) > maxLabelLen || (v != "" && !labelRe.MatchString(v)) {
			return errInvalid("Invalid value %q for label %q.", v, k)
		}
	}

	return nil
}

func validateAutoscaling(cfg *adminpb.Cluster_ClusterAutoscalingConfig) error {
	lim, tgt := cfg.GetAutoscalingLimits(), cfg.GetAutoscalingTargets()
	if lim == nil || tgt == nil {
		return errInvalid("cluster_autoscaling_config requires both autoscaling_limits and autoscaling_targets.")
	}

	if lim.GetMinServeNodes() < 1 || lim.GetMaxServeNodes() < lim.GetMinServeNodes() {
		return errInvalid("autoscaling_limits: min_serve_nodes must be at least 1 and no greater than max_serve_nodes.")
	}

	if cpu := tgt.GetCpuUtilizationPercent(); cpu != 0 && (cpu < cpuTargetMin || cpu > cpuTargetMax) {
		return errInvalid("cpu_utilization_percent must be between %d and %d.", cpuTargetMin, cpuTargetMax)
	}

	if tgt.GetCpuUtilizationPercent() == 0 && tgt.GetStorageUtilizationGibPerNode() == 0 {
		return errInvalid("autoscaling_targets must set at least one target.")
	}

	if tgt.GetStorageUtilizationGibPerNode() < 0 {
		return errInvalid("storage_utilization_gib_per_node must not be negative.")
	}

	return nil
}

func buildCluster(project, instance, id string, in *adminpb.Cluster, dev bool) (*adminpb.Cluster, error) {
	if err := validateClusterID(id); err != nil {
		return nil, err
	}

	if in == nil {
		return nil, errInvalid("Cluster %q is required.", id)
	}

	loc := locationRe.FindStringSubmatch(in.GetLocation())
	if loc == nil || loc[1] != project {
		return nil, errInvalid("Invalid location %q: want projects/%s/locations/{location}.", in.GetLocation(), project)
	}

	out := clone(in)
	out.Name = clusterName(instance, id)
	out.State = adminpb.Cluster_READY

	if out.GetDefaultStorageType() == adminpb.StorageType_STORAGE_TYPE_UNSPECIFIED {
		out.DefaultStorageType = adminpb.StorageType_SSD
	}

	if out.GetNodeScalingFactor() == adminpb.Cluster_NODE_SCALING_FACTOR_UNSPECIFIED {
		out.NodeScalingFactor = adminpb.Cluster_NODE_SCALING_FACTOR_1X
	}

	if err := settleScale(out, dev); err != nil {
		return nil, err
	}

	return out, nil
}

func settleScale(c *adminpb.Cluster, dev bool) error {
	if cfg := c.GetClusterConfig().GetClusterAutoscalingConfig(); cfg != nil {
		if dev {
			return errInvalid("Autoscaling is not supported for DEVELOPMENT instances.")
		}

		if err := validateAutoscaling(cfg); err != nil {
			return err
		}

		c.ServeNodes = min(max(c.GetServeNodes(), cfg.GetAutoscalingLimits().GetMinServeNodes()),
			cfg.GetAutoscalingLimits().GetMaxServeNodes())

		return nil
	}

	c.Config = nil

	switch {
	case dev && c.GetServeNodes() != 0:
		return errInvalid("serve_nodes must not be set for DEVELOPMENT instances.")
	case !dev && c.GetServeNodes() < 1:
		return errInvalid("serve_nodes must be at least 1 for PRODUCTION clusters.")
	}

	return nil
}

func (b *InMemoryBackend) instanceLocked(name string) (*instRec, error) {
	if _, _, err := parseInstance(name); err != nil {
		return nil, err
	}

	rec, ok := b.instances[name]
	if !ok {
		return nil, errInstanceNotFound(name)
	}

	return rec, nil
}

// CreateInstance creates an instance with its clusters.
func (b *InMemoryBackend) CreateInstance(in *adminpb.CreateInstanceRequest) (*longrunningpb.Operation, error) {
	project, err := parseProject(in.GetParent())
	if err != nil {
		return nil, err
	}

	if err = validateInstanceID(in.GetInstanceId()); err != nil {
		return nil, err
	}

	rec, err := buildInstance(project, in, b.now().UTC())
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CreateInstance")
	defer b.mu.Unlock()

	if _, exists := b.instances[rec.pb.GetName()]; exists {
		return nil, errExists("Instance %q already exists.", rec.pb.GetName())
	}

	b.instances[rec.pb.GetName()] = rec

	return b.finish(rec.pb.GetName(), &adminpb.CreateInstanceMetadata{
		OriginalRequest: clone(in), RequestTime: b.ts(), FinishTime: b.ts(),
	}, clone(rec.pb))
}

func buildInstance(project string, in *adminpb.CreateInstanceRequest, now time.Time) (*instRec, error) {
	inst := in.GetInstance()
	if inst == nil {
		return nil, errInvalid("instance is required.")
	}

	if inst.GetDisplayName() == "" {
		return nil, errInvalid("instance.display_name is required.")
	}

	if len(in.GetClusters()) == 0 {
		return nil, errInvalid("At least one cluster is required.")
	}

	if err := validateLabels(inst.GetLabels()); err != nil {
		return nil, err
	}

	typ := inst.GetType()

	switch typ {
	case adminpb.Instance_TYPE_UNSPECIFIED:
		typ = adminpb.Instance_PRODUCTION
	case adminpb.Instance_PRODUCTION:
	case adminpb.Instance_DEVELOPMENT:
		if len(in.GetClusters()) != 1 {
			return nil, errInvalid("A DEVELOPMENT instance must have exactly one cluster.")
		}
	default:
		return nil, errInvalid("Unknown instance type %v.", typ)
	}

	name := instanceName(project, in.GetInstanceId())
	pb := clone(inst)
	pb.Name, pb.State, pb.Type = name, adminpb.Instance_READY, typ
	pb.CreateTime = timestamppb.New(now)

	rec := newInstRec(pb)
	locations := map[string]bool{}

	for _, id := range collections.SortedKeys(in.GetClusters()) {
		c, err := buildCluster(project, name, id, in.GetClusters()[id], typ == adminpb.Instance_DEVELOPMENT)
		if err != nil {
			return nil, err
		}

		if locations[c.GetLocation()] {
			return nil, errInvalid("Only one cluster per location is allowed: %q.", c.GetLocation())
		}

		locations[c.GetLocation()] = true
		rec.clusters[id] = c
	}

	return rec, nil
}

// GetInstance returns an instance.
func (b *InMemoryBackend) GetInstance(name string) (*adminpb.Instance, error) {
	b.mu.RLock("GetInstance")
	defer b.mu.RUnlock()

	rec, err := b.instanceLocked(name)
	if err != nil {
		return nil, err
	}

	return clone(rec.pb), nil
}

// ListInstances lists the instances of a project.
func (b *InMemoryBackend) ListInstances(in *adminpb.ListInstancesRequest) (*adminpb.ListInstancesResponse, error) {
	project, err := parseProject(in.GetParent())
	if err != nil {
		return nil, err
	}

	prefix := "projects/" + project + "/instances/"

	b.mu.RLock("ListInstances")
	defer b.mu.RUnlock()

	out := &adminpb.ListInstancesResponse{}

	for _, name := range collections.SortedKeys(b.instances) {
		if strings.HasPrefix(name, prefix) {
			out.Instances = append(out.Instances, clone(b.instances[name].pb))
		}
	}

	return out, nil
}

// UpdateInstance replaces an instance's display name and type.
func (b *InMemoryBackend) UpdateInstance(in *adminpb.Instance) (*adminpb.Instance, error) {
	b.mu.Lock("UpdateInstance")
	defer b.mu.Unlock()

	rec, err := b.instanceLocked(in.GetName())
	if err != nil {
		return nil, err
	}

	next := clone(rec.pb)
	next.DisplayName = in.GetDisplayName()

	if in.GetType() != adminpb.Instance_TYPE_UNSPECIFIED {
		next.Type = in.GetType()
	}

	if err = applyInstanceUpdate(rec, next); err != nil {
		return nil, err
	}

	return clone(rec.pb), nil
}

func applyInstanceUpdate(rec *instRec, next *adminpb.Instance) error {
	if next.GetDisplayName() == "" {
		return errInvalid("display_name must not be empty.")
	}

	if next.GetType() != rec.pb.GetType() {
		if rec.pb.GetType() != adminpb.Instance_DEVELOPMENT || next.GetType() != adminpb.Instance_PRODUCTION {
			return errInvalid("An instance can only be upgraded from DEVELOPMENT to PRODUCTION.")
		}

		for _, c := range rec.clusters {
			c.ServeNodes = max(c.GetServeNodes(), 1)
		}
	}

	rec.pb = next

	return nil
}

// PartialUpdateInstance updates the masked fields of an instance.
func (b *InMemoryBackend) PartialUpdateInstance(
	in *adminpb.PartialUpdateInstanceRequest,
) (*longrunningpb.Operation, error) {
	paths := in.GetUpdateMask().GetPaths()
	if len(paths) == 0 {
		return nil, errInvalid("update_mask must be set.")
	}

	b.mu.Lock("PartialUpdateInstance")
	defer b.mu.Unlock()

	rec, err := b.instanceLocked(in.GetInstance().GetName())
	if err != nil {
		return nil, err
	}

	next := clone(rec.pb)
	src := in.GetInstance()

	for _, p := range paths {
		switch {
		case p == "display_name":
			next.DisplayName = src.GetDisplayName()
		case p == "type":
			next.Type = src.GetType()
		case p == "edition":
			next.Edition = src.GetEdition()
		case p == "labels":
			if err = validateLabels(src.GetLabels()); err != nil {
				return nil, err
			}

			next.Labels = clone(src).GetLabels()
		case strings.HasPrefix(p, "labels."):
			key := strings.TrimPrefix(p, "labels.")
			if v, ok := src.GetLabels()[key]; ok {
				if next.Labels == nil {
					next.Labels = map[string]string{}
				}

				next.Labels[key] = v
			} else {
				delete(next.Labels, key)
			}
		default:
			return nil, errInvalid("Unsupported update_mask path %q for Instance.", p)
		}
	}

	if err = validateLabels(next.GetLabels()); err != nil {
		return nil, err
	}

	if err = applyInstanceUpdate(rec, next); err != nil {
		return nil, err
	}

	return b.finish(next.GetName(), &adminpb.UpdateInstanceMetadata{
		OriginalRequest: clone(in), RequestTime: b.ts(), FinishTime: b.ts(),
	}, clone(rec.pb))
}

// DeleteInstance deletes an instance and everything in it.
func (b *InMemoryBackend) DeleteInstance(name string) (*emptypb.Empty, error) {
	b.mu.Lock("DeleteInstance")
	defer b.mu.Unlock()

	if _, err := b.instanceLocked(name); err != nil {
		return nil, err
	}

	delete(b.instances, name)

	return &emptypb.Empty{}, nil
}

func (b *InMemoryBackend) clusterLocked(name string) (*instRec, string, error) {
	instName, err := parseCluster(name)
	if err != nil {
		return nil, "", err
	}

	rec, err := b.instanceLocked(instName)
	if err != nil {
		return nil, "", err
	}

	id := lastSegment(name)
	if _, ok := rec.clusters[id]; !ok {
		return nil, "", errClusterNotFound(name)
	}

	return rec, id, nil
}

// CreateCluster adds a cluster to an instance.
func (b *InMemoryBackend) CreateCluster(in *adminpb.CreateClusterRequest) (*longrunningpb.Operation, error) {
	project, _, err := parseInstance(in.GetParent())
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CreateCluster")
	defer b.mu.Unlock()

	rec, err := b.instanceLocked(in.GetParent())
	if err != nil {
		return nil, err
	}

	if rec.pb.GetType() == adminpb.Instance_DEVELOPMENT {
		return nil, errPrecondition("Clusters cannot be added to a DEVELOPMENT instance.")
	}

	c, err := buildCluster(project, in.GetParent(), in.GetClusterId(), in.GetCluster(), false)
	if err != nil {
		return nil, err
	}

	if _, exists := rec.clusters[in.GetClusterId()]; exists {
		return nil, errExists("Cluster %q already exists.", c.GetName())
	}

	for _, other := range rec.clusters {
		if other.GetLocation() == c.GetLocation() {
			return nil, errInvalid("Only one cluster per location is allowed: %q.", c.GetLocation())
		}
	}

	rec.clusters[in.GetClusterId()] = c

	return b.finish(in.GetParent(), &adminpb.CreateClusterMetadata{
		OriginalRequest: clone(in), RequestTime: b.ts(), FinishTime: b.ts(),
	}, clone(c))
}

// GetCluster returns a cluster.
func (b *InMemoryBackend) GetCluster(name string) (*adminpb.Cluster, error) {
	b.mu.RLock("GetCluster")
	defer b.mu.RUnlock()

	rec, id, err := b.clusterLocked(name)
	if err != nil {
		return nil, err
	}

	return clone(rec.clusters[id]), nil
}

// ListClusters lists the clusters of an instance, or of every instance in a project when the instance is "-".
func (b *InMemoryBackend) ListClusters(in *adminpb.ListClustersRequest) (*adminpb.ListClustersResponse, error) {
	project, instID, err := parseInstance(in.GetParent())
	if err != nil {
		return nil, err
	}

	b.mu.RLock("ListClusters")
	defer b.mu.RUnlock()

	var recs []*instRec

	if instID == wildcard {
		prefix := "projects/" + project + "/instances/"

		for _, n := range collections.SortedKeys(b.instances) {
			if strings.HasPrefix(n, prefix) {
				recs = append(recs, b.instances[n])
			}
		}
	} else {
		rec, ierr := b.instanceLocked(in.GetParent())
		if ierr != nil {
			return nil, ierr
		}

		recs = append(recs, rec)
	}

	out := &adminpb.ListClustersResponse{}

	for _, rec := range recs {
		for _, id := range collections.SortedKeys(rec.clusters) {
			out.Clusters = append(out.Clusters, clone(rec.clusters[id]))
		}
	}

	return out, nil
}

// UpdateCluster changes the serve_nodes of a cluster.
func (b *InMemoryBackend) UpdateCluster(in *adminpb.Cluster) (*longrunningpb.Operation, error) {
	b.mu.Lock("UpdateCluster")
	defer b.mu.Unlock()

	rec, id, err := b.clusterLocked(in.GetName())
	if err != nil {
		return nil, err
	}

	next := clone(rec.clusters[id])
	next.ServeNodes = in.GetServeNodes()
	next.Config = nil

	if err = settleScale(next, rec.pb.GetType() == adminpb.Instance_DEVELOPMENT); err != nil {
		return nil, err
	}

	rec.clusters[id] = next

	return b.finish(rec.pb.GetName(), &adminpb.UpdateClusterMetadata{
		OriginalRequest: clone(in), RequestTime: b.ts(), FinishTime: b.ts(),
	}, clone(next))
}

// PartialUpdateCluster updates the masked fields of a cluster.
func (b *InMemoryBackend) PartialUpdateCluster(
	in *adminpb.PartialUpdateClusterRequest,
) (*longrunningpb.Operation, error) {
	paths := in.GetUpdateMask().GetPaths()
	if len(paths) == 0 {
		return nil, errInvalid("update_mask must be set.")
	}

	b.mu.Lock("PartialUpdateCluster")
	defer b.mu.Unlock()

	rec, id, err := b.clusterLocked(in.GetCluster().GetName())
	if err != nil {
		return nil, err
	}

	next := clone(rec.clusters[id])

	if err = applyClusterMask(next, in.GetCluster(), paths); err != nil {
		return nil, err
	}

	if err = settleScale(next, rec.pb.GetType() == adminpb.Instance_DEVELOPMENT); err != nil {
		return nil, err
	}

	rec.clusters[id] = next

	return b.finish(rec.pb.GetName(), &adminpb.PartialUpdateClusterMetadata{
		OriginalRequest: clone(in), RequestTime: b.ts(), FinishTime: b.ts(),
	}, clone(next))
}

func applyClusterMask(dst, src *adminpb.Cluster, paths []string) error {
	const autoscaling = "cluster_config.cluster_autoscaling_config"

	for _, p := range paths {
		switch {
		case p == "serve_nodes":
			dst.ServeNodes = src.GetServeNodes()
		case p == "cluster_config" || p == autoscaling:
			dst.Config = nil

			if cfg := src.GetClusterConfig().GetClusterAutoscalingConfig(); cfg != nil {
				dst.Config = clone(src).Config
			}
		case strings.HasPrefix(p, autoscaling+"."):
			if err := applyAutoscalingSubpath(dst, src, strings.TrimPrefix(p, autoscaling+".")); err != nil {
				return err
			}
		default:
			return errInvalid("Cluster field %q cannot be updated.", p)
		}
	}

	return nil
}

func applyAutoscalingSubpath(dst, src *adminpb.Cluster, sub string) error {
	cfg := dst.GetClusterConfig().GetClusterAutoscalingConfig()
	if cfg == nil {
		return errInvalid("Cluster has no autoscaling config to update.")
	}

	in := src.GetClusterConfig().GetClusterAutoscalingConfig()

	switch sub {
	case "autoscaling_limits":
		cfg.AutoscalingLimits = clone(in.GetAutoscalingLimits())
	case "autoscaling_limits.min_serve_nodes":
		cfg.GetAutoscalingLimits().MinServeNodes = in.GetAutoscalingLimits().GetMinServeNodes()
	case "autoscaling_limits.max_serve_nodes":
		cfg.GetAutoscalingLimits().MaxServeNodes = in.GetAutoscalingLimits().GetMaxServeNodes()
	case "autoscaling_targets":
		cfg.AutoscalingTargets = clone(in.GetAutoscalingTargets())
	case "autoscaling_targets.cpu_utilization_percent":
		cfg.GetAutoscalingTargets().CpuUtilizationPercent = in.GetAutoscalingTargets().GetCpuUtilizationPercent()
	case "autoscaling_targets.storage_utilization_gib_per_node":
		cfg.GetAutoscalingTargets().StorageUtilizationGibPerNode = in.GetAutoscalingTargets().GetStorageUtilizationGibPerNode()
	default:
		return errInvalid("Unsupported update_mask path %q.", sub)
	}

	return nil
}

// DeleteCluster deletes a cluster and its backups; the last cluster of an instance cannot be deleted.
func (b *InMemoryBackend) DeleteCluster(name string) (*emptypb.Empty, error) {
	b.mu.Lock("DeleteCluster")
	defer b.mu.Unlock()

	rec, id, err := b.clusterLocked(name)
	if err != nil {
		return nil, err
	}

	if len(rec.clusters) == 1 {
		return nil, errPrecondition("Cannot delete the last cluster of an instance; delete the instance instead.")
	}

	for _, pr := range rec.profiles {
		if sr := pr.GetSingleClusterRouting(); sr != nil && sr.GetClusterId() == id {
			return nil, errPrecondition("App profile %q routes to this cluster.", pr.GetName())
		}
	}

	delete(rec.clusters, id)

	for n := range rec.backups {
		if strings.HasPrefix(n, name+"/backups/") {
			delete(rec.backups, n)
		}
	}

	return &emptypb.Empty{}, nil
}
