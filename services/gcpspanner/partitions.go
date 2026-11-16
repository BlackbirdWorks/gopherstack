package gcpspanner

import (
	"regexp"
	"slices"
	"strings"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxPartitions     = 100
	partitionSegment  = "/instancePartitions/"
	allInstancesToken = "-"
)

var partitionIDRe = regexp.MustCompile(`^[a-z][-a-z0-9]*[a-z0-9]$`)

func clonePartition(p *instancepb.InstancePartition) *instancepb.InstancePartition {
	out, _ := proto.Clone(p).(*instancepb.InstancePartition)

	return out
}

func parsePartition(name string) (string, string, error) {
	inst, id, ok := strings.Cut(name, partitionSegment)
	if !ok || id == "" || strings.Contains(id, "/") {
		return "", "", errInvalid("Invalid instance partition name %q.", name)
	}

	if _, _, err := parseInstance(inst); err != nil {
		return "", "", err
	}

	return inst, id, nil
}

func partitionCapacity(p *instancepb.InstancePartition) (bool, bool) {
	_, nodeSet := p.GetComputeCapacity().(*instancepb.InstancePartition_NodeCount)
	_, puSet := p.GetComputeCapacity().(*instancepb.InstancePartition_ProcessingUnits)

	return nodeSet, puSet
}

func setPartitionCapacity(p *instancepb.InstancePartition, c capacity) {
	if c.nodes != 0 {
		p.ComputeCapacity = &instancepb.InstancePartition_NodeCount{NodeCount: c.nodes}

		return
	}

	p.ComputeCapacity = &instancepb.InstancePartition_ProcessingUnits{ProcessingUnits: c.units}
}

// settlePartitionCapacity validates the capacity of p; autoscaling makes it output-only and starts it at the
// minimum limit.
func settlePartitionCapacity(p *instancepb.InstancePartition, cfg *instancepb.InstanceConfig) error {
	if a := p.GetAutoscalingConfig(); a != nil {
		if err := validateAutoscaling(a, cfg); err != nil {
			return err
		}

		setPartitionCapacity(p, autoscaleMin(a))

		return nil
	}

	nodeSet, puSet := partitionCapacity(p)
	tmp := &instancepb.Instance{NodeCount: p.GetNodeCount(), ProcessingUnits: p.GetProcessingUnits()}

	if err := normalizeCapacity(tmp, nodeSet, puSet); err != nil {
		return err
	}

	if nodeSet {
		setPartitionCapacity(p, capacity{nodes: tmp.GetNodeCount()})
	} else {
		setPartitionCapacity(p, capacity{units: tmp.GetProcessingUnits()})
	}

	return nil
}

// CreateInstancePartition creates a partition in an instance.
func (b *InMemoryBackend) CreateInstancePartition(
	in *instancepb.CreateInstancePartitionRequest,
) (*longrunningpb.Operation, error) {
	if _, _, err := parseInstance(in.GetParent()); err != nil {
		return nil, err
	}

	id := in.GetInstancePartitionId()
	if len(id) < minConfigIDLen || len(id) > maxConfigIDLen || !partitionIDRe.MatchString(id) {
		return nil, errInvalid(
			"instance_partition_id %q must match [a-z][-a-z0-9]*[a-z0-9] and be 2-64 characters.",
			id,
		)
	}

	pb := clonePartition(in.GetInstancePartition())
	if pb == nil {
		return nil, errInvalid("instance_partition is required.")
	}

	name := in.GetParent() + partitionSegment + id
	if pb.GetName() != "" && pb.GetName() != name {
		return nil, errInvalid("instance_partition.name must be %s.", name)
	}

	cfg, ok := b.lookupConfigName(pb.GetConfig())
	if !ok {
		return nil, errNotFound("Instance config not found: %s", pb.GetConfig())
	}

	if n := len(pb.GetDisplayName()); n < minDisplayName || n > maxDisplayName {
		return nil, errInvalid("display_name must be %d-%d characters.", minDisplayName, maxDisplayName)
	}

	if err := settlePartitionCapacity(pb, cfg); err != nil {
		return nil, err
	}

	start := b.ts()
	pb.Name, pb.State, pb.CreateTime, pb.UpdateTime = name, instancepb.InstancePartition_READY, start, start
	pb.ReferencingDatabases = nil
	pb.ProtoReflect().Clear(pb.ProtoReflect().Descriptor().Fields().ByName("referencing_backups"))

	b.mu.Lock("CreateInstancePartition")

	rec, err := b.instanceLocked(in.GetParent())
	if err == nil {
		err = b.insertPartitionLocked(rec, id, pb)
	}
	b.mu.Unlock()

	if err != nil {
		return nil, err
	}

	meta := &instancepb.CreateInstancePartitionMetadata{InstancePartition: pb, StartTime: start, EndTime: b.ts()}

	return b.finished(opName(name, ""), meta, pb)
}

func (b *InMemoryBackend) insertPartitionLocked(rec *instRec, id string, pb *instancepb.InstancePartition) error {
	if _, dup := rec.parts[id]; dup {
		return errExists("Instance partition already exists: %s", pb.GetName())
	}

	if len(rec.parts) >= maxPartitions {
		return errExhausted("At most %d instance partitions can exist in an instance.", maxPartitions)
	}

	pb.Etag = b.nextEtagString()
	rec.parts[id] = clonePartition(pb)

	return nil
}

// GetInstancePartition returns a partition.
func (b *InMemoryBackend) GetInstancePartition(name string) (*instancepb.InstancePartition, error) {
	inst, id, err := parsePartition(name)
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GetInstancePartition")
	defer b.mu.RUnlock()

	rec, err := b.instanceLocked(inst)
	if err != nil {
		return nil, err
	}

	p, ok := rec.parts[id]
	if !ok {
		return nil, errNotFound("Instance partition not found: %s", name)
	}

	return clonePartition(p), nil
}

// ListInstancePartitions lists the partitions of an instance, or of every instance with instances/-.
func (b *InMemoryBackend) ListInstancePartitions(
	in *instancepb.ListInstancePartitionsRequest,
) (*instancepb.ListInstancePartitionsResponse, error) {
	project, instID, err := parseInstance(in.GetParent())
	if err != nil {
		return nil, err
	}

	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	if err = page.ValidateToken(in.GetPageToken()); err != nil {
		return nil, errInvalid("Invalid page_token.")
	}

	b.mu.RLock("ListInstancePartitions")

	var names []string

	for n := range b.instances {
		if instID == allInstancesToken && strings.HasPrefix(n, "projects/"+project+"/instances/") ||
			n == in.GetParent() {
			names = append(names, n)
		}
	}

	if instID != allInstancesToken && len(names) == 0 {
		b.mu.RUnlock()

		return nil, errInstanceNotFound(in.GetParent())
	}

	slices.Sort(names)

	var all []*instancepb.InstancePartition

	for _, n := range names {
		ids := make([]string, 0, len(b.instances[n].parts))
		for id := range b.instances[n].parts {
			ids = append(ids, id)
		}

		slices.Sort(ids)

		for _, id := range ids {
			all = append(all, clonePartition(b.instances[n].parts[id]))
		}
	}
	b.mu.RUnlock()

	pg := page.New(all, in.GetPageToken(), int(in.GetPageSize()), defaultPageSize)

	return &instancepb.ListInstancePartitionsResponse{InstancePartitions: pg.Data, NextPageToken: pg.Next}, nil
}

// UpdateInstancePartition applies display_name, node_count, processing_units and autoscaling_config.
func (b *InMemoryBackend) UpdateInstancePartition(
	in *instancepb.UpdateInstancePartitionRequest,
) (*longrunningpb.Operation, error) {
	pb := in.GetInstancePartition()
	if pb == nil || pb.GetName() == "" {
		return nil, errInvalid("instance_partition.name is required.")
	}

	paths := in.GetFieldMask().GetPaths()
	if len(paths) == 0 {
		return nil, errInvalid("field_mask is required.")
	}

	inst, id, err := parsePartition(pb.GetName())
	if err != nil {
		return nil, err
	}

	start := b.ts()

	b.mu.Lock("UpdateInstancePartition")
	next, err := b.updatePartitionLocked(inst, id, pb, paths)
	b.mu.Unlock()

	if err != nil {
		return nil, err
	}

	meta := &instancepb.UpdateInstancePartitionMetadata{InstancePartition: next, StartTime: start, EndTime: b.ts()}

	return b.finished(opName(next.GetName(), ""), meta, next)
}

func (b *InMemoryBackend) updatePartitionLocked(
	inst, id string, in *instancepb.InstancePartition, paths []string,
) (*instancepb.InstancePartition, error) {
	rec, err := b.instanceLocked(inst)
	if err != nil {
		return nil, err
	}

	cur, ok := rec.parts[id]
	if !ok {
		return nil, errNotFound("Instance partition not found: %s", in.GetName())
	}

	if in.GetEtag() != "" && in.GetEtag() != cur.GetEtag() {
		return nil, errEtag(in.GetName())
	}

	next := clonePartition(cur)

	for _, p := range paths {
		if !copyPartitionPath(next, in, p) {
			return nil, errInvalid("Update mask path %q is not an updatable field.", p)
		}
	}

	cfg, _ := b.lookupConfigLocked(next.GetConfig())
	if err = b.settleUpdatedPartition(next, cur, paths, cfg); err != nil {
		return nil, err
	}

	next.UpdateTime, next.Etag = b.ts(), b.nextEtagString()
	rec.parts[id] = clonePartition(next)

	return next, nil
}

func copyPartitionPath(dst, src *instancepb.InstancePartition, path string) bool {
	switch name, _, _ := strings.Cut(path, "."); name {
	case fieldDisplayName:
		if n := len(src.GetDisplayName()); n < minDisplayName || n > maxDisplayName {
			return false
		}

		dst.DisplayName = src.GetDisplayName()
	case "node_count":
		dst.ComputeCapacity = &instancepb.InstancePartition_NodeCount{NodeCount: src.GetNodeCount()}
	case "processing_units":
		dst.ComputeCapacity = &instancepb.InstancePartition_ProcessingUnits{ProcessingUnits: src.GetProcessingUnits()}
	case "autoscaling_config":
		dst.AutoscalingConfig = src.GetAutoscalingConfig()
	default:
		return false
	}

	return true
}

func (b *InMemoryBackend) settleUpdatedPartition(
	next, cur *instancepb.InstancePartition, paths []string, cfg *instancepb.InstanceConfig,
) error {
	capSet := maskHas(paths, "node_count") || maskHas(paths, "processing_units")

	if next.GetAutoscalingConfig() != nil && capSet {
		return errInvalid("node_count and processing_units are output-only while autoscaling is enabled.")
	}

	switch {
	case next.GetAutoscalingConfig() != nil:
		if err := validateAutoscaling(next.GetAutoscalingConfig(), cfg); err != nil {
			return err
		}

		setPartitionCapacity(
			next,
			capacity{units: cur.GetProcessingUnits(), nodes: cur.GetNodeCount()}.clamp(next.GetAutoscalingConfig()),
		)
	case capSet:
		return settlePartitionCapacity(next, cfg)
	}

	return nil
}

// DeleteInstancePartition deletes a partition.
func (b *InMemoryBackend) DeleteInstancePartition(
	in *instancepb.DeleteInstancePartitionRequest,
) (*emptypb.Empty, error) {
	inst, id, err := parsePartition(in.GetName())
	if err != nil {
		return nil, err
	}

	b.mu.Lock("DeleteInstancePartition")
	defer b.mu.Unlock()

	rec, err := b.instanceLocked(inst)
	if err != nil {
		return nil, err
	}

	cur, ok := rec.parts[id]
	if !ok {
		return nil, errNotFound("Instance partition not found: %s", in.GetName())
	}

	if in.GetEtag() != "" && in.GetEtag() != cur.GetEtag() {
		return nil, errEtag(in.GetName())
	}

	delete(rec.parts, id)

	return &emptypb.Empty{}, nil
}

// ListInstancePartitionOperations lists the partition operations of an instance.
func (b *InMemoryBackend) ListInstancePartitionOperations(
	in *instancepb.ListInstancePartitionOperationsRequest,
) (*instancepb.ListInstancePartitionOperationsResponse, error) {
	if _, _, err := parseInstance(in.GetParent()); err != nil {
		return nil, err
	}

	ops, err := b.listOps(in.GetParent()+partitionSegment, in.GetFilter(), in.GetPageSize(), in.GetPageToken())
	if err != nil {
		return nil, err
	}

	return &instancepb.ListInstancePartitionOperationsResponse{Operations: ops.Data, NextPageToken: ops.Next}, nil
}
