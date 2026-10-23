package gcpspanner

import (
	"context"
	"errors"
	"slices"
	"strings"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// CreateInstance starts creating an instance; the operation completes once the data plane has it too.
func (b *InMemoryBackend) CreateInstance(in *instancepb.CreateInstanceRequest) (*longrunningpb.Operation, error) {
	project, err := parseProject(in.GetParent())
	if err != nil {
		return nil, err
	}

	if err = validateInstanceID(in.GetInstanceId()); err != nil {
		return nil, err
	}

	if in.GetInstance() == nil {
		return nil, errInvalid("instance is required.")
	}

	pb, err := buildInstance(project, in.GetInstanceId(), in.GetInstance(), b.now())
	if err != nil {
		return nil, err
	}

	if err = b.insertInstance(&instRec{pb: pb, dbs: make(map[string]*dbRec)}); err != nil {
		return nil, err
	}

	name, start := pb.GetName(), b.ts()
	meta := func(end *timestamppb.Timestamp) proto.Message {
		return &instancepb.CreateInstanceMetadata{
			Instance:  b.snapshotInstance(name, pb),
			StartTime: start,
			EndTime:   end,
		}
	}

	return b.begin(opName(name, ""), meta, func() (proto.Message, error) { return b.finishCreate(project, pb) })
}

func (b *InMemoryBackend) snapshotInstance(name string, fallback *instancepb.Instance) *instancepb.Instance {
	b.mu.RLock("snapshotInstance")
	defer b.mu.RUnlock()

	if r, ok := b.instances[name]; ok {
		return cloneInstance(r.pb)
	}

	return cloneInstance(fallback)
}

func (b *InMemoryBackend) insertInstance(rec *instRec) error {
	b.mu.Lock("CreateInstance")
	defer b.mu.Unlock()

	name := rec.pb.GetName()
	if _, exists := b.instances[name]; exists {
		return errExists("Instance already exists: %s", name)
	}

	if len(b.instances) >= maxInstances {
		return errExhausted("At most %d instances can exist in the emulator.", maxInstances)
	}

	b.instances[name] = rec

	return nil
}

func (b *InMemoryBackend) finishCreate(project string, pb *instancepb.Instance) (proto.Message, error) {
	err := b.emu.mirrorInstance(context.Background(), project, pb)
	if err != nil && !errors.Is(err, errNoDataPlane) {
		b.mu.Lock("finishCreate")
		if b.instances[pb.GetName()] != nil && b.instances[pb.GetName()].pb == pb {
			delete(b.instances, pb.GetName())
		}
		b.mu.Unlock()

		return nil, err
	}

	b.mu.Lock("finishCreate")
	defer b.mu.Unlock()

	rec, ok := b.instances[pb.GetName()]
	if !ok || rec.pb != pb {
		return nil, errGone
	}

	pb.State = instancepb.Instance_READY
	pb.UpdateTime = b.ts()

	return cloneInstance(pb), nil
}

// GetInstance returns an instance.
func (b *InMemoryBackend) GetInstance(name string) (*instancepb.Instance, error) {
	if _, _, err := parseInstance(name); err != nil {
		return nil, err
	}

	b.mu.RLock("GetInstance")
	defer b.mu.RUnlock()

	rec, ok := b.instances[name]
	if !ok {
		return nil, errInstanceNotFound(name)
	}

	return cloneInstance(rec.pb), nil
}

// ListInstances lists the instances of a project.
func (b *InMemoryBackend) ListInstances(
	in *instancepb.ListInstancesRequest,
) (*instancepb.ListInstancesResponse, error) {
	project, err := parseProject(in.GetParent())
	if err != nil {
		return nil, err
	}

	if in.GetFilter() != "" {
		return nil, errUnimplemented("ListInstances filter is not supported by this emulator.")
	}

	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	if err = page.ValidateToken(in.GetPageToken()); err != nil {
		return nil, errInvalid("Invalid page_token.")
	}

	prefix := "projects/" + project + "/instances/"

	b.mu.RLock("ListInstances")
	names := make([]string, 0, len(b.instances))

	for name := range b.instances {
		if strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	}

	slices.Sort(names)

	all := make([]*instancepb.Instance, 0, len(names))
	for _, n := range names {
		all = append(all, cloneInstance(b.instances[n].pb))
	}
	b.mu.RUnlock()

	pg := page.New(all, in.GetPageToken(), int(in.GetPageSize()), defaultPageSize)

	return &instancepb.ListInstancesResponse{Instances: pg.Data, NextPageToken: pg.Next}, nil
}

// UpdateInstance applies the masked fields of the instance.
func (b *InMemoryBackend) UpdateInstance(in *instancepb.UpdateInstanceRequest) (*longrunningpb.Operation, error) {
	pb := in.GetInstance()
	if pb == nil {
		return nil, errInvalid("instance is required.")
	}

	if _, _, err := parseInstance(pb.GetName()); err != nil {
		return nil, err
	}

	paths := in.GetFieldMask().GetPaths()
	if len(paths) == 0 {
		return nil, errInvalid("field_mask is required.")
	}

	start := b.ts()

	b.mu.Lock("UpdateInstance")
	rec, ok := b.instances[pb.GetName()]

	if !ok {
		b.mu.Unlock()

		return nil, errInstanceNotFound(pb.GetName())
	}

	updated, err := b.applyUpdate(rec, pb, paths)
	b.mu.Unlock()

	if err != nil {
		return nil, err
	}

	meta := &instancepb.UpdateInstanceMetadata{Instance: updated, StartTime: start, EndTime: b.ts()}

	return b.finished(opName(pb.GetName(), ""), meta, updated)
}

// applyUpdate mutates rec; the caller holds b.mu.
func (b *InMemoryBackend) applyUpdate(
	rec *instRec,
	in *instancepb.Instance,
	paths []string,
) (*instancepb.Instance, error) {
	if rec.pb.GetState() != instancepb.Instance_READY {
		return nil, errPrecondition("Instance %s is not ready (state %s).", rec.pb.GetName(), rec.pb.GetState())
	}

	next := cloneInstance(rec.pb)
	if err := applyInstanceMask(next, in, paths); err != nil {
		return nil, err
	}

	if maskHas(paths, "node_count") || maskHas(paths, "processing_units") {
		if err := normalizeCapacity(
			next,
			maskHas(paths, "node_count"),
			maskHas(paths, "processing_units"),
		); err != nil {
			return nil, err
		}
	}

	if err := validateCommon(next); err != nil {
		return nil, err
	}

	next.UpdateTime = b.ts()
	rec.pb = next

	return cloneInstance(next), nil
}

// DeleteInstance deletes an instance together with its databases.
func (b *InMemoryBackend) DeleteInstance(name string) (*emptypb.Empty, error) {
	if _, _, err := parseInstance(name); err != nil {
		return nil, err
	}

	b.mu.Lock("DeleteInstance")
	rec, ok := b.instances[name]

	if !ok {
		b.mu.Unlock()

		return nil, errInstanceNotFound(name)
	}

	if rec.pb.GetState() == instancepb.Instance_CREATING {
		b.mu.Unlock()

		return nil, errPrecondition("Instance %s is still being created.", name)
	}

	delete(b.instances, name)
	b.mu.Unlock()

	b.emu.dropInstance(context.Background(), name)

	return &emptypb.Empty{}, nil
}
