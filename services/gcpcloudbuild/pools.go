package gcpcloudbuild

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"strings"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

var poolImmutable = []string{ //nolint:gochecknoglobals // immutable catalog
	"name", "uid", fieldCreateTime, "update_time", "delete_time", "state", "etag",
}

func poolEtag(p *cloudbuildpb.WorkerPool) string {
	cp := clonePool(p)
	cp.Etag, cp.UpdateTime = "", nil
	raw, _ := protojson.Marshal(cp)
	sum := sha256.Sum256(raw)

	return hex.EncodeToString(sum[:8])
}

func validPoolName(name string) error {
	parts := strings.Split(name, "/")
	if len(parts) != 6 || parts[0] != kwProjects || parts[2] != kwLocations || parts[4] != "workerPools" ||
		parts[1] == "" || parts[3] == "" ||
		parts[5] == "" {
		return errInvalid(
			"Invalid worker pool name %q: want projects/{project}/locations/{location}/workerPools/{id}.",
			name,
		)
	}

	return nil
}

// poolOp records an already finished worker pool operation under parent.
func (b *InMemoryBackend) poolOp(
	parent string,
	meta, resp proto.Message,
) (*longrunningpb.Operation, error) {
	op, err := gcplro.Done(parent+"/operations/operation-"+uuid.NewString(), meta, resp)
	if err != nil {
		return nil, errInternal(err)
	}

	return b.ops.Put(op), nil
}

// CreateWorkerPool stores a worker pool; the pool is RUNNING when the operation completes.
func (b *InMemoryBackend) CreateWorkerPool(
	in *cloudbuildpb.CreateWorkerPoolRequest,
) (*longrunningpb.Operation, error) {
	project, location, err := parseParent(in.GetParent(), "")
	if err != nil || !strings.Contains(in.GetParent(), "/locations/") {
		return nil, errInvalid(
			"Invalid parent %q: want projects/{project}/locations/{location}.",
			in.GetParent(),
		)
	}

	if !idPattern.MatchString(in.GetWorkerPoolId()) {
		return nil, errInvalid("Invalid worker_pool_id %q.", in.GetWorkerPoolId())
	}

	if in.GetWorkerPool() == nil {
		return nil, errInvalid("worker_pool is required.")
	}

	now := b.ts()
	p := clonePool(in.GetWorkerPool())
	p.Name = "projects/" + project + "/locations/" + location + "/workerPools/" + in.GetWorkerPoolId()
	p.Uid, p.State = uuid.NewString(), cloudbuildpb.WorkerPool_RUNNING
	p.CreateTime, p.UpdateTime, p.DeleteTime = now, now, nil
	p.Etag = poolEtag(p)

	meta := &cloudbuildpb.CreateWorkerPoolOperationMetadata{
		WorkerPool:   p.GetName(),
		CreateTime:   now,
		CompleteTime: now,
	}

	if in.GetValidateOnly() {
		return b.poolOp(in.GetParent(), meta, p)
	}

	b.mu.Lock("CreateWorkerPool")
	defer b.mu.Unlock()

	if _, dup := b.pools[p.GetName()]; dup {
		return nil, status.Errorf(
			codes.AlreadyExists,
			"Worker pool %q already exists.",
			p.GetName(),
		)
	}

	if len(b.pools) >= maxWorkerPools {
		return nil, errPrecondition("Too many worker pools.")
	}

	b.pools[p.GetName()] = p

	return b.poolOp(in.GetParent(), meta, p)
}

// GetWorkerPool returns a worker pool.
func (b *InMemoryBackend) GetWorkerPool(name string) (*cloudbuildpb.WorkerPool, error) {
	if err := validPoolName(name); err != nil {
		return nil, err
	}

	b.mu.RLock("GetWorkerPool")
	defer b.mu.RUnlock()

	p, ok := b.pools[name]
	if !ok {
		return nil, errNotFound("Worker pool %q not found.", name)
	}

	return clonePool(p), nil
}

// ListWorkerPools lists the worker pools of a location ordered by name.
func (b *InMemoryBackend) ListWorkerPools(
	in *cloudbuildpb.ListWorkerPoolsRequest,
) ([]*cloudbuildpb.WorkerPool, string, error) {
	if _, _, err := parseParent(in.GetParent(), ""); err != nil ||
		!strings.Contains(in.GetParent(), "/locations/") {
		return nil, "", errInvalid(
			"Invalid parent %q: want projects/{project}/locations/{location}.",
			in.GetParent(),
		)
	}

	if in.GetPageSize() < 0 {
		return nil, "", errInvalid("page_size must not be negative.")
	}

	b.mu.RLock("ListWorkerPools")

	var all []*cloudbuildpb.WorkerPool

	for name, p := range b.pools {
		if strings.HasPrefix(name, in.GetParent()+"/workerPools/") {
			all = append(all, clonePool(p))
		}
	}

	b.mu.RUnlock()

	slices.SortFunc(
		all,
		func(x, y *cloudbuildpb.WorkerPool) int { return cmp.Compare(x.GetName(), y.GetName()) },
	)

	pg := page.New(all, in.GetPageToken(), int(in.GetPageSize()), defaultPageSize)

	return pg.Data, pg.Next, nil
}

// UpdateWorkerPool applies the masked fields (all mutable fields when no mask is given).
func (b *InMemoryBackend) UpdateWorkerPool(
	in *cloudbuildpb.UpdateWorkerPoolRequest,
) (*longrunningpb.Operation, error) {
	name := in.GetWorkerPool().GetName()
	if err := validPoolName(name); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateWorkerPool")
	defer b.mu.Unlock()

	cur, ok := b.pools[name]
	if !ok {
		return nil, errNotFound("Worker pool %q not found.", name)
	}

	next := clonePool(cur)

	paths := in.GetUpdateMask().GetPaths()
	if len(paths) == 0 {
		paths = []string{"display_name", "annotations", "private_pool_v1_config"}
	}

	if err := applyMask(next, in.GetWorkerPool(), paths, poolImmutable); err != nil {
		return nil, err
	}

	now := b.ts()
	next.UpdateTime = now
	next.Etag = poolEtag(next)
	meta := &cloudbuildpb.UpdateWorkerPoolOperationMetadata{
		WorkerPool:   name,
		CreateTime:   now,
		CompleteTime: now,
	}

	parent := poolParent(name)

	if in.GetValidateOnly() {
		return b.poolOp(parent, meta, next)
	}

	b.pools[name] = next

	return b.poolOp(parent, meta, next)
}

// DeleteWorkerPool removes a worker pool; a stale etag aborts the call.
func (b *InMemoryBackend) DeleteWorkerPool(
	in *cloudbuildpb.DeleteWorkerPoolRequest,
) (*longrunningpb.Operation, error) {
	name := in.GetName()
	if err := validPoolName(name); err != nil {
		return nil, err
	}

	parent := poolParent(name)
	now := b.ts()
	meta := &cloudbuildpb.DeleteWorkerPoolOperationMetadata{
		WorkerPool:   name,
		CreateTime:   now,
		CompleteTime: now,
	}

	b.mu.Lock("DeleteWorkerPool")
	defer b.mu.Unlock()

	cur, ok := b.pools[name]
	if !ok {
		if in.GetAllowMissing() {
			return b.poolOp(parent, meta, &emptypb.Empty{})
		}

		return nil, errNotFound("Worker pool %q not found.", name)
	}

	if in.GetEtag() != "" && in.GetEtag() != cur.GetEtag() {
		return nil, status.Errorf(
			codes.Aborted,
			"The etag %q does not match the worker pool's current etag.",
			in.GetEtag(),
		)
	}

	if !in.GetValidateOnly() {
		delete(b.pools, name)
	}

	return b.poolOp(parent, meta, &emptypb.Empty{})
}

// poolParent returns the location a validated worker pool name belongs to.
func poolParent(name string) string {
	parent, _, _ := strings.Cut(name, "/workerPools/")

	return parent
}
