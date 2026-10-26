package gcpdatastore

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/datastore/admin/apiv1/adminpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	indexIDBase     = 1_000_000_000_000_000_000
	defaultIdxPage  = 100
	maxIndexProps   = 100
	maxCompositeIdx = 200
)

type adminServer struct {
	adminpb.UnimplementedDatastoreAdminServer
	b *InMemoryBackend
}

func (s *adminServer) CreateIndex(
	_ context.Context, in *adminpb.CreateIndexRequest,
) (*longrunningpb.Operation, error) {
	return s.b.CreateIndex(in)
}

func (s *adminServer) DeleteIndex(
	_ context.Context, in *adminpb.DeleteIndexRequest,
) (*longrunningpb.Operation, error) {
	return s.b.DeleteIndex(in)
}

func (s *adminServer) GetIndex(_ context.Context, in *adminpb.GetIndexRequest) (*adminpb.Index, error) {
	return s.b.GetIndex(in)
}

func (s *adminServer) ListIndexes(
	_ context.Context, in *adminpb.ListIndexesRequest,
) (*adminpb.ListIndexesResponse, error) {
	return s.b.ListIndexes(in)
}

func indexKey(project, id string) string { return project + "/" + id }

func cloneIndex(i *adminpb.Index) *adminpb.Index {
	out, _ := proto.Clone(i).(*adminpb.Index)

	return out
}

func validateIndex(project string, idx *adminpb.Index) error {
	if project == "" {
		return errInvalid("Missing required project ID.")
	}

	if idx == nil || idx.GetKind() == "" {
		return errInvalid("An index requires a kind.")
	}

	if idx.GetAncestor() == adminpb.Index_ANCESTOR_MODE_UNSPECIFIED {
		return errInvalid("An index requires an ancestor mode (NONE or ALL_ANCESTORS).")
	}

	if len(idx.GetProperties()) == 0 || len(idx.GetProperties()) > maxIndexProps {
		return errInvalid("An index needs between 1 and %d properties.", maxIndexProps)
	}

	seen := map[string]struct{}{}

	for _, p := range idx.GetProperties() {
		if p.GetName() == "" || p.GetDirection() == adminpb.Index_DIRECTION_UNSPECIFIED {
			return errInvalid("Every index property needs a name and a direction.")
		}

		if _, dup := seen[p.GetName()]; dup {
			return errInvalid("Index property %q is listed more than once.", p.GetName())
		}

		seen[p.GetName()] = struct{}{}
	}

	return nil
}

func sameDefinition(a, b *adminpb.Index) bool {
	if a.GetKind() != b.GetKind() || a.GetAncestor() != b.GetAncestor() ||
		len(a.GetProperties()) != len(b.GetProperties()) {
		return false
	}

	for i, p := range a.GetProperties() {
		if p.GetName() != b.GetProperties()[i].GetName() || p.GetDirection() != b.GetProperties()[i].GetDirection() {
			return false
		}
	}

	return true
}

func (b *InMemoryBackend) indexOp(
	project string, op adminpb.OperationType, idx *adminpb.Index,
) (*longrunningpb.Operation, error) {
	now := timestamppb.New(b.wallLocked())
	meta := &adminpb.IndexOperationMetadata{
		Common: &adminpb.CommonMetadata{
			StartTime: now, EndTime: now, OperationType: op, State: adminpb.CommonMetadata_SUCCESSFUL,
		},
		IndexId: idx.GetIndexId(),
	}

	name := "projects/" + project + "/operations/" + strconv.FormatInt(time.Now().UnixNano(), 36) + idx.GetIndexId()

	o, err := gcplro.Done(name, meta, idx)
	if err != nil {
		return nil, errInvalid("%v", err)
	}

	return b.ops.Put(o), nil
}

// CreateIndex registers a composite index; it is READY immediately.
func (b *InMemoryBackend) CreateIndex(in *adminpb.CreateIndexRequest) (*longrunningpb.Operation, error) {
	if err := validateIndex(in.GetProjectId(), in.GetIndex()); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateIndex")
	defer b.mu.Unlock()

	n := 0

	for _, ex := range b.indexes {
		if ex.GetProjectId() != in.GetProjectId() {
			continue
		}

		n++

		if sameDefinition(ex, in.GetIndex()) {
			return nil, errExists("Index already exists: %s.", ex.GetIndexId())
		}
	}

	if n >= maxCompositeIdx {
		return nil, errPrecondition("The project already has the maximum of %d composite indexes.", maxCompositeIdx)
	}

	b.nextIndex++

	idx := cloneIndex(in.GetIndex())
	idx.ProjectId = in.GetProjectId()
	idx.IndexId = strconv.FormatInt(indexIDBase+b.nextIndex, 10)
	idx.State = adminpb.Index_READY
	b.indexes[indexKey(idx.GetProjectId(), idx.GetIndexId())] = idx

	return b.indexOp(idx.GetProjectId(), adminpb.OperationType_CREATE_INDEX, cloneIndex(idx))
}

// DeleteIndex removes a composite index.
func (b *InMemoryBackend) DeleteIndex(in *adminpb.DeleteIndexRequest) (*longrunningpb.Operation, error) {
	if in.GetProjectId() == "" || in.GetIndexId() == "" {
		return nil, errInvalid("project_id and index_id are required.")
	}

	b.mu.Lock("DeleteIndex")
	defer b.mu.Unlock()

	k := indexKey(in.GetProjectId(), in.GetIndexId())

	idx, ok := b.indexes[k]
	if !ok {
		return nil, errNotFound("Index %s not found.", in.GetIndexId())
	}

	delete(b.indexes, k)

	out := cloneIndex(idx)
	out.State = adminpb.Index_DELETING

	return b.indexOp(in.GetProjectId(), adminpb.OperationType_DELETE_INDEX, out)
}

// GetIndex returns one composite index.
func (b *InMemoryBackend) GetIndex(in *adminpb.GetIndexRequest) (*adminpb.Index, error) {
	b.mu.RLock("GetIndex")
	defer b.mu.RUnlock()

	idx, ok := b.indexes[indexKey(in.GetProjectId(), in.GetIndexId())]
	if !ok {
		return nil, errNotFound("Index %s not found.", in.GetIndexId())
	}

	return cloneIndex(idx), nil
}

// ListIndexes pages through a project's composite indexes in creation order.
func (b *InMemoryBackend) ListIndexes(in *adminpb.ListIndexesRequest) (*adminpb.ListIndexesResponse, error) {
	if in.GetProjectId() == "" {
		return nil, errInvalid("Missing required project ID.")
	}

	if in.GetFilter() != "" {
		return nil, errInvalid("ListIndexes filter is not supported.")
	}

	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	if in.GetPageToken() != "" {
		if err := page.ValidateToken(in.GetPageToken()); err != nil {
			return nil, errInvalid("Invalid page token.")
		}
	}

	b.mu.RLock("ListIndexes")

	var all []*adminpb.Index

	for _, idx := range b.indexes {
		if idx.GetProjectId() == in.GetProjectId() {
			all = append(all, cloneIndex(idx))
		}
	}

	b.mu.RUnlock()

	slices.SortFunc(all, func(x, y *adminpb.Index) int { return strings.Compare(x.GetIndexId(), y.GetIndexId()) })

	pg := page.New(all, in.GetPageToken(), int(in.GetPageSize()), defaultIdxPage)

	return &adminpb.ListIndexesResponse{Indexes: pg.Data, NextPageToken: pg.Next}, nil
}
