package appmesh

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) meshARN(meshName string) string {
	return arn.Build("appmesh", b.region, b.accountID, fmt.Sprintf("mesh/%s", meshName))
}

func (b *InMemoryBackend) CreateMesh(name string, spec json.RawMessage, tags map[string]string) (*Mesh, error) {
	b.mu.Lock("CreateMesh")
	defer b.mu.Unlock()
	if err := validateMeshSpec(spec); err != nil {
		return nil, err
	}
	if b.meshes.Has(name) {
		return nil, ErrMeshAlreadyExists
	}
	arn := b.meshARN(name)
	m := &Mesh{
		Meta:   newMeta(arn, b.accountID),
		Name:   name,
		Spec:   normalizeSpec(spec),
		Status: statusActive,
	}
	b.meshes.Put(m)
	if len(tags) > 0 {
		b.tags[arn] = cloneTags(tags)
	}

	return m, nil
}

func (b *InMemoryBackend) DescribeMesh(name string) (*Mesh, error) {
	b.mu.RLock("DescribeMesh")
	defer b.mu.RUnlock()
	m, ok := b.meshes.Get(name)
	if !ok {
		return nil, ErrMeshNotFound
	}

	return m, nil
}

func (b *InMemoryBackend) UpdateMesh(name string, spec json.RawMessage) (*Mesh, error) {
	b.mu.Lock("UpdateMesh")
	defer b.mu.Unlock()
	if err := validateMeshSpec(spec); err != nil {
		return nil, err
	}
	m, ok := b.meshes.Get(name)
	if !ok {
		return nil, ErrMeshNotFound
	}
	m.Spec = normalizeSpec(spec)
	m.Meta.UpdatedAt = time.Now().UTC()
	m.Meta.Version++

	return m, nil
}

func (b *InMemoryBackend) DeleteMesh(name string) (*Mesh, error) {
	b.mu.Lock("DeleteMesh")
	defer b.mu.Unlock()
	m, ok := b.meshes.Get(name)
	if !ok {
		return nil, ErrMeshNotFound
	}
	if len(b.virtualNodesByMesh.Get(name)) > 0 || len(b.virtualRoutersByMesh.Get(name)) > 0 ||
		len(b.virtualSvcsByMesh.Get(name)) > 0 || len(b.virtualGWsByMesh.Get(name)) > 0 {
		return nil, ErrMeshInUse
	}
	b.meshes.Delete(name)
	delete(b.tags, m.Meta.Arn)
	m.Status = statusDeleted

	return m, nil
}

func (b *InMemoryBackend) ListMeshes(maxResults int32, nextToken string) ([]*MeshSummary, string, error) {
	b.mu.RLock("ListMeshes")
	defer b.mu.RUnlock()
	all := b.meshes.Snapshot()
	names := make([]string, len(all))
	for i, m := range all {
		names[i] = m.Name
	}
	items, next := paginateStrings(names, nextToken, maxResults)
	summaries := make([]*MeshSummary, 0, len(items))
	for _, n := range items {
		m, _ := b.meshes.Get(n)
		summaries = append(summaries, &MeshSummary{
			CreatedAt:     m.Meta.CreatedAt,
			UpdatedAt:     m.Meta.UpdatedAt,
			Arn:           m.Meta.Arn,
			Name:          m.Name,
			MeshOwner:     m.Meta.MeshOwner,
			ResourceOwner: m.Meta.ResourceOwner,
			Version:       m.Meta.Version,
		})
	}

	return summaries, next, nil
}
