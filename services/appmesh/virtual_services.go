package appmesh

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) virtualServiceARN(meshName, name string) string {
	return arn.Build("appmesh", b.region, b.accountID, fmt.Sprintf("mesh/%s/virtualService/%s", meshName, name))
}

func (b *InMemoryBackend) CreateVirtualService(
	meshName, name string, spec json.RawMessage, tags map[string]string,
) (*VirtualService, error) {
	b.mu.Lock("CreateVirtualService")
	defer b.mu.Unlock()
	if !b.meshes.Has(meshName) {
		return nil, ErrMeshNotFound
	}
	key := meshChildKey(meshName, name)
	if b.virtualSvcs.Has(key) {
		return nil, ErrVirtualServiceAlreadyExists
	}
	arn := b.virtualServiceARN(meshName, name)
	vs := &VirtualService{
		Meta:               newMeta(arn, b.accountID),
		MeshName:           meshName,
		VirtualServiceName: name,
		Spec:               normalizeSpec(spec),
		Status:             statusActive,
	}
	b.virtualSvcs.Put(vs)
	if len(tags) > 0 {
		b.tags[arn] = cloneTags(tags)
	}

	return vs, nil
}

func (b *InMemoryBackend) DescribeVirtualService(meshName, name string) (*VirtualService, error) {
	b.mu.RLock("DescribeVirtualService")
	defer b.mu.RUnlock()
	if !b.meshes.Has(meshName) {
		return nil, ErrMeshNotFound
	}
	vs, ok := b.virtualSvcs.Get(meshChildKey(meshName, name))
	if !ok {
		return nil, ErrVirtualServiceNotFound
	}

	return vs, nil
}

func (b *InMemoryBackend) UpdateVirtualService(meshName, name string, spec json.RawMessage) (*VirtualService, error) {
	b.mu.Lock("UpdateVirtualService")
	defer b.mu.Unlock()
	if !b.meshes.Has(meshName) {
		return nil, ErrMeshNotFound
	}
	vs, ok := b.virtualSvcs.Get(meshChildKey(meshName, name))
	if !ok {
		return nil, ErrVirtualServiceNotFound
	}
	vs.Spec = normalizeSpec(spec)
	vs.Meta.UpdatedAt = time.Now().UTC()
	vs.Meta.Version++

	return vs, nil
}

func (b *InMemoryBackend) DeleteVirtualService(meshName, name string) (*VirtualService, error) {
	b.mu.Lock("DeleteVirtualService")
	defer b.mu.Unlock()
	if !b.meshes.Has(meshName) {
		return nil, ErrMeshNotFound
	}
	key := meshChildKey(meshName, name)
	vs, ok := b.virtualSvcs.Get(key)
	if !ok {
		return nil, ErrVirtualServiceNotFound
	}
	b.virtualSvcs.Delete(key)
	delete(b.tags, vs.Meta.Arn)

	return vs, nil
}

//nolint:dupl // list/create pattern is structurally identical across resource types
func (b *InMemoryBackend) ListVirtualServices(
	meshName string, maxResults int32, nextToken string,
) ([]*VirtualServiceSummary, string, error) {
	b.mu.RLock("ListVirtualServices")
	defer b.mu.RUnlock()
	if !b.meshes.Has(meshName) {
		return nil, "", ErrMeshNotFound
	}
	svcs := b.virtualSvcsByMesh.Get(meshName)
	names := make([]string, len(svcs))
	for i, vs := range svcs {
		names[i] = vs.VirtualServiceName
	}
	sort.Strings(names)
	items, next := paginateStrings(names, nextToken, maxResults)
	summaries := make([]*VirtualServiceSummary, 0, len(items))
	for _, n := range items {
		vs, _ := b.virtualSvcs.Get(meshChildKey(meshName, n))
		summaries = append(summaries, &VirtualServiceSummary{
			CreatedAt:          vs.Meta.CreatedAt,
			UpdatedAt:          vs.Meta.UpdatedAt,
			Arn:                vs.Meta.Arn,
			MeshName:           meshName,
			VirtualServiceName: vs.VirtualServiceName,
			MeshOwner:          vs.Meta.MeshOwner,
			ResourceOwner:      vs.Meta.ResourceOwner,
			Version:            vs.Meta.Version,
		})
	}

	return summaries, next, nil
}
