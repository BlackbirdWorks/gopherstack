package gcpkms

import (
	"cloud.google.com/go/kms/apiv1/kmspb"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func cloneRing(r *kmspb.KeyRing) *kmspb.KeyRing {
	out, _ := proto.Clone(r).(*kmspb.KeyRing)

	return out
}

// CreateKeyRing creates a key ring under a project location.
func (b *InMemoryBackend) CreateKeyRing(req *kmspb.CreateKeyRingRequest) (*kmspb.KeyRing, error) {
	if err := parseResource(req.GetParent(), depthLocation); err != nil {
		return nil, err
	}

	if err := validID("KeyRing", req.GetKeyRingId()); err != nil {
		return nil, err
	}

	name := req.GetParent() + "/keyRings/" + req.GetKeyRingId()

	b.mu.Lock("CreateKeyRing")
	defer b.mu.Unlock()

	b.sweepLocked()

	if _, ok := b.rings[name]; ok {
		return nil, errExists("KeyRing", name)
	}

	r := &kmspb.KeyRing{Name: name, CreateTime: tsOf(b.now())}
	b.rings[name] = r

	return cloneRing(r), nil
}

// GetKeyRing returns a key ring.
func (b *InMemoryBackend) GetKeyRing(name string) (*kmspb.KeyRing, error) {
	if err := parseResource(name, depthKeyRing); err != nil {
		return nil, err
	}

	b.mu.RLock("GetKeyRing")
	defer b.mu.RUnlock()

	r, ok := b.rings[name]
	if !ok {
		return nil, errNotFound("KeyRing", name)
	}

	return cloneRing(r), nil
}

// ListKeyRings lists a location's key rings sorted by name.
func (b *InMemoryBackend) ListKeyRings(req *kmspb.ListKeyRingsRequest) ([]*kmspb.KeyRing, string, int, error) {
	if err := parseResource(req.GetParent(), depthLocation); err != nil {
		return nil, "", 0, err
	}

	if err := rejectListOptions(req.GetFilter(), req.GetOrderBy()); err != nil {
		return nil, "", 0, err
	}

	size, err := pageSizeOf(req.GetPageSize())
	if err != nil {
		return nil, "", 0, err
	}

	b.mu.RLock("ListKeyRings")
	defer b.mu.RUnlock()

	names := childrenOf(b.rings, req.GetParent(), "keyRings")
	all := make([]*kmspb.KeyRing, 0, len(names))

	for _, n := range names {
		all = append(all, cloneRing(b.rings[n]))
	}

	pg := page.New(all, req.GetPageToken(), size, defaultPageSize)

	return pg.Data, pg.Next, len(all), nil
}
