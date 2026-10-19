package gcpkms

import (
	"strings"

	"cloud.google.com/go/kms/apiv1/kmspb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	retiredIDLen     = 16
	cryptoKeyResType = "cloudkms.googleapis.com/CryptoKey"
)

func cloneRetired(r *kmspb.RetiredResource) *kmspb.RetiredResource {
	out, _ := proto.Clone(r).(*kmspb.RetiredResource)

	return out
}

func (b *InMemoryBackend) doneOperation(name string, meta, resp proto.Message) (*longrunningpb.Operation, error) {
	op, err := gcplro.Done(name, meta, resp)
	if err != nil {
		return nil, errInvalid("%v", err)
	}

	return b.ops.Put(op), nil
}

func opName(resourceName string) string {
	loc := strings.Join(strings.Split(resourceName, "/")[:depthLocation*2], "/")

	return loc + "/operations/" + awsid.NewN("", retiredIDLen)
}

// DeleteCryptoKey permanently deletes a key whose versions have all been deleted and records a retired resource.
func (b *InMemoryBackend) DeleteCryptoKey(req *kmspb.DeleteCryptoKeyRequest) (*longrunningpb.Operation, error) {
	if err := parseResource(req.GetName(), depthKey); err != nil {
		return nil, err
	}

	b.mu.Lock("DeleteCryptoKey")
	defer b.mu.Unlock()

	b.sweepLocked()

	k, ok := b.keys[req.GetName()]
	if !ok {
		return nil, errNotFound("CryptoKey", req.GetName())
	}

	if err := checkKeyDeletable(k); err != nil {
		return nil, err
	}

	loc := strings.Join(strings.Split(req.GetName(), "/")[:depthLocation*2], "/")
	rname := loc + "/retiredResources/" + awsid.NewN("", retiredIDLen)

	b.retired[rname] = &kmspb.RetiredResource{
		Name: rname, OriginalResource: req.GetName(), ResourceType: cryptoKeyResType, DeleteTime: tsOf(b.now()),
	}

	delete(b.keys, req.GetName())
	delete(b.policies, req.GetName())

	return b.doneOperation(
		opName(req.GetName()),
		&kmspb.DeleteCryptoKeyMetadata{RetiredResource: rname},
		&emptypb.Empty{},
	)
}

// checkKeyDeletable requires every non-primary version to be DESTROYED or deleted; the primary goes with the key.
func checkKeyDeletable(k *keyRec) error {
	for _, v := range k.versions {
		if v.deleted || v.id == k.primary || v.pb.GetState() == kmspb.CryptoKeyVersion_DESTROYED {
			continue
		}

		return errPrecondition("CryptoKeyVersion %s is in state %s; destroy it before deleting the CryptoKey.",
			v.pb.GetName(), v.pb.GetState())
	}

	return nil
}

// DeleteCryptoKeyVersion permanently deletes a DESTROYED version; its id is never reused.
func (b *InMemoryBackend) DeleteCryptoKeyVersion(
	req *kmspb.DeleteCryptoKeyVersionRequest,
) (*longrunningpb.Operation, error) {
	b.mu.Lock("DeleteCryptoKeyVersion")
	defer b.mu.Unlock()

	b.sweepLocked()

	k, v, err := b.findVersionLocked(req.GetName())
	if err != nil {
		return nil, err
	}

	if v.pb.GetState() != kmspb.CryptoKeyVersion_DESTROYED {
		return nil, errPrecondition("CryptoKeyVersion %s is in state %s; only DESTROYED versions can be deleted.",
			req.GetName(), v.pb.GetState())
	}

	if k.primary == v.id {
		return nil, errPrecondition("CryptoKeyVersion %s is the primary version and cannot be deleted.", req.GetName())
	}

	v.deleted = true
	v.material = nil

	return b.doneOperation(opName(req.GetName()), &kmspb.DeleteCryptoKeyVersionMetadata{}, &emptypb.Empty{})
}

// retiredNameLocked reports whether a deleted key already used name.
func (b *InMemoryBackend) retiredNameLocked(name string) bool {
	for _, r := range b.retired {
		if r.GetOriginalResource() == name {
			return true
		}
	}

	return false
}

// GetRetiredResource returns a retired-resource record.
func (b *InMemoryBackend) GetRetiredResource(name string) (*kmspb.RetiredResource, error) {
	if err := parseRetiredName(name); err != nil {
		return nil, err
	}

	b.mu.RLock("GetRetiredResource")
	defer b.mu.RUnlock()

	r, ok := b.retired[name]
	if !ok {
		return nil, errNotFound("RetiredResource", name)
	}

	return cloneRetired(r), nil
}

func parseRetiredName(name string) error {
	parts := strings.Split(name, "/")
	if len(parts) != depthLocation*2+2 || parts[depthLocation*2] != "retiredResources" ||
		parts[depthLocation*2+1] == "" {
		return errInvalid("Invalid retired resource name %q.", name)
	}

	return parseResource(strings.Join(parts[:depthLocation*2], "/"), depthLocation)
}

// ListRetiredResources lists a location's retired resources sorted by name.
func (b *InMemoryBackend) ListRetiredResources(
	req *kmspb.ListRetiredResourcesRequest,
) ([]*kmspb.RetiredResource, string, int, error) {
	if err := parseResource(req.GetParent(), depthLocation); err != nil {
		return nil, "", 0, err
	}

	size, err := pageSizeOf(req.GetPageSize())
	if err != nil {
		return nil, "", 0, err
	}

	b.mu.RLock("ListRetiredResources")
	defer b.mu.RUnlock()

	names := childrenOf(b.retired, req.GetParent(), "retiredResources")
	all := make([]*kmspb.RetiredResource, 0, len(names))

	for _, n := range names {
		all = append(all, cloneRetired(b.retired[n]))
	}

	pg := page.New(all, req.GetPageToken(), size, defaultPageSize)

	return pg.Data, pg.Next, len(all), nil
}
