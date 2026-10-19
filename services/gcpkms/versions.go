package gcpkms

import (
	"strconv"

	"cloud.google.com/go/kms/apiv1/kmspb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func parseVersionID(s string) (int64, error) {
	id, err := strconv.ParseInt(s, 10, 64)
	if err != nil || id < 1 {
		return 0, errInvalid("Invalid crypto_key_version_id %q.", s)
	}

	return id, nil
}

// findVersionLocked resolves a version name to its key and version records.
func (b *InMemoryBackend) findVersionLocked(name string) (*keyRec, *versionRec, error) {
	keyName, id, err := splitVersion(name)
	if err != nil {
		return nil, nil, err
	}

	k, ok := b.keys[keyName]
	if !ok {
		return nil, nil, errNotFound("CryptoKey", keyName)
	}

	v := k.version(id)
	if v == nil {
		return nil, nil, errNotFound("CryptoKeyVersion", name)
	}

	return k, v, nil
}

// CreateCryptoKeyVersion generates a new ENABLED version from the key's template.
func (b *InMemoryBackend) CreateCryptoKeyVersion(
	req *kmspb.CreateCryptoKeyVersionRequest,
) (*kmspb.CryptoKeyVersion, error) {
	if err := parseResource(req.GetParent(), depthKey); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateCryptoKeyVersion")
	defer b.mu.Unlock()

	b.sweepLocked()

	k, ok := b.keys[req.GetParent()]
	if !ok {
		return nil, errNotFound("CryptoKey", req.GetParent())
	}

	if k.pb.GetImportOnly() {
		return nil, errPrecondition("CryptoKey %s is import-only; versions must be imported.", req.GetParent())
	}

	v, err := b.newVersionLocked(k, b.now())
	if err != nil {
		return nil, err
	}

	if k.primary == 0 && k.pb.GetPurpose() == kmspb.CryptoKey_ENCRYPT_DECRYPT {
		k.primary = v.id
	}

	return cloneVersion(v.pb), nil
}

// GetCryptoKeyVersion returns a version's metadata.
func (b *InMemoryBackend) GetCryptoKeyVersion(name string) (*kmspb.CryptoKeyVersion, error) {
	b.mu.Lock("GetCryptoKeyVersion")
	defer b.mu.Unlock()

	b.sweepLocked()

	_, v, err := b.findVersionLocked(name)
	if err != nil {
		return nil, err
	}

	return cloneVersion(v.pb), nil
}

// ListCryptoKeyVersions lists a key's versions in ascending version order.
func (b *InMemoryBackend) ListCryptoKeyVersions(
	req *kmspb.ListCryptoKeyVersionsRequest,
) ([]*kmspb.CryptoKeyVersion, string, int, error) {
	if err := parseResource(req.GetParent(), depthKey); err != nil {
		return nil, "", 0, err
	}

	if err := rejectListOptions(req.GetFilter(), req.GetOrderBy()); err != nil {
		return nil, "", 0, err
	}

	size, err := pageSizeOf(req.GetPageSize())
	if err != nil {
		return nil, "", 0, err
	}

	b.mu.Lock("ListCryptoKeyVersions")
	defer b.mu.Unlock()

	b.sweepLocked()

	k, ok := b.keys[req.GetParent()]
	if !ok {
		return nil, "", 0, errNotFound("CryptoKey", req.GetParent())
	}

	all := make([]*kmspb.CryptoKeyVersion, 0, len(k.versions))
	for _, v := range k.versions {
		all = append(all, cloneVersion(v.pb))
	}

	pg := page.New(all, req.GetPageToken(), size, defaultPageSize)

	return pg.Data, pg.Next, len(all), nil
}

// UpdateCryptoKeyVersion toggles a version between ENABLED and DISABLED (update_mask "state").
func (b *InMemoryBackend) UpdateCryptoKeyVersion(
	req *kmspb.UpdateCryptoKeyVersionRequest,
) (*kmspb.CryptoKeyVersion, error) {
	in := req.GetCryptoKeyVersion()
	if in == nil {
		return nil, errInvalid("crypto_key_version must be specified.")
	}

	paths := req.GetUpdateMask().GetPaths()
	if len(paths) != 1 || paths[0] != pathState {
		return nil, errInvalid("update_mask must be exactly \"state\".")
	}

	if s := in.GetState(); s != kmspb.CryptoKeyVersion_ENABLED && s != kmspb.CryptoKeyVersion_DISABLED {
		return nil, errInvalid("state may only be set to ENABLED or DISABLED.")
	}

	b.mu.Lock("UpdateCryptoKeyVersion")
	defer b.mu.Unlock()

	b.sweepLocked()

	_, v, err := b.findVersionLocked(in.GetName())
	if err != nil {
		return nil, err
	}

	if cur := v.pb.GetState(); cur != kmspb.CryptoKeyVersion_ENABLED && cur != kmspb.CryptoKeyVersion_DISABLED {
		return nil, errPrecondition("CryptoKeyVersion %s is in state %s and cannot be enabled or disabled.",
			in.GetName(), cur)
	}

	v.pb.State = in.GetState()

	return cloneVersion(v.pb), nil
}

// DestroyCryptoKeyVersion schedules a version's destruction after the key's destroy_scheduled_duration.
func (b *InMemoryBackend) DestroyCryptoKeyVersion(
	req *kmspb.DestroyCryptoKeyVersionRequest,
) (*kmspb.CryptoKeyVersion, error) {
	b.mu.Lock("DestroyCryptoKeyVersion")
	defer b.mu.Unlock()

	b.sweepLocked()

	k, v, err := b.findVersionLocked(req.GetName())
	if err != nil {
		return nil, err
	}

	switch v.pb.GetState() {
	case kmspb.CryptoKeyVersion_ENABLED, kmspb.CryptoKeyVersion_DISABLED:
	default:
		return nil, errPrecondition("CryptoKeyVersion %s is in state %s and cannot be destroyed.",
			req.GetName(), v.pb.GetState())
	}

	if k.primary == v.id {
		return nil, errPrecondition("CryptoKeyVersion %s is the primary version and cannot be destroyed.",
			req.GetName())
	}

	now := b.now()
	v.pb.State = kmspb.CryptoKeyVersion_DESTROY_SCHEDULED
	v.pb.DestroyTime = tsOf(now.Add(destroyTTLOf(k).AsDuration()))

	return cloneVersion(v.pb), nil
}

// RestoreCryptoKeyVersion cancels a scheduled destruction, leaving the version DISABLED.
func (b *InMemoryBackend) RestoreCryptoKeyVersion(
	req *kmspb.RestoreCryptoKeyVersionRequest,
) (*kmspb.CryptoKeyVersion, error) {
	b.mu.Lock("RestoreCryptoKeyVersion")
	defer b.mu.Unlock()

	b.sweepLocked()

	_, v, err := b.findVersionLocked(req.GetName())
	if err != nil {
		return nil, err
	}

	if v.pb.GetState() != kmspb.CryptoKeyVersion_DESTROY_SCHEDULED {
		return nil, errPrecondition("CryptoKeyVersion %s is in state %s and cannot be restored.",
			req.GetName(), v.pb.GetState())
	}

	v.pb.State = kmspb.CryptoKeyVersion_DISABLED
	v.pb.DestroyTime = nil

	return cloneVersion(v.pb), nil
}
