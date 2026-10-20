package gcpkms

import (
	"time"

	"cloud.google.com/go/kms/apiv1/kmspb"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func validateLabels(labels map[string]string) error {
	if len(labels) > maxLabels {
		return errInvalid("A CryptoKey can have at most %d labels.", maxLabels)
	}

	for k, v := range labels {
		if k == "" || len(k) > maxLabelLen || len(v) > maxLabelLen {
			return errInvalid("Label keys must be 1-%d characters and values at most %d.", maxLabelLen, maxLabelLen)
		}
	}

	return nil
}

func rotationPeriodOf(k *kmspb.CryptoKey) (time.Duration, bool) {
	if p := k.GetRotationPeriod(); p != nil {
		return p.AsDuration(), true
	}

	return 0, false
}

func (b *InMemoryBackend) validateRotation(k *kmspb.CryptoKey) error {
	period, hasPeriod := rotationPeriodOf(k)
	if hasPeriod && (period < minRotationEvery || period > maxRotationEvery) {
		return errInvalid("rotation_period must be between 24 hours and 876000 hours.")
	}

	if hasPeriod && k.GetNextRotationTime() == nil {
		return errInvalid("next_rotation_time must be set when rotation_period is set.")
	}

	if nt := k.GetNextRotationTime(); nt != nil && !nt.AsTime().After(b.now()) {
		return errInvalid("next_rotation_time must be in the future.")
	}

	return nil
}

func validateDestroyTTL(d *durationpb.Duration) error {
	if d == nil {
		return nil
	}

	if t := d.AsDuration(); t < minDestroyTTL || t > maxDestroyTTL {
		return errInvalid("destroy_scheduled_duration must be between 24 hours and 120 days.")
	}

	return nil
}

func resolveTemplate(in *kmspb.CryptoKey) (*kmspb.CryptoKeyVersionTemplate, error) {
	t := in.GetVersionTemplate()
	a := t.GetAlgorithm()

	if a == kmspb.CryptoKeyVersion_CRYPTO_KEY_VERSION_ALGORITHM_UNSPECIFIED {
		a = defaultAlgorithm(in.GetPurpose())
	}

	if a == kmspb.CryptoKeyVersion_CRYPTO_KEY_VERSION_ALGORITHM_UNSPECIFIED {
		return nil, errInvalid("version_template.algorithm is required for purpose %s.", in.GetPurpose())
	}

	if _, err := specFor(in.GetPurpose(), a); err != nil {
		return nil, err
	}

	level := t.GetProtectionLevel()

	switch level {
	case kmspb.ProtectionLevel_PROTECTION_LEVEL_UNSPECIFIED:
		level = kmspb.ProtectionLevel_SOFTWARE
	case kmspb.ProtectionLevel_SOFTWARE, kmspb.ProtectionLevel_HSM:
	default:
		return nil, errInvalid("Protection level %s is not supported by this emulator.", level)
	}

	return &kmspb.CryptoKeyVersionTemplate{ProtectionLevel: level, Algorithm: a}, nil
}

func (b *InMemoryBackend) validateNewKey(req *kmspb.CreateCryptoKeyRequest) (*kmspb.CryptoKeyVersionTemplate, error) {
	in := req.GetCryptoKey()
	if in == nil {
		return nil, errInvalid("crypto_key must be specified.")
	}

	if !supportedPurpose(in.GetPurpose()) {
		return nil, errInvalid("CryptoKey purpose %s is not supported by this emulator.", in.GetPurpose())
	}

	if in.GetCryptoKeyBackend() != "" || in.GetKeyAccessJustificationsPolicy() != nil {
		return nil, errUnsupported("crypto_key_backend and key_access_justifications_policy are not supported.")
	}

	if err := validateLabels(in.GetLabels()); err != nil {
		return nil, err
	}

	if err := validateDestroyTTL(in.GetDestroyScheduledDuration()); err != nil {
		return nil, err
	}

	if err := b.validateRotation(in); err != nil {
		return nil, err
	}

	return resolveTemplate(in)
}

// CreateCryptoKey creates a key and, unless skipped or import-only, its first version.
func (b *InMemoryBackend) CreateCryptoKey(req *kmspb.CreateCryptoKeyRequest) (*kmspb.CryptoKey, error) {
	if err := parseResource(req.GetParent(), depthKeyRing); err != nil {
		return nil, err
	}

	if err := validID("CryptoKey", req.GetCryptoKeyId()); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateCryptoKey")
	defer b.mu.Unlock()

	b.sweepLocked()

	tmpl, err := b.validateNewKey(req)
	if err != nil {
		return nil, err
	}

	if _, ok := b.rings[req.GetParent()]; !ok {
		return nil, errNotFound("KeyRing", req.GetParent())
	}

	name := req.GetParent() + "/cryptoKeys/" + req.GetCryptoKeyId()
	if _, ok := b.keys[name]; ok {
		return nil, errExists("CryptoKey", name)
	}

	if b.retiredNameLocked(name) {
		return nil, errExists("CryptoKey", name+" (retired; deleted key names cannot be reused)")
	}

	return b.insertKeyLocked(req, name, tmpl)
}

func (b *InMemoryBackend) insertKeyLocked(
	req *kmspb.CreateCryptoKeyRequest, name string, tmpl *kmspb.CryptoKeyVersionTemplate,
) (*kmspb.CryptoKey, error) {
	in := req.GetCryptoKey()
	now := b.now()

	ttl := in.GetDestroyScheduledDuration()
	if ttl == nil {
		ttl = durationpb.New(defaultDestroy)
	}

	pb := &kmspb.CryptoKey{
		Name:                     name,
		Purpose:                  in.GetPurpose(),
		CreateTime:               tsOf(now),
		NextRotationTime:         in.GetNextRotationTime(),
		RotationSchedule:         in.GetRotationSchedule(),
		VersionTemplate:          tmpl,
		Labels:                   in.GetLabels(),
		ImportOnly:               in.GetImportOnly(),
		DestroyScheduledDuration: ttl,
	}
	k := &keyRec{pb: pb}

	if !req.GetSkipInitialVersionCreation() && !in.GetImportOnly() {
		v, err := b.newVersionLocked(k, now)
		if err != nil {
			return nil, err
		}

		if pb.GetPurpose() == kmspb.CryptoKey_ENCRYPT_DECRYPT {
			k.primary = v.id
		}
	}

	b.keys[name] = k

	return k.view(), nil
}

// GetCryptoKey returns a key with its primary version.
func (b *InMemoryBackend) GetCryptoKey(name string) (*kmspb.CryptoKey, error) {
	if err := parseResource(name, depthKey); err != nil {
		return nil, err
	}

	b.mu.Lock("GetCryptoKey")
	defer b.mu.Unlock()

	b.sweepLocked()

	k, ok := b.keys[name]
	if !ok {
		return nil, errNotFound("CryptoKey", name)
	}

	return k.view(), nil
}

// ListCryptoKeys lists a key ring's keys sorted by name.
func (b *InMemoryBackend) ListCryptoKeys(req *kmspb.ListCryptoKeysRequest) ([]*kmspb.CryptoKey, string, int, error) {
	if err := parseResource(req.GetParent(), depthKeyRing); err != nil {
		return nil, "", 0, err
	}

	size, err := pageSizeOf(req.GetPageSize())
	if err != nil {
		return nil, "", 0, err
	}

	b.mu.Lock("ListCryptoKeys")
	defer b.mu.Unlock()

	b.sweepLocked()

	if _, ok := b.rings[req.GetParent()]; !ok {
		return nil, "", 0, errNotFound("KeyRing", req.GetParent())
	}

	names := childrenOf(b.keys, req.GetParent(), "cryptoKeys")
	all := make([]*kmspb.CryptoKey, 0, len(names))

	for _, n := range names {
		all = append(all, b.keys[n].view())
	}

	all, err = applyListOptions(all, req.GetFilter(), req.GetOrderBy(), cryptoKeySpec())
	if err != nil {
		return nil, "", 0, err
	}

	pg := page.New(all, req.GetPageToken(), size, defaultPageSize)

	return pg.Data, pg.Next, len(all), nil
}
