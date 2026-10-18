package gcpkms

import (
	"slices"

	"cloud.google.com/go/kms/apiv1/kmspb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	pathLabels       = "labels"
	pathRotation     = "rotation_period"
	pathNextRotation = "next_rotation_time"
	pathTemplate     = "version_template"
	pathTemplateAlg  = "version_template.algorithm"
	pathTemplateLvl  = "version_template.protection_level"
	pathState        = "state"
)

// UpdateCryptoKey updates labels, rotation settings and the version template per update_mask.
func (b *InMemoryBackend) UpdateCryptoKey(req *kmspb.UpdateCryptoKeyRequest) (*kmspb.CryptoKey, error) {
	in := req.GetCryptoKey()
	if in == nil {
		return nil, errInvalid("crypto_key must be specified.")
	}

	if err := parseResource(in.GetName(), depthKey); err != nil {
		return nil, err
	}

	paths := req.GetUpdateMask().GetPaths()
	if len(paths) == 0 {
		return nil, errInvalid("update_mask is required.")
	}

	b.mu.Lock("UpdateCryptoKey")
	defer b.mu.Unlock()

	b.sweepLocked()

	k, ok := b.keys[in.GetName()]
	if !ok {
		return nil, errNotFound("CryptoKey", in.GetName())
	}

	work, _ := proto.Clone(k.pb).(*kmspb.CryptoKey)

	if err := b.applyKeyPaths(work, in, paths); err != nil {
		return nil, err
	}

	k.pb = work

	return k.view(), nil
}

func (b *InMemoryBackend) applyKeyPaths(work, in *kmspb.CryptoKey, paths []string) error {
	rotation := false

	for _, p := range paths {
		switch p {
		case pathLabels:
			if err := validateLabels(in.GetLabels()); err != nil {
				return err
			}

			work.Labels = in.GetLabels()
		case pathRotation:
			work.RotationSchedule = in.GetRotationSchedule()
			rotation = true
		case pathNextRotation:
			work.NextRotationTime = in.GetNextRotationTime()
			rotation = true
		case pathTemplate, pathTemplateAlg, pathTemplateLvl:
			if err := applyTemplate(work, in, p); err != nil {
				return err
			}
		default:
			return errInvalid("Field %q cannot be updated.", p)
		}
	}

	if !rotation {
		return nil
	}

	if period, ok := rotationPeriodOf(work); ok && !slices.Contains(paths, pathNextRotation) {
		work.NextRotationTime = tsOf(b.now().Add(period))
	}

	return b.validateRotation(work)
}

func applyTemplate(work, in *kmspb.CryptoKey, path string) error {
	next := &kmspb.CryptoKeyVersionTemplate{
		Algorithm:       work.GetVersionTemplate().GetAlgorithm(),
		ProtectionLevel: work.GetVersionTemplate().GetProtectionLevel(),
	}

	if path != pathTemplateLvl {
		next.Algorithm = in.GetVersionTemplate().GetAlgorithm()
	}

	if path != pathTemplateAlg {
		next.ProtectionLevel = in.GetVersionTemplate().GetProtectionLevel()
	}

	probe := &kmspb.CryptoKey{Purpose: work.GetPurpose(), VersionTemplate: next}

	tmpl, err := resolveTemplate(probe)
	if err != nil {
		return err
	}

	work.VersionTemplate = tmpl

	return nil
}

// UpdateCryptoKeyPrimaryVersion promotes an ENABLED version of an ENCRYPT_DECRYPT key.
func (b *InMemoryBackend) UpdateCryptoKeyPrimaryVersion(
	req *kmspb.UpdateCryptoKeyPrimaryVersionRequest,
) (*kmspb.CryptoKey, error) {
	if err := parseResource(req.GetName(), depthKey); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateCryptoKeyPrimaryVersion")
	defer b.mu.Unlock()

	b.sweepLocked()

	k, ok := b.keys[req.GetName()]
	if !ok {
		return nil, errNotFound("CryptoKey", req.GetName())
	}

	if k.pb.GetPurpose() != kmspb.CryptoKey_ENCRYPT_DECRYPT {
		return nil, errPrecondition("Only keys with purpose ENCRYPT_DECRYPT have a primary version.")
	}

	id, err := parseVersionID(req.GetCryptoKeyVersionId())
	if err != nil {
		return nil, err
	}

	v := k.version(id)
	if v == nil {
		return nil, errNotFound("CryptoKeyVersion", versionName(req.GetName(), id))
	}

	if v.pb.GetState() != kmspb.CryptoKeyVersion_ENABLED {
		return nil, errPrecondition("CryptoKeyVersion %s is not enabled, current state is: %s.",
			v.pb.GetName(), v.pb.GetState())
	}

	k.primary = id

	return k.view(), nil
}

func destroyTTLOf(k *keyRec) *durationpb.Duration { return k.pb.GetDestroyScheduledDuration() }
