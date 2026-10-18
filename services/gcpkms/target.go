package gcpkms

import (
	"cloud.google.com/go/kms/apiv1/kmspb"
)

// target is a snapshot of one ENABLED key version taken under the lock so crypto runs outside it.
type target struct {
	name      string
	material  []byte
	spec      algSpec
	alg       alg
	level     kmspb.ProtectionLevel
	id        int64
	isPrimary bool
}

func purposeError(op string, have, want purpose) error {
	return errInvalid("CryptoKey purpose is %s, but %s requires %s.", have, op, want)
}

func enabledTarget(k *keyRec, v *versionRec, op string, want purpose) (target, error) {
	if k.pb.GetPurpose() != want {
		return target{}, purposeError(op, k.pb.GetPurpose(), want)
	}

	if v.pb.GetState() != kmspb.CryptoKeyVersion_ENABLED {
		return target{}, errPrecondition("CryptoKeyVersion %s is not enabled, current state is: %s.",
			v.pb.GetName(), v.pb.GetState())
	}

	s, _ := specOf(v.pb.GetAlgorithm())

	return target{
		name: v.pb.GetName(), material: v.material, spec: s, alg: v.pb.GetAlgorithm(),
		level: v.pb.GetProtectionLevel(), id: v.id, isPrimary: k.primary == v.id,
	}, nil
}

// versionTarget resolves a cryptoKeyVersions name for an operation of the given purpose.
func (b *InMemoryBackend) versionTarget(name, op string, want purpose) (target, error) {
	b.mu.Lock(op)
	defer b.mu.Unlock()

	b.sweepLocked()

	k, v, err := b.findVersionLocked(name)
	if err != nil {
		return target{}, err
	}

	return enabledTarget(k, v, op, want)
}

// keyOrVersionTarget resolves a key (its primary version) or an explicit version name.
func (b *InMemoryBackend) keyOrVersionTarget(name, op string, want purpose) (target, error) {
	if err := parseResource(name, depthKey); err != nil {
		return b.versionTarget(name, op, want)
	}

	b.mu.Lock(op)
	defer b.mu.Unlock()

	b.sweepLocked()

	k, ok := b.keys[name]
	if !ok {
		return target{}, errNotFound("CryptoKey", name)
	}

	v := k.version(k.primary)
	if v == nil {
		if k.pb.GetPurpose() != want {
			return target{}, purposeError(op, k.pb.GetPurpose(), want)
		}

		return target{}, errPrecondition("CryptoKey %s does not have a primary version.", name)
	}

	return enabledTarget(k, v, op, want)
}

// decryptTarget finds the version a ciphertext was produced with.
func (b *InMemoryBackend) decryptTarget(keyName string, ct []byte) (target, error) {
	if err := parseResource(keyName, depthKey); err != nil {
		return target{}, err
	}

	b.mu.Lock("Decrypt")
	defer b.mu.Unlock()

	b.sweepLocked()

	k, ok := b.keys[keyName]
	if !ok {
		return target{}, errNotFound("CryptoKey", keyName)
	}

	if k.pb.GetPurpose() != kmspb.CryptoKey_ENCRYPT_DECRYPT {
		return target{}, purposeError("Decrypt", k.pb.GetPurpose(), kmspb.CryptoKey_ENCRYPT_DECRYPT)
	}

	id, ok := ciphertextVersion(ct)
	if !ok {
		return target{}, errInvalid("Decryption failed: the ciphertext is invalid.")
	}

	v := k.version(id)
	if v == nil {
		return target{}, errInvalid("Decryption failed: the ciphertext is invalid.")
	}

	return enabledTarget(k, v, "Decrypt", kmspb.CryptoKey_ENCRYPT_DECRYPT)
}
