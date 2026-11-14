package gcpsecretmanager

import (
	"errors"
	"strings"

	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"

	"github.com/blackbirdworks/gopherstack/pkgs/cmek"
)

const locationGlobal = "global"

// SetKeyChecker validates customer-managed encryption keys against Cloud KMS; nil disables validation.
func (b *InMemoryBackend) SetKeyChecker(c cmek.Checker) {
	b.mu.Lock("SetKeyChecker")
	defer b.mu.Unlock()

	b.keys = c
}

type keyRef struct{ name, location string }

// secretKeys lists the CMEK keys of a secret with the location each must be in.
func secretKeys(pb *secretmanagerpb.Secret) []keyRef {
	var refs []keyRef

	add := func(c *secretmanagerpb.CustomerManagedEncryption, loc string) {
		if c.GetKmsKeyName() != "" {
			refs = append(refs, keyRef{name: c.GetKmsKeyName(), location: loc})
		}
	}

	if isRegional(pb.GetName()) {
		add(pb.GetCustomerManagedEncryption(), strings.Split(pb.GetName(), "/")[3])

		return refs
	}

	add(pb.GetReplication().GetAutomatic().GetCustomerManagedEncryption(), locationGlobal)

	for _, rp := range pb.GetReplication().GetUserManaged().GetReplicas() {
		add(rp.GetCustomerManagedEncryption(), rp.GetLocation())
	}

	return refs
}

// keyProblem returns nil when the key is usable, else the gRPC error for it.
func (b *InMemoryBackend) keyProblem(r keyRef) error {
	err := cmek.Check(b.keys, r.name, r.location)

	switch {
	case err == nil:
		return nil
	case errors.Is(err, cmek.ErrNotFound):
		return errInvalid("Cloud KMS key %s was not found.", r.name)
	case errors.Is(err, cmek.ErrLocation):
		return errInvalid("Cloud KMS key %s must be in location %s, the location of the secret replica it encrypts.",
			r.name, r.location)
	case errors.Is(err, cmek.ErrPurpose):
		return errInvalid("Cloud KMS key %s must be a symmetric ENCRYPT_DECRYPT key.", r.name)
	case errors.Is(err, cmek.ErrDisabled):
		return errPrecondition("Cloud KMS key %s has no enabled primary version.", r.name)
	}

	return err
}

// checkKeysForWrite requires every CMEK key of the secret to be usable.
func (b *InMemoryBackend) checkKeysForWrite(pb *secretmanagerpb.Secret) error {
	if b.keys == nil {
		return nil
	}

	for _, r := range secretKeys(pb) {
		if err := b.keyProblem(r); err != nil {
			return err
		}
	}

	return nil
}

// checkKeysForRead fails only when every replica is CMEK-encrypted and none of its keys is usable.
func (b *InMemoryBackend) checkKeysForRead(pb *secretmanagerpb.Secret) error {
	if b.keys == nil {
		return nil
	}

	refs := secretKeys(pb)
	if len(refs) == 0 || len(refs) < replicaCount(pb) {
		return nil
	}

	var first error

	for _, r := range refs {
		err := b.keyProblem(r)
		if err == nil {
			return nil
		}

		if first == nil {
			first = err
		}
	}

	return first
}

func replicaCount(pb *secretmanagerpb.Secret) int {
	if isRegional(pb.GetName()) || pb.GetReplication().GetAutomatic() != nil {
		return 1
	}

	return len(pb.GetReplication().GetUserManaged().GetReplicas())
}
