package gcpspanner

import (
	"errors"
	"slices"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"

	"github.com/blackbirdworks/gopherstack/pkgs/cmek"
)

// SetKeyChecker validates database encryption keys against Cloud KMS; without one, encryption_config is
// UNIMPLEMENTED.
func (b *InMemoryBackend) SetKeyChecker(c cmek.Checker) {
	b.mu.Lock("SetKeyChecker")
	defer b.mu.Unlock()

	b.keys = c
}

func (b *InMemoryBackend) keyChecker() cmek.Checker {
	b.mu.RLock("keyChecker")
	defer b.mu.RUnlock()

	return b.keys
}

func encryptionKeys(cfg *databasepb.EncryptionConfig) []string {
	keys := append([]string(nil), cfg.GetKmsKeyNames()...)
	if cfg.GetKmsKeyName() != "" {
		keys = append(keys, cfg.GetKmsKeyName())
	}

	return keys
}

const emulatorLocation = "emulator-location"

// replicaLocations returns the distinct locations of a configuration's replicas.
func replicaLocations(c *instancepb.InstanceConfig) []string {
	var out []string

	for _, r := range c.GetReplicas() {
		if !slices.Contains(out, r.GetLocation()) {
			out = append(out, r.GetLocation())
		}
	}

	return out
}

// checkCoverage requires kms_key_names to hold one key in each replica location of a multi-region configuration
// (https://docs.cloud.google.com/spanner/docs/use-cmek lists one regional key per replica location).
func checkCoverage(cfg *databasepb.EncryptionConfig, locs []string) error {
	if len(cfg.GetKmsKeyNames()) == 0 || len(locs) < 2 {
		return nil
	}

	have := map[string]int{}
	for _, k := range cfg.GetKmsKeyNames() {
		have[cmek.Location(k)]++
	}

	for _, l := range locs {
		if have[l] != 1 {
			return errInvalid(
				"kms_key_names must hold exactly one key in each replica location; %q has %d.",
				l,
				have[l],
			)
		}
	}

	if len(have) != len(locs) {
		return errInvalid("kms_key_names holds a key outside the replica locations %v.", locs)
	}

	return nil
}

// checkEncryption requires every key of a database's encryption_config to be usable, in the instance's region
// on a single-region instance and, with kms_key_names, to cover every replica location of a multi-region one.
// The caller must not hold the lock: Cloud KMS is consulted.
func (b *InMemoryBackend) checkEncryption(cfg *databasepb.EncryptionConfig, instanceConfig string) error {
	c := b.keyChecker()
	if c == nil {
		return errUnimplemented("Customer-managed encryption is not supported by this emulator.")
	}

	keys := encryptionKeys(cfg)
	if len(keys) == 0 {
		return errInvalid("encryption_config must name kms_key_name or kms_key_names.")
	}

	ic, _ := b.lookupConfigName(instanceConfig)
	locs := replicaLocations(ic)

	var loc string
	if len(locs) == 1 && locs[0] != emulatorLocation {
		loc = locs[0]
	}

	if err := checkCoverage(cfg, locs); err != nil {
		return err
	}

	for _, key := range keys {
		err := cmek.Check(c, key, loc)

		switch {
		case err == nil:
		case errors.Is(err, cmek.ErrNotFound):
			return errInvalid("Cloud KMS key %s was not found.", key)
		case errors.Is(err, cmek.ErrLocation):
			return errInvalid("Cloud KMS key %s must be in the region of the instance (%s).", key, loc)
		case errors.Is(err, cmek.ErrPurpose):
			return errInvalid("Cloud KMS key %s must be a symmetric ENCRYPT_DECRYPT key.", key)
		case errors.Is(err, cmek.ErrDisabled):
			return errPrecondition("Cloud KMS key %s has no enabled primary version.", key)
		default:
			return err
		}
	}

	return nil
}

// encryptionInfo reports the customer-managed encryption of a database; a key that has become unusable shows
// as a FAILED_PRECONDITION encryption_status.
func encryptionInfo(c cmek.Checker, cfg *databasepb.EncryptionConfig) []*databasepb.EncryptionInfo {
	if cfg == nil {
		return nil
	}

	keys := encryptionKeys(cfg)
	out := make([]*databasepb.EncryptionInfo, 0, len(keys))

	for _, key := range keys {
		info := &databasepb.EncryptionInfo{
			EncryptionType: databasepb.EncryptionInfo_CUSTOMER_MANAGED_ENCRYPTION,
			KmsKeyVersion:  cmek.VersionName(c, key),
		}

		if err := cmek.Check(c, key, ""); err != nil {
			info.EncryptionStatus = &status.Status{Code: int32(codes.FailedPrecondition), Message: err.Error()}
		}

		out = append(out, info)
	}

	return out
}

// withEncryptionInfo adds the live encryption_info to a database copy. The caller must not hold the lock:
// Cloud KMS is consulted.
func (b *InMemoryBackend) withEncryptionInfo(out *databasepb.Database) *databasepb.Database {
	if out.GetEncryptionConfig() != nil {
		out.EncryptionInfo = encryptionInfo(b.keyChecker(), out.GetEncryptionConfig())
	}

	return out
}
