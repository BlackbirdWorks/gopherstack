package gcpspanner

import (
	"errors"
	"strings"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"

	"github.com/blackbirdworks/gopherstack/pkgs/cmek"
)

const regionalConfigPrefix = "regional-"

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

// regionalLocation returns the region of a single-region instance config, "" for multi-region configs.
func regionalLocation(instanceConfig string) string {
	_, id, ok := strings.Cut(instanceConfig, "/instanceConfigs/")
	if !ok || !strings.HasPrefix(id, regionalConfigPrefix) {
		return ""
	}

	return strings.TrimPrefix(id, regionalConfigPrefix)
}

// checkEncryption requires every key of a database's encryption_config to be usable and, on a single-region
// instance, in the instance's region. The caller holds the lock.
func (b *InMemoryBackend) checkEncryption(cfg *databasepb.EncryptionConfig, instanceConfig string) error {
	c := b.keys
	if c == nil {
		return errUnimplemented("Customer-managed encryption is not supported by this emulator.")
	}

	keys := encryptionKeys(cfg)
	if len(keys) == 0 {
		return errInvalid("encryption_config must name kms_key_name or kms_key_names.")
	}

	loc := regionalLocation(instanceConfig)

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

// databaseView clones a database with its live encryption_info; the caller holds the lock.
func (b *InMemoryBackend) databaseView(pb *databasepb.Database) *databasepb.Database {
	out := cloneDatabase(pb)
	if out.GetEncryptionConfig() != nil {
		out.EncryptionInfo = encryptionInfo(b.keys, out.GetEncryptionConfig())
	}

	return out
}
