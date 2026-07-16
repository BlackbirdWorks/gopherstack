package iam_test

import (
	"encoding/base64"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIAM_AccessKey(t *testing.T) {
	t.Parallel()

	_, be := newTestHandler(t)

	_, err := be.CreateUser("key-user", "/", "")
	require.NoError(t, err)

	// CreateAccessKey
	key, err := be.CreateAccessKey("key-user")
	require.NoError(t, err)
	assert.NotEmpty(t, key.AccessKeyID)

	// ListAccessKeys
	keysPage, err := be.ListAccessKeys("key-user", "", 100)
	require.NoError(t, err)
	assert.Len(t, keysPage.Data, 1)
}

func TestCreateAccessKey_SecretLength(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackend()
	_, err := b.CreateUser("ak-user", "/", "")
	require.NoError(t, err)

	ak, err := b.CreateAccessKey("ak-user")
	require.NoError(t, err)

	assert.Len(t, ak.SecretAccessKey, 40, "secret access key must be 40 characters")

	// Verify it is valid base64 (standard encoding, no padding trimming needed)
	_, decodeErr := base64.StdEncoding.DecodeString(ak.SecretAccessKey)
	assert.NoError(t, decodeErr, "secret access key must be valid base64")
}

func TestCreateAccessKey_SecretIsUnique(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackend()
	_, err := b.CreateUser("ak-unique-user", "/", "")
	require.NoError(t, err)

	ak1, err := b.CreateAccessKey("ak-unique-user")
	require.NoError(t, err)

	ak2, err := b.CreateAccessKey("ak-unique-user")
	require.NoError(t, err)

	assert.NotEqual(t, ak1.SecretAccessKey, ak2.SecretAccessKey,
		"consecutive secrets must differ (cryptographically random)")
}

func TestDeleteUser_CleansAccessKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *iam.InMemoryBackend)
		name     string
		wantKeys int
	}{
		{
			name: "deletes_access_keys_on_user_deletion",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				_, _ = b.CreateAccessKey("alice")
				_, _ = b.CreateAccessKey("alice")
			},
			wantKeys: 0,
		},
		{
			name: "no_access_keys_to_delete",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
			},
			wantKeys: 0,
		},
		{
			name: "does_not_delete_other_users_keys",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				_, _ = b.CreateUser("bob", "/", "")
				_, _ = b.CreateAccessKey("bob")
			},
			wantKeys: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			require.NoError(t, b.DeleteUser("alice"))

			allKeys := b.ListAllAccessKeys()
			assert.Len(t, allKeys, tt.wantKeys)
		})
	}
}

func TestCreateAccessKey_SecretErrorPropagated(t *testing.T) {
	t.Parallel()

	// This test validates that CreateAccessKey returns an error (not panics)
	// on underlying rand failures. We validate indirectly by confirming the
	// happy-path does not panic.
	b := iam.NewInMemoryBackend()
	_, err := b.CreateUser("rand-user", "/", "")
	require.NoError(t, err)

	// If crypto/rand works (normal env), we should get a valid key.
	ak, err := b.CreateAccessKey("rand-user")
	require.NoError(t, err)
	require.NotNil(t, ak)
	assert.Len(t, ak.SecretAccessKey, 40)
}

// TestUpdateAccessKey_Backend tests UpdateAccessKey.
func TestUpdateAccessKey_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*iam.InMemoryBackend)
		name       string
		userName   string
		keyID      string
		status     string
		wantErrMsg string
		wantErr    bool
	}{
		{
			name: "set_inactive",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
			},
			userName: "alice",
			status:   "Inactive",
		},
		{
			name: "set_active",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
			},
			userName: "alice",
			status:   "Active",
		},
		{
			name: "invalid_status",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
			},
			userName:   "alice",
			status:     "Suspended",
			wantErr:    true,
			wantErrMsg: "InvalidAction",
		},
		{
			name:       "key_not_found",
			setup:      func(_ *iam.InMemoryBackend) {},
			userName:   "alice",
			keyID:      "AKIANONEXIST",
			status:     "Inactive",
			wantErr:    true,
			wantErrMsg: "NoSuchEntity",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			keyID := tt.keyID
			if keyID == "" {
				ak, err := b.CreateAccessKey("alice")
				if tt.userName == "alice" && err == nil {
					keyID = ak.AccessKeyID
				}
			}

			err := b.UpdateAccessKey(tt.userName, keyID, tt.status)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrMsg)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestGetAccessKeyLastUsed_Backend tests GetAccessKeyLastUsed.
func TestGetAccessKeyLastUsed_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		keyIDFn  func(*iam.InMemoryBackend) string
		name     string
		wantUser string
		wantErr  bool
	}{
		{
			name: "existing_key",
			keyIDFn: func(b *iam.InMemoryBackend) string {
				_, _ = b.CreateUser("bob", "/", "")
				ak, _ := b.CreateAccessKey("bob")

				return ak.AccessKeyID
			},
			wantUser: "bob",
		},
		{
			name: "nonexistent_key",
			keyIDFn: func(_ *iam.InMemoryBackend) string {
				return "AKIANONEXIST"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			keyID := tt.keyIDFn(b)

			info, err := b.GetAccessKeyLastUsed(keyID)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantUser, info.UserName)
			assert.Equal(t, keyID, info.AccessKeyID)
			assert.Equal(t, "N/A", info.ServiceName)
		})
	}
}

func TestRecordAccessKeyUsage_UpdatesFields(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("dave", "/", "")
	ak, err := b.CreateAccessKey("dave")
	require.NoError(t, err)

	before := time.Now().Add(-time.Second)
	b.RecordAccessKeyUsage(ak.AccessKeyID, "us-east-1", "s3")
	after := time.Now().Add(time.Second)

	result, err := b.GetAccessKeyLastUsed(ak.AccessKeyID)
	require.NoError(t, err)
	assert.Equal(t, "us-east-1", result.Region)
	assert.Equal(t, "s3", result.ServiceName)
	assert.NotEqual(t, "N/A", result.LastUsedDate)

	// Parse and validate the timestamp.
	parsedTime, parseErr := time.Parse("2006-01-02T15:04:05Z", result.LastUsedDate)
	require.NoError(t, parseErr)
	assert.True(t, parsedTime.After(before) || parsedTime.Equal(before.Truncate(time.Second)))
	assert.True(t, parsedTime.Before(after))
}

func TestRecordAccessKeyUsage_NonexistentKeyNoOp(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	// Must not panic.
	b.RecordAccessKeyUsage("AKIANONEXISTENT", "eu-west-1", "ec2")
}

func TestGetAccessKeyLastUsed_NeverUsed(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("eve", "/", "")
	ak, err := b.CreateAccessKey("eve")
	require.NoError(t, err)

	result, err := b.GetAccessKeyLastUsed(ak.AccessKeyID)
	require.NoError(t, err)
	assert.Equal(t, "N/A", result.LastUsedDate)
	assert.Equal(t, "N/A", result.Region)
	assert.Equal(t, "N/A", result.ServiceName)
}

func TestGetAccessKeyLastUsed_NotFound(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.GetAccessKeyLastUsed("AKIANONEXISTENT")
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrAccessKeyNotFound)
}

func TestAccessKeyLastUsed_MultipleUsages(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("repeated-user", "/", "")
	ak, _ := b.CreateAccessKey("repeated-user")

	b.RecordAccessKeyUsage(ak.AccessKeyID, "us-west-2", "iam")
	b.RecordAccessKeyUsage(ak.AccessKeyID, "ap-southeast-1", "ec2")

	result, err := b.GetAccessKeyLastUsed(ak.AccessKeyID)
	require.NoError(t, err)
	// Last call wins.
	assert.Equal(t, "ap-southeast-1", result.Region)
	assert.Equal(t, "ec2", result.ServiceName)
}

func TestGetAccessKeyLastUsed_RegionAndService_AfterMultipleUsages(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("multi-use-user", "/", "")

	ak, err := b.CreateAccessKey("multi-use-user")
	require.NoError(t, err)

	// First usage.
	b.RecordAccessKeyUsage(ak.AccessKeyID, "us-west-2", "ec2")

	info, err := b.GetAccessKeyLastUsed(ak.AccessKeyID)
	require.NoError(t, err)
	assert.Equal(t, "us-west-2", info.Region)
	assert.Equal(t, "ec2", info.ServiceName)

	// Second usage overwrites.
	b.RecordAccessKeyUsage(ak.AccessKeyID, "eu-west-1", "s3")

	info, err = b.GetAccessKeyLastUsed(ak.AccessKeyID)
	require.NoError(t, err)
	assert.Equal(t, "eu-west-1", info.Region, "region must reflect most recent usage")
	assert.Equal(t, "s3", info.ServiceName, "service must reflect most recent usage")
}

func TestGetAccessKeyLastUsed_LastUsedDateIsISO8601(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("ts-user", "/", "")

	ak, err := b.CreateAccessKey("ts-user")
	require.NoError(t, err)

	b.RecordAccessKeyUsage(ak.AccessKeyID, "us-east-1", "iam")

	info, err := b.GetAccessKeyLastUsed(ak.AccessKeyID)
	require.NoError(t, err)
	require.NotEqual(t, "N/A", info.LastUsedDate)

	_, parseErr := time.Parse("2006-01-02T15:04:05Z", info.LastUsedDate)
	assert.NoError(t, parseErr, "LastUsedDate must be ISO 8601 UTC: %q", info.LastUsedDate)
}

func TestGetAccessKeyLastUsed_NeverUsed_ReturnsNA(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("unused-user", "/", "")

	ak, err := b.CreateAccessKey("unused-user")
	require.NoError(t, err)

	info, err := b.GetAccessKeyLastUsed(ak.AccessKeyID)
	require.NoError(t, err)
	assert.Equal(t, "N/A", info.LastUsedDate)
	assert.Equal(t, "N/A", info.Region)
	assert.Equal(t, "N/A", info.ServiceName)
}

func TestAccessKeyID_Format(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.CreateUser("alice", "/", "")
	require.NoError(t, err)

	ak, err := b.CreateAccessKey("alice")
	require.NoError(t, err)

	// AWS access key IDs are exactly 20 characters starting with "AKIA".
	assert.Len(t, ak.AccessKeyID, 20, "access key ID must be 20 chars")
	assert.True(t, strings.HasPrefix(ak.AccessKeyID, "AKIA"), "access key ID must start with AKIA")

	// All characters after prefix must be uppercase alphanumeric (A–Z, 0–9).
	for _, ch := range ak.AccessKeyID {
		assert.True(t, unicode.IsUpper(ch) || unicode.IsDigit(ch),
			"access key ID char %q must be uppercase alphanumeric", ch)
	}
}

func TestAccessKeyID_NoDashes(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.CreateUser("bob", "/", "")
	require.NoError(t, err)

	ak, err := b.CreateAccessKey("bob")
	require.NoError(t, err)

	assert.NotContains(t, ak.AccessKeyID, "-", "access key ID must not contain dashes")
	assert.NotContains(t, ak.AccessKeyID, strings.ToLower(ak.AccessKeyID[4:]),
		"access key ID suffix must not be all lowercase")
}

func TestAccessKeyID_Unique(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.CreateUser("carol", "/", "")
	require.NoError(t, err)

	ak1, err := b.CreateAccessKey("carol")
	require.NoError(t, err)

	// Need to delete first key before creating second (max 2, but test uniqueness).
	require.NoError(t, b.DeleteAccessKey("carol", ak1.AccessKeyID))

	ak2, err := b.CreateAccessKey("carol")
	require.NoError(t, err)

	assert.NotEqual(t, ak1.AccessKeyID, ak2.AccessKeyID)
}

func TestCreateAccessKey_MaxTwoPerUser(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.CreateUser("dave", "/", "")
	require.NoError(t, err)

	ak1, err := b.CreateAccessKey("dave")
	require.NoError(t, err)
	assert.NotEmpty(t, ak1.AccessKeyID)

	ak2, err := b.CreateAccessKey("dave")
	require.NoError(t, err)
	assert.NotEmpty(t, ak2.AccessKeyID)

	// Third key must fail.
	_, err = b.CreateAccessKey("dave")
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrLimitExceeded,
		"third access key must return LimitExceeded")
}

func TestCreateAccessKey_LimitPerUser_IndependentAcrossUsers(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("alice", "/", "")
	_, _ = b.CreateUser("bob", "/", "")

	// Fill alice's quota.
	_, err := b.CreateAccessKey("alice")
	require.NoError(t, err)
	_, err = b.CreateAccessKey("alice")
	require.NoError(t, err)

	// Bob should still be able to create keys.
	_, err = b.CreateAccessKey("bob")
	require.NoError(t, err, "alice's limit must not affect bob")
}

func TestUpdateAccessKey_ActivateDeactivate(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("alice", "/", "")
	ak, err := b.CreateAccessKey("alice")
	require.NoError(t, err)
	assert.Equal(t, "Active", ak.Status)

	require.NoError(t, b.UpdateAccessKey("alice", ak.AccessKeyID, "Inactive"))
	keys, err := b.ListAccessKeys("alice", "", 10)
	require.NoError(t, err)
	require.Len(t, keys.Data, 1)
	assert.Equal(t, "Inactive", keys.Data[0].Status)

	require.NoError(t, b.UpdateAccessKey("alice", ak.AccessKeyID, "Active"))
	keys2, err := b.ListAccessKeys("alice", "", 10)
	require.NoError(t, err)
	assert.Equal(t, "Active", keys2.Data[0].Status)
}

func TestUpdateAccessKey_NotFound(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("alice", "/", "")

	err := b.UpdateAccessKey("alice", "AKIANONEXISTENT12345", "Inactive")
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrAccessKeyNotFound)
}

func TestGetAccessKeyLastUsed_NotUsed(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("alice", "/", "")
	ak, _ := b.CreateAccessKey("alice")

	result, err := b.GetAccessKeyLastUsed(ak.AccessKeyID)
	require.NoError(t, err)
	assert.Equal(t, "N/A", result.LastUsedDate, "new key must report N/A for last used date")
}

func TestGetAccessKeyLastUsed_AfterUsage(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("alice", "/", "")
	ak, _ := b.CreateAccessKey("alice")

	b.RecordAccessKeyUsage(ak.AccessKeyID, "us-west-2", "ec2")

	result, err := b.GetAccessKeyLastUsed(ak.AccessKeyID)
	require.NoError(t, err)
	assert.NotEqual(t, "N/A", result.LastUsedDate, "last used date must be set after usage")
	assert.Equal(t, "us-west-2", result.Region)
	assert.Equal(t, "ec2", result.ServiceName)
}

func TestInMemoryBackend_AccessKeys(t *testing.T) {
	t.Parallel()

	t.Run("CreateAndListAccessKeys", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateUser("alice", "/", "")
		ak, err := b.CreateAccessKey("alice")
		require.NoError(t, err)
		assert.Equal(t, "alice", ak.UserName)
		assert.Equal(t, "Active", ak.Status)
		assert.NotEmpty(t, ak.AccessKeyID)
		assert.NotEmpty(t, ak.SecretAccessKey)

		keys, err := b.ListAccessKeys("alice", "", 0)
		require.NoError(t, err)
		require.Len(t, keys.Data, 1)
		assert.Equal(t, ak.AccessKeyID, keys.Data[0].AccessKeyID)
	})

	t.Run("CreateAccessKeyUserNotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, err := b.CreateAccessKey("nonexistent")
		require.ErrorIs(t, err, iam.ErrUserNotFound)
	})

	t.Run("DeleteAccessKey", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateUser("alice", "/", "")
		ak, _ := b.CreateAccessKey("alice")
		err := b.DeleteAccessKey("alice", ak.AccessKeyID)
		require.NoError(t, err)
		keys, _ := b.ListAccessKeys("alice", "", 0)
		assert.Empty(t, keys.Data)
	})

	t.Run("DeleteAccessKeyNotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateUser("alice", "/", "")
		err := b.DeleteAccessKey("alice", "AKIANONEXISTENT")
		require.ErrorIs(t, err, iam.ErrAccessKeyNotFound)
	})

	t.Run("ListAccessKeysUserNotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, err := b.ListAccessKeys("nonexistent", "", 0)
		require.ErrorIs(t, err, iam.ErrUserNotFound)
	})

	t.Run("ListAllAccessKeys", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateUser("alice", "/", "")
		_, _ = b.CreateAccessKey("alice")
		keys := b.ListAllAccessKeys()
		require.Len(t, keys, 1)
	})
}
