package kms_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/cmek"
	"github.com/blackbirdworks/gopherstack/services/kms"
)

func TestNewKeyChecker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ref   func(id, arn string) string
		setup func(t *testing.T, b *kms.InMemoryBackend, id string)
		want  error
		name  string
		usage string
	}{
		{name: "key id", ref: func(id, _ string) string { return id }},
		{name: "arn", ref: func(_, arn string) string { return arn }},
		{
			name: "missing",
			ref:  func(string, string) string { return "00000000-0000-0000-0000-000000000000" },
			want: cmek.ErrNotFound,
		},
		{name: "managed alias", ref: func(string, string) string { return "alias/aws/secretsmanager" }},
		{
			name: "unknown managed alias",
			ref:  func(string, string) string { return "alias/aws/nonesuch" },
			want: cmek.ErrNotFound,
		},
		{
			name:  "signing key",
			usage: kms.KeyUsageSignVerify,
			ref:   func(id, _ string) string { return id },
			want:  cmek.ErrPurpose,
		},
		{
			name: "disabled", ref: func(id, _ string) string { return id }, want: cmek.ErrDisabled,
			setup: func(t *testing.T, b *kms.InMemoryBackend, id string) {
				t.Helper()

				err := b.DisableKey(t.Context(), &kms.DisableKeyInput{KeyID: id})
				require.NoError(t, err)
			},
		},
		{
			name: "pending deletion", ref: func(id, _ string) string { return id }, want: cmek.ErrState,
			setup: func(t *testing.T, b *kms.InMemoryBackend, id string) {
				t.Helper()

				_, err := b.ScheduleKeyDeletion(
					t.Context(),
					&kms.ScheduleKeyDeletionInput{KeyID: id, PendingWindowInDays: 7},
				)
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()

			in := &kms.CreateKeyInput{KeyUsage: tt.usage}
			if tt.usage == kms.KeyUsageSignVerify {
				in.KeySpec = "ECC_NIST_P256"
			}

			out, err := b.CreateKey(t.Context(), in)
			require.NoError(t, err)

			if tt.setup != nil {
				tt.setup(t, b, out.KeyMetadata.KeyID)
			}

			err = kms.NewKeyChecker(b).CheckKey(t.Context(), tt.ref(out.KeyMetadata.KeyID, out.KeyMetadata.Arn))
			if tt.want == nil {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, tt.want)
		})
	}
}
