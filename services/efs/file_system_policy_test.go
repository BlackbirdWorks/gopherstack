package efs_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/efs"
)

// TestDescribeFileSystemPolicy_PolicyNotFound verifies the backend returns
// ErrPolicyNotFound (not ErrNotFound) for a file system with no policy.
func TestDescribeFileSystemPolicy_PolicyNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "fs_with_no_policy"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestEFSBackend()
			fs, err := b.CreateFileSystem(
				context.Background(),
				efs.CreateFileSystemRequest{CreationToken: "policy-backend-" + tt.name},
			)
			require.NoError(t, err)

			_, err = b.DescribeFileSystemPolicy(context.Background(), fs.FileSystemID)
			require.ErrorIs(t, err, efs.ErrPolicyNotFound)
			require.NotErrorIs(t, err, efs.ErrNotFound)
		})
	}
}

// TestFileSystemPolicyValidation verifies policy must be valid JSON and ≤ 20 KB.
func TestFileSystemPolicyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		name      string
		policy    string
		wantErr   bool
	}{
		{
			name:   "valid_json_accepted",
			policy: `{"Version":"2012-10-17","Statement":[]}`,
		},
		{
			name:      "invalid_json_rejected",
			policy:    `{not valid json}`,
			wantErr:   true,
			wantErrIs: efs.ErrInvalidPolicy,
		},
		{
			name:      "empty_string_rejected",
			policy:    ``,
			wantErr:   true,
			wantErrIs: efs.ErrInvalidPolicy,
		},
		{
			name:      "oversized_policy_rejected",
			policy:    `{"Version":"2012-10-17","Statement":[{"Sid":"` + strings.Repeat("a", 21*1024) + `"}]}`,
			wantErr:   true,
			wantErrIs: efs.ErrInvalidPolicy,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestEFSBackend()
			fs, err := b.CreateFileSystem(context.Background(), fsReq("tok-fsp-val-"+tt.name))
			require.NoError(t, err)

			err = b.PutFileSystemPolicy(context.Background(), fs.FileSystemID, tt.policy)

			if tt.wantErr {
				require.ErrorIs(t, err, tt.wantErrIs)
				require.NotErrorIs(t, err, efs.ErrValidation,
					"malformed FileSystemPolicy must map to InvalidPolicyException, not ValidationException")
			} else {
				require.NoError(t, err)
			}
		})
	}
}
