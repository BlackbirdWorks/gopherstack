package efs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/efs"
)

// TestLifecyclePolicyValidation verifies invalid enum values are rejected.
func TestLifecyclePolicyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		policy    efs.LifecyclePolicy
		name      string
		wantErr   bool
	}{
		{
			name:   "valid_transition_to_ia",
			policy: efs.LifecyclePolicy{TransitionToIA: "AFTER_30_DAYS"},
		},
		{
			name:   "valid_transition_to_primary",
			policy: efs.LifecyclePolicy{TransitionToPrimaryStorageClass: "AFTER_1_ACCESS"},
		},
		{
			name:      "invalid_transition_to_ia",
			policy:    efs.LifecyclePolicy{TransitionToIA: "AFTER_FOREVER"},
			wantErr:   true,
			wantErrIs: efs.ErrValidation,
		},
		{
			name:      "invalid_transition_to_primary",
			policy:    efs.LifecyclePolicy{TransitionToPrimaryStorageClass: "NEVER"},
			wantErr:   true,
			wantErrIs: efs.ErrValidation,
		},
		{
			name:   "all_none_valid",
			policy: efs.LifecyclePolicy{TransitionToIA: "NONE"},
		},
		{
			name:   "empty_policy_valid",
			policy: efs.LifecyclePolicy{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestEFSBackend()
			fs, err := b.CreateFileSystem(context.Background(), fsReq("tok-lp-"+tt.name))
			require.NoError(t, err)

			_, err = b.PutLifecycleConfiguration(
				context.Background(),
				fs.FileSystemID,
				[]efs.LifecyclePolicy{tt.policy},
			)

			if tt.wantErr {
				require.ErrorIs(t, err, tt.wantErrIs)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
