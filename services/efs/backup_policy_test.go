package efs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/efs"
)

// TestBackupPolicyValidation verifies only valid status values are accepted.
func TestBackupPolicyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		name      string
		status    string
		wantErr   bool
	}{
		{name: "enabled_valid", status: "ENABLED"},
		{name: "disabled_valid", status: "DISABLED"},
		{name: "enabling_valid", status: "ENABLING"},
		{name: "disabling_valid", status: "DISABLING"},
		{name: "empty_invalid", status: "", wantErr: true, wantErrIs: efs.ErrValidation},
		{
			name:      "unknown_status_invalid",
			status:    "ACTIVE",
			wantErr:   true,
			wantErrIs: efs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestEFSBackend()
			fs, err := b.CreateFileSystem(context.Background(), fsReq("tok-bp-val-"+tt.name))
			require.NoError(t, err)

			err = b.PutBackupPolicy(context.Background(), fs.FileSystemID, tt.status)

			if tt.wantErr {
				require.ErrorIs(t, err, tt.wantErrIs)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
