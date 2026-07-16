package rds_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestModifyDBClusterEndpoint(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs    error
		name         string
		endpointID   string
		endpointType string
		wantErr      bool
	}{
		{
			name:         "success updates type",
			endpointID:   "my-endpoint",
			endpointType: "READER",
		},
		{
			name:       "not found",
			endpointID: "missing",
			wantErr:    true,
			wantErrIs:  rds.ErrClusterEndpointNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateDBCluster(
					"my-cluster",
					"aurora-mysql",
					"admin",
					"",
					"",
					0,
					nil,
					rds.DBClusterOptions{},
				)
				require.NoError(t, err)
				_, err = b.CreateDBClusterEndpoint(tt.endpointID, "my-cluster", "WRITER")
				require.NoError(t, err)
			}
			got, err := b.ModifyDBClusterEndpoint(tt.endpointID, tt.endpointType)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			if tt.endpointType != "" {
				assert.Equal(t, tt.endpointType, got.EndpointType)
			}
		})
	}
}
