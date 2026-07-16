package memorydb_test

import (
	"context"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_SubnetGroup_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		sgName  string
		wantErr bool
	}{
		{
			name:   "create_and_describe",
			sgName: "test-sg",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			req := &memorydb.ExportedCreateSubnetGroupRequest{
				SubnetGroupName: tt.sgName,
				SubnetIDs:       []string{"subnet-1", "subnet-2"},
			}

			sg, err := b.CreateSubnetGroup(context.Background(), req)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.sgName, sg.Name)
			assert.NotEmpty(t, sg.ARN)

			sgs, err := b.DescribeSubnetGroups(context.Background(), tt.sgName)
			require.NoError(t, err)
			require.Len(t, sgs, 1)

			_, err = b.DeleteSubnetGroup(context.Background(), tt.sgName)
			require.NoError(t, err)
		})
	}
}
