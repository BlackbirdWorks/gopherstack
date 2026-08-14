package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlacementGroupOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		op      string
		wantErr bool
	}{
		{name: "create", op: "create"},
		{name: "create_bad_name", op: "create_bad_name", wantErr: true},
		{name: "create_duplicate", op: "create_duplicate", wantErr: true},
		{name: "describe_all", op: "describe_all"},
		{name: "describe_by_name", op: "describe_by_name"},
		{name: "delete", op: "delete"},
		{name: "delete_not_found", op: "delete_not_found", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			switch tt.op {
			case "create":
				pg, err := b.CreatePlacementGroup("test-pg", "cluster", nil)
				require.NoError(t, err)
				assert.Equal(t, "test-pg", pg.Name)
				assert.Equal(t, "cluster", pg.Strategy)
				assert.Equal(t, "available", pg.State)

			case "create_bad_name":
				_, err := b.CreatePlacementGroup("", "cluster", nil)
				require.Error(t, err)

			case "create_duplicate":
				_, err := b.CreatePlacementGroup("dup-pg", "cluster", nil)
				require.NoError(t, err)
				_, err = b.CreatePlacementGroup("dup-pg", "spread", nil)
				require.Error(t, err)

			case "describe_all":
				_, err := b.CreatePlacementGroup("pg1", "cluster", nil)
				require.NoError(t, err)
				pgs := b.DescribePlacementGroups(nil)
				assert.NotEmpty(t, pgs)

			case "describe_by_name":
				_, err := b.CreatePlacementGroup("pg-named", "spread", nil)
				require.NoError(t, err)
				pgs := b.DescribePlacementGroups([]string{"pg-named"})
				require.Len(t, pgs, 1)
				assert.Equal(t, "pg-named", pgs[0].Name)

			case "delete":
				_, err := b.CreatePlacementGroup("del-pg", "cluster", nil)
				require.NoError(t, err)
				err = b.DeletePlacementGroup("del-pg")
				require.NoError(t, err)
				pgs := b.DescribePlacementGroups([]string{"del-pg"})
				assert.Empty(t, pgs)

			case "delete_not_found":
				err := b.DeletePlacementGroup("nonexistent-pg")
				require.Error(t, err)
			}
		})
	}
}
