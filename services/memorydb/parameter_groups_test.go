package memorydb_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

func TestBackend_ParameterGroup_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		pgName string
		family string
	}{
		{
			name:   "create_and_describe",
			pgName: "test-pg",
			family: "memorydb_redis7",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			req := &memorydb.ExportedCreateParameterGroupRequest{
				ParameterGroupName: tt.pgName,
				Family:             tt.family,
			}

			pg, err := b.CreateParameterGroup(context.Background(), req)

			require.NoError(t, err)
			assert.Equal(t, tt.pgName, pg.Name)
			assert.Equal(t, tt.family, pg.Family)
			assert.NotEmpty(t, pg.ARN)

			pgs, err := b.DescribeParameterGroups(context.Background(), tt.pgName)
			require.NoError(t, err)
			require.Len(t, pgs, 1)

			_, err = b.DeleteParameterGroup(context.Background(), tt.pgName)
			require.NoError(t, err)
		})
	}
}

// TestRefinement3_DescribeParameters_Backend tests DescribeParameters backend directly.
func TestDescribeParameters(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

	_, err := b.CreateParameterGroup(context.Background(), &memorydb.ExportedCreateParameterGroupRequest{
		ParameterGroupName: "test-pg",
		Family:             "memorydb_redis7",
	})
	require.NoError(t, err)

	params, err := b.DescribeParameters(context.Background(), "test-pg")
	require.NoError(t, err)
	assert.NotNil(t, params)
}

// TestRefinement3_DescribeParameters_Backend_NotFound tests DescribeParameters returns error for unknown group.
func TestDescribeParameters_NotFound(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

	_, err := b.DescribeParameters(context.Background(), "no-such-group")
	require.Error(t, err)
}

// TestRefinement3_ResetParameterGroup_Backend tests ResetParameterGroup backend directly.
func TestResetParameterGroup(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

	_, err := b.CreateParameterGroup(context.Background(), &memorydb.ExportedCreateParameterGroupRequest{
		ParameterGroupName: "reset-pg",
		Family:             "memorydb_redis7",
	})
	require.NoError(t, err)

	pg, err := b.ResetParameterGroup(context.Background(), "reset-pg", nil, true)
	require.NoError(t, err)
	assert.Equal(t, "reset-pg", pg.Name)
}

// TestRefinement3_DescribeParameters_Sorted verifies DescribeParameters returns sorted results.
func TestDescribeParameters_Sorted(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

	_, err := b.CreateParameterGroup(context.Background(), &memorydb.ExportedCreateParameterGroupRequest{
		ParameterGroupName: "sort-pg",
		Family:             "memorydb_redis7",
	})
	require.NoError(t, err)

	h := memorydb.NewHandler(b)

	rec := doRequest(t, h, "DescribeParameters", map[string]any{
		"ParameterGroupName": "sort-pg",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	params, _ := out["Parameters"].([]any)
	for i := 1; i < len(params); i++ {
		prev := params[i-1].(map[string]any)["Name"].(string)
		curr := params[i].(map[string]any)["Name"].(string)
		assert.LessOrEqual(t, prev, curr, "parameters should be sorted by name")
	}
}

// TestRefinement1_CreateParameterGroupMissingFamily verifies family is required.
func TestCreateParameterGroupMissingFamily(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateParameterGroup", map[string]any{
		"ParameterGroupName": "no-family-pg",
		// Family intentionally omitted
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestResetReseedsDefaultParameterGroups(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

	// Count default parameter groups before reset.
	initialCount := memorydb.ParameterGroupCount(b)
	assert.GreaterOrEqual(t, initialCount, 4, "default parameter groups should be seeded")

	// Reset.
	b.Reset()

	// Count after reset.
	afterCount := memorydb.ParameterGroupCount(b)
	assert.Equal(t, initialCount, afterCount, "Reset should re-seed the same default parameter groups")
}

// -- NodeType validation ---------------------------------------------------------
