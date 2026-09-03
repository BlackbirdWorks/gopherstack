package cloudformation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *cloudformation.InMemoryBackend) string
		verify func(t *testing.T, b *cloudformation.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *cloudformation.InMemoryBackend) string {
				stack, err := b.CreateStack(
					t.Context(),
					"test-stack",
					`{"AWSTemplateFormatVersion":"2010-09-09"}`,
					nil,
					cloudformation.StackOptions{},
				)
				if err != nil {
					return ""
				}

				return stack.StackName
			},
			verify: func(t *testing.T, b *cloudformation.InMemoryBackend, id string) {
				t.Helper()

				stack, err := b.DescribeStack(id)
				require.NoError(t, err)
				assert.Equal(t, id, stack.StackName)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *cloudformation.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *cloudformation.InMemoryBackend, _ string) {
				t.Helper()

				stacks, err := b.ListStacks(nil, "")
				require.NoError(t, err)
				assert.Empty(t, stacks.Data)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := cloudformation.NewInMemoryBackend()
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := cloudformation.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := cloudformation.NewInMemoryBackend()
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_SnapshotRestore_PlainMapFields locks in persistence for
// the one-to-many / nested map fields that are NOT store.Table-registered
// (see store_setup.go's registerAllTables doc comment) and were previously
// silently dropped by Snapshot/Restore: stackInstances, stackSetOperations,
// typeConfigs, typeVersions, resourceDriftStatus/Detail, and the
// driftByStackID reverse index rebuilt from the (table-backed) drift
// detections.
func TestInMemoryBackend_SnapshotRestore_PlainMapFields(t *testing.T) {
	t.Parallel()

	original := cloudformation.NewInMemoryBackend()
	ctx := t.Context()

	_, err := original.CreateStackSet("test-set", "desc", `{"Resources":{}}`, cloudformation.StackSetOptions{})
	require.NoError(t, err)

	opID, err := original.CreateStackInstances(
		ctx, "test-set", []string{"111111111111"}, nil, []string{"us-east-1"},
	)
	require.NoError(t, err)
	require.NotEmpty(t, opID)

	stack, err := original.CreateStack(ctx, "drift-stack", `{"Resources":{}}`, nil, cloudformation.StackOptions{})
	require.NoError(t, err)

	detectionID, err := original.DetectStackDrift(stack.StackName)
	require.NoError(t, err)
	require.NotEmpty(t, detectionID)

	token, err := original.RegisterType("Acme::Demo::Widget", "")
	require.NoError(t, err)
	require.NotEmpty(t, token)

	_, err = original.SetTypeConfiguration("Acme::Demo::Widget", `{"key":"value"}`)
	require.NoError(t, err)

	snap := original.Snapshot(ctx)
	require.NotNil(t, snap)

	fresh := cloudformation.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(ctx, snap))

	instances, err := fresh.ListStackInstances("test-set", "", cloudformation.ListStackInstancesFilter{})
	require.NoError(t, err)
	require.Len(t, instances.Data, 1)
	assert.Equal(t, "111111111111", instances.Data[0].Account)
	assert.Equal(t, "us-east-1", instances.Data[0].Region)

	ops, err := fresh.ListStackSetOperations("test-set", "")
	require.NoError(t, err)
	require.NotEmpty(t, ops.Data)

	versions, err := fresh.ListTypeVersions("Acme::Demo::Widget", "")
	require.NoError(t, err)
	require.NotEmpty(t, versions)

	details, errs, unprocessed := fresh.BatchDescribeTypeConfigurations(
		[]cloudformation.TypeConfigurationIdentifier{{TypeName: "Acme::Demo::Widget"}},
	)
	assert.Empty(t, errs)
	assert.Empty(t, unprocessed)
	require.Len(t, details, 1)
	assert.JSONEq(t, `{"key":"value"}`, details[0].Configuration)
}
