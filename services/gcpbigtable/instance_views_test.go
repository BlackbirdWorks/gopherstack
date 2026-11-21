package gcpbigtable_test

import (
	"testing"

	"cloud.google.com/go/bigtable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestLogicalViewLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, instance)
	ctx := t.Context()

	require.NoError(t, e.iac.CreateLogicalView(ctx, instance, &bigtable.LogicalViewInfo{
		LogicalViewID: "lv", Query: "SELECT _key FROM t", DeletionProtection: bigtable.Protected,
	}))

	got, err := e.iac.LogicalViewInfo(ctx, instance, "lv")
	require.NoError(t, err)
	assert.Equal(t, "SELECT _key FROM t", got.Query)
	assert.Equal(t, bigtable.Protected, got.DeletionProtection)

	list, err := e.iac.LogicalViews(ctx, instance)
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, "lv", list[0].LogicalViewID)

	assert.Equal(t, codes.AlreadyExists, code(e.iac.CreateLogicalView(ctx, instance, &bigtable.LogicalViewInfo{
		LogicalViewID: "lv", Query: "SELECT 1",
	})))
	assert.Equal(t, codes.FailedPrecondition, code(e.iac.DeleteLogicalView(ctx, instance, "lv")))

	require.NoError(t, e.iac.UpdateLogicalView(ctx, instance, bigtable.LogicalViewInfo{
		LogicalViewID: "lv", Query: "SELECT _key, cf FROM t", DeletionProtection: bigtable.Unprotected,
	}))

	got, err = e.iac.LogicalViewInfo(ctx, instance, "lv")
	require.NoError(t, err)
	assert.Equal(t, "SELECT _key, cf FROM t", got.Query)
	assert.Equal(t, bigtable.Unprotected, got.DeletionProtection)

	require.NoError(t, e.iac.DeleteLogicalView(ctx, instance, "lv"))

	_, err = e.iac.LogicalViewInfo(ctx, instance, "lv")
	assert.Equal(t, codes.NotFound, code(err))
}

func TestMaterializedViewLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, instance)
	ctx := t.Context()

	require.NoError(t, e.iac.CreateMaterializedView(ctx, instance, &bigtable.MaterializedViewInfo{
		MaterializedViewID: "mv", Query: "SELECT _key, COUNT(*) FROM t GROUP BY _key",
		DeletionProtection: bigtable.Protected,
	}))

	got, err := e.iac.MaterializedViewInfo(ctx, instance, "mv")
	require.NoError(t, err)
	assert.Equal(t, bigtable.Protected, got.DeletionProtection)

	list, err := e.iac.MaterializedViews(ctx, instance)
	require.NoError(t, err)
	assert.Len(t, list, 1)

	assert.Equal(t, codes.FailedPrecondition, code(e.iac.DeleteMaterializedView(ctx, instance, "mv")))

	require.NoError(t, e.iac.UpdateMaterializedView(ctx, instance, bigtable.MaterializedViewInfo{
		MaterializedViewID: "mv", DeletionProtection: bigtable.Unprotected,
	}))

	got, err = e.iac.MaterializedViewInfo(ctx, instance, "mv")
	require.NoError(t, err)
	assert.Equal(t, bigtable.Unprotected, got.DeletionProtection)
	assert.Equal(t, "SELECT _key, COUNT(*) FROM t GROUP BY _key", got.Query, "the query is immutable")

	require.NoError(t, e.iac.DeleteMaterializedView(ctx, instance, "mv"))
}

func TestInstanceViewErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(e *env) error
		name string
		want codes.Code
	}{
		{name: "logical empty query", want: codes.InvalidArgument, run: func(e *env) error {
			return e.iac.CreateLogicalView(t.Context(), instance, &bigtable.LogicalViewInfo{LogicalViewID: "lv"})
		}},
		{name: "logical bad id", want: codes.InvalidArgument, run: func(e *env) error {
			return e.iac.CreateLogicalView(
				t.Context(),
				instance,
				&bigtable.LogicalViewInfo{LogicalViewID: "-x", Query: "q"},
			)
		}},
		{name: "materialized empty query", want: codes.InvalidArgument, run: func(e *env) error {
			return e.iac.CreateMaterializedView(
				t.Context(),
				instance,
				&bigtable.MaterializedViewInfo{MaterializedViewID: "mv"},
			)
		}},
		{name: "materialized missing", want: codes.NotFound, run: func(e *env) error {
			_, err := e.iac.MaterializedViewInfo(t.Context(), instance, "none")

			return err
		}},
		{name: "logical missing instance", want: codes.NotFound, run: func(e *env) error {
			_, err := e.iac.LogicalViewInfo(t.Context(), "ghost", "lv")

			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.createInstance(t, instance)
			assert.Equal(t, tt.want, code(tt.run(e)))
		})
	}
}

func TestInstanceViewsPersistence(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	src.createInstance(t, instance)
	require.NoError(t, src.iac.CreateLogicalView(t.Context(), instance, &bigtable.LogicalViewInfo{
		LogicalViewID: "lv", Query: "SELECT 1", DeletionProtection: bigtable.Protected,
	}))
	require.NoError(t, src.iac.CreateMaterializedView(t.Context(), instance, &bigtable.MaterializedViewInfo{
		MaterializedViewID: "mv", Query: "SELECT 2",
	}))

	dst := newEnv(t)
	require.NoError(t, dst.handler.Backend.Restore(t.Context(), src.handler.Backend.Snapshot(t.Context())))

	lv, err := dst.iac.LogicalViewInfo(t.Context(), instance, "lv")
	require.NoError(t, err)
	assert.Equal(t, bigtable.Protected, lv.DeletionProtection)

	mv, err := dst.iac.MaterializedViewInfo(t.Context(), instance, "mv")
	require.NoError(t, err)
	assert.Equal(t, "SELECT 2", mv.Query)
}
