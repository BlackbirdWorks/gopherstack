package gcpbigtable_test

import (
	"testing"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func subset(prefixes []string, fams map[string]bigtable.FamilySubset) *bigtable.SubsetViewConf {
	c := &bigtable.SubsetViewConf{FamilySubsets: fams}
	for _, p := range prefixes {
		c.AddRowPrefix([]byte(p))
	}

	return c
}

func viewConf(protection bigtable.DeletionProtection) *bigtable.AuthorizedViewConf {
	return &bigtable.AuthorizedViewConf{
		TableID: "t", AuthorizedViewID: "v", DeletionProtection: protection,
		AuthorizedView: subset([]string{"pub#"}, map[string]bigtable.FamilySubset{
			"cf1": {QualifierPrefixes: [][]byte{[]byte("a")}, Qualifiers: [][]byte{[]byte("z")}},
		}),
	}
}

func viewEnv(t *testing.T, protection bigtable.DeletionProtection) (*env, *bigtable.Table) {
	t.Helper()

	e := newEnv(t)
	tbl := e.seeded(t, "t", "cf1", "cf2")
	require.NoError(t, e.ac.CreateAuthorizedView(t.Context(), viewConf(protection)))

	return e, tbl
}

func TestAuthorizedViewErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(e *env) error
		name string
		want codes.Code
	}{
		{
			name: "duplicate", want: codes.AlreadyExists,
			run: func(e *env) error { return e.ac.CreateAuthorizedView(t.Context(), viewConf(bigtable.None)) },
		},
		{
			name: "unknown family", want: codes.InvalidArgument,
			run: func(e *env) error {
				return e.ac.CreateAuthorizedView(t.Context(), &bigtable.AuthorizedViewConf{
					TableID: "t", AuthorizedViewID: "bad",
					AuthorizedView: subset([]string{""}, map[string]bigtable.FamilySubset{"nope": {}}),
				})
			},
		},
		{
			name: "missing", want: codes.NotFound,
			run: func(e *env) error {
				_, err := e.ac.AuthorizedViewInfo(t.Context(), "t", "none")

				return err
			},
		},
		{
			name: "protected delete", want: codes.FailedPrecondition,
			run: func(e *env) error { return e.ac.DeleteAuthorizedView(t.Context(), "t", "v") },
		},
		{
			name: "protected table delete", want: codes.FailedPrecondition,
			run: func(e *env) error { return e.ac.DeleteTable(t.Context(), "t") },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, _ := viewEnv(t, bigtable.Protected)
			assert.Equal(t, tt.want, code(tt.run(e)))
		})
	}
}

const viewName = "projects/" + project + "/instances/" + instance + "/tables/t/authorizedViews/v"

func TestAuthorizedViewLifecycle(t *testing.T) {
	t.Parallel()

	e, _ := viewEnv(t, bigtable.Protected)
	ctx := t.Context()

	info, err := e.ac.AuthorizedViewInfo(ctx, "t", "v")
	require.NoError(t, err)

	assert.Nil(t, info.AuthorizedView, "an unspecified view is BASIC: no subset_view")
	assert.Equal(t, bigtable.Protected, info.DeletionProtection)

	full, err := adminpb.NewBigtableTableAdminClient(e.conn).GetAuthorizedView(ctx, &adminpb.GetAuthorizedViewRequest{
		Name: viewName, View: adminpb.AuthorizedView_FULL,
	})
	require.NoError(t, err)
	assert.Equal(t, [][]byte{[]byte("pub#")}, full.GetSubsetView().GetRowPrefixes())

	names, err := e.ac.AuthorizedViews(ctx, "t")
	require.NoError(t, err)
	assert.Equal(t, []string{"v"}, names)

	require.NoError(t, e.ac.UpdateAuthorizedView(ctx, bigtable.UpdateAuthorizedViewConf{
		AuthorizedViewConf: bigtable.AuthorizedViewConf{
			TableID: "t", AuthorizedViewID: "v", DeletionProtection: bigtable.Unprotected,
		},
	}))

	info, err = e.ac.AuthorizedViewInfo(ctx, "t", "v")
	require.NoError(t, err)
	assert.Equal(t, bigtable.Unprotected, info.DeletionProtection)

	full, err = adminpb.NewBigtableTableAdminClient(e.conn).GetAuthorizedView(ctx, &adminpb.GetAuthorizedViewRequest{
		Name: viewName, View: adminpb.AuthorizedView_FULL,
	})
	require.NoError(t, err)
	assert.NotNil(t, full.GetSubsetView(), "subset_view is untouched by a deletion_protection-only update")

	require.NoError(t, e.ac.DeleteAuthorizedView(ctx, "t", "v"))

	_, err = e.ac.AuthorizedViewInfo(ctx, "t", "v")
	assert.Equal(t, codes.NotFound, code(err))
}

func TestAuthorizedViewData(t *testing.T) {
	t.Parallel()

	seed := func(t *testing.T) bigtable.TableAPI {
		t.Helper()

		e, tbl := viewEnv(t, bigtable.None)
		put(t, tbl,
			seedCell{"pub#1", "cf1", "a1", "in", 1000},
			seedCell{"pub#1", "cf1", "b1", "hidden-col", 1000},
			seedCell{"pub#1", "cf2", "a1", "hidden-fam", 1000},
			seedCell{"priv#1", "cf1", "a1", "hidden-row", 1000},
		)

		return e.client(t).OpenAuthorizedView("t", "v")
	}

	t.Run("read", func(t *testing.T) {
		t.Parallel()

		view := seed(t)

		var rows []bigtable.Row

		require.NoError(t, view.ReadRows(t.Context(), bigtable.InfiniteRange(""), func(r bigtable.Row) bool {
			rows = append(rows, r)

			return true
		}))
		assert.Equal(t, []string{"pub#1|cf1:a1|1000|in"}, flat(rows))

		rows = nil

		require.NoError(t, view.ReadRows(t.Context(), bigtable.SingleRow("priv#1"), func(r bigtable.Row) bool {
			rows = append(rows, r)

			return true
		}))
		assert.Empty(t, rows)
	})

	applyTests := []struct {
		row, fam, col string
		want          codes.Code
	}{
		{"pub#2", "cf1", "a2", codes.OK},
		{"priv#2", "cf1", "a2", codes.PermissionDenied},
		{"pub#2", "cf1", "b2", codes.PermissionDenied},
		{"pub#2", "cf2", "a2", codes.PermissionDenied},
		{"pub#2", "nofam", "a2", codes.PermissionDenied},
		{"priv#2", "nofam", "a2", codes.PermissionDenied},
	}

	for _, tt := range applyTests {
		t.Run("apply "+tt.row+" "+tt.fam+":"+tt.col, func(t *testing.T) {
			t.Parallel()

			m := bigtable.NewMutation()
			m.Set(tt.fam, tt.col, 2000, []byte("x"))
			assert.Equal(t, tt.want, code(seed(t).Apply(t.Context(), tt.row, m)))
		})
	}

	t.Run("delete row needs whole row", func(t *testing.T) {
		t.Parallel()

		m := bigtable.NewMutation()
		m.DeleteRow()
		assert.Equal(t, codes.PermissionDenied, code(seed(t).Apply(t.Context(), "pub#1", m)))
	})

	t.Run("read modify write", func(t *testing.T) {
		t.Parallel()

		rmw := bigtable.NewReadModifyWrite()
		rmw.AppendValue("cf1", "b1", []byte("x"))

		_, err := seed(t).ApplyReadModifyWrite(t.Context(), "pub#1", rmw)
		assert.Equal(t, codes.PermissionDenied, code(err))
	})

	t.Run("unknown view", func(t *testing.T) {
		t.Parallel()

		e, _ := viewEnv(t, bigtable.None)
		m := bigtable.NewMutation()
		m.Set("cf1", "a1", 1, []byte("x"))
		missing := e.client(t).OpenAuthorizedView("t", "nope")
		assert.Equal(t, codes.NotFound, code(missing.Apply(t.Context(), "pub#1", m)))
	})
}

func TestAuthorizedViewIAMAndRestore(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	_ = src.seeded(t, "t", "cf1")
	ctx := t.Context()

	require.NoError(t, src.ac.CreateAuthorizedView(ctx, &bigtable.AuthorizedViewConf{
		TableID: "t", AuthorizedViewID: "v", DeletionProtection: bigtable.Protected,
		AuthorizedView: subset([]string{""}, map[string]bigtable.FamilySubset{
			"cf1": {QualifierPrefixes: [][]byte{{}}},
		}),
	}))

	h := src.ac.AuthorizedViewIAM("t", "v")
	pol, err := h.Policy(ctx)
	require.NoError(t, err)
	pol.Add("user:a@example.com", "roles/bigtable.reader")
	require.NoError(t, h.SetPolicy(ctx, pol))

	dst := newEnv(t)
	require.NoError(t, dst.handler.Backend.Restore(ctx, src.handler.Backend.Snapshot(ctx)))

	info, err := dst.ac.AuthorizedViewInfo(ctx, "t", "v")
	require.NoError(t, err)
	assert.Equal(t, bigtable.Protected, info.DeletionProtection)

	got, err := dst.ac.AuthorizedViewIAM("t", "v").Policy(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"user:a@example.com"}, got.Members("roles/bigtable.reader"))

	assert.Equal(t, codes.FailedPrecondition, code(dst.iac.DeleteInstance(ctx, instance)))
}
