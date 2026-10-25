package gcpbigtable_test

import (
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func familyPolicies(t *testing.T, e *env, table string) map[string]string {
	t.Helper()

	info, err := e.ac.TableInfo(t.Context(), table)
	require.NoError(t, err)

	out := map[string]string{}
	for _, f := range info.FamilyInfos {
		out[f.Name] = f.GCPolicy
	}

	return out
}

func TestTableLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, instance)
	ctx := t.Context()

	require.NoError(t, e.ac.CreateTableFromConf(ctx, &bigtable.TableConf{
		TableID:   "alpha",
		SplitKeys: []string{"m"},
		ColumnFamilies: map[string]bigtable.Family{
			"cf1": {GCPolicy: bigtable.MaxVersionsPolicy(3)},
			"cf2": {GCPolicy: bigtable.MaxAgePolicy(24 * time.Hour)},
			"cf3": {},
		},
		DeletionProtection: bigtable.Unprotected,
	}))
	require.NoError(t, e.ac.CreateTable(ctx, "beta"))

	tables, err := e.ac.Tables(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"alpha", "beta"}, tables)

	info, err := e.ac.TableInfo(ctx, "alpha")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"cf1", "cf2", "cf3"}, info.Families)
	assert.Equal(t, bigtable.Unprotected, info.DeletionProtection)

	pol := familyPolicies(t, e, "alpha")
	assert.Equal(t, "versions() > 3", pol["cf1"])
	assert.Equal(t, "age() > 1d", pol["cf2"])
	assert.Equal(t, "<never>", pol["cf3"])

	assert.Equal(t, codes.AlreadyExists, code(e.ac.CreateTable(ctx, "alpha")))
	assert.Equal(t, codes.InvalidArgument, code(e.ac.CreateTable(ctx, "bad table!")))
	assert.Equal(t, codes.InvalidArgument, code(e.ac.CreatePresplitTable(ctx, "dupes", []string{"a", "a"})))

	require.NoError(t, e.ac.DeleteTable(ctx, "beta"))
	assert.Equal(t, codes.NotFound, code(e.ac.DeleteTable(ctx, "beta")))

	_, err = e.ac.TableInfo(ctx, "beta")
	assert.Equal(t, codes.NotFound, code(err))
}

func TestColumnFamilies(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, instance)
	ctx := t.Context()

	require.NoError(t, e.ac.CreateTable(ctx, "t"))
	require.NoError(t, e.ac.CreateColumnFamily(ctx, "t", "cf"))
	require.NoError(t, e.ac.CreateColumnFamilyWithConfig(ctx, "t", "cf2", bigtable.Family{GCPolicy: bigtable.MaxVersionsPolicy(1)}))

	assert.Equal(t, codes.AlreadyExists, code(e.ac.CreateColumnFamily(ctx, "t", "cf")))
	assert.Equal(t, codes.InvalidArgument, code(e.ac.CreateColumnFamily(ctx, "t", "bad family!")))
	assert.Equal(t, codes.NotFound, code(e.ac.CreateColumnFamily(ctx, "ghost", "cf")))

	require.NoError(t, e.ac.SetGCPolicy(ctx, "t", "cf", bigtable.UnionPolicy(bigtable.MaxVersionsPolicy(2), bigtable.MaxAgePolicy(time.Hour))))
	assert.Equal(t, "(versions() > 2 || age() > 1h)", familyPolicies(t, e, "t")["cf"])

	require.NoError(t, e.ac.SetGCPolicy(ctx, "t", "cf", bigtable.NoGcPolicy()))
	assert.Equal(t, "<never>", familyPolicies(t, e, "t")["cf"])

	assert.Equal(t, codes.InvalidArgument, code(e.ac.SetGCPolicy(ctx, "t", "cf", bigtable.MaxVersionsPolicy(0))))
	assert.Equal(t, codes.NotFound, code(e.ac.SetGCPolicy(ctx, "t", "ghost", bigtable.MaxVersionsPolicy(1))))

	require.NoError(t, e.ac.DeleteColumnFamily(ctx, "t", "cf2"))
	assert.Equal(t, codes.NotFound, code(e.ac.DeleteColumnFamily(ctx, "t", "cf2")))
	assert.NotContains(t, familyPolicies(t, e, "t"), "cf2")
}

func TestDropFamilyRemovesData(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tbl := e.seeded(t, "t", "cf", "keep")
	put(t, tbl, seedCell{"r1", "cf", "q", "v", 1000}, seedCell{"r1", "keep", "q", "k", 1000}, seedCell{"r2", "cf", "q", "v", 1000})

	require.NoError(t, e.ac.DeleteColumnFamily(t.Context(), "t", "cf"))
	assert.Equal(t, []string{"r1|keep:q|1000|k"}, readAll(t, tbl, bigtable.RowRange{}))

	require.NoError(t, e.ac.CreateColumnFamily(t.Context(), "t", "cf"))
	assert.Equal(t, []string{"r1|keep:q|1000|k"}, readAll(t, tbl, bigtable.RowRange{}), "a recreated family starts empty")
}

func TestDropRowRange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		drop func(e *env) error
		name string
		want []string
	}{
		{func(e *env) error { return e.ac.DropRowRange(t.Context(), "t", "user#") }, "prefix", []string{"other"}},
		{func(e *env) error { return e.ac.DropAllRows(t.Context(), "t") }, "all", nil},
		{func(e *env) error { return e.ac.DropRowRange(t.Context(), "t", "") }, "empty prefix", []string{"other", "user#1", "user#2"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			tbl := e.seeded(t, "t", "cf")
			put(t, tbl, seedCell{"user#1", "cf", "q", "v", 1000}, seedCell{"user#2", "cf", "q", "v", 1000}, seedCell{"other", "cf", "q", "v", 1000})

			err := tt.drop(e)
			if tt.name == "empty prefix" {
				assert.Equal(t, codes.InvalidArgument, code(err))
			} else {
				require.NoError(t, err)
			}

			var keys []string
			for _, r := range readAll(t, tbl, bigtable.RowRange{}) {
				keys = append(keys, r[:len(r)-len("|cf:q|1000|v")])
			}

			assert.Equal(t, tt.want, keys)
		})
	}
}

func TestTableUpdates(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, instance)
	ctx := t.Context()

	require.NoError(t, e.ac.CreateTable(ctx, "t"))
	require.NoError(t, e.ac.CreateColumnFamily(ctx, "t", "cf"))

	require.NoError(t, e.ac.UpdateTableWithDeletionProtection(ctx, "t", bigtable.Protected))

	info, err := e.ac.TableInfo(ctx, "t")
	require.NoError(t, err)
	assert.Equal(t, bigtable.Protected, info.DeletionProtection)

	assert.Equal(t, codes.FailedPrecondition, code(e.ac.DeleteTable(ctx, "t")))
	assert.Equal(t, codes.FailedPrecondition, code(e.ac.DeleteColumnFamily(ctx, "t", "cf")))

	require.NoError(t, e.ac.UpdateTableWithChangeStream(ctx, "t", 48*time.Hour))

	info, err = e.ac.TableInfo(ctx, "t")
	require.NoError(t, err)
	assert.Equal(t, 48*time.Hour, info.ChangeStreamRetention)

	assert.Error(t, e.ac.UpdateTableWithChangeStream(ctx, "t", 30*24*time.Hour), "retention above 7 days")
	require.NoError(t, e.ac.UpdateTableDisableChangeStream(ctx, "t"))

	info, err = e.ac.TableInfo(ctx, "t")
	require.NoError(t, err)
	assert.Nil(t, info.ChangeStreamRetention)

	require.NoError(t, e.ac.UpdateTableWithDeletionProtection(ctx, "t", bigtable.Unprotected))
	require.NoError(t, e.ac.DeleteTable(ctx, "t"))
}

func TestConsistency(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, instance)
	require.NoError(t, e.ac.CreateTable(t.Context(), "t"))
	require.NoError(t, e.ac.WaitForReplication(t.Context(), "t"))

	assert.Equal(t, codes.NotFound, code(e.ac.WaitForReplication(t.Context(), "ghost")))
}

func TestTableIAM(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, instance)
	require.NoError(t, e.ac.CreateTable(t.Context(), "t"))

	h := e.ac.TableIAM("t")
	pol, err := h.Policy(t.Context())
	require.NoError(t, err)

	pol.Add("serviceAccount:sa@p.iam.gserviceaccount.com", "roles/bigtable.admin")
	require.NoError(t, h.SetPolicy(t.Context(), pol))

	got, err := h.Policy(t.Context())
	require.NoError(t, err)
	assert.Len(t, got.Roles(), 1)

	_, err = e.ac.TableIAM("ghost").Policy(t.Context())
	assert.Equal(t, codes.NotFound, code(err))
}

func TestBackups(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tbl := e.seeded(t, "src", "cf")
	put(t, tbl, seedCell{"r1", "cf", "q", "v1", 1000}, seedCell{"r2", "cf", "q", "v2", 2000})
	ctx := t.Context()

	expire := time.Now().Add(48 * time.Hour)
	require.NoError(t, e.ac.CreateBackup(ctx, "src", cluster, "bk", expire))

	put(t, tbl, seedCell{"r3", "cf", "q", "later", 3000})

	info, err := e.ac.BackupInfo(ctx, cluster, "bk")
	require.NoError(t, err)
	assert.Equal(t, "bk", info.Name)
	assert.Equal(t, "READY", info.State)
	assert.Equal(t, "src", info.SourceTable)
	assert.Positive(t, info.SizeBytes)
	assert.WithinDuration(t, expire, info.ExpireTime, time.Second)

	it := e.ac.Backups(ctx, cluster)
	first, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, "bk", first.Name)

	require.NoError(t, e.ac.UpdateBackup(ctx, cluster, "bk", time.Now().Add(72*time.Hour)))
	assert.Error(t, e.ac.UpdateBackup(ctx, cluster, "bk", time.Now().Add(time.Hour)), "expiry under 6 hours")

	require.NoError(t, e.ac.RestoreTable(ctx, "restored", cluster, "bk"))

	restored := e.client(t).Open("restored")
	assert.Equal(t, []string{"r1|cf:q|1000|v1", "r2|cf:q|2000|v2"}, readAll(t, restored, bigtable.RowRange{}), "backup holds the data at backup time")

	tables, err := e.ac.Tables(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"restored", "src"}, tables)

	t.Run("errors", func(t *testing.T) {
		assert.Equal(t, codes.AlreadyExists, code(e.ac.CreateBackup(ctx, "src", cluster, "bk", expire)))
		assert.Equal(t, codes.AlreadyExists, code(e.ac.RestoreTable(ctx, "restored", cluster, "bk")))
		assert.Equal(t, codes.NotFound, code(e.ac.CreateBackup(ctx, "ghost", cluster, "b2", expire)))
		assert.Equal(t, codes.NotFound, code(e.ac.RestoreTable(ctx, "x", cluster, "ghost")))
		assert.Equal(t, codes.InvalidArgument, code(e.ac.CreateBackup(ctx, "src", cluster, "b3", time.Now().Add(time.Hour))))
	})

	require.NoError(t, e.ac.DeleteBackup(ctx, cluster, "bk"))
	assert.Equal(t, codes.NotFound, code(e.ac.DeleteBackup(ctx, cluster, "bk")))
}

func TestEncryptionInfo(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	key := "projects/" + project + "/locations/us-central1/keyRings/r/cryptoKeys/k"

	require.NoError(t, e.iac.CreateInstanceWithClusters(ctx, &bigtable.InstanceWithClustersConfig{
		InstanceID: instance, DisplayName: "d",
		Clusters: []bigtable.ClusterConfig{
			{InstanceID: instance, ClusterID: "plain", Zone: zone, NumNodes: 1},
			{InstanceID: instance, ClusterID: "keyed", Zone: "us-east1-b", NumNodes: 1, KMSKeyName: key},
		},
	}))
	require.NoError(t, e.ac.CreateTable(ctx, "t"))

	info, err := e.ac.EncryptionInfo(ctx, "t")
	require.NoError(t, err)
	require.Len(t, info["plain"], 1)
	assert.Equal(t, bigtable.GoogleDefaultEncryption, info["plain"][0].Type)
	require.Len(t, info["keyed"], 1)
	assert.Equal(t, bigtable.CustomerManagedEncryption, info["keyed"][0].Type)
	assert.Equal(t, key+"/cryptoKeyVersions/1", info["keyed"][0].KMSKeyVersion)
}
