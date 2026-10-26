package gcpbigtable_test

import (
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	ctx := t.Context()
	tbl := src.seeded(t, "t", "cf", "cf2")

	require.NoError(t, src.ac.SetGCPolicy(ctx, "t", "cf", bigtable.MaxVersionsPolicy(2)))
	put(
		t,
		tbl,
		seedCell{
			"r1",
			"cf",
			"q",
			"v1",
			1000,
		},
		seedCell{"r1", "cf", "q", "v2", 2000},
		seedCell{"r1", "cf", "q", "v3", 3000},
		seedCell{"r\xff\x00bin", "cf2", "q\x01", "\x00\x01\x02", 4000},
	)

	_, err := src.iac.CreateAppProfile(ctx, bigtable.ProfileConf{
		ProfileID: "p", InstanceID: instance, Description: "kept",
		RoutingConfig: &bigtable.SingleClusterRoutingConfig{ClusterID: cluster, AllowTransactionalWrites: true},
	})
	require.NoError(t, err)

	require.NoError(t, src.ac.UpdateTableWithDeletionProtection(ctx, "t", bigtable.Protected))
	require.NoError(t, src.ac.CreateBackup(ctx, "t", cluster, "bk", time.Now().Add(48*time.Hour)))

	pol, err := src.iac.InstanceIAM(instance).Policy(ctx)
	require.NoError(t, err)
	pol.Add("user:a@example.com", "roles/bigtable.reader")
	require.NoError(t, src.iac.InstanceIAM(instance).SetPolicy(ctx, pol))

	snap := src.handler.Backend.Snapshot(ctx)
	require.NotEmpty(t, snap)

	dst := newEnv(t)
	require.NoError(t, dst.handler.Backend.Restore(ctx, snap))

	info, err := dst.iac.InstanceInfo(ctx, instance)
	require.NoError(t, err)
	assert.Equal(t, instance, info.DisplayName)

	prof, err := dst.iac.GetAppProfile(ctx, instance, "p")
	require.NoError(t, err)
	assert.Equal(t, "kept", prof.GetDescription())

	ti, err := dst.ac.TableInfo(ctx, "t")
	require.NoError(t, err)
	assert.Equal(t, bigtable.Protected, ti.DeletionProtection)
	assert.Equal(t, "versions() > 2", familyPolicies(t, dst, "t")["cf"])

	got := readAll(t, dst.client(t).Open("t"), bigtable.RowRange{})
	assert.Equal(t, []string{
		"r1|cf:q|3000|v3", "r1|cf:q|2000|v2", "r\xff\x00bin|cf2:q\x01|4000|\x00\x01\x02",
	}, got, "binary keys, qualifiers and values survive; collected versions stay gone")

	bk, err := dst.ac.BackupInfo(ctx, cluster, "bk")
	require.NoError(t, err)
	assert.Equal(t, "READY", bk.State)

	require.NoError(t, dst.ac.RestoreTable(ctx, "from-backup", cluster, "bk"))

	p2, err := dst.iac.InstanceIAM(instance).Policy(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"user:a@example.com"}, p2.Members("roles/bigtable.reader"))
}

func TestRestoreDiscardsIncompatibleSnapshot(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, instance)

	require.NoError(t, e.handler.Backend.Restore(t.Context(), []byte(`{"version":999,"instances":{}}`)))

	_, err := e.iac.InstanceInfo(t.Context(), instance)
	assert.Equal(t, codes.NotFound, code(err))
}

func TestReset(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.seeded(t, "t", "cf")
	e.handler.Reset()

	_, err := e.iac.InstanceInfo(t.Context(), instance)
	assert.Equal(t, codes.NotFound, code(err))
}
