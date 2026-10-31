package app

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	redshiftbackend "github.com/blackbirdworks/gopherstack/services/redshift"
	redshiftdatabackend "github.com/blackbirdworks/gopherstack/services/redshiftdata"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

func TestRedshiftWorkgroupEngineAndS3Wiring(t *testing.T) {
	t.Parallel()

	log := buildLogger("")
	cli := CLI{AccountID: "000000000000", Region: "us-east-1"}
	cli.portAlloc = setupPortAllocatorWithReservations(t.Context(), log, cli)
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServicesT(t, &service.AppContext{
		Logger: log, Config: &cli, JanitorCtx: t.Context(), PortAlloc: cli.portAlloc,
	})
	require.NoError(t, err)

	byName := serviceByName(services)
	wireRedshiftClusterEngines(byName)

	slH, ok := byName["RedshiftServerless"].(*redshiftbackend.ServerlessHandler)
	require.True(t, ok)

	rt := &engineWiringRuntime{}
	slH.Backend.EnableEngine(redshiftbackend.EngineConfig{
		Runtime: rt, StartTimeout: time.Minute,
		Probe: func(context.Context, redshiftbackend.EngineLogin) error { return nil },
	})
	t.Cleanup(slH.Backend.Close)

	_, err = slH.Backend.CreateNamespace(redshiftbackend.CreateNamespaceParams{
		NamespaceName: "ns-wire", AdminUserPassword: "Passw0rdXyz",
	})
	require.NoError(t, err)

	_, err = slH.Backend.CreateWorkgroup("wg-wire", "ns-wire", redshiftbackend.WorkgroupParams{}, nil)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		st, _ := slH.Backend.WorkgroupStatus("wg-wire")

		return st == "AVAILABLE"
	}, 10*time.Second, 5*time.Millisecond)

	dataH, ok := byName["RedshiftData"].(*redshiftdatabackend.Handler)
	require.True(t, ok)

	dataBk, ok := dataH.Backend.(*redshiftdatabackend.InMemoryBackend)
	require.True(t, ok)

	exec := &engineWiringExecutor{}
	dataBk.SetSQLExecutor(exec)

	stmt, err := dataBk.ExecuteStatement(t.Context(), "SELECT 1", "", "wg-wire", "dev", "", "", "", false, "", nil, "")
	require.NoError(t, err)
	assert.Equal(t, "FINISHED", stmt.Status)
	require.Len(t, exec.logins, 1)
	assert.Equal(t, "Passw0rdXyz", exec.logins[0].Password)
}

func TestRedshiftObjectStoreRoundTrip(t *testing.T) {
	t.Parallel()

	bk := s3backend.NewInMemoryBackend(nil)
	_, err := bk.CreateBucket(t.Context(), &s3sdk.CreateBucketInput{Bucket: aws.String("lake")})
	require.NoError(t, err)

	store := redshiftObjectStore{s3: bk}

	require.NoError(t, store.PutObject(t.Context(), "lake", "out/a_0000", []byte("one")))
	require.NoError(t, store.PutObject(t.Context(), "lake", "out/a_0001", []byte("two")))
	require.NoError(t, store.PutObject(t.Context(), "lake", "other", []byte("x")))

	keys, err := store.ListKeys(t.Context(), "lake", "out/a_")
	require.NoError(t, err)
	assert.Equal(t, []string{"out/a_0000", "out/a_0001"}, keys)

	got, err := store.GetObject(t.Context(), "lake", "out/a_0001")
	require.NoError(t, err)
	assert.Equal(t, "two", string(got))

	require.NoError(t, store.DeleteObject(t.Context(), "lake", "out/a_0000"))

	keys, err = store.ListKeys(t.Context(), "lake", "out/")
	require.NoError(t, err)
	assert.Equal(t, []string{"out/a_0001"}, keys)

	_, err = store.GetObject(t.Context(), "lake", "missing")
	require.Error(t, err)

	_, err = store.ListKeys(t.Context(), "nobucket", "")
	require.Error(t, err)
}
