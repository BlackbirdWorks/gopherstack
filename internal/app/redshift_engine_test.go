package app

import (
	"context"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/redshift"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	rdtypes "github.com/aws/aws-sdk-go-v2/service/redshiftdata/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	redshiftbackend "github.com/blackbirdworks/gopherstack/services/redshift"
	redshiftdatabackend "github.com/blackbirdworks/gopherstack/services/redshiftdata"
)

type engineWiringRuntime struct {
	mu      sync.Mutex
	created int
}

func (r *engineWiringRuntime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.created++

	return "ctr-" + spec.Name, nil
}

func (*engineWiringRuntime) StopAndRemove(context.Context, string) error  { return nil }
func (*engineWiringRuntime) StopContainer(context.Context, string) error  { return nil }
func (*engineWiringRuntime) StartContainer(context.Context, string) error { return nil }

type engineWiringExecutor struct {
	logins []redshiftdatabackend.EngineLogin
	mu     sync.Mutex
}

func (e *engineWiringExecutor) ExecuteSQL(
	_ context.Context, l redshiftdatabackend.EngineLogin, _ []string, _ []redshiftdatabackend.SQLParameter, _ bool,
) []redshiftdatabackend.SQLOutcome {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.logins = append(e.logins, l)
	v := int64(42)

	return []redshiftdatabackend.SQLOutcome{{Result: &redshiftdatabackend.ResultSet{
		Columns: []redshiftdatabackend.ColumnMetadata{{Name: "answer", Label: "answer", TypeName: "int4"}},
		Rows:    [][]redshiftdatabackend.Field{{{LongValue: &v}}},
	}}}
}

func TestRedshiftEngineWiring(t *testing.T) {
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

	rsH, ok := byName["Redshift"].(*redshiftbackend.Handler)
	require.True(t, ok)

	rt := &engineWiringRuntime{}
	home, ok := rsH.Backend.(*redshiftbackend.InMemoryBackend)
	require.True(t, ok)

	home.EnableEngine(redshiftbackend.EngineConfig{
		Runtime: rt, StartTimeout: time.Minute,
		Probe: func(context.Context, redshiftbackend.EngineLogin) error { return nil },
	})
	t.Cleanup(home.Close)

	dataH, ok := byName["RedshiftData"].(*redshiftdatabackend.Handler)
	require.True(t, ok)

	dataBk, ok := dataH.Backend.(*redshiftdatabackend.InMemoryBackend)
	require.True(t, ok)

	exec := &engineWiringExecutor{}
	dataBk.SetSQLExecutor(exec)

	e := buildEchoServer(t.Context(), log, nil, services, cli)
	require.NoError(t, setupChaosAndRegistry(e, log, &cli, services))

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awscfg.WithBaseEndpoint(srv.URL),
	)
	require.NoError(t, err)

	rs := redshift.NewFromConfig(cfg)
	rd := redshiftdata.NewFromConfig(cfg)

	_, err = rs.CreateCluster(t.Context(), &redshift.CreateClusterInput{
		ClusterIdentifier: aws.String("wired"), NodeType: aws.String("dc2.large"),
		MasterUsername: aws.String("admin"), MasterUserPassword: aws.String("Passw0rdXyz"), DBName: aws.String("dev"),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		got, gerr := rs.DescribeClusters(
			t.Context(),
			&redshift.DescribeClustersInput{ClusterIdentifier: aws.String("wired")},
		)

		return gerr == nil && len(got.Clusters) == 1 && aws.ToString(got.Clusters[0].ClusterStatus) == "available"
	}, 10*time.Second, 10*time.Millisecond)

	got, err := rs.DescribeClusters(
		t.Context(),
		&redshift.DescribeClustersInput{ClusterIdentifier: aws.String("wired")},
	)
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1", aws.ToString(got.Clusters[0].Endpoint.Address))

	out, err := rd.ExecuteStatement(t.Context(), &redshiftdata.ExecuteStatementInput{
		ClusterIdentifier: aws.String("wired"), Database: aws.String("dev"), Sql: aws.String("select 42 as answer"),
	})
	require.NoError(t, err)

	res, err := rd.GetStatementResult(t.Context(), &redshiftdata.GetStatementResultInput{Id: out.Id})
	require.NoError(t, err)
	require.Len(t, res.Records, 1)
	assert.Equal(t, int64(42), res.Records[0][0].(*rdtypes.FieldMemberLongValue).Value)

	exec.mu.Lock()
	defer exec.mu.Unlock()

	require.Len(t, exec.logins, 1)
	assert.Equal(t, "admin", exec.logins[0].User)
	assert.Equal(t, "Passw0rdXyz", exec.logins[0].Password)
}
