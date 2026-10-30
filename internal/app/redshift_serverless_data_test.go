package app

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	"github.com/aws/aws-sdk-go-v2/service/redshiftserverless"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const serverlessDwell = 150 * time.Millisecond

func TestRedshiftServerlessDataAPI(t *testing.T) {
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
	wireRedshiftServerlessData(byName)
	applyRedshiftServerless(byName["RedshiftServerless"], serverlessDwell)

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

	rs := redshiftserverless.NewFromConfig(cfg)
	rd := redshiftdata.NewFromConfig(cfg)

	_, err = rs.CreateNamespace(
		t.Context(),
		&redshiftserverless.CreateNamespaceInput{NamespaceName: aws.String("ns-data")},
	)
	require.NoError(t, err)

	created, err := rs.CreateWorkgroup(t.Context(), &redshiftserverless.CreateWorkgroupInput{
		NamespaceName: aws.String("ns-data"), WorkgroupName: aws.String("wg-data"),
	})
	require.NoError(t, err)
	assert.Equal(t, "CREATING", string(created.Workgroup.Status))

	execute := func(workgroup string) error {
		_, execErr := rd.ExecuteStatement(t.Context(), &redshiftdata.ExecuteStatementInput{
			WorkgroupName: aws.String(workgroup), Database: aws.String("dev"), Sql: aws.String("select 1"),
		})

		return execErr
	}

	var apiErr smithy.APIError

	require.ErrorAs(t, execute("wg-data"), &apiErr)
	assert.Equal(t, "ValidationException", apiErr.ErrorCode())

	require.Eventually(t, func() bool {
		got, gerr := rs.GetWorkgroup(
			t.Context(),
			&redshiftserverless.GetWorkgroupInput{WorkgroupName: aws.String("wg-data")},
		)

		return gerr == nil && string(got.Workgroup.Status) == "AVAILABLE"
	}, 10*time.Second, 25*time.Millisecond)

	require.NoError(t, execute("wg-data"))

	require.ErrorAs(t, execute("wg-missing"), &apiErr)
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
}
