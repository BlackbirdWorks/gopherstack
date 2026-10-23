package app

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/sql/armsql"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/azuresql"
)

const (
	sqlSDKRG   = "sdk-rg"
	sqlSDKPass = "Passw0rd!sdk"
)

type sqlSDKStack struct {
	backend *azuresql.InMemoryBackend
	factory *armsql.ClientFactory
}

func newSQLSDKStack(t *testing.T, engine bool) *sqlSDKStack {
	t.Helper()

	armSvc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	armH, ok := armSvc.(*azurearm.Handler)
	require.True(t, ok)

	var backend *azuresql.InMemoryBackend

	if engine {
		sqlSvc, initErr := (&azuresql.Provider{}).Init(&service.AppContext{Logger: slog.Default()})
		require.NoError(t, initErr)

		sqlH, hOk := sqlSvc.(*azuresql.Handler)
		require.True(t, hOk)

		backend = sqlH.Backend

		t.Cleanup(func() { sqlH.Shutdown(context.WithoutCancel(t.Context())) })
	} else {
		backend = azuresql.NewInMemoryBackend()
		t.Cleanup(backend.Close)
	}

	armH.Registry.SetSQL(&azureSQLAdapter{b: backend})

	e := echo.New()
	e.Any("/*", armH.Handler())

	srv := httptest.NewTLSServer(e)
	t.Cleanup(srv.Close)

	opts := &arm.ClientOptions{
		Cloud: cloud.Configuration{Services: map[cloud.ServiceName]cloud.ServiceConfiguration{
			cloud.ResourceManager: {Endpoint: srv.URL, Audience: "https://management.core.windows.net/"},
		}},
		Transport: srv.Client(),
		Retry:     policy.RetryOptions{MaxRetries: -1},
	}

	f, err := armsql.NewClientFactory(redisSDKSub, redisSDKCredential{}, opts)
	require.NoError(t, err)

	return &sqlSDKStack{backend: backend, factory: f}
}

func (s *sqlSDKStack) createServer(t *testing.T, name string) *armsql.Server {
	t.Helper()

	poller, err := s.factory.NewServersClient().BeginCreateOrUpdate(t.Context(), sqlSDKRG, name, armsql.Server{
		Location: new("local"),
		Properties: &armsql.ServerProperties{
			AdministratorLogin: new("sqladmin"), AdministratorLoginPassword: new(sqlSDKPass),
			MinimalTLSVersion: new("1.2"),
		},
	}, nil)
	require.NoError(t, err)

	res, err := poller.PollUntilDone(t.Context(), pollOpts())
	require.NoError(t, err)

	return &res.Server
}

func (s *sqlSDKStack) createDatabase(t *testing.T, server, name string) *armsql.Database {
	t.Helper()

	poller, err := s.factory.NewDatabasesClient().
		BeginCreateOrUpdate(t.Context(), sqlSDKRG, server, name, armsql.Database{
			Location: new("local"),
			SKU:      &armsql.SKU{Name: new("S0"), Tier: new("Standard")},
			Properties: &armsql.DatabaseProperties{
				Collation:    new("Latin1_General_CI_AS"),
				MaxSizeBytes: new(int64(1 << 30)),
			},
		}, nil)
	require.NoError(t, err)

	res, err := poller.PollUntilDone(t.Context(), pollOpts())
	require.NoError(t, err)

	return &res.Database
}

// TestAzureSQLARMSDKStateOnly drives Microsoft.Sql through armsql with no container runtime.
func TestAzureSQLARMSDKStateOnly(t *testing.T) {
	t.Parallel()

	s := newSQLSDKStack(t, false)
	ctx := t.Context()

	srv := s.createServer(t, "sdkstate")
	assert.Equal(t, "sdkstate.database.windows.net", *srv.Properties.FullyQualifiedDomainName)
	assert.Equal(t, "12.0", *srv.Properties.Version)
	assert.Equal(t, "Ready", *srv.Properties.State)

	db := s.createDatabase(t, "sdkstate", "app")
	assert.Equal(t, "Latin1_General_CI_AS", *db.Properties.Collation)
	assert.Equal(t, int64(1<<30), *db.Properties.MaxSizeBytes)
	assert.Equal(t, "S0", *db.SKU.Name)

	_, err := s.factory.NewFirewallRulesClient().CreateOrUpdate(ctx, sqlSDKRG, "sdkstate", "AllowAllWindowsAzureIps",
		armsql.FirewallRule{Properties: &armsql.ServerFirewallRuleProperties{
			StartIPAddress: new("0.0.0.0"), EndIPAddress: new("0.0.0.0"),
		}}, nil)
	require.NoError(t, err)

	adminPoller, err := s.factory.NewServerAzureADAdministratorsClient().BeginCreateOrUpdate(ctx, sqlSDKRG, "sdkstate",
		armsql.AdministratorNameActiveDirectory, armsql.ServerAzureADAdministrator{
			Properties: &armsql.AdministratorProperties{
				AdministratorType: new(armsql.AdministratorTypeActiveDirectory), Login: new("dba@example.com"),
				Sid: new("11111111-1111-1111-1111-111111111111"), TenantID: new("22222222-2222-2222-2222-222222222222"),
			},
		}, nil)
	require.NoError(t, err)
	_, err = adminPoller.PollUntilDone(ctx, pollOpts())
	require.NoError(t, err)

	poolPoller, err := s.factory.NewElasticPoolsClient().BeginCreateOrUpdate(ctx, sqlSDKRG, "sdkstate", "pool1",
		armsql.ElasticPool{
			Location: new("local"),
			SKU:      &armsql.SKU{Name: new("GP_Gen5"), Tier: new("GeneralPurpose")},
		}, nil)
	require.NoError(t, err)
	_, err = poolPoller.PollUntilDone(ctx, pollOpts())
	require.NoError(t, err)

	tde, err := s.factory.NewTransparentDataEncryptionsClient().CreateOrUpdate(ctx, sqlSDKRG, "sdkstate", "app",
		armsql.TransparentDataEncryptionNameCurrent, armsql.LogicalDatabaseTransparentDataEncryption{
			Properties: &armsql.TransparentDataEncryptionProperties{
				State: new(armsql.TransparentDataEncryptionStateDisabled),
			},
		}, nil)
	require.NoError(t, err)
	assert.Equal(t, armsql.TransparentDataEncryptionStateDisabled, *tde.Properties.State)

	var names []string

	pager := s.factory.NewDatabasesClient().NewListByServerPager(sqlSDKRG, "sdkstate", nil)
	for pager.More() {
		page, pageErr := pager.NextPage(ctx)
		require.NoError(t, pageErr)

		for _, d := range page.Value {
			names = append(names, *d.Name)
		}
	}

	assert.Equal(t, []string{"app"}, names)

	delDB, err := s.factory.NewDatabasesClient().BeginDelete(ctx, sqlSDKRG, "sdkstate", "app", nil)
	require.NoError(t, err)
	_, err = delDB.PollUntilDone(ctx, pollOpts())
	require.NoError(t, err)

	delSrv, err := s.factory.NewServersClient().BeginDelete(ctx, sqlSDKRG, "sdkstate", nil)
	require.NoError(t, err)
	_, err = delSrv.PollUntilDone(ctx, pollOpts())
	require.NoError(t, err)

	_, err = s.factory.NewServersClient().Get(ctx, sqlSDKRG, "sdkstate", nil)
	require.Error(t, err)
}

// TestAzureSQLARMSDKEngine creates a server and database through armsql against a real SQL Server container,
// logs in as the admin through sqlcmd inside it, and checks the container is gone after the delete. It is
// skipped without a container runtime.
func TestAzureSQLARMSDKEngine(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("docker engine test skipped in short mode")
	}

	lr := gcpserve.NewLazyRuntime(slog.Default(), "azuresql.test.runtime")
	t.Cleanup(func() { _ = lr.Close() })

	if _, err := lr.Get(); err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	s := newSQLSDKStack(t, true)
	ctx := t.Context()

	srv := s.createServer(t, "sdkengine")
	s.createDatabase(t, "sdkengine", "app")

	ep, ok := s.backend.Endpoint(strings.ToLower("/subscriptions/" + redisSDKSub + "/resourceGroups/" + sqlSDKRG +
		"/providers/Microsoft.Sql/servers/sdkengine"))
	require.True(t, ok)
	require.NotZero(t, ep.Port, "the server runs in a container")
	assert.Equal(t, "sdkengine.database.windows.net", *srv.Properties.FullyQualifiedDomainName)

	id := containerByPort(t, ep.Port)
	out := sqlcmdIn(t, id, "sqladmin", sqlSDKPass, "app", "SELECT DB_NAME()")
	assert.Equal(t, "app", out)

	out = sqlcmdIn(
		t,
		id,
		"sqladmin",
		sqlSDKPass,
		"app",
		"SELECT CAST(DATABASEPROPERTYEX('app','Collation') AS varchar(64))",
	)
	assert.Equal(t, "Latin1_General_CI_AS", out)

	names, err := s.backend.Databases("/subscriptions/" + redisSDKSub + "/resourceGroups/" + sqlSDKRG +
		"/providers/Microsoft.Sql/servers/sdkengine")
	require.NoError(t, err)
	assert.Equal(t, []string{"app"}, names)

	delSrv, err := s.factory.NewServersClient().BeginDelete(ctx, sqlSDKRG, "sdkengine", nil)
	require.NoError(t, err)
	_, err = delSrv.PollUntilDone(ctx, pollOpts())
	require.NoError(t, err)

	gone, err := exec.CommandContext(ctx, "docker", "ps", "-aq", "--filter", "id="+id).Output()
	require.NoError(t, err)
	assert.Empty(t, strings.TrimSpace(string(gone)), "deleting the server removes its container")
}

func containerByPort(t *testing.T, port int) string {
	t.Helper()

	out, err := exec.CommandContext(t.Context(), "docker", "ps", "-q", "--filter", "publish="+strconv.Itoa(port)).
		Output()
	require.NoError(t, err)

	id := strings.TrimSpace(string(out))
	require.NotEmpty(t, id)

	return id
}

func sqlcmdIn(t *testing.T, id, login, password, db, query string) string {
	t.Helper()

	out, err := exec.CommandContext(t.Context(), "docker", "exec", id, "/opt/mssql-tools18/bin/sqlcmd", "-S", "localhost",
		"-U", login, "-P", password, "-C", "-b", "-h", "-1", "-W", "-d", db, "-Q", "SET NOCOUNT ON; "+query).
		CombinedOutput()
	require.NoError(t, err, string(out))

	return strings.TrimSpace(string(out))
}
