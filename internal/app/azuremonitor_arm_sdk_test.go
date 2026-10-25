package app

import (
	"net/http/httptest"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/monitor/ingestion/azlogs"
	queryazlogs "github.com/Azure/azure-sdk-for-go/sdk/monitor/query/azlogs"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/monitor/armmonitor"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/operationalinsights/armoperationalinsights/v2"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/azuremonitor"
)

const (
	monSub = "00000000-0000-0000-0000-000000000000"
	monRG  = "mon-rg"
)

type monitorSDKStack struct {
	cred    redisSDKCredential
	monitor *httptest.Server
	handler *azuremonitor.Handler
	opts    *arm.ClientOptions
}

func newMonitorSDKStack(t *testing.T) *monitorSDKStack {
	t.Helper()

	armSvc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	armH, ok := armSvc.(*azurearm.Handler)
	require.True(t, ok)

	monH := azuremonitor.NewHandler(
		azuremonitor.NewBackend(azuremonitor.DefaultSettings()),
		azuremonitor.DefaultSettings(),
	)
	me := echo.New()
	me.Any("/*", monH.Handler())

	monSrv := httptest.NewTLSServer(me)
	t.Cleanup(monSrv.Close)

	monH.Backend.Configure(azuremonitor.Settings{Advertise: monSrv.URL})

	wireAzureMonitor(map[string]service.Registerable{"AzureARM": armH, "AzureMonitor": monH})

	ae := echo.New()
	ae.Any("/*", armH.Handler())

	armSrv := httptest.NewTLSServer(ae)
	t.Cleanup(armSrv.Close)

	return &monitorSDKStack{
		monitor: monSrv, handler: monH,
		opts: &arm.ClientOptions{
			Cloud: cloud.Configuration{
				Services: map[cloud.ServiceName]cloud.ServiceConfiguration{
					cloud.ResourceManager: {Endpoint: armSrv.URL, Audience: "https://management.core.windows.net/"},
				},
			},
			Transport: armSrv.Client(),
			Retry:     policy.RetryOptions{MaxRetries: -1},
		},
	}
}

func TestAzureMonitorARMAndDataPlaneSDK(t *testing.T) {
	t.Parallel()

	s := newMonitorSDKStack(t)
	ctx := t.Context()

	oi, err := armoperationalinsights.NewClientFactory(monSub, s.cred, s.opts)
	require.NoError(t, err)

	wsPoller, err := oi.NewWorkspacesClient().
		BeginCreateOrUpdate(ctx, monRG, "sdk-workspace", armoperationalinsights.Workspace{
			Location: new("westus"), Tags: map[string]*string{"env": new("test")},
			Properties: &armoperationalinsights.WorkspaceProperties{
				RetentionInDays: new(int32(60)),
				SKU: &armoperationalinsights.WorkspaceSKU{
					Name: new(armoperationalinsights.WorkspaceSKUNameEnumPerGB2018),
				},
			},
		}, nil)
	require.NoError(t, err)

	ws, err := wsPoller.PollUntilDone(ctx, pollOpts())
	require.NoError(t, err)
	require.NotEmpty(t, *ws.Properties.CustomerID)
	assert.EqualValues(t, 60, *ws.Properties.RetentionInDays)
	assert.Equal(t, armoperationalinsights.WorkspaceSKUNameEnumPerGB2018, *ws.Properties.SKU.Name)
	assert.Equal(t, "test", *ws.Tags["env"])

	got, err := oi.NewWorkspacesClient().Get(ctx, monRG, "sdk-workspace", nil)
	require.NoError(t, err)
	assert.Equal(t, *ws.Properties.CustomerID, *got.Properties.CustomerID)

	keys, err := oi.NewSharedKeysClient().GetSharedKeys(ctx, monRG, "sdk-workspace", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, *keys.PrimarySharedKey)

	regen, err := oi.NewSharedKeysClient().Regenerate(ctx, monRG, "sdk-workspace", nil)
	require.NoError(t, err)
	assert.NotEqual(t, *keys.PrimarySharedKey, *regen.PrimarySharedKey)

	updated, err := oi.NewWorkspacesClient().Update(ctx, monRG, "sdk-workspace", armoperationalinsights.WorkspacePatch{
		Tags: map[string]*string{"env": new("prod")},
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, "prod", *updated.Tags["env"])

	tblPoller, err := oi.NewTablesClient().
		BeginCreateOrUpdate(ctx, monRG, "sdk-workspace", "Orders_CL", armoperationalinsights.Table{
			Properties: &armoperationalinsights.TableProperties{
				RetentionInDays: new(int32(30)),
				Schema: &armoperationalinsights.Schema{
					Name: new("Orders_CL"),
					Columns: []*armoperationalinsights.Column{
						{Name: new("TimeGenerated"), Type: new(armoperationalinsights.ColumnTypeEnumDateTime)},
						{Name: new("OrderId"), Type: new(armoperationalinsights.ColumnTypeEnumString)},
						{Name: new("Amount"), Type: new(armoperationalinsights.ColumnTypeEnumReal)},
						{Name: new("Lines"), Type: new(armoperationalinsights.ColumnTypeEnumInt)},
					},
				},
			},
		}, nil)
	require.NoError(t, err)

	tbl, err := tblPoller.PollUntilDone(ctx, pollOpts())
	require.NoError(t, err)
	assert.Equal(t, "Orders_CL", *tbl.Name)
	assert.Len(t, tbl.Properties.Schema.Columns, 4)

	pager := oi.NewTablesClient().NewListByWorkspacePager(monRG, "sdk-workspace", nil)
	names := []string{}

	for pager.More() {
		page, perr := pager.NextPage(ctx)
		require.NoError(t, perr)

		for _, tb := range page.Value {
			names = append(names, *tb.Name)
		}
	}

	assert.Contains(t, names, "Orders_CL")
	assert.Contains(t, names, "Heartbeat")

	mon, err := armmonitor.NewClientFactory(monSub, s.cred, s.opts)
	require.NoError(t, err)

	dce, err := mon.NewDataCollectionEndpointsClient().Create(ctx, monRG, "sdk-dce",
		armmonitor.DataCollectionEndpointResource{Location: new("westus")}, nil)
	require.NoError(t, err)
	require.NotNil(t, dce.Properties.LogsIngestion)
	endpoint := *dce.Properties.LogsIngestion.Endpoint
	assert.Equal(t, s.monitor.URL, endpoint)
	assert.NotEmpty(t, *dce.Properties.ImmutableID)

	dcr, err := mon.NewDataCollectionRulesClient().Create(ctx, monRG, "sdk-dcr",
		armmonitor.DataCollectionRuleResource{
			Location: new("westus"), Kind: new(armmonitor.KnownDataCollectionRuleResourceKind("Direct")),
			Properties: &armmonitor.DataCollectionRuleResourceProperties{
				DataCollectionEndpointID: dce.ID,
				StreamDeclarations: map[string]*armmonitor.StreamDeclaration{"Custom-Orders": {
					Columns: []*armmonitor.ColumnDefinition{
						{Name: new("Time"), Type: new(armmonitor.KnownColumnDefinitionTypeDatetime)},
						{Name: new("OrderId"), Type: new(armmonitor.KnownColumnDefinitionTypeString)},
						{Name: new("Amount"), Type: new(armmonitor.KnownColumnDefinitionTypeReal)},
						{Name: new("Lines"), Type: new(armmonitor.KnownColumnDefinitionTypeInt)},
					},
				}},
				Destinations: &armmonitor.DataCollectionRuleDestinations{
					LogAnalytics: []*armmonitor.LogAnalyticsDestination{
						{Name: new("la"), WorkspaceResourceID: ws.ID},
					},
				},
				DataFlows: []*armmonitor.DataFlow{
					{
						Streams: []*armmonitor.KnownDataFlowStreams{
							new(armmonitor.KnownDataFlowStreams("Custom-Orders")),
						},
						Destinations: []*string{new("la")},
						TransformKql: new("source | extend TimeGenerated = Time | project-away Time"),
						OutputStream: new("Custom-Orders_CL"),
					},
				},
			},
		}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, *dcr.Properties.ImmutableID)
	assert.Equal(t, *ws.Properties.CustomerID, *dcr.Properties.Destinations.LogAnalytics[0].WorkspaceID)
	assert.Equal(t, endpoint, *dcr.Properties.Endpoints.LogsIngestion)

	ing, err := azlogs.NewClient(endpoint, s.cred, &azlogs.ClientOptions{
		Transport: s.monitor.Client(), Retry: policy.RetryOptions{MaxRetries: -1},
	})
	require.NoError(t, err)

	_, err = ing.Upload(ctx, *dcr.Properties.ImmutableID, "Custom-Orders", []byte(
		`[{"Time":"2024-05-01T10:00:00Z","OrderId":"o-1","Amount":12.5,"Lines":2},`+
			`{"Time":"2024-05-01T11:00:00Z","OrderId":"o-2","Amount":7.5,"Lines":1}]`), nil)
	require.NoError(t, err)

	qc, err := queryazlogs.NewClient(s.cred, &queryazlogs.ClientOptions{
		Transport: s.monitor.Client(),
		Retry:     policy.RetryOptions{MaxRetries: -1},
		Cloud: cloud.Configuration{
			Services: map[cloud.ServiceName]cloud.ServiceConfiguration{ //nolint:exhaustive // only the query service is needed
				queryazlogs.ServiceName: {Endpoint: s.monitor.URL, Audience: "https://api.loganalytics.io"},
			},
		},
	})
	require.NoError(t, err)

	res, err := qc.QueryWorkspace(ctx, *ws.Properties.CustomerID, queryazlogs.QueryBody{
		Query: new("Orders_CL | summarize total = sum(Amount), lines = sum(Lines), n = count()"),
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, queryazlogs.Row{20.0, 3.0, 2.0}, res.Tables[0].Rows[0])

	list := mon.NewDataCollectionRulesClient().NewListByResourceGroupPager(monRG, nil)
	count := 0

	for list.More() {
		page, perr := list.NextPage(ctx)
		require.NoError(t, perr)

		count += len(page.Value)
	}

	assert.Equal(t, 1, count)

	patched, err := mon.NewDataCollectionRulesClient().Update(ctx, monRG, "sdk-dcr",
		armmonitor.ResourceForUpdate{Tags: map[string]*string{"k": new("v")}}, nil)
	require.NoError(t, err)
	assert.Equal(t, "v", *patched.Tags["k"])

	_, err = mon.NewDataCollectionRulesClient().Delete(ctx, monRG, "sdk-dcr", nil)
	require.NoError(t, err)

	_, err = mon.NewDataCollectionRulesClient().Get(ctx, monRG, "sdk-dcr", nil)
	require.Error(t, err)

	delPoller, err := oi.NewWorkspacesClient().BeginDelete(ctx, monRG, "sdk-workspace", nil)
	require.NoError(t, err)
	_, err = delPoller.PollUntilDone(ctx, pollOpts())
	require.NoError(t, err)

	_, err = oi.NewWorkspacesClient().Get(ctx, monRG, "sdk-workspace", nil)
	require.Error(t, err)
}
