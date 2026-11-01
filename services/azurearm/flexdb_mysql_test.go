package azurearm_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/mysql/armmysqlflexibleservers/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const myNS = "Microsoft.DBforMySQL"

func (s *flexStack) myFactory(t *testing.T) *armmysqlflexibleservers.ClientFactory {
	t.Helper()

	f, err := armmysqlflexibleservers.NewClientFactory(aciSub, aciCred{}, s.opts)
	require.NoError(t, err)

	return f
}

func mySrv() armmysqlflexibleservers.Server {
	return armmysqlflexibleservers.Server{
		Location: new(flexLoc),
		Tags:     map[string]*string{"env": new("test")},
		SKU: &armmysqlflexibleservers.MySQLServerSKU{
			Name: new("Standard_B1ms"), Tier: new(armmysqlflexibleservers.ServerSKUTierBurstable),
		},
		Properties: &armmysqlflexibleservers.ServerProperties{
			AdministratorLogin:         new("myadmin"),
			AdministratorLoginPassword: new(flexPassword),
			Version:                    new(armmysqlflexibleservers.ServerVersionEight021),
			Storage:                    &armmysqlflexibleservers.Storage{StorageSizeGB: new(int32(20))},
			Backup:                     &armmysqlflexibleservers.Backup{BackupRetentionDays: new(int32(1))},
		},
	}
}

func (s *flexStack) myCreate(t *testing.T, name string) armmysqlflexibleservers.Server {
	t.Helper()

	poller, err := s.myFactory(t).NewServersClient().BeginCreate(t.Context(), flexRG, name, mySrv(), nil)
	require.NoError(t, err)

	res, err := poller.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	return res.Server
}

func (s *flexStack) myCreateGP(t *testing.T, name string) armmysqlflexibleservers.Server {
	t.Helper()

	srv := mySrv()
	srv.SKU = &armmysqlflexibleservers.MySQLServerSKU{
		Name: new("Standard_D2ds_v4"), Tier: new(armmysqlflexibleservers.ServerSKUTierGeneralPurpose),
	}

	poller, err := s.myFactory(t).NewServersClient().BeginCreate(t.Context(), flexRG, name, srv, nil)
	require.NoError(t, err)

	res, err := poller.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	return res.Server
}

func TestMySQLFlexServerCRUD(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	f := s.myFactory(t)
	srv := s.myCreate(t, "my-crud")

	require.Equal(t, "my-crud.mysql.database.azure.com", *srv.Properties.FullyQualifiedDomainName)
	assert.Equal(t, armmysqlflexibleservers.ServerStateReady, *srv.Properties.State)
	assert.Equal(t, "8.0.21", string(*srv.Properties.Version))
	assert.Equal(t, int32(1), *srv.Properties.Backup.BackupRetentionDays)
	assert.Equal(t, armmysqlflexibleservers.EnableStatusEnumEnabled, *srv.Properties.Storage.AutoGrow)
	assert.Equal(t, "Microsoft.DBforMySQL/flexibleServers", *srv.Type)

	status, raw := s.raw(t, http.MethodGet, flexServerPath(myNS, "my-crud"), "", flexAPIMy)
	require.Equal(t, http.StatusOK, status)

	enc, err := json.Marshal(raw)
	require.NoError(t, err)
	assert.NotContains(t, string(enc), flexPassword)
	assert.NotContains(t, string(enc), "administratorLoginPassword")

	up, err := f.NewServersClient().BeginUpdate(t.Context(), flexRG, "my-crud", armmysqlflexibleservers.ServerForUpdate{
		Tags: map[string]*string{"env": new("prod")},
		Properties: &armmysqlflexibleservers.ServerPropertiesForUpdate{
			Storage: &armmysqlflexibleservers.Storage{StorageSizeGB: new(int32(40))},
		},
	}, nil)
	require.NoError(t, err)

	upRes, err := up.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)
	assert.Equal(t, "prod", *upRes.Tags["env"])
	assert.Equal(t, int32(40), *upRes.Properties.Storage.StorageSizeGB)

	shrink := s.rawNoFail(t, http.MethodPatch, flexServerPath(myNS, "my-crud"),
		`{"properties":{"storage":{"storageSizeGB":20}}}`, flexAPIMy)
	assert.Equal(t, http.StatusBadRequest, shrink)

	pager := f.NewServersClient().NewListByResourceGroupPager(flexRG, nil)
	page, err := pager.NextPage(t.Context())
	require.NoError(t, err)
	require.Len(t, page.Value, 1)

	del, err := f.NewServersClient().BeginDelete(t.Context(), flexRG, "my-crud", nil)
	require.NoError(t, err)
	_, err = del.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	status, body := s.raw(t, http.MethodGet, flexServerPath(myNS, "my-crud"), "", flexAPIMy)
	assert.Equal(t, http.StatusNotFound, status)
	assert.Equal(t, "ResourceNotFound", flexErrCode(body))
}

func (s *flexStack) rawNoFail(t *testing.T, method, path, body, api string) int {
	t.Helper()

	status, _ := s.raw(t, method, path, body, api)

	return status
}

const myLoginPW = `"administratorLogin":"a1","administratorLoginPassword":"` + flexPassword + `"`

func myReq(props string) string {
	return `{"location":"x","sku":{"name":"Standard_B1ms","tier":"Burstable"},"properties":{` + props + `}}`
}

func TestMySQLFlexServerValidation(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	login := func(l string) string {
		return `"administratorLogin":"` + l + `","administratorLoginPassword":"` + flexPassword + `"`
	}
	geo := `{"location":"x","properties":{"createMode":"GeoRestore","sourceServerResourceId":"/subscriptions/` +
		aciSub + `/resourceGroups/` + flexRG + `/providers/` + myNS + `/flexibleServers/myv-0"}}`

	tests := []struct {
		name   string
		body   string
		status int
	}{
		{"valid 5.7", myReq(myLoginPW + `,"version":"5.7"`), http.StatusCreated},
		{"valid 8.4", myReq(myLoginPW + `,"version":"8.4"`), http.StatusCreated},
		{"bad version", myReq(myLoginPW + `,"version":"8.1"`), http.StatusBadRequest},
		{"storage below min", myReq(myLoginPW + `,"storage":{"storageSizeGB":19}`), http.StatusBadRequest},
		{"storage above max", myReq(myLoginPW + `,"storage":{"storageSizeGB":16385}`), http.StatusBadRequest},
		{"storage valid odd", myReq(myLoginPW + `,"storage":{"storageSizeGB":33}`), http.StatusCreated},
		{"retention zero", myReq(myLoginPW + `,"backup":{"backupRetentionDays":0}`), http.StatusBadRequest},
		{"retention 36", myReq(myLoginPW + `,"backup":{"backupRetentionDays":36}`), http.StatusBadRequest},
		{"retention 35", myReq(myLoginPW + `,"backup":{"backupRetentionDays":35}`), http.StatusCreated},
		{"geo invalid", myReq(myLoginPW + `,"backup":{"geoRedundantBackup":"Maybe"}`), http.StatusBadRequest},
		{"reserved root", myReq(login("root")), http.StatusBadRequest},
		{"long login", myReq(login(strings.Repeat("a", 33))), http.StatusBadRequest},
		{"dash login", myReq(login("a-b")), http.StatusBadRequest},
		{
			"weak password",
			myReq(`"administratorLogin":"a1","administratorLoginPassword":"alllowercase1"`),
			http.StatusBadRequest,
		},
		{"burstable zr", myReq(myLoginPW + `,"highAvailability":{"mode":"ZoneRedundant"}`), http.StatusBadRequest},
		{"bad zone", myReq(myLoginPW + `,"availabilityZone":"7"`), http.StatusBadRequest},
		{"bad access", myReq(myLoginPW + `,"network":{"publicNetworkAccess":"Sometimes"}`), http.StatusBadRequest},
		{"geo restore without source", geo, http.StatusBadRequest},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			name := "myv-" + string(rune('a'+i))
			status, body := s.raw(t, http.MethodPut, flexServerPath(myNS, name), tt.body, flexAPIMy)
			assert.Equal(t, tt.status, status, body)

			if tt.status == http.StatusBadRequest {
				assert.Equal(t, "InvalidParameterValue", flexErrCode(body))
			}
		})
	}
}

func TestMySQLFlexSKUCatalog(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)

	tests := []struct {
		sku, tier string
		status    int
	}{
		{"Standard_B1s", "Burstable", http.StatusCreated},
		{"Standard_D2ads_v5", "GeneralPurpose", http.StatusCreated},
		{"Standard_D96ads_v5", "GeneralPurpose", http.StatusCreated},
		{"Standard_E20ds_v4", "MemoryOptimized", http.StatusCreated},
		{"Standard_D2ds_v5", "GeneralPurpose", http.StatusBadRequest},
		{"Standard_B1ms", "MemoryOptimized", http.StatusBadRequest},
		{"Standard_B1s", "GeneralPurpose", http.StatusBadRequest},
	}

	for i, tt := range tests {
		t.Run(tt.sku+"/"+tt.tier, func(t *testing.T) {
			t.Parallel()

			body := `{"location":"x","sku":{"name":"` + tt.sku + `","tier":"` + tt.tier + `"},"properties":{` + myLoginPW + `}}`
			status, _ := s.raw(t, http.MethodPut, flexServerPath(myNS, "mysku-"+string(rune('a'+i))), body, flexAPIMy)
			assert.Equal(t, tt.status, status)
		})
	}
}

func myPoll[T any](t *testing.T, poller *runtime.Poller[T], err error) T {
	t.Helper()

	require.NoError(t, err)

	res, err := poller.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	return res
}

func TestMySQLFlexFirewall(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	s.myCreate(t, "my-fw")

	fw := s.myFactory(t).NewFirewallRulesClient()
	ctx := t.Context()
	start, end := "0.0.0.0", "0.0.0.0"
	rule := "AllowAllAzureServicesAndResourcesWithinAzureIps_x"

	p, err := fw.BeginCreateOrUpdate(ctx, flexRG, "my-fw", rule,
		armmysqlflexibleservers.FirewallRule{Properties: &armmysqlflexibleservers.FirewallRuleProperties{
			StartIPAddress: &start, EndIPAddress: &end,
		}}, nil)
	res := myPoll(t, p, err)
	assert.Equal(t, "0.0.0.0", *res.Properties.EndIPAddress)

	got, err := fw.Get(ctx, flexRG, "my-fw", rule, nil)
	require.NoError(t, err)
	assert.Equal(t, "0.0.0.0", *got.Properties.StartIPAddress)

	del, err := fw.BeginDelete(ctx, flexRG, "my-fw", rule, nil)
	myPoll(t, del, err)

	page, err := fw.NewListByServerPager(flexRG, "my-fw", nil).NextPage(ctx)
	require.NoError(t, err)
	assert.Empty(t, page.Value)

	status, _ := s.raw(t, http.MethodPut, flexServerPath(myNS, "my-fw")+"/firewallRules/bad",
		`{"properties":{"startIpAddress":"2.2.2.2","endIpAddress":"1.1.1.1"}}`, flexAPIMy)
	assert.Equal(t, http.StatusBadRequest, status)
}

func TestMySQLFlexDatabases(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	s.myCreate(t, "my-db")

	dbs := s.myFactory(t).NewDatabasesClient()
	ctx := t.Context()

	p, err := dbs.BeginCreateOrUpdate(ctx, flexRG, "my-db", "appdb", armmysqlflexibleservers.Database{
		Properties: &armmysqlflexibleservers.DatabaseProperties{
			Charset: new("latin1"), Collation: new("latin1_swedish_ci"),
		},
	}, nil)
	res := myPoll(t, p, err)
	assert.Equal(t, "latin1", *res.Properties.Charset)

	def, err := dbs.BeginCreateOrUpdate(ctx, flexRG, "my-db", "plain", armmysqlflexibleservers.Database{}, nil)
	defRes := myPoll(t, def, err)
	assert.Equal(t, "utf8mb4", *defRes.Properties.Charset)
	assert.Equal(t, "utf8mb4_0900_ai_ci", *defRes.Properties.Collation)

	status, _ := s.raw(t, http.MethodPut, flexServerPath(myNS, "my-db")+"/databases/appdb",
		`{"properties":{"charset":"utf8mb4","collation":"utf8mb4_bin"}}`, flexAPIMy)
	assert.Equal(t, http.StatusBadRequest, status)

	_, err = dbs.BeginCreateOrUpdate(ctx, flexRG, "my-db", "mysql", armmysqlflexibleservers.Database{}, nil)
	require.Error(t, err)

	var names []string

	pager := dbs.NewListByServerPager(flexRG, "my-db", nil)
	for pager.More() {
		page, pageErr := pager.NextPage(ctx)
		require.NoError(t, pageErr)

		for _, d := range page.Value {
			names = append(names, *d.Name)
		}
	}

	assert.ElementsMatch(
		t,
		[]string{"appdb", "plain", "information_schema", "mysql", "performance_schema", "sys"},
		names,
	)

	del, err := dbs.BeginDelete(ctx, flexRG, "my-db", "appdb", nil)
	myPoll(t, del, err)

	_, err = dbs.BeginDelete(ctx, flexRG, "my-db", "sys", nil)
	require.Error(t, err)
}

func TestMySQLFlexConfigurations(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	s.myCreate(t, "my-cfg")

	cfg := s.myFactory(t).NewConfigurationsClient()
	ctx := t.Context()

	def, err := cfg.Get(ctx, flexRG, "my-cfg", "wait_timeout", nil)
	require.NoError(t, err)
	assert.Equal(t, "28800", *def.Properties.Value)
	assert.Equal(t, armmysqlflexibleservers.ConfigurationSourceSystemDefault, *def.Properties.Source)
	assert.Equal(t, armmysqlflexibleservers.IsDynamicConfigTrue, *def.Properties.IsDynamicConfig)
	assert.Equal(t, armmysqlflexibleservers.IsReadOnlyFalse, *def.Properties.IsReadOnly)

	put, err := cfg.BeginCreateOrUpdate(ctx, flexRG, "my-cfg", "wait_timeout", armmysqlflexibleservers.Configuration{
		Properties: &armmysqlflexibleservers.ConfigurationProperties{
			Value: new("600"), Source: new(armmysqlflexibleservers.ConfigurationSourceUserOverride),
		},
	}, nil)
	res := myPoll(t, put, err)
	assert.Equal(t, "600", *res.Properties.Value)
	assert.Equal(t, "600", *res.Properties.CurrentValue)

	_, err = cfg.BeginCreateOrUpdate(
		ctx,
		flexRG,
		"my-cfg",
		"lower_case_table_names",
		armmysqlflexibleservers.Configuration{
			Properties: &armmysqlflexibleservers.ConfigurationProperties{Value: new("2")},
		},
		nil,
	)
	require.Error(t, err)

	for name, val := range map[string]string{
		"slow_query_log": "MAYBE", "wait_timeout": "0", "sql_mode": "NOT_A_MODE", "tls_version": "TLSv1.0",
	} {
		status, _ := s.raw(t, http.MethodPut, flexServerPath(myNS, "my-cfg")+"/configurations/"+name,
			`{"properties":{"value":"`+val+`"}}`, flexAPIMy)
		assert.Equal(t, http.StatusBadRequest, status, name)
	}

	status, _ := s.raw(t, http.MethodPut, flexServerPath(myNS, "my-cfg")+"/configurations/sql_mode",
		`{"properties":{"value":"ANSI_QUOTES,STRICT_ALL_TABLES"}}`, flexAPIMy)
	assert.Equal(t, http.StatusOK, status)

	tls, err := cfg.BeginCreateOrUpdate(ctx, flexRG, "my-cfg", "tls_version", armmysqlflexibleservers.Configuration{
		Properties: &armmysqlflexibleservers.ConfigurationProperties{Value: new("TLSv1.2,TLSv1.3")},
	}, nil)
	myPoll(t, tls, err)

	got, err := cfg.Get(ctx, flexRG, "my-cfg", "tls_version", nil)
	require.NoError(t, err)
	assert.Equal(t, armmysqlflexibleservers.IsConfigPendingRestartTrue, *got.Properties.IsConfigPendingRestart)
}

func TestMySQLFlexReplicasPowerAndCapabilities(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	src := s.myCreateGP(t, "my-source")
	f := s.myFactory(t)
	ctx := t.Context()

	rep := armmysqlflexibleservers.Server{
		Location: new(flexLoc),
		Properties: &armmysqlflexibleservers.ServerProperties{
			CreateMode:             new(armmysqlflexibleservers.CreateModeReplica),
			SourceServerResourceID: src.ID,
		},
	}

	p, err := f.NewServersClient().BeginCreate(ctx, flexRG, "my-replica", rep, nil)
	require.NoError(t, err)

	replica, err := p.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)
	assert.Equal(t, armmysqlflexibleservers.ReplicationRoleReplica, *replica.Properties.ReplicationRole)

	primary, err := f.NewServersClient().Get(ctx, flexRG, "my-source", nil)
	require.NoError(t, err)
	assert.Equal(t, armmysqlflexibleservers.ReplicationRoleSource, *primary.Properties.ReplicationRole)

	pager := f.NewReplicasClient().NewListByServerPager(flexRG, "my-source", nil)
	page, err := pager.NextPage(ctx)
	require.NoError(t, err)
	require.Len(t, page.Value, 1)
	assert.Equal(t, "my-replica", *page.Value[0].Name)

	_, err = f.NewServersClient().BeginStop(ctx, flexRG, "my-replica", nil)
	require.Error(t, err)

	other := s.myCreate(t, "my-solo")

	stop, err := f.NewServersClient().BeginStop(ctx, flexRG, "my-solo", nil)
	require.NoError(t, err)
	_, err = stop.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	got, err := f.NewServersClient().Get(ctx, flexRG, *other.Name, nil)
	require.NoError(t, err)
	assert.Equal(t, armmysqlflexibleservers.ServerStateStopped, *got.Properties.State)

	_, err = f.NewServersClient().
		BeginRestart(ctx, flexRG, "my-solo", armmysqlflexibleservers.ServerRestartParameter{}, nil)
	require.Error(t, err)

	start, err := f.NewServersClient().BeginStart(ctx, flexRG, "my-solo", nil)
	require.NoError(t, err)
	_, err = start.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	caps := f.NewLocationBasedCapabilitiesClient().NewListPager(flexLoc, nil)
	capPage, err := caps.NextPage(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, capPage.Value)
	assert.Len(t, capPage.Value[0].SupportedFlexibleServerEditions, 3)

	sets, err := f.NewLocationBasedCapabilitySetClient().NewListPager(flexLoc, nil).NextPage(ctx)
	require.NoError(t, err)
	require.Len(t, sets.Value, 1)
	assert.Equal(t, "default", *sets.Value[0].Name)

	set, err := f.NewLocationBasedCapabilitySetClient().Get(ctx, flexLoc, "default", nil)
	require.NoError(t, err)
	assert.Len(t, set.Properties.SupportedServerVersions, 3)
	assert.Len(t, set.Properties.SupportedFlexibleServerEditions, 3)

	loc, err := f.NewCheckNameAvailabilityClient().Execute(ctx, flexLoc,
		armmysqlflexibleservers.NameAvailabilityRequest{
			Name: new("my-source"), Type: new("Microsoft.DBforMySQL/flexibleServers"),
		}, nil)
	require.NoError(t, err)
	assert.False(t, *loc.NameAvailable)

	free, err := f.NewCheckNameAvailabilityWithoutLocationClient().Execute(ctx,
		armmysqlflexibleservers.NameAvailabilityRequest{
			Name: new("my-free"), Type: new("Microsoft.DBforMySQL/flexibleServers"),
		}, nil)
	require.NoError(t, err)
	assert.True(t, *free.NameAvailable)
}

func TestMySQLFlexEngineAndPersistence(t *testing.T) {
	t.Parallel()

	eng := newFakeFlexEngine()
	s := newFlexStack(t, eng)
	f := s.myFactory(t)
	ctx := t.Context()

	s.myCreate(t, "my-engine")
	assert.Contains(t, eng.log(), "ensure:myadmin:8.0.21")

	db, err := f.NewDatabasesClient().
		BeginCreateOrUpdate(ctx, flexRG, "my-engine", "shop", armmysqlflexibleservers.Database{}, nil)
	require.NoError(t, err)
	_, err = db.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)
	assert.Contains(t, eng.log(), "createdb:shop:utf8mb4:utf8mb4_0900_ai_ci")

	_, body := s.raw(t, http.MethodGet, flexServerPath(myNS, "my-engine"), "", flexAPIMy)
	props, _ := body["properties"].(map[string]any)
	assert.Equal(t, "127.0.0.1:15432", props["gopherstackEndpoint"])

	snap := s.handler.Snapshot(context.Background())
	assert.NotContains(t, string(snap), flexPassword)

	s2 := newFlexStack(t, nil)
	require.NoError(t, s2.handler.Restore(context.Background(), snap))

	status, got := s2.raw(t, http.MethodGet, flexServerPath(myNS, "my-engine")+"/databases/shop", "", flexAPIMy)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "shop", got["name"])

	status, _ = s.raw(t, http.MethodPut, flexServerPath(myNS, "my-pitr"),
		`{"location":"x","properties":{"createMode":"PointInTimeRestore","sourceServerResourceId":"x"}}`, flexAPIMy)
	assert.Equal(t, http.StatusBadRequest, status)
}

func TestMySQLFlexPointInTimeRestore(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	src := s.myCreate(t, "my-pitr-src")

	_, body := s.raw(t, http.MethodGet, flexServerPath(myNS, "my-pitr-src"), "", flexAPIMy)
	created, _ := body["systemData"].(map[string]any)["createdAt"].(string)

	status, resp := s.raw(t, http.MethodPut, flexServerPath(myNS, "my-pitr-new"),
		`{"location":"x","properties":{"createMode":"PointInTimeRestore","sourceServerResourceId":"`+*src.ID+
			`","restorePointInTime":"`+created+`"}}`, flexAPIMy)
	require.Equal(t, http.StatusCreated, status, resp)

	props, _ := resp["properties"].(map[string]any)
	assert.Equal(t, "myadmin", props["administratorLogin"])
	assert.Equal(t, "PointInTimeRestore", props["createMode"])

	status, _ = s.raw(t, http.MethodPut, flexServerPath(myNS, "my-pitr-bad"),
		`{"location":"x","properties":{"createMode":"PointInTimeRestore","sourceServerResourceId":"`+*src.ID+
			`","restorePointInTime":"2001-01-01T00:00:00Z"}}`, flexAPIMy)
	assert.Equal(t, http.StatusBadRequest, status)
}

const myVersion84 = armmysqlflexibleservers.ServerVersion("8.4")

func myDatabase() armmysqlflexibleservers.Database { return armmysqlflexibleservers.Database{} }

func myPasswordUpdate(pw string) armmysqlflexibleservers.ServerForUpdate {
	return armmysqlflexibleservers.ServerForUpdate{
		Properties: &armmysqlflexibleservers.ServerPropertiesForUpdate{AdministratorLoginPassword: &pw},
	}
}
