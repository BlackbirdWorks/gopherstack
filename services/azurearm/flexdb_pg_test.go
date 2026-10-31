package azurearm_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/postgresql/armpostgresqlflexibleservers/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const pgNS = "Microsoft.DBforPostgreSQL"

func (s *flexStack) pgFactory(t *testing.T) *armpostgresqlflexibleservers.ClientFactory {
	t.Helper()

	f, err := armpostgresqlflexibleservers.NewClientFactory(aciSub, aciCred{}, s.opts)
	require.NoError(t, err)

	return f
}

func pgServer() armpostgresqlflexibleservers.Server {
	return armpostgresqlflexibleservers.Server{
		Location: new(flexLoc),
		Tags:     map[string]*string{"env": new("test")},
		SKU: &armpostgresqlflexibleservers.SKU{
			Name: new("Standard_B1ms"), Tier: new(armpostgresqlflexibleservers.SKUTierBurstable),
		},
		Properties: &armpostgresqlflexibleservers.ServerProperties{
			AdministratorLogin:         new("pgadmin"),
			AdministratorLoginPassword: new(flexPassword),
			Version:                    new(armpostgresqlflexibleservers.ServerVersion("16")),
			Storage:                    &armpostgresqlflexibleservers.Storage{StorageSizeGB: new(int32(32))},
			Backup:                     &armpostgresqlflexibleservers.Backup{BackupRetentionDays: new(int32(7))},
		},
	}
}

func (s *flexStack) pgCreate(t *testing.T, name string) armpostgresqlflexibleservers.Server {
	t.Helper()

	poller, err := s.pgFactory(t).NewServersClient().BeginCreate(t.Context(), flexRG, name, pgServer(), nil)
	require.NoError(t, err)

	res, err := poller.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	return res.Server
}

func TestPGFlexServerCRUD(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	f := s.pgFactory(t)
	srv := s.pgCreate(t, "pg-crud")

	require.Equal(t, "pg-crud.postgres.database.azure.com", *srv.Properties.FullyQualifiedDomainName)
	assert.Equal(t, armpostgresqlflexibleservers.ServerStateReady, *srv.Properties.State)
	assert.Equal(t, "Standard_B1ms", *srv.SKU.Name)
	assert.Equal(t, int32(32), *srv.Properties.Storage.StorageSizeGB)
	assert.Equal(t, "pgadmin", *srv.Properties.AdministratorLogin)
	assert.Equal(t, "Microsoft.DBforPostgreSQL/flexibleServers", *srv.Type)

	status, raw := s.raw(t, http.MethodGet, flexServerPath(pgNS, "pg-crud"), "", flexAPIPG)
	require.Equal(t, http.StatusOK, status)

	enc, err := json.Marshal(raw)
	require.NoError(t, err)
	assert.NotContains(t, string(enc), flexPassword)
	assert.NotContains(t, string(enc), "administratorLoginPassword")

	up, err := f.NewServersClient().
		BeginUpdate(t.Context(), flexRG, "pg-crud", armpostgresqlflexibleservers.ServerForUpdate{
			Tags: map[string]*string{"env": new("prod")},
			Properties: &armpostgresqlflexibleservers.ServerPropertiesForUpdate{
				Storage: &armpostgresqlflexibleservers.Storage{StorageSizeGB: new(int32(64))},
			},
		}, nil)
	require.NoError(t, err)

	upRes, err := up.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)
	assert.Equal(t, "prod", *upRes.Tags["env"])
	assert.Equal(t, int32(64), *upRes.Properties.Storage.StorageSizeGB)

	var names []string

	pager := f.NewServersClient().NewListByResourceGroupPager(flexRG, nil)
	for pager.More() {
		page, pageErr := pager.NextPage(t.Context())
		require.NoError(t, pageErr)

		for _, v := range page.Value {
			names = append(names, *v.Name)
		}
	}

	assert.Equal(t, []string{"pg-crud"}, names)

	allPager := f.NewServersClient().NewListPager(nil)
	all, err := allPager.NextPage(t.Context())
	require.NoError(t, err)
	assert.Len(t, all.Value, 1)

	del, err := f.NewServersClient().BeginDelete(t.Context(), flexRG, "pg-crud", nil)
	require.NoError(t, err)
	_, err = del.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	_, err = f.NewServersClient().Get(t.Context(), flexRG, "pg-crud", nil)
	require.Error(t, err)

	var respErr *azcore.ResponseError

	require.ErrorAs(t, err, &respErr)
	assert.Equal(t, http.StatusNotFound, respErr.StatusCode)
	assert.Equal(t, "ResourceNotFound", respErr.ErrorCode)
}

const pgLoginPW = `"administratorLogin":"a1","administratorLoginPassword":"` + flexPassword + `"`

func pgReq(props string) string {
	return `{"location":"x","sku":{"name":"Standard_B1ms","tier":"Burstable"},"properties":{` + props + `}}`
}

func TestPGFlexServerValidation(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	subnet := "/subscriptions/" + aciSub + "/resourceGroups/rg/providers/Microsoft.Network/virtualNetworks/v/subnets/s"
	zone := "/subscriptions/" + aciSub + "/resourceGroups/rg/providers/Microsoft.Network/privateDnsZones/z"

	tests := []struct {
		name   string
		server string
		body   string
		status int
	}{
		{
			"unknown sku",
			"pg-v1",
			`{"location":"x","sku":{"name":"Standard_Z9","tier":"Burstable"},"properties":{` + pgLoginPW + `}}`,
			400,
		},
		{
			"tier mismatch",
			"pg-v2",
			`{"location":"x","sku":{"name":"Standard_B1ms","tier":"GeneralPurpose"},"properties":{` + pgLoginPW + `}}`,
			400,
		},
		{"missing sku", "pg-v3", `{"location":"x","properties":{` + pgLoginPW + `}}`, 400},
		{"bad version", "pg-v4", pgReq(pgLoginPW + `,"version":"9"`), 400},
		{
			"reserved login",
			"pg-v5",
			pgReq(`"administratorLogin":"admin","administratorLoginPassword":"` + flexPassword + `"`),
			400,
		},
		{
			"pg_ login",
			"pg-v6",
			pgReq(`"administratorLogin":"pg_boss","administratorLoginPassword":"` + flexPassword + `"`),
			400,
		},
		{"weak password", "pg-v7", pgReq(`"administratorLogin":"a1","administratorLoginPassword":"password"`), 400},
		{"short password", "pg-v8", pgReq(`"administratorLogin":"a1","administratorLoginPassword":"Ab1!"`), 400},
		{"missing password", "pg-v9", pgReq(`"administratorLogin":"a1"`), 400},
		{"bad storage", "pg-v10", pgReq(pgLoginPW + `,"storage":{"storageSizeGB":33}`), 400},
		{"low retention", "pg-v11", pgReq(pgLoginPW + `,"backup":{"backupRetentionDays":6}`), 400},
		{"high retention", "pg-v12", pgReq(pgLoginPW + `,"backup":{"backupRetentionDays":36}`), 400},
		{"burstable ha", "pg-v13", pgReq(pgLoginPW + `,"highAvailability":{"mode":"ZoneRedundant"}`), 400},
		{"uppercase name", "PG-V14", pgReq(pgLoginPW), 400},
		{"bad create mode", "pg-v15", pgReq(pgLoginPW + `,"createMode":"Bogus"`), 400},
		{
			"subnet without dns",
			"pg-v16",
			pgReq(pgLoginPW + `,"network":{"delegatedSubnetResourceId":"` + subnet + `"}`),
			400,
		},
		{"missing subnet", "pg-v17", pgReq(pgLoginPW + `,"network":{"delegatedSubnetResourceId":"` + subnet +
			`","privateDnsZoneArmResourceId":"` + zone + `"}`), 400},
		{"replica without source", "pg-v18", `{"location":"x","properties":{"createMode":"Replica"}}`, 400},
		{"valid", "pg-v19", pgReq(pgLoginPW + `,"version":"15"`), 201},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status, body := s.raw(t, http.MethodPut, flexServerPath(pgNS, tt.server), tt.body, flexAPIPG)
			assert.Equal(t, tt.status, status, body)

			if tt.status == http.StatusBadRequest {
				assert.Equal(t, "InvalidParameterValue", flexErrCode(body))
			}
		})
	}

	t.Run("patch missing", func(t *testing.T) {
		t.Parallel()

		status, body := s.raw(t, http.MethodPatch, flexServerPath(pgNS, "pg-nope"), `{"tags":{"a":"b"}}`, flexAPIPG)
		assert.Equal(t, http.StatusNotFound, status)
		assert.Equal(t, "ResourceNotFound", flexErrCode(body))
	})
}

func TestPGFlexNameConflictAcrossGroups(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	s.pgCreate(t, "pg-global")
	s.raw(t, http.MethodPut, "/subscriptions/"+aciSub+"/resourcegroups/other", `{"location":"x"}`, "2021-04-01")

	status, body := s.raw(
		t,
		http.MethodPut,
		"/subscriptions/"+aciSub+"/resourceGroups/other/providers/"+pgNS+"/flexibleServers/pg-global",
		pgReq(pgLoginPW),
		flexAPIPG,
	)
	assert.Equal(t, http.StatusConflict, status)
	assert.Equal(t, "Conflict", flexErrCode(body))
}

func pgPoll[T any](t *testing.T, poller *runtime.Poller[T], err error) T {
	t.Helper()

	require.NoError(t, err)

	res, err := poller.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	return res
}

func TestPGFlexFirewall(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	s.pgCreate(t, "pg-fw")

	fw := s.pgFactory(t).NewFirewallRulesClient()
	ctx := t.Context()

	for _, name := range []string{"AllowAllAzureServicesAndResourcesWithinAzureIps_2026", "office"} {
		start, end := "10.0.0.1", "10.0.0.9"
		if strings.HasPrefix(name, "AllowAll") {
			start, end = "0.0.0.0", "0.0.0.0"
		}

		p, err := fw.BeginCreateOrUpdate(ctx, flexRG, "pg-fw", name, armpostgresqlflexibleservers.FirewallRule{
			Properties: &armpostgresqlflexibleservers.FirewallRuleProperties{
				StartIPAddress: &start,
				EndIPAddress:   &end,
			},
		}, nil)
		res := pgPoll(t, p, err)
		assert.Equal(t, start, *res.Properties.StartIPAddress)
	}

	got, err := fw.Get(ctx, flexRG, "pg-fw", "office", nil)
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.9", *got.Properties.EndIPAddress)

	count := 0
	pager := fw.NewListByServerPager(flexRG, "pg-fw", nil)

	for pager.More() {
		page, pageErr := pager.NextPage(ctx)
		require.NoError(t, pageErr)

		count += len(page.Value)
	}

	assert.Equal(t, 2, count)

	del, err := fw.BeginDelete(ctx, flexRG, "pg-fw", "office", nil)
	pgPoll(t, del, err)

	_, err = fw.Get(ctx, flexRG, "pg-fw", "office", nil)
	require.Error(t, err)

	path := flexServerPath(pgNS, "pg-fw") + "/firewallRules/"

	for name, body := range map[string]string{
		"bad-ip":   `{"properties":{"startIpAddress":"nope","endIpAddress":"1.1.1.1"}}`,
		"reversed": `{"properties":{"startIpAddress":"10.0.0.9","endIpAddress":"10.0.0.1"}}`,
		"ipv6":     `{"properties":{"startIpAddress":"::1","endIpAddress":"::2"}}`,
		"bad%20!":  `{"properties":{"startIpAddress":"1.1.1.1","endIpAddress":"1.1.1.2"}}`,
	} {
		status, resp := s.raw(t, http.MethodPut, path+name, body, flexAPIPG)
		assert.Equal(t, http.StatusBadRequest, status, name)
		assert.Equal(t, "InvalidParameterValue", flexErrCode(resp), name)
	}
}

func TestPGFlexDatabases(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	s.pgCreate(t, "pg-db")

	dbs := s.pgFactory(t).NewDatabasesClient()
	ctx := t.Context()

	p, err := dbs.BeginCreate(ctx, flexRG, "pg-db", "appdb", armpostgresqlflexibleservers.Database{}, nil)
	res := pgPoll(t, p, err)
	assert.Equal(t, "UTF8", *res.Properties.Charset)
	assert.Equal(t, "en_US.utf8", *res.Properties.Collation)

	var names []string

	pager := dbs.NewListByServerPager(flexRG, "pg-db", nil)
	for pager.More() {
		page, pageErr := pager.NextPage(ctx)
		require.NoError(t, pageErr)

		for _, d := range page.Value {
			names = append(names, *d.Name)
		}
	}

	assert.ElementsMatch(t, []string{"appdb", "azure_maintenance", "azure_sys", "postgres"}, names)

	_, err = dbs.BeginCreate(ctx, flexRG, "pg-db", "postgres", armpostgresqlflexibleservers.Database{}, nil)
	require.Error(t, err)

	del, err := dbs.BeginDelete(ctx, flexRG, "pg-db", "appdb", nil)
	pgPoll(t, del, err)

	_, err = dbs.Get(ctx, flexRG, "pg-db", "appdb", nil)
	require.Error(t, err)

	_, err = dbs.BeginDelete(ctx, flexRG, "pg-db", "postgres", nil)
	require.Error(t, err)
}

func TestPGFlexConfigurations(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	s.pgCreate(t, "pg-cfg")

	f := s.pgFactory(t)
	cfg := f.NewConfigurationsClient()
	ctx := t.Context()

	def, err := cfg.Get(ctx, flexRG, "pg-cfg", "max_connections", nil)
	require.NoError(t, err)
	assert.Equal(t, "50", *def.Properties.Value)
	assert.Equal(t, "system-default", *def.Properties.Source)
	assert.Equal(t, "25-262143", *def.Properties.AllowedValues)
	assert.False(t, *def.Properties.IsReadOnly)

	put, err := cfg.BeginPut(ctx, flexRG, "pg-cfg", "work_mem", armpostgresqlflexibleservers.Configuration{
		Properties: &armpostgresqlflexibleservers.ConfigurationProperties{
			Value: new("8192"), Source: new("user-override"),
		},
	}, nil)
	res := pgPoll(t, put, err)
	assert.Equal(t, "8192", *res.Properties.Value)
	assert.Equal(t, "user-override", *res.Properties.Source)

	static, err := cfg.BeginPut(ctx, flexRG, "pg-cfg", "wal_level", armpostgresqlflexibleservers.Configuration{
		Properties: &armpostgresqlflexibleservers.ConfigurationProperties{Value: new("logical")},
	}, nil)
	pgPoll(t, static, err)

	got, err := cfg.Get(ctx, flexRG, "pg-cfg", "wal_level", nil)
	require.NoError(t, err)
	assert.True(t, *got.Properties.IsConfigPendingRestart)

	rst, err := f.NewServersClient().BeginRestart(ctx, flexRG, "pg-cfg", nil)
	pgPoll(t, rst, err)

	got, err = cfg.Get(ctx, flexRG, "pg-cfg", "wal_level", nil)
	require.NoError(t, err)
	assert.False(t, *got.Properties.IsConfigPendingRestart)
	assert.Equal(t, "logical", *got.Properties.Value)

	reset, err := cfg.BeginUpdate(
		ctx,
		flexRG,
		"pg-cfg",
		"work_mem",
		armpostgresqlflexibleservers.ConfigurationForUpdate{
			Properties: &armpostgresqlflexibleservers.ConfigurationProperties{Source: new("system-default")},
		},
		nil,
	)
	pgPoll(t, reset, err)

	got, err = cfg.Get(ctx, flexRG, "pg-cfg", "work_mem", nil)
	require.NoError(t, err)
	assert.Equal(t, "4096", *got.Properties.Value)

	count := 0
	pager := cfg.NewListByServerPager(flexRG, "pg-cfg", nil)

	for pager.More() {
		page, pageErr := pager.NextPage(ctx)
		require.NoError(t, pageErr)

		count += len(page.Value)
	}

	assert.Greater(t, count, 10)
}

func TestPGFlexConfigurationErrors(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	s.pgCreate(t, "pg-cfgerr")

	path := flexServerPath(pgNS, "pg-cfgerr") + "/configurations/"

	for name, tc := range map[string]struct{ body, code string }{
		"max_connections":  {`{"properties":{"value":"5"}}`, "InvalidParameterValue"},
		"log_statement":    {`{"properties":{"value":"bogus"}}`, "InvalidParameterValue"},
		"server_version":   {`{"properties":{"value":"1"}}`, "InvalidParameterValue"},
		"azure.extensions": {`{"properties":{"value":"NOT_AN_EXT"}}`, "InvalidParameterValue"},
		"work_mem":         {`{"properties":{}}`, "InvalidParameterValue"},
		"no_such_param":    {`{"properties":{"value":"1"}}`, "ResourceNotFound"},
	} {
		status, resp := s.raw(t, http.MethodPut, path+name, tc.body, flexAPIPG)
		assert.GreaterOrEqual(t, status, http.StatusBadRequest, name)
		assert.Equal(t, tc.code, flexErrCode(resp), name)
	}

	status, resp := s.raw(t, http.MethodPut, path+"azure.extensions",
		`{"properties":{"value":"pg_trgm,uuid-ossp"}}`, flexAPIPG)
	assert.Equal(t, http.StatusOK, status, resp)
}

func TestPGFlexStartStopRestart(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	s.pgCreate(t, "pg-power")

	f := s.pgFactory(t)
	ctx := t.Context()
	state := func() string {
		got, err := f.NewServersClient().Get(ctx, flexRG, "pg-power", nil)
		require.NoError(t, err)

		return string(*got.Properties.State)
	}

	stop, err := f.NewServersClient().BeginStop(ctx, flexRG, "pg-power", nil)
	require.NoError(t, err)
	_, err = stop.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)
	assert.Equal(t, "Stopped", state())

	_, err = f.NewServersClient().BeginRestart(ctx, flexRG, "pg-power", nil)
	require.Error(t, err)

	status, body := s.raw(
		t,
		http.MethodPut,
		flexServerPath(pgNS, "pg-power")+"/databases/blocked",
		`{"properties":{}}`,
		flexAPIPG,
	)
	assert.Equal(t, http.StatusConflict, status)
	assert.Equal(t, "ServerIsNotReady", flexErrCode(body))

	status, _ = s.raw(t, http.MethodPatch, flexServerPath(pgNS, "pg-power"), `{"tags":{"a":"b"}}`, flexAPIPG)
	assert.Equal(t, http.StatusConflict, status)

	start, err := f.NewServersClient().BeginStart(ctx, flexRG, "pg-power", nil)
	require.NoError(t, err)
	_, err = start.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)
	assert.Equal(t, "Ready", state())

	rst, err := f.NewServersClient().BeginRestart(ctx, flexRG, "pg-power", nil)
	require.NoError(t, err)
	_, err = rst.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)
	assert.Equal(t, "Ready", state())
}

func TestPGFlexStopHAServer(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	f := s.pgFactory(t)
	ctx := t.Context()

	srv := pgServer()
	srv.SKU = &armpostgresqlflexibleservers.SKU{
		Name: new("Standard_D2ds_v5"), Tier: new(armpostgresqlflexibleservers.SKUTierGeneralPurpose),
	}
	srv.Properties.HighAvailability = &armpostgresqlflexibleservers.HighAvailability{
		Mode: new(armpostgresqlflexibleservers.HighAvailabilityModeZoneRedundant),
	}

	p, err := f.NewServersClient().BeginCreate(ctx, flexRG, "pg-ha", srv, nil)
	require.NoError(t, err)
	_, err = p.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	stop, err := f.NewServersClient().BeginStop(ctx, flexRG, "pg-ha", nil)
	require.NoError(t, err)
	_, err = stop.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	got, err := f.NewServersClient().Get(ctx, flexRG, "pg-ha", nil)
	require.NoError(t, err)
	assert.Equal(t, armpostgresqlflexibleservers.ServerStateStopped, *got.Properties.State)
}

func TestPGFlexCheckNameAvailability(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	s.pgCreate(t, "pg-exists")

	c := s.pgFactory(t).NewCheckNameAvailabilityClient()
	typ := "Microsoft.DBforPostgreSQL/flexibleServers"

	tests := []struct {
		name       string
		typ        string
		reason     string
		available  bool
		wantReason bool
	}{
		{"pg-fresh", typ, "", true, false},
		{"pg-exists", typ, "AlreadyExists", false, true},
		{"PG_BAD", typ, "Invalid", false, true},
		{"pg-wrongtype", "Microsoft.Sql/servers", "Invalid", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res, err := c.Execute(t.Context(), armpostgresqlflexibleservers.CheckNameAvailabilityRequest{
				Name: new(tt.name), Type: new(tt.typ),
			}, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.available, *res.NameAvailable)

			if tt.wantReason {
				assert.Equal(t, tt.reason, string(*res.Reason))
			}
		})
	}

	loc, err := s.pgFactory(t).NewCheckNameAvailabilityWithLocationClient().Execute(t.Context(), flexLoc,
		armpostgresqlflexibleservers.CheckNameAvailabilityRequest{Name: new("pg-exists"), Type: new(typ)}, nil)
	require.NoError(t, err)
	assert.False(t, *loc.NameAvailable)
}

func TestPGFlexCapabilities(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	pager := s.pgFactory(t).NewLocationBasedCapabilitiesClient().NewExecutePager(flexLoc, nil)
	page, err := pager.NextPage(t.Context())
	require.NoError(t, err)
	require.NotEmpty(t, page.Value)

	editions := map[string]bool{}
	skus := map[string]bool{}

	for _, e := range page.Value[0].SupportedFlexibleServerEditions {
		editions[*e.Name] = true

		for _, v := range e.SupportedServerVersions {
			for _, c := range v.SupportedVcores {
				skus[*c.Name] = true
			}
		}
	}

	assert.Equal(t, map[string]bool{"Burstable": true, "GeneralPurpose": true, "MemoryOptimized": true}, editions)
	assert.True(t, skus["Standard_B1ms"])
	assert.True(t, skus["Standard_D4ds_v5"])
	assert.True(t, skus["Standard_E96ads_v5"])
}

func (s *flexStack) pgCreateGP(t *testing.T, name string) armpostgresqlflexibleservers.Server {
	t.Helper()

	srv := pgServer()
	srv.SKU = &armpostgresqlflexibleservers.SKU{
		Name: new("Standard_D2ds_v5"), Tier: new(armpostgresqlflexibleservers.SKUTierGeneralPurpose),
	}

	poller, err := s.pgFactory(t).NewServersClient().BeginCreate(t.Context(), flexRG, name, srv, nil)
	require.NoError(t, err)

	res, err := poller.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	return res.Server
}

func TestPGFlexReplicasAndRestore(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	src := s.pgCreateGP(t, "pg-primary")
	f := s.pgFactory(t)
	ctx := t.Context()

	rep := armpostgresqlflexibleservers.Server{
		Location: new(flexLoc),
		Properties: &armpostgresqlflexibleservers.ServerProperties{
			CreateMode:             new(armpostgresqlflexibleservers.CreateModeReplica),
			SourceServerResourceID: src.ID,
		},
	}

	p, err := f.NewServersClient().BeginCreate(ctx, flexRG, "pg-replica", rep, nil)
	require.NoError(t, err)

	replica, err := p.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)
	assert.Equal(t, armpostgresqlflexibleservers.ReplicationRoleAsyncReplica, *replica.Properties.ReplicationRole)
	assert.Equal(t, "Standard_D2ds_v5", *replica.SKU.Name)
	assert.Equal(t, "pgadmin", *replica.Properties.AdministratorLogin)

	primary, err := f.NewServersClient().Get(ctx, flexRG, "pg-primary", nil)
	require.NoError(t, err)
	assert.Equal(t, armpostgresqlflexibleservers.ReplicationRolePrimary, *primary.Properties.ReplicationRole)

	var listed []string

	pager := f.NewReplicasClient().NewListByServerPager(flexRG, "pg-primary", nil)
	for pager.More() {
		page, pageErr := pager.NextPage(ctx)
		require.NoError(t, pageErr)

		for _, r := range page.Value {
			listed = append(listed, *r.Name)
		}
	}

	assert.Equal(t, []string{"pg-replica"}, listed)

	stop, err := f.NewServersClient().BeginStop(ctx, flexRG, "pg-primary", nil)
	require.NoError(t, err)
	_, err = stop.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	start, err := f.NewServersClient().BeginStart(ctx, flexRG, "pg-primary", nil)
	require.NoError(t, err)
	_, err = start.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	nested := armpostgresqlflexibleservers.Server{
		Location: new(flexLoc),
		Properties: &armpostgresqlflexibleservers.ServerProperties{
			CreateMode:             new(armpostgresqlflexibleservers.CreateModeReplica),
			SourceServerResourceID: replica.ID,
		},
	}

	n, err := f.NewServersClient().BeginCreate(ctx, flexRG, "pg-nested", nested, nil)
	require.NoError(t, err)

	cascaded, err := n.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	tooDeep := nested
	tooDeep.Properties = &armpostgresqlflexibleservers.ServerProperties{
		CreateMode:             new(armpostgresqlflexibleservers.CreateModeReplica),
		SourceServerResourceID: cascaded.ID,
	}

	_, err = f.NewServersClient().BeginCreate(ctx, flexRG, "pg-deep", tooDeep, nil)
	require.Error(t, err)

	dropNested, err := f.NewServersClient().BeginDelete(ctx, flexRG, "pg-nested", nil)
	require.NoError(t, err)
	_, err = dropNested.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	del, err := f.NewServersClient().BeginDelete(ctx, flexRG, "pg-primary", nil)
	require.NoError(t, err)
	_, err = del.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	promoted, err := f.NewServersClient().Get(ctx, flexRG, "pg-replica", nil)
	require.NoError(t, err)
	assert.Equal(t, armpostgresqlflexibleservers.ReplicationRoleNone, *promoted.Properties.ReplicationRole)
}

func TestPGFlexPointInTimeRestore(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	src := s.pgCreate(t, "pg-pitr-src")

	_, body := s.raw(t, http.MethodGet, flexServerPath(pgNS, "pg-pitr-src"), "", flexAPIPG)
	created, _ := body["systemData"].(map[string]any)["createdAt"].(string)
	require.NotEmpty(t, created)

	tests := []struct {
		name     string
		pit      string
		wantCode string
		status   int
	}{
		{"valid", created, "", http.StatusCreated},
		{"before source", "2001-01-01T00:00:00Z", "InvalidParameterValue", http.StatusBadRequest},
		{"missing", "", "InvalidParameterValue", http.StatusBadRequest},
		{"garbage", "yesterday", "InvalidParameterValue", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pit := ""
			if tt.pit != "" {
				pit = `,"pointInTimeUTC":"` + tt.pit + `"`
			}

			status, resp := s.raw(
				t,
				http.MethodPut,
				flexServerPath(pgNS, "pg-pitr-"+strings.ReplaceAll(tt.name, " ", "")),
				`{"location":"x","properties":{"createMode":"PointInTimeRestore","sourceServerResourceId":"`+*src.ID+`"`+pit+`}}`,
				flexAPIPG,
			)
			assert.Equal(t, tt.status, status, resp)
			assert.Equal(t, tt.wantCode, flexErrCode(resp))
		})
	}
}

func TestPGFlexPersistence(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	s.pgCreate(t, "pg-persist")

	dbPath := flexServerPath(pgNS, "pg-persist")
	status, _ := s.raw(t, http.MethodPut, dbPath+"/databases/keepme", `{"properties":{}}`, flexAPIPG)
	require.Equal(t, http.StatusCreated, status)

	status, _ = s.raw(t, http.MethodPut, dbPath+"/firewallRules/fw1",
		`{"properties":{"startIpAddress":"1.2.3.4","endIpAddress":"1.2.3.4"}}`, flexAPIPG)
	require.Equal(t, http.StatusCreated, status)

	snap := s.handler.Snapshot(context.Background())
	assert.NotContains(t, string(snap), flexPassword)

	s2 := newFlexStack(t, nil)
	require.NoError(t, s2.handler.Restore(context.Background(), snap))

	status, body := s2.raw(t, http.MethodGet, dbPath, "", flexAPIPG)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "pg-persist", body["name"])

	status, body = s2.raw(t, http.MethodGet, dbPath+"/databases/keepme", "", flexAPIPG)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "keepme", body["name"])

	status, _ = s2.raw(t, http.MethodGet, dbPath+"/firewallRules/fw1", "", flexAPIPG)
	assert.Equal(t, http.StatusOK, status)
}

func TestPGFlexEngine(t *testing.T) {
	t.Parallel()

	eng := newFakeFlexEngine()
	s := newFlexStack(t, eng)
	f := s.pgFactory(t)
	ctx := t.Context()

	s.pgCreate(t, "pg-engine")

	status, body := s.raw(t, http.MethodGet, flexServerPath(pgNS, "pg-engine"), "", flexAPIPG)
	require.Equal(t, http.StatusOK, status)

	props, _ := body["properties"].(map[string]any)
	assert.Equal(t, "127.0.0.1:15432", props["gopherstackEndpoint"])
	assert.Equal(t, "pg-engine.postgres.database.azure.com", props["fullyQualifiedDomainName"])
	assert.Contains(t, eng.log(), "ensure:pgadmin:16")

	eng.mu.Lock()
	for _, pw := range eng.pw {
		assert.Equal(t, flexPassword, pw)
	}
	eng.mu.Unlock()

	db, err := f.NewDatabasesClient().
		BeginCreate(ctx, flexRG, "pg-engine", "shop", armpostgresqlflexibleservers.Database{}, nil)
	require.NoError(t, err)
	_, err = db.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)
	assert.Contains(t, eng.log(), "createdb:shop:UTF8:en_US.utf8")

	up, err := f.NewServersClient().BeginUpdate(ctx, flexRG, "pg-engine", armpostgresqlflexibleservers.ServerForUpdate{
		Properties: &armpostgresqlflexibleservers.ServerPropertiesForUpdate{
			AdministratorLoginPassword: new("An0therS3cret!"),
		},
	}, nil)
	require.NoError(t, err)
	_, err = up.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)
	assert.Contains(t, eng.log(), "password:pgadmin")

	stop, err := f.NewServersClient().BeginStop(ctx, flexRG, "pg-engine", nil)
	require.NoError(t, err)
	_, err = stop.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)
	assert.Contains(t, eng.log(), "stop")

	st, err := f.NewServersClient().BeginStart(ctx, flexRG, "pg-engine", nil)
	require.NoError(t, err)
	_, err = st.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)
	assert.Contains(t, eng.log(), "start")

	dd, err := f.NewDatabasesClient().BeginDelete(ctx, flexRG, "pg-engine", "shop", nil)
	require.NoError(t, err)
	_, err = dd.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)
	assert.Contains(t, eng.log(), "dropdb:shop")

	del, err := f.NewServersClient().BeginDelete(ctx, flexRG, "pg-engine", nil)
	require.NoError(t, err)
	_, err = del.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)
	assert.Contains(t, eng.log(), "delete")

	status, _ = s.raw(t, http.MethodPut, flexServerPath(pgNS, "pg-repl"),
		`{"location":"x","properties":{"createMode":"Replica","sourceServerResourceId":"/subscriptions/`+aciSub+
			`/resourceGroups/`+flexRG+`/providers/`+pgNS+`/flexibleServers/x"}}`, flexAPIPG)
	assert.Equal(t, http.StatusBadRequest, status)
}

func TestPGFlexEngineFailure(t *testing.T) {
	t.Parallel()

	eng := newFakeFlexEngine()
	eng.failEnsure = true
	s := newFlexStack(t, eng)
	ctx := t.Context()

	poller, err := s.pgFactory(t).NewServersClient().BeginCreate(ctx, flexRG, "pg-fail", pgServer(), nil)
	require.NoError(t, err)

	_, err = poller.PollUntilDone(ctx, pollFast())
	require.Error(t, err)

	got, err := s.pgFactory(t).NewServersClient().Get(ctx, flexRG, "pg-fail", nil)
	require.NoError(t, err)
	assert.Equal(t, armpostgresqlflexibleservers.ServerStateDisabled, *got.Properties.State)
}

const srvVersion16 = armpostgresqlflexibleservers.ServerVersion("16")

func pgDatabase() armpostgresqlflexibleservers.Database {
	return armpostgresqlflexibleservers.Database{}
}

func pgPasswordUpdate(pw string) armpostgresqlflexibleservers.ServerForUpdate {
	return armpostgresqlflexibleservers.ServerForUpdate{
		Properties: &armpostgresqlflexibleservers.ServerPropertiesForUpdate{AdministratorLoginPassword: &pw},
	}
}
