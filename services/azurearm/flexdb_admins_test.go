package azurearm_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/mysql/armmysqlflexibleservers/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/postgresql/armpostgresqlflexibleservers/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	flexOID    = "34a4ab42-0d72-47d9-bd1a-aed207386dac"
	flexTenant = "c24c76aa-8897-4027-9b03-8f7928b54ff6"
)

func TestPGFlexAdministrators(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	s.pgCreate(t, "pg-adm")

	admins := s.pgFactory(t).NewAdministratorsClient()
	ctx := t.Context()

	add := armpostgresqlflexibleservers.ActiveDirectoryAdministratorAdd{
		Properties: &armpostgresqlflexibleservers.AdministratorPropertiesForAdd{
			PrincipalName: new(
				"dba@contoso.example",
			),
			PrincipalType: new(armpostgresqlflexibleservers.PrincipalTypeUser),
			TenantID:      new(flexTenant),
		},
	}

	_, err := admins.BeginCreate(ctx, flexRG, "pg-adm", flexOID, add, nil)
	require.Error(t, err)
	assert.Equal(t, "InvalidParameterValue", sdkErrCode(err))

	srv := pgServer()
	srv.Properties.AuthConfig = &armpostgresqlflexibleservers.AuthConfig{
		ActiveDirectoryAuth: new(armpostgresqlflexibleservers.ActiveDirectoryAuthEnumEnabled),
		PasswordAuth:        new(armpostgresqlflexibleservers.PasswordAuthEnumEnabled),
		TenantID:            new(flexTenant),
	}
	sp, err := s.pgFactory(t).NewServersClient().BeginCreate(ctx, flexRG, "pg-entra", srv, nil)
	pgPoll(t, sp, err)

	tests := []struct {
		mod      func(*armpostgresqlflexibleservers.ActiveDirectoryAdministratorAdd)
		name     string
		objectID string
		wantCode string
	}{
		{name: "bad object id", objectID: "nope", wantCode: "InvalidParameterValue"},
		{name: "no principal name", objectID: flexOID, wantCode: "InvalidParameterValue",
			mod: func(a *armpostgresqlflexibleservers.ActiveDirectoryAdministratorAdd) {
				a.Properties.PrincipalName = nil
			}},
		{name: "bad principal type", objectID: flexOID, wantCode: "InvalidParameterValue",
			mod: func(a *armpostgresqlflexibleservers.ActiveDirectoryAdministratorAdd) {
				a.Properties.PrincipalType = new(armpostgresqlflexibleservers.PrincipalType("Robot"))
			}},
		{name: "bad tenant", objectID: flexOID, wantCode: "InvalidParameterValue",
			mod: func(a *armpostgresqlflexibleservers.ActiveDirectoryAdministratorAdd) {
				a.Properties.TenantID = new("t")
			}},
	}

	for _, tt := range tests {
		in := add
		props := *add.Properties
		in.Properties = &props

		if tt.mod != nil {
			tt.mod(&in)
		}

		_, cerr := admins.BeginCreate(ctx, flexRG, "pg-entra", tt.objectID, in, nil)
		require.Error(t, cerr, tt.name)
		assert.Equal(t, tt.wantCode, sdkErrCode(cerr), tt.name)
	}

	cp, err := admins.BeginCreate(ctx, flexRG, "pg-entra", flexOID, add, nil)
	res := pgPoll(t, cp, err)
	assert.Equal(t, flexOID, *res.Properties.ObjectID)

	got, err := admins.Get(ctx, flexRG, "pg-entra", flexOID, nil)
	require.NoError(t, err)
	assert.Equal(t, "dba@contoso.example", *got.Properties.PrincipalName)
	assert.Equal(t, armpostgresqlflexibleservers.PrincipalTypeUser, *got.Properties.PrincipalType)
	assert.Equal(t, "Microsoft.DBforPostgreSQL/flexibleServers/administrators", *got.Type)

	n := 0

	for pager := admins.NewListByServerPager(flexRG, "pg-entra", nil); pager.More(); {
		page, perr := pager.NextPage(ctx)
		require.NoError(t, perr)

		n += len(page.Value)
	}

	assert.Equal(t, 1, n)

	dp, err := admins.BeginDelete(ctx, flexRG, "pg-entra", flexOID, nil)
	pgPoll(t, dp, err)

	_, err = admins.Get(ctx, flexRG, "pg-entra", flexOID, nil)
	require.Error(t, err)
}

func TestMySQLFlexAdministratorsAndFailover(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	ctx := t.Context()
	uami := "/subscriptions/" + aciSub +
		"/resourceGroups/rg/providers/Microsoft.ManagedIdentity/userAssignedIdentities/uami"

	srv := mySrv()
	srv.SKU = &armmysqlflexibleservers.MySQLServerSKU{
		Name: new("Standard_D2ds_v4"), Tier: new(armmysqlflexibleservers.ServerSKUTierGeneralPurpose),
	}
	srv.Identity = &armmysqlflexibleservers.MySQLServerIdentity{
		Type:                   new(armmysqlflexibleservers.ManagedServiceIdentityTypeUserAssigned),
		UserAssignedIdentities: map[string]*armmysqlflexibleservers.UserAssignedIdentity{uami: {}},
	}
	srv.Properties.AvailabilityZone = new("1")
	srv.Properties.HighAvailability = &armmysqlflexibleservers.HighAvailability{
		Mode: new(armmysqlflexibleservers.HighAvailabilityModeZoneRedundant), StandbyAvailabilityZone: new("2"),
	}

	servers := s.myFactory(t).NewServersClient()
	cp, err := servers.BeginCreate(ctx, flexRG, "my-ha", srv, nil)
	myPoll(t, cp, err)

	plain := mySrv()
	pp, err := servers.BeginCreate(ctx, flexRG, "my-plain", plain, nil)
	myPoll(t, pp, err)

	_, err = servers.BeginFailover(ctx, flexRG, "my-plain", nil)
	require.Error(t, err)
	assert.Equal(t, "InvalidParameterValue", sdkErrCode(err))

	fp, err := servers.BeginFailover(ctx, flexRG, "my-ha", nil)
	myPoll(t, fp, err)

	got, err := servers.Get(ctx, flexRG, "my-ha", nil)
	require.NoError(t, err)
	assert.Equal(t, "2", *got.Properties.AvailabilityZone)
	assert.Equal(t, "1", *got.Properties.HighAvailability.StandbyAvailabilityZone)

	admins := s.myFactory(t).NewAzureADAdministratorsClient()
	in := armmysqlflexibleservers.AzureADAdministrator{Properties: &armmysqlflexibleservers.AdministratorProperties{
		AdministratorType: new(armmysqlflexibleservers.AdministratorTypeActiveDirectory), Login: new("dba"),
		Sid: new(flexOID), TenantID: new(flexTenant), IdentityResourceID: new(uami),
	}}

	other := in
	props := *in.Properties
	props.IdentityResourceID = new(uami + "x")
	other.Properties = &props

	_, err = admins.BeginCreateOrUpdate(
		ctx,
		flexRG,
		"my-ha",
		armmysqlflexibleservers.AdministratorNameActiveDirectory,
		other,
		nil,
	)
	require.Error(t, err)
	assert.Equal(t, "InvalidParameterValue", sdkErrCode(err))

	ap, err := admins.BeginCreateOrUpdate(
		ctx,
		flexRG,
		"my-ha",
		armmysqlflexibleservers.AdministratorNameActiveDirectory,
		in,
		nil,
	)
	res := myPoll(t, ap, err)
	assert.Equal(t, "dba", *res.Properties.Login)

	back, err := admins.Get(ctx, flexRG, "my-ha", armmysqlflexibleservers.AdministratorNameActiveDirectory, nil)
	require.NoError(t, err)
	assert.Equal(t, uami, *back.Properties.IdentityResourceID)

	n := 0

	for pager := admins.NewListByServerPager(flexRG, "my-ha", nil); pager.More(); {
		page, perr := pager.NextPage(ctx)
		require.NoError(t, perr)

		n += len(page.Value)
	}

	assert.Equal(t, 1, n)

	snap := s.handler.Snapshot(ctx)
	fresh := newFlexStack(t, nil)
	require.NoError(t, fresh.handler.Restore(ctx, snap))

	kept, err := fresh.myFactory(t).NewAzureADAdministratorsClient().
		Get(ctx, flexRG, "my-ha", armmysqlflexibleservers.AdministratorNameActiveDirectory, nil)
	require.NoError(t, err)
	assert.Equal(t, "dba", *kept.Properties.Login)

	dp, err := admins.BeginDelete(ctx, flexRG, "my-ha", armmysqlflexibleservers.AdministratorNameActiveDirectory, nil)
	myPoll(t, dp, err)
}
