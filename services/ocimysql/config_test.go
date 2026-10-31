package ocimysql_test

import (
	"net/http"
	"testing"

	my "github.com/oracle/oci-go-sdk/v65/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfigurations(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()

	lst, err := e.aas.ListConfigurations(ctx, my.ListConfigurationsRequest{
		CompartmentId: new(testCompartment), ShapeName: new(shapeSmall),
		SortBy: my.ListConfigurationsSortByDisplayname, SortOrder: my.ListConfigurationsSortOrderAsc,
	})
	require.NoError(t, err)
	require.Len(t, lst.Items, 2)
	assert.Equal(t, "MySQL.2.HA", *lst.Items[0].DisplayName)
	assert.Equal(t, "MySQL.2.Standalone", *lst.Items[1].DisplayName)

	for _, c := range lst.Items {
		assert.Equal(t, my.ConfigurationTypeDefault, c.Type)
		assert.Equal(t, my.ConfigurationLifecycleStateActive, c.LifecycleState)
	}

	free, err := e.aas.ListConfigurations(ctx, my.ListConfigurationsRequest{
		CompartmentId: new(testCompartment), ShapeName: new("MySQL.Free"),
	})
	require.NoError(t, err)
	assert.Len(t, free.Items, 1, "the free shape has no HA configuration")

	got, err := e.aas.GetConfiguration(ctx, my.GetConfigurationRequest{ConfigurationId: lst.Items[1].Id})
	require.NoError(t, err)
	assert.Equal(t, 16*16, *got.Variables.MaxConnections)
	assert.True(t, *got.Variables.Autocommit)
	assert.NotNil(t, got.Options)

	_, err = e.aas.UpdateConfiguration(ctx, my.UpdateConfigurationRequest{
		ConfigurationId: got.Id, DisplayName: new("x"),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "defaults are read-only")

	_, err = e.aas.DeleteConfiguration(ctx, my.DeleteConfigurationRequest{ConfigurationId: got.Id})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	db := e.mkDB(t, "cfg", nil)
	assert.Equal(t, *lst.Items[1].Id, *db.ConfigurationId, "standalone default for the shape")

	ha := e.mkDB(t, "cfgha", func(d *my.CreateDbSystemDetails) { d.IsHighlyAvailable = new(true) })
	assert.Equal(t, *lst.Items[0].Id, *ha.ConfigurationId, "HA default for HA systems")
}

func TestCustomConfigurationLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()

	created, err := e.aas.CreateConfiguration(ctx, my.CreateConfigurationRequest{
		CompartmentId: new(testCompartment),
		ShapeName:     new(shapeSmall),
		DisplayName:   new("tuned"),
		Variables: &my.ConfigurationVariables{
			MaxConnections: new(777), Autocommit: new(false), SqlMode: new("STRICT_ALL_TABLES"),
			BinlogExpireLogsSeconds: new(86400),
		},
		InitVariables: &my.InitializationVariables{
			LowerCaseTableNames: my.InitializationVariablesLowerCaseTableNamesInsensitiveLowercase,
		},
	})
	require.NoError(t, err)

	c := created.Configuration
	assert.Equal(t, my.ConfigurationTypeCustom, c.Type)
	assert.Equal(t, 777, *c.Variables.MaxConnections)
	assert.False(t, *c.Variables.Autocommit)
	assert.NotNil(t, c.Variables.InnodbBufferPoolSize, "unset variables inherit from the shape default")
	assert.NotEmpty(t, *c.ParentConfigurationId)
	assert.Equal(
		t,
		my.InitializationVariablesLowerCaseTableNamesInsensitiveLowercase,
		c.InitVariables.LowerCaseTableNames,
	)

	upd, err := e.aas.UpdateConfiguration(ctx, my.UpdateConfigurationRequest{
		ConfigurationId: c.Id, IfMatch: created.Etag,
		DisplayName: new("renamed"), Description: new("d"),
	})
	require.NoError(t, err)
	assert.Equal(t, "d", *upd.Description)
	assert.Equal(t, 777, *upd.Variables.MaxConnections, "variables are immutable after create")

	_, err = e.aas.UpdateConfiguration(ctx, my.UpdateConfigurationRequest{
		ConfigurationId: c.Id, IfMatch: new("stale"),
		DisplayName: new("n"),
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	lst, err := e.aas.ListConfigurations(ctx, my.ListConfigurationsRequest{
		CompartmentId: new(testCompartment), Type: []my.ListConfigurationsTypeEnum{my.ListConfigurationsTypeCustom},
	})
	require.NoError(t, err)
	require.Len(t, lst.Items, 1)
	assert.Equal(t, "renamed", *lst.Items[0].DisplayName)

	other, err := e.aas.ListConfigurations(ctx, my.ListConfigurationsRequest{
		CompartmentId: new(otherCompart), Type: []my.ListConfigurationsTypeEnum{my.ListConfigurationsTypeCustom},
	})
	require.NoError(t, err)
	assert.Empty(t, other.Items, "custom configurations are per compartment")

	db := e.mkDB(t, "uses", func(d *my.CreateDbSystemDetails) { d.ConfigurationId = c.Id })
	assert.Equal(t, *c.Id, *db.ConfigurationId)

	_, err = e.aas.DeleteConfiguration(ctx, my.DeleteConfigurationRequest{ConfigurationId: c.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "in use")

	_, err = e.db.DeleteDbSystem(ctx, my.DeleteDbSystemRequest{DbSystemId: db.Id})
	require.NoError(t, err)
	e.settle()

	_, err = e.aas.DeleteConfiguration(ctx, my.DeleteConfigurationRequest{ConfigurationId: c.Id})
	require.NoError(t, err)

	_, err = e.aas.GetConfiguration(ctx, my.GetConfigurationRequest{ConfigurationId: c.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestConfigurationValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{
			"unknown variable",
			`{"compartmentId":"` + testCompartment + `","shapeName":"MySQL.2","variables":{"notAVariable":1}}`,
		},
		{
			"wrong type int",
			`{"compartmentId":"` + testCompartment + `","shapeName":"MySQL.2","variables":{"maxConnections":"many"}}`,
		},
		{
			"fractional int",
			`{"compartmentId":"` + testCompartment + `","shapeName":"MySQL.2","variables":{"maxConnections":1.5}}`,
		},
		{
			"wrong type bool",
			`{"compartmentId":"` + testCompartment + `","shapeName":"MySQL.2","variables":{"autocommit":"yes"}}`,
		},
		{
			"wrong type string",
			`{"compartmentId":"` + testCompartment + `","shapeName":"MySQL.2","variables":{"sqlMode":5}}`,
		},
		{"bad shape", `{"compartmentId":"` + testCompartment + `","shapeName":"nope"}`},
		{"heatwave shape", `{"compartmentId":"` + testCompartment + `","shapeName":"MySQL.HeatWave.VM.Standard"}`},
		{"no shape", `{"compartmentId":"` + testCompartment + `"}`},
		{"no compartment", `{"shapeName":"MySQL.2"}`},
		{
			"unknown parent",
			`{"compartmentId":"` + testCompartment + `","shapeName":"MySQL.2",` +
				`"parentConfigurationId":"ocid1.mysqlconfiguration.oc1..nope"}`,
		},
		{
			"bad lower case",
			`{"compartmentId":"` + testCompartment + `","shapeName":"MySQL.2","initVariables":{"lowerCaseTableNames":"MAYBE"}}`,
		},
	}

	e := newEnv(t, envOpts{})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status, body := rawDo(t, http.MethodPost, e.base+"/20190415/configurations", tt.body)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Contains(t, body, `"code":"`)
		})
	}

	e2 := newEnv(t, envOpts{})
	custom, err := e2.aas.CreateConfiguration(t.Context(), my.CreateConfigurationRequest{
		CompartmentId: new(testCompartment),
		ShapeName:     new(shapeBig),
	})
	require.NoError(t, err)

	_, err = e2.db.CreateDbSystem(
		t.Context(),
		my.CreateDbSystemRequest{CreateDbSystemDetails: func() my.CreateDbSystemDetails {
			d := dbDetails("mismatch")
			d.ConfigurationId = custom.Id

			return d
		}()},
	)
	assert.Equal(
		t,
		http.StatusBadRequest,
		svcErr(t, err).GetHTTPStatusCode(),
		"configuration shape must match the DB system shape",
	)
}
