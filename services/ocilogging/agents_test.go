package ocilogging_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func loggingServiceConfig(
	logID *string,
	sources ...logging.UnifiedAgentLoggingSource,
) logging.UnifiedAgentLoggingConfiguration {
	return logging.UnifiedAgentLoggingConfiguration{
		Sources:     sources,
		Destination: &logging.UnifiedAgentLoggingDestination{LogObjectId: logID},
	}
}

func tailSource(name string) logging.UnifiedAgentTailLogSource {
	return logging.UnifiedAgentTailLogSource{
		Name: new(name), Paths: []string{"/var/log/app.log"},
		Parser: logging.UnifiedAgentNoneParser{},
	}
}

func TestUnifiedAgentConfigurationLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	g := e.mkGroup(t, "g")
	l := e.mkLog(t, g, "app", nil)

	create := func(mod func(*logging.CreateUnifiedAgentConfigurationDetails)) error {
		d := logging.CreateUnifiedAgentConfigurationDetails{
			CompartmentId: new(testCompartment), DisplayName: new("agent"), IsEnabled: new(true),
			Description:          new("tail the app log"),
			ServiceConfiguration: loggingServiceConfig(l.Id, tailSource("app")),
			GroupAssociation:     &logging.GroupAssociationDetails{GroupList: []string{"ocid1.dynamicgroup.oc1..dg"}},
		}
		if mod != nil {
			mod(&d)
		}

		_, err := e.mgmt.CreateUnifiedAgentConfiguration(t.Context(), logging.CreateUnifiedAgentConfigurationRequest{
			CreateUnifiedAgentConfigurationDetails: d,
		})

		return err
	}

	failures := []struct {
		mod  func(*logging.CreateUnifiedAgentConfigurationDetails)
		name string
	}{
		{func(d *logging.CreateUnifiedAgentConfigurationDetails) {
			d.ServiceConfiguration = loggingServiceConfig(new("ocid1.log.oc1..ghost"), tailSource("a"))
		}, "unknown destination log"},
		{func(d *logging.CreateUnifiedAgentConfigurationDetails) {
			d.ServiceConfiguration = loggingServiceConfig(l.Id, []logging.UnifiedAgentLoggingSource{}...)
		}, "no sources"},
		{func(d *logging.CreateUnifiedAgentConfigurationDetails) { d.DisplayName = new("") }, "blank name"},
	}

	for _, f := range failures {
		assert.Equal(t, http.StatusBadRequest, svcErr(t, create(f.mod)).GetHTTPStatusCode(), f.name)
	}

	require.NoError(t, create(nil))
	e.settle()

	list, err := e.mgmt.ListUnifiedAgentConfigurations(t.Context(), logging.ListUnifiedAgentConfigurationsRequest{
		CompartmentId: new(testCompartment), LogId: l.Id,
	})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, logging.UnifiedAgentServiceConfigurationTypesLogging, list.Items[0].ConfigurationType)
	assert.Equal(t, logging.LogLifecycleStateActive, list.Items[0].LifecycleState)

	got, err := e.mgmt.GetUnifiedAgentConfiguration(t.Context(), logging.GetUnifiedAgentConfigurationRequest{
		UnifiedAgentConfigurationId: list.Items[0].Id,
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"ocid1.dynamicgroup.oc1..dg"}, got.GroupAssociation.GroupList)

	cfg, ok := got.ServiceConfiguration.(logging.UnifiedAgentLoggingConfiguration)
	require.True(t, ok)
	require.Len(t, cfg.Sources, 1)
	assert.Equal(t, *l.Id, *cfg.Destination.LogObjectId)

	_, err = e.mgmt.UpdateUnifiedAgentConfiguration(t.Context(), logging.UpdateUnifiedAgentConfigurationRequest{
		UnifiedAgentConfigurationId: got.Id,
		DisplayName:                 new("agent2"), IsEnabled: new(false),
		ServiceConfiguration: loggingServiceConfig(l.Id, tailSource("a"), tailSource("b")),
	})
	require.NoError(t, err)
	e.settle()

	got, err = e.mgmt.GetUnifiedAgentConfiguration(t.Context(), logging.GetUnifiedAgentConfigurationRequest{
		UnifiedAgentConfigurationId: got.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, "agent2", *got.DisplayName)
	assert.False(t, *got.IsEnabled)

	_, err = e.mgmt.ChangeUnifiedAgentConfigurationCompartment(t.Context(),
		logging.ChangeUnifiedAgentConfigurationCompartmentRequest{
			UnifiedAgentConfigurationId: got.Id,
			CompartmentId:               new("ocid1.compartment.oc1..moved"),
		})
	require.NoError(t, err)

	_, err = e.mgmt.DeleteUnifiedAgentConfiguration(t.Context(), logging.DeleteUnifiedAgentConfigurationRequest{
		UnifiedAgentConfigurationId: got.Id,
	})
	require.NoError(t, err)
	e.settle()

	_, err = e.mgmt.GetUnifiedAgentConfiguration(t.Context(), logging.GetUnifiedAgentConfigurationRequest{
		UnifiedAgentConfigurationId: got.Id,
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}
