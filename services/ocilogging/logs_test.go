package ocilogging_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func svcConfig(service, resource, category string) *logging.Configuration {
	return &logging.Configuration{Source: logging.OciService{
		Service: new(service), Resource: new(resource), Category: new(category),
	}}
}

func TestCreateLogValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mod    func(*logging.CreateLogDetails)
		name   string
		code   string
		status int
	}{
		{nil, "custom ok", "", 0},
		{func(d *logging.CreateLogDetails) { d.RetentionDuration = new(90) }, "retention 90", "", 0},
		{func(d *logging.CreateLogDetails) { d.RetentionDuration = new(45) }, "retention 45", "InvalidParameter", 400},
		{
			func(d *logging.CreateLogDetails) { d.RetentionDuration = new(210) },
			"retention 210",
			"InvalidParameter",
			400,
		},
		{func(d *logging.CreateLogDetails) { d.DisplayName = new("") }, "blank name", "MissingParameter", 400},
		{func(d *logging.CreateLogDetails) {
			d.Configuration = svcConfig("objectstorage", "b", "write")
		}, "custom with service source", "InvalidParameter", 400},
		{func(d *logging.CreateLogDetails) {
			d.LogType = logging.CreateLogDetailsLogTypeService
			d.Configuration = svcConfig("objectstorage", "bucket1", "write")
		}, "service ok", "", 0},
		{func(d *logging.CreateLogDetails) {
			d.LogType = logging.CreateLogDetailsLogTypeService
		}, "service without configuration", "MissingParameter", 400},
		{func(d *logging.CreateLogDetails) {
			d.LogType = logging.CreateLogDetailsLogTypeService
			d.Configuration = svcConfig("nosuch", "r", "c")
		}, "service unknown service", "InvalidParameter", 400},
		{func(d *logging.CreateLogDetails) {
			d.LogType = logging.CreateLogDetailsLogTypeService
			d.Configuration = svcConfig("objectstorage", "r", "nosuch")
		}, "service unknown category", "InvalidParameter", 400},
		{func(d *logging.CreateLogDetails) { d.DisplayName = new("existing") }, "duplicate name", "Conflict", 409},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			g := e.mkGroup(t, "g")
			e.mkLog(t, g, "existing", nil)

			d := logging.CreateLogDetails{DisplayName: new("l"), LogType: logging.CreateLogDetailsLogTypeCustom}
			if tt.mod != nil {
				tt.mod(&d)
			}

			resp, err := e.mgmt.CreateLog(t.Context(), logging.CreateLogRequest{LogGroupId: g.Id, CreateLogDetails: d})
			if tt.status == 0 {
				require.NoError(t, err)
				assert.NotEmpty(t, *resp.OpcWorkRequestId)

				return
			}

			se := svcErr(t, err)
			assert.Equal(t, tt.status, se.GetHTTPStatusCode())
			assert.Equal(t, tt.code, se.GetCode())
		})
	}
}

func TestLogLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	g := e.mkGroup(t, "g")
	l := e.mkLog(t, g, "app", func(d *logging.CreateLogDetails) { d.RetentionDuration = new(60) })

	assert.Equal(t, logging.LogLifecycleStateActive, l.LifecycleState)
	assert.Equal(t, logging.LogLogTypeCustom, l.LogType)
	assert.Equal(t, 60, *l.RetentionDuration)
	assert.True(t, *l.IsEnabled)
	assert.Equal(t, testCompartment, *l.CompartmentId)

	_, err := e.mgmt.UpdateLog(t.Context(), logging.UpdateLogRequest{
		LogGroupId: g.Id, LogId: l.Id, IfMatch: new("stale"),
		IsEnabled: new(false),
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.mgmt.UpdateLog(t.Context(), logging.UpdateLogRequest{
		LogGroupId: g.Id, LogId: l.Id,
		IsEnabled: new(false), DisplayName: new("app2"), RetentionDuration: new(120),
		FreeformTags: map[string]string{"a": "b"},
	})
	require.NoError(t, err)

	mid, err := e.mgmt.GetLog(t.Context(), logging.GetLogRequest{LogGroupId: g.Id, LogId: l.Id})
	require.NoError(t, err)
	assert.Equal(t, logging.LogLifecycleStateUpdating, mid.LifecycleState)

	e.settle()

	got, err := e.mgmt.GetLog(t.Context(), logging.GetLogRequest{LogGroupId: g.Id, LogId: l.Id})
	require.NoError(t, err)
	assert.Equal(t, logging.LogLifecycleStateInactive, got.LifecycleState)
	assert.Equal(t, "app2", *got.DisplayName)
	assert.Equal(t, 120, *got.RetentionDuration)
	assert.False(t, *got.IsEnabled)

	_, err = e.mgmt.GetLog(
		t.Context(),
		logging.GetLogRequest{LogGroupId: new("ocid1.loggroup.oc1..wrong"), LogId: l.Id},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.mgmt.DeleteLogGroup(t.Context(), logging.DeleteLogGroupRequest{LogGroupId: g.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.mgmt.DeleteLog(t.Context(), logging.DeleteLogRequest{LogGroupId: g.Id, LogId: l.Id})
	require.NoError(t, err)
	e.settle()

	_, err = e.mgmt.GetLog(t.Context(), logging.GetLogRequest{LogGroupId: g.Id, LogId: l.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.mgmt.DeleteLogGroup(t.Context(), logging.DeleteLogGroupRequest{LogGroupId: g.Id})
	require.NoError(t, err)
}

func TestServiceLogConfiguration(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	g := e.mkGroup(t, "g")
	l := e.mkLog(t, g, "bucket-write", func(d *logging.CreateLogDetails) {
		d.LogType = logging.CreateLogDetailsLogTypeService
		d.Configuration = svcConfig("objectstorage", "bucket1", "write")
	})

	svc, ok := l.Configuration.Source.(logging.OciService)
	require.True(t, ok)
	assert.Equal(t, "objectstorage", *svc.Service)
	assert.Equal(t, "bucket1", *svc.Resource)
	assert.Equal(t, "write", *svc.Category)

	dup, err := e.mgmt.CreateLog(t.Context(), logging.CreateLogRequest{LogGroupId: g.Id,
		DisplayName: new("other"), LogType: logging.CreateLogDetailsLogTypeService,
		Configuration: svcConfig("objectstorage", "bucket1", "write")})
	_ = dup
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.mgmt.UpdateLog(t.Context(), logging.UpdateLogRequest{
		LogGroupId: g.Id, LogId: l.Id,
		Configuration: &logging.UpdateConfigurationDetails{
			Source: &logging.SourceUpdateDetails{Parameters: map[string]string{"p": "1"}},
		},
	})
	require.NoError(t, err)
	e.settle()

	got, err := e.mgmt.GetLog(t.Context(), logging.GetLogRequest{LogGroupId: g.Id, LogId: l.Id})
	require.NoError(t, err)

	svc, ok = got.Configuration.Source.(logging.OciService)
	require.True(t, ok)
	assert.Equal(t, map[string]string{"p": "1"}, svc.Parameters)

	list, err := e.mgmt.ListLogs(t.Context(), logging.ListLogsRequest{
		LogGroupId: g.Id, SourceService: new("objectstorage"), LogType: logging.ListLogsLogTypeService,
	})
	require.NoError(t, err)
	assert.Len(t, list.Items, 1)

	list, err = e.mgmt.ListLogs(t.Context(), logging.ListLogsRequest{LogGroupId: g.Id, SourceService: new("functions")})
	require.NoError(t, err)
	assert.Empty(t, list.Items)

	svcs, err := e.mgmt.ListServices(t.Context(), logging.ListServicesRequest{})
	require.NoError(t, err)

	names := make([]string, 0, len(svcs.Items))
	for _, s := range svcs.Items {
		names = append(names, *s.Name)
	}

	assert.Contains(t, names, "objectstorage")
	assert.Contains(t, names, "functions")
}

func TestChangeLogLogGroup(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	src, dst := e.mkGroup(t, "src"), e.mkGroup(t, "dst")
	l := e.mkLog(t, src, "app", nil)
	e.mkLog(t, dst, "clash", nil)

	tests := []struct {
		target *string
		name   string
		status int
	}{
		{new("ocid1.loggroup.oc1..ghost"), "unknown target", http.StatusNotFound},
		{dst.Id, "moves", 0},
	}

	for _, tt := range tests {
		resp, err := e.mgmt.ChangeLogLogGroup(t.Context(), logging.ChangeLogLogGroupRequest{
			LogGroupId: src.Id, LogId: l.Id,
			TargetLogGroupId: tt.target,
		})
		if tt.status != 0 {
			assert.Equal(t, tt.status, svcErr(t, err).GetHTTPStatusCode(), tt.name)

			continue
		}

		require.NoError(t, err, tt.name)
		assert.NotEmpty(t, *resp.OpcWorkRequestId)
	}

	got, err := e.mgmt.GetLog(t.Context(), logging.GetLogRequest{LogGroupId: dst.Id, LogId: l.Id})
	require.NoError(t, err)
	assert.Equal(t, *dst.Id, *got.LogGroupId)
}
