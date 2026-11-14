package ocimysql_test

import (
	"net/http"
	"testing"
	"time"

	my "github.com/oracle/oci-go-sdk/v65/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDBSystemStatusStateOnly(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	db := e.mkDB(t, "status", nil)

	_, err := e.db.GetDbSystemStatus(ctx, my.GetDbSystemStatusRequest{DbSystemId: db.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode(), "nothing generated yet")

	gen, err := e.db.GenerateDbSystemStatus(ctx, my.GenerateDbSystemStatusRequest{
		DbSystemId: db.Id, GenerateDbSystemStatusDetails: my.GenerateDbSystemStatusDetails{},
	})
	require.NoError(t, err)
	require.NotNil(t, gen.OpcWorkRequestId)

	wr, err := e.wr.GetWorkRequest(ctx, my.GetWorkRequestRequest{WorkRequestId: gen.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, my.WorkRequestOperationTypeGenerateDbsystemStatus, wr.OperationType)
	assert.Equal(t, my.WorkRequestOperationStatusSucceeded, wr.Status)

	got, err := e.db.GetDbSystemStatus(ctx, my.GetDbSystemStatusRequest{DbSystemId: db.Id})
	require.NoError(t, err)

	res := got.DbSystemStatusResult
	require.NotNil(t, res)
	assert.True(t, *res.CanAcceptClientConnections)
	assert.True(t, *res.IsWritable)
	assert.False(t, *res.IsInOfflineMode)
	assert.True(t, *res.AreAllMysqlInstancesHealthy)
	assert.Nil(t, res.GtidSet, "a DB system without a server has no GTID set")

	_, err = e.db.StopDbSystem(ctx, my.StopDbSystemRequest{DbSystemId: db.Id, ShutdownType: my.InnoDbShutdownModeFast})
	require.NoError(t, err)
	e.settle()

	_, err = e.db.GenerateDbSystemStatus(ctx, my.GenerateDbSystemStatusRequest{DbSystemId: db.Id})
	require.NoError(t, err)

	stopped, err := e.db.GetDbSystemStatus(ctx, my.GetDbSystemStatusRequest{DbSystemId: db.Id})
	require.NoError(t, err)
	assert.False(t, *stopped.DbSystemStatusResult.CanAcceptClientConnections)
	assert.False(t, *stopped.DbSystemStatusResult.IsWritable)
}

func TestDBSystemStatusChannels(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	db := e.mkDB(t, "status", nil)
	other := e.mkDB(t, "other", nil)

	ch, err := e.ch.CreateChannel(ctx, my.CreateChannelRequest{CreateChannelDetails: channelDetails(db.Id)})
	require.NoError(t, err)
	e.settle()

	_, err = e.db.GenerateDbSystemStatus(ctx, my.GenerateDbSystemStatusRequest{
		DbSystemId: db.Id, ChannelIds: []string{*ch.Id},
	})
	require.NoError(t, err)

	got, err := e.db.GetDbSystemStatus(ctx, my.GetDbSystemStatusRequest{DbSystemId: db.Id})
	require.NoError(t, err)
	require.Len(t, got.DbSystemStatusResult.ChannelsStatus, 1)
	assert.Equal(t, *ch.Id, *got.DbSystemStatusResult.ChannelsStatus[0].ChannelId)
	assert.True(t, *got.DbSystemStatusResult.ChannelsStatus[0].IsHealthy)

	_, err = e.db.GenerateDbSystemStatus(ctx, my.GenerateDbSystemStatusRequest{
		DbSystemId: other.Id, ChannelIds: []string{*ch.Id},
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "a channel of another DB system")

	_, err = e.db.GenerateDbSystemStatus(ctx, my.GenerateDbSystemStatusRequest{
		DbSystemId: db.Id, GtidSetToApply: new("x'; DROP"),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "gtidSetToApply is validated")
}

func TestDBSystemStatusEngine(t *testing.T) {
	t.Parallel()

	e, _ := newEngineEnv(t)
	ctx := t.Context()
	db := e.mkEngineDB(t, "status", nil)

	_, err := e.db.GenerateDbSystemStatus(ctx, my.GenerateDbSystemStatusRequest{
		DbSystemId:     db.Id,
		GtidSetToApply: new("8b6d94d8-e080-11f1-b9cf-daf2b3bd5823:1-3"),
	})
	require.NoError(t, err)

	var res *my.DbSystemStatusResult

	require.Eventually(t, func() bool {
		got, gerr := e.db.GetDbSystemStatus(ctx, my.GetDbSystemStatusRequest{DbSystemId: db.Id})
		res = got.DbSystemStatusResult

		return gerr == nil && res != nil
	}, e.waitFor(), 20*time.Millisecond)

	assert.True(t, *res.CanAcceptClientConnections)
	assert.True(t, *res.IsWritable)
	assert.Equal(t, "uuid:1-5", *res.GtidSet, "the GTID set comes from the server")
	assert.True(t, *res.IsGtidSetApplied)
	assert.False(t, *res.IsStorageFull)

	_, err = e.db.GenerateDbSystemStatus(ctx, my.GenerateDbSystemStatusRequest{
		DbSystemId:  db.Id,
		GtidSetType: my.GenerateDbSystemStatusDetailsGtidSetTypeAvailable,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		got, gerr := e.db.GetDbSystemStatus(ctx, my.GetDbSystemStatusRequest{DbSystemId: db.Id})

		return gerr == nil && got.DbSystemStatusResult != nil && got.DbSystemStatusResult.GtidSet != nil &&
			*got.DbSystemStatusResult.GtidSet == "uuid:3-5"
	}, e.waitFor(), 20*time.Millisecond)
}

func TestChannelStatus(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	db := e.mkDB(t, "target", nil)

	ch, err := e.ch.CreateChannel(ctx, my.CreateChannelRequest{CreateChannelDetails: channelDetails(db.Id)})
	require.NoError(t, err)
	e.settle()

	_, err = e.ch.GetChannelStatus(ctx, my.GetChannelStatusRequest{ChannelId: ch.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	gen, err := e.ch.GenerateChannelStatus(ctx, my.GenerateChannelStatusRequest{ChannelId: ch.Id})
	require.NoError(t, err)
	require.NotNil(t, gen.OpcWorkRequestId)

	got, err := e.ch.GetChannelStatus(ctx, my.GetChannelStatusRequest{ChannelId: ch.Id})
	require.NoError(t, err)
	assert.Equal(t, *ch.Id, *got.ChannelStatusResult.ChannelId)
	assert.True(t, *got.ChannelStatusResult.IsHealthy)

	_, err = e.ch.UpdateChannel(ctx, my.UpdateChannelRequest{
		ChannelId: ch.Id, IsEnabled: new(false),
	})
	require.NoError(t, err)
	e.settle()

	_, err = e.ch.GenerateChannelStatus(ctx, my.GenerateChannelStatusRequest{ChannelId: ch.Id})
	require.NoError(t, err)

	paused, err := e.ch.GetChannelStatus(ctx, my.GetChannelStatusRequest{ChannelId: ch.Id})
	require.NoError(t, err)
	assert.False(t, *paused.ChannelStatusResult.IsHealthy, "a disabled channel is not healthy")

	_, err = e.ch.GenerateChannelStatus(
		ctx,
		my.GenerateChannelStatusRequest{ChannelId: new("ocid1.mysqlchannel.oc1.iad.nope")},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestHeatWaveMemoryEstimate(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	db := e.mkDB(t, "estimate", nil)

	_, err := e.db.GetHeatWaveClusterMemoryEstimate(ctx, my.GetHeatWaveClusterMemoryEstimateRequest{DbSystemId: db.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	gen, err := e.db.GenerateHeatWaveClusterMemoryEstimate(ctx, my.GenerateHeatWaveClusterMemoryEstimateRequest{
		DbSystemId: db.Id,
	})
	require.NoError(t, err)
	require.NotNil(t, gen.OpcWorkRequestId)

	got, err := e.db.GetHeatWaveClusterMemoryEstimate(
		ctx,
		my.GetHeatWaveClusterMemoryEstimateRequest{DbSystemId: db.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, my.HeatWaveClusterMemoryEstimateStatusSucceeded, got.Status)
	assert.Empty(t, got.TableSchemas, "a DB system without a server has no user tables")

	_, err = e.db.StopDbSystem(ctx, my.StopDbSystemRequest{DbSystemId: db.Id, ShutdownType: my.InnoDbShutdownModeFast})
	require.NoError(t, err)

	_, err = e.db.GenerateHeatWaveClusterMemoryEstimate(ctx, my.GenerateHeatWaveClusterMemoryEstimateRequest{
		DbSystemId: db.Id,
	})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "the DB system must be ACTIVE")
}

func TestHeatWaveMemoryEstimateEngine(t *testing.T) {
	t.Parallel()

	e, _ := newEngineEnv(t)
	ctx := t.Context()
	db := e.mkEngineDB(t, "estimate", nil)

	_, err := e.db.GenerateHeatWaveClusterMemoryEstimate(ctx, my.GenerateHeatWaveClusterMemoryEstimateRequest{
		DbSystemId: db.Id,
	})
	require.NoError(t, err)

	var got my.GetHeatWaveClusterMemoryEstimateResponse

	require.Eventually(t, func() bool {
		var gerr error

		got, gerr = e.db.GetHeatWaveClusterMemoryEstimate(ctx, my.GetHeatWaveClusterMemoryEstimateRequest{
			DbSystemId: db.Id,
		})

		return gerr == nil && got.Status == my.HeatWaveClusterMemoryEstimateStatusSucceeded
	}, e.waitFor(), 20*time.Millisecond)

	require.Len(t, got.TableSchemas, 2)
	assert.Equal(t, "shop", *got.TableSchemas[0].SchemaName)
	require.Len(t, got.TableSchemas[0].PerTableEstimates, 2)

	orders := got.TableSchemas[0].PerTableEstimates[0]
	assert.Equal(t, "orders", *orders.TableName)
	assert.Equal(t, 4, *orders.ToLoadColumnCount)
	assert.Equal(t, 2, *orders.VarlenColumnCount)
	assert.EqualValues(t, 120, *orders.EstimatedRowCount)
	assert.EqualValues(t, 4, *orders.AnalyticalFootprintInMbs, "3 MiB and a byte round up")
	assert.EqualValues(t, 1, *got.TableSchemas[0].PerTableEstimates[1].AnalyticalFootprintInMbs, "at least 1 MB")
	assert.Equal(t, "sales", *got.TableSchemas[1].SchemaName)
}

func TestMaintenanceEvents(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	db := e.mkDB(t, "maint", func(d *my.CreateDbSystemDetails) { d.MysqlVersion = new("8.4.3") })

	list := func(req my.ListMaintenanceEventsRequest) []my.MaintenanceEvent {
		req.DbSystemId = db.Id
		got, err := e.db.ListMaintenanceEvents(ctx, req)
		require.NoError(t, err)

		return got.Items
	}

	assert.Empty(t, list(my.ListMaintenanceEventsRequest{}))

	_, err := e.db.UpdateDbSystem(ctx, my.UpdateDbSystemRequest{
		DbSystemId: db.Id, MysqlVersion: new("8.4.6"),
	})
	require.NoError(t, err)
	e.settle()

	_, err = e.db.UpdateDbSystem(ctx, my.UpdateDbSystemRequest{
		DbSystemId: db.Id, ShapeName: new("MySQL.4"),
	})
	require.NoError(t, err)
	e.settle()

	all := list(my.ListMaintenanceEventsRequest{})
	require.Len(t, all, 2)

	assert.Equal(t, my.MaintenanceTypeShape, all[0].MaintenanceType, "newest first")
	assert.Equal(t, my.MaintenanceTypeManual, all[1].MaintenanceType)
	assert.Equal(t, my.MaintenanceActionDatabase, all[1].MaintenanceAction)
	assert.Equal(t, "8.4.3", *all[1].MysqlVersionBeforeMaintenance)
	assert.Equal(t, "8.4.6", *all[1].MysqlVersionAfterMaintenance)
	assert.Equal(t, my.MaintenanceEventMaintenanceStatusSucceeded, all[1].MaintenanceStatus)
	assert.NotNil(t, all[1].TimeEnded)

	assert.Len(
		t,
		list(my.ListMaintenanceEventsRequest{MaintenanceType: my.ListMaintenanceEventsMaintenanceTypeShape}),
		1,
	)
	assert.Len(t, list(my.ListMaintenanceEventsRequest{MysqlVersionBeforeMaintenance: new("8.4.3")}), 1)
	assert.Empty(
		t,
		list(my.ListMaintenanceEventsRequest{MaintenanceType: my.ListMaintenanceEventsMaintenanceTypeAutomatic}),
	)

	asc := list(my.ListMaintenanceEventsRequest{SortOrder: my.ListMaintenanceEventsSortOrderAsc})
	assert.Equal(t, my.MaintenanceTypeManual, asc[0].MaintenanceType)

	_, err = e.db.ListMaintenanceEvents(ctx, my.ListMaintenanceEventsRequest{
		DbSystemId: new("ocid1.mysqldbsystem.oc1.iad.nope"),
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestControlledUpdate(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	ha := e.mkDB(t, "ha", func(d *my.CreateDbSystemDetails) {
		d.IsHighlyAvailable, d.MysqlVersion = new(true), new("8.4.3")
	})
	plain := e.mkDB(t, "plain", nil)

	update := func(id *string, version string, target my.ControlledUpdateTargetDbInstancesEnum) error {
		_, err := e.db.ControlledUpdateDbSystem(ctx, my.ControlledUpdateDbSystemRequest{
			DbSystemId:         id,
			TargetMysqlVersion: &version, TargetDbInstances: target,
		})

		return err
	}

	assert.Equal(t, http.StatusBadRequest,
		svcErr(t, update(plain.Id, "8.4.6", my.ControlledUpdateTargetDbInstancesAllButPrimary)).GetHTTPStatusCode(),
		"a standalone DB system has nothing to update in steps")
	assert.Equal(t, http.StatusBadRequest,
		svcErr(t, update(ha.Id, "8.0.39", my.ControlledUpdateTargetDbInstancesAllButPrimary)).GetHTTPStatusCode(),
		"no downgrade")
	assert.Equal(t, http.StatusConflict,
		svcErr(t, update(ha.Id, "8.4.6", my.ControlledUpdateTargetDbInstancesPrimaryOnly)).GetHTTPStatusCode(),
		"the primary goes last")

	require.NoError(t, update(ha.Id, "8.4.6", my.ControlledUpdateTargetDbInstancesAllButPrimary))
	e.settle()

	mid := e.getDB(t, ha.Id)
	assert.Equal(t, "8.4.3", *mid.MysqlVersion, "the primary has not been upgraded yet")
	require.NotNil(t, mid.ControlledUpdate)
	assert.Equal(t, "8.4.6", *mid.ControlledUpdate.TargetMysqlVersion)
	assert.Equal(t, my.ControlledUpdateTargetDbInstancesAllButPrimary, mid.ControlledUpdate.TargetDbInstances)

	assert.Equal(t, http.StatusConflict,
		svcErr(t, update(ha.Id, "8.4.5", my.ControlledUpdateTargetDbInstancesPrimaryOnly)).GetHTTPStatusCode(),
		"PRIMARY_ONLY needs the same target version")

	require.NoError(t, update(ha.Id, "8.4.6", my.ControlledUpdateTargetDbInstancesPrimaryOnly))
	e.settle()

	done := e.getDB(t, ha.Id)
	assert.Equal(t, "8.4.6", *done.MysqlVersion)
	assert.Equal(t, my.ControlledUpdateTargetDbInstancesPrimaryOnly, done.ControlledUpdate.TargetDbInstances)

	events, err := e.db.ListMaintenanceEvents(ctx, my.ListMaintenanceEventsRequest{DbSystemId: ha.Id})
	require.NoError(t, err)
	require.Len(t, events.Items, 2)
	assert.Equal(t, my.MaintenanceScopePrimaryOnly, events.Items[0].MaintenanceScope)
	assert.Equal(t, my.MaintenanceScopeAllButPrimary, events.Items[1].MaintenanceScope)
}
