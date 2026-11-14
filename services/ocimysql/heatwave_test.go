package ocimysql_test

import (
	"net/http"
	"testing"

	my "github.com/oracle/oci-go-sdk/v65/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ocimysql "github.com/blackbirdworks/gopherstack/services/ocimysql"
)

const shapeHeatWave = "MySQL.HeatWave.VM.Standard"

func TestHeatWaveClusterLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	db := e.mkDB(t, "hw", nil)

	add, err := e.db.AddHeatWaveCluster(t.Context(), my.AddHeatWaveClusterRequest{
		DbSystemId: db.Id,
		ShapeName:  new(shapeHeatWave), ClusterSize: new(2),
	})
	require.NoError(t, err)
	assert.Equal(t, my.HeatWaveClusterLifecycleStateCreating, add.LifecycleState)
	assert.Len(t, add.ClusterNodes, 2)
	require.NotNil(t, add.OpcWorkRequestId)

	wr, err := e.wr.GetWorkRequest(t.Context(), my.GetWorkRequestRequest{WorkRequestId: add.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, my.WorkRequestOperationTypeAddHeatwaveCluster, wr.OperationType)

	got := e.getDB(t, db.Id)
	assert.True(t, *got.IsHeatWaveClusterAttached)
	require.NotNil(t, got.HeatWaveCluster)

	_, err = e.db.AddHeatWaveCluster(t.Context(), my.AddHeatWaveClusterRequest{
		DbSystemId: db.Id,
		ShapeName:  new(shapeHeatWave), ClusterSize: new(1),
	})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	e.settle()

	hw, err := e.db.GetHeatWaveCluster(t.Context(), my.GetHeatWaveClusterRequest{DbSystemId: db.Id})
	require.NoError(t, err)
	assert.Equal(t, my.HeatWaveClusterLifecycleStateActive, hw.LifecycleState)
	assert.Equal(t, 2, *hw.ClusterSize)

	_, err = e.db.StartHeatWaveCluster(t.Context(), my.StartHeatWaveClusterRequest{DbSystemId: db.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.db.StopHeatWaveCluster(t.Context(), my.StopHeatWaveClusterRequest{DbSystemId: db.Id})
	require.NoError(t, err)
	e.settle()

	hw, err = e.db.GetHeatWaveCluster(t.Context(), my.GetHeatWaveClusterRequest{DbSystemId: db.Id})
	require.NoError(t, err)
	assert.Equal(t, my.HeatWaveClusterLifecycleStateInactive, hw.LifecycleState)

	_, err = e.db.StartHeatWaveCluster(t.Context(), my.StartHeatWaveClusterRequest{DbSystemId: db.Id})
	require.NoError(t, err)
	e.settle()

	_, err = e.db.UpdateHeatWaveCluster(t.Context(), my.UpdateHeatWaveClusterRequest{
		DbSystemId: db.Id, IfMatch: new("stale"),
		ClusterSize: new(3),
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.db.UpdateHeatWaveCluster(t.Context(), my.UpdateHeatWaveClusterRequest{
		DbSystemId:  db.Id,
		ClusterSize: new(3), IsLakehouseEnabled: new(true),
	})
	require.NoError(t, err)
	e.settle()

	hw, err = e.db.GetHeatWaveCluster(t.Context(), my.GetHeatWaveClusterRequest{DbSystemId: db.Id})
	require.NoError(t, err)
	assert.Len(t, hw.ClusterNodes, 3)
	assert.True(t, *hw.IsLakehouseEnabled)

	list, err := e.db.ListDbSystems(t.Context(), my.ListDbSystemsRequest{
		CompartmentId: new(testCompartment), IsHeatWaveClusterAttached: new(true),
	})
	require.NoError(t, err)
	assert.Len(t, list.Items, 1)

	snap := e.backend.Snapshot(t.Context())
	restored := ocimysql.NewInMemoryBackend()
	require.NoError(t, restored.Restore(t.Context(), snap))
	_, _, err = restored.GetHeatWaveCluster(*db.Id)
	require.NoError(t, err)

	_, err = e.db.RestartHeatWaveCluster(t.Context(), my.RestartHeatWaveClusterRequest{DbSystemId: db.Id})
	require.NoError(t, err)
	e.settle()

	_, err = e.db.DeleteHeatWaveCluster(t.Context(), my.DeleteHeatWaveClusterRequest{DbSystemId: db.Id})
	require.NoError(t, err)
	e.settle()

	_, err = e.db.GetHeatWaveCluster(t.Context(), my.GetHeatWaveClusterRequest{DbSystemId: db.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
	assert.False(t, *e.getDB(t, db.Id).IsHeatWaveClusterAttached)
}

func TestHeatWaveClusterValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		shape string
		size  int
	}{
		{name: "db shape", shape: shapeSmall, size: 1},
		{name: "empty shape", shape: "", size: 1},
		{name: "zero size", shape: shapeHeatWave, size: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			db := e.mkDB(t, "hw", nil)

			_, err := e.db.AddHeatWaveCluster(t.Context(), my.AddHeatWaveClusterRequest{
				DbSystemId: db.Id,
				ShapeName:  new(tt.shape), ClusterSize: new(tt.size),
			})
			assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

			_, err = e.db.GetHeatWaveCluster(t.Context(), my.GetHeatWaveClusterRequest{DbSystemId: db.Id})
			assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestHeatWaveAddRetryToken(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	db := e.mkDB(t, "hw", nil)
	req := my.AddHeatWaveClusterRequest{
		DbSystemId: db.Id, OpcRetryToken: new("tok"),
		ShapeName: new(shapeHeatWave), ClusterSize: new(1),
	}

	a, err := e.db.AddHeatWaveCluster(t.Context(), req)
	require.NoError(t, err)

	b, err := e.db.AddHeatWaveCluster(t.Context(), req)
	require.NoError(t, err)
	assert.Equal(t, *a.OpcWorkRequestId, *b.OpcWorkRequestId)
}
