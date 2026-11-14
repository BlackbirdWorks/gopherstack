package ocimysql_test

import (
	"net/http"
	"testing"
	"time"

	my "github.com/oracle/oci-go-sdk/v65/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const shapeReplica = "MySQL.8"

func (e *env) mkReplicaDB(t *testing.T, name string) my.DbSystem {
	t.Helper()

	return e.mkDB(t, name, func(d *my.CreateDbSystemDetails) { d.ShapeName = new(shapeReplica) })
}

func (e *env) mkReplica(t *testing.T, db *string, name string) my.CreateReplicaResponse {
	t.Helper()

	resp, err := e.rp.CreateReplica(t.Context(), my.CreateReplicaRequest{
		DbSystemId: db, DisplayName: new(name),
	})
	require.NoError(t, err)

	return resp
}

func TestReplicaLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	db := e.mkReplicaDB(t, "primary")

	created := e.mkReplica(t, db.Id, "r1")
	assert.Equal(t, my.ReplicaLifecycleStateCreating, created.LifecycleState)
	assert.Equal(t, shapeReplica, *created.ShapeName, "the shape is inherited")
	assert.Equal(t, *db.MysqlVersion, *created.MysqlVersion)
	assert.Equal(t, *db.ConfigurationId, *created.ConfigurationId)
	assert.NotEqual(t, *db.IpAddress, *created.IpAddress)
	assert.Equal(t, *db.Port, *created.Port)
	require.NotNil(t, created.OpcWorkRequestId)

	wr, err := e.wr.GetWorkRequest(ctx, my.GetWorkRequestRequest{WorkRequestId: created.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, my.WorkRequestOperationTypeCreateReplica, wr.OperationType)
	assert.Equal(t, "replica", *wr.Resources[0].EntityType)

	e.settle()

	got, err := e.rp.GetReplica(ctx, my.GetReplicaRequest{ReplicaId: created.Id})
	require.NoError(t, err)
	assert.Equal(t, my.ReplicaLifecycleStateActive, got.LifecycleState)
	require.NotNil(t, got.Etag)

	withReplica := e.getDB(t, db.Id)
	require.Len(t, withReplica.Endpoints, 3, "the DB system, the replica and the read load balancer")

	types := map[string]string{}
	for _, ep := range withReplica.Endpoints {
		types[string(ep.ResourceType)] = *ep.IpAddress
	}

	assert.Equal(t, *created.IpAddress, types[string(my.DbSystemEndpointResourceTypeReadReplica)])
	assert.NotEmpty(t, types[string(my.DbSystemEndpointResourceTypeLoadBalancer)])
	assert.NotEqual(t, *created.IpAddress, types[string(my.DbSystemEndpointResourceTypeLoadBalancer)])

	_, err = e.rp.UpdateReplica(ctx, my.UpdateReplicaRequest{
		ReplicaId: created.Id, IfMatch: new("stale"),
		DisplayName: new("renamed"),
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	upd, err := e.rp.UpdateReplica(ctx, my.UpdateReplicaRequest{
		ReplicaId: created.Id, IfMatch: got.Etag,
		DisplayName: new("renamed"), IsDeleteProtected: new(true), FreeformTags: map[string]string{"k": "v"},
	})
	require.NoError(t, err)
	require.NotNil(t, upd.OpcWorkRequestId)

	renamed, err := e.rp.GetReplica(ctx, my.GetReplicaRequest{ReplicaId: created.Id})
	require.NoError(t, err)
	assert.Equal(t, "renamed", *renamed.DisplayName)
	assert.Equal(t, "v", renamed.FreeformTags["k"])
	assert.Equal(t, my.ReplicaLifecycleStateActive, renamed.LifecycleState)

	_, err = e.rp.DeleteReplica(ctx, my.DeleteReplicaRequest{ReplicaId: created.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "delete protection")

	_, err = e.rp.UpdateReplica(ctx, my.UpdateReplicaRequest{
		ReplicaId:         created.Id,
		IsDeleteProtected: new(false),
	})
	require.NoError(t, err)

	del, err := e.rp.DeleteReplica(ctx, my.DeleteReplicaRequest{ReplicaId: created.Id})
	require.NoError(t, err)
	require.NotNil(t, del.OpcWorkRequestId)

	e.settle()

	gone, err := e.rp.GetReplica(ctx, my.GetReplicaRequest{ReplicaId: created.Id})
	require.NoError(t, err)
	assert.Equal(t, my.ReplicaLifecycleStateDeleted, gone.LifecycleState)

	assert.Len(t, e.getDB(t, db.Id).Endpoints, 1, "a deleted replica leaves the endpoint list")
}

func TestReplicaCreateErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(t *testing.T, e *env, db my.DbSystem) my.CreateReplicaDetails
		name   string
		code   string
		status int
	}{
		{func(*testing.T, *env, my.DbSystem) my.CreateReplicaDetails {
			return my.CreateReplicaDetails{}
		}, "missing system", "MissingParameter", http.StatusBadRequest},
		{func(*testing.T, *env, my.DbSystem) my.CreateReplicaDetails {
			return my.CreateReplicaDetails{DbSystemId: new("ocid1.mysqldbsystem.oc1.iad.nope")}
		}, "unknown system", "NotAuthorizedOrNotFound", http.StatusNotFound},
		{func(t *testing.T, e *env, _ my.DbSystem) my.CreateReplicaDetails {
			t.Helper()

			return my.CreateReplicaDetails{DbSystemId: e.mkDB(t, "small", nil).Id}
		}, "small shape", "InvalidParameter", http.StatusBadRequest},
		{func(t *testing.T, e *env, _ my.DbSystem) my.CreateReplicaDetails {
			t.Helper()

			free := e.mkDB(t, "free", func(d *my.CreateDbSystemDetails) { d.ShapeName = new("MySQL.Free") })

			return my.CreateReplicaDetails{DbSystemId: free.Id}
		}, "free shape", "InvalidParameter", http.StatusBadRequest},
		{func(_ *testing.T, _ *env, db my.DbSystem) my.CreateReplicaDetails {
			return my.CreateReplicaDetails{
				DbSystemId: db.Id, ReplicaOverrides: &my.ReplicaOverrides{ShapeName: new("MySQL.4")},
			}
		}, "override below minimum", "InvalidParameter", http.StatusBadRequest},
		{func(_ *testing.T, _ *env, db my.DbSystem) my.CreateReplicaDetails {
			return my.CreateReplicaDetails{
				DbSystemId:       db.Id,
				ReplicaOverrides: &my.ReplicaOverrides{ShapeName: new("MySQL.VM.Standard.E4.2.32GB")},
			}
		}, "vm shape below minimum", "InvalidParameter", http.StatusBadRequest},
		{func(_ *testing.T, _ *env, db my.DbSystem) my.CreateReplicaDetails {
			return my.CreateReplicaDetails{
				DbSystemId: db.Id, ReplicaOverrides: &my.ReplicaOverrides{MysqlVersion: new("1.2.3")},
			}
		}, "unknown version", "InvalidParameter", http.StatusBadRequest},
		{func(_ *testing.T, _ *env, db my.DbSystem) my.CreateReplicaDetails {
			return my.CreateReplicaDetails{
				DbSystemId:       db.Id,
				ReplicaOverrides: &my.ReplicaOverrides{ConfigurationId: new("ocid1.mysqlconfiguration.oc1..nope")},
			}
		}, "config for other shape", "InvalidParameter", http.StatusBadRequest},
		{func(t *testing.T, e *env, db my.DbSystem) my.CreateReplicaDetails {
			t.Helper()

			_, err := e.db.StopDbSystem(t.Context(), my.StopDbSystemRequest{
				DbSystemId: db.Id, ShutdownType: my.InnoDbShutdownModeFast,
			})
			require.NoError(t, err)
			e.settle()

			return my.CreateReplicaDetails{DbSystemId: db.Id}
		}, "stopped system", "IncorrectState", http.StatusConflict},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			db := e.mkReplicaDB(t, "primary")

			_, err := e.rp.CreateReplica(t.Context(), my.CreateReplicaRequest{
				CreateReplicaDetails: tt.mutate(t, e, db),
			})
			se := svcErr(t, err)
			assert.Equal(t, tt.status, se.GetHTTPStatusCode())
			assert.Equal(t, tt.code, se.GetCode())
		})
	}
}

func TestReplicaLimit(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	db := e.mkReplicaDB(t, "primary")

	for i := range 18 {
		e.mkReplica(t, db.Id, "r"+string(rune('a'+i)))
	}

	_, err := e.rp.CreateReplica(t.Context(), my.CreateReplicaRequest{
		DbSystemId: db.Id,
	})
	se := svcErr(t, err)
	assert.Equal(t, http.StatusBadRequest, se.GetHTTPStatusCode())
	assert.Equal(t, "LimitExceeded", se.GetCode())

	ips := map[string]bool{*db.IpAddress: true}

	list, err := e.rp.ListReplicas(t.Context(), my.ListReplicasRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	require.Len(t, list.Items, 18)

	for _, r := range list.Items {
		assert.False(t, ips[*r.IpAddress], "every replica gets its own address")
		ips[*r.IpAddress] = true
	}
}

func TestReplicaOverridesAndInheritance(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	db := e.mkReplicaDB(t, "primary")

	plain := e.mkReplica(t, db.Id, "plain")
	over, err := e.rp.CreateReplica(ctx, my.CreateReplicaRequest{
		DbSystemId: db.Id, DisplayName: new("over"),
		ReplicaOverrides: &my.ReplicaOverrides{ShapeName: new("MySQL.16"), MysqlVersion: new("8.4.5")}})
	require.NoError(t, err)
	assert.Equal(t, "MySQL.16", *over.ShapeName)
	assert.Equal(t, "8.4.5", *over.MysqlVersion)
	assert.NotEqual(t, *db.ConfigurationId, *over.ConfigurationId, "a shape override gets that shape's configuration")
	assert.Equal(t, "MySQL.16", *over.ReplicaOverrides.ShapeName)

	e.settle()

	_, err = e.db.UpdateDbSystem(ctx, my.UpdateDbSystemRequest{
		DbSystemId: db.Id,
		ShapeName:  new("MySQL.32"), MysqlVersion: new("8.4.6"),
	})
	require.NoError(t, err)
	e.settle()

	gotPlain, err := e.rp.GetReplica(ctx, my.GetReplicaRequest{ReplicaId: plain.Id})
	require.NoError(t, err)
	assert.Equal(t, "MySQL.32", *gotPlain.ShapeName, "an inherited shape follows the DB system")

	gotOver, err := e.rp.GetReplica(ctx, my.GetReplicaRequest{ReplicaId: over.Id})
	require.NoError(t, err)
	assert.Equal(t, "MySQL.16", *gotOver.ShapeName, "an overridden shape does not")
	assert.Equal(t, "8.4.5", *gotOver.MysqlVersion)

	upToDate, err := e.rp.ListReplicas(ctx, my.ListReplicasRequest{
		CompartmentId: new(testCompartment), IsUpToDate: new(true),
	})
	require.NoError(t, err)
	require.Len(t, upToDate.Items, 1)
	assert.Equal(t, *plain.Id, *upToDate.Items[0].Id)

	_, err = e.rp.UpdateReplica(ctx, my.UpdateReplicaRequest{
		ReplicaId:        over.Id,
		ReplicaOverrides: &my.ReplicaOverrides{ShapeName: new(""), MysqlVersion: new("")},
	})
	require.NoError(t, err)
	e.settle()

	cleared, err := e.rp.GetReplica(ctx, my.GetReplicaRequest{ReplicaId: over.Id})
	require.NoError(t, err)
	assert.Equal(t, "MySQL.32", *cleared.ShapeName, "an empty override is inherited again")
	assert.Equal(t, my.ReplicaLifecycleStateActive, cleared.LifecycleState)

	_, err = e.db.UpdateDbSystem(ctx, my.UpdateDbSystemRequest{
		DbSystemId: db.Id,
		ShapeName:  new("MySQL.4"),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "replicas need a supported shape")
}

func TestReplicaFollowsDBSystem(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	db := e.mkReplicaDB(t, "primary")
	r := e.mkReplica(t, db.Id, "r1")
	e.settle()

	_, err := e.db.StopDbSystem(ctx, my.StopDbSystemRequest{DbSystemId: db.Id, ShutdownType: my.InnoDbShutdownModeFast})
	require.NoError(t, err)
	e.settle()

	got, err := e.rp.GetReplica(ctx, my.GetReplicaRequest{ReplicaId: r.Id})
	require.NoError(t, err)
	assert.Equal(t, my.ReplicaLifecycleStateInactive, got.LifecycleState, "replicas stop with the DB system")

	_, err = e.db.StartDbSystem(ctx, my.StartDbSystemRequest{DbSystemId: db.Id})
	require.NoError(t, err)
	e.settle()

	got, err = e.rp.GetReplica(ctx, my.GetReplicaRequest{ReplicaId: r.Id})
	require.NoError(t, err)
	assert.Equal(t, my.ReplicaLifecycleStateActive, got.LifecycleState, "replicas start with the DB system")

	_, err = e.rp.UpdateReplica(ctx, my.UpdateReplicaRequest{
		ReplicaId:         r.Id,
		IsDeleteProtected: new(true),
	})
	require.NoError(t, err)

	_, err = e.db.DeleteDbSystem(ctx, my.DeleteDbSystemRequest{DbSystemId: db.Id})
	require.NoError(t, err)
	e.settle()

	got, err = e.rp.GetReplica(ctx, my.GetReplicaRequest{ReplicaId: r.Id})
	require.NoError(t, err)
	assert.Equal(t, my.ReplicaLifecycleStateDeleted, got.LifecycleState, "replicas go with the DB system")
}

func TestReplicaListFilters(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	db1, db2 := e.mkReplicaDB(t, "one"), e.mkReplicaDB(t, "two")
	r1 := e.mkReplica(t, db1.Id, "alpha")
	e.mkReplica(t, db2.Id, "beta")
	e.settle()

	tests := []struct {
		req  my.ListReplicasRequest
		name string
		want int
	}{
		{my.ListReplicasRequest{}, "all", 2},
		{my.ListReplicasRequest{DbSystemId: db1.Id}, "by system", 1},
		{my.ListReplicasRequest{ReplicaId: r1.Id}, "by id", 1},
		{my.ListReplicasRequest{DisplayName: new("beta")}, "by name", 1},
		{my.ListReplicasRequest{LifecycleState: my.ReplicaSummaryLifecycleStateInactive}, "by state", 0},
		{my.ListReplicasRequest{ConfigurationId: db1.ConfigurationId}, "by config", 2},
		{my.ListReplicasRequest{CompartmentId: new(otherCompart)}, "other compartment", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.req.CompartmentId == nil {
				tt.req.CompartmentId = new(testCompartment)
			}

			got, err := e.rp.ListReplicas(ctx, tt.req)
			require.NoError(t, err)
			assert.Len(t, got.Items, tt.want)
		})
	}

	status, _ := rawDo(t, http.MethodGet, e.base+"/20190415/replicas", "")
	assert.Equal(t, http.StatusBadRequest, status, "compartmentId is required")
}

func TestReplicaRetryToken(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	db := e.mkReplicaDB(t, "primary")

	create := func() my.CreateReplicaResponse {
		resp, err := e.rp.CreateReplica(t.Context(), my.CreateReplicaRequest{
			DbSystemId: db.Id, OpcRetryToken: new("tok"),
		})
		require.NoError(t, err)

		return resp
	}

	first, second := create(), create()
	assert.Equal(t, *first.Id, *second.Id)
	assert.Equal(t, *first.OpcWorkRequestId, *second.OpcWorkRequestId)
}

func TestReplicaSnapshotRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	db := e.mkReplicaDB(t, "primary")
	r := e.mkReplica(t, db.Id, "r1")
	e.settle()

	restored := newEnv(t, envOpts{})
	require.NoError(t, restored.backend.Restore(ctx, e.backend.Snapshot(ctx)))
	restored.clock.ns.Store(e.clock.ns.Load())

	got, err := restored.rp.GetReplica(ctx, my.GetReplicaRequest{ReplicaId: r.Id})
	require.NoError(t, err)
	assert.Equal(t, "r1", *got.DisplayName)
	assert.Equal(t, *r.IpAddress, *got.IpAddress)
	assert.Equal(t, my.ReplicaLifecycleStateActive, got.LifecycleState)
}

func TestReplicaEngine(t *testing.T) {
	t.Parallel()

	e, l := newEngineEnv(t)
	ctx := t.Context()
	db := e.mkEngineDB(t, "primary", func(d *my.CreateDbSystemDetails) { d.ShapeName = new(shapeReplica) })
	l.addTable(*db.Id, testTable)

	created := e.mkReplica(t, db.Id, "r1")
	require.Eventually(t, func() bool {
		r, err := e.rp.GetReplica(ctx, my.GetReplicaRequest{ReplicaId: created.Id})

		return err == nil && r.LifecycleState == my.ReplicaLifecycleStateActive
	}, e.waitFor(), 20*time.Millisecond)

	got, err := e.rp.GetReplica(ctx, my.GetReplicaRequest{ReplicaId: created.Id})
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1", *got.IpAddress, "the replica endpoint is its own server")
	assert.NotEqual(t, *db.Port, *got.Port)

	l.mu.Lock()
	src, rep := l.insts[*db.Id], l.insts[*created.Id]
	srcSpec, repSpec := l.specs[0], l.specs[1]
	l.mu.Unlock()

	require.NotNil(t, rep)
	assert.NotZero(t, srcSpec.ServerID, "the source starts replication-ready")
	assert.NotZero(t, repSpec.ServerID)
	assert.NotEqual(t, srcSpec.ServerID, repSpec.ServerID)
	assert.True(t, repSpec.Fresh)
	assert.NotEmpty(t, repSpec.Network, "the replica joins the source's network")
	assert.NotEqual(t, srcSpec.RootSecret, repSpec.RootSecret)

	l.mu.Lock()
	assert.Equal(t, testTable, string(rep.seed), "the replica is seeded from the source")
	assert.Equal(t, []string{testTable}, rep.tables)
	require.NotNil(t, rep.source)
	assert.Equal(t, adminPass, rep.admins[adminUser], "the replica has the administrator account")
	assert.NotNil(t, src)
	l.mu.Unlock()

	_, err = e.db.StopDbSystem(ctx, my.StopDbSystemRequest{DbSystemId: db.Id, ShutdownType: my.InnoDbShutdownModeFast})
	require.NoError(t, err)
	e.awaitState(t, db.Id, my.DbSystemLifecycleStateInactive)

	l.mu.Lock()
	assert.True(t, rep.stopped, "the replica server stops with the DB system")
	l.mu.Unlock()

	r, err := e.rp.GetReplica(ctx, my.GetReplicaRequest{ReplicaId: created.Id})
	require.NoError(t, err)
	assert.Equal(t, my.ReplicaLifecycleStateInactive, r.LifecycleState)

	_, err = e.db.StartDbSystem(ctx, my.StartDbSystemRequest{DbSystemId: db.Id})
	require.NoError(t, err)
	e.awaitState(t, db.Id, my.DbSystemLifecycleStateActive)

	l.mu.Lock()
	assert.False(t, rep.stopped)
	l.mu.Unlock()

	_, err = e.rp.DeleteReplica(ctx, my.DeleteReplicaRequest{ReplicaId: created.Id})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		x, gerr := e.rp.GetReplica(ctx, my.GetReplicaRequest{ReplicaId: created.Id})

		return gerr == nil && x.LifecycleState == my.ReplicaLifecycleStateDeleted
	}, e.waitFor(), 20*time.Millisecond)

	l.mu.Lock()
	assert.True(t, rep.closed)
	assert.True(t, rep.purged, "deleting a replica removes its volume")
	l.mu.Unlock()
}

func TestReplicaEngineVersionUpgrade(t *testing.T) {
	t.Parallel()

	e, l := newEngineEnv(t)
	ctx := t.Context()
	db := e.mkEngineDB(t, "primary", func(d *my.CreateDbSystemDetails) {
		d.ShapeName, d.MysqlVersion = new(shapeReplica), new("8.4.5")
	})
	plain := e.mkReplica(t, db.Id, "plain")
	pinned, err := e.rp.CreateReplica(ctx, my.CreateReplicaRequest{
		DbSystemId: db.Id, ReplicaOverrides: &my.ReplicaOverrides{MysqlVersion: new("8.4.5")}})
	require.NoError(t, err)

	for _, id := range []*string{plain.Id, pinned.Id} {
		require.Eventually(t, func() bool {
			r, gerr := e.rp.GetReplica(ctx, my.GetReplicaRequest{ReplicaId: id})

			return gerr == nil && r.LifecycleState == my.ReplicaLifecycleStateActive
		}, e.waitFor(), 20*time.Millisecond)
	}

	l.mu.Lock()
	before := len(l.specs)
	l.mu.Unlock()

	_, err = e.db.UpdateDbSystem(ctx, my.UpdateDbSystemRequest{
		DbSystemId: db.Id, MysqlVersion: new("9.1.0"),
	})
	require.NoError(t, err)
	e.awaitState(t, db.Id, my.DbSystemLifecycleStateActive)

	l.mu.Lock()
	defer l.mu.Unlock()

	launched := map[string]string{}
	for _, sp := range l.specs[before:] {
		launched[sp.ID] = sp.Image
	}

	assert.Equal(t, "mysql:9", launched[*db.Id])
	assert.Equal(t, "mysql:9", launched[*plain.Id], "a replica that inherits the version is relaunched on it")
	assert.NotContains(t, launched, *pinned.Id, "an overridden version is left alone")
}
