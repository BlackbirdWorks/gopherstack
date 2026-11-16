package ocimysql_test

import (
	"net/http"
	"testing"
	"time"

	my "github.com/oracle/oci-go-sdk/v65/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const bgPassword = "Bg#Passw0rd1"

func bgDetails(src *string, mod func(*my.CreateBlueGreenDeploymentDetails)) my.CreateBlueGreenDeploymentDetails {
	d := my.CreateBlueGreenDeploymentDetails{
		CompartmentId: new(testCompartment), DisplayName: new("bg"), SourceDbSystemId: src,
		TargetDbSystemDetails: &my.CreateBlueGreenDeploymentTargetDbSystemDetails{MysqlVersion: new("9.7.4")},
		ChannelDetails: &my.CreateBlueGreenDeploymentChannelDetails{
			SourceUsername: new("repl"), SourcePassword: new(bgPassword), SslMode: my.SslModeRequired,
		},
	}
	if mod != nil {
		mod(&d)
	}

	return d
}

func (e *env) mkBlue(t *testing.T) my.DbSystem {
	t.Helper()

	return e.mkDB(t, "blue", func(d *my.CreateDbSystemDetails) { d.MysqlVersion = new("8.4.6") })
}

func (e *env) getBG(t *testing.T, id *string) my.BlueGreenDeployment {
	t.Helper()

	r, err := e.bg.GetBlueGreenDeployment(t.Context(), my.GetBlueGreenDeploymentRequest{BlueGreenDeploymentId: id})
	require.NoError(t, err)

	return r.BlueGreenDeployment
}

func TestBlueGreenStubLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	blue := e.mkBlue(t)

	created, err := e.bg.CreateBlueGreenDeployment(ctx, my.CreateBlueGreenDeploymentRequest{
		CreateBlueGreenDeploymentDetails: bgDetails(blue.Id, nil),
	})
	require.NoError(t, err)
	require.NotNil(t, created.OpcWorkRequestId)
	assert.Equal(t, my.BlueGreenDeploymentLifecycleStateCreating, created.LifecycleState)
	assert.Equal(t, *blue.Id, *created.ActiveDbSystemId)
	assert.Equal(t, "9.7.4", *created.TargetDbSystemDetails.MysqlVersion)
	assert.Equal(t, my.SslModeRequired, created.SslMode)

	e.settle()

	got := e.getBG(t, created.Id)
	assert.Equal(t, my.BlueGreenDeploymentLifecycleStateActive, got.LifecycleState)
	assert.Equal(t, my.BlueGreenDeploymentSwitchoverStatusReadyForSwitchover, got.SwitchoverStatus)

	green := e.getDB(t, got.TargetDbSystemId)
	assert.Equal(t, "9.7.4", *green.MysqlVersion)
	assert.Equal(t, my.DbSystemLifecycleStateActive, green.LifecycleState)

	ch, err := e.ch.GetChannel(ctx, my.GetChannelRequest{ChannelId: got.ReplicationChannelId})
	require.NoError(t, err)
	assert.Equal(t, my.ChannelLifecycleStateActive, ch.LifecycleState)

	list, err := e.bg.ListBlueGreenDeployments(ctx, my.ListBlueGreenDeploymentsRequest{
		CompartmentId: new(testCompartment), SourceDbSystemId: blue.Id,
	})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)

	_, err = e.bg.UpdateBlueGreenDeployment(ctx, my.UpdateBlueGreenDeploymentRequest{
		BlueGreenDeploymentId: created.Id, DisplayName: new("renamed"),
	})
	require.NoError(t, err)
	assert.Equal(t, "renamed", *e.getBG(t, created.Id).DisplayName)

	blueIP, greenIP := *e.getDB(t, blue.Id).IpAddress, *green.IpAddress

	sw, err := e.bg.SwitchoverBlueGreenDeployment(
		ctx,
		my.SwitchoverBlueGreenDeploymentRequest{BlueGreenDeploymentId: created.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, *got.TargetDbSystemId, *sw.ActiveDbSystemId, "the green DB system becomes the active one")
	assert.Equal(t, my.BlueGreenDeploymentLifecycleStateUpdating, sw.LifecycleState)
	assert.Equal(t, greenIP, *e.getDB(t, blue.Id).IpAddress, "the endpoints are exchanged")
	assert.Equal(t, blueIP, *e.getDB(t, got.TargetDbSystemId).IpAddress)

	e.settle()

	done := e.getBG(t, created.Id)
	assert.Equal(t, my.BlueGreenDeploymentLifecycleStateActive, done.LifecycleState)
	assert.Equal(t, my.BlueGreenDeploymentSwitchoverStatusSwitchoverCompleted, done.SwitchoverStatus)

	_, err = e.db.DeleteDbSystem(ctx, my.DeleteDbSystemRequest{DbSystemId: blue.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "a member DB system cannot be deleted")

	_, err = e.bg.DeleteBlueGreenDeployment(ctx, my.DeleteBlueGreenDeploymentRequest{BlueGreenDeploymentId: created.Id})
	require.NoError(t, err)
	e.settle()

	_, err = e.bg.GetBlueGreenDeployment(ctx, my.GetBlueGreenDeploymentRequest{BlueGreenDeploymentId: created.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
	assert.Equal(t, my.DbSystemLifecycleStateActive, e.getDB(t, blue.Id).LifecycleState, "both DB systems are kept")
}

func TestBlueGreenValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mod    func(*my.CreateBlueGreenDeploymentDetails)
		name   string
		status int
	}{
		{name: "lower version", mod: func(d *my.CreateBlueGreenDeploymentDetails) {
			d.TargetDbSystemDetails.MysqlVersion = new("8.4.3")
		}, status: http.StatusBadRequest},
		{name: "unknown version", mod: func(d *my.CreateBlueGreenDeploymentDetails) {
			d.TargetDbSystemDetails.MysqlVersion = new("1.2.3")
		}, status: http.StatusBadRequest},
		{name: "weak password", mod: func(d *my.CreateBlueGreenDeploymentDetails) {
			d.ChannelDetails.SourcePassword = new("short")
		}, status: http.StatusBadRequest},
		{name: "verify ca without certificate", mod: func(d *my.CreateBlueGreenDeploymentDetails) {
			d.ChannelDetails.SslMode = my.SslModeVerifyCa
		}, status: http.StatusBadRequest},
		{name: "unknown source", mod: func(d *my.CreateBlueGreenDeploymentDetails) {
			d.SourceDbSystemId = new("ocid1.mysqldbsystem.oc1.iad.nope")
		}, status: http.StatusNotFound},
		{name: "ok", mod: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			blue := e.mkBlue(t)

			_, err := e.bg.CreateBlueGreenDeployment(t.Context(), my.CreateBlueGreenDeploymentRequest{
				CreateBlueGreenDeploymentDetails: bgDetails(blue.Id, tt.mod),
			})
			if tt.status == 0 {
				require.NoError(t, err)

				_, err = e.bg.CreateBlueGreenDeployment(t.Context(), my.CreateBlueGreenDeploymentRequest{
					CreateBlueGreenDeploymentDetails: bgDetails(blue.Id, nil),
				})
				assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "one deployment per DB system")

				return
			}

			assert.Equal(t, tt.status, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestBlueGreenEngine(t *testing.T) {
	t.Parallel()

	e, l := newEngineEnv(t)
	ctx := t.Context()
	blue := e.mkEngineDB(t, "blue", func(d *my.CreateDbSystemDetails) { d.MysqlVersion = new("8.4.6") })
	l.addTable(*blue.Id, testTable)

	created, err := e.bg.CreateBlueGreenDeployment(ctx, my.CreateBlueGreenDeploymentRequest{
		CreateBlueGreenDeploymentDetails: bgDetails(blue.Id, nil),
	})
	require.NoError(t, err)

	var got my.BlueGreenDeployment

	require.Eventually(t, func() bool {
		got = e.getBG(t, created.Id)

		return got.LifecycleState == my.BlueGreenDeploymentLifecycleStateActive
	}, e.waitFor(), 20*time.Millisecond)

	assert.Equal(t, my.BlueGreenDeploymentSwitchoverStatusReadyForSwitchover, got.SwitchoverStatus)

	l.mu.Lock()
	greenInst := l.insts[*got.TargetDbSystemId]
	spec, hasChannel := greenInst.channels["bg"+(*created.Id)[len(*created.Id)-10:]]
	assert.Equal(t, []byte(testTable), greenInst.seeded, "the green server starts from the blue data")
	l.mu.Unlock()
	require.True(t, hasChannel, "the channel is configured on the green server")
	assert.Equal(t, "repl", spec.User)
	assert.Equal(t, bgPassword, spec.Password)

	blueBefore := *e.getDB(t, blue.Id).Port

	_, err = e.bg.SwitchoverBlueGreenDeployment(ctx, my.SwitchoverBlueGreenDeploymentRequest{
		BlueGreenDeploymentId: created.Id,
		WaitTimeInSeconds:     new(5),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return e.getBG(t, created.Id).SwitchoverStatus == my.BlueGreenDeploymentSwitchoverStatusSwitchoverCompleted &&
			e.getBG(t, created.Id).LifecycleState == my.BlueGreenDeploymentLifecycleStateActive
	}, e.waitFor(), 20*time.Millisecond)

	after := e.getBG(t, created.Id)
	assert.Equal(t, *got.TargetDbSystemId, *after.ActiveDbSystemId)
	assert.NotEqual(t, blueBefore, *e.getDB(t, blue.Id).Port, "the DB system's endpoint is now a stable front address")

	l.mu.Lock()
	assert.Contains(t, l.insts[*blue.Id].queries, "SET GLOBAL super_read_only = ON", "blue stops taking writes")
	assert.NotEmpty(t, greenInst.chRemoved, "the channel is stopped on the green server")
	l.mu.Unlock()
}

func TestBlueGreenEngineSwitchoverTimeout(t *testing.T) {
	t.Parallel()

	e, l := newEngineEnv(t)
	ctx := t.Context()
	blue := e.mkEngineDB(t, "blue", func(d *my.CreateDbSystemDetails) { d.MysqlVersion = new("8.4.6") })

	created, err := e.bg.CreateBlueGreenDeployment(ctx, my.CreateBlueGreenDeploymentRequest{
		CreateBlueGreenDeploymentDetails: bgDetails(blue.Id, nil),
	})
	require.NoError(t, err)

	var got my.BlueGreenDeployment

	require.Eventually(t, func() bool {
		got = e.getBG(t, created.Id)

		return got.LifecycleState == my.BlueGreenDeploymentLifecycleStateActive
	}, e.waitFor(), 20*time.Millisecond)

	l.mu.Lock()
	l.insts[*got.TargetDbSystemId].behind = true
	l.mu.Unlock()

	_, err = e.bg.SwitchoverBlueGreenDeployment(ctx, my.SwitchoverBlueGreenDeploymentRequest{
		BlueGreenDeploymentId: created.Id,
		WaitTimeInSeconds:     new(0),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return e.getBG(t, created.Id).LifecycleState == my.BlueGreenDeploymentLifecycleStateFailed
	}, e.waitFor(), 20*time.Millisecond)

	failed := e.getBG(t, created.Id)
	assert.Equal(t, my.BlueGreenDeploymentSwitchoverStatusSwitchoverFailed, failed.SwitchoverStatus)
	assert.Contains(t, *failed.LifecycleDetails, "did not apply")
	assert.Equal(t, *blue.Id, *failed.ActiveDbSystemId, "nothing was swapped")

	l.mu.Lock()
	defer l.mu.Unlock()

	assert.Contains(t, l.insts[*blue.Id].queries, "SET GLOBAL super_read_only = OFF", "blue accepts writes again")
}
