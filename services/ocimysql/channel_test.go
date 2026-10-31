package ocimysql_test

import (
	"net/http"
	"testing"

	my "github.com/oracle/oci-go-sdk/v65/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func channelDetails(dbID *string) my.CreateChannelDetails {
	return my.CreateChannelDetails{
		DisplayName: new("repl"),
		Source: my.CreateChannelSourceFromMysqlDetails{
			Hostname: new("source.example.com"), Username: new("repl"), Password: new("S0urce#pass"),
			SslMode: my.ChannelSourceMysqlSslModeRequired,
		},
		Target: my.CreateChannelTargetFromDbSystemDetails{DbSystemId: dbID, ChannelName: new("ch1")},
	}
}

func TestChannelLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	db := e.mkDB(t, "target", nil)

	created, err := e.ch.CreateChannel(ctx, my.CreateChannelRequest{CreateChannelDetails: channelDetails(db.Id)})
	require.NoError(t, err)
	assert.Equal(t, my.ChannelLifecycleStateCreating, created.LifecycleState)
	assert.True(t, *created.IsEnabled)
	assert.Equal(t, testCompartment, *created.CompartmentId, "defaults to the DB system's compartment")

	src, ok := created.Source.(my.ChannelSourceMysql)
	require.True(t, ok)
	assert.Equal(t, 3306, *src.Port)
	assert.Equal(t, "source.example.com", *src.Hostname)

	tgt, ok := created.Target.(my.ChannelTargetDbSystem)
	require.True(t, ok)
	assert.Equal(t, 0, *tgt.DelayInSeconds)
	assert.Equal(
		t,
		my.ChannelTargetDbSystemTablesWithoutPrimaryKeyHandlingRaiseError,
		tgt.TablesWithoutPrimaryKeyHandling,
	)

	e.settle()

	got, err := e.ch.GetChannel(ctx, my.GetChannelRequest{ChannelId: created.Id})
	require.NoError(t, err)
	assert.Equal(t, my.ChannelLifecycleStateActive, got.LifecycleState)

	withCh := e.getDB(t, db.Id)
	require.Len(t, withCh.Channels, 1)
	assert.Equal(t, *created.Id, *withCh.Channels[0].Id)

	_, err = e.ch.ResumeChannel(ctx, my.ResumeChannelRequest{ChannelId: created.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "an ACTIVE channel cannot be resumed")

	upd, err := e.ch.UpdateChannel(ctx, my.UpdateChannelRequest{
		ChannelId: created.Id, IfMatch: got.Etag,
		IsEnabled: new(false), DisplayName: new("paused"),
	})
	require.NoError(t, err)
	require.NotNil(t, upd.OpcWorkRequestId)

	e.settle()

	paused, err := e.ch.GetChannel(ctx, my.GetChannelRequest{ChannelId: created.Id})
	require.NoError(t, err)
	assert.Equal(t, my.ChannelLifecycleStateInactive, paused.LifecycleState)
	assert.Equal(t, "paused", *paused.DisplayName)
	assert.False(t, *paused.IsEnabled)

	_, err = e.ch.ResumeChannel(ctx, my.ResumeChannelRequest{ChannelId: created.Id})
	require.NoError(t, err)
	e.settle()

	resumed, err := e.ch.GetChannel(ctx, my.GetChannelRequest{ChannelId: created.Id})
	require.NoError(t, err)
	assert.Equal(t, my.ChannelLifecycleStateActive, resumed.LifecycleState)
	assert.True(t, *resumed.IsEnabled)

	lst, err := e.ch.ListChannels(ctx, my.ListChannelsRequest{CompartmentId: new(testCompartment), DbSystemId: db.Id})
	require.NoError(t, err)
	require.Len(t, lst.Items, 1)

	del, err := e.ch.DeleteChannel(ctx, my.DeleteChannelRequest{ChannelId: created.Id})
	require.NoError(t, err)
	require.NotNil(t, del.OpcWorkRequestId)
	e.settle()

	lst, err = e.ch.ListChannels(ctx, my.ListChannelsRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	assert.Empty(t, lst.Items)
	assert.Empty(t, e.getDB(t, db.Id).Channels)
}

func TestChannelValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mod  func(d *my.CreateChannelDetails)
		name string
		want int
	}{
		{func(d *my.CreateChannelDetails) {
			d.Source = my.CreateChannelSourceFromMysqlDetails{
				Username: new("u"),
				Password: new("p"),
				SslMode:  my.ChannelSourceMysqlSslModeRequired,
			}
		}, "no hostname", 400},
		{func(d *my.CreateChannelDetails) {
			d.Source = my.CreateChannelSourceFromMysqlDetails{
				Hostname: new("h"),
				Username: new("u"),
				SslMode:  my.ChannelSourceMysqlSslModeRequired,
			}
		}, "no password", 400},
		{func(d *my.CreateChannelDetails) {
			d.Source = my.CreateChannelSourceFromMysqlDetails{
				Hostname: new("h"),
				Username: new("u"),
				Password: new("p"),
			}
		}, "no ssl mode", 400},
		{func(d *my.CreateChannelDetails) {
			d.Source = my.CreateChannelSourceFromMysqlDetails{
				Hostname: new(
					"h",
				),
				Username: new("u"),
				Password: new("p"),
				Port:     new(70000),
				SslMode:  my.ChannelSourceMysqlSslModeDisabled,
			}
		}, "bad port", 400},
		{func(d *my.CreateChannelDetails) {
			d.Target = my.CreateChannelTargetFromDbSystemDetails{DbSystemId: new("ocid1.mysqldbsystem.oc1.iad.nope")}
		}, "unknown target", 400},
		{func(d *my.CreateChannelDetails) {
			d.Target = my.CreateChannelTargetFromDbSystemDetails{}
		}, "no target", 400},
		{func(d *my.CreateChannelDetails) {
			d.Target = my.CreateChannelTargetFromDbSystemDetails{DelayInSeconds: new(-1)}
		}, "negative delay", 400},
		{func(*my.CreateChannelDetails) {}, "valid", 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			db := e.mkDB(t, "target", nil)

			d := channelDetails(db.Id)
			tt.mod(&d)

			if tgt, ok := d.Target.(my.CreateChannelTargetFromDbSystemDetails); ok && tgt.DbSystemId == nil &&
				tt.name == "negative delay" {
				tgt.DbSystemId = db.Id
				d.Target = tgt
			}

			_, err := e.ch.CreateChannel(t.Context(), my.CreateChannelRequest{CreateChannelDetails: d})
			if tt.want == 200 {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, tt.want, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}
