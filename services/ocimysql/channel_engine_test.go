package ocimysql_test

import (
	"errors"
	"strconv"
	"testing"
	"time"

	my "github.com/oracle/oci-go-sdk/v65/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/mysqlengine"
)

const testChannel = "ch1"

var errChannelBroken = errors.New("cannot reach the source")

func (e *env) awaitChannel(t *testing.T, id *string, state my.ChannelLifecycleStateEnum) my.Channel {
	t.Helper()

	var got my.Channel

	require.Eventually(t, func() bool {
		r, err := e.ch.GetChannel(t.Context(), my.GetChannelRequest{ChannelId: id})
		if err != nil {
			return false
		}

		got = r.Channel

		return got.LifecycleState == state
	}, e.waitFor(), 20*time.Millisecond)

	return got
}

func (l *fakeLauncher) channelOf(sys string) (mysqlengine.ChannelSpec, bool, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	in := l.insts[sys]
	c, ok := in.channels[testChannel]

	return c, in.started[testChannel], ok
}

func TestChannelEngine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantState  my.ChannelLifecycleStateEnum
		wantHost   string
		wantPort   int
		enabled    bool
		external   bool
		wantJoined bool
	}{
		{
			name:       "emulated source",
			enabled:    true,
			wantState:  my.ChannelLifecycleStateActive,
			wantHost:   "src",
			wantPort:   3306,
			wantJoined: true,
		},
		{
			name:      "external source",
			enabled:   true,
			external:  true,
			wantState: my.ChannelLifecycleStateActive,
			wantHost:  "db.example.com",
			wantPort:  3307,
		},
		{
			name:       "disabled",
			enabled:    false,
			wantState:  my.ChannelLifecycleStateInactive,
			wantHost:   "src",
			wantPort:   3306,
			wantJoined: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, l := newEngineEnv(t)
			src := e.mkEngineDB(t, "source", nil)
			tgt := e.mkEngineDB(t, "target", nil)

			d := channelDetails(tgt.Id)
			d.IsEnabled = new(tt.enabled)
			d.Source = my.CreateChannelSourceFromMysqlDetails{
				Hostname: src.IpAddress, Port: src.Port, Username: new("repl"), Password: new("S0urce#pass"),
				SslMode: my.ChannelSourceMysqlSslModeRequired,
			}

			if tt.external {
				d.Source = my.CreateChannelSourceFromMysqlDetails{
					Hostname: new(
						"db.example.com",
					),
					Port:     new(3307),
					Username: new("repl"),
					Password: new("S0urce#pass"),
					SslMode:  my.ChannelSourceMysqlSslModeDisabled,
				}
			}

			created, err := e.ch.CreateChannel(t.Context(), my.CreateChannelRequest{CreateChannelDetails: d})
			require.NoError(t, err)
			assert.Equal(t, my.ChannelLifecycleStateCreating, created.LifecycleState)

			e.awaitChannel(t, created.Id, tt.wantState)

			spec, started, ok := l.channelOf(*tgt.Id)
			require.True(t, ok, "the channel is configured on the target server")
			assert.Equal(t, tt.wantHost, spec.Host)
			assert.Equal(t, tt.wantPort, spec.Port)
			assert.Equal(t, "repl", spec.User)
			assert.Equal(t, "S0urce#pass", spec.Password)
			assert.Equal(t, !tt.external, spec.SSL)
			assert.Equal(t, tt.enabled, started)

			l.mu.Lock()
			assert.Equal(t, tt.wantJoined, len(l.insts[*tgt.Id].joined) > 0, strconv.Itoa(len(l.insts[*tgt.Id].joined)))
			l.mu.Unlock()
		})
	}
}

func TestChannelEngineLifecycle(t *testing.T) {
	t.Parallel()

	e, l := newEngineEnv(t)
	tgt := e.mkEngineDB(t, "target", nil)
	ctx := t.Context()

	created, err := e.ch.CreateChannel(ctx, my.CreateChannelRequest{CreateChannelDetails: channelDetails(tgt.Id)})
	require.NoError(t, err)
	e.awaitChannel(t, created.Id, my.ChannelLifecycleStateActive)

	_, err = e.ch.UpdateChannel(ctx, my.UpdateChannelRequest{
		ChannelId: created.Id, IsEnabled: new(false),
	})
	require.NoError(t, err)
	e.awaitChannel(t, created.Id, my.ChannelLifecycleStateInactive)

	_, started, _ := l.channelOf(*tgt.Id)
	assert.False(t, started, "disabling stops replication")

	_, err = e.ch.ResumeChannel(ctx, my.ResumeChannelRequest{ChannelId: created.Id})
	require.NoError(t, err)
	e.awaitChannel(t, created.Id, my.ChannelLifecycleStateActive)

	_, started, _ = l.channelOf(*tgt.Id)
	assert.True(t, started)

	_, err = e.ch.DeleteChannel(ctx, my.DeleteChannelRequest{ChannelId: created.Id})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, gerr := e.ch.GetChannel(ctx, my.GetChannelRequest{ChannelId: created.Id})

		return gerr != nil
	}, e.waitFor(), 20*time.Millisecond)

	_, _, ok := l.channelOf(*tgt.Id)
	assert.False(t, ok, "deleting forgets the channel on the server")
}

func TestChannelEngineFailureAndStatus(t *testing.T) {
	t.Parallel()

	e, l := newEngineEnv(t)
	tgt := e.mkEngineDB(t, "target", nil)
	ctx := t.Context()

	l.mu.Lock()
	l.insts[*tgt.Id].chErr = errChannelBroken
	l.mu.Unlock()

	created, err := e.ch.CreateChannel(ctx, my.CreateChannelRequest{CreateChannelDetails: channelDetails(tgt.Id)})
	require.NoError(t, err)

	bad := e.awaitChannel(t, created.Id, my.ChannelLifecycleStateNeedsAttention)
	assert.Contains(t, *bad.LifecycleDetails, "cannot reach the source")

	l.mu.Lock()
	l.insts[*tgt.Id].chErr = nil
	l.insts[*tgt.Id].chState = mysqlengine.ChannelState{IORunning: true, SQLActive: true, Running: true}
	l.mu.Unlock()

	_, err = e.ch.ResumeChannel(ctx, my.ResumeChannelRequest{ChannelId: created.Id})
	require.NoError(t, err)
	e.awaitChannel(t, created.Id, my.ChannelLifecycleStateActive)

	l.mu.Lock()
	l.insts[*tgt.Id].chState = mysqlengine.ChannelState{IORunning: false, IOError: "Access denied for user 'repl'"}
	l.mu.Unlock()

	st, err := e.ch.GenerateChannelStatus(ctx, my.GenerateChannelStatusRequest{ChannelId: created.Id})
	require.NoError(t, err)
	assert.False(t, *st.ChannelStatusResult.IsHealthy)
	assert.Contains(t, st.ChannelStatusResult.Errors, "Access denied for user 'repl'")
}
