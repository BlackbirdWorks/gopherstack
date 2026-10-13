package mq_test

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/mq"
)

const stockActiveMQXML = `<beans xmlns="http://www.springframework.org/schema/beans">
<!-- The <broker> element configures the broker. -->
<broker xmlns="http://activemq.apache.org/schema/core" brokerName="localhost">
<transportConnectors>
<transportConnector name="stomp" uri="stomp://0.0.0.0:61613"/>
</transportConnectors>
<plugins>
<simpleAuthenticationPlugin>
<users>
<authenticationUser username="admin" password="s3cretpassword"/>
</users>
</simpleAuthenticationPlugin>
</plugins>
</broker>
<import resource="jetty.xml"/>
</beans>`

var errExecDown = errors.New("exec down")

type reconfigRuntime struct {
	execErr error
	writes  []map[string]string
	fakeRuntime
	stops  int
	starts int
	emu    sync.Mutex
}

func (r *reconfigRuntime) Exec(_ context.Context, _ string, cmd []string) (container.ExecResult, error) {
	r.emu.Lock()
	defer r.emu.Unlock()

	if r.execErr != nil {
		return container.ExecResult{}, r.execErr
	}

	if cmd[0] == "cat" {
		return container.ExecResult{Stdout: stockActiveMQXML}, nil
	}

	files := map[string]string{}
	names := []string{"activemq.xml", "users.properties", "groups.properties"}

	for i, name := range names {
		raw, err := base64.StdEncoding.DecodeString(cmd[3+i])
		if err != nil {
			return container.ExecResult{ExitCode: 1, Stderr: err.Error()}, nil
		}

		files[name] = string(raw)
	}

	r.writes = append(r.writes, files)

	return container.ExecResult{}, nil
}

func (r *reconfigRuntime) StopContainer(context.Context, string) error {
	r.emu.Lock()
	defer r.emu.Unlock()

	r.stops++

	return nil
}

func (r *reconfigRuntime) StartContainer(context.Context, string) error {
	r.emu.Lock()
	defer r.emu.Unlock()

	r.starts++

	return nil
}

func (r *reconfigRuntime) snapshot() ([]map[string]string, int, int) {
	r.emu.Lock()
	defer r.emu.Unlock()

	return append([]map[string]string(nil), r.writes...), r.stops, r.starts
}

func reconfigBackend(t *testing.T, rt *reconfigRuntime) *mq.InMemoryBackend {
	t.Helper()

	probe := newGatedProbe()
	close(probe.gate)

	b := mq.NewInMemoryBackend("000000000000", "us-east-1")
	b.EnableBrokers(mq.BrokerConfig{Runtime: rt, Probe: probe.probe, StartTimeout: time.Minute})
	t.Cleanup(b.Close)

	return b
}

func TestDockerBrokerReboot_AppliesUsersAndConfiguration(t *testing.T) {
	t.Parallel()

	cfgXML := `<?xml version="1.0"?><broker xmlns="http://activemq.apache.org/schema/core"><queues>` +
		`<marker/></queues></broker>`

	tests := []struct {
		name         string
		extraUser    string
		wantUsers    string
		wantGroups   string
		wantXML      []string
		wantNoXML    []string
		groupsForNew []string
		deleteAdmin  bool
		consoleUser  bool
	}{
		{
			name:         "create_console_user",
			extraUser:    "operator",
			consoleUser:  true,
			groupsForNew: []string{"ops", "dev"},
			wantXML: []string{
				`<authenticationUser username="operator" password="operatorpassword1" groups="ops,dev"/>`,
				`<authenticationUser username="admin" password="s3cretpassword"/>`,
				`<marker/>`, `<transportConnectors>`, `<import resource="jetty.xml"/>`,
			},
			wantUsers:  "operator=operatorpassword1\n",
			wantGroups: "admins=operator\n",
		},
		{
			name:        "delete_user",
			extraUser:   "operator",
			deleteAdmin: true,
			wantXML:     []string{`username="operator"`},
			wantNoXML:   []string{`username="admin"`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := &reconfigRuntime{}
			b := reconfigBackend(t, rt)

			cfg, err := b.CreateConfiguration("c1", "", mq.EngineTypeActiveMQ, "", "SIMPLE", nil)
			require.NoError(t, err)

			_, err = b.UpdateConfiguration(cfg.ID, "marker", base64.StdEncoding.EncodeToString([]byte(cfgXML)))
			require.NoError(t, err)

			br, err := b.CreateBrokerWithOptions("b1", "", mq.EngineTypeActiveMQ, "", "", false, false, nil, nil,
				[]*mq.User{{Username: "admin", Password: "s3cretpassword"}}, nil,
				&mq.CreateBrokerOptions{Configuration: &mq.ConfigurationID{ID: cfg.ID, Revision: 2}})
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				return brokerState(b, br.BrokerID) == mq.BrokerStateRunning
			}, 10*time.Second, time.Millisecond)

			writes, stops, starts := rt.snapshot()
			require.Len(t, writes, 1, "configuration applied once at start")
			assert.Contains(t, writes[0]["activemq.xml"], "<marker/>")
			assert.Equal(t, 1, stops)
			assert.Equal(t, 1, starts)

			require.NoError(
				t,
				b.CreateUser(br.BrokerID, tt.extraUser, "operatorpassword1", tt.groupsForNew, tt.consoleUser, false),
			)

			if tt.deleteAdmin {
				require.NoError(t, b.DeleteUser(br.BrokerID, "admin"))
			}

			writes, _, _ = rt.snapshot()
			require.Len(t, writes, 1, "users are staged until reboot")

			require.NoError(t, b.RebootBroker(br.BrokerID))
			assert.Equal(t, mq.BrokerStateRebooting, brokerState(b, br.BrokerID))

			require.Eventually(t, func() bool {
				return brokerState(b, br.BrokerID) == mq.BrokerStateRunning
			}, 10*time.Second, time.Millisecond)

			writes, stops, starts = rt.snapshot()
			require.Len(t, writes, 2)
			assert.Equal(t, 2, stops)
			assert.Equal(t, 2, starts)

			xml := writes[1]["activemq.xml"]
			for _, want := range tt.wantXML {
				assert.Contains(t, xml, want)
			}

			for _, no := range tt.wantNoXML {
				assert.NotContains(t, xml, no)
			}

			assert.Equal(t, 1, strings.Count(xml, "<simpleAuthenticationPlugin>"))
			assert.Equal(t, tt.wantUsers, writes[1]["users.properties"])
			assert.Equal(t, tt.wantGroups, writes[1]["groups.properties"])

			u, err := b.DescribeUser(br.BrokerID, tt.extraUser)
			require.NoError(t, err)
			assert.Nil(t, u.Pending)
		})
	}
}

func TestDockerBrokerReboot_ExecFailureStillPromotes(t *testing.T) {
	t.Parallel()

	rt := &reconfigRuntime{}
	b := reconfigBackend(t, rt)
	br := createBroker(t, b, mq.EngineTypeActiveMQ)

	require.Eventually(t, func() bool {
		return brokerState(b, br.BrokerID) == mq.BrokerStateRunning
	}, 10*time.Second, time.Millisecond)

	rt.emu.Lock()
	rt.execErr = errExecDown
	rt.emu.Unlock()

	require.NoError(t, b.CreateUser(br.BrokerID, "operator", "operatorpassword1", nil, false, false))
	require.NoError(t, b.RebootBroker(br.BrokerID))

	require.Eventually(t, func() bool {
		return brokerState(b, br.BrokerID) == mq.BrokerStateRunning
	}, 10*time.Second, time.Millisecond)

	_, _, ok := b.MQConsumerEndpoint(br.BrokerArn)
	assert.False(t, ok, "broker is unreachable after a failed reconfiguration")
}
