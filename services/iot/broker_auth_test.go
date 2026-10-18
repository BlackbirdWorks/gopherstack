package iot_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

const subackFailure = 0x80

var errSubscribeRefused = errors.New("subscribe refused")

const (
	authDeadline = 5 * time.Second
	authMarker   = "auth/marker"
)

func authPolicy(statements ...string) string {
	return `{"Version":"2012-10-17","Statement":[` + strings.Join(statements, ",") + `]}`
}

func allowStmt(action, resource string) string {
	return fmt.Sprintf(`{"Effect":"Allow","Action":%q,"Resource":%q}`, action, resource)
}

func denyStmt(action, resource string) string {
	return fmt.Sprintf(`{"Effect":"Deny","Action":%q,"Resource":%q}`, action, resource)
}

const (
	allowAll       = `{"Effect":"Allow","Action":"iot:*","Resource":"*"}`
	allowConnect   = `{"Effect":"Allow","Action":"iot:Connect","Resource":"*"}`
	allowMarkerPub = `{"Effect":"Allow","Action":"iot:Publish","Resource":"arn:aws:iot:*:*:topic/auth/marker"}`
)

type authEnv struct {
	backend *iot.InMemoryBackend
	port    int
	tls     bool
}

func newAuthEnv(t *testing.T, authOn bool) *authEnv {
	t.Helper()

	backend := iot.NewInMemoryBackend()
	broker := iot.NewBroker(backend, 0)
	iot.NewHandler(backend, broker)
	broker.SetAuthRequired(authOn)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() {
		_ = broker.Start(ctx)
		close(done)
	}()

	t.Cleanup(func() {
		cancel()
		<-done
		backend.DrainBackground(context.Background())
	})

	select {
	case <-broker.Ready():
	case <-time.After(authDeadline):
		require.FailNow(t, "broker not ready")
	}

	return &authEnv{backend: backend, port: broker.Port(), tls: authOn}
}

type authDevice struct {
	key  string
	cert iot.Certificate
}

func (e *authEnv) device(t *testing.T, active bool, thing, policyName, policyDoc string) authDevice {
	t.Helper()

	cert, _, key, err := e.backend.CreateKeysAndCertificate(active)
	require.NoError(t, err)

	_, err = e.backend.CreatePolicy(&iot.CreatePolicyInput{PolicyName: policyName, PolicyDocument: policyDoc})
	require.NoError(t, err)
	require.NoError(t, e.backend.AttachPolicy(&iot.AttachPolicyInput{PolicyName: policyName, Target: cert.ARN}))

	if thing != "" {
		_, err = e.backend.CreateThing(&iot.CreateThingInput{ThingName: thing})
		require.NoError(t, err)
		require.NoError(t, e.backend.AttachThingPrincipal(&iot.AttachThingPrincipalInput{
			ThingName: thing, Principal: cert.ARN,
		}))
	}

	return authDevice{cert: *cert, key: key}
}

func (e *authEnv) connect(
	t *testing.T, d authDevice, clientID string, msgs chan<- pahomqtt.Message,
) (pahomqtt.Client, error) {
	t.Helper()

	opts := pahomqtt.NewClientOptions().
		SetClientID(clientID).
		SetConnectTimeout(authDeadline).
		SetAutoReconnect(false).
		SetDefaultPublishHandler(func(_ pahomqtt.Client, m pahomqtt.Message) {
			if msgs != nil {
				msgs <- m
			}
		})

	if e.tls {
		pair, err := tls.X509KeyPair([]byte(d.cert.PEM), []byte(d.key))
		require.NoError(t, err)

		cfg := &tls.Config{
			Certificates:       []tls.Certificate{pair},
			InsecureSkipVerify: true,
		}
		opts.AddBroker(fmt.Sprintf("ssl://127.0.0.1:%d", e.port)).SetTLSConfig(cfg)
	} else {
		opts.AddBroker(fmt.Sprintf("tcp://127.0.0.1:%d", e.port))
	}

	c := pahomqtt.NewClient(opts)
	tok := c.Connect()

	if !tok.WaitTimeout(authDeadline) {
		return nil, assert.AnError
	}

	if err := tok.Error(); err != nil {
		return nil, err
	}

	t.Cleanup(func() { c.Disconnect(100) })

	return c, nil
}

func subscribeErr(c pahomqtt.Client, filter string) error {
	tok := c.Subscribe(filter, 0, nil)
	if !tok.WaitTimeout(authDeadline) {
		return assert.AnError
	}

	if err := tok.Error(); err != nil {
		return err
	}

	if st, ok := tok.(*pahomqtt.SubscribeToken); ok && st.Result()[filter] == subackFailure {
		return errSubscribeRefused
	}

	return nil
}

func TestMQTTAuthPublish(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		policy    string
		thing     string
		clientID  string
		topic     string
		authOn    bool
		delivered bool
	}{
		{name: "auth_off_is_open", authOn: false, policy: authPolicy(allowMarkerPub), clientID: "anyone",
			topic: "open/topic", delivered: true},
		{name: "allowed_publish", authOn: true, clientID: "dev1", topic: "t/allowed", delivered: true,
			policy: authPolicy(allowConnect, allowMarkerPub, allowStmt("iot:Publish", "arn:aws:iot:*:*:topic/t/*"))},
		{name: "no_publish_permission_drops", authOn: true, clientID: "dev1", topic: "t/denied", delivered: false,
			policy: authPolicy(allowConnect, allowMarkerPub)},
		{name: "explicit_deny_wins", authOn: true, clientID: "dev1", topic: "t/secret", delivered: false,
			policy: authPolicy(allowConnect, allowMarkerPub, allowStmt("iot:Publish", "*"),
				denyStmt("iot:Publish", "arn:aws:iot:*:*:topic/t/secret"))},
		{name: "thing_variable_own_topic", authOn: true, thing: "dev1", clientID: "dev1",
			topic: "$aws/things/dev1/jobs/get", delivered: true,
			policy: authPolicy(
				allowConnect,
				allowMarkerPub,
				allowStmt(
					"iot:Publish",
					"arn:aws:iot:*:*:topic/$aws/things/${iot:Connection.Thing.ThingName}/jobs/*",
				),
			)},
		{name: "thing_variable_other_topic", authOn: true, thing: "dev1", clientID: "dev1",
			topic: "$aws/things/dev2/jobs/get", delivered: false,
			policy: authPolicy(
				allowConnect,
				allowMarkerPub,
				allowStmt(
					"iot:Publish",
					"arn:aws:iot:*:*:topic/$aws/things/${iot:Connection.Thing.ThingName}/jobs/*",
				),
			)},
		{name: "client_id_variable", authOn: true, clientID: "dev1", topic: "c/dev1/x", delivered: true,
			policy: authPolicy(allowConnect, allowMarkerPub,
				allowStmt("iot:Publish", "arn:aws:iot:*:*:topic/c/${iot:ClientId}/*"))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newAuthEnv(t, tt.authOn)
			pub := env.device(t, true, tt.thing, "pub-policy", tt.policy)
			obs := env.device(t, true, "", "obs-policy", authPolicy(allowAll))

			msgs := make(chan pahomqtt.Message, 8)
			observer, err := env.connect(t, obs, "observer", msgs)
			require.NoError(t, err)
			require.NoError(t, subscribeErr(observer, tt.topic))
			require.NoError(t, subscribeErr(observer, authMarker))

			c, err := env.connect(t, pub, tt.clientID, nil)
			require.NoError(t, err)

			require.True(t, c.Publish(tt.topic, 0, false, "x").WaitTimeout(authDeadline))
			require.True(t, c.Publish(authMarker, 0, false, "m").WaitTimeout(authDeadline))

			var got []string

			timer := time.NewTimer(authDeadline)
			defer timer.Stop()

			for len(got) == 0 || got[len(got)-1] != authMarker {
				select {
				case m := <-msgs:
					got = append(got, m.Topic())
				case <-timer.C:
					require.FailNow(t, "marker never delivered", "got %v", got)
				}
			}

			if tt.delivered {
				assert.Equal(t, []string{tt.topic, authMarker}, got)
			} else {
				assert.Equal(t, []string{authMarker}, got)
			}
		})
	}
}

func TestMQTTAuthConnectSubscribe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		policy      string
		clientID    string
		subscribe   string
		inactive    bool
		wantConnect bool
		wantSub     bool
	}{
		{
			name:        "allowed",
			policy:      authPolicy(allowConnect, allowStmt("iot:Subscribe", "arn:aws:iot:*:*:topicfilter/s/*")),
			clientID:    "dev1",
			subscribe:   "s/a",
			wantConnect: true,
			wantSub:     true,
		},
		{name: "inactive_cert_rejected", policy: authPolicy(allowAll), clientID: "dev1", inactive: true},
		{name: "no_connect_permission", policy: authPolicy(allowStmt("iot:Publish", "*")), clientID: "dev1"},
		{
			name:     "connect_other_client_id",
			policy:   authPolicy(allowStmt("iot:Connect", "arn:aws:iot:*:*:client/${iot:ClientId}x")),
			clientID: "dev1",
		},
		{name: "no_subscribe_permission", policy: authPolicy(allowConnect),
			clientID: "dev1", subscribe: "s/a", wantConnect: true},
		{name: "subscribe_denied_filter", policy: authPolicy(allowConnect, allowStmt("iot:Subscribe", "*"),
			denyStmt("iot:Subscribe", "arn:aws:iot:*:*:topicfilter/s/private/*")),
			clientID: "dev1", subscribe: "s/private/#", wantConnect: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newAuthEnv(t, true)
			d := env.device(t, !tt.inactive, "", "p", tt.policy)

			c, err := env.connect(t, d, tt.clientID, nil)
			if !tt.wantConnect {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			err = subscribeErr(c, tt.subscribe)
			if tt.wantSub {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestMQTTAuthRequiresCertificate(t *testing.T) {
	t.Parallel()

	env := newAuthEnv(t, true)
	opts := pahomqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("ssl://127.0.0.1:%d", env.port)).
		SetClientID("nocert").SetAutoReconnect(false).SetConnectTimeout(authDeadline).
		SetTLSConfig(&tls.Config{InsecureSkipVerify: true})

	tok := pahomqtt.NewClient(opts).Connect()
	require.True(t, tok.WaitTimeout(authDeadline))
	require.Error(t, tok.Error())
}
