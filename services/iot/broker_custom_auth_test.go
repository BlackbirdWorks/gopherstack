package iot_test

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	"github.com/aws/aws-sdk-go-v2/service/iot/types"
	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

const (
	authzLambdaARN = "arn:aws:lambda:us-east-1:000000000000:function:authz"
	authzSNI       = "iot.example.com"
)

type fakeAuthLambda struct {
	respond func(event map[string]any) any
	events  []map[string]any
	mu      sync.Mutex
}

func (f *fakeAuthLambda) RequestLambda(_ context.Context, _, _ string, payload []byte) ([]byte, error) {
	var ev map[string]any
	if err := json.Unmarshal(payload, &ev); err != nil {
		return nil, err
	}

	f.mu.Lock()
	f.events = append(f.events, ev)
	respond := f.respond
	f.mu.Unlock()

	return json.Marshal(respond(ev))
}

func (f *fakeAuthLambda) calls() []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]map[string]any(nil), f.events...)
}

func (f *fakeAuthLambda) setRespond(r func(map[string]any) any) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.respond = r
}

func authzResponse(principal string, refresh, disconnect int, statements ...string) map[string]any {
	var doc any
	_ = json.Unmarshal([]byte(authPolicy(statements...)), &doc)

	out := map[string]any{"isAuthenticated": true, "principalId": principal, "policyDocuments": []any{doc}}
	if refresh > 0 {
		out["refreshAfterInSeconds"] = refresh
	}

	if disconnect > 0 {
		out["disconnectAfterInSeconds"] = disconnect
	}

	return out
}

func mqttPassword(ev map[string]any) string {
	mq, _ := ev["protocolData"].(map[string]any)["mqtt"].(map[string]any)
	raw, _ := base64.StdEncoding.DecodeString(fmt.Sprint(mq["password"]))

	return string(raw)
}

type customAuthEnv struct {
	backend *iot.InMemoryBackend
	broker  *iot.Broker
	lambda  *fakeAuthLambda
	port    int
}

func newCustomAuthEnv(t *testing.T) *customAuthEnv {
	t.Helper()

	backend := iot.NewInMemoryBackend()
	broker := iot.NewBroker(backend, 0)
	iot.NewHandler(backend, broker)
	broker.SetAuthRequired(true)

	lambda := &fakeAuthLambda{respond: func(map[string]any) any { return map[string]any{"isAuthenticated": false} }}
	backend.SetActionTargets(&iot.ActionTargets{Lambda: lambda})

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

	return &customAuthEnv{backend: backend, broker: broker, lambda: lambda, port: broker.Port()}
}

func (e *customAuthEnv) createAuthorizer(t *testing.T, name string, in *iot.CreateAuthorizerInput) {
	t.Helper()

	in.AuthorizerName, in.AuthorizerFunctionARN, in.Status = name, authzLambdaARN, "ACTIVE"
	_, err := e.backend.CreateAuthorizer(in)
	require.NoError(t, err)
}

func (e *customAuthEnv) connect(
	t *testing.T, clientID, username, password string, msgs chan<- pahomqtt.Message, lost chan<- struct{},
) (pahomqtt.Client, error) {
	t.Helper()

	opts := pahomqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("ssl://127.0.0.1:%d", e.port)).
		SetTLSConfig(&tls.Config{InsecureSkipVerify: true, ServerName: authzSNI}).
		SetClientID(clientID).SetUsername(username).SetPassword(password).
		SetConnectTimeout(authDeadline).SetAutoReconnect(false).
		SetDefaultPublishHandler(func(_ pahomqtt.Client, m pahomqtt.Message) {
			if msgs != nil {
				msgs <- m
			}
		}).
		SetConnectionLostHandler(func(pahomqtt.Client, error) {
			if lost != nil {
				lost <- struct{}{}
			}
		})

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

// delivered reports whether a message published by pub on topic reaches an observer.
func (e *customAuthEnv) delivered(t *testing.T, pub pahomqtt.Client, topic string) bool {
	t.Helper()

	msgs := make(chan pahomqtt.Message, 8)
	obs, err := e.connect(t, "observer-"+topic, "u", "observer", msgs, nil)
	require.NoError(t, err)
	require.NoError(t, subscribeErr(obs, topic))
	require.NoError(t, subscribeErr(obs, authMarker))

	require.True(t, pub.Publish(topic, 0, false, "x").WaitTimeout(authDeadline))
	require.True(t, pub.Publish(authMarker, 0, false, "m").WaitTimeout(authDeadline))

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

	return len(got) == 2
}

func observerAllow() map[string]any { return authzResponse("observer", 0, 0, allowAll) }

func TestCustomAuthorizerMQTT(t *testing.T) {
	t.Parallel()

	devPolicy := func(ev map[string]any) any {
		switch mqttPassword(ev) {
		case "observer":
			return observerAllow()
		case "dev":
			return authzResponse("dev1", 0, 0, allowConnect, allowMarkerPub,
				allowStmt("iot:Publish", "arn:aws:iot:*:*:topic/telemetry/${iot:ClientId}"))
		}

		return map[string]any{"isAuthenticated": false}
	}

	t.Run("authenticated_default_authorizer_and_event", func(t *testing.T) {
		t.Parallel()

		e := newCustomAuthEnv(t)
		e.createAuthorizer(t, "def", &iot.CreateAuthorizerInput{SigningDisabled: true})
		require.NoError(t, e.backend.SetDefaultAuthorizer("def"))
		e.lambda.setRespond(devPolicy)

		c, err := e.connect(t, "car1", "user", "dev", nil, nil)
		require.NoError(t, err)

		ev := e.lambda.calls()[0]
		assert.ElementsMatch(t, []any{"tls", "mqtt"}, ev["protocols"])

		pd, _ := ev["protocolData"].(map[string]any)
		assert.Equal(t, map[string]any{"serverName": authzSNI}, pd["tls"])
		assert.Equal(
			t,
			map[string]any{
				"username": "user",
				"password": base64.StdEncoding.EncodeToString([]byte("dev")),
				"clientId": "car1",
			},
			pd["mqtt"],
		)
		assert.NotEmpty(t, ev["connectionMetadata"].(map[string]any)["id"])
		assert.Equal(t, false, ev["signatureVerified"])

		assert.True(t, e.delivered(t, c, "telemetry/car1"))
		assert.False(t, e.delivered(t, c, "telemetry/other"))
	})

	t.Run("named_authorizer_overrides_default", func(t *testing.T) {
		t.Parallel()

		e := newCustomAuthEnv(t)
		e.createAuthorizer(t, "def", &iot.CreateAuthorizerInput{SigningDisabled: true})
		e.createAuthorizer(t, "other", &iot.CreateAuthorizerInput{SigningDisabled: true})
		require.NoError(t, e.backend.SetDefaultAuthorizer("def"))
		e.lambda.setRespond(devPolicy)

		_, err := e.connect(t, "car1", "user?x-amz-customauthorizer-name=other", "dev", nil, nil)
		require.NoError(t, err)

		mq := e.lambda.calls()[0]["protocolData"].(map[string]any)["mqtt"].(map[string]any)
		assert.Equal(t, "user?x-amz-customauthorizer-name=other", mq["username"])
	})

	t.Run("rejections", func(t *testing.T) {
		t.Parallel()

		e := newCustomAuthEnv(t)
		e.createAuthorizer(t, "a", &iot.CreateAuthorizerInput{SigningDisabled: true})
		e.lambda.setRespond(devPolicy)

		_, err := e.connect(t, "c1", "u", "dev", nil, nil)
		require.Error(t, err, "no authorizer named and no default")

		_, err = e.connect(t, "c2", "u?x-amz-customauthorizer-name=missing", "dev", nil, nil)
		require.Error(t, err, "unknown authorizer")

		_, err = e.connect(t, "c3", "u?x-amz-customauthorizer-name=a", "wrong", nil, nil)
		require.Error(t, err, "lambda says not authenticated")

		e.lambda.setRespond(func(map[string]any) any {
			return authzResponse("x", 0, 0, `{"Effect":"Allow","Action":"iot:Publish","Resource":"*"}`)
		})
		_, err = e.connect(t, "c4", "u?x-amz-customauthorizer-name=a", "dev", nil, nil)
		require.Error(t, err, "policy does not allow iot:Connect")

		e.lambda.setRespond(
			func(map[string]any) any { return map[string]any{"isAuthenticated": true, "principalId": "bad id!"} },
		)
		_, err = e.connect(t, "c5", "u?x-amz-customauthorizer-name=a", "dev", nil, nil)
		require.Error(t, err, "invalid principalId")
	})

	t.Run("signed_token", func(t *testing.T) {
		t.Parallel()

		key, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)

		pubDER, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
		require.NoError(t, err)

		e := newCustomAuthEnv(t)
		e.createAuthorizer(t, "signed", &iot.CreateAuthorizerInput{
			TokenKeyName: "tk",
			TokenSigningPublicKeys: map[string]string{
				"k1": string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})),
			},
		})
		e.lambda.setRespond(func(map[string]any) any { return authzResponse("dev1", 0, 0, allowAll) })

		digest := sha256.Sum256([]byte("tokenvalue"))
		sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
		require.NoError(t, err)

		userFor := func(token, sig string) string {
			return "u?x-amz-customauthorizer-name=signed&x-amz-customauthorizer-signature=" + url.QueryEscape(
				sig,
			) + "&tk=" + token
		}

		_, err = e.connect(
			t,
			"bad",
			userFor("tokenvalue", base64.StdEncoding.EncodeToString([]byte("nope"))),
			"",
			nil,
			nil,
		)
		require.Error(t, err)
		assert.Empty(t, e.lambda.calls(), "lambda must not run for an unverified signature")

		_, err = e.connect(t, "unsigned", "u?x-amz-customauthorizer-name=signed&tk=tokenvalue", "", nil, nil)
		require.Error(t, err)

		_, err = e.connect(t, "good", userFor("tokenvalue", base64.StdEncoding.EncodeToString(sig)), "", nil, nil)
		require.NoError(t, err)

		ev := e.lambda.calls()[0]
		assert.Equal(t, "tokenvalue", ev["token"])
		assert.Equal(t, true, ev["signatureVerified"])
	})

	t.Run("policy_refresh", func(t *testing.T) {
		t.Parallel()

		e := newCustomAuthEnv(t)
		e.createAuthorizer(t, "a", &iot.CreateAuthorizerInput{SigningDisabled: true})
		require.NoError(t, e.backend.SetDefaultAuthorizer("a"))

		now := time.Now()
		var cmu sync.Mutex
		e.broker.SetClock(iot.BrokerClock{
			Now: func() time.Time {
				cmu.Lock()
				defer cmu.Unlock()

				return now
			},
			AfterFunc: func(time.Duration, func()) func() bool { return func() bool { return true } },
		})

		e.lambda.setRespond(func(ev map[string]any) any {
			if mqttPassword(ev) == "observer" {
				return observerAllow()
			}

			return authzResponse(
				"dev1",
				300,
				0,
				allowConnect,
				allowMarkerPub,
				allowStmt("iot:Publish", "arn:aws:iot:*:*:topic/a"),
			)
		})

		c, err := e.connect(t, "car1", "u", "dev", nil, nil)
		require.NoError(t, err)
		assert.True(t, e.delivered(t, c, "a"))
		assert.False(t, e.delivered(t, c, "b"))

		before := len(e.lambda.calls())

		e.lambda.setRespond(func(ev map[string]any) any {
			if mqttPassword(ev) == "observer" {
				return observerAllow()
			}

			return authzResponse(
				"dev1",
				300,
				0,
				allowConnect,
				allowMarkerPub,
				allowStmt("iot:Publish", "arn:aws:iot:*:*:topic/b"),
			)
		})

		cmu.Lock()
		now = now.Add(301 * time.Second)
		cmu.Unlock()

		assert.True(t, e.delivered(t, c, "b"), "refreshed policy allows b")
		assert.False(t, e.delivered(t, c, "a"), "refreshed policy no longer allows a")
		assert.Greater(t, len(e.lambda.calls()), before)
	})

	t.Run("disconnect_after", func(t *testing.T) {
		t.Parallel()

		e := newCustomAuthEnv(t)
		e.createAuthorizer(t, "a", &iot.CreateAuthorizerInput{SigningDisabled: true})
		require.NoError(t, e.backend.SetDefaultAuthorizer("a"))

		fire := make(chan func(), 1)
		var gotDelay time.Duration
		var dmu sync.Mutex

		e.broker.SetClock(iot.BrokerClock{
			Now: time.Now,
			AfterFunc: func(d time.Duration, f func()) func() bool {
				dmu.Lock()
				defer dmu.Unlock()

				if d > 0 && gotDelay == 0 {
					gotDelay = d
					fire <- f
				}

				return func() bool { return true }
			},
		})
		e.lambda.setRespond(func(map[string]any) any { return authzResponse("dev1", 0, 600, allowAll) })

		lost := make(chan struct{}, 1)
		c, err := e.connect(t, "car1", "u", "dev", nil, lost)
		require.NoError(t, err)
		require.True(t, c.IsConnectionOpen())

		dmu.Lock()
		assert.Equal(t, 600*time.Second, gotDelay)
		dmu.Unlock()

		(<-fire)()

		select {
		case <-lost:
		case <-time.After(authDeadline):
			require.FailNow(t, "client was not disconnected")
		}
	})
}

func TestTestInvokeAuthorizerSDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		respond func(map[string]any) any
		check   func(t *testing.T, out *iotsdk.TestInvokeAuthorizerOutput, ev map[string]any)
		in      *iotsdk.TestInvokeAuthorizerInput
		name    string
		wantErr string
	}{
		{
			name: "forwards_contexts_and_maps_response",
			in: &iotsdk.TestInvokeAuthorizerInput{
				Token:       aws.String("tok"),
				HttpContext: &types.HttpContext{Headers: map[string]string{"h": "v"}, QueryString: aws.String("?a=b")},
				TlsContext:  &types.TlsContext{ServerName: aws.String("sni.example.com")},
				MqttContext: &types.MqttContext{
					ClientId: aws.String("c1"),
					Username: aws.String("u"),
					Password: []byte("pw"),
				},
			},
			respond: func(map[string]any) any { return authzResponse("p1", 600, 3600, allowConnect) },
			check: func(t *testing.T, out *iotsdk.TestInvokeAuthorizerOutput, ev map[string]any) {
				t.Helper()

				assert.True(t, aws.ToBool(out.IsAuthenticated))
				assert.Equal(t, "p1", aws.ToString(out.PrincipalId))
				assert.EqualValues(t, 600, aws.ToInt32(out.RefreshAfterInSeconds))
				assert.EqualValues(t, 3600, aws.ToInt32(out.DisconnectAfterInSeconds))
				require.Len(t, out.PolicyDocuments, 1)
				assert.JSONEq(t, authPolicy(allowConnect), out.PolicyDocuments[0])

				assert.Equal(t, "tok", ev["token"])
				assert.ElementsMatch(t, []any{"tls", "http", "mqtt"}, ev["protocols"])

				pd, _ := ev["protocolData"].(map[string]any)
				assert.Equal(t, map[string]any{"headers": map[string]any{"h": "v"}, "queryString": "?a=b"}, pd["http"])
				assert.Equal(t, map[string]any{"serverName": "sni.example.com"}, pd["tls"])
				assert.Equal(
					t,
					map[string]any{
						"username": "u",
						"password": base64.StdEncoding.EncodeToString([]byte("pw")),
						"clientId": "c1",
					},
					pd["mqtt"],
				)
			},
		},
		{
			name:    "not_authenticated",
			in:      &iotsdk.TestInvokeAuthorizerInput{Token: aws.String("tok")},
			respond: func(map[string]any) any { return map[string]any{"isAuthenticated": false} },
			check: func(t *testing.T, out *iotsdk.TestInvokeAuthorizerOutput, _ map[string]any) {
				t.Helper()

				assert.False(t, aws.ToBool(out.IsAuthenticated))
				assert.Empty(t, out.PolicyDocuments)
			},
		},
		{
			name:    "defaults_disconnect_to_one_day",
			in:      &iotsdk.TestInvokeAuthorizerInput{Token: aws.String("tok")},
			respond: func(map[string]any) any { return authzResponse("p1", 0, 0, allowAll) },
			check: func(t *testing.T, out *iotsdk.TestInvokeAuthorizerOutput, _ map[string]any) {
				t.Helper()

				assert.EqualValues(t, 86400, aws.ToInt32(out.DisconnectAfterInSeconds))
			},
		},
		{
			name:    "invalid_principal",
			in:      &iotsdk.TestInvokeAuthorizerInput{Token: aws.String("tok")},
			wantErr: "InvalidResponseException",
			respond: func(map[string]any) any { return map[string]any{"isAuthenticated": true, "principalId": "no spaces"} },
		},
		{
			name:    "refresh_out_of_range",
			in:      &iotsdk.TestInvokeAuthorizerInput{Token: aws.String("tok")},
			wantErr: "InvalidResponseException",
			respond: func(map[string]any) any { return authzResponse("p1", 10, 0, allowAll) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, backend := newIoTSDKClient(t)
			lambda := &fakeAuthLambda{respond: tt.respond}
			backend.SetActionTargets(&iot.ActionTargets{Lambda: lambda})

			_, err := backend.CreateAuthorizer(&iot.CreateAuthorizerInput{
				AuthorizerName: "a", AuthorizerFunctionARN: authzLambdaARN, SigningDisabled: true, Status: "ACTIVE",
			})
			require.NoError(t, err)

			in := *tt.in
			in.AuthorizerName = aws.String("a")

			out, err := client.TestInvokeAuthorizer(t.Context(), &in)
			if tt.wantErr != "" {
				assert.Equal(t, tt.wantErr, apiCode(err))

				return
			}

			require.NoError(t, err)
			require.Len(t, lambda.calls(), 1)
			tt.check(t, out, lambda.calls()[0])
		})
	}

	t.Run("no_lambda_service", func(t *testing.T) {
		t.Parallel()

		client, backend := newIoTSDKClient(t)
		_, err := backend.CreateAuthorizer(&iot.CreateAuthorizerInput{
			AuthorizerName: "a", AuthorizerFunctionARN: authzLambdaARN, SigningDisabled: true, Status: "ACTIVE",
		})
		require.NoError(t, err)

		_, err = client.TestInvokeAuthorizer(t.Context(), &iotsdk.TestInvokeAuthorizerInput{
			AuthorizerName: aws.String("a"), Token: aws.String("t"),
		})
		assert.Equal(t, "ServiceUnavailableException", apiCode(err))
	})
}
