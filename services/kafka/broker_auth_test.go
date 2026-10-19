package kafka_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/sasl/aws"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

func TestPlanListeners(t *testing.T) {
	t.Parallel()

	enc := func(mode string) *kafka.EncryptionInfo {
		return &kafka.EncryptionInfo{EncryptionInTransit: &kafka.EncryptionInTransit{ClientBroker: mode}}
	}
	sasl := func(scram, iamOn bool) *kafka.ClientAuthentication {
		return &kafka.ClientAuthentication{Sasl: &kafka.SaslSettings{
			Scram: &kafka.SaslScram{Enabled: scram}, Iam: &kafka.SaslIam{Enabled: iamOn},
		}}
	}

	tests := []struct {
		name                               string
		c                                  kafka.Cluster
		plaintext, tls, scram, iam, secure bool
	}{
		{name: "legacy_default", c: kafka.Cluster{}, plaintext: true},
		{name: "tls_mode", c: kafka.Cluster{EncryptionInfo: enc("TLS")}, tls: true, secure: true},
		{name: "plaintext_mode", c: kafka.Cluster{EncryptionInfo: enc("PLAINTEXT")}, plaintext: true, secure: true},
		{
			name:      "tls_plaintext",
			c:         kafka.Cluster{EncryptionInfo: enc("TLS_PLAINTEXT")},
			plaintext: true,
			tls:       true,
			secure:    true,
		},
		{
			name:   "scram_default_tls",
			c:      kafka.Cluster{ClientAuthentication: sasl(true, false)},
			tls:    true,
			scram:  true,
			secure: true,
		},
		{name: "scram_and_iam", c: kafka.Cluster{ClientAuthentication: sasl(true, true), EncryptionInfo: enc("TLS")},
			tls: true, scram: true, iam: true, secure: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := kafka.PlanListenersForTest(&tt.c)
			assert.Equal(t, []bool{tt.plaintext, tt.tls, tt.scram, tt.iam, tt.secure}, got)
		})
	}
}

func TestAuthenticateIAMPayload(t *testing.T) {
	t.Parallel()

	const clusterArn = "arn:aws:kafka:us-east-1:123456789012:cluster/c/abc"

	payload := func(secret, host string) []byte {
		m := aws.Auth{AccessKey: "AKIAMSKTEST", SecretKey: secret}.AsManagedStreamingIAMMechanism()
		_, p, err := m.Authenticate(t.Context(), host)
		require.NoError(t, err)

		return p
	}

	const host = "b-1.c.c2.kafka.us-east-1.amazonaws.com:9098"

	tests := []struct {
		now     time.Time
		cfg     kafka.IAMAuthConfig
		wantErr string
		name    string
		raw     []byte
	}{
		{
			name: "valid",
			cfg:  kafka.IAMAuthConfig{Identities: fakeIdentities{secret: "s"}},
			raw:  payload("s", host),
			now:  time.Now(),
		},
		{
			name:    "wrong_secret",
			cfg:     kafka.IAMAuthConfig{Identities: fakeIdentities{secret: "s"}},
			raw:     payload("x", host),
			now:     time.Now(),
			wantErr: "signature mismatch",
		},
		{name: "expired", cfg: kafka.IAMAuthConfig{Identities: fakeIdentities{secret: "s"}}, raw: payload("s", host),
			now: time.Now().Add(time.Hour), wantErr: "expired"},
		{
			name: "wrong_region",
			cfg:  kafka.IAMAuthConfig{Identities: fakeIdentities{secret: "s"}},
			raw: payload(
				"s",
				"b-1.c.c2.kafka.eu-west-1.amazonaws.com:9098",
			),
			now:     time.Now(),
			wantErr: "region mismatch",
		},
		{name: "no_identities", raw: payload("s", host), now: time.Now(), wantErr: "unknown"},
		{name: "garbage", cfg: kafka.IAMAuthConfig{Identities: fakeIdentities{secret: "s"}}, raw: []byte("{"),
			now: time.Now(), wantErr: "malformed"},
		{name: "enforced_denied", cfg: kafka.IAMAuthConfig{Identities: fakeIdentities{secret: "s"}, Enforce: true},
			raw: payload("s", host), now: time.Now(), wantErr: "does not allow"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := kafka.AuthenticateIAMPayloadForTest(tt.cfg, clusterArn, tt.raw, tt.now)
			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

type recordingScramAdmin struct {
	upserts []string
	removes []string
	mu      sync.Mutex
}

func (r *recordingScramAdmin) apply(_ context.Context, _ string, up []kafka.ScramUser, rm []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, u := range up {
		r.upserts = append(r.upserts, u.Username+":"+u.Password)
	}

	r.removes = append(r.removes, rm...)

	return nil
}

func (r *recordingScramAdmin) snapshot() ([]string, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return slices.Clone(r.upserts), slices.Clone(r.removes)
}

func TestBroker_ScramSecretsSyncToBroker(t *testing.T) {
	t.Parallel()

	rec := &recordingScramAdmin{}
	rt := &fakeBrokerRuntime{}
	b := newTestBackend(t)
	b.EnableBrokers(kafka.BrokerConfig{
		Runtime:    rt,
		Probe:      func(context.Context, string) error { return nil },
		ScramAdmin: rec.apply,
	})
	t.Cleanup(b.Close)

	const (
		good = "arn:aws:secretsmanager:us-east-1:123456789012:secret:AmazonMSK_ok"
		bad  = "arn:aws:secretsmanager:us-east-1:123456789012:secret:plain-name"
		gone = "arn:aws:secretsmanager:us-east-1:123456789012:secret:AmazonMSK_gone"
	)

	b.SetScramSecretResolver(fakeScramSource{
		good: {Username: "bob", Password: "pw", SecretName: "AmazonMSK_ok"},
		bad:  {Username: "eve", Password: "pw", SecretName: "plain-name"},
	})

	auth := &kafka.ClientAuthentication{Sasl: &kafka.SaslSettings{Scram: &kafka.SaslScram{Enabled: true}}}
	c, err := b.CreateCluster(t.Context(), "sc", "3.9.1", 1, kafka.BrokerNodeGroupInfo{}, auth, nil)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		got, derr := b.DescribeCluster(t.Context(), c.ClusterArn)

		return derr == nil && got.State == kafka.ClusterStateActive
	}, 10*time.Second, 5*time.Millisecond)

	errs, err := b.BatchAssociateScramSecret(t.Context(), c.ClusterArn, []string{good, bad, gone})
	require.NoError(t, err)
	require.Len(t, errs, 2)

	codes := map[string]string{}
	for _, e := range errs {
		codes[e.SecretArn] = e.ErrorCode
	}

	assert.Equal(t, "InvalidSecret", codes[bad])
	assert.Equal(t, "SecretNotFound", codes[gone])

	listed, err := b.ListScramSecrets(t.Context(), c.ClusterArn)
	require.NoError(t, err)
	assert.Equal(t, []string{good}, listed)

	up, rm := rec.snapshot()
	assert.Equal(t, []string{"bob:pw"}, up)
	assert.Empty(t, rm)

	_, err = b.BatchDisassociateScramSecret(t.Context(), c.ClusterArn, []string{good})
	require.NoError(t, err)

	_, rm = rec.snapshot()
	assert.Equal(t, []string{"bob"}, rm)
	assert.NotErrorIs(t, err, kafka.ErrNotFound)
}

func TestBroker_SecureSpecAndEndpoints(t *testing.T) {
	t.Parallel()

	rt := &fakeBrokerRuntime{}
	b := newTestBackend(t)
	b.EnableBrokers(kafka.BrokerConfig{Runtime: rt, Probe: func(context.Context, string) error { return nil }})
	t.Cleanup(b.Close)

	auth := &kafka.ClientAuthentication{Sasl: &kafka.SaslSettings{
		Scram: &kafka.SaslScram{Enabled: true}, Iam: &kafka.SaslIam{Enabled: true},
	}}
	c, err := b.CreateCluster(t.Context(), "sec", "3.9.1", 1, kafka.BrokerNodeGroupInfo{}, auth, nil,
		kafka.ClusterCreateOptions{EncryptionInfo: &kafka.EncryptionInfo{
			EncryptionInTransit: &kafka.EncryptionInTransit{ClientBroker: "TLS_PLAINTEXT"},
		}})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		ep, _ := b.ManagedEndpointsFor(c.ClusterArn)

		return ep.SaslIam != ""
	}, 10*time.Second, 5*time.Millisecond)

	ep, managed := b.ManagedEndpointsFor(c.ClusterArn)
	require.True(t, managed)
	assert.NotEmpty(t, ep.Plaintext)
	assert.NotEmpty(t, ep.TLS)
	assert.NotEmpty(t, ep.SaslScram)

	specs := rt.startedSpecs()
	require.Len(t, specs, 1)
	assert.Len(t, specs[0].Ports, 5)
	assert.Contains(t, specs[0].Env, "KAFKA_INTER_BROKER_LISTENER_NAME=ADMIN")
	assert.Contains(t, specs[0].Env, "KAFKA_SSL_KEYSTORE_TYPE=PEM")
	assert.Contains(t, specs[0].Env, "KAFKA_LISTENER_NAME_SCRAM_SASL_ENABLED_MECHANISMS=SCRAM-SHA-512")
	assert.Contains(t, specs[0].Env, "KAFKA_LISTENER_NAME_IAMBACK_SASL_ENABLED_MECHANISMS=PLAIN")
	assert.NotEmpty(t, b.BrokerCAPEM())
}
