package kafka_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/blackbirdworks/gopherstack/services/kafka"
)

type fakeScramSource map[string]kafka.ScramCredential

func (f fakeScramSource) ResolveScramSecret(_ context.Context, arn string) (kafka.ScramCredential, error) {
	c, ok := f[arn]
	if !ok {
		return kafka.ScramCredential{}, kafka.ErrScramSecretNotFound
	}

	return c, nil
}

type fakeIdentities struct{ secret string }

func (f fakeIdentities) ListAllAccessKeys() []iam.AccessKey {
	return []iam.AccessKey{{AccessKeyID: "AKIAMSKTEST", SecretAccessKey: f.secret, Status: "Active"}}
}

func (fakeIdentities) GetUserByAccessKeyID(string) (*iam.User, error) {
	return &iam.User{UserName: "msk", Arn: "arn:aws:iam::123456789012:user/msk"}, nil
}
func (fakeIdentities) GetPoliciesForUser(string) ([]string, error) { return nil, nil }
func (fakeIdentities) GetPoliciesForRole(string) ([]string, error) { return nil, nil }

func dockerRuntimeOrSkip(t *testing.T) container.Runtime {
	t.Helper()

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	if err = rt.Ping(ctx); err != nil {
		t.Skipf("container runtime not reachable: %v", err)
	}

	if ok, herr := rt.HasImage(ctx, kafka.BrokerImage); herr != nil || !ok {
		t.Skipf("image %s not present", kafka.BrokerImage)
	}

	return rt
}

type regionPinned struct{ sasl.Mechanism }

func (r regionPinned) Authenticate(ctx context.Context, _ string) (sasl.Session, []byte, error) {
	return r.Mechanism.Authenticate(ctx, "b-1.test.c2.kafka.us-east-1.amazonaws.com:9098")
}

// mskIAM signs for an MSK hostname so the SDK-style region lookup works against a loopback endpoint.
func mskIAM(accessKey, secret string) sasl.Mechanism {
	return regionPinned{aws.Auth{AccessKey: accessKey, SecretKey: secret}.AsManagedStreamingIAMMechanism()}
}

func roundTrip(t *testing.T, opts ...kgo.Opt) error {
	t.Helper()

	topic := "auth-" + strings.ReplaceAll(t.Name(), "/", "-")
	base := append([]kgo.Opt{kgo.AllowAutoTopicCreation(), kgo.RecordDeliveryTimeout(15 * time.Second)}, opts...)

	cl, err := kgo.NewClient(base...)
	require.NoError(t, err)

	defer cl.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	return cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("hello")}).FirstErr()
}

func TestBroker_ClientAuthenticationDocker(t *testing.T) { //nolint:tparallel // ordered revocation
	t.Parallel()

	rt := dockerRuntimeOrSkip(t)
	b := newTestBackend(t)
	b.EnableBrokers(kafka.BrokerConfig{Runtime: rt, StartTimeout: 3 * time.Minute})
	t.Cleanup(b.Close)

	const secretArn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:AmazonMSK_app-AbCdEf"

	b.SetScramSecretResolver(
		fakeScramSource{secretArn: {Username: "alice", Password: "s3cret", SecretName: "AmazonMSK_app"}},
	)
	b.SetIAMAuth(func() kafka.IAMAuthConfig {
		return kafka.IAMAuthConfig{Identities: fakeIdentities{secret: "iam-secret"}}
	})

	auth := &kafka.ClientAuthentication{Sasl: &kafka.SaslSettings{
		Scram: &kafka.SaslScram{Enabled: true}, Iam: &kafka.SaslIam{Enabled: true},
	}}
	c, err := b.CreateCluster(t.Context(), "authd", "3.9.1", 1, kafka.BrokerNodeGroupInfo{}, auth, nil,
		kafka.ClusterCreateOptions{EncryptionInfo: &kafka.EncryptionInfo{
			EncryptionInTransit: &kafka.EncryptionInTransit{ClientBroker: "TLS"},
		}})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		got, derr := b.DescribeCluster(t.Context(), c.ClusterArn)

		return derr == nil && got.State == kafka.ClusterStateActive
	}, 4*time.Minute, 500*time.Millisecond)

	errs, err := b.BatchAssociateScramSecret(t.Context(), c.ClusterArn, []string{secretArn})
	require.NoError(t, err)
	require.Empty(t, errs)

	ep, managed := b.ManagedEndpointsFor(c.ClusterArn)
	require.True(t, managed)
	assert.Empty(t, ep.Plaintext)
	assert.NotEmpty(t, ep.TLS)
	require.NotEmpty(t, ep.SaslScram)
	require.NotEmpty(t, ep.SaslIam)

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(b.BrokerCAPEM()))

	tlsCfg := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}

	tests := []struct {
		name    string
		opts    []kgo.Opt
		wantErr bool
	}{
		{name: "scram_ok", opts: []kgo.Opt{
			kgo.SeedBrokers(ep.SaslScram), kgo.DialTLSConfig(tlsCfg),
			kgo.SASL(scram.Auth{User: "alice", Pass: "s3cret"}.AsSha512Mechanism()),
		}},
		{name: "scram_bad_password", wantErr: true, opts: []kgo.Opt{
			kgo.SeedBrokers(ep.SaslScram), kgo.DialTLSConfig(tlsCfg),
			kgo.SASL(scram.Auth{User: "alice", Pass: "nope"}.AsSha512Mechanism()),
			kgo.RequestRetries(1), kgo.RetryTimeout(5 * time.Second),
		}},
		{name: "iam_ok", opts: []kgo.Opt{
			kgo.SeedBrokers(ep.SaslIam), kgo.DialTLSConfig(tlsCfg),
			kgo.SASL(mskIAM("AKIAMSKTEST", "iam-secret")),
		}},
		{name: "iam_bad_secret", wantErr: true, opts: []kgo.Opt{
			kgo.SeedBrokers(ep.SaslIam), kgo.DialTLSConfig(tlsCfg),
			kgo.SASL(mskIAM("AKIAMSKTEST", "wrong")),
			kgo.RequestRetries(1), kgo.RetryTimeout(5 * time.Second),
		}},
	}

	t.Run("clients", func(t *testing.T) {
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				rerr := roundTrip(t, tt.opts...)
				if tt.wantErr {
					require.Error(t, rerr)

					return
				}

				require.NoError(t, rerr)
			})
		}
	})

	_, err = b.BatchDisassociateScramSecret(t.Context(), c.ClusterArn, []string{secretArn})
	require.NoError(t, err)

	err = roundTrip(t, kgo.SeedBrokers(ep.SaslScram), kgo.DialTLSConfig(tlsCfg),
		kgo.SASL(scram.Auth{User: "alice", Pass: "s3cret"}.AsSha512Mechanism()),
		kgo.RequestRetries(1), kgo.RetryTimeout(5*time.Second))
	require.Error(t, err)
	assert.NotErrorIs(t, err, context.Canceled)
}
