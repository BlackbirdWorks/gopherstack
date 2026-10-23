package kafka_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

const certValidity = time.Hour

type clientPKI struct {
	client tls.Certificate
	caPEM  []byte
}

func newClientPKI(t *testing.T) clientPKI {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "msk-client-ca"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(certValidity),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)

	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	leaf := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "msk-client"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(certValidity),
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, KeyUsage: x509.KeyUsageDigitalSignature,
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, caCert, &key.PublicKey, caKey)
	require.NoError(t, err)

	return clientPKI{
		caPEM:  pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		client: tls.Certificate{Certificate: [][]byte{leafDER}, PrivateKey: key},
	}
}

func TestBroker_UpdateSecurityEnablesMutualTLSDocker(t *testing.T) {
	t.Parallel()

	rt := dockerRuntimeOrSkip(t)
	b := newTestBackend(t)
	b.EnableBrokers(kafka.BrokerConfig{Runtime: rt, StartTimeout: 3 * time.Minute})
	t.Cleanup(b.Close)

	pki := newClientPKI(t)

	const caArn = "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority/mtls"

	b.SetClientCAResolver(func(_ context.Context, arn string) ([]byte, error) {
		if arn != caArn {
			return nil, errCAGone
		}

		return pki.caPEM, nil
	})

	c, err := b.CreateCluster(t.Context(), "mtls-live", "3.9.1", 1, kafka.BrokerNodeGroupInfo{}, nil, nil)
	require.NoError(t, err)
	awaitActive(t, b, c.ClusterArn)

	op, err := b.UpdateSecurity(t.Context(), c.ClusterArn, kafka.UpdateSecuritySettings{
		ClientAuthentication: &kafka.ClientAuthentication{TLS: &kafka.TLSSettings{
			Enabled: true, CertificateAuthorityArnList: []string{caArn},
		}},
		EncryptionInfo: &kafka.EncryptionInfo{EncryptionInTransit: &kafka.EncryptionInTransit{ClientBroker: "TLS"}},
	})
	require.NoError(t, err)
	assert.Equal(t, kafka.ClusterOperationStateUpdateInProgress, op.OperationState)
	awaitActive(t, b, c.ClusterArn)
	assert.Equal(t, kafka.ClusterOperationStateUpdateComplete,
		operationByArn(t, b, op.ClusterOperationArn).OperationState)

	ep, managed := b.ManagedEndpointsFor(c.ClusterArn)
	require.True(t, managed)
	require.NotEmpty(t, ep.TLS)
	assert.Empty(t, ep.Plaintext)

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(b.BrokerCAPEM()))

	withCert := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pki.client}}
	noCert := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}

	t.Run("client_certificate_accepted", func(t *testing.T) {
		t.Parallel()

		require.NoError(t, roundTrip(t, kgo.SeedBrokers(ep.TLS), kgo.DialTLSConfig(withCert)))
	})

	t.Run("missing_certificate_rejected", func(t *testing.T) {
		t.Parallel()

		require.Error(t, roundTrip(t, kgo.SeedBrokers(ep.TLS), kgo.DialTLSConfig(noCert),
			kgo.RequestRetries(1), kgo.RetryTimeout(5*time.Second)))
	})
}

func awaitActive(t *testing.T, b *kafka.InMemoryBackend, arn string) {
	t.Helper()

	require.Eventually(t, func() bool {
		got, err := b.DescribeCluster(t.Context(), arn)

		return err == nil && got.State == kafka.ClusterStateActive
	}, 4*time.Minute, 500*time.Millisecond)
}

const perAPIPolicy = `{"Version":"2012-10-17","Statement":[
 {"Effect":"Allow","Action":["kafka-cluster:Connect","kafka-cluster:WriteDataIdempotently"],"Resource":"*"},
 {"Effect":"Allow","Action":["kafka-cluster:DescribeTopic","kafka-cluster:WriteData"],
  "Resource":"arn:aws:kafka:*:*:topic/*/*/allowed-topic"}]}`

type policyIdentities struct {
	fakeIdentities
	policy string
}

func (p policyIdentities) GetPoliciesForUser(string) ([]string, error) {
	return []string{p.policy}, nil
}

func TestBroker_IAMPerAPIActionsDocker(t *testing.T) {
	t.Parallel()

	rt := dockerRuntimeOrSkip(t)
	b := newTestBackend(t)
	b.EnableBrokers(kafka.BrokerConfig{Runtime: rt, StartTimeout: 3 * time.Minute})
	t.Cleanup(b.Close)

	b.SetIAMAuth(func() kafka.IAMAuthConfig {
		return kafka.IAMAuthConfig{
			Identities: policyIdentities{fakeIdentities: fakeIdentities{secret: "iam-secret"}, policy: perAPIPolicy},
			Enforce:    true,
		}
	})

	c, err := b.CreateCluster(t.Context(), "iam-api", "3.9.1", 1, kafka.BrokerNodeGroupInfo{},
		&kafka.ClientAuthentication{Sasl: &kafka.SaslSettings{Iam: &kafka.SaslIam{Enabled: true}}}, nil,
		kafka.ClusterCreateOptions{EncryptionInfo: &kafka.EncryptionInfo{
			EncryptionInTransit: &kafka.EncryptionInTransit{ClientBroker: "TLS"},
		}})
	require.NoError(t, err)
	awaitActive(t, b, c.ClusterArn)

	ep, _ := b.ManagedEndpointsFor(c.ClusterArn)
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(b.BrokerCAPEM()))

	produce := func(topic string) error {
		cl, cerr := kgo.NewClient(kgo.SeedBrokers(ep.SaslIam),
			kgo.DialTLSConfig(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}),
			kgo.SASL(mskIAM("AKIAMSKTEST", "iam-secret")), kgo.AllowAutoTopicCreation(),
			kgo.RecordDeliveryTimeout(15*time.Second), kgo.RecordRetries(1))
		require.NoError(t, cerr)

		defer cl.Close()

		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		return cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("v")}).FirstErr()
	}

	tests := []struct {
		wantErr error
		name    string
		topic   string
	}{
		{name: "allowed_topic", topic: "allowed-topic"},
		{name: "denied_topic", topic: "other-topic", wantErr: kerr.TopicAuthorizationFailed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			perr := produce(tt.topic)
			if tt.wantErr == nil {
				require.NoError(t, perr)

				return
			}

			require.ErrorIs(t, perr, tt.wantErr)
		})
	}
}
