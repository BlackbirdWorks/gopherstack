package kafka_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

var errCAGone = errors.New("ca not found")

func operationByArn(t *testing.T, b *kafka.InMemoryBackend, arn string) *kafka.ClusterOperation {
	t.Helper()

	op, err := b.DescribeClusterOperation(t.Context(), arn)
	require.NoError(t, err)

	return op
}

func TestUpdateSecurity_RelaunchesBroker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		startErr  error
		wantState string
		wantOp    string
	}{
		{
			name:      "success",
			wantState: kafka.ClusterStateActive,
			wantOp:    kafka.ClusterOperationStateUpdateComplete,
		},
		{
			name:      "relaunch_fails",
			startErr:  errBrokerDown,
			wantState: kafka.ClusterStateFailed,
			wantOp:    kafka.ClusterOperationStateUpdateFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := &fakeBrokerRuntime{}
			f := newBrokerFixture(t, rt, 0)
			f.ready.Store(true)

			arn := f.create(t, "sec-update")
			f.awaitState(t, arn, kafka.ClusterStateActive)

			f.ready.Store(false)
			gate := make(chan struct{})
			rt.mu.Lock()
			rt.startErr = tt.startErr
			rt.gate = gate
			rt.mu.Unlock()

			op, err := f.backend.UpdateSecurity(t.Context(), arn, kafka.UpdateSecuritySettings{
				ClientAuthentication: &kafka.ClientAuthentication{Sasl: &kafka.SaslSettings{
					Scram: &kafka.SaslScram{Enabled: true},
				}},
				EncryptionInfo: &kafka.EncryptionInfo{
					EncryptionInTransit: &kafka.EncryptionInTransit{ClientBroker: "TLS"},
				},
			})
			require.NoError(t, err)
			assert.Equal(t, kafka.ClusterOperationStateUpdateInProgress, op.OperationState)

			assert.Equal(t, kafka.ClusterStateUpdating, f.state(t, arn))
			assert.Equal(t, kafka.ClusterOperationStateUpdateInProgress,
				operationByArn(t, f.backend, op.ClusterOperationArn).OperationState)

			ops, err := f.backend.ListClusterOperations(t.Context(), arn)
			require.NoError(t, err)
			require.Len(t, ops, 1)
			assert.Equal(t, "UPDATE_SECURITY", ops[0].OperationType)

			f.ready.Store(true)
			close(gate)
			f.awaitState(t, arn, tt.wantState)

			done := operationByArn(t, f.backend, op.ClusterOperationArn)
			assert.Equal(t, tt.wantOp, done.OperationState)
			assert.NotEmpty(t, done.EndTime)

			require.Eventually(t, func() bool {
				return len(rt.removedIDs()) >= 1
			}, 10*time.Second, 5*time.Millisecond)
			assert.Equal(t, "ctr-1", rt.removedIDs()[0])

			if tt.startErr != nil {
				return
			}

			specs := rt.startedSpecs()
			require.Len(t, specs, 2)
			assert.Contains(t, specs[1].Env, "KAFKA_LISTENER_NAME_SCRAM_SASL_ENABLED_MECHANISMS=SCRAM-SHA-512")
			assert.NotContains(t, specs[0].Env, "KAFKA_LISTENER_NAME_SCRAM_SASL_ENABLED_MECHANISMS=SCRAM-SHA-512")

			c, derr := f.backend.DescribeCluster(t.Context(), arn)
			require.NoError(t, derr)
			assert.Empty(t, c.ActiveOperationArn)
		})
	}
}

func envValue(env []string, key string) (string, bool) {
	for _, e := range env {
		if v, ok := strings.CutPrefix(e, key+"="); ok {
			return v, true
		}
	}

	return "", false
}

func mountDir(t *testing.T, mounts []string) string {
	t.Helper()
	require.NotEmpty(t, mounts)

	dir, _, ok := strings.Cut(mounts[0], ":")
	require.True(t, ok)

	return dir
}

func TestBroker_MutualTLS(t *testing.T) {
	t.Parallel()

	const (
		caOne = "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority/one"
		caTwo = "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority/two"
	)

	pems := map[string]string{
		caOne: "-----BEGIN CERTIFICATE-----\nONE\n-----END CERTIFICATE-----\n",
		caTwo: "-----BEGIN CERTIFICATE-----\nTWO\n-----END CERTIFICATE-----\n",
	}

	resolver := func(_ context.Context, arn string) ([]byte, error) {
		if p, ok := pems[arn]; ok {
			return []byte(p), nil
		}

		return nil, errCAGone
	}

	tests := []struct {
		resolver     kafka.ClientCAResolver
		name         string
		wantAuth     string
		wantTrust    string
		arns         []string
		unauth       bool
		wantFailed   bool
		wantBrokerCA bool
	}{
		{
			name: "required", resolver: resolver, arns: []string{caOne, caTwo},
			wantAuth: "required", wantTrust: "ONE,TWO",
		},
		{name: "optional_with_unauthenticated", resolver: resolver, arns: []string{caOne}, unauth: true,
			wantAuth: "requested", wantTrust: "ONE"},
		{
			name: "unknown_ca_fails", resolver: resolver, wantFailed: true,
			arns: []string{"arn:aws:acm-pca:us-east-1:1:certificate-authority/x"},
		},
		{name: "no_resolver_trusts_broker_ca", arns: []string{caOne}, wantAuth: "required", wantBrokerCA: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := &fakeBrokerRuntime{}
			f := newBrokerFixture(t, rt, 0)
			f.ready.Store(true)

			if tt.resolver != nil {
				f.backend.SetClientCAResolver(tt.resolver)
			}

			auth := &kafka.ClientAuthentication{TLS: &kafka.TLSSettings{
				Enabled: true, CertificateAuthorityArnList: tt.arns,
			}}
			if tt.unauth {
				auth.Unauthenticated = &kafka.UnauthenticatedSettings{Enabled: true}
			}

			c, err := f.backend.CreateCluster(t.Context(), "mtls", "3.9.1", 1, kafka.BrokerNodeGroupInfo{}, auth, nil,
				kafka.ClusterCreateOptions{EncryptionInfo: &kafka.EncryptionInfo{
					EncryptionInTransit: &kafka.EncryptionInTransit{ClientBroker: "TLS"},
				}})
			require.NoError(t, err)

			if tt.wantFailed {
				f.awaitState(t, c.ClusterArn, kafka.ClusterStateFailed)
				assert.Empty(t, rt.startedSpecs())

				return
			}

			f.awaitState(t, c.ClusterArn, kafka.ClusterStateActive)

			specs := rt.startedSpecs()
			require.Len(t, specs, 1)

			got, _ := envValue(specs[0].Env, "KAFKA_LISTENER_NAME_TLS_SSL_CLIENT_AUTH")
			assert.Equal(t, tt.wantAuth, got)

			store, _ := envValue(specs[0].Env, "KAFKA_SSL_TRUSTSTORE_LOCATION")
			assert.True(t, strings.HasSuffix(store, "/truststore.pem"))

			trust, rerr := os.ReadFile(filepath.Join(mountDir(t, specs[0].Mounts), "truststore.pem"))
			require.NoError(t, rerr)

			if tt.wantBrokerCA {
				assert.Contains(t, string(trust), strings.TrimSpace(string(f.backend.BrokerCAPEM())))

				return
			}

			for want := range strings.SplitSeq(tt.wantTrust, ",") {
				assert.Contains(t, string(trust), want)
			}
		})
	}
}

func TestBroker_TLSWithoutCAListStaysServerOnly(t *testing.T) {
	t.Parallel()

	rt := &fakeBrokerRuntime{}
	f := newBrokerFixture(t, rt, 0)
	f.ready.Store(true)

	c, err := f.backend.CreateCluster(t.Context(), "tlsonly", "3.9.1", 1, kafka.BrokerNodeGroupInfo{},
		&kafka.ClientAuthentication{TLS: &kafka.TLSSettings{Enabled: true}}, nil)
	require.NoError(t, err)
	f.awaitState(t, c.ClusterArn, kafka.ClusterStateActive)

	specs := rt.startedSpecs()
	require.Len(t, specs, 1)

	_, has := envValue(specs[0].Env, "KAFKA_LISTENER_NAME_TLS_SSL_CLIENT_AUTH")
	assert.False(t, has)
}

func TestClientEndpointFor(t *testing.T) {
	t.Parallel()

	const secretArn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:AmazonMSK_app-AbCdEf"

	tests := []struct {
		auth       *kafka.ClientAuthentication
		name       string
		mode       string
		secret     string
		wantUser   string
		wantOK     bool
		wantRootCA bool
	}{
		{name: "plaintext", mode: "PLAINTEXT", wantOK: true},
		{name: "tls_only", mode: "TLS", auth: &kafka.ClientAuthentication{TLS: &kafka.TLSSettings{Enabled: true}},
			wantOK: true, wantRootCA: true},
		{name: "scram", mode: "TLS", secret: secretArn, wantUser: "alice", wantOK: true, wantRootCA: true,
			auth: &kafka.ClientAuthentication{Sasl: &kafka.SaslSettings{Scram: &kafka.SaslScram{Enabled: true}}}},
		{name: "scram_without_secret_falls_back_to_tls", mode: "TLS", wantOK: true, wantRootCA: true,
			auth: &kafka.ClientAuthentication{Sasl: &kafka.SaslSettings{Scram: &kafka.SaslScram{Enabled: true}}}},
		{name: "mutual_tls_only", mode: "TLS", auth: &kafka.ClientAuthentication{TLS: &kafka.TLSSettings{
			Enabled: true, CertificateAuthorityArnList: []string{"arn:aws:acm-pca:us-east-1:1:certificate-authority/x"},
		}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := &fakeBrokerRuntime{}
			f := newBrokerFixture(t, rt, 0)
			f.ready.Store(true)
			f.backend.SetScramSecretResolver(fakeScramSource{
				secretArn: {Username: "alice", Password: "pw", SecretName: "AmazonMSK_app"},
			})
			f.backend.SetClientCAResolver(func(context.Context, string) ([]byte, error) { return []byte("pem"), nil })

			c, err := f.backend.CreateCluster(t.Context(), "ep", "3.9.1", 1, kafka.BrokerNodeGroupInfo{}, tt.auth, nil,
				kafka.ClusterCreateOptions{EncryptionInfo: &kafka.EncryptionInfo{
					EncryptionInTransit: &kafka.EncryptionInTransit{ClientBroker: tt.mode},
				}})
			require.NoError(t, err)
			f.awaitState(t, c.ClusterArn, kafka.ClusterStateActive)

			ep, ok := f.backend.ClientEndpointFor(t.Context(), c.ClusterArn, tt.secret)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantUser, ep.Username)
			assert.Equal(t, tt.wantRootCA, len(ep.RootCAPEM) > 0)

			if tt.wantOK {
				assert.Len(t, ep.Brokers, 1)
			}
		})
	}
}

func TestSecurityUpdate_SupersededOperationSettles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		stop func(t *testing.T, f *brokerFixture, arn string)
		name string
	}{
		{
			name: "second_update",
			stop: func(t *testing.T, f *brokerFixture, arn string) {
				t.Helper()

				_, err := f.backend.UpdateSecurity(t.Context(), arn, kafka.UpdateSecuritySettings{
					EncryptionInfo: &kafka.EncryptionInfo{
						EncryptionInTransit: &kafka.EncryptionInTransit{ClientBroker: "TLS_PLAINTEXT"},
					},
				})
				require.NoError(t, err)
			},
		},
		{
			name: "delete_cluster",
			stop: func(t *testing.T, f *brokerFixture, arn string) {
				t.Helper()
				require.NoError(t, f.backend.DeleteCluster(t.Context(), arn))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := &fakeBrokerRuntime{}
			f := newBrokerFixture(t, rt, 0)
			f.ready.Store(true)

			arn := f.create(t, "superseded")
			f.awaitState(t, arn, kafka.ClusterStateActive)
			f.ready.Store(false)

			first, err := f.backend.UpdateSecurity(t.Context(), arn, kafka.UpdateSecuritySettings{
				EncryptionInfo: &kafka.EncryptionInfo{
					EncryptionInTransit: &kafka.EncryptionInTransit{ClientBroker: "TLS"},
				},
			})
			require.NoError(t, err)

			tt.stop(t, f, arn)

			assert.Equal(t, kafka.ClusterOperationStateUpdateFailed,
				operationByArn(t, f.backend, first.ClusterOperationArn).OperationState)
		})
	}
}
