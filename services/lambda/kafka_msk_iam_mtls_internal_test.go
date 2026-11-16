package lambda

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json/v2"
	"encoding/pem"
	"math/big"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	clientCertSecretARN = "arn:aws:secretsmanager:us-east-1:000000000000:secret:msk-client-AbCdEf"
	mskTestRoleARN      = "arn:aws:iam::000000000000:role/esm"
)

type countingIssuer struct{ calls atomic.Int32 }

func (c *countingIssuer) IssueFunctionCredentials(role, _ string) (RoleCredentials, error) {
	n := c.calls.Add(1)

	return RoleCredentials{
		AccessKeyID: "ASIA" + role[len(role)-3:], SecretAccessKey: "secret", SessionToken: "tok" + strconv.Itoa(int(n)),
		Expires: time.Now().Add(time.Hour),
	}, nil
}

type staticSecrets map[string]string

func (s staticSecrets) SecretString(_ context.Context, arn string) (string, error) {
	return s[arn], nil
}

func selfSignedPair(t *testing.T) (string, string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "esm"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
}

func TestMSKESMIAMAndMutualTLS(t *testing.T) {
	t.Parallel()

	certPEM, keyPEM := selfSignedPair(t)
	secretDoc, err := json.Marshal(map[string]string{"certificate": certPEM, "privateKey": keyPEM}, jsonx.V1())
	require.NoError(t, err)

	tests := []struct {
		secrets  MQSecretResolver
		check    func(t *testing.T, cfg kafkaConsumerConfig)
		name     string
		conn     MSKConnection
		wantPoll bool
	}{
		{
			name:     "iam",
			wantPoll: true,
			conn: MSKConnection{
				Brokers:   []string{"127.0.0.1:19098"},
				RootCAPEM: []byte("pem"),
				IAM:       true,
				IAMRegion: "eu-west-1",
			},
			check: func(t *testing.T, cfg kafkaConsumerConfig) {
				t.Helper()

				require.NotNil(t, cfg.IAMCredentials)
				assert.Equal(t, "eu-west-1", cfg.IAMRegion)

				auth, authErr := cfg.IAMCredentials(t.Context())
				require.NoError(t, authErr)
				assert.Equal(t, "ASIAesm", auth.AccessKey)
				assert.NotEmpty(t, auth.SessionToken)
			},
		},
		{
			name: "mutual_tls", wantPoll: true, secrets: staticSecrets{clientCertSecretARN: string(secretDoc)},
			conn: MSKConnection{Brokers: []string{"127.0.0.1:19094"}, RootCAPEM: []byte("pem"), MutualTLS: true},
			check: func(t *testing.T, cfg kafkaConsumerConfig) {
				t.Helper()

				assert.Equal(t, certPEM, string(cfg.ClientCertPEM))
				assert.Equal(t, keyPEM, string(cfg.ClientKeyPEM))
			},
		},
		{
			name: "mutual_tls_without_secret",
			conn: MSKConnection{Brokers: []string{"127.0.0.1:19094"}, RootCAPEM: []byte("pem"), MutualTLS: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newKafkaTestBackend(t)
			b.SetRoleCredentialIssuer(&countingIssuer{})
			require.NoError(t, b.CreateFunction(&FunctionConfiguration{
				FunctionName: "fn", PackageType: PackageTypeZip, Runtime: "python3.12", Handler: "index.handler",
				ZipData: []byte("x"), Timeout: 30, Role: mskTestRoleARN,
			}))

			res := &fakeMSKConnResolver{conn: tt.conn}
			tracker := &consumerTracker{created: make(chan *fakeKafkaConsumer, 4)}

			p := NewEventSourcePoller(b, nil)
			p.SetMSKBrokerResolver(res)

			if tt.secrets != nil {
				p.SetMQSecretResolver(tt.secrets)
			}

			p.kafkaFactory = func(cfg kafkaConsumerConfig) (kafkaConsumer, error) {
				tracker.mu.Lock()
				tracker.cfgs = append(tracker.cfgs, cfg)
				tracker.mu.Unlock()

				f := newFakeKafkaConsumer(kafkaOffsets(1))
				tracker.created <- f

				return f, nil
			}
			p.kafkaInvoker = func(context.Context, string, []byte) error { return nil }

			t.Cleanup(p.stopAllKafka)

			_, cerr := b.CreateEventSourceMapping(&CreateEventSourceMappingInput{
				FunctionName: "fn", EventSourceARN: mskTestARN, Enabled: true, Topics: []string{"orders"},
				SourceAccessConfigurations: []SourceAccessConfiguration{
					{Type: "CLIENT_CERTIFICATE_TLS_AUTH", URI: clientCertSecretARN},
				},
			})
			require.NoError(t, cerr)

			p.poll(t.Context())

			if !tt.wantPoll {
				assert.Empty(t, p.kafkaWorkers)

				return
			}

			awaitConsumer(t, tracker.created)

			tracker.mu.Lock()
			cfg := tracker.cfgs[0]
			tracker.mu.Unlock()

			tt.check(t, cfg)
		})
	}
}

func TestKafkaSecurityOptsIAMAndClientCert(t *testing.T) {
	t.Parallel()

	certPEM, keyPEM := selfSignedPair(t)

	tests := []struct {
		name    string
		cfg     kafkaConsumerConfig
		wantLen int
		wantErr bool
	}{
		{name: "client_cert", cfg: kafkaConsumerConfig{
			RootCAPEM: []byte(certPEM), ClientCertPEM: []byte(certPEM), ClientKeyPEM: []byte(keyPEM),
		}, wantLen: 1},
		{name: "bad_client_cert", cfg: kafkaConsumerConfig{
			RootCAPEM: []byte(certPEM), ClientCertPEM: []byte("junk"), ClientKeyPEM: []byte("junk"),
		}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			opts, err := kafkaSecurityOpts(tt.cfg)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, opts, tt.wantLen)
		})
	}
}
