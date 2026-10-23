package lambda

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const scramSecretARN = "arn:aws:secretsmanager:us-east-1:000000000000:secret:AmazonMSK_app-AbCdEf"

type fakeMSKConnResolver struct {
	fakeMSKResolver
	conn      MSKConnection
	gotAccess []SourceAccessConfiguration
}

func (f *fakeMSKConnResolver) MSKConnection(
	_ context.Context, _ string, access []SourceAccessConfiguration,
) (MSKConnection, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.gotAccess = access

	return f.conn, len(f.conn.Brokers) > 0
}

func TestMSKESMUsesSecureConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantUser string
		conn     MSKConnection
		wantPEM  bool
		wantPoll bool
	}{
		{
			name: "scram_over_tls", wantUser: "alice", wantPEM: true, wantPoll: true,
			conn: MSKConnection{
				Brokers: []string{"127.0.0.1:19096"}, RootCAPEM: []byte("pem"), Username: "alice", Password: "pw",
			},
		},
		{
			name: "tls_only", wantPEM: true, wantPoll: true,
			conn: MSKConnection{Brokers: []string{"127.0.0.1:19094"}, RootCAPEM: []byte("pem")},
		},
		{name: "unreachable_cluster", conn: MSKConnection{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newKafkaTestBackend(t)
			res := &fakeMSKConnResolver{conn: tt.conn}
			tracker := &consumerTracker{created: make(chan *fakeKafkaConsumer, 4)}

			p := NewEventSourcePoller(b, nil)
			p.SetMSKBrokerResolver(res)
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

			_, err := b.CreateEventSourceMapping(&CreateEventSourceMappingInput{
				FunctionName: "fn", EventSourceARN: mskTestARN, Enabled: true, Topics: []string{"orders"},
				SourceAccessConfigurations: []SourceAccessConfiguration{
					{Type: "SASL_SCRAM_512_AUTH", URI: scramSecretARN},
				},
			})
			require.NoError(t, err)

			p.poll(t.Context())

			if !tt.wantPoll {
				assert.Empty(t, p.kafkaWorkers)

				return
			}

			awaitConsumer(t, tracker.created)

			tracker.mu.Lock()
			cfg := tracker.cfgs[0]
			tracker.mu.Unlock()

			assert.Equal(t, tt.conn.Brokers, cfg.Brokers)
			assert.Equal(t, tt.wantUser, cfg.Username)
			assert.Equal(t, tt.wantPEM, len(cfg.RootCAPEM) > 0)

			res.mu.Lock()
			defer res.mu.Unlock()

			require.Len(t, res.gotAccess, 1)
			assert.Equal(t, scramSecretARN, res.gotAccess[0].URI)
		})
	}
}

func TestKafkaSecurityOpts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     kafkaConsumerConfig
		wantLen int
		wantErr bool
	}{
		{name: "plain", cfg: kafkaConsumerConfig{}},
		{name: "bad_pem", cfg: kafkaConsumerConfig{RootCAPEM: []byte("junk")}, wantErr: true},
		{name: "scram_only", cfg: kafkaConsumerConfig{Username: "u", Password: "p"}, wantLen: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			opts, err := kafkaSecurityOpts(tt.cfg)
			if tt.wantErr {
				require.ErrorIs(t, err, errKafkaRootCA)

				return
			}

			require.NoError(t, err)
			assert.Len(t, opts, tt.wantLen)
		})
	}
}
