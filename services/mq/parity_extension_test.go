package mq_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mq"
)

func newParityBackend(t *testing.T) *mq.InMemoryBackend {
	t.Helper()

	return mq.NewInMemoryBackend("000000000000", "us-east-1")
}

func TestCreateBrokerWithOptions_StorageAndIdempotency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		engine      string
		opts        *mq.CreateBrokerOptions
		wantStorage string
		wantErr     bool
	}{
		{
			name:        "rabbitmq_default_ebs",
			engine:      mq.EngineTypeRabbitMQ,
			opts:        nil,
			wantStorage: mq.StorageTypeEBS,
		},
		{
			name:        "activemq_default_efs",
			engine:      mq.EngineTypeActiveMQ,
			opts:        nil,
			wantStorage: mq.StorageTypeEFS,
		},
		{
			name:    "rabbitmq_rejects_efs",
			engine:  mq.EngineTypeRabbitMQ,
			opts:    &mq.CreateBrokerOptions{StorageType: mq.StorageTypeEFS},
			wantErr: true,
		},
		{
			name:        "activemq_accepts_ebs",
			engine:      mq.EngineTypeActiveMQ,
			opts:        &mq.CreateBrokerOptions{StorageType: mq.StorageTypeEBS},
			wantStorage: mq.StorageTypeEBS,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newParityBackend(t)
			br, err := b.CreateBrokerWithOptions(
				"b-"+tt.name, mq.DeploymentModeSingleInstance, tt.engine,
				"", "", false, false, nil, nil, nil, nil, tt.opts,
			)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantStorage, br.StorageType)
		})
	}
}

func TestCreateBroker_CreatorRequestIDIdempotency(t *testing.T) {
	t.Parallel()

	b := newParityBackend(t)

	first, err := b.CreateBrokerWithOptions(
		"idem-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "",
		false, false, nil, nil, nil, nil,
		&mq.CreateBrokerOptions{CreatorRequestID: "req-1"},
	)
	require.NoError(t, err)

	second, err := b.CreateBrokerWithOptions(
		"idem-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "",
		false, false, nil, nil, nil, nil,
		&mq.CreateBrokerOptions{CreatorRequestID: "req-1"},
	)
	require.NoError(t, err)
	assert.Equal(t, first.BrokerID, second.BrokerID)

	// Same name, no idempotency token => duplicate-name error.
	_, err = b.CreateBrokerWithOptions(
		"idem-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "",
		false, false, nil, nil, nil, nil, nil,
	)
	require.Error(t, err)
}

func TestRebootBroker_StateTransition(t *testing.T) {
	t.Parallel()

	b := newParityBackend(t)
	br, err := b.CreateBroker(
		"reboot-me", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "",
		false, false, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	assert.Equal(t, mq.BrokerStateRunning, br.BrokerState)

	require.NoError(t, b.RebootBroker(br.BrokerID))

	rebooting, err := b.DescribeBroker(br.BrokerID)
	require.NoError(t, err)
	assert.Equal(t, mq.BrokerStateRebooting, rebooting.BrokerState)

	settled, err := b.DescribeBroker(br.BrokerID)
	require.NoError(t, err)
	assert.Equal(t, mq.BrokerStateRunning, settled.BrokerState)
}

func TestUpdateConfiguration_DataSizeLimit(t *testing.T) {
	t.Parallel()

	b := newParityBackend(t)
	cfg, err := b.CreateConfiguration(
		"cfg-size", "desc", mq.EngineTypeActiveMQ, "", nil,
	)
	require.NoError(t, err)

	huge := strings.Repeat("a", 256*1024+1)
	_, err = b.UpdateConfiguration(cfg.ID, "too big", huge)
	require.Error(t, err)
}

func TestCreateBrokerWithOptions_PersistsLogsAndEncryption(t *testing.T) {
	t.Parallel()

	b := newParityBackend(t)
	br, err := b.CreateBrokerWithOptions(
		"feat-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "", false, false,
		nil, nil, nil, nil,
		&mq.CreateBrokerOptions{
			AuthenticationStrategy: "SIMPLE",
			EncryptionOptions:      &mq.EncryptionOptions{UseAWSOwnedKey: true},
			Logs:                   &mq.Logs{General: true, Audit: true},
			MaintenanceWindowStartTime: &mq.WeeklyStartTime{
				DayOfWeek: "MONDAY", TimeOfDay: "03:00", TimeZone: "UTC",
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "SIMPLE", br.AuthenticationStrategy)
	require.NotNil(t, br.EncryptionOptions)
	assert.True(t, br.EncryptionOptions.UseAWSOwnedKey)
	require.NotNil(t, br.LogsSummary)
	assert.True(t, br.LogsSummary.General)
	assert.Contains(t, br.LogsSummary.GeneralLogGroup, br.BrokerID)
	require.NotNil(t, br.MaintenanceWindowStartTime)
	assert.Equal(t, "MONDAY", br.MaintenanceWindowStartTime.DayOfWeek)
}
