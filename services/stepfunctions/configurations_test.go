package stepfunctions_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

const validRoleARN = "arn:aws:iam::000000000000:role/sfn-test"

const minimalDefinition = `{"StartAt":"S","States":{"S":{"Type":"Pass","End":true}}}`

func TestSetStateMachineConfigurations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*stepfunctions.InMemoryBackend) string
		tracing     *stepfunctions.TracingConfiguration
		logging     *stepfunctions.LoggingConfiguration
		name        string
		wantTracing bool
		wantLogging bool
		wantErr     bool
	}{
		{
			name: "set_both_configs",
			setup: func(b *stepfunctions.InMemoryBackend) string {
				sm, err := b.CreateStateMachine(
					context.Background(),
					"sm-both",
					minimalDefinition,
					validRoleARN,
					"STANDARD",
				)
				require.NoError(t, err)

				return sm.StateMachineArn
			},
			tracing:     &stepfunctions.TracingConfiguration{Enabled: true},
			logging:     &stepfunctions.LoggingConfiguration{Level: "ALL", IncludeExecutionData: true},
			wantTracing: true,
			wantLogging: true,
		},
		{
			name: "set_only_tracing",
			setup: func(b *stepfunctions.InMemoryBackend) string {
				sm, err := b.CreateStateMachine(
					context.Background(),
					"sm-tracing",
					minimalDefinition,
					validRoleARN,
					"STANDARD",
				)
				require.NoError(t, err)

				return sm.StateMachineArn
			},
			tracing:     &stepfunctions.TracingConfiguration{Enabled: true},
			wantTracing: true,
		},
		{
			name: "missing_state_machine_returns_error",
			setup: func(_ *stepfunctions.InMemoryBackend) string {
				return "arn:aws:states:us-east-1:000000000000:stateMachine:none"
			},
			tracing: &stepfunctions.TracingConfiguration{Enabled: true},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := stepfunctions.NewInMemoryBackend()
			arn := tt.setup(backend)
			err := backend.SetStateMachineConfigurations(arn, tt.tracing, tt.logging, nil)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			sm, err := backend.DescribeStateMachine(arn)
			require.NoError(t, err)

			if tt.wantTracing {
				require.NotNil(t, sm.TracingConfiguration)
				assert.True(t, sm.TracingConfiguration.Enabled)
			}

			if tt.wantLogging {
				require.NotNil(t, sm.LoggingConfiguration)
				assert.Equal(t, "ALL", sm.LoggingConfiguration.Level)
				assert.True(t, sm.LoggingConfiguration.IncludeExecutionData)
			}
		})
	}
}

func TestSetStateMachineConfigurations_Encryption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		encryption *stepfunctions.EncryptionConfiguration
		name       string
		wantType   string
	}{
		{
			name:       "nil_encryption_no_change",
			encryption: nil,
			wantType:   "",
		},
		{
			name: "set_customer_managed_key",
			encryption: &stepfunctions.EncryptionConfiguration{
				Type:     "CUSTOMER_MANAGED_KMS_KEY",
				KMSKeyID: "arn:aws:kms:us-east-1:123456789012:key/test",
			},
			wantType: "CUSTOMER_MANAGED_KMS_KEY",
		},
		{
			name: "set_aws_owned_key",
			encryption: &stepfunctions.EncryptionConfiguration{
				Type: "AWS_OWNED_KEY",
			},
			wantType: "AWS_OWNED_KEY",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := stepfunctions.NewInMemoryBackend()
			sm, err := b.CreateStateMachine(
				context.Background(),
				"enc-cfg-sm-"+tt.name,
				sfnPassDefinition,
				"arn:role",
				"STANDARD",
			)
			require.NoError(t, err)

			err = b.SetStateMachineConfigurations(sm.StateMachineArn, nil, nil, tt.encryption)
			require.NoError(t, err)

			described, err := b.DescribeStateMachine(sm.StateMachineArn)
			require.NoError(t, err)

			if tt.wantType == "" {
				assert.Nil(t, described.EncryptionConfiguration)
			} else {
				require.NotNil(t, described.EncryptionConfiguration)
				assert.Equal(t, tt.wantType, described.EncryptionConfiguration.Type)
			}
		})
	}
}

func TestConfig_LoggingPersisted(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(
		context.Background(),
		"log-sm",
		minimalDefinition,
		validRoleARN,
		"STANDARD",
	)
	require.NoError(t, err)

	logging := &stepfunctions.LoggingConfiguration{
		Level:                "ALL",
		IncludeExecutionData: true,
		Destinations: []stepfunctions.LoggingDestination{{
			CloudWatchLogsLogGroup: &stepfunctions.CloudWatchLogsLogGroup{
				LogGroupArn: "arn:aws:logs:us-east-1:123:log-group:sfn:*",
			},
		}},
	}

	require.NoError(t, b.SetStateMachineConfigurations(sm.StateMachineArn, nil, logging, nil))

	got, err := b.DescribeStateMachine(sm.StateMachineArn)
	require.NoError(t, err)
	require.NotNil(t, got.LoggingConfiguration)
	assert.Equal(t, "ALL", got.LoggingConfiguration.Level)
	assert.True(t, got.LoggingConfiguration.IncludeExecutionData)
	require.Len(t, got.LoggingConfiguration.Destinations, 1)
	assert.Equal(t, "arn:aws:logs:us-east-1:123:log-group:sfn:*",
		got.LoggingConfiguration.Destinations[0].CloudWatchLogsLogGroup.LogGroupArn)
}

func TestConfig_TracingPersisted(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(
		context.Background(),
		"trace-sm",
		minimalDefinition,
		validRoleARN,
		"STANDARD",
	)
	require.NoError(t, err)

	require.NoError(t, b.SetStateMachineConfigurations(sm.StateMachineArn,
		&stepfunctions.TracingConfiguration{Enabled: true}, nil, nil))

	got, err := b.DescribeStateMachine(sm.StateMachineArn)
	require.NoError(t, err)
	require.NotNil(t, got.TracingConfiguration)
	assert.True(t, got.TracingConfiguration.Enabled)
}

func TestConfig_EncryptionPersisted(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(
		context.Background(),
		"enc-sm",
		minimalDefinition,
		validRoleARN,
		"STANDARD",
	)
	require.NoError(t, err)

	enc := &stepfunctions.EncryptionConfiguration{
		KMSKeyID:                     "arn:aws:kms:us-east-1:123:key/abc",
		Type:                         "CUSTOMER_MANAGED_KMS_KEY",
		KMSDataKeyReusePeriodSeconds: 300,
	}

	require.NoError(t, b.SetStateMachineConfigurations(sm.StateMachineArn, nil, nil, enc))

	got, err := b.DescribeStateMachine(sm.StateMachineArn)
	require.NoError(t, err)
	require.NotNil(t, got.EncryptionConfiguration)
	assert.Equal(t, "arn:aws:kms:us-east-1:123:key/abc", got.EncryptionConfiguration.KMSKeyID)
	assert.Equal(t, 300, got.EncryptionConfiguration.KMSDataKeyReusePeriodSeconds)
}

func TestConfig_NilArgDoesNotClearExisting(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(
		context.Background(),
		"nil-cfg-sm",
		minimalDefinition,
		validRoleARN,
		"STANDARD",
	)
	require.NoError(t, err)

	require.NoError(t, b.SetStateMachineConfigurations(sm.StateMachineArn,
		&stepfunctions.TracingConfiguration{Enabled: true}, nil, nil))

	// Passing nil logging must not clear existing tracing.
	require.NoError(t, b.SetStateMachineConfigurations(sm.StateMachineArn, nil,
		&stepfunctions.LoggingConfiguration{Level: "ERROR"}, nil))

	got, err := b.DescribeStateMachine(sm.StateMachineArn)
	require.NoError(t, err)
	require.NotNil(t, got.TracingConfiguration, "tracing should not be cleared by nil update")
	assert.True(t, got.TracingConfiguration.Enabled)
	require.NotNil(t, got.LoggingConfiguration)
	assert.Equal(t, "ERROR", got.LoggingConfiguration.Level)
}

// ─── ValidateStateMachineDefinition (HTTP) ────────────────────────────────────
