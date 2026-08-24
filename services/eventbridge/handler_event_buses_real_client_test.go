package eventbridge_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	gsebe "github.com/blackbirdworks/gopherstack/services/eventbridge"
)

// TestEventBus_KmsDlqLogConfig_RealSDKClient proves the PARITY.md
// "KmsKeyIdentifier/DeadLetterConfig/LogConfig NOT added" note (deep sweep,
// parity-3, 2026-07-23) is stale: commit 69bbb940a (2026-08-15, #2417) added
// all three end to end. Drives a real CreateEventBus -> DescribeEventBus
// round trip through RouteMatcher (gopherstack-92ft, via the shared
// newTestEventBridgeClient helper in handler_partner_source_accounts_sdk_test.go)
// rather than calling the Handler directly.
func TestEventBus_KmsDlqLogConfig_RealSDKClient(t *testing.T) {
	t.Parallel()

	h := gsebe.NewHandler(gsebe.NewInMemoryBackend())
	client := newTestEventBridgeClient(t, h)

	_, err := client.CreateEventBus(t.Context(), &eventbridge.CreateEventBusInput{
		Name:             aws.String("kms-dlq-log-bus"),
		KmsKeyIdentifier: aws.String("alias/verify-key"),
		DeadLetterConfig: &ebtypes.DeadLetterConfig{
			Arn: aws.String("arn:aws:sqs:us-east-1:000000000000:verify-dlq"),
		},
		LogConfig: &ebtypes.LogConfig{
			Level:         ebtypes.LevelTrace,
			IncludeDetail: ebtypes.IncludeDetailFull,
		},
	})
	require.NoError(t, err)

	got, err := client.DescribeEventBus(t.Context(), &eventbridge.DescribeEventBusInput{
		Name: aws.String("kms-dlq-log-bus"),
	})
	require.NoError(t, err)

	assert.Equal(t, "alias/verify-key", aws.ToString(got.KmsKeyIdentifier))
	require.NotNil(t, got.DeadLetterConfig)
	assert.Equal(t, "arn:aws:sqs:us-east-1:000000000000:verify-dlq", aws.ToString(got.DeadLetterConfig.Arn))
	require.NotNil(t, got.LogConfig)
	assert.Equal(t, ebtypes.LevelTrace, got.LogConfig.Level)
	assert.Equal(t, ebtypes.IncludeDetailFull, got.LogConfig.IncludeDetail)
}

// TestArchive_KmsKeyIdentifier_RealSDKClient proves the PARITY.md
// "KmsKeyIdentifier NOT modeled" CreateArchive note is also stale, fixed by
// the same commit 69bbb940a as the EventBus fields above.
func TestArchive_KmsKeyIdentifier_RealSDKClient(t *testing.T) {
	t.Parallel()

	h := gsebe.NewHandler(gsebe.NewInMemoryBackend())
	client := newTestEventBridgeClient(t, h)

	_, err := client.CreateEventBus(t.Context(), &eventbridge.CreateEventBusInput{
		Name: aws.String("archive-src-bus"),
	})
	require.NoError(t, err)

	_, err = client.CreateArchive(t.Context(), &eventbridge.CreateArchiveInput{
		ArchiveName:      aws.String("kms-archive"),
		EventSourceArn:   aws.String("arn:aws:events:us-east-1:000000000000:event-bus/archive-src-bus"),
		KmsKeyIdentifier: aws.String("alias/verify-archive-key"),
	})
	require.NoError(t, err)

	got, err := client.DescribeArchive(t.Context(), &eventbridge.DescribeArchiveInput{
		ArchiveName: aws.String("kms-archive"),
	})
	require.NoError(t, err)
	assert.Equal(t, "alias/verify-archive-key", aws.ToString(got.KmsKeyIdentifier))
}
