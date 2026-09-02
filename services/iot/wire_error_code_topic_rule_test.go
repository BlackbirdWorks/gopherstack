package iot_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	"github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

// Test_TopicRuleFamily_UnknownRuleIsInvalidRequest proves that the whole
// TopicRule/TopicRuleDestination op family's unknown-name path is
// wire-shape-wrong. Unlike almost everything else in this service, none of
// GetTopicRule/DeleteTopicRule/DisableTopicRule/EnableTopicRule/
// ReplaceTopicRule/GetTopicRuleDestination/UpdateTopicRuleDestination/
// DeleteTopicRuleDestination's own deserializeOpError switches
// (iot@v1.77.4/deserializers.go) declare a ResourceNotFoundException case --
// this family's real vocabulary is
// {InternalException, InvalidRequestException, ServiceUnavailableException,
// UnauthorizedException} plus ConflictingResourceUpdateException/
// SqlParseException where applicable, confirmed by direct per-op read.
// gopherstack's backend wraps the shared ErrRuleNotFound/
// ErrTopicRuleDestinationNotFound sentinels for an unknown name, which
// writeIoTError renders as ResourceNotFoundException -- a code none of
// these operations' real deserializer switches match, so each falls to its
// switch's default case and produces a *smithy.GenericAPIError instead of
// any typed exception.
func Test_TopicRuleFamily_UnknownRuleIsInvalidRequest(t *testing.T) {
	t.Parallel()

	newClient := func(t *testing.T) *iotsdk.Client {
		t.Helper()

		backend := iot.NewInMemoryBackend()
		h := iot.NewHandler(backend, nil)

		return newTestIoTClient(t, h)
	}

	t.Run("GetTopicRule", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.GetTopicRule(t.Context(), &iotsdk.GetTopicRuleInput{
			RuleName: aws.String("no-such-rule"),
		})
		require.Error(t, err)

		var ire *types.InvalidRequestException
		require.ErrorAs(t, err, &ire, "expected a typed InvalidRequestException, got: %v", err)
	})

	t.Run("DeleteTopicRule", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.DeleteTopicRule(t.Context(), &iotsdk.DeleteTopicRuleInput{
			RuleName: aws.String("no-such-rule"),
		})
		require.Error(t, err)

		var ire *types.InvalidRequestException
		require.ErrorAs(t, err, &ire, "expected a typed InvalidRequestException, got: %v", err)
	})

	t.Run("EnableTopicRule", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.EnableTopicRule(t.Context(), &iotsdk.EnableTopicRuleInput{
			RuleName: aws.String("no-such-rule"),
		})
		require.Error(t, err)

		var ire *types.InvalidRequestException
		require.ErrorAs(t, err, &ire, "expected a typed InvalidRequestException, got: %v", err)
	})

	t.Run("DisableTopicRule", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.DisableTopicRule(t.Context(), &iotsdk.DisableTopicRuleInput{
			RuleName: aws.String("no-such-rule"),
		})
		require.Error(t, err)

		var ire *types.InvalidRequestException
		require.ErrorAs(t, err, &ire, "expected a typed InvalidRequestException, got: %v", err)
	})

	t.Run("ReplaceTopicRule", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.ReplaceTopicRule(t.Context(), &iotsdk.ReplaceTopicRuleInput{
			RuleName: aws.String("no-such-rule"),
			TopicRulePayload: &types.TopicRulePayload{
				Sql: aws.String("SELECT * FROM 'topic'"),
				Actions: []types.Action{
					{
						Republish: &types.RepublishAction{
							RoleArn: aws.String("arn:aws:iam::000000000000:role/x"),
							Topic:   aws.String("t"),
						},
					},
				},
			},
		})
		require.Error(t, err)

		var ire *types.InvalidRequestException
		require.ErrorAs(t, err, &ire, "expected a typed InvalidRequestException, got: %v", err)
	})

	t.Run("GetTopicRuleDestination", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.GetTopicRuleDestination(t.Context(), &iotsdk.GetTopicRuleDestinationInput{
			Arn: aws.String("arn:aws:iot:us-east-1:000000000000:ruledestination/http/no-such-dest"),
		})
		require.Error(t, err)

		var ire *types.InvalidRequestException
		require.ErrorAs(t, err, &ire, "expected a typed InvalidRequestException, got: %v", err)
	})

	t.Run("UpdateTopicRuleDestination", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.UpdateTopicRuleDestination(t.Context(), &iotsdk.UpdateTopicRuleDestinationInput{
			Arn:    aws.String("arn:aws:iot:us-east-1:000000000000:ruledestination/http/no-such-dest"),
			Status: types.TopicRuleDestinationStatusEnabled,
		})
		require.Error(t, err)

		var ire *types.InvalidRequestException
		require.ErrorAs(t, err, &ire, "expected a typed InvalidRequestException, got: %v", err)
	})

	t.Run("DeleteTopicRuleDestination", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.DeleteTopicRuleDestination(t.Context(), &iotsdk.DeleteTopicRuleDestinationInput{
			Arn: aws.String("arn:aws:iot:us-east-1:000000000000:ruledestination/http/no-such-dest"),
		})
		require.Error(t, err)

		var ire *types.InvalidRequestException
		require.ErrorAs(t, err, &ire, "expected a typed InvalidRequestException, got: %v", err)
	})
}
