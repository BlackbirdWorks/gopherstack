package eventbridge_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	eventbridgesdk "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	eventbridgetypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

// PutRule and TestEventPattern both declare InvalidEventPatternException
// (eventbridge types/errors.go); InvalidParameterException names no SDK type.
func TestInvalidEventPattern_RealClient(t *testing.T) {
	t.Parallel()

	noRetry := func(o *eventbridgesdk.Options) { o.RetryMaxAttempts = 1 }

	tests := []struct {
		call func(c *eventbridgesdk.Client) error
		name string
	}{
		{
			name: "put rule",
			call: func(c *eventbridgesdk.Client) error {
				_, err := c.PutRule(t.Context(), &eventbridgesdk.PutRuleInput{
					Name:         aws.String("bad-pattern"),
					EventPattern: aws.String("not-json"),
				}, noRetry)

				return err
			},
		},
		{
			name: "test event pattern",
			call: func(c *eventbridgesdk.Client) error {
				_, err := c.TestEventPattern(t.Context(), &eventbridgesdk.TestEventPatternInput{
					EventPattern: aws.String("not-json"),
					Event:        aws.String(`{"source":"x"}`),
				}, noRetry)

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEventBridgeClient(t, eventbridge.NewHandler(newBackend()))

			var typed *eventbridgetypes.InvalidEventPatternException
			require.ErrorAs(t, tt.call(client), &typed)
		})
	}
}

func TestMissingRequiredField_ValidationException_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "empty event", want: "ValidationException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEventBridgeClient(t, eventbridge.NewHandler(newBackend()))

			_, err := client.TestEventPattern(t.Context(), &eventbridgesdk.TestEventPatternInput{
				EventPattern: aws.String(`{"source":["x"]}`),
				Event:        aws.String(""),
			}, func(o *eventbridgesdk.Options) { o.RetryMaxAttempts = 1 })

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.want, apiErr.ErrorCode())
		})
	}
}
