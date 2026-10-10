package iotdataplane_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotdataplanesdk "github.com/aws/aws-sdk-go-v2/service/iotdataplane"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotdataplane"
)

func TestErrorMessages_NoCodePrefix(t *testing.T) {
	t.Parallel()

	update := func(payload string) func(*iotdataplanesdk.Client) error {
		return func(c *iotdataplanesdk.Client) error {
			_, err := c.UpdateThingShadow(t.Context(), &iotdataplanesdk.UpdateThingShadowInput{
				ThingName: aws.String("t"), Payload: []byte(payload),
			})

			return err
		}
	}

	tests := []struct {
		call    func(*iotdataplanesdk.Client) error
		name    string
		code    string
		message string
	}{
		{
			name: "get_missing", code: "ResourceNotFoundException",
			message: "No shadow exists with name: nope",
			call: func(c *iotdataplanesdk.Client) error {
				_, err := c.GetThingShadow(
					t.Context(),
					&iotdataplanesdk.GetThingShadowInput{ThingName: aws.String("nope")},
				)

				return err
			},
		},
		{name: "bad_version", code: "InvalidRequestException", message: "Invalid version",
			call: update(`{"state":{"desired":{"a":1}},"version":"x"}`)},
		{name: "no_state", code: "InvalidRequestException", message: "Missing required node: state",
			call: update(`{"foo":1}`)},
		{name: "bad_json", code: "InvalidRequestException", message: "Invalid JSON", call: update(`{`)},
		{name: "desired_scalar", code: "InvalidRequestException", message: "Desired node must be an object",
			call: update(`{"state":{"desired":5}}`)},
		{
			name: "version_conflict", code: "ConflictException", message: "Version conflict",
			call: update(`{"state":{"desired":{"a":1}},"version":7}`),
		},
		{
			name: "bad_token", code: "InvalidRequestException", message: "invalid nextToken",
			call: func(c *iotdataplanesdk.Client) error {
				_, err := c.ListRetainedMessages(t.Context(), &iotdataplanesdk.ListRetainedMessagesInput{
					NextToken: aws.String("garbage"),
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, _ := newTestIoTDataPlaneSDKClient(t, iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend()))

			err := tt.call(client)
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.code, apiErr.ErrorCode())
			assert.Equal(t, tt.message, apiErr.ErrorMessage())
		})
	}
}

func TestPublish_TopicSlashLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		topic   string
		wantErr bool
	}{
		{name: "eight_slashes", topic: "a/b/c/d/e/f/g/h/i", wantErr: false},
		{name: "nine_slashes", topic: "a/b/c/d/e/f/g/h/i/j", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, _ := newTestIoTDataPlaneSDKClient(t, iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend()))

			_, err := client.Publish(t.Context(), &iotdataplanesdk.PublishInput{
				Topic: aws.String(tt.topic), Payload: []byte("x"),
			})

			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "InvalidRequestException", apiErr.ErrorCode())
		})
	}
}
