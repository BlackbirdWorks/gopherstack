package sqs_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sqssdk "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateQueue_FifoAttributeNeedsFifoSuffix_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		attrs    map[string]string
		name     string
		queue    string
		wantCode string
	}{
		{
			name:     "attr without suffix",
			queue:    "plain",
			attrs:    map[string]string{"FifoQueue": "true"},
			wantCode: "InvalidParameterValue",
		},
		{name: "attr with suffix", queue: "ok.fifo", attrs: map[string]string{"FifoQueue": "true"}},
		{name: "standard", queue: "std"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, _ := newSQSTestServer(t)
			_, err := client.CreateQueue(t.Context(), &sqssdk.CreateQueueInput{
				QueueName: aws.String(tc.queue), Attributes: tc.attrs,
			})
			if tc.wantCode == "" {
				require.NoError(t, err)

				return
			}

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.True(t, strings.HasSuffix(apiErr.ErrorCode(), tc.wantCode), apiErr.ErrorCode())
		})
	}
}

func TestSendMessage_ContentErrors_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode string
	}{
		{name: "valid", body: "hello\t\n\r \U0001F600"},
		{name: "control char", body: "a\x01b", wantCode: "InvalidMessageContents"},
		{name: "nul", body: "a\x00b", wantCode: "InvalidMessageContents"},
		{name: "oversized", body: strings.Repeat("x", 262145), wantCode: "InvalidParameterValue"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, _ := newSQSTestServer(t)
			q, err := client.CreateQueue(t.Context(), &sqssdk.CreateQueueInput{QueueName: aws.String("content")})
			require.NoError(t, err)

			_, err = client.SendMessage(t.Context(), &sqssdk.SendMessageInput{
				QueueUrl: q.QueueUrl, MessageBody: aws.String(tc.body),
			})
			if tc.wantCode == "" {
				require.NoError(t, err)

				return
			}

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.True(t, strings.HasSuffix(apiErr.ErrorCode(), tc.wantCode), apiErr.ErrorCode())

			var invalid *sqstypes.InvalidMessageContents
			assert.Equal(t, tc.wantCode == "InvalidMessageContents", errors.As(err, &invalid))
		})
	}
}
