package sqs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sqssdk "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
)

func TestGetQueueUrl_QueueOwnerAWSAccountId_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		owner   *string
		name    string
		wantErr bool
	}{
		{name: "unset", owner: nil},
		{name: "own account", owner: aws.String(config.DefaultAccountID)},
		{name: "other account", owner: aws.String("999999999999"), wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, _ := newSQSTestServer(t)
			created, err := client.CreateQueue(t.Context(), &sqssdk.CreateQueueInput{QueueName: aws.String("owned")})
			require.NoError(t, err)

			out, err := client.GetQueueUrl(t.Context(), &sqssdk.GetQueueUrlInput{
				QueueName: aws.String("owned"), QueueOwnerAWSAccountId: tc.owner,
			})
			if tc.wantErr {
				var target *sqstypes.QueueDoesNotExist
				require.ErrorAs(t, err, &target)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, aws.ToString(created.QueueUrl), aws.ToString(out.QueueUrl))
		})
	}
}
