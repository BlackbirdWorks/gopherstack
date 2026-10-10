package ram_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ramsdk "github.com/aws/aws-sdk-go-v2/service/ram"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ram"
)

func TestClientTokenReplay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		token     string
		wantRetry bool
	}{
		{name: "same token replays", token: "tok-1", wantRetry: true},
		{name: "no token errors", token: "", wantRetry: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ram.NewInMemoryBackend("000000000000", "us-east-1")
			share, err := backend.CreateResourceShare("replay", false, nil, nil, nil)
			require.NoError(t, err)

			client := newRoundTripClient(t, ram.NewHandler(backend))
			in := &ramsdk.DeleteResourceShareInput{ResourceShareArn: aws.String(share.ARN)}
			if tt.token != "" {
				in.ClientToken = aws.String(tt.token)
			}

			_, err = client.DeleteResourceShare(t.Context(), in)
			require.NoError(t, err)

			_, err = client.DeleteResourceShare(t.Context(), in)
			if tt.wantRetry {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestClientTokenParamMismatch(t *testing.T) {
	t.Parallel()

	backend := ram.NewInMemoryBackend("000000000000", "us-east-1")
	share, err := backend.CreateResourceShare("mismatch", false, nil, nil, nil)
	require.NoError(t, err)

	client := newRoundTripClient(t, ram.NewHandler(backend))

	_, err = client.UpdateResourceShare(t.Context(), &ramsdk.UpdateResourceShareInput{
		ResourceShareArn: aws.String(share.ARN), Name: aws.String("one"), ClientToken: aws.String("tok"),
	})
	require.NoError(t, err)

	_, err = client.UpdateResourceShare(t.Context(), &ramsdk.UpdateResourceShareInput{
		ResourceShareArn: aws.String(share.ARN), Name: aws.String("two"), ClientToken: aws.String("tok"),
	})
	require.ErrorContains(t, err, "IdempotentParameterMismatch")
}
