package eventbridge_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

func TestResolveConnectionAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr     error
		name        string
		arn         string
		wantKey     string
		deauthorize bool
	}{
		{name: "authorized", wantKey: "k1"},
		{name: "deauthorized", deauthorize: true, wantErr: eventbridge.ErrConnectionNotAuthorized},
		{
			name:    "unknown",
			arn:     "arn:aws:events:us-east-1:123456789012:connection/absent",
			wantErr: eventbridge.ErrNotFound,
		},
		{name: "malformed", arn: "not-an-arn", wantErr: eventbridge.ErrNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			conn, err := b.CreateConnection(t.Context(), eventbridge.CreateConnectionInput{
				Name: "c", AuthorizationType: "API_KEY",
				AuthParameters: &eventbridge.ConnectionAuthParameters{
					APIKeyAuthParameters: &eventbridge.ConnectionAPIKeyAuthParameters{
						APIKeyName:  "k1",
						APIKeyValue: "v",
					},
					InvocationHTTPParameters: &eventbridge.ConnectionHTTPParameters{
						HeaderParameters: []eventbridge.ConnectionHeaderParameter{{Key: "h", Value: "1"}},
					},
				},
			})
			require.NoError(t, err)

			if tt.deauthorize {
				_, err = b.DeauthorizeConnection(t.Context(), "c")
				require.NoError(t, err)
			}

			arn := tt.arn
			if arn == "" {
				arn = conn.ConnectionArn
			}

			got, err := b.ResolveConnectionAuth(arn)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantKey, got.APIKeyName)
			assert.Equal(t, "v", got.APIKeyValue)

			got.HeaderParameters[0].Value = "mutated"

			again, err := b.ResolveConnectionAuth(arn)
			require.NoError(t, err)
			assert.Equal(t, "1", again.HeaderParameters[0].Value)
		})
	}
}
