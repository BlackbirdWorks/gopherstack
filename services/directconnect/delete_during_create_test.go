package directconnect_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	directconnectsdk "github.com/aws/aws-sdk-go-v2/service/directconnect"
	"github.com/aws/aws-sdk-go-v2/service/directconnect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/directconnect"
)

// aws_dx_lag deletes the LAG's auto-created connection immediately after
// create; the creation chain must not resurrect it to "available".
func TestDeleteConnection_DuringCreationChainStaysDeleted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		lag  bool
	}{
		{name: "standalone connection"},
		{name: "lag child connection", lag: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := directconnect.NewHandler(directconnect.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1"))
			client := newRoundTripClient(t, h)

			var connID *string

			if tt.lag {
				lag, err := client.CreateLag(t.Context(), &directconnectsdk.CreateLagInput{
					LagName: aws.String("l"), NumberOfConnections: 1,
					ConnectionsBandwidth: aws.String("1Gbps"), Location: aws.String("EqDC2"),
				})
				require.NoError(t, err)
				require.Len(t, lag.Connections, 1)

				connID = lag.Connections[0].ConnectionId
			} else {
				c, err := client.CreateConnection(t.Context(), &directconnectsdk.CreateConnectionInput{
					ConnectionName: aws.String("c"), Bandwidth: aws.String("1Gbps"), Location: aws.String("EqDC2"),
				})
				require.NoError(t, err)

				connID = c.ConnectionId
			}

			_, err := client.DeleteConnection(t.Context(), &directconnectsdk.DeleteConnectionInput{
				ConnectionId: connID,
			})
			require.NoError(t, err)

			state := func() types.ConnectionState {
				out, descErr := client.DescribeConnections(t.Context(), &directconnectsdk.DescribeConnectionsInput{
					ConnectionId: connID,
				})
				if descErr != nil || len(out.Connections) != 1 {
					return ""
				}

				return out.Connections[0].ConnectionState
			}

			require.Eventually(t, func() bool { return state() == types.ConnectionStateDeleted },
				defaultAsyncWait, defaultAsyncPoll)

			assert.Never(t, func() bool { return state() != types.ConnectionStateDeleted },
				500*time.Millisecond, defaultAsyncPoll)
		})
	}
}
