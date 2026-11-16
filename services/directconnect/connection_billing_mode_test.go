package directconnect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	directconnectsdk "github.com/aws/aws-sdk-go-v2/service/directconnect"
	"github.com/aws/aws-sdk-go-v2/service/directconnect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateConnection_BillingMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mode    types.RequestBillingMode
		want    types.BillingMode
		wantErr bool
	}{
		{name: "payg", mode: types.RequestBillingModePayAsYouGo, want: types.BillingModePayAsYouGo},
		{name: "flat", mode: types.RequestBillingModeFlatRateTier3, want: types.BillingModeFlatRateTier3},
		{name: "omitted", mode: "", want: ""},
		{name: "invalid", mode: "Bogus", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestHandlerAndClient(t)
			ctx := t.Context()

			out, err := client.CreateConnection(ctx, &directconnectsdk.CreateConnectionInput{
				Bandwidth:      aws.String("1Gbps"),
				ConnectionName: aws.String("c"),
				Location:       aws.String("EqDC2"),
				BillingMode:    tt.mode,
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, out.BillingMode)

			got, err := client.DescribeConnections(ctx, &directconnectsdk.DescribeConnectionsInput{
				ConnectionId: out.ConnectionId,
			})
			require.NoError(t, err)
			require.Len(t, got.Connections, 1)
			assert.Equal(t, tt.want, got.Connections[0].BillingMode)
		})
	}
}
