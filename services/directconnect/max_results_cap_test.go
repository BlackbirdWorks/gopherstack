package directconnect_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	directconnectsdk "github.com/aws/aws-sdk-go-v2/service/directconnect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// "If MaxResults is given a value larger than 100, only 100 results are returned."
// (api_op_DescribeDirectConnectGateways.go:37).
func TestDescribeDirectConnectGateways_MaxResultsCap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		maxRes   *int32
		name     string
		wantLen  int
		wantNext bool
	}{
		{name: "over cap", maxRes: aws.Int32(500), wantLen: 100, wantNext: true},
		{name: "default", wantLen: 100, wantNext: true},
		{name: "explicit", maxRes: aws.Int32(7), wantLen: 7, wantNext: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestHandlerAndClient(t)
			ctx := t.Context()

			for i := range 105 {
				_, err := client.CreateDirectConnectGateway(ctx, &directconnectsdk.CreateDirectConnectGatewayInput{
					DirectConnectGatewayName: aws.String(fmt.Sprintf("gw-%03d", i)),
				})
				require.NoError(t, err)
			}

			out, err := client.DescribeDirectConnectGateways(ctx, &directconnectsdk.DescribeDirectConnectGatewaysInput{
				MaxResults: tt.maxRes,
			})
			require.NoError(t, err)
			assert.Len(t, out.DirectConnectGateways, tt.wantLen)
			assert.Equal(t, tt.wantNext, out.NextToken != nil)
		})
	}
}
