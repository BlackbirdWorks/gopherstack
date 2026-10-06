package kafkaconnect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kafkaconnectsdk "github.com/aws/aws-sdk-go-v2/service/kafkaconnect"
	"github.com/aws/aws-sdk-go-v2/service/kafkaconnect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateConnector_NetworkTypeDefault(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   types.NetworkType
		want types.NetworkType
	}{
		{name: "omitted defaults to IPV4", want: types.NetworkTypeIpv4},
		{name: "explicit DUAL kept", in: types.NetworkTypeDual, want: types.NetworkTypeDual},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := newTestClient(t, newTestHandler())
			in := minimalCreateConnectorInput("c", createPlugin(t, client, "p"))
			in.NetworkType = tc.in

			created, err := client.CreateConnector(ctx, in)
			require.NoError(t, err)

			got, err := client.DescribeConnector(ctx, &kafkaconnectsdk.DescribeConnectorInput{
				ConnectorArn: created.ConnectorArn,
			})
			require.NoError(t, err)
			assert.Equal(t, tc.want, got.NetworkType)
			assert.Equal(t, "c", aws.ToString(got.ConnectorName))
		})
	}
}
