package medialive_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	medialivesdk "github.com/aws/aws-sdk-go-v2/service/medialive"
	medialivetypes "github.com/aws/aws-sdk-go-v2/service/medialive/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListOfferings_RealClient_Filters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   medialivesdk.ListOfferingsInput
		wantIDs []string
	}{
		{name: "none", input: medialivesdk.ListOfferingsInput{}, wantIDs: []string{"87654321", "12345678", "11223344"}},
		{
			name:    "codec hevc",
			input:   medialivesdk.ListOfferingsInput{Codec: aws.String("HEVC")},
			wantIDs: []string{"12345678"},
		},
		{
			name:    "resource type input",
			input:   medialivesdk.ListOfferingsInput{ResourceType: aws.String("INPUT")},
			wantIDs: []string{"11223344"},
		},
		{
			name: "bitrate and framerate",
			input: medialivesdk.ListOfferingsInput{
				MaximumBitrate: aws.String("MAX_20_MBPS"), ResourceType: aws.String("OUTPUT"),
			},
			wantIDs: []string{"87654321"},
		},
		{
			name:    "duration match",
			input:   medialivesdk.ListOfferingsInput{Duration: aws.String("12")},
			wantIDs: []string{"87654321", "12345678", "11223344"},
		},
		{
			name:    "duration miss",
			input:   medialivesdk.ListOfferingsInput{Duration: aws.String("36")},
			wantIDs: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestMediaLiveClient(t, newTestHandler(t))

			out, err := client.ListOfferings(t.Context(), &tt.input)
			require.NoError(t, err)

			got := make([]string, 0, len(out.Offerings))
			for _, o := range out.Offerings {
				got = append(got, aws.ToString(o.OfferingId))
			}

			assert.ElementsMatch(t, tt.wantIDs, got)
		})
	}
}

func TestNode_RealClient_InterfaceMappings(t *testing.T) {
	t.Parallel()

	client := newTestMediaLiveClient(t, newTestHandler(t))

	cluster, err := client.CreateCluster(t.Context(), &medialivesdk.CreateClusterInput{Name: aws.String("map-cluster")})
	require.NoError(t, err)

	created, err := client.CreateNode(t.Context(), &medialivesdk.CreateNodeInput{
		ClusterId: cluster.Id,
		Name:      aws.String("map-node"),
		NodeInterfaceMappings: []medialivetypes.NodeInterfaceMappingCreateRequest{{
			LogicalInterfaceName:  aws.String("my-inputs"),
			NetworkInterfaceMode:  "NAT",
			PhysicalInterfaceName: aws.String("eth1"),
		}},
	})
	require.NoError(t, err)
	require.Len(t, created.NodeInterfaceMappings, 1)

	got, err := client.DescribeNode(
		t.Context(),
		&medialivesdk.DescribeNodeInput{ClusterId: cluster.Id, NodeId: created.Id},
	)
	require.NoError(t, err)
	require.Len(t, got.NodeInterfaceMappings, 1)
	assert.Equal(t, "my-inputs", aws.ToString(got.NodeInterfaceMappings[0].LogicalInterfaceName))
	assert.Equal(t, "eth1", aws.ToString(got.NodeInterfaceMappings[0].PhysicalInterfaceName))
	assert.EqualValues(t, "NAT", got.NodeInterfaceMappings[0].NetworkInterfaceMode)

	list, err := client.ListNodes(t.Context(), &medialivesdk.ListNodesInput{ClusterId: cluster.Id})
	require.NoError(t, err)
	require.Len(t, list.Nodes, 1)
	require.Len(t, list.Nodes[0].NodeInterfaceMappings, 1)
}

func TestUpdateNode_RealClient_SdiSourceMappings(t *testing.T) {
	t.Parallel()

	client := newTestMediaLiveClient(t, newTestHandler(t))

	cluster, err := client.CreateCluster(t.Context(), &medialivesdk.CreateClusterInput{Name: aws.String("sdi-cluster")})
	require.NoError(t, err)

	node, err := client.CreateNode(
		t.Context(),
		&medialivesdk.CreateNodeInput{ClusterId: cluster.Id, Name: aws.String("sdi-node")},
	)
	require.NoError(t, err)

	upd, err := client.UpdateNode(t.Context(), &medialivesdk.UpdateNodeInput{
		ClusterId: cluster.Id,
		NodeId:    node.Id,
		SdiSourceMappings: []medialivetypes.SdiSourceMappingUpdateRequest{
			{CardNumber: aws.Int32(1), ChannelNumber: aws.Int32(2), SdiSource: aws.String("src-1")},
		},
	})
	require.NoError(t, err)
	require.Len(t, upd.SdiSourceMappings, 1)

	got, err := client.DescribeNode(
		t.Context(),
		&medialivesdk.DescribeNodeInput{ClusterId: cluster.Id, NodeId: node.Id},
	)
	require.NoError(t, err)
	require.Len(t, got.SdiSourceMappings, 1)
	assert.Equal(t, int32(2), aws.ToInt32(got.SdiSourceMappings[0].ChannelNumber))
	assert.Equal(t, "src-1", aws.ToString(got.SdiSourceMappings[0].SdiSource))
}
