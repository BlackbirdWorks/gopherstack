package elasticache_test

import (
	"testing"

	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_PurchaseReservedCacheNode_ARNInResponse(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.PurchaseReservedCacheNodesOffering(
		t.Context(),
		&elasticachesdk.PurchaseReservedCacheNodesOfferingInput{
			ReservedCacheNodesOfferingId: aws.String("31153cd5-4ce6-45a9-b6ce-7f0b6789b8fa"),
			ReservedCacheNodeId:          aws.String("my-reserved-node"),
			CacheNodeCount:               aws.Int32(1),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, out.ReservedCacheNode)
	assert.Equal(t, "my-reserved-node", aws.ToString(out.ReservedCacheNode.ReservedCacheNodeId))
}

// ----------------------------------------
// CopySnapshot with KmsKeyId (route to copySnapshotFull handler)
// ----------------------------------------

func TestDescribeReservedCacheNodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, client *elasticachesdk.Client)
		name      string
		nodeID    string
		wantCount int
		wantErr   bool
	}{
		{
			name:      "empty",
			wantCount: 0,
		},
		{
			name:      "after_purchase",
			wantCount: 1,
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.PurchaseReservedCacheNodesOffering(
					t.Context(),
					&elasticachesdk.PurchaseReservedCacheNodesOfferingInput{
						ReservedCacheNodesOfferingId: aws.String("31153cd5-4ce6-45a9-b6ce-7f0b6789b8fa"),
						ReservedCacheNodeId:          aws.String("my-rcn"),
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			nodeID:  "no-such-rcn",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			input := &elasticachesdk.DescribeReservedCacheNodesInput{}
			if tt.nodeID != "" {
				input.ReservedCacheNodeId = aws.String(tt.nodeID)
			}

			out, err := client.DescribeReservedCacheNodes(t.Context(), input)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.ReservedCacheNodes, tt.wantCount)
		})
	}
}

// ----------------------------------------
// DescribeReservedCacheNodesOfferings
// ----------------------------------------

func TestDescribeReservedCacheNodesOfferings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		offeringID string
		wantCount  int
		wantErr    bool
	}{
		{
			name:      "all_offerings",
			wantCount: 3,
		},
		{
			name:       "specific_offering",
			offeringID: "31153cd5-4ce6-45a9-b6ce-7f0b6789b8fa",
			wantCount:  1,
		},
		{
			name:       "not_found",
			offeringID: "no-such-offering",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			input := &elasticachesdk.DescribeReservedCacheNodesOfferingsInput{}
			if tt.offeringID != "" {
				input.ReservedCacheNodesOfferingId = aws.String(tt.offeringID)
			}

			out, err := client.DescribeReservedCacheNodesOfferings(t.Context(), input)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.ReservedCacheNodesOfferings, tt.wantCount)
		})
	}
}

// ----------------------------------------
// PurchaseReservedCacheNodesOffering
// ----------------------------------------

func TestPurchaseReservedCacheNodesOffering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		offeringID string
		nodeID     string
		wantErr    bool
	}{
		{
			name:       "success",
			offeringID: "31153cd5-4ce6-45a9-b6ce-7f0b6789b8fa",
			nodeID:     "my-purchase",
		},
		{
			name:       "not_found_offering",
			offeringID: "no-such-offering",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			out, err := client.PurchaseReservedCacheNodesOffering(
				t.Context(),
				&elasticachesdk.PurchaseReservedCacheNodesOfferingInput{
					ReservedCacheNodesOfferingId: aws.String(tt.offeringID),
					ReservedCacheNodeId:          aws.String(tt.nodeID),
				},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.nodeID, aws.ToString(out.ReservedCacheNode.ReservedCacheNodeId))
		})
	}
}
