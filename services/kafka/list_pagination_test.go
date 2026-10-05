package kafka_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kafkasdk "github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

// TestList_HonoursMaxResultsAndNextToken pins the query-bound maxResults and
// nextToken members of the cluster-scoped and global list ops
// (kafka@v1.x serializers.go, e.g. ListNodes).
func TestList_HonoursMaxResultsAndNextToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		list func(t *testing.T, c *kafkasdk.Client, arn string, size *int32, token *string) (int, *string)
		name string
		want int
	}{
		{
			name: "nodes",
			want: 3,
			list: func(t *testing.T, c *kafkasdk.Client, arn string, s *int32, tok *string) (int, *string) {
				t.Helper()

				o, err := c.ListNodes(
					t.Context(),
					&kafkasdk.ListNodesInput{ClusterArn: &arn, MaxResults: s, NextToken: tok},
				)
				require.NoError(t, err)

				return len(o.NodeInfoList), o.NextToken
			},
		},
		{
			name: "kafka_versions",
			want: 3,
			list: func(t *testing.T, c *kafkasdk.Client, _ string, s *int32, tok *string) (int, *string) {
				t.Helper()

				o, err := c.ListKafkaVersions(
					t.Context(),
					&kafkasdk.ListKafkaVersionsInput{MaxResults: s, NextToken: tok},
				)
				require.NoError(t, err)

				return len(o.KafkaVersions), o.NextToken
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := kafka.NewHandler(kafka.NewInMemoryBackend("000000000000", "us-east-1"))
			client := newTestKafkaClient(t, h)
			arn := createTestCluster(t, h, "paged")

			all, _ := tt.list(t, client, arn, nil, nil)
			require.GreaterOrEqual(t, all, tt.want)

			seen, pages := 0, 0

			var token *string

			for {
				n, next := tt.list(t, client, arn, aws.Int32(2), token)
				require.LessOrEqual(t, n, 2)

				seen += n
				pages++

				if next == nil || *next == "" {
					break
				}

				token = next
			}

			require.Equal(t, all, seen)
			require.Equal(t, (all+1)/2, pages)
		})
	}
}
