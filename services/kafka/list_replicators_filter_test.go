package kafka_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kafkasdk "github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

// TestListReplicators_NameFilterIsPrefix pins ReplicatorNameFilter, documented
// "Returns replicators starting with given name" (api_op_ListReplicators.go:40).
func TestListReplicators_NameFilterIsPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		filter *string
		want   []string
	}{
		{name: "prefix", filter: aws.String("prod-"), want: []string{"prod-a", "prod-b"}},
		{name: "no_match", filter: aws.String("zzz"), want: []string{}},
		{name: "unset", filter: nil, want: []string{"dev-a", "prod-a", "prod-b"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kafka.NewInMemoryBackend("000000000000", "us-east-1")

			for _, n := range []string{"prod-a", "prod-b", "dev-a"} {
				_, err := b.CreateReplicator(
					context.Background(), n, "", "arn:aws:iam::000000000000:role/r", nil, nil, nil, nil,
				)
				require.NoError(t, err)
			}

			client := newTestKafkaClient(t, kafka.NewHandler(b))

			out, err := client.ListReplicators(
				t.Context(),
				&kafkasdk.ListReplicatorsInput{ReplicatorNameFilter: tt.filter},
			)
			require.NoError(t, err)

			got := make([]string, 0, len(out.Replicators))
			for _, r := range out.Replicators {
				got = append(got, aws.ToString(r.ReplicatorName))
			}

			require.ElementsMatch(t, tt.want, got)
		})
	}
}
