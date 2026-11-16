package eks_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ekssdk "github.com/aws/aws-sdk-go-v2/service/eks"
	ekstypes "github.com/aws/aws-sdk-go-v2/service/eks/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eks"
)

func TestQuotaLimiterClusterCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		limit   float64
		creates int
		known   bool
		wantErr bool
	}{
		{name: "quota allows two", limit: 2, known: true, creates: 2},
		{name: "quota rejects third", limit: 2, known: true, creates: 3, wantErr: true},
		{name: "unknown quota keeps built-in cap", limit: 1, known: false, creates: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			b.SetQuotaLimiter(func(_, _, svc, code string) (float64, bool) {
				return tt.limit, tt.known && svc == "eks" && code == "L-1194D53C"
			})

			client := newTestEKSClient(t, eks.NewHandler(b))

			var err error

			for i := range tt.creates {
				_, err = client.CreateCluster(t.Context(), &ekssdk.CreateClusterInput{
					Name:               aws.String("c" + string(rune('a'+i))),
					RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
					ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
				})
			}

			if tt.wantErr {
				var rle *ekstypes.ResourceLimitExceededException
				require.ErrorAs(t, err, &rle)
				assert.Equal(t, 2, b.ClusterTotal())

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.creates, b.ClusterTotal())
		})
	}
}
