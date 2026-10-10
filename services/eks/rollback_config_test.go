package eks_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ekssdk "github.com/aws/aws-sdk-go-v2/service/eks"
	ekstypes "github.com/aws/aws-sdk-go-v2/service/eks/types"
	"github.com/stretchr/testify/require"
)

func TestUpdateClusterVersion_RollbackConfigTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		timeout int32
		wantErr bool
	}{
		{name: "min", timeout: 120},
		{name: "max", timeout: 10080},
		{name: "default-ish", timeout: 720},
		{name: "below min", timeout: 119, wantErr: true},
		{name: "above max", timeout: 10081, wantErr: true},
		{name: "zero", timeout: 0, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEKSClient(t, newRealClientHandler(t))
			createTestCluster(t, client, "rb-cluster")

			_, err := client.UpdateClusterVersion(t.Context(), &ekssdk.UpdateClusterVersionInput{
				Name:           aws.String("rb-cluster"),
				Version:        aws.String("1.32"),
				RollbackConfig: &ekstypes.RollbackConfig{TimeoutMinutes: aws.Int32(tt.timeout)},
			})

			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			var invalid *ekstypes.InvalidParameterException
			require.ErrorAs(t, err, &invalid)
		})
	}
}
