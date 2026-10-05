package cloudcontrol_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudcontrolsdk "github.com/aws/aws-sdk-go-v2/service/cloudcontrol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudcontrol"
)

// ListResourceRequests MaxResults "The default is 20" (api_op_ListResourceRequests.go:42).
func TestListResourceRequests_DefaultPageSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		maxRes   *int32
		name     string
		total    int
		wantLen  int
		wantNext bool
	}{
		{name: "default 20", total: 25, wantLen: 20, wantNext: true},
		{name: "under default", total: 5, wantLen: 5},
		{name: "explicit above default", total: 25, maxRes: aws.Int32(25), wantLen: 25},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCloudControlSDKClient(t, cloudcontrol.NewHandler(
				cloudcontrol.NewInMemoryBackend("123456789012", "us-east-1")))
			ctx := t.Context()

			for i := range tt.total {
				_, err := client.CreateResource(ctx, &cloudcontrolsdk.CreateResourceInput{
					TypeName:     aws.String("AWS::S3::Bucket"),
					DesiredState: aws.String(fmt.Sprintf(`{"BucketName":"b-%03d"}`, i)),
				})
				require.NoError(t, err)
			}

			out, err := client.ListResourceRequests(
				ctx,
				&cloudcontrolsdk.ListResourceRequestsInput{MaxResults: tt.maxRes},
			)
			require.NoError(t, err)
			assert.Len(t, out.ResourceRequestStatusSummaries, tt.wantLen)
			assert.Equal(t, tt.wantNext, out.NextToken != nil)
		})
	}
}
