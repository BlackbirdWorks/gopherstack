package cloudcontrol_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudcontrolsdk "github.com/aws/aws-sdk-go-v2/service/cloudcontrol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateResource_PatchKeepsUntouchedProperties(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		patch string
		want  string
	}{
		{
			name:  "replace one",
			patch: `[{"op":"replace","path":"/A","value":"x"}]`,
			want:  `{"BucketName":"b","A":"x","B":{"c":2}}`,
		},
		{
			name:  "remove and add",
			patch: `[{"op":"remove","path":"/A"},{"op":"add","path":"/B/d","value":3}]`,
			want:  `{"BucketName":"b","B":{"c":2,"d":3}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := newTestCloudControlSDKClient(t, newTestHandler(t))

			created, err := client.CreateResource(ctx, &cloudcontrolsdk.CreateResourceInput{
				TypeName:     aws.String("AWS::S3::Bucket"),
				DesiredState: aws.String(`{"BucketName":"b","A":"1","B":{"c":2}}`),
			})
			require.NoError(t, err)

			_, err = client.UpdateResource(ctx, &cloudcontrolsdk.UpdateResourceInput{
				TypeName: aws.String("AWS::S3::Bucket"), Identifier: created.ProgressEvent.Identifier,
				PatchDocument: aws.String(tt.patch),
			})
			require.NoError(t, err)

			got, err := client.GetResource(ctx, &cloudcontrolsdk.GetResourceInput{
				TypeName: aws.String("AWS::S3::Bucket"), Identifier: created.ProgressEvent.Identifier,
			})
			require.NoError(t, err)
			assert.JSONEq(t, tt.want, aws.ToString(got.ResourceDescription.Properties))
		})
	}
}
