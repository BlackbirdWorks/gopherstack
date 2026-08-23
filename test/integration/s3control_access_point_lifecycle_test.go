package integration_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3controlsdk "github.com/aws/aws-sdk-go-v2/service/s3control"
	"github.com/aws/aws-sdk-go-v2/service/s3control/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createS3ControlClient returns an S3 Control client pointed at the shared
// test container.
func createS3ControlClient(t *testing.T) *s3controlsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return s3controlsdk.NewFromConfig(cfg, func(o *s3controlsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_S3Control_AccessPointLifecycle drives S3 Control's access
// point lifecycle (create -> read -> policy attach/detach -> destroy) through
// the real aws-sdk-go-v2 client: CreateAccessPoint, GetAccessPoint,
// ListAccessPoints, PutAccessPointPolicy, GetAccessPointPolicy,
// GetAccessPointPolicyStatus, DeleteAccessPointPolicy, DeleteAccessPoint.
// s3control was zero of 97 ops covered by any typed client anywhere in
// test/integration, and only 7 of those (create+tag ops, no read/update/
// destroy leg) were covered even by services/s3control's own in-process
// typed tests (gopherstack-n3zi).
func TestIntegration_S3Control_AccessPointLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name            string
		accessPointName string
		accountID       string
		bucket          string
	}{
		{
			name:            "full_lifecycle",
			accessPointName: "integ-s3control-ap",
			accountID:       "123456789012",
			bucket:          "integ-s3control-bucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createS3ControlClient(t)

			createOut, err := client.CreateAccessPoint(ctx, &s3controlsdk.CreateAccessPointInput{
				AccountId: aws.String(tt.accountID),
				Name:      aws.String(tt.accessPointName),
				Bucket:    aws.String(tt.bucket),
			})
			require.NoError(t, err, "CreateAccessPoint should succeed")
			assert.NotEmpty(t, aws.ToString(createOut.AccessPointArn))
			assert.NotEmpty(t, aws.ToString(createOut.Alias),
				"CreateAccessPointOutput.Alias should be populated -- AWS's documented "+
					"alternative to the ARN for S3 data-plane calls against this access point")

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteAccessPoint(cleanupCtx, &s3controlsdk.DeleteAccessPointInput{
					AccountId: aws.String(tt.accountID),
					Name:      aws.String(tt.accessPointName),
				})
			})

			getOut, err := client.GetAccessPoint(ctx, &s3controlsdk.GetAccessPointInput{
				AccountId: aws.String(tt.accountID),
				Name:      aws.String(tt.accessPointName),
			})
			require.NoError(t, err, "GetAccessPoint should succeed")
			assert.Equal(t, tt.bucket, aws.ToString(getOut.Bucket))
			assert.Equal(t, tt.accessPointName, aws.ToString(getOut.Name))
			assert.Equal(t, types.NetworkOriginInternet, getOut.NetworkOrigin)
			assert.Equal(t, aws.ToString(createOut.AccessPointArn), aws.ToString(getOut.AccessPointArn))
			assert.Equal(t, aws.ToString(createOut.Alias), aws.ToString(getOut.Alias))

			listOut, err := client.ListAccessPoints(ctx, &s3controlsdk.ListAccessPointsInput{
				AccountId: aws.String(tt.accountID),
			})
			require.NoError(t, err, "ListAccessPoints should succeed")

			var found bool
			for _, ap := range listOut.AccessPointList {
				if aws.ToString(ap.Name) == tt.accessPointName {
					found = true
				}
			}
			assert.True(t, found, "ListAccessPoints should list the created access point")

			statusOut, err := client.GetAccessPointPolicyStatus(ctx, &s3controlsdk.GetAccessPointPolicyStatusInput{
				AccountId: aws.String(tt.accountID),
				Name:      aws.String(tt.accessPointName),
			})
			require.NoError(t, err, "GetAccessPointPolicyStatus should succeed with no policy attached yet")
			require.NotNil(t, statusOut.PolicyStatus)
			assert.False(t, statusOut.PolicyStatus.IsPublic, "no policy attached yet -- IsPublic should be false")

			policy := fmt.Sprintf(
				`{"Version":"2012-10-17","Statement":[{"Effect":"Allow",`+
					`"Principal":{"AWS":"arn:aws:iam::%s:root"},"Action":"s3:GetObject",`+
					`"Resource":"arn:aws:s3:us-east-1:%[1]s:accesspoint/%s/object/*"}]}`,
				tt.accountID, tt.accessPointName,
			)

			_, err = client.PutAccessPointPolicy(ctx, &s3controlsdk.PutAccessPointPolicyInput{
				AccountId: aws.String(tt.accountID),
				Name:      aws.String(tt.accessPointName),
				Policy:    aws.String(policy),
			})
			require.NoError(t, err, "PutAccessPointPolicy should succeed")

			policyOut, err := client.GetAccessPointPolicy(ctx, &s3controlsdk.GetAccessPointPolicyInput{
				AccountId: aws.String(tt.accountID),
				Name:      aws.String(tt.accessPointName),
			})
			require.NoError(t, err, "GetAccessPointPolicy should succeed")
			assert.JSONEq(t, policy, aws.ToString(policyOut.Policy))

			statusOut, err = client.GetAccessPointPolicyStatus(ctx, &s3controlsdk.GetAccessPointPolicyStatusInput{
				AccountId: aws.String(tt.accountID),
				Name:      aws.String(tt.accessPointName),
			})
			require.NoError(t, err, "GetAccessPointPolicyStatus should succeed once a policy is attached")
			require.NotNil(t, statusOut.PolicyStatus)
			assert.True(t, statusOut.PolicyStatus.IsPublic, "a policy is now attached")

			_, err = client.DeleteAccessPointPolicy(ctx, &s3controlsdk.DeleteAccessPointPolicyInput{
				AccountId: aws.String(tt.accountID),
				Name:      aws.String(tt.accessPointName),
			})
			require.NoError(t, err, "DeleteAccessPointPolicy should succeed")

			_, err = client.GetAccessPointPolicy(ctx, &s3controlsdk.GetAccessPointPolicyInput{
				AccountId: aws.String(tt.accountID),
				Name:      aws.String(tt.accessPointName),
			})
			require.Error(t, err, "GetAccessPointPolicy after DeleteAccessPointPolicy should fail")

			var noPolicyErr smithy.APIError
			require.ErrorAs(t, err, &noPolicyErr, "expected a real AWS API error from the SDK deserializer")
			assert.Equal(t, "NoSuchAccessPointPolicy", noPolicyErr.ErrorCode())

			_, err = client.DeleteAccessPoint(ctx, &s3controlsdk.DeleteAccessPointInput{
				AccountId: aws.String(tt.accountID),
				Name:      aws.String(tt.accessPointName),
			})
			require.NoError(t, err, "DeleteAccessPoint should succeed")

			_, err = client.GetAccessPoint(ctx, &s3controlsdk.GetAccessPointInput{
				AccountId: aws.String(tt.accountID),
				Name:      aws.String(tt.accessPointName),
			})
			require.Error(t, err, "GetAccessPoint on a deleted access point should fail")

			var deletedErr smithy.APIError
			require.ErrorAs(t, err, &deletedErr, "expected a real AWS API error from the SDK deserializer")
			assert.Equal(t, "NoSuchAccessPoint", deletedErr.ErrorCode())
		})
	}
}
