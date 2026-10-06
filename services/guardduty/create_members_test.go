package guardduty_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	guarddutysdk "github.com/aws/aws-sdk-go-v2/service/guardduty"
	"github.com/aws/aws-sdk-go-v2/service/guardduty/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateOps_ClientTokenReplay(t *testing.T) {
	t.Parallel()

	const (
		destBase  = "arn:aws:s3:::pubdest-bucket"
		resource  = "arn:aws:ec2:us-east-1:000000000000:instance/i-0123456789abcdef0"
		otherArn  = "arn:aws:ec2:us-east-1:000000000000:instance/i-0fedcba9876543210"
		roleBase  = "arn:aws:iam::000000000000:role/"
		tokenName = "token-1"
	)

	tests := []struct {
		create func(t *testing.T, c *guarddutysdk.Client, detectorID, token, variant string) (string, error)
		name   string
	}{
		{
			name: "detector",
			create: func(t *testing.T, c *guarddutysdk.Client, _, token, variant string) (string, error) {
				t.Helper()

				out, err := c.CreateDetector(t.Context(), &guarddutysdk.CreateDetectorInput{
					Enable: aws.Bool(true), ClientToken: aws.String(token), Tags: map[string]string{"v": variant},
				})
				if err != nil {
					return "", err
				}

				return aws.ToString(out.DetectorId), nil
			},
		},
		{
			name: "filter",
			create: func(t *testing.T, c *guarddutysdk.Client, detectorID, token, variant string) (string, error) {
				t.Helper()

				out, err := c.CreateFilter(t.Context(), &guarddutysdk.CreateFilterInput{
					DetectorId: aws.String(detectorID), Name: aws.String("f-1"), ClientToken: aws.String(token),
					Action: types.FilterActionArchive, Description: aws.String(variant),
					FindingCriteria: &types.FindingCriteria{},
				})
				if err != nil {
					return "", err
				}

				return aws.ToString(out.Name), nil
			},
		},
		{
			name: "ipset",
			create: func(t *testing.T, c *guarddutysdk.Client, detectorID, token, variant string) (string, error) {
				t.Helper()

				out, err := c.CreateIPSet(t.Context(), &guarddutysdk.CreateIPSetInput{
					DetectorId: aws.String(detectorID), Name: aws.String("ip-1"), ClientToken: aws.String(token),
					Format: types.IpSetFormatTxt, Location: aws.String("s3://b/" + variant), Activate: aws.Bool(false),
				})
				if err != nil {
					return "", err
				}

				return aws.ToString(out.IpSetId), nil
			},
		},
		{
			name: "threatintelset",
			create: func(t *testing.T, c *guarddutysdk.Client, detectorID, token, variant string) (string, error) {
				t.Helper()

				out, err := c.CreateThreatIntelSet(t.Context(), &guarddutysdk.CreateThreatIntelSetInput{
					DetectorId: aws.String(detectorID), Name: aws.String("ti-1"), ClientToken: aws.String(token),
					Format: types.ThreatIntelSetFormatTxt, Location: aws.String("s3://b/" + variant),
					Activate: aws.Bool(false),
				})
				if err != nil {
					return "", err
				}

				return aws.ToString(out.ThreatIntelSetId), nil
			},
		},
		{
			name: "threatentityset",
			create: func(t *testing.T, c *guarddutysdk.Client, detectorID, token, variant string) (string, error) {
				t.Helper()

				out, err := c.CreateThreatEntitySet(t.Context(), &guarddutysdk.CreateThreatEntitySetInput{
					DetectorId: aws.String(detectorID), Name: aws.String("te-1"), ClientToken: aws.String(token),
					Format: types.ThreatEntitySetFormatTxt, Location: aws.String("s3://b/" + variant),
					Activate: aws.Bool(false),
				})
				if err != nil {
					return "", err
				}

				return aws.ToString(out.ThreatEntitySetId), nil
			},
		},
		{
			name: "trustedentityset",
			create: func(t *testing.T, c *guarddutysdk.Client, detectorID, token, variant string) (string, error) {
				t.Helper()

				out, err := c.CreateTrustedEntitySet(t.Context(), &guarddutysdk.CreateTrustedEntitySetInput{
					DetectorId: aws.String(detectorID), Name: aws.String("tr-1"), ClientToken: aws.String(token),
					Format: types.TrustedEntitySetFormatTxt, Location: aws.String("s3://b/" + variant),
					Activate: aws.Bool(false),
				})
				if err != nil {
					return "", err
				}

				return aws.ToString(out.TrustedEntitySetId), nil
			},
		},
		{
			name: "publishingdestination",
			create: func(t *testing.T, c *guarddutysdk.Client, detectorID, token, variant string) (string, error) {
				t.Helper()

				out, err := c.CreatePublishingDestination(t.Context(), &guarddutysdk.CreatePublishingDestinationInput{
					DetectorId: aws.String(detectorID), ClientToken: aws.String(token),
					DestinationType: types.DestinationTypeS3,
					DestinationProperties: &types.DestinationProperties{
						DestinationArn: aws.String(destBase + "/" + variant),
						KmsKeyArn:      aws.String("arn:aws:kms:us-east-1:000000000000:key/k"),
					},
				})
				if err != nil {
					return "", err
				}

				return aws.ToString(out.DestinationId), nil
			},
		},
		{
			name: "malwareprotectionplan",
			create: func(t *testing.T, c *guarddutysdk.Client, _, token, variant string) (string, error) {
				t.Helper()

				out, err := c.CreateMalwareProtectionPlan(t.Context(), &guarddutysdk.CreateMalwareProtectionPlanInput{
					ClientToken: aws.String(token), Role: aws.String(roleBase + variant),
					ProtectedResource: &types.CreateProtectedResource{
						S3Bucket: &types.CreateS3BucketResource{BucketName: aws.String("plan-bucket")},
					},
				})
				if err != nil {
					return "", err
				}

				return aws.ToString(out.MalwareProtectionPlanId), nil
			},
		},
		{
			name: "malwarescan",
			create: func(t *testing.T, c *guarddutysdk.Client, _, token, variant string) (string, error) {
				t.Helper()

				arn := resource
				if variant == "b" {
					arn = otherArn
				}

				out, err := c.StartMalwareScan(t.Context(), &guarddutysdk.StartMalwareScanInput{
					ClientToken: aws.String(token), ResourceArn: aws.String(arn),
				})
				if err != nil {
					return "", err
				}

				return aws.ToString(out.ScanId), nil
			},
		},
		{
			name: "investigation",
			create: func(t *testing.T, c *guarddutysdk.Client, detectorID, token, variant string) (string, error) {
				t.Helper()

				out, err := c.CreateInvestigation(t.Context(), &guarddutysdk.CreateInvestigationInput{
					DetectorId: aws.String(detectorID), ClientToken: aws.String(token),
					TriggerPrompt: aws.String("analyze " + variant),
				})
				if err != nil {
					return "", err
				}

				return aws.ToString(out.InvestigationId), nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			detectorID := createRealClientDetector(t, client)

			if tt.name == "detector" {
				_, err := client.DeleteDetector(t.Context(), &guarddutysdk.DeleteDetectorInput{
					DetectorId: aws.String(detectorID),
				})
				require.NoError(t, err)
			}

			first, err := tt.create(t, client, detectorID, tokenName, "a")
			require.NoError(t, err)
			require.NotEmpty(t, first)

			replay, err := tt.create(t, client, detectorID, tokenName, "a")
			require.NoError(t, err)
			assert.Equal(t, first, replay, "same token and parameters replays the first resource")

			_, err = tt.create(t, client, detectorID, tokenName, "b")
			require.Error(t, err, "same token with different parameters is rejected")
		})
	}
}
