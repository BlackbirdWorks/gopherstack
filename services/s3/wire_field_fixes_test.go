package s3_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListObjects_OwnerPopulated is a regression test for gopherstack-6flj:
// Object.Owner (s3@v1.106.5 deserializers.go's awsRestxml_deserializeDocumentObject,
// case strings.EqualFold("Owner", ...)) is a real member shared by ListObjects
// and ListObjectsV2's Contents items, but gopherstack's shared ObjectXML struct
// had no field for it at all -- every real client's Contents[i].Owner was nil
// regardless of backend state.
//
// ListObjects (V1) has no FetchOwner request member at all (confirmed absent
// from ListObjectsInput, api_op_ListObjects.go) -- Owner is unconditionally
// present on every item. ListObjectsV2 only includes it when the request sets
// FetchOwner=true (api_op_ListObjectsV2.go's FetchOwner doc: "the owner field
// is not returned" by default) -- a near-duplicate-shape pair where the two
// ops genuinely differ, not a copy-paste mismatch.
func TestListObjects_OwnerPopulated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		list      func(t *testing.T, client *sdk_s3.Client, bucket string) *types.Owner
		name      string
		bucket    string
		wantOwner bool
	}{
		{
			name:      "v1_always_includes_owner",
			bucket:    "owner-fields-v1",
			wantOwner: true,
			list: func(t *testing.T, client *sdk_s3.Client, bucket string) *types.Owner {
				t.Helper()

				out, err := client.ListObjects(t.Context(), &sdk_s3.ListObjectsInput{
					Bucket: aws.String(bucket),
				})
				require.NoError(t, err)
				require.Len(t, out.Contents, 1)

				return out.Contents[0].Owner
			},
		},
		{
			name:      "v2_omits_owner_by_default",
			bucket:    "owner-fields-v2-default",
			wantOwner: false,
			list: func(t *testing.T, client *sdk_s3.Client, bucket string) *types.Owner {
				t.Helper()

				out, err := client.ListObjectsV2(t.Context(), &sdk_s3.ListObjectsV2Input{
					Bucket: aws.String(bucket),
				})
				require.NoError(t, err)
				require.Len(t, out.Contents, 1)

				return out.Contents[0].Owner
			},
		},
		{
			name:      "v2_includes_owner_when_fetch_owner_true",
			bucket:    "owner-fields-v2-fetch",
			wantOwner: true,
			list: func(t *testing.T, client *sdk_s3.Client, bucket string) *types.Owner {
				t.Helper()

				out, err := client.ListObjectsV2(t.Context(), &sdk_s3.ListObjectsV2Input{
					Bucket:     aws.String(bucket),
					FetchOwner: aws.Bool(true),
				})
				require.NoError(t, err)
				require.Len(t, out.Contents, 1)

				return out.Contents[0].Owner
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealS3ClientTest(t)

			_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(tt.bucket)})
			require.NoError(t, err)

			_, err = client.PutObject(t.Context(), &sdk_s3.PutObjectInput{
				Bucket: aws.String(tt.bucket),
				Key:    aws.String("owned.txt"),
				Body:   nil,
			})
			require.NoError(t, err)

			owner := tt.list(t, client, tt.bucket)

			if tt.wantOwner {
				require.NotNil(t, owner, "Contents[].Owner must be populated")
				assert.NotEmpty(t, aws.ToString(owner.ID))
			} else {
				assert.Nil(t, owner, "Contents[].Owner must be omitted without FetchOwner")
			}
		})
	}
}

// TestBucketVersioning_MfaDeleteEcho is a regression test for gopherstack-6flj:
// GetBucketVersioningOutput.MFADelete (s3@v1.106.5 deserializers.go's
// awsRestxml_deserializeOpDocumentGetBucketVersioningOutput, case
// strings.EqualFold("MfaDelete", ...)) is a real, sibling member to Status --
// PutBucketVersioning's request VersioningConfiguration.MFADelete
// (types.MFADelete) was read nowhere, stored nowhere, and GetBucketVersioning
// never echoed it, so a real client's MFADelete was always empty regardless of
// what was requested.
func TestBucketVersioning_MfaDeleteEcho(t *testing.T) {
	t.Parallel()

	client := newRealS3ClientTest(t)
	bucket := "mfa-delete-echo"

	_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	// Never configured: MFADelete must be absent (empty), not "Disabled" --
	// matches the real doc: "only returned if the bucket has been configured
	// with MFA delete."
	out, err := client.GetBucketVersioning(t.Context(), &sdk_s3.GetBucketVersioningInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)
	assert.Empty(t, out.MFADelete)

	_, err = client.PutBucketVersioning(t.Context(), &sdk_s3.PutBucketVersioningInput{
		Bucket: aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status:    types.BucketVersioningStatusEnabled,
			MFADelete: types.MFADeleteEnabled,
		},
	})
	require.NoError(t, err)

	out, err = client.GetBucketVersioning(t.Context(), &sdk_s3.GetBucketVersioningInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)
	assert.Equal(t, types.BucketVersioningStatusEnabled, out.Status)
	assert.Equal(t, types.MFADeleteStatusEnabled, out.MFADelete)
}

// TestDeleteObject_BypassGovernanceRetention is a regression test: the real
// DeleteObjectInput/DeleteObjectsInput header X-Amz-Bypass-Governance-Retention
// (s3@v1.106.5 serializers.go, awsRestxml_serializeOpHttpBindingsDeleteObjectInput
// and ...DeleteObjectsInput) was never read anywhere in gopherstack (confirmed
// by grep before this fix), so checkObjectLockForDelete blocked every delete of
// a retained object unconditionally -- indistinguishable from real AWS's
// unbypassable COMPLIANCE mode even for GOVERNANCE mode, where a caller sending
// this header (with s3:BypassGovernanceRetention permission, which this
// emulator does not model separately) must be allowed to delete. The backend
// already tracked RetentionMode (GOVERNANCE vs COMPLIANCE) -- only the
// enforcement path ignored both the mode and the header.
func TestDeleteObject_BypassGovernanceRetention(t *testing.T) {
	t.Parallel()

	tests := []struct {
		bypass      *bool
		name        string
		bucket      string
		mode        types.ObjectLockRetentionMode
		wantAllowed bool
	}{
		{
			name:        "governance_without_bypass_is_blocked",
			bucket:      "bypass-governance-without",
			mode:        types.ObjectLockRetentionModeGovernance,
			bypass:      nil,
			wantAllowed: false,
		},
		{
			name:        "governance_with_bypass_is_allowed",
			bucket:      "bypass-governance-with",
			mode:        types.ObjectLockRetentionModeGovernance,
			bypass:      aws.Bool(true),
			wantAllowed: true,
		},
		{
			name:        "compliance_with_bypass_stays_blocked",
			bucket:      "bypass-compliance-with",
			mode:        types.ObjectLockRetentionModeCompliance,
			bypass:      aws.Bool(true),
			wantAllowed: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealS3ClientTest(t)
			bucket := tt.bucket
			key := "locked.txt"

			_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{
				Bucket:                     aws.String(bucket),
				ObjectLockEnabledForBucket: aws.Bool(true),
			})
			require.NoError(t, err)

			_, err = client.PutObject(t.Context(), &sdk_s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Body:   nil,
			})
			require.NoError(t, err)

			_, err = client.PutObjectRetention(t.Context(), &sdk_s3.PutObjectRetentionInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Retention: &types.ObjectLockRetention{
					Mode:            tt.mode,
					RetainUntilDate: aws.Time(time.Now().Add(time.Hour)),
				},
			})
			require.NoError(t, err)

			_, err = client.DeleteObject(t.Context(), &sdk_s3.DeleteObjectInput{
				Bucket:                    aws.String(bucket),
				Key:                       aws.String(key),
				BypassGovernanceRetention: tt.bypass,
			})

			if tt.wantAllowed {
				require.NoError(t, err)
			} else {
				require.Error(t, err)

				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, "InvalidObjectState", apiErr.ErrorCode())
			}
		})
	}
}
