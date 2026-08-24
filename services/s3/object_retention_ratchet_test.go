package s3_test

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPutObjectRetention_Ratchet is a regression test for gopherstack-101r-ish
// (see PARITY.md): PutObjectRetention previously overwrote RetentionMode /
// RetainUntil unconditionally, so a client could shorten or remove a
// COMPLIANCE-mode retention -- the one guarantee real S3 never breaks, for any
// principal, root included (docs.aws.amazon.com/AmazonS3/latest/userguide/
// object-lock-overview.html: "When an object is locked in compliance mode,
// its retention mode can't be changed, and its retention period can't be
// shortened"). GOVERNANCE mode allows shortening/removal/upgrade only with
// x-amz-bypass-governance-retention: true (object-lock-managing.html
// "Bypassing governance mode").
//
// Each case drives a real aws-sdk-go-v2 client through the full serialize/
// deserialize round trip so the asserted error code/status is what a real
// caller actually observes.
func TestPutObjectRetention_Ratchet(t *testing.T) {
	t.Parallel()

	now := time.Now()
	tFar := now.Add(4 * time.Hour)     // extended target
	tMid := now.Add(2 * time.Hour)     // initial retention
	tNear := now.Add(30 * time.Minute) // shortened target (still active, but earlier than tMid)

	tests := []struct {
		bypass      *bool
		setupUntil  time.Time
		newUntil    time.Time
		name        string
		setupMode   types.ObjectLockRetentionMode
		newMode     types.ObjectLockRetentionMode
		wantErrCode string
	}{
		{
			name:       "compliance_extend_allowed",
			setupMode:  types.ObjectLockRetentionModeCompliance,
			setupUntil: tMid,
			newMode:    types.ObjectLockRetentionModeCompliance,
			newUntil:   tFar,
		},
		{
			name:        "compliance_shorten_denied",
			setupMode:   types.ObjectLockRetentionModeCompliance,
			setupUntil:  tMid,
			newMode:     types.ObjectLockRetentionModeCompliance,
			newUntil:    tNear,
			wantErrCode: "AccessDenied",
		},
		{
			name:        "compliance_shorten_denied_even_with_bypass",
			setupMode:   types.ObjectLockRetentionModeCompliance,
			setupUntil:  tMid,
			newMode:     types.ObjectLockRetentionModeCompliance,
			newUntil:    tNear,
			bypass:      aws.Bool(true),
			wantErrCode: "AccessDenied",
		},
		{
			name:        "compliance_downgrade_to_governance_denied",
			setupMode:   types.ObjectLockRetentionModeCompliance,
			setupUntil:  tMid,
			newMode:     types.ObjectLockRetentionModeGovernance,
			newUntil:    tFar,
			wantErrCode: "AccessDenied",
		},
		{
			name:       "governance_extend_allowed",
			setupMode:  types.ObjectLockRetentionModeGovernance,
			setupUntil: tMid,
			newMode:    types.ObjectLockRetentionModeGovernance,
			newUntil:   tFar,
		},
		{
			name:        "governance_shorten_denied_without_bypass",
			setupMode:   types.ObjectLockRetentionModeGovernance,
			setupUntil:  tMid,
			newMode:     types.ObjectLockRetentionModeGovernance,
			newUntil:    tNear,
			wantErrCode: "AccessDenied",
		},
		{
			name:       "governance_shorten_allowed_with_bypass",
			setupMode:  types.ObjectLockRetentionModeGovernance,
			setupUntil: tMid,
			newMode:    types.ObjectLockRetentionModeGovernance,
			newUntil:   tNear,
			bypass:     aws.Bool(true),
		},
		{
			name:       "governance_upgrade_to_compliance_allowed",
			setupMode:  types.ObjectLockRetentionModeGovernance,
			setupUntil: tMid,
			newMode:    types.ObjectLockRetentionModeCompliance,
			newUntil:   tFar,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealS3ClientTest(t)
			bucket := "retention-ratchet-" + strings.ReplaceAll(tt.name, "_", "-")
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
					Mode:            tt.setupMode,
					RetainUntilDate: aws.Time(tt.setupUntil),
				},
			})
			require.NoError(t, err, "initial retention setup must succeed")

			_, err = client.PutObjectRetention(t.Context(), &sdk_s3.PutObjectRetentionInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Retention: &types.ObjectLockRetention{
					Mode:            tt.newMode,
					RetainUntilDate: aws.Time(tt.newUntil),
				},
				BypassGovernanceRetention: tt.bypass,
			})

			if tt.wantErrCode == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantErrCode, apiErr.ErrorCode())

			var respErr *smithyhttp.ResponseError
			require.ErrorAs(t, err, &respErr)
			assert.Equal(t, 403, respErr.HTTPStatusCode())
		})
	}
}
