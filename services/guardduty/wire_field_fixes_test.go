package guardduty_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	guarddutysdk "github.com/aws/aws-sdk-go-v2/service/guardduty"
	"github.com/aws/aws-sdk-go-v2/service/guardduty/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/guardduty"
)

// TestGetFilter_TimestampsAndVersion proves GetFilter emits createdAt/
// updatedAt/version -- the backend already tracked all three (Filter.CreatedAt/
// UpdatedAt/Version) but the handler never emitted them, so a real client's
// typed fields were always nil/zero regardless of backend state
// (gopherstack-6flj).
func TestGetFilter_TimestampsAndVersion(t *testing.T) {
	t.Parallel()

	backend := guardduty.NewInMemoryBackend("123456789012", "us-east-1")
	h := guardduty.NewHandler(backend)
	client := newTestGuardDutyClient(t, h)

	det, err := client.CreateDetector(t.Context(), &guarddutysdk.CreateDetectorInput{Enable: aws.Bool(true)})
	require.NoError(t, err)
	detectorID := aws.ToString(det.DetectorId)

	_, err = client.CreateFilter(t.Context(), &guarddutysdk.CreateFilterInput{
		DetectorId: aws.String(detectorID),
		Name:       aws.String("wire-filter"),
		Action:     types.FilterActionNoop,
		FindingCriteria: &types.FindingCriteria{
			Criterion: map[string]types.Condition{"severity": {Gte: aws.Int32(4)}},
		},
	})
	require.NoError(t, err)

	first, err := client.GetFilter(t.Context(), &guarddutysdk.GetFilterInput{
		DetectorId: aws.String(detectorID),
		FilterName: aws.String("wire-filter"),
	})
	require.NoError(t, err)
	require.NotNil(t, first.CreatedAt)
	require.NotNil(t, first.UpdatedAt)
	require.NotNil(t, first.Version)
	assert.EqualValues(t, 1, aws.ToInt64(first.Version))

	_, err = client.UpdateFilter(t.Context(), &guarddutysdk.UpdateFilterInput{
		DetectorId: aws.String(detectorID),
		FilterName: aws.String("wire-filter"),
		Rank:       aws.Int32(3),
	})
	require.NoError(t, err)

	second, err := client.GetFilter(t.Context(), &guarddutysdk.GetFilterInput{
		DetectorId: aws.String(detectorID),
		FilterName: aws.String("wire-filter"),
	})
	require.NoError(t, err)
	require.NotNil(t, second.Version)
	assert.EqualValues(t, 2, aws.ToInt64(second.Version))
}

// TestIPSetAndThreatIntelSet_ExpectedBucketOwner proves ExpectedBucketOwner
// (a real member on both Create/UpdateIPSetInput and Create/
// UpdateThreatIntelSetInput, and on GetIPSetOutput/GetThreatIntelSetOutput)
// round-trips through create and update -- gopherstack's IPSet/ThreatIntelSet
// models had no field for it at all, silently dropping a value a real client
// supplied, even though the sibling ThreatEntitySet/TrustedEntitySet types in
// the same service already modeled it correctly (gopherstack-6flj).
func TestIPSetAndThreatIntelSet_ExpectedBucketOwner(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(t *testing.T, c *guarddutysdk.Client, detectorID string) string
		get    func(t *testing.T, c *guarddutysdk.Client, detectorID, id string) *string
		update func(t *testing.T, c *guarddutysdk.Client, detectorID, id string)
		name   string
	}{
		{
			name: "ip set",
			create: func(t *testing.T, c *guarddutysdk.Client, detectorID string) string {
				t.Helper()
				out, err := c.CreateIPSet(t.Context(), &guarddutysdk.CreateIPSetInput{
					DetectorId:          aws.String(detectorID),
					Name:                aws.String("wire-ipset"),
					Format:              types.IpSetFormatTxt,
					Location:            aws.String("s3://bucket/ipset.txt"),
					Activate:            aws.Bool(false),
					ExpectedBucketOwner: aws.String("111122223333"),
				})
				require.NoError(t, err)

				return aws.ToString(out.IpSetId)
			},
			get: func(t *testing.T, c *guarddutysdk.Client, detectorID, id string) *string {
				t.Helper()
				out, err := c.GetIPSet(t.Context(), &guarddutysdk.GetIPSetInput{
					DetectorId: aws.String(detectorID),
					IpSetId:    aws.String(id),
				})
				require.NoError(t, err)

				return out.ExpectedBucketOwner
			},
			update: func(t *testing.T, c *guarddutysdk.Client, detectorID, id string) {
				t.Helper()
				_, err := c.UpdateIPSet(t.Context(), &guarddutysdk.UpdateIPSetInput{
					DetectorId:          aws.String(detectorID),
					IpSetId:             aws.String(id),
					ExpectedBucketOwner: aws.String("444455556666"),
				})
				require.NoError(t, err)
			},
		},
		{
			name: "threat intel set",
			create: func(t *testing.T, c *guarddutysdk.Client, detectorID string) string {
				t.Helper()
				out, err := c.CreateThreatIntelSet(t.Context(), &guarddutysdk.CreateThreatIntelSetInput{
					DetectorId:          aws.String(detectorID),
					Name:                aws.String("wire-tiset"),
					Format:              types.ThreatIntelSetFormatTxt,
					Location:            aws.String("s3://bucket/tiset.txt"),
					Activate:            aws.Bool(false),
					ExpectedBucketOwner: aws.String("111122223333"),
				})
				require.NoError(t, err)

				return aws.ToString(out.ThreatIntelSetId)
			},
			get: func(t *testing.T, c *guarddutysdk.Client, detectorID, id string) *string {
				t.Helper()
				out, err := c.GetThreatIntelSet(t.Context(), &guarddutysdk.GetThreatIntelSetInput{
					DetectorId:       aws.String(detectorID),
					ThreatIntelSetId: aws.String(id),
				})
				require.NoError(t, err)

				return out.ExpectedBucketOwner
			},
			update: func(t *testing.T, c *guarddutysdk.Client, detectorID, id string) {
				t.Helper()
				_, err := c.UpdateThreatIntelSet(t.Context(), &guarddutysdk.UpdateThreatIntelSetInput{
					DetectorId:          aws.String(detectorID),
					ThreatIntelSetId:    aws.String(id),
					ExpectedBucketOwner: aws.String("444455556666"),
				})
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := guardduty.NewInMemoryBackend("123456789012", "us-east-1")
			h := guardduty.NewHandler(backend)
			client := newTestGuardDutyClient(t, h)

			det, err := client.CreateDetector(t.Context(), &guarddutysdk.CreateDetectorInput{Enable: aws.Bool(true)})
			require.NoError(t, err)
			detectorID := aws.ToString(det.DetectorId)

			id := tt.create(t, client, detectorID)
			assert.Equal(t, "111122223333", aws.ToString(tt.get(t, client, detectorID, id)))

			tt.update(t, client, detectorID, id)
			assert.Equal(t, "444455556666", aws.ToString(tt.get(t, client, detectorID, id)))
		})
	}
}

// TestOrganizationConfiguration_AutoEnableOrganizationMembers proves
// UpdateOrganizationConfiguration/DescribeOrganizationConfiguration round-trip
// AutoEnableOrganizationMembers, the non-deprecated replacement for AutoEnable
// that gopherstack's OrgConfig model had no slot for at all (gopherstack-6flj).
func TestOrganizationConfiguration_AutoEnableOrganizationMembers(t *testing.T) {
	t.Parallel()

	backend := guardduty.NewInMemoryBackend("123456789012", "us-east-1")
	h := guardduty.NewHandler(backend)
	client := newTestGuardDutyClient(t, h)

	det, err := client.CreateDetector(t.Context(), &guarddutysdk.CreateDetectorInput{Enable: aws.Bool(true)})
	require.NoError(t, err)
	detectorID := aws.ToString(det.DetectorId)

	_, err = client.UpdateOrganizationConfiguration(t.Context(), &guarddutysdk.UpdateOrganizationConfigurationInput{
		DetectorId:                    aws.String(detectorID),
		AutoEnableOrganizationMembers: types.AutoEnableMembersNew,
	})
	require.NoError(t, err)

	got, err := client.DescribeOrganizationConfiguration(
		t.Context(), &guarddutysdk.DescribeOrganizationConfigurationInput{DetectorId: aws.String(detectorID)},
	)
	require.NoError(t, err)
	assert.Equal(t, types.AutoEnableMembersNew, got.AutoEnableOrganizationMembers)
}
