package guardduty_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	guarddutysdk "github.com/aws/aws-sdk-go-v2/service/guardduty"
	gdtypes "github.com/aws/aws-sdk-go-v2/service/guardduty/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/guardduty"
)

func newRealismClient(t *testing.T) (*guarddutysdk.Client, string) {
	t.Helper()

	c := newTestGuardDutyClient(t, guardduty.NewHandler(guardduty.NewInMemoryBackend("000000000000", "us-east-1")))

	out, err := c.CreateDetector(t.Context(), &guarddutysdk.CreateDetectorInput{Enable: aws.Bool(true)})
	require.NoError(t, err)

	return c, aws.ToString(out.DetectorId)
}

func TestSDK_ErrorCodesFollowDeclaredErrorSets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run      func(t *testing.T, c *guarddutysdk.Client, id string) error
		name     string
		wantCode string
		wantMsg  string
	}{
		{
			name: "unknown_detector",
			run: func(t *testing.T, c *guarddutysdk.Client, _ string) error {
				t.Helper()

				_, err := c.GetDetector(t.Context(), &guarddutysdk.GetDetectorInput{DetectorId: aws.String("nope")})

				return err
			},
			wantCode: "BadRequestException",
			wantMsg:  "The request is rejected because the input detectorId is not owned by the current account.",
		},
		{
			name: "duplicate_detector",
			run: func(t *testing.T, c *guarddutysdk.Client, _ string) error {
				t.Helper()

				_, err := c.CreateDetector(t.Context(), &guarddutysdk.CreateDetectorInput{Enable: aws.Bool(true)})

				return err
			},
			wantCode: "BadRequestException",
			wantMsg:  "The request is rejected because a detector already exists for the current account.",
		},
		{
			name: "unknown_filter",
			run: func(t *testing.T, c *guarddutysdk.Client, id string) error {
				t.Helper()

				_, err := c.GetFilter(t.Context(), &guarddutysdk.GetFilterInput{
					DetectorId: aws.String(id), FilterName: aws.String("missing"),
				})

				return err
			},
			wantCode: "BadRequestException",
		},
		{
			name: "unknown_malware_scan",
			run: func(t *testing.T, c *guarddutysdk.Client, _ string) error {
				t.Helper()

				_, err := c.GetMalwareScan(
					t.Context(),
					&guarddutysdk.GetMalwareScanInput{ScanId: aws.String("missing")},
				)

				return err
			},
			wantCode: "ResourceNotFoundException",
		},
		{
			name: "list_detectors_over_50",
			run: func(t *testing.T, c *guarddutysdk.Client, _ string) error {
				t.Helper()

				_, err := c.ListDetectors(t.Context(), &guarddutysdk.ListDetectorsInput{MaxResults: aws.Int32(51)})

				return err
			},
			wantCode: "BadRequestException",
		},
		{
			name: "list_findings_over_50",
			run: func(t *testing.T, c *guarddutysdk.Client, id string) error {
				t.Helper()

				_, err := c.ListFindings(t.Context(), &guarddutysdk.ListFindingsInput{
					DetectorId: aws.String(id), MaxResults: aws.Int32(51),
				})

				return err
			},
			wantCode: "BadRequestException",
		},
		{
			name: "filter_bad_action",
			run: func(t *testing.T, c *guarddutysdk.Client, id string) error {
				t.Helper()

				_, err := c.CreateFilter(t.Context(), &guarddutysdk.CreateFilterInput{
					DetectorId: aws.String(id), Name: aws.String("flt"), Action: gdtypes.FilterAction("DROP"),
					FindingCriteria: &gdtypes.FindingCriteria{},
				})

				return err
			},
			wantCode: "BadRequestException",
		},
		{
			name: "filter_rank_too_high",
			run: func(t *testing.T, c *guarddutysdk.Client, id string) error {
				t.Helper()

				_, err := c.CreateFilter(t.Context(), &guarddutysdk.CreateFilterInput{
					DetectorId: aws.String(id), Name: aws.String("flt"), Rank: aws.Int32(101),
					FindingCriteria: &gdtypes.FindingCriteria{},
				})

				return err
			},
			wantCode: "BadRequestException",
		},
		{
			name: "sample_findings_bad_type",
			run: func(t *testing.T, c *guarddutysdk.Client, id string) error {
				t.Helper()

				_, err := c.CreateSampleFindings(t.Context(), &guarddutysdk.CreateSampleFindingsInput{
					DetectorId: aws.String(id), FindingTypes: []string{"Bogus"},
				})

				return err
			},
			wantCode: "BadRequestException",
		},
		{
			name: "sample_findings_valid_type",
			run: func(t *testing.T, c *guarddutysdk.Client, id string) error {
				t.Helper()

				_, err := c.CreateSampleFindings(t.Context(), &guarddutysdk.CreateSampleFindingsInput{
					DetectorId: aws.String(id), FindingTypes: []string{"Recon:EC2/PortProbeUnprotectedPort"},
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c, id := newRealismClient(t)

			err := tt.run(t, c, id)
			if tt.wantCode == "" {
				require.NoError(t, err)

				return
			}

			var apiErr smithy.APIError

			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			assert.NotEqual(t, tt.wantCode, apiErr.ErrorMessage(), "message must not be the bare code")
			assert.NotContains(t, apiErr.ErrorMessage(), tt.wantCode+":")

			if tt.wantMsg != "" {
				assert.Equal(t, tt.wantMsg, apiErr.ErrorMessage())
			}
		})
	}
}

func TestSDK_CreateMembersAccountIDAndInvitedAt(t *testing.T) {
	t.Parallel()

	c, id := newRealismClient(t)

	out, err := c.CreateMembers(t.Context(), &guarddutysdk.CreateMembersInput{
		DetectorId: aws.String(id),
		AccountDetails: []gdtypes.AccountDetail{
			{AccountId: aws.String("111111111111"), Email: aws.String("a@example.com")},
			{AccountId: aws.String("not-an-id"), Email: aws.String("b@example.com")},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.UnprocessedAccounts, 1)
	assert.Equal(t, "not-an-id", aws.ToString(out.UnprocessedAccounts[0].AccountId))
	assert.Equal(t, "InvalidInput", aws.ToString(out.UnprocessedAccounts[0].Result))

	got, err := c.GetMembers(t.Context(), &guarddutysdk.GetMembersInput{
		DetectorId: aws.String(id), AccountIds: []string{"111111111111"},
	})
	require.NoError(t, err)
	require.Len(t, got.Members, 1)
	assert.Equal(t, "Created", aws.ToString(got.Members[0].RelationshipStatus))
	assert.Nil(t, got.Members[0].InvitedAt, "a member that was never invited has no InvitedAt")
}
