package inspector2_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	inspector2sdk "github.com/aws/aws-sdk-go-v2/service/inspector2"
	"github.com/aws/aws-sdk-go-v2/service/inspector2/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

func TestEnableDisable_StatusLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantEnable  types.Status
		wantOverall types.Status
		delay       time.Duration
		elapsed     time.Duration
		disable     bool
	}{
		{name: "instant", wantEnable: types.StatusEnabled, wantOverall: types.StatusEnabled},
		{
			name:        "enabling",
			delay:       time.Minute,
			elapsed:     time.Second,
			wantEnable:  types.StatusEnabling,
			wantOverall: types.StatusEnabling,
		},
		{
			name:        "enabled_after_delay",
			delay:       time.Minute,
			elapsed:     2 * time.Minute,
			wantEnable:  types.StatusEnabled,
			wantOverall: types.StatusEnabled,
		},
		{
			name: "disabling", delay: time.Minute, elapsed: 2 * time.Minute, disable: true,
			wantEnable: types.StatusDisabling, wantOverall: types.StatusDisabling,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := inspector2.NewInMemoryBackend("123456789012", "us-east-1")
			now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
			b.SetClock(func() time.Time { return now })
			b.SetLifecycleDelay(tt.delay)

			c := newRoundTripClient(t, inspector2.NewHandler(b))

			out, err := c.Enable(t.Context(), &inspector2sdk.EnableInput{
				ResourceTypes: []types.ResourceScanType{types.ResourceScanTypeEc2},
			})
			require.NoError(t, err)
			require.Len(t, out.Accounts, 1)

			now = now.Add(tt.elapsed)

			if tt.disable {
				dis, derr := c.Disable(t.Context(), &inspector2sdk.DisableInput{
					ResourceTypes: []types.ResourceScanType{types.ResourceScanTypeEc2},
				})
				require.NoError(t, derr)
				require.Len(t, dis.Accounts, 1)
				assert.Equal(t, tt.wantOverall, dis.Accounts[0].Status)
				assert.Equal(t, tt.wantEnable, dis.Accounts[0].ResourceStatus.Ec2)

				return
			}

			status, err := c.BatchGetAccountStatus(t.Context(), &inspector2sdk.BatchGetAccountStatusInput{})
			require.NoError(t, err)
			require.Len(t, status.Accounts, 1)
			assert.Equal(t, tt.wantOverall, status.Accounts[0].State.Status)
			assert.Equal(t, tt.wantEnable, status.Accounts[0].ResourceState.Ec2.Status)
		})
	}
}

func TestInputValidation_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run      func(t *testing.T, c *inspector2sdk.Client) error
		name     string
		wantCode string
	}{
		{
			name: "enable_bad_resource_type",
			run: func(t *testing.T, c *inspector2sdk.Client) error {
				t.Helper()

				_, err := c.Enable(t.Context(), &inspector2sdk.EnableInput{
					ResourceTypes: []types.ResourceScanType{types.ResourceScanType("BOGUS")},
				})

				return err
			},
			wantCode: "ValidationException",
		},
		{
			name: "enable_bad_account_id",
			run: func(t *testing.T, c *inspector2sdk.Client) error {
				t.Helper()

				_, err := c.Enable(t.Context(), &inspector2sdk.EnableInput{
					ResourceTypes: []types.ResourceScanType{types.ResourceScanTypeEc2}, AccountIds: []string{"abc"},
				})

				return err
			},
			wantCode: "ValidationException",
		},
		{
			name: "list_findings_over_100",
			run: func(t *testing.T, c *inspector2sdk.Client) error {
				t.Helper()

				_, err := c.ListFindings(t.Context(), &inspector2sdk.ListFindingsInput{MaxResults: aws.Int32(101)})

				return err
			},
			wantCode: "ValidationException",
		},
		{
			name: "list_coverage_150_ok",
			run: func(t *testing.T, c *inspector2sdk.Client) error {
				t.Helper()

				_, err := c.ListCoverage(t.Context(), &inspector2sdk.ListCoverageInput{MaxResults: aws.Int32(150)})

				return err
			},
		},
		{
			name: "list_coverage_over_200",
			run: func(t *testing.T, c *inspector2sdk.Client) error {
				t.Helper()

				_, err := c.ListCoverage(t.Context(), &inspector2sdk.ListCoverageInput{MaxResults: aws.Int32(201)})

				return err
			},
			wantCode: "ValidationException",
		},
		{
			name: "unknown_report",
			run: func(t *testing.T, c *inspector2sdk.Client) error {
				t.Helper()

				_, err := c.GetFindingsReportStatus(t.Context(), &inspector2sdk.GetFindingsReportStatusInput{
					ReportId: aws.String("missing"),
				})

				return err
			},
			wantCode: "ResourceNotFoundException",
		},
		{
			name: "duplicate_filter",
			run: func(t *testing.T, c *inspector2sdk.Client) error {
				t.Helper()

				in := &inspector2sdk.CreateFilterInput{
					Name: aws.String(
						"dup-filter",
					),
					Action:         types.FilterActionNone,
					FilterCriteria: &types.FilterCriteria{},
				}
				if _, err := c.CreateFilter(t.Context(), in); err != nil {
					return err
				}

				_, err := c.CreateFilter(t.Context(), in)

				return err
			},
			wantCode: "ConflictException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newDroppedMembersClient(t)

			err := tt.run(t, c)
			if tt.wantCode == "" {
				require.NoError(t, err)

				return
			}

			var apiErr smithy.APIError

			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			assert.NotEqual(t, tt.wantCode, apiErr.ErrorMessage(), "message must not be the bare code")
			assert.NotContains(t, apiErr.ErrorMessage(), tt.wantCode+":")
		})
	}
}
