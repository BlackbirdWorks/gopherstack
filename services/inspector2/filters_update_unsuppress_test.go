package inspector2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	inspector2sdk "github.com/aws/aws-sdk-go-v2/service/inspector2"
	"github.com/aws/aws-sdk-go-v2/service/inspector2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

func TestUpdateSuppressFilter_ReactivatesFindings(t *testing.T) {
	t.Parallel()

	typeFilter := func(v string) *types.FilterCriteria {
		return &types.FilterCriteria{FindingType: []types.StringFilter{{
			Comparison: types.StringComparisonEquals, Value: aws.String(v),
		}}}
	}

	tests := []struct {
		update     inspector2sdk.UpdateFilterInput
		name       string
		wantStatus string
	}{
		{
			name:       "action to none",
			update:     inspector2sdk.UpdateFilterInput{Action: types.FilterActionNone},
			wantStatus: "ACTIVE",
		},
		{
			name:       "criteria narrowed",
			update:     inspector2sdk.UpdateFilterInput{FilterCriteria: typeFilter("CODE_VULNERABILITY")},
			wantStatus: "ACTIVE",
		},
		{
			name:       "rename only",
			update:     inspector2sdk.UpdateFilterInput{Name: aws.String("renamed")},
			wantStatus: "SUPPRESSED",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend, client := newRealClient(t)
			ctx := t.Context()

			rule, err := client.CreateFilter(ctx, &inspector2sdk.CreateFilterInput{
				Name: aws.String(
					"rule",
				), Action: types.FilterActionSuppress, FilterCriteria: typeFilter("PACKAGE_VULNERABILITY"),
			})
			require.NoError(t, err)

			arn := inspector2.SeedFinding(backend, "PACKAGE_VULNERABILITY", "HIGH", "ACTIVE", "t", "d", nil)

			status := func() string {
				out, listErr := client.ListFindings(ctx, &inspector2sdk.ListFindingsInput{})
				require.NoError(t, listErr)

				for _, f := range out.Findings {
					if aws.ToString(f.FindingArn) == arn {
						return string(f.Status)
					}
				}

				return ""
			}
			require.Equal(t, "SUPPRESSED", status())

			in := tc.update
			in.FilterArn = rule.Arn
			_, err = client.UpdateFilter(ctx, &in)
			require.NoError(t, err)
			assert.Equal(t, tc.wantStatus, status())
		})
	}
}
