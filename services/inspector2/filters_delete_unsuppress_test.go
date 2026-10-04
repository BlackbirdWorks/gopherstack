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

func TestDeleteSuppressFilter_ReactivatesFindings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus string
		extraRules int
	}{
		{name: "sole_rule_deleted", extraRules: 0, wantStatus: "ACTIVE"},
		{name: "other_rule_still_matches", extraRules: 1, wantStatus: "SUPPRESSED"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend, client := newRealClient(t)
			ctx := t.Context()
			criteria := &types.FilterCriteria{
				FindingType: []types.StringFilter{{
					Comparison: types.StringComparisonEquals, Value: aws.String("PACKAGE_VULNERABILITY"),
				}},
			}
			target, err := client.CreateFilter(ctx, &inspector2sdk.CreateFilterInput{
				Name: aws.String("rule"), Action: types.FilterActionSuppress, FilterCriteria: criteria,
			})
			require.NoError(t, err)
			for range tt.extraRules {
				_, err = client.CreateFilter(ctx, &inspector2sdk.CreateFilterInput{
					Name: aws.String("other"), Action: types.FilterActionSuppress, FilterCriteria: criteria,
				})
				require.NoError(t, err)
			}
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

			_, err = client.DeleteFilter(ctx, &inspector2sdk.DeleteFilterInput{Arn: target.Arn})
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, status())
		})
	}
}
