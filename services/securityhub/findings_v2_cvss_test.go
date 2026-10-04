package securityhub_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	securityhubsdk "github.com/aws/aws-sdk-go-v2/service/securityhub"
	"github.com/aws/aws-sdk-go-v2/service/securityhub/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func cvssFinding(id string, scores ...float64) types.AwsSecurityFinding {
	f := types.AwsSecurityFinding{
		AwsAccountId:  aws.String("111111111111"),
		CreatedAt:     aws.String("2024-01-01T00:00:00Z"),
		UpdatedAt:     aws.String("2024-01-01T00:00:00Z"),
		Description:   aws.String("d"),
		GeneratorId:   aws.String("g"),
		Id:            aws.String(id),
		ProductArn:    aws.String("arn:aws:securityhub:us-east-1:111111111111:product/111111111111/default"),
		SchemaVersion: aws.String("2018-10-08"),
		Title:         aws.String("t"),
		Types:         []string{"Software and Configuration Checks"},
		Severity:      &types.Severity{Label: types.SeverityLabelLow},
		Resources:     []types.Resource{{Id: aws.String("r"), Type: aws.String("Other")}},
	}

	if len(scores) > 0 {
		vuln := types.Vulnerability{Id: aws.String("CVE-2024-0001")}
		for _, s := range scores {
			vuln.Cvss = append(vuln.Cvss, types.Cvss{BaseScore: aws.Float64(s)})
		}

		f.Vulnerabilities = []types.Vulnerability{vuln}
	}

	return f
}

// TestRealClient_GetFindingsV2_CvssBaseScore checks the cvss base_score filter matches any
// Vulnerabilities[].Cvss[].BaseScore within bounds.
func TestRealClient_GetFindingsV2_CvssBaseScore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		filter  types.NumberFilter
		name    string
		wantIDs []string
	}{
		{name: "gte_any_score", filter: types.NumberFilter{Gte: aws.Float64(9)}, wantIDs: []string{"hi", "mixed"}},
		{name: "lt_any_score", filter: types.NumberFilter{Lt: aws.Float64(3)}, wantIDs: []string{"lo", "mixed"}},
		{name: "eq_exact", filter: types.NumberFilter{Eq: aws.Float64(5.5)}, wantIDs: []string{"mid"}},
		{name: "gt_none", filter: types.NumberFilter{Gt: aws.Float64(10)}, wantIDs: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newRealClientBackendAndClient(t)
			ctx := t.Context()

			_, err := client.BatchImportFindings(ctx, &securityhubsdk.BatchImportFindingsInput{
				Findings: []types.AwsSecurityFinding{
					cvssFinding("hi", 9.8),
					cvssFinding("lo", 2.0),
					cvssFinding("mid", 5.5),
					cvssFinding("mixed", 1.0, 9.0),
					cvssFinding("none"),
				},
			})
			require.NoError(t, err)

			out, err := client.GetFindingsV2(ctx, &securityhubsdk.GetFindingsV2Input{
				Filters: &types.OcsfFindingFilters{
					CompositeFilters: []types.CompositeFilter{{
						NumberFilters: []types.OcsfNumberFilter{{
							FieldName: types.OcsfNumberFieldVulnerabilitiesCveCvssBaseScore,
							Filter:    &tt.filter,
						}},
					}},
				},
			})
			require.NoError(t, err)
			assert.Len(t, out.Findings, len(tt.wantIDs))
		})
	}
}
