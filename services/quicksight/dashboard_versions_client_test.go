package quicksight_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	quicksightsdk "github.com/aws/aws-sdk-go-v2/service/quicksight"
	"github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListDashboardVersions_PerVersionMetadata(t *testing.T) {
	t.Parallel()

	const templateArn = "arn:aws:quicksight:us-east-1:000000000000:template/ver-src"

	tests := []struct {
		name      string
		wantDescs []string
		wantSrcs  []string
	}{
		{
			name:      "descriptions and sources recorded per version",
			wantDescs: []string{"first", "", "third"},
			wantSrcs:  []string{templateArn, "", templateArn},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newQuickSightTestClient(t)
			ctx := t.Context()

			_, err := client.CreateDashboard(ctx, &quicksightsdk.CreateDashboardInput{
				AwsAccountId:       aws.String(qsTestAccountID),
				DashboardId:        aws.String("ver-dash"),
				Name:               aws.String("ver-dash"),
				VersionDescription: aws.String("first"),
				SourceEntity: &types.DashboardSourceEntity{
					SourceTemplate: &types.DashboardSourceTemplate{
						Arn: aws.String(templateArn),
						DataSetReferences: []types.DataSetReference{{
							DataSetArn:         aws.String("arn:aws:quicksight:us-east-1:000000000000:dataset/d"),
							DataSetPlaceholder: aws.String("p"),
						}},
					},
				},
			})
			require.NoError(t, err)

			for _, upd := range []struct {
				src  *types.DashboardSourceEntity
				desc string
			}{
				{desc: ""},
				{desc: "third", src: &types.DashboardSourceEntity{
					SourceTemplate: &types.DashboardSourceTemplate{
						Arn: aws.String(templateArn),
						DataSetReferences: []types.DataSetReference{{
							DataSetArn:         aws.String("arn:aws:quicksight:us-east-1:000000000000:dataset/d"),
							DataSetPlaceholder: aws.String("p"),
						}},
					},
				}},
			} {
				_, err = client.UpdateDashboard(ctx, &quicksightsdk.UpdateDashboardInput{
					AwsAccountId:       aws.String(qsTestAccountID),
					DashboardId:        aws.String("ver-dash"),
					Name:               aws.String("ver-dash"),
					VersionDescription: aws.String(upd.desc),
					SourceEntity:       upd.src,
				})
				require.NoError(t, err)
			}

			out, err := client.ListDashboardVersions(ctx, &quicksightsdk.ListDashboardVersionsInput{
				AwsAccountId: aws.String(qsTestAccountID),
				DashboardId:  aws.String("ver-dash"),
			})
			require.NoError(t, err)
			require.Len(t, out.DashboardVersionSummaryList, len(tt.wantDescs))

			for i, v := range out.DashboardVersionSummaryList {
				assert.Equal(t, int64(i+1), aws.ToInt64(v.VersionNumber))
				assert.Equal(t, tt.wantDescs[i], aws.ToString(v.Description))
				assert.Equal(t, tt.wantSrcs[i], aws.ToString(v.SourceEntityArn))
			}
		})
	}
}
