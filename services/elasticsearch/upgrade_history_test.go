package elasticsearch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	essdk "github.com/aws/aws-sdk-go-v2/service/elasticsearchservice"
	"github.com/aws/aws-sdk-go-v2/service/elasticsearchservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticsearch"
)

func TestUpgradeHistoryAndStatus_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantName     string
		targets      []string
		wantVersions []string
		wantSteps    int
		checkOnly    bool
	}{
		{name: "no upgrades", targets: nil},
		{
			name:         "one upgrade",
			targets:      []string{"7.10"},
			wantName:     "Upgrade from 7.4 to 7.10",
			wantSteps:    3,
			wantVersions: []string{"7.10"},
		},
		{
			name:         "check only",
			targets:      []string{"7.10"},
			checkOnly:    true,
			wantName:     "Upgrade eligibility check from 7.4 to 7.10",
			wantSteps:    1,
			wantVersions: []string{"7.4"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := elasticsearch.NewInMemoryBackend("000000000000", "us-east-1")
			client := newTestElasticsearchClient(t, elasticsearch.NewHandler(backend))
			ctx := t.Context()

			_, err := client.CreateElasticsearchDomain(ctx, &essdk.CreateElasticsearchDomainInput{
				DomainName:           aws.String("up-dom"),
				ElasticsearchVersion: aws.String("7.4"),
			})
			require.NoError(t, err)

			for _, target := range tt.targets {
				_, err = client.UpgradeElasticsearchDomain(ctx, &essdk.UpgradeElasticsearchDomainInput{
					DomainName:       aws.String("up-dom"),
					TargetVersion:    aws.String(target),
					PerformCheckOnly: aws.Bool(tt.checkOnly),
				})
				require.NoError(t, err)
			}

			history, err := client.GetUpgradeHistory(
				ctx,
				&essdk.GetUpgradeHistoryInput{DomainName: aws.String("up-dom")},
			)
			require.NoError(t, err)
			require.Len(t, history.UpgradeHistories, len(tt.targets))

			status, err := client.GetUpgradeStatus(ctx, &essdk.GetUpgradeStatusInput{DomainName: aws.String("up-dom")})
			require.NoError(t, err)

			if len(tt.targets) == 0 {
				assert.Nil(t, status.UpgradeName)

				return
			}

			assert.Equal(t, tt.wantName, aws.ToString(status.UpgradeName))
			assert.Equal(t, tt.wantName, aws.ToString(history.UpgradeHistories[0].UpgradeName))
			assert.Len(t, history.UpgradeHistories[0].StepsList, tt.wantSteps)
			assert.Equal(t, types.UpgradeStatusSucceeded, history.UpgradeHistories[0].UpgradeStatus)
			require.NotNil(t, history.UpgradeHistories[0].StartTimestamp)
			assert.False(t, history.UpgradeHistories[0].StartTimestamp.IsZero())

			desc, err := client.DescribeElasticsearchDomain(ctx, &essdk.DescribeElasticsearchDomainInput{
				DomainName: aws.String("up-dom"),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantVersions[0], aws.ToString(desc.DomainStatus.ElasticsearchVersion))
		})
	}
}

func TestUpgradeHistory_PaginationNewestFirst_RealClient(t *testing.T) {
	t.Parallel()

	backend := elasticsearch.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestElasticsearchClient(t, elasticsearch.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateElasticsearchDomain(ctx, &essdk.CreateElasticsearchDomainInput{
		DomainName:           aws.String("up-page"),
		ElasticsearchVersion: aws.String("7.1"),
	})
	require.NoError(t, err)

	for _, target := range []string{"7.4", "7.7", "7.10"} {
		_, err = client.UpgradeElasticsearchDomain(ctx, &essdk.UpgradeElasticsearchDomainInput{
			DomainName:    aws.String("up-page"),
			TargetVersion: aws.String(target),
		})
		require.NoError(t, err)
	}

	first, err := client.GetUpgradeHistory(ctx, &essdk.GetUpgradeHistoryInput{
		DomainName: aws.String("up-page"),
		MaxResults: 2,
	})
	require.NoError(t, err)
	require.Len(t, first.UpgradeHistories, 2)
	require.NotNil(t, first.NextToken)
	assert.Equal(t, "Upgrade from 7.7 to 7.10", aws.ToString(first.UpgradeHistories[0].UpgradeName))

	second, err := client.GetUpgradeHistory(ctx, &essdk.GetUpgradeHistoryInput{
		DomainName: aws.String("up-page"),
		MaxResults: 2,
		NextToken:  first.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, second.UpgradeHistories, 1)
	assert.Equal(t, "Upgrade from 7.1 to 7.4", aws.ToString(second.UpgradeHistories[0].UpgradeName))
	assert.Nil(t, second.NextToken)
}
