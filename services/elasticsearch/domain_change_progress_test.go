package elasticsearch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticsearchsdk "github.com/aws/aws-sdk-go-v2/service/elasticsearchservice"
	"github.com/aws/aws-sdk-go-v2/service/elasticsearchservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticsearch"
)

func TestDescribeDomainChangeProgress_ChangeId_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		pick     func(ids []string) string
		name     string
		updates  int
		wantErr  bool
		wantLast bool
	}{
		{name: "omitted_returns_latest", updates: 2, pick: func([]string) string { return "" }, wantLast: true},
		{name: "latest_by_id", updates: 2, pick: func(ids []string) string { return ids[len(ids)-1] }, wantLast: true},
		{name: "earlier_by_id", updates: 2, pick: func(ids []string) string { return ids[0] }},
		{name: "unknown_id", updates: 1, pick: func([]string) string { return "no-such-change" }, wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestElasticsearchClient(
				t, elasticsearch.NewHandler(elasticsearch.NewInMemoryBackend("123456789012", rtTestRegion)),
			)
			ctx := t.Context()

			_, err := client.CreateElasticsearchDomain(ctx, &elasticsearchsdk.CreateElasticsearchDomainInput{
				DomainName: aws.String("chg-domain"),
			})
			require.NoError(t, err)

			ids := []string{}

			first, err := client.DescribeDomainChangeProgress(ctx, &elasticsearchsdk.DescribeDomainChangeProgressInput{
				DomainName: aws.String("chg-domain"),
			})
			require.NoError(t, err)
			ids = append(ids, aws.ToString(first.ChangeProgressStatus.ChangeId))

			for range tc.updates {
				_, err = client.UpdateElasticsearchDomainConfig(
					ctx,
					&elasticsearchsdk.UpdateElasticsearchDomainConfigInput{
						DomainName:     aws.String("chg-domain"),
						AccessPolicies: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
					},
				)
				require.NoError(t, err)

				step, descErr := client.DescribeDomainChangeProgress(
					ctx,
					&elasticsearchsdk.DescribeDomainChangeProgressInput{
						DomainName: aws.String("chg-domain"),
					},
				)
				require.NoError(t, descErr)
				ids = append(ids, aws.ToString(step.ChangeProgressStatus.ChangeId))
			}

			require.Len(t, ids, tc.updates+1)
			assert.NotEqual(t, ids[0], ids[len(ids)-1], "each update starts a new change")

			want := tc.pick(ids)
			out, err := client.DescribeDomainChangeProgress(ctx, &elasticsearchsdk.DescribeDomainChangeProgressInput{
				DomainName: aws.String("chg-domain"),
				ChangeId:   aws.String(want),
			})
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			if want == "" {
				want = ids[len(ids)-1]
			}

			assert.Equal(t, want, aws.ToString(out.ChangeProgressStatus.ChangeId))
			assert.Equal(t, types.OverallChangeStatusCompleted, out.ChangeProgressStatus.Status)
			assert.Equal(t, tc.wantLast, out.ChangeProgressStatus.StartTime != nil)
		})
	}
}
