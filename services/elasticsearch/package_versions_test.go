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

func TestPackageVersionHistory_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		commits      []string
		wantVersions []string
		maxResults   int32
		wantNext     bool
	}{
		{name: "created only", wantVersions: []string{"v1"}},
		{name: "two updates", commits: []string{"first", "second"}, wantVersions: []string{"v3", "v2", "v1"}},
		{name: "paged", commits: []string{"a", "b"}, maxResults: 2, wantVersions: []string{"v3", "v2"}, wantNext: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := elasticsearch.NewInMemoryBackend("000000000000", "us-east-1")
			client := newTestElasticsearchClient(t, elasticsearch.NewHandler(backend))
			ctx := t.Context()

			created, err := client.CreatePackage(ctx, &essdk.CreatePackageInput{
				PackageName: aws.String("pkg"),
				PackageType: types.PackageTypeTxtDictionary,
				PackageSource: &types.PackageSource{
					S3BucketName: aws.String("b"), S3Key: aws.String("k"),
				},
			})
			require.NoError(t, err)
			assert.Equal(t, "v1", aws.ToString(created.PackageDetails.AvailablePackageVersion))

			id := created.PackageDetails.PackageID

			var updated *essdk.UpdatePackageOutput

			for _, msg := range tt.commits {
				updated, err = client.UpdatePackage(ctx, &essdk.UpdatePackageInput{
					PackageID:     id,
					CommitMessage: aws.String(msg),
					PackageSource: &types.PackageSource{S3BucketName: aws.String("b"), S3Key: aws.String("k2")},
				})
				require.NoError(t, err)
			}

			if updated != nil {
				want := "v" + string(rune('1'+len(tt.commits)))
				assert.Equal(t, want, aws.ToString(updated.PackageDetails.AvailablePackageVersion))
			}

			out, err := client.GetPackageVersionHistory(ctx, &essdk.GetPackageVersionHistoryInput{
				PackageID:  id,
				MaxResults: tt.maxResults,
			})
			require.NoError(t, err)
			require.Len(t, out.PackageVersionHistoryList, len(tt.wantVersions))

			for i, want := range tt.wantVersions {
				assert.Equal(t, want, aws.ToString(out.PackageVersionHistoryList[i].PackageVersion))
				assert.NotNil(t, out.PackageVersionHistoryList[i].CreatedAt)
			}

			assert.Equal(t, tt.wantNext, out.NextToken != nil)

			if len(tt.commits) > 0 && tt.maxResults == 0 {
				assert.Equal(t, "second", aws.ToString(out.PackageVersionHistoryList[0].CommitMessage))
			}
		})
	}
}

func TestDescribeDomain_CreatedDeletedFlags_RealClient(t *testing.T) {
	t.Parallel()

	backend := elasticsearch.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestElasticsearchClient(t, elasticsearch.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateElasticsearchDomain(ctx, &essdk.CreateElasticsearchDomainInput{
		DomainName: aws.String("flags-dom"),
	})
	require.NoError(t, err)

	out, err := client.DescribeElasticsearchDomain(ctx, &essdk.DescribeElasticsearchDomainInput{
		DomainName: aws.String("flags-dom"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.DomainStatus.Created)
	require.NotNil(t, out.DomainStatus.Deleted)
	assert.True(t, *out.DomainStatus.Created)
	assert.False(t, *out.DomainStatus.Deleted)
}

func TestUpdateDomainConfig_AutoTuneRollbackOnDisable_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		want      types.RollbackOnDisable
		rollbacks []types.RollbackOnDisable
		wantErr   bool
	}{
		{
			name:      "stored",
			rollbacks: []types.RollbackOnDisable{types.RollbackOnDisableNoRollback},
			want:      types.RollbackOnDisableNoRollback,
		},
		{
			name:      "kept when omitted",
			rollbacks: []types.RollbackOnDisable{types.RollbackOnDisableDefaultRollback, ""},
			want:      types.RollbackOnDisableDefaultRollback,
		},
		{name: "invalid", rollbacks: []types.RollbackOnDisable{"BOGUS"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := elasticsearch.NewInMemoryBackend("000000000000", "us-east-1")
			client := newTestElasticsearchClient(t, elasticsearch.NewHandler(backend))
			ctx := t.Context()

			_, err := client.CreateElasticsearchDomain(ctx, &essdk.CreateElasticsearchDomainInput{
				DomainName: aws.String("rb-dom"),
			})
			require.NoError(t, err)

			for _, rb := range tt.rollbacks {
				_, err = client.UpdateElasticsearchDomainConfig(ctx, &essdk.UpdateElasticsearchDomainConfigInput{
					DomainName: aws.String("rb-dom"),
					AutoTuneOptions: &types.AutoTuneOptions{
						DesiredState:      types.AutoTuneDesiredStateDisabled,
						RollbackOnDisable: rb,
					},
				})
				if tt.wantErr {
					require.Error(t, err)

					return
				}

				require.NoError(t, err)
			}

			cfg, err := client.DescribeElasticsearchDomainConfig(ctx, &essdk.DescribeElasticsearchDomainConfigInput{
				DomainName: aws.String("rb-dom"),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.want, cfg.DomainConfig.AutoTuneOptions.Options.RollbackOnDisable)
		})
	}
}
