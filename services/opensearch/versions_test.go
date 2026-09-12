package opensearch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	opensearchsdk "github.com/aws/aws-sdk-go-v2/service/opensearch"
	"github.com/aws/aws-sdk-go-v2/service/opensearch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

// TestListVersions_RealClient_MatchesDocumentedTable proves ListVersions
// serves the AWS-documented version catalog (versions.go's file-level
// citation) rather than the previous invented one, which included
// "Elasticsearch_8.11" -- a version OpenSearch Service never offered (its
// Elasticsearch support tops out at 7.10, per the same doc).
func TestListVersions_RealClient_MatchesDocumentedTable(t *testing.T) {
	t.Parallel()

	h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestOpenSearchClient(t, h)

	out, err := client.ListVersions(t.Context(), &opensearchsdk.ListVersionsInput{})
	require.NoError(t, err)

	assert.Contains(t, out.Versions, "OpenSearch_2.19")
	assert.Contains(t, out.Versions, "Elasticsearch_7.10")
	assert.NotContains(t, out.Versions, "Elasticsearch_8.11", "OpenSearch Service never offered Elasticsearch 8.x")
	assert.NotContains(t, out.Versions, "OpenSearch_2.10", "2.10 is not in AWS's documented supported-version list")
}

// TestGetCompatibleVersions_RealClient_UpgradePaths proves GetCompatibleVersions'
// TargetVersions follow the real documented "Supported upgrade paths" table
// (versions.go's file-level citation), not a fabricated 4-row static list.
func TestGetCompatibleVersions_RealClient_UpgradePaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		sourceVersion  string
		wantContains   []string
		wantNotContain []string
	}{
		{
			// 2.19 is the documented required stepping stone to 3.x, so
			// unlike other 2.x versions it can go directly to 3.x too.
			name:          "opensearch_2_19_can_reach_3x_directly",
			sourceVersion: "OpenSearch_2.19",
			wantContains:  []string{"OpenSearch_3.1", "OpenSearch_3.5"},
		},
		{
			// "OpenSearch 1.3, 2.x, or 3.x -> OpenSearch 3.x ... must first
			// upgrade to 2.19": 1.3 cannot reach 3.x directly.
			name:           "opensearch_1_3_cannot_reach_3x_directly",
			sourceVersion:  "OpenSearch_1.3",
			wantContains:   []string{"OpenSearch_2.19", "OpenSearch_2.3"},
			wantNotContain: []string{"OpenSearch_3.1"},
		},
		{
			// "Elasticsearch 6.8 -> Elasticsearch 7.x or OpenSearch 1.x".
			name:          "elasticsearch_6_8_reaches_7x_and_opensearch_1x",
			sourceVersion: "Elasticsearch_6.8",
			wantContains:  []string{"Elasticsearch_7.10", "OpenSearch_1.0"},
		},
		{
			// Elasticsearch 1.5/2.3 predate the documented in-place-upgrade
			// floor (Elasticsearch 5.1) -- no compatible target at all.
			name:           "elasticsearch_1_5_has_no_upgrade_path",
			sourceVersion:  "Elasticsearch_1.5",
			wantNotContain: []string{"Elasticsearch_2.3", "Elasticsearch_5.1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
			client := newTestOpenSearchClient(t, h)
			ctx := t.Context()

			_, err := client.CreateDomain(ctx, &opensearchsdk.CreateDomainInput{
				DomainName:    aws.String("cv-dom"),
				EngineVersion: aws.String(tt.sourceVersion),
			})
			require.NoError(t, err)

			out, err := client.GetCompatibleVersions(ctx, &opensearchsdk.GetCompatibleVersionsInput{
				DomainName: aws.String("cv-dom"),
			})
			require.NoError(t, err)
			require.Len(t, out.CompatibleVersions, 1)

			entry := out.CompatibleVersions[0]
			assert.Equal(t, tt.sourceVersion, aws.ToString(entry.SourceVersion))

			for _, want := range tt.wantContains {
				assert.Contains(t, entry.TargetVersions, want)
			}

			for _, notWant := range tt.wantNotContain {
				assert.NotContains(t, entry.TargetVersions, notWant)
			}
		})
	}
}

// TestGetCompatibleVersions_RealClient_UnknownDomain proves the unknown-domain
// path still reports the real ResourceNotFoundException (unaffected by the
// version-table rewrite).
func TestGetCompatibleVersions_RealClient_UnknownDomain(t *testing.T) {
	t.Parallel()

	h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestOpenSearchClient(t, h)

	_, err := client.GetCompatibleVersions(t.Context(), &opensearchsdk.GetCompatibleVersionsInput{
		DomainName: aws.String("no-such-domain"),
	})
	require.Error(t, err)

	var rnf *types.ResourceNotFoundException
	require.ErrorAs(t, err, &rnf)
}

// TestDescribeDomainAutoTunes_RealClient_UnknownDomain proves
// DescribeDomainAutoTunes reports an unknown domain as a real
// ResourceNotFoundException rather than silently succeeding with a
// fabricated empty AutoTunes list (handler.go's dispatchDomainGetStatusRoutes
// used to swallow the backend's ErrDomainNotFound).
func TestDescribeDomainAutoTunes_RealClient_UnknownDomain(t *testing.T) {
	t.Parallel()

	h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestOpenSearchClient(t, h)

	_, err := client.DescribeDomainAutoTunes(t.Context(), &opensearchsdk.DescribeDomainAutoTunesInput{
		DomainName: aws.String("no-such-domain"),
	})
	require.Error(t, err)

	var rnf *types.ResourceNotFoundException
	require.ErrorAs(t, err, &rnf)
}

// TestDescribeDomainAutoTunes_RealClient_ValidAutoTuneType proves
// DescribeDomainAutoTunes reports the real types.AutoTuneType enum value
// ("SCHEDULED_ACTION" -- the type's only member, opensearch@v1.75.4
// types/enums.go) rather than the invented "SCHEDULED".
func TestDescribeDomainAutoTunes_RealClient_ValidAutoTuneType(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
	h := opensearch.NewHandler(b)
	client := newTestOpenSearchClient(t, h)
	ctx := t.Context()

	_, err := client.CreateDomain(ctx, &opensearchsdk.CreateDomainInput{
		DomainName: aws.String("autotune-dom"),
	})
	require.NoError(t, err)
	require.NoError(t, b.SetAutoTune("autotune-dom", "ENABLED", nil))

	out, err := client.DescribeDomainAutoTunes(ctx, &opensearchsdk.DescribeDomainAutoTunesInput{
		DomainName: aws.String("autotune-dom"),
	})
	require.NoError(t, err)
	require.Len(t, out.AutoTunes, 1)
	assert.Equal(t, types.AutoTuneTypeScheduledAction, out.AutoTunes[0].AutoTuneType)
}
