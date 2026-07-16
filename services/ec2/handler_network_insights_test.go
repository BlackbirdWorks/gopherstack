package ec2_test

import (
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Network Insights Path ----.
func TestNetworkInsightsPath(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var pathID string

	t.Run("create path", func(t *testing.T) { //nolint:paralleltest // existing issue.
		p, err := b.CreateNetworkInsightsPath("eni-src", "eni-dst", "tcp", 80)
		require.NoError(t, err)
		assert.NotEmpty(t, p.NetworkInsightsPathID)
		assert.NotEmpty(t, p.NetworkInsightsPathArn)
		assert.Equal(t, "eni-src", p.SourceID)
		assert.Equal(t, "tcp", p.Protocol)
		assert.Equal(t, 80, p.DestinationPort)
		pathID = p.NetworkInsightsPathID
	})

	t.Run("describe returns created path", func(t *testing.T) { //nolint:paralleltest // existing issue.
		paths := b.DescribeNetworkInsightsPaths([]string{pathID})
		require.Len(t, paths, 1)
		assert.Equal(t, "eni-dst", paths[0].DestinationID)
	})

	t.Run("describe all paths", func(t *testing.T) { //nolint:paralleltest // existing issue.
		paths := b.DescribeNetworkInsightsPaths(nil)
		assert.NotEmpty(t, paths)
	})

	t.Run("delete path", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DeleteNetworkInsightsPath(pathID))
		paths := b.DescribeNetworkInsightsPaths([]string{pathID})
		assert.Empty(t, paths)
	})

	t.Run("delete non-existent returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.DeleteNetworkInsightsPath("nip-nonexistent"))
	})

	t.Run("create with empty source returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CreateNetworkInsightsPath("", "dst", "tcp", 80)
		require.Error(t, err)
	})
}

// ---- Network Insights Analysis ----.

// ---- Network Insights Analysis ----.
func TestNetworkInsightsAnalysis(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	path, pathErr := b.CreateNetworkInsightsPath("eni-a", "eni-b", "tcp", 443)
	require.NoError(t, pathErr)
	pathID := path.NetworkInsightsPathID

	var analysisID string

	t.Run("start analysis", func(t *testing.T) { //nolint:paralleltest // existing issue.
		a, err := b.StartNetworkInsightsAnalysis(pathID)
		require.NoError(t, err)
		assert.NotEmpty(t, a.NetworkInsightsAnalysisID)
		assert.Equal(t, "succeeded", a.Status)
		assert.True(t, a.NetworkPathFound)
		assert.Equal(t, pathID, a.NetworkInsightsPathID)
		analysisID = a.NetworkInsightsAnalysisID
	})

	t.Run("describe returns analysis", func(t *testing.T) { //nolint:paralleltest // existing issue.
		analyses := b.DescribeNetworkInsightsAnalyses([]string{analysisID})
		require.Len(t, analyses, 1)
		assert.Equal(t, "succeeded", analyses[0].Status)
	})

	t.Run("describe all", func(t *testing.T) { //nolint:paralleltest // existing issue.
		analyses := b.DescribeNetworkInsightsAnalyses(nil)
		assert.NotEmpty(t, analyses)
	})

	t.Run("delete analysis", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DeleteNetworkInsightsAnalysis(analysisID))
		analyses := b.DescribeNetworkInsightsAnalyses([]string{analysisID})
		assert.Empty(t, analyses)
	})

	t.Run( //nolint:paralleltest // existing issue.
		"start analysis on non-existent path returns error",
		func(t *testing.T) {
			_, err := b.StartNetworkInsightsAnalysis("nip-nonexistent")
			require.Error(t, err)
		},
	)

	t.Run("delete non-existent analysis returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.DeleteNetworkInsightsAnalysis("nia-nonexistent"))
	})
}

// ---- Network Insights Access Scope ----.

// ---- Network Insights Access Scope ----.
func TestNetworkInsightsAccessScope(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var scopeID string

	t.Run("create scope", func(t *testing.T) { //nolint:paralleltest // existing issue.
		s, err := b.CreateNetworkInsightsAccessScope()
		require.NoError(t, err)
		assert.NotEmpty(t, s.NetworkInsightsAccessScopeID)
		assert.NotEmpty(t, s.NetworkInsightsAccessScopeArn)
		scopeID = s.NetworkInsightsAccessScopeID
	})

	t.Run("describe returns created scope", func(t *testing.T) { //nolint:paralleltest // existing issue.
		scopes := b.DescribeNetworkInsightsAccessScopes([]string{scopeID})
		require.Len(t, scopes, 1)
		assert.Equal(t, scopeID, scopes[0].NetworkInsightsAccessScopeID)
	})

	t.Run("start scope analysis", func(t *testing.T) { //nolint:paralleltest // existing issue.
		a, err := b.StartNetworkInsightsAccessScopeAnalysis(scopeID)
		require.NoError(t, err)
		assert.NotEmpty(t, a.NetworkInsightsAccessScopeAnalysisID)
		assert.Equal(t, "succeeded", a.Status)
		assert.Equal(t, scopeID, a.NetworkInsightsAccessScopeID)
	})

	t.Run("describe scope analyses", func(t *testing.T) { //nolint:paralleltest // existing issue.
		analyses := b.DescribeNetworkInsightsAccessScopeAnalyses(nil)
		assert.NotEmpty(t, analyses)
	})

	t.Run("delete scope analysis", func(t *testing.T) { //nolint:paralleltest // existing issue.
		analyses := b.DescribeNetworkInsightsAccessScopeAnalyses(nil)
		require.NotEmpty(t, analyses)
		require.NoError(t, b.DeleteNetworkInsightsAccessScopeAnalysis(analyses[0].NetworkInsightsAccessScopeAnalysisID))
	})

	t.Run("delete scope", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DeleteNetworkInsightsAccessScope(scopeID))
		scopes := b.DescribeNetworkInsightsAccessScopes([]string{scopeID})
		assert.Empty(t, scopes)
	})

	t.Run( //nolint:paralleltest // existing issue.
		"start analysis on non-existent scope returns error",
		func(t *testing.T) {
			_, err := b.StartNetworkInsightsAccessScopeAnalysis("nias-nonexistent")
			require.Error(t, err)
		},
	)

	t.Run("delete non-existent scope returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.DeleteNetworkInsightsAccessScope("nias-nonexistent"))
	})

	t.Run("delete non-existent analysis returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.DeleteNetworkInsightsAccessScopeAnalysis("niasa-nonexistent"))
	})
}

// ---- BYOIP ----.

func TestParityFinalHTTP_EnableReachabilityAnalyzerOrganizationSharing(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp, err := ec2.ExportDispatch(h, url.Values{"Action": {"EnableReachabilityAnalyzerOrganizationSharing"}})
	require.NoError(t, err)
	assert.Contains(t, resp, "<EnableReachabilityAnalyzerOrganizationSharingResponse>")
	assert.Contains(t, resp, "<returnValue>true</returnValue>")
}
