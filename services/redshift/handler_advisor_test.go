package redshift_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListRecommendations_DerivedFromClusterConfig verifies that Advisor
// recommendations are synthesized from real cluster configuration.
func TestListRecommendations_DerivedFromClusterConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		createBody   string
		listBody     string
		wantContains []string
		wantAbsent   []string
		wantCode     int
	}{
		{
			name: "legacy unencrypted single-node yields multiple recommendations",
			createBody: "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=advisor-legacy" +
				"&NodeType=dc2.large&ClusterType=single-node",
			listBody: "Action=ListRecommendations&Version=2012-12-01&ClusterIdentifier=advisor-legacy",
			wantContains: []string{
				"ListRecommendationsResponse",
				"advisor-legacy",
				"Migrate to RA3 nodes",
				"Enable database encryption",
				"Use a multi-node cluster",
				"CostOptimization",
				"Security",
				"HIGH",
			},
			wantCode: http.StatusOK,
		},
		{
			name: "unknown cluster returns not found",
			createBody: "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=advisor-real" +
				"&NodeType=dc2.large",
			listBody: "Action=ListRecommendations&Version=2012-12-01&ClusterIdentifier=does-not-exist",
			wantContains: []string{
				"NotFound",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "account-wide list aggregates across clusters",
			createBody: "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=advisor-acct" +
				"&NodeType=dc2.large",
			listBody:     "Action=ListRecommendations&Version=2012-12-01",
			wantContains: []string{"advisor-acct", "Recommendation"},
			wantCode:     http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			postRedshiftForm(t, h, tc.createBody)

			rec := postRedshiftForm(t, h, tc.listBody)
			require.Equal(t, tc.wantCode, rec.Code, "body: %s", rec.Body.String())

			body := rec.Body.String()
			for _, want := range tc.wantContains {
				assert.Contains(t, body, want)
			}

			for _, absent := range tc.wantAbsent {
				assert.NotContains(t, body, absent)
			}
		})
	}
}

// TestListRecommendations_WellConfiguredCluster verifies a modern, encrypted,
// multi-node cluster with a maintenance window does not receive the
// security/RA3/HA/management findings that legacy clusters get.
func TestListRecommendations_WellConfiguredCluster(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	postRedshiftForm(t, h,
		"Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=modern&NodeType=ra3.4xlarge")

	// Modify into an encrypted, multi-node configuration so the security and
	// high-availability findings no longer apply.
	postRedshiftForm(t, h,
		"Action=ModifyCluster&Version=2012-12-01&ClusterIdentifier=modern"+
			"&Encrypted=true&NumberOfNodes=4&ApplyImmediately=true")

	body := postRedshiftForm(t, h,
		"Action=ListRecommendations&Version=2012-12-01&ClusterIdentifier=modern").Body.String()

	assert.Contains(t, body, "ListRecommendationsResponse")
	assert.NotContains(t, body, "Migrate to RA3 nodes")
	assert.NotContains(t, body, "Enable database encryption")
	assert.NotContains(t, body, "Use a multi-node cluster")
}

// TestListRecommendations_StableIDs verifies recommendation ids are deterministic.
func TestListRecommendations_StableIDs(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	postRedshiftForm(t, h,
		"Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=stable&NodeType=dc2.large")

	body1 := postRedshiftForm(t, h,
		"Action=ListRecommendations&Version=2012-12-01&ClusterIdentifier=stable").Body.String()
	body2 := postRedshiftForm(t, h,
		"Action=ListRecommendations&Version=2012-12-01&ClusterIdentifier=stable").Body.String()

	assert.Equal(t, body1, body2, "recommendations must be stable across reads")
	assert.Contains(t, body1, "<Id>rec-")
}

// TestDescribeNodeConfigurationOptions verifies real node configuration options.
func TestDescribeNodeConfigurationOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantAbsent   []string
		wantCode     int
	}{
		{
			name: "recommend-node-config returns multiple options",
			body: "Action=DescribeNodeConfigurationOptions&Version=2012-12-01" +
				"&ActionType=recommend-node-config",
			wantContains: []string{
				"DescribeNodeConfigurationOptionsResponse",
				"NodeConfigurationOption",
				"ra3.xlplus",
				"<NumberOfNodes>2</NumberOfNodes>",
				"<NumberOfNodes>4</NumberOfNodes>",
				"<NumberOfNodes>8</NumberOfNodes>",
			},
			wantCode: http.StatusOK,
		},
		{
			name: "resize-cluster reports elastic mode",
			body: "Action=DescribeNodeConfigurationOptions&Version=2012-12-01" +
				"&ActionType=resize-cluster",
			wantContains: []string{"<Mode>elastic</Mode>"},
			wantCode:     http.StatusOK,
		},
		{
			name: "NumberOfNodes filter narrows results",
			body: "Action=DescribeNodeConfigurationOptions&Version=2012-12-01" +
				"&ActionType=recommend-node-config&NumberOfNodes=4",
			wantContains: []string{"<NumberOfNodes>4</NumberOfNodes>"},
			wantAbsent:   []string{"<NumberOfNodes>2</NumberOfNodes>", "<NumberOfNodes>8</NumberOfNodes>"},
			wantCode:     http.StatusOK,
		},
		{
			name: "missing ActionType is rejected",
			body: "Action=DescribeNodeConfigurationOptions&Version=2012-12-01",
			wantContains: []string{
				"InvalidParameterValue",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "invalid ActionType is rejected",
			body: "Action=DescribeNodeConfigurationOptions&Version=2012-12-01&ActionType=bogus",
			wantContains: []string{
				"InvalidParameterValue",
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			rec := postRedshiftForm(t, h, tc.body)
			require.Equal(t, tc.wantCode, rec.Code, "body: %s", rec.Body.String())

			body := rec.Body.String()
			for _, want := range tc.wantContains {
				assert.Contains(t, body, want)
			}

			for _, absent := range tc.wantAbsent {
				assert.NotContains(t, body, absent)
			}
		})
	}
}

// TestDescribeNodeConfigurationOptions_UsesClusterNodeType verifies the source
// cluster's node type informs the returned options.
func TestDescribeNodeConfigurationOptions_UsesClusterNodeType(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	postRedshiftForm(t, h,
		"Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=ra3src&NodeType=ra3.4xlarge")

	rec := postRedshiftForm(t, h,
		"Action=DescribeNodeConfigurationOptions&Version=2012-12-01"+
			"&ActionType=restore-cluster&ClusterIdentifier=ra3src")
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "ra3.4xlarge")
	assert.Contains(t, body, "NodeConfigurationOption")
}
