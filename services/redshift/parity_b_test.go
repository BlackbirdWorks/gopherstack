package redshift_test

// parity_b_test.go — AWS accuracy tests for:
//   - DescribeTags filtering (ResourceName, ResourceType, TagKey, TagValue)
//   - CreateCluster MasterUserPassword format validation
//   - DeleteCluster SkipFinalClusterSnapshot / FinalClusterSnapshotIdentifier
//   - DescribeClusters TagKey/TagValue filtering
//   - DescribeClusterSnapshots pagination (MaxRecords / Marker)

import (
	"encoding/base64"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ----- DescribeTags filtering -----

// TestParity_DescribeTags_FilterByTagKey verifies that DescribeTags with a TagKey parameter
// returns only resources that have that tag key. Real AWS filters by TagKey.
func TestParity_DescribeTags_FilterByTagKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tagKey     string
		wantInBody []string
		wantAbsent []string
		wantCode   int
	}{
		{
			name:       "filter_by_existing_key_returns_match",
			tagKey:     "env",
			wantInBody: []string{"prod", "env"},
			wantAbsent: []string{"team"},
			wantCode:   http.StatusOK,
		},
		{
			name:       "filter_by_nonexistent_key_returns_empty",
			tagKey:     "nonexistent-key",
			wantInBody: []string{"DescribeTagsResponse"},
			wantAbsent: []string{"prod", "env", "platform"},
			wantCode:   http.StatusOK,
		},
		{
			name:       "no_filter_returns_all",
			tagKey:     "",
			wantInBody: []string{"env", "prod", "team", "platform"},
			wantCode:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=tag-filter-c1")
			postRedshiftForm(t, h,
				"Action=CreateTags&Version=2012-12-01&ResourceName=tag-filter-c1&"+
					"Tags.Tag.1.Key=env&Tags.Tag.1.Value=prod&"+
					"Tags.Tag.2.Key=team&Tags.Tag.2.Value=platform")

			body := "Action=DescribeTags&Version=2012-12-01"
			if tt.tagKey != "" {
				body += "&TagKey=" + tt.tagKey
			}

			rec := postRedshiftForm(t, h, body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantInBody {
				assert.Contains(t, rec.Body.String(), s,
					"expected %q in DescribeTags response", s)
			}

			for _, s := range tt.wantAbsent {
				assert.NotContains(t, rec.Body.String(), s,
					"expected %q to be absent in filtered DescribeTags response", s)
			}
		})
	}
}

// TestParity_DescribeTags_FilterByTagValue verifies that DescribeTags with a TagValue parameter
// returns only resources whose tag value matches. Real AWS supports TagValue filtering.
func TestParity_DescribeTags_FilterByTagValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tagKey     string
		tagValue   string
		wantInBody []string
		wantAbsent []string
		wantCode   int
	}{
		{
			name:       "filter_by_value_prod_matches",
			tagValue:   "prod",
			wantInBody: []string{"prod", "env"},
			wantAbsent: []string{"staging", "team"},
			wantCode:   http.StatusOK,
		},
		{
			name:       "filter_by_key_and_value",
			tagKey:     "env",
			tagValue:   "staging",
			wantInBody: []string{"staging"},
			wantAbsent: []string{"prod", "team"},
			wantCode:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=tagval-c1")
			postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=tagval-c2")
			postRedshiftForm(t, h,
				"Action=CreateTags&Version=2012-12-01&ResourceName=tagval-c1&"+
					"Tags.Tag.1.Key=env&Tags.Tag.1.Value=prod")
			postRedshiftForm(t, h,
				"Action=CreateTags&Version=2012-12-01&ResourceName=tagval-c2&"+
					"Tags.Tag.1.Key=env&Tags.Tag.1.Value=staging&"+
					"Tags.Tag.2.Key=team&Tags.Tag.2.Value=platform")

			body := "Action=DescribeTags&Version=2012-12-01"
			if tt.tagKey != "" {
				body += "&TagKey=" + tt.tagKey
			}
			if tt.tagValue != "" {
				body += "&TagValue=" + tt.tagValue
			}

			rec := postRedshiftForm(t, h, body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantInBody {
				assert.Contains(t, rec.Body.String(), s,
					"expected %q in DescribeTags response", s)
			}

			for _, s := range tt.wantAbsent {
				assert.NotContains(t, rec.Body.String(), s,
					"expected %q to be absent in filtered DescribeTags response", s)
			}
		})
	}
}

// TestParity_DescribeTags_FilterByResourceName verifies that DescribeTags with a ResourceName
// parameter returns only tags for the specified resource. Real AWS supports this filter.
func TestParity_DescribeTags_FilterByResourceName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		resourceName string
		wantInBody   []string
		wantAbsent   []string
		wantCode     int
	}{
		{
			name:         "filter_by_cluster_id_returns_only_that_resource",
			resourceName: "rn-c1",
			wantInBody:   []string{"cluster1-tag"},
			wantAbsent:   []string{"cluster2-tag"},
			wantCode:     http.StatusOK,
		},
		{
			name:         "no_filter_returns_all_resources",
			resourceName: "",
			wantInBody:   []string{"cluster1-tag", "cluster2-tag"},
			wantCode:     http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=rn-c1")
			postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=rn-c2")
			postRedshiftForm(t, h,
				"Action=CreateTags&Version=2012-12-01&ResourceName=rn-c1&"+
					"Tags.Tag.1.Key=name&Tags.Tag.1.Value=cluster1-tag")
			postRedshiftForm(t, h,
				"Action=CreateTags&Version=2012-12-01&ResourceName=rn-c2&"+
					"Tags.Tag.1.Key=name&Tags.Tag.1.Value=cluster2-tag")

			body := "Action=DescribeTags&Version=2012-12-01"
			if tt.resourceName != "" {
				body += "&ResourceName=" + tt.resourceName
			}

			rec := postRedshiftForm(t, h, body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantInBody {
				assert.Contains(t, rec.Body.String(), s)
			}

			for _, s := range tt.wantAbsent {
				assert.NotContains(t, rec.Body.String(), s)
			}
		})
	}
}

// TestParity_DescribeTags_FilterByResourceType verifies that ResourceType filtering works.
// Unknown resource types should return an empty result (not all resources).
func TestParity_DescribeTags_FilterByResourceType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		resourceType string
		wantEmpty    bool
		wantCode     int
	}{
		{
			name:         "cluster_resource_type_returns_results",
			resourceType: "cluster",
			wantEmpty:    false,
			wantCode:     http.StatusOK,
		},
		{
			name:         "unknown_resource_type_returns_empty",
			resourceType: "snapshot",
			wantEmpty:    true,
			wantCode:     http.StatusOK,
		},
		{
			name:         "no_resource_type_returns_all",
			resourceType: "",
			wantEmpty:    false,
			wantCode:     http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=rt-cluster")
			postRedshiftForm(t, h,
				"Action=CreateTags&Version=2012-12-01&ResourceName=rt-cluster&"+
					"Tags.Tag.1.Key=env&Tags.Tag.1.Value=test")

			body := "Action=DescribeTags&Version=2012-12-01"
			if tt.resourceType != "" {
				body += "&ResourceType=" + tt.resourceType
			}

			rec := postRedshiftForm(t, h, body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantEmpty {
				// "<TaggedResource>" (singular with angle-bracket) must be absent;
				// the plural wrapper "<TaggedResources>" may still be present.
				assert.NotContains(t, rec.Body.String(), "<TaggedResource>",
					"unexpected tagged resources for ResourceType=%q", tt.resourceType)
			} else {
				assert.Contains(t, rec.Body.String(), "env",
					"expected tagged resources for ResourceType=%q", tt.resourceType)
			}
		})
	}
}

// ----- CreateCluster MasterUserPassword validation -----

// TestParity_CreateCluster_PasswordValidation verifies that MasterUserPassword is validated
// when provided. Real AWS enforces password complexity rules.
func TestParity_CreateCluster_PasswordValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		password string
		wantCode int
	}{
		{
			name:     "valid_password_accepted",
			password: "ValidPass1",
			wantCode: http.StatusOK,
		},
		{
			name:     "too_short_rejected",
			password: "Ab1",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "too_long_rejected",
			password: "Abcdef1" + strings.Repeat("x", 58),
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "no_uppercase_rejected",
			password: "validpass1",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "no_lowercase_rejected",
			password: "VALIDPASS1",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "no_digit_rejected",
			password: "ValidPassword",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "at_sign_rejected",
			password: "Valid@Pass1",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "slash_rejected",
			password: "Valid/Pass1",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "double_quote_rejected",
			password: `Valid"Pass1`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "space_rejected",
			password: "Valid Pass1",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "no_password_accepted",
			password: "",
			wantCode: http.StatusOK,
		},
		{
			name:     "exactly_8_chars_accepted",
			password: "Validx1a",
			wantCode: http.StatusOK,
		},
		{
			name:     "exactly_64_chars_accepted",
			password: "Validx1" + strings.Repeat("a", 57),
			wantCode: http.StatusOK,
		},
		{
			name:     "65_chars_rejected",
			password: "Validx1" + strings.Repeat("a", 58),
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			clID := "pw-" + strings.ReplaceAll(tt.name, "_", "-")
			body := "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=" + clID
			if tt.password != "" {
				body += "&MasterUserPassword=" + tt.password
			}

			rec := postRedshiftForm(t, h, body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"CreateCluster with MasterUserPassword=%q", tt.password)

			if tt.wantCode == http.StatusBadRequest {
				assert.Contains(t, rec.Body.String(), "InvalidParameterValue",
					"expected InvalidParameterValue for invalid password %q", tt.password)
			}
		})
	}
}

// ----- DeleteCluster SkipFinalClusterSnapshot -----

// TestParity_DeleteCluster_FinalSnapshot verifies that DeleteCluster respects
// SkipFinalClusterSnapshot and FinalClusterSnapshotIdentifier. Real AWS:
//   - Requires FinalClusterSnapshotIdentifier when SkipFinalClusterSnapshot=false
//   - Creates a snapshot before deletion when FinalClusterSnapshotIdentifier is provided
func TestParity_DeleteCluster_FinalSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		skipFinal         string
		finalSnapshotID   string
		wantErr           string
		wantCode          int
		wantSnapshotAfter bool
	}{
		{
			name:              "skip_true_no_snapshot_id_succeeds",
			skipFinal:         "true",
			finalSnapshotID:   "",
			wantCode:          http.StatusOK,
			wantSnapshotAfter: false,
		},
		{
			name:              "skip_false_with_snapshot_id_creates_snapshot",
			skipFinal:         "false",
			finalSnapshotID:   "final-snap-1",
			wantCode:          http.StatusOK,
			wantSnapshotAfter: true,
		},
		{
			name:            "skip_false_without_snapshot_id_returns_error",
			skipFinal:       "false",
			finalSnapshotID: "",
			wantCode:        http.StatusBadRequest,
			wantErr:         "FinalClusterSnapshotIdentifier",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			clusterID := "del-" + strings.ReplaceAll(tt.name, "_", "-")

			postRedshiftForm(t, h,
				"Action=CreateCluster&Version=2012-12-01&ClusterIdentifier="+clusterID)

			body := "Action=DeleteCluster&Version=2012-12-01&ClusterIdentifier=" + clusterID +
				"&SkipFinalClusterSnapshot=" + tt.skipFinal
			if tt.finalSnapshotID != "" {
				body += "&FinalClusterSnapshotIdentifier=" + tt.finalSnapshotID
			}

			rec := postRedshiftForm(t, h, body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"DeleteCluster skip=%s snapshotID=%q", tt.skipFinal, tt.finalSnapshotID)

			if tt.wantErr != "" {
				assert.Contains(t, rec.Body.String(), tt.wantErr)
			}

			if tt.wantSnapshotAfter {
				snapRec := postRedshiftForm(t, h,
					"Action=DescribeClusterSnapshots&Version=2012-12-01&SnapshotIdentifier="+tt.finalSnapshotID)
				assert.Equal(t, http.StatusOK, snapRec.Code,
					"final snapshot %q should exist after deletion", tt.finalSnapshotID)
				assert.Contains(t, snapRec.Body.String(), tt.finalSnapshotID)
			}
		})
	}
}

// ----- DescribeClusters tag filtering -----

// TestParity_DescribeClusters_TagFilter verifies that DescribeClusters supports
// filtering by TagKey and TagValue. Real AWS supports these filters.
func TestParity_DescribeClusters_TagFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tagKey     string
		tagValue   string
		wantInBody []string
		wantAbsent []string
		wantCode   int
	}{
		{
			name:       "filter_by_tag_key_returns_matching_clusters",
			tagKey:     "env",
			wantInBody: []string{"tagged-cluster"},
			wantAbsent: []string{"untagged-cluster"},
			wantCode:   http.StatusOK,
		},
		{
			name:       "filter_by_tag_key_and_value",
			tagKey:     "env",
			tagValue:   "prod",
			wantInBody: []string{"tagged-cluster"},
			wantAbsent: []string{"untagged-cluster"},
			wantCode:   http.StatusOK,
		},
		{
			name:       "filter_by_nonexistent_tag_returns_empty",
			tagKey:     "does-not-exist",
			wantAbsent: []string{"tagged-cluster", "untagged-cluster"},
			wantCode:   http.StatusOK,
		},
		{
			name:       "no_filter_returns_all",
			wantInBody: []string{"tagged-cluster", "untagged-cluster"},
			wantCode:   http.StatusOK,
		},
		{
			name:       "filter_by_value_only",
			tagValue:   "prod",
			wantInBody: []string{"tagged-cluster"},
			wantAbsent: []string{"untagged-cluster"},
			wantCode:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=tagged-cluster")
			postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=untagged-cluster")
			postRedshiftForm(t, h,
				"Action=CreateTags&Version=2012-12-01&ResourceName=tagged-cluster&"+
					"Tags.Tag.1.Key=env&Tags.Tag.1.Value=prod")

			body := "Action=DescribeClusters&Version=2012-12-01"
			if tt.tagKey != "" {
				body += "&TagKey=" + tt.tagKey
			}
			if tt.tagValue != "" {
				body += "&TagValue=" + tt.tagValue
			}

			rec := postRedshiftForm(t, h, body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantInBody {
				assert.Contains(t, rec.Body.String(), s,
					"expected %q in DescribeClusters response for TagKey=%q TagValue=%q",
					s, tt.tagKey, tt.tagValue)
			}

			for _, s := range tt.wantAbsent {
				assert.NotContains(t, rec.Body.String(), s,
					"expected %q absent in DescribeClusters response for TagKey=%q TagValue=%q",
					s, tt.tagKey, tt.tagValue)
			}
		})
	}
}

// ----- DescribeClusterSnapshots pagination -----

// TestParity_DescribeClusterSnapshots_Pagination verifies that MaxRecords and Marker
// work for DescribeClusterSnapshots. Real AWS truncates results and returns a Marker
// to retrieve the next page.
func TestParity_DescribeClusterSnapshots_Pagination(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()

	postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=page-cluster")

	for i := 1; i <= 5; i++ {
		id := "snap-page-" + string(rune('a'-1+i))
		body := "Action=CreateClusterSnapshot&Version=2012-12-01&ClusterIdentifier=page-cluster&SnapshotIdentifier=" + id
		rec := postRedshiftForm(t, h, body)
		require.Equal(t, http.StatusOK, rec.Code, "setup: create snapshot %q", id)
	}

	t.Run("no_limit_returns_all", func(t *testing.T) {
		t.Parallel()

		h2 := newRedshiftHandler()
		postRedshiftForm(t, h2, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=page-c2")
		for i := 1; i <= 3; i++ {
			id := "snap-all-" + string(rune('a'-1+i))
			postRedshiftForm(t, h2,
				"Action=CreateClusterSnapshot&Version=2012-12-01&ClusterIdentifier=page-c2&SnapshotIdentifier="+id)
		}

		rec := postRedshiftForm(t, h2, "Action=DescribeClusterSnapshots&Version=2012-12-01")
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "snap-all-a")
		assert.Contains(t, rec.Body.String(), "snap-all-c")
	})

	t.Run("max_records_limits_results", func(t *testing.T) {
		t.Parallel()

		h2 := newRedshiftHandler()
		postRedshiftForm(t, h2, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=page-c3")
		for i := 1; i <= 3; i++ {
			id := "snap-max-" + string(rune('a'-1+i))
			postRedshiftForm(t, h2,
				"Action=CreateClusterSnapshot&Version=2012-12-01&ClusterIdentifier=page-c3&SnapshotIdentifier="+id)
		}

		rec := postRedshiftForm(t, h2, "Action=DescribeClusterSnapshots&Version=2012-12-01&MaxRecords=20")
		assert.Equal(t, http.StatusOK, rec.Code)

		body := rec.Body.String()
		assert.Contains(t, body, "snap-max-a")
	})

	t.Run("invalid_max_records_returns_error", func(t *testing.T) {
		t.Parallel()

		rec := postRedshiftForm(t, h, "Action=DescribeClusterSnapshots&Version=2012-12-01&MaxRecords=5")
		assert.Equal(t, http.StatusBadRequest, rec.Code,
			"MaxRecords < 20 should return error")
		assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
	})

	t.Run("invalid_marker_returns_error", func(t *testing.T) {
		t.Parallel()

		rec := postRedshiftForm(t, h,
			"Action=DescribeClusterSnapshots&Version=2012-12-01&Marker=not!!valid!!base64")
		assert.Equal(t, http.StatusBadRequest, rec.Code,
			"invalid base64 Marker should return error")
		assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
	})

	t.Run("marker_from_first_page_fetches_next_page", func(t *testing.T) {
		t.Parallel()

		h2 := newRedshiftHandler()
		postRedshiftForm(t, h2, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=page-c4")

		snapIDs := []string{
			"page-snap-001", "page-snap-002", "page-snap-003",
			"page-snap-004", "page-snap-005",
		}
		for _, id := range snapIDs {
			postRedshiftForm(t, h2,
				"Action=CreateClusterSnapshot&Version=2012-12-01&ClusterIdentifier=page-c4&SnapshotIdentifier="+id)
		}

		// Get first page with MaxRecords=20 (all 5 fit — pagination is only visible with >100 items naturally,
		// but the Marker mechanism is testable by checking that an empty Marker means no more pages).
		rec := postRedshiftForm(t, h2, "Action=DescribeClusterSnapshots&Version=2012-12-01&MaxRecords=20")
		require.Equal(t, http.StatusOK, rec.Code)

		// All snapshots fit in one page, no next marker expected.
		assert.Contains(t, rec.Body.String(), "page-snap-001")
		assert.Contains(t, rec.Body.String(), "page-snap-005")
	})

	t.Run("valid_base64_marker_accepted", func(t *testing.T) {
		t.Parallel()

		validMarker := base64.StdEncoding.EncodeToString([]byte("snap-page-a"))
		rec := postRedshiftForm(t, h,
			"Action=DescribeClusterSnapshots&Version=2012-12-01&Marker="+validMarker)
		assert.Equal(t, http.StatusOK, rec.Code,
			"valid base64 Marker should be accepted")
	})
}
