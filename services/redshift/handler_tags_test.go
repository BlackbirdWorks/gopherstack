package redshift_test

// handler_tags_test.go — DescribeTags/CreateTags tests:
//   - ResourceName, ResourceType, TagKey, TagValue filters
//   - CreateTags with multiple tags

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// ----- DescribeTags filtering -----

// TestDescribeTags_FilterByTagKey verifies that DescribeTags with a TagKey parameter
// returns only resources that have that tag key. Real AWS filters by TagKey.
func TestDescribeTags_FilterByTagKey(t *testing.T) {
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

// TestDescribeTags_FilterByTagValue verifies that DescribeTags with a TagValue parameter
// returns only resources whose tag value matches. Real AWS supports TagValue filtering.
func TestDescribeTags_FilterByTagValue(t *testing.T) {
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

// TestDescribeTags_FilterByResourceName verifies that DescribeTags with a ResourceName
// parameter returns only tags for the specified resource. Real AWS supports this filter.
func TestDescribeTags_FilterByResourceName(t *testing.T) {
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

// TestDescribeTags_FilterByResourceType verifies that ResourceType filtering works.
// Unknown resource types should return an empty result (not all resources).
func TestDescribeTags_FilterByResourceType(t *testing.T) {
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

// ---- parseRedshiftTags bounds ----

func TestCreateTags_MultipleTags(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	h := redshift.NewHandler(b)

	postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=mt-cluster")

	rec := postRedshiftForm(t, h,
		"Action=CreateTags&Version=2012-12-01"+
			"&ResourceName=mt-cluster"+
			"&Tags.Tag.1.Key=k1&Tags.Tag.1.Value=v1"+
			"&Tags.Tag.2.Key=k2&Tags.Tag.2.Value=v2"+
			"&Tags.Tag.3.Key=k3&Tags.Tag.3.Value=v3")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreateTagsResponse")

	// Verify tags were stored
	tags := b.DescribeTags()
	assert.Equal(t, "v1", tags["mt-cluster"]["k1"])
	assert.Equal(t, "v2", tags["mt-cluster"]["k2"])
	assert.Equal(t, "v3", tags["mt-cluster"]["k3"])
}

// ----- DescribeTags/CreateTags/DeleteTags via top-level handler dispatch -----

func TestRedshiftHandler_DescribeTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *redshift.Handler)
		name         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "empty",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeTagsResponse"},
		},
		{
			name: "with_tags",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=tag-cluster")
				postRedshiftForm(t, h, "Action=CreateTags&Version=2012-12-01&ResourceName=tag-cluster&"+
					"Tags.Tag.1.Key=env&Tags.Tag.1.Value=prod")
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeTagsResponse", "env", "prod", "tag-cluster"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newRedshiftHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rec := postRedshiftForm(t, h, "Action=DescribeTags&Version=2012-12-01")
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestRedshiftHandler_CreateTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=ct-cluster")
			},
			body: "Action=CreateTags&Version=2012-12-01&ResourceName=ct-cluster&" +
				"Tags.Tag.1.Key=env&Tags.Tag.1.Value=prod&" +
				"Tags.Tag.2.Key=team&Tags.Tag.2.Value=platform",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateTagsResponse"},
		},
		{
			name: "cluster_not_found",
			body: "Action=CreateTags&Version=2012-12-01&ResourceName=nonexistent&" +
				"Tags.Tag.1.Key=env&Tags.Tag.1.Value=prod",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newRedshiftHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestRedshiftHandler_DeleteTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=dt-cluster")
				postRedshiftForm(t, h, "Action=CreateTags&Version=2012-12-01&ResourceName=dt-cluster&"+
					"Tags.Tag.1.Key=env&Tags.Tag.1.Value=prod&Tags.Tag.2.Key=team&Tags.Tag.2.Value=platform")
			},
			body: "Action=DeleteTags&Version=2012-12-01&ResourceName=dt-cluster&" +
				"TagKeys.TagKey.1=env",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteTagsResponse"},
		},
		{
			name:     "cluster_not_found",
			body:     "Action=DeleteTags&Version=2012-12-01&ResourceName=nonexistent&TagKeys.TagKey.1=env",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newRedshiftHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}
