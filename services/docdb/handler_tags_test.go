package docdb_test

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/docdb"
)

func TestHandler_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "add_tags",
			vals: url.Values{
				"Action":           {"AddTagsToResource"},
				"Version":          {"2014-10-31"},
				"ResourceName":     {"arn:aws:rds:us-east-1:000000000000:cluster:my-cluster"},
				"Tags.Tag.1.Key":   {"env"},
				"Tags.Tag.1.Value": {"prod"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "AddTagsToResourceResponse",
		},
		{
			name: "list_tags",
			vals: url.Values{
				"Action":       {"ListTagsForResource"},
				"Version":      {"2014-10-31"},
				"ResourceName": {"arn:aws:rds:us-east-1:000000000000:cluster:my-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "ListTagsForResourceResponse",
		},
		{
			name: "remove_tags",
			vals: url.Values{
				"Action":           {"RemoveTagsFromResource"},
				"Version":          {"2014-10-31"},
				"ResourceName":     {"arn:aws:rds:us-east-1:000000000000:cluster:my-cluster"},
				"TagKeys.member.1": {"env"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "RemoveTagsFromResourceResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":           {"AddTagsToResource"},
				"Version":          {"2014-10-31"},
				"ResourceName":     {"arn:aws:rds:us-east-1:000000000000:cluster:my-cluster"},
				"Tags.Tag.1.Key":   {"env"},
				"Tags.Tag.1.Value": {"prod"},
			})

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestSortedListTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tags     []docdb.Tag
		wantKeys []string
	}{
		{
			name: "sorted_by_key",
			tags: []docdb.Tag{
				{Key: "z-key", Value: "v3"},
				{Key: "a-key", Value: "v1"},
				{Key: "m-key", Value: "v2"},
			},
			wantKeys: []string{"a-key", "m-key", "z-key"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(
				t,
				b.AddTagsToResource(context.Background(), "arn:aws:rds:us-east-1:000000000000:cluster:test", tt.tags),
			)

			got := b.ListTagsForResource(context.Background(), "arn:aws:rds:us-east-1:000000000000:cluster:test")

			gotKeys := make([]string, len(got))
			for i, t := range got {
				gotKeys[i] = t.Key
			}

			assert.Equal(t, tt.wantKeys, gotKeys)
		})
	}
}

func TestTagValidation_OnCreate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		tagCount     int
		keyLen       int
		valueLen     int
		wantStatus   int
	}{
		{
			name:         "valid_single_tag",
			tagCount:     1,
			keyLen:       3,
			valueLen:     4,
			wantStatus:   http.StatusOK,
			wantContains: "CreateDBClusterResponse",
		},
		{
			name:         "key_128_chars_ok",
			tagCount:     1,
			keyLen:       128,
			valueLen:     1,
			wantStatus:   http.StatusOK,
			wantContains: "CreateDBClusterResponse",
		},
		{
			name:         "key_129_chars_fails",
			tagCount:     1,
			keyLen:       129,
			valueLen:     1,
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "value_256_chars_ok",
			tagCount:     1,
			keyLen:       3,
			valueLen:     256,
			wantStatus:   http.StatusOK,
			wantContains: "CreateDBClusterResponse",
		},
		{
			name:         "value_257_chars_fails",
			tagCount:     1,
			keyLen:       3,
			valueLen:     257,
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "50_tags_ok",
			tagCount:     50,
			keyLen:       3,
			valueLen:     1,
			wantStatus:   http.StatusOK,
			wantContains: "CreateDBClusterResponse",
		},
		{
			name:         "51_tags_fails",
			tagCount:     51,
			keyLen:       3,
			valueLen:     1,
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"tag-cluster"},
				"Engine":              {"docdb"},
			}
			for i := range tt.tagCount {
				vals.Set(fmt.Sprintf("Tags.Tag.%d.Key", i+1), fmt.Sprintf("%s%d", strings.Repeat("k", tt.keyLen-1), i))
				vals.Set(fmt.Sprintf("Tags.Tag.%d.Value", i+1), strings.Repeat("v", tt.valueLen))
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestAddTagsToResource_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		tagCount     int
		keyLen       int
		valueLen     int
		wantStatus   int
	}{
		{
			name:         "valid_tag",
			tagCount:     1,
			keyLen:       3,
			valueLen:     4,
			wantStatus:   http.StatusOK,
			wantContains: "AddTagsToResourceResponse",
		},
		{
			name:         "key_too_long",
			tagCount:     1,
			keyLen:       129,
			valueLen:     1,
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "value_too_long",
			tagCount:     1,
			keyLen:       3,
			valueLen:     257,
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "51_tags_fails",
			tagCount:     51,
			keyLen:       3,
			valueLen:     1,
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"tag-cluster"},
				"Engine":              {"docdb"},
			})
			clusters, err := h.Backend.DescribeDBClusters(context.Background(), "tag-cluster")
			require.NoError(t, err)
			require.Len(t, clusters, 1)
			clusterARN := clusters[0].DBClusterArn

			vals := url.Values{
				"Action":       {"AddTagsToResource"},
				"Version":      {"2014-10-31"},
				"ResourceName": {clusterARN},
			}
			for i := range tt.tagCount {
				vals.Set(fmt.Sprintf("Tags.Tag.%d.Key", i+1), fmt.Sprintf("%s%d", strings.Repeat("k", tt.keyLen-1), i))
				vals.Set(fmt.Sprintf("Tags.Tag.%d.Value", i+1), strings.Repeat("v", tt.valueLen))
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}
