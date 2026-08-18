package elb_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elb"
)

// TestTagOperations tests AddTags, DescribeTags, RemoveTags.
func TestTagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops        func(t *testing.T, h *elb.Handler)
		name       string
		wantStatus int
	}{
		{
			name: "add_and_describe_tags",
			ops: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "tag-lb")

				rec := doELB(t, h, url.Values{
					"Action":                     {"AddTags"},
					"Version":                    {"2012-06-01"},
					"LoadBalancerNames.member.1": {"tag-lb"},
					"Tags.member.1.Key":          {"Env"},
					"Tags.member.1.Value":        {"prod"},
					"Tags.member.2.Key":          {"Team"},
					"Tags.member.2.Value":        {"platform"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec2 := doELB(t, h, url.Values{
					"Action":                     {"DescribeTags"},
					"Version":                    {"2012-06-01"},
					"LoadBalancerNames.member.1": {"tag-lb"},
				})
				assert.Equal(t, http.StatusOK, rec2.Code)

				var resp struct {
					XMLName xml.Name `xml:"DescribeTagsResponse"`
					Result  struct {
						TagDescriptions struct {
							Members []struct {
								Name string `xml:"LoadBalancerName"`
								Tags struct {
									Members []struct {
										Key   string `xml:"Key"`
										Value string `xml:"Value"`
									} `xml:"member"`
								} `xml:"Tags"`
							} `xml:"member"`
						} `xml:"TagDescriptions"`
					} `xml:"DescribeTagsResult"`
				}
				require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &resp))
				require.Len(t, resp.Result.TagDescriptions.Members, 1)
				assert.Equal(t, "tag-lb", resp.Result.TagDescriptions.Members[0].Name)
				assert.Len(t, resp.Result.TagDescriptions.Members[0].Tags.Members, 2)
			},
		},
		{
			name: "remove_tags",
			ops: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "rmtag-lb")

				doELB(t, h, url.Values{
					"Action":                     {"AddTags"},
					"Version":                    {"2012-06-01"},
					"LoadBalancerNames.member.1": {"rmtag-lb"},
					"Tags.member.1.Key":          {"Env"},
					"Tags.member.1.Value":        {"prod"},
					"Tags.member.2.Key":          {"Extra"},
					"Tags.member.2.Value":        {"remove-me"},
				})

				rec := doELB(t, h, url.Values{
					"Action":                     {"RemoveTags"},
					"Version":                    {"2012-06-01"},
					"LoadBalancerNames.member.1": {"rmtag-lb"},
					"Tags.member.1.Key":          {"Extra"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec2 := doELB(t, h, url.Values{
					"Action":                     {"DescribeTags"},
					"Version":                    {"2012-06-01"},
					"LoadBalancerNames.member.1": {"rmtag-lb"},
				})
				var resp struct {
					XMLName xml.Name `xml:"DescribeTagsResponse"`
					Result  struct {
						TagDescriptions struct {
							Members []struct {
								Tags struct {
									Members []struct {
										Key string `xml:"Key"`
									} `xml:"member"`
								} `xml:"Tags"`
							} `xml:"member"`
						} `xml:"TagDescriptions"`
					} `xml:"DescribeTagsResult"`
				}
				require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &resp))
				assert.Len(t, resp.Result.TagDescriptions.Members[0].Tags.Members, 1)
				assert.Equal(t, "Env", resp.Result.TagDescriptions.Members[0].Tags.Members[0].Key)
			},
		},
		{
			name: "add_tags_lb_not_found",
			ops: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				rec := doELB(t, h, url.Values{
					"Action":                     {"AddTags"},
					"Version":                    {"2012-06-01"},
					"LoadBalancerNames.member.1": {"no-lb"},
					"Tags.member.1.Key":          {"k"},
					"Tags.member.1.Value":        {"v"},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "describe_tags_lb_not_found",
			ops: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				rec := doELB(t, h, url.Values{
					"Action":                     {"DescribeTags"},
					"Version":                    {"2012-06-01"},
					"LoadBalancerNames.member.1": {"no-lb"},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "remove_tags_lb_not_found",
			ops: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				rec := doELB(t, h, url.Values{
					"Action":                     {"RemoveTags"},
					"Version":                    {"2012-06-01"},
					"LoadBalancerNames.member.1": {"no-lb"},
					"Tags.member.1.Key":          {"k"},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "add_tags_missing_lb_name",
			ops: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				rec := doELB(t, h, url.Values{
					"Action":  {"AddTags"},
					"Version": {"2012-06-01"},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "describe_tags_missing_lb_name",
			ops: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				rec := doELB(t, h, url.Values{
					"Action":  {"DescribeTags"},
					"Version": {"2012-06-01"},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "remove_tags_missing_lb_name",
			ops: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				rec := doELB(t, h, url.Values{
					"Action":  {"RemoveTags"},
					"Version": {"2012-06-01"},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			tt.ops(t, h)
		})
	}
}

func TestTagsMultiLBAddAndDescribe(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "tags-a-lb")
	mustCreateLB(t, h, "tags-b-lb")

	rec := doELB(t, h, url.Values{
		"Action":                     {"AddTags"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"tags-a-lb"},
		"LoadBalancerNames.member.2": {"tags-b-lb"},
		"Tags.member.1.Key":          {"Env"},
		"Tags.member.1.Value":        {"prod"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doELB(t, h, url.Values{
		"Action":                     {"DescribeTags"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"tags-a-lb"},
		"LoadBalancerNames.member.2": {"tags-b-lb"},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeTagsResponse"`
		Result  struct {
			TagDescriptions struct {
				Members []struct {
					LoadBalancerName string `xml:"LoadBalancerName"`
					Tags             struct {
						Members []struct {
							Key   string `xml:"Key"`
							Value string `xml:"Value"`
						} `xml:"member"`
					} `xml:"Tags"`
				} `xml:"member"`
			} `xml:"TagDescriptions"`
		} `xml:"DescribeTagsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TagDescriptions.Members, 2)

	for _, td := range resp.Result.TagDescriptions.Members {
		require.Len(t, td.Tags.Members, 1)
		assert.Equal(t, "Env", td.Tags.Members[0].Key)
	}
}

func TestTagsMaxLimit(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "tags-max-lb")

	// Add 10 tags (the max).
	rec := doELB(t, h, url.Values{
		"Action":                     {"AddTags"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"tags-max-lb"},
		"Tags.member.1.Key":          {"k1"}, "Tags.member.1.Value": {"v1"},
		"Tags.member.2.Key": {"k2"}, "Tags.member.2.Value": {"v2"},
		"Tags.member.3.Key": {"k3"}, "Tags.member.3.Value": {"v3"},
		"Tags.member.4.Key": {"k4"}, "Tags.member.4.Value": {"v4"},
		"Tags.member.5.Key": {"k5"}, "Tags.member.5.Value": {"v5"},
		"Tags.member.6.Key": {"k6"}, "Tags.member.6.Value": {"v6"},
		"Tags.member.7.Key": {"k7"}, "Tags.member.7.Value": {"v7"},
		"Tags.member.8.Key": {"k8"}, "Tags.member.8.Value": {"v8"},
		"Tags.member.9.Key": {"k9"}, "Tags.member.9.Value": {"v9"},
		"Tags.member.10.Key": {"k10"}, "Tags.member.10.Value": {"v10"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Adding one more should fail.
	rec2 := doELB(t, h, url.Values{
		"Action":                     {"AddTags"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"tags-max-lb"},
		"Tags.member.1.Key":          {"k11"},
		"Tags.member.1.Value":        {"v11"},
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestTagsRemove(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "tags-rm-lb")

	doELB(t, h, url.Values{
		"Action":                     {"AddTags"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"tags-rm-lb"},
		"Tags.member.1.Key":          {"k1"}, "Tags.member.1.Value": {"v1"},
		"Tags.member.2.Key": {"k2"}, "Tags.member.2.Value": {"v2"},
	})

	doELB(t, h, url.Values{
		"Action":                     {"RemoveTags"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"tags-rm-lb"},
		"Tags.member.1.Key":          {"k1"},
	})

	rec := doELB(t, h, url.Values{
		"Action":                     {"DescribeTags"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"tags-rm-lb"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeTagsResponse"`
		Result  struct {
			TagDescriptions struct {
				Members []struct {
					Tags struct {
						Members []struct {
							Key string `xml:"Key"`
						} `xml:"member"`
					} `xml:"Tags"`
				} `xml:"member"`
			} `xml:"TagDescriptions"`
		} `xml:"DescribeTagsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TagDescriptions.Members, 1)
	tags := resp.Result.TagDescriptions.Members[0].Tags.Members
	require.Len(t, tags, 1)
	assert.Equal(t, "k2", tags[0].Key)
}

// TestRemoveTagsEmptyList verifies that RemoveTags with no tag keys
// returns a ValidationError.
func TestRemoveTagsEmptyList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals       url.Values
		name       string
		wantStatus int
	}{
		{
			name: "no_tags_rejected",
			vals: url.Values{
				"Action":                     {"RemoveTags"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerNames.member.1": {"rmtags-empty-lb"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "with_tags_accepted",
			vals: url.Values{
				"Action":                     {"RemoveTags"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerNames.member.1": {"rmtags-nonempty-lb"},
				"Tags.member.1.Key":          {"mykey"},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			mustCreateLB(t, h, "rmtags-empty-lb")
			mustCreateLB(t, h, "rmtags-nonempty-lb")
			rec := doELB(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestAddTagsReservedPrefixes verifies that tag keys with reserved
// prefixes (aws:, amazon:, elasticloadbalancing:) are rejected.
func TestAddTagsReservedPrefixes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tagKey     string
		name       string
		wantStatus int
	}{
		{"my-tag", "plain_key_accepted", http.StatusOK},
		{"aws:reserved", "aws_prefix_rejected", http.StatusBadRequest},
		{"AWS:Reserved", "aws_prefix_case_insensitive_rejected", http.StatusBadRequest},
		{"amazon:reserved", "amazon_prefix_rejected", http.StatusBadRequest},
		{"Amazon:reserved", "amazon_prefix_case_insensitive_rejected", http.StatusBadRequest},
		{"elasticloadbalancing:reserved", "elb_prefix_rejected", http.StatusBadRequest},
		{"ElasticLoadBalancing:reserved", "elb_prefix_case_insensitive_rejected", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			mustCreateLB(t, h, "addtag-prefix-lb")

			rec := doELB(t, h, url.Values{
				"Action":                     {"AddTags"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerNames.member.1": {"addtag-prefix-lb"},
				"Tags.member.1.Key":          {tt.tagKey},
				"Tags.member.1.Value":        {"value"},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestAddTagsRejectsAwsPrefix verifies that tags with 'aws:' prefix are rejected.
func TestAddTagsRejectsAwsPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tagKey     string
		wantStatus int
	}{
		{
			name:       "aws_prefix_rejected",
			tagKey:     "aws:stack",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "regular_key_accepted",
			tagKey:     "Environment",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			h := elb.NewHandler(b)
			mustCreateLB(t, h, "tag-prefix-lb")

			rec := doELB(t, h, url.Values{
				"Action":                     {"AddTags"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerNames.member.1": {"tag-prefix-lb"},
				"Tags.member.1.Key":          {tt.tagKey},
				"Tags.member.1.Value":        {"val"},
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRemoveTagsRequiresAtLeastOneName verifies RemoveTags needs at least one LB name.
func TestRemoveTagsRequiresAtLeastOneName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals       url.Values
		name       string
		wantStatus int
	}{
		{
			name: "no_lb_names_rejected",
			vals: url.Values{
				"Action":            {"RemoveTags"},
				"Version":           {"2012-06-01"},
				"Tags.member.1.Key": {"Env"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "with_lb_name_accepted",
			vals: url.Values{
				"Action":                     {"RemoveTags"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerNames.member.1": {"remove-tags-lb"},
				"Tags.member.1.Key":          {"Env"},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			h := elb.NewHandler(b)

			if tt.wantStatus == http.StatusOK {
				mustCreateLB(t, h, "remove-tags-lb")
			}

			rec := doELB(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestAddTagsDuplicateKeys verifies that AddTags correctly handles
// duplicate keys in the input (uses len(newKeys) not len(kvs) for limit check).
func TestAddTagsDuplicateKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		vals       url.Values
		name       string
		wantStatus int
	}{
		{
			name: "duplicate_keys_counted_once_not_rejected",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				// Create an LB with 9 existing tags.
				doELB(t, h, url.Values{
					"Action":                              {"CreateLoadBalancer"},
					"Version":                             {"2012-06-01"},
					"LoadBalancerName":                    {"tag-dup-lb"},
					"AvailabilityZones.member.1":          {"us-east-1a"},
					"Listeners.member.1.Protocol":         {"HTTP"},
					"Listeners.member.1.LoadBalancerPort": {"80"},
					"Listeners.member.1.InstancePort":     {"80"},
				})
				doELB(t, h, url.Values{
					"Action":                     {"AddTags"},
					"Version":                    {"2012-06-01"},
					"LoadBalancerNames.member.1": {"tag-dup-lb"},
					"Tags.member.1.Key":          {"k1"}, "Tags.member.1.Value": {"v1"},
					"Tags.member.2.Key": {"k2"}, "Tags.member.2.Value": {"v2"},
					"Tags.member.3.Key": {"k3"}, "Tags.member.3.Value": {"v3"},
					"Tags.member.4.Key": {"k4"}, "Tags.member.4.Value": {"v4"},
					"Tags.member.5.Key": {"k5"}, "Tags.member.5.Value": {"v5"},
					"Tags.member.6.Key": {"k6"}, "Tags.member.6.Value": {"v6"},
					"Tags.member.7.Key": {"k7"}, "Tags.member.7.Value": {"v7"},
					"Tags.member.8.Key": {"k8"}, "Tags.member.8.Value": {"v8"},
					"Tags.member.9.Key": {"k9"}, "Tags.member.9.Value": {"v9"},
				})
			},
			// Now add 2 new tags where one is a duplicate of an existing key.
			// Unique new keys = 1 (k10 is new; k9 is overwrite).
			// Total = 9 existing non-overwritten + 1 truly new = 10 → should succeed.
			vals: url.Values{
				"Action":                     {"AddTags"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerNames.member.1": {"tag-dup-lb"},
				"Tags.member.1.Key":          {"k9"}, "Tags.member.1.Value": {"newv9"},
				"Tags.member.2.Key": {"k10"}, "Tags.member.2.Value": {"v10"},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := elb.NewInMemoryBackend("123456789012", "us-east-1")
			h := elb.NewHandler(backend)

			if tt.setup != nil {
				tt.setup(t, h)
			}

			got := doELB(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, got.Code)
		})
	}
}
