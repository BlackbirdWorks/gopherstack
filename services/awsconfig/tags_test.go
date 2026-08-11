package awsconfig_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

// TestCreationTags_Decoded verifies that Tags supplied at PutConfigRule /
// PutConfigurationAggregator / PutConformancePack / PutStoredQuery creation
// time reach ListTagsForResource. Real AWS Config accepts a top-level Tags
// member on each of these Put*Request shapes (botocore config/2014-11-12:
// PutConfigRuleRequest.Tags, PutConfigurationAggregatorRequest.Tags,
// PutConformancePackRequest.Tags, PutStoredQueryRequest.Tags), but gopherstack's
// decode structs for these four ops previously had no Tags field at all.
func TestCreationTags_Decoded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(t *testing.T, h *awsconfig.Handler) string
		name   string
	}{
		{
			name: "config_rule",
			create: func(t *testing.T, h *awsconfig.Handler) string {
				t.Helper()
				doAWSConfigRequest(t, h, "PutConfigRule", map[string]any{
					"ConfigRule": map[string]any{
						"ConfigRuleName": "r1",
						"Source": map[string]any{
							"Owner":            "AWS",
							"SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
						},
					},
					"Tags": []map[string]string{{"Key": "env", "Value": "prod"}},
				})
				rules, err := h.Backend.DescribeConfigRules(nil)
				require.NoError(t, err)
				require.Len(t, rules, 1)

				return rules[0].ConfigRuleArn
			},
		},
		{
			name: "configuration_aggregator",
			create: func(t *testing.T, h *awsconfig.Handler) string {
				t.Helper()
				doAWSConfigRequest(t, h, "PutConfigurationAggregator", map[string]any{
					"ConfigurationAggregatorName": "agg1",
					"AccountAggregationSources": []map[string]any{
						{"AccountIds": []string{"123456789012"}, "AllAwsRegions": true},
					},
					"Tags": []map[string]string{{"Key": "env", "Value": "prod"}},
				})
				aggs := h.Backend.DescribeConfigurationAggregators()
				require.Len(t, aggs, 1)

				return aggs[0].ConfigurationAggregatorArn
			},
		},
		{
			name: "conformance_pack",
			create: func(t *testing.T, h *awsconfig.Handler) string {
				t.Helper()
				doAWSConfigRequest(t, h, "PutConformancePack", map[string]any{
					"ConformancePackName": "cp1",
					"TemplateBody":        "Resources: {}",
					"Tags":                []map[string]string{{"Key": "env", "Value": "prod"}},
				})
				packs := h.Backend.DescribeConformancePacks()
				require.Len(t, packs, 1)

				return packs[0].ConformancePackArn
			},
		},
		{
			name: "stored_query",
			create: func(t *testing.T, h *awsconfig.Handler) string {
				t.Helper()
				doAWSConfigRequest(t, h, "PutStoredQuery", map[string]any{
					"StoredQuery": map[string]any{
						"QueryName":   "q1",
						"Expression":  "SELECT resourceId",
						"Description": "desc",
					},
					"Tags": []map[string]string{{"Key": "env", "Value": "prod"}},
				})
				metas := h.Backend.ListStoredQueries()
				require.Len(t, metas, 1)

				return metas[0].QueryArn
			},
		},
		{
			name: "aggregation_authorization",
			create: func(t *testing.T, h *awsconfig.Handler) string {
				t.Helper()
				doAWSConfigRequest(t, h, "PutAggregationAuthorization", map[string]any{
					"AuthorizedAccountId": "123456789012",
					"AuthorizedAwsRegion": "us-east-1",
					"Tags":                []map[string]string{{"Key": "env", "Value": "prod"}},
				})
				auths := h.Backend.DescribeAggregationAuthorizations()
				require.Len(t, auths, 1)

				return auths[0].AggregationAuthorizationArn
			},
		},
		{
			name: "service_linked_configuration_recorder",
			create: func(t *testing.T, h *awsconfig.Handler) string {
				t.Helper()
				rec := doAWSConfigRequest(t, h, "PutServiceLinkedConfigurationRecorder", map[string]any{
					"ServicePrincipal": "guardduty.amazonaws.com",
					"Tags":             []map[string]string{{"Key": "env", "Value": "prod"}},
				})

				var out struct {
					Arn string `json:"Arn"`
				}
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

				return out.Arn
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			resourceARN := tt.create(t, h)

			tags := h.Backend.ListTagsForResource(resourceARN)
			require.Len(t, tags, 1, "tags supplied at creation must be visible via ListTagsForResource")
			assert.Equal(t, "env", tags[0].Key)
			assert.Equal(t, "prod", tags[0].Value)
		})
	}
}

func TestTagResource(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()

	err := b.TagResource("arn:aws:config::123:rule/r1", []awsconfig.Tag{{Key: "env", Value: "prod"}})
	if err != nil {
		t.Fatalf("TagResource: %v", err)
	}

	tags := b.ListTagsForResource("arn:aws:config::123:rule/r1")
	if len(tags) != 1 || tags[0].Key != "env" || tags[0].Value != "prod" {
		t.Fatalf("ListTagsForResource: got %v, want [{env prod}]", tags)
	}
}

func TestTagResource_Update(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	arn := "arn:aws:config::123:rule/r1"

	_ = b.TagResource(arn, []awsconfig.Tag{{Key: "env", Value: "prod"}})
	_ = b.TagResource(arn, []awsconfig.Tag{{Key: "env", Value: "staging"}, {Key: "owner", Value: "team"}})

	tags := b.ListTagsForResource(arn)
	if len(tags) != 2 {
		t.Fatalf("expected 2 tags, got %d: %v", len(tags), tags)
	}

	for _, tag := range tags {
		if tag.Key == "env" && tag.Value != "staging" {
			t.Errorf("env tag not updated: got %q", tag.Value)
		}
	}
}

func TestUntagResource(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	arn := "arn:aws:config::123:rule/r1"

	_ = b.TagResource(arn, []awsconfig.Tag{{Key: "env", Value: "prod"}, {Key: "owner", Value: "team"}})
	_ = b.UntagResource(arn, []string{"env"})

	tags := b.ListTagsForResource(arn)
	if len(tags) != 1 || tags[0].Key != "owner" {
		t.Fatalf("UntagResource: got %v, want [{owner team}]", tags)
	}
}

func TestListTagsForResource_Empty(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	tags := b.ListTagsForResource("arn:missing")
	if len(tags) != 0 {
		t.Fatalf("expected empty tags, got %v", tags)
	}
}
