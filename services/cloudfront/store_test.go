package cloudfront_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestInMemoryBackend_Operations exercises the in-memory backend directly.
func TestInMemoryBackend_Operations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(*testing.T, *cloudfront.InMemoryBackend)
		name string
	}{
		{
			name: "region",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, config.DefaultRegion, b.Region())
			},
		},
		{
			name: "list_distributions_empty",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				dists := b.ListDistributions()
				assert.Empty(t, dists)
			},
		},
		{
			name: "list_oais_empty",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				oais := b.ListOAIs()
				assert.Empty(t, oais)
			},
		},
		{
			name: "distribution_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.GetDistribution("NOTEXIST")
				require.Error(t, err)
			},
		},
		{
			name: "oai_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.GetOAI("NOTEXIST")
				require.Error(t, err)
			},
		},
		{
			name: "update_nonexistent_distribution",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.UpdateDistribution("NOTEXIST", "comment", true, nil)
				require.Error(t, err)
			},
		},
		{
			name: "delete_nonexistent_distribution",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				err := b.DeleteDistribution("NOTEXIST")
				require.Error(t, err)
			},
		},
		{
			name: "delete_nonexistent_oai",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				err := b.DeleteOAI("NOTEXIST")
				require.Error(t, err)
			},
		},
		{
			name: "tag_resource_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				err := b.TagResource("arn:aws:cloudfront::123:distribution/NOTEXIST", map[string]string{"k": "v"})
				require.Error(t, err)
			},
		},
		{
			name: "untag_resource_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				err := b.UntagResource("arn:aws:cloudfront::123:distribution/NOTEXIST", []string{"k"})
				require.Error(t, err)
			},
		},
		{
			name: "list_tags_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.ListTags("arn:aws:cloudfront::123:distribution/NOTEXIST")
				require.Error(t, err)
			},
		},
		{
			name: "full_distribution_lifecycle",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				raw := minimalDistConfig("r1", "c1", true)
				d, err := b.CreateDistribution("r1", "c1", true, raw)
				require.NoError(t, err)
				assert.NotEmpty(t, d.ID)
				assert.NotEmpty(t, d.ARN)
				assert.NotEmpty(t, d.ETag)
				assert.Equal(t, "Deployed", d.Status)
				assert.Contains(t, d.DomainName, ".cloudfront.net")

				got, err := b.GetDistribution(d.ID)
				require.NoError(t, err)
				assert.Equal(t, d.ID, got.ID)

				updated, err := b.UpdateDistribution(d.ID, "updated-comment", false, raw)
				require.NoError(t, err)
				assert.NotEqual(t, d.ETag, updated.ETag)
				assert.Equal(t, "updated-comment", updated.Comment)

				err = b.TagResource(d.ARN, map[string]string{"k": "v"})
				require.NoError(t, err)

				tags, err := b.ListTags(d.ARN)
				require.NoError(t, err)
				assert.Equal(t, "v", tags["k"])

				err = b.UntagResource(d.ARN, []string{"k"})
				require.NoError(t, err)

				err = b.DeleteDistribution(d.ID)
				require.NoError(t, err)

				_, err = b.GetDistribution(d.ID)
				require.Error(t, err)
			},
		},
		{
			name: "full_oai_lifecycle",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				oai, err := b.CreateOAI("oai-ref", "oai-comment")
				require.NoError(t, err)
				assert.NotEmpty(t, oai.ID)
				assert.NotEmpty(t, oai.ETag)
				assert.NotEmpty(t, oai.S3CanonicalUserID)

				got, err := b.GetOAI(oai.ID)
				require.NoError(t, err)
				assert.Equal(t, oai.ID, got.ID)

				list := b.ListOAIs()
				assert.Len(t, list, 1)

				err = b.DeleteOAI(oai.ID)
				require.NoError(t, err)

				_, err = b.GetOAI(oai.ID)
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudfront.NewInMemoryBackend("123456789012", config.DefaultRegion)
			tt.run(t, b)
		})
	}
}

// TestInMemoryBackend_NewOperations exercises all new backend methods directly.
func TestInMemoryBackend_NewOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(*testing.T, *cloudfront.InMemoryBackend)
		name string
	}{
		{
			name: "associate_alias_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				err := b.AssociateAlias("NOTEXIST", "alias.example.com")
				require.Error(t, err)
			},
		},
		{
			name: "associate_alias_empty",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				d, err := b.CreateDistribution("ref-ba-001", "ba-dist", true, nil)
				require.NoError(t, err)
				err = b.AssociateAlias(d.ID, "")
				require.Error(t, err)
			},
		},
		{
			name: "associate_distribution_web_acl_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				err := b.AssociateDistributionWebACL("NOTEXIST", "acl-id")
				require.Error(t, err)
			},
		},
		{
			name: "associate_distribution_tenant_web_acl_empty_tenant",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				err := b.AssociateDistributionTenantWebACL("", "acl-id")
				require.Error(t, err)
			},
		},
		{
			name: "copy_distribution_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.CopyDistribution("NOTEXIST", "ref")
				require.Error(t, err)
			},
		},
		{
			name: "copy_distribution_empty_caller_ref",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				d, err := b.CreateDistribution("ref-cp-001", "cp-dist", true, nil)
				require.NoError(t, err)
				_, err = b.CopyDistribution(d.ID, "")
				require.Error(t, err)
			},
		},
		{
			name: "create_anycast_ip_list_empty_name",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateAnycastIPList("", 5)
				require.Error(t, err)
			},
		},
		{
			name: "create_cache_policy_empty_name",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateCachePolicy("", "comment", 86400, 0, 0)
				require.Error(t, err)
			},
		},
		{
			name: "create_connection_function_empty_name",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateConnectionFunction("", "comment")
				require.Error(t, err)
			},
		},
		{
			name: "create_connection_group_empty_name",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateConnectionGroup("", "comment")
				require.Error(t, err)
			},
		},
		{
			name: "create_anycast_ip_list_success",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				list, err := b.CreateAnycastIPList("my-list", 5)
				require.NoError(t, err)
				assert.NotEmpty(t, list.ID)
				assert.NotEmpty(t, list.ARN)
				assert.Equal(t, "my-list", list.Name)
				assert.Equal(t, int32(5), list.IPCount)
				assert.Equal(t, "Deployed", list.Status)
			},
		},
		{
			name: "create_cache_policy_success",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				p, err := b.CreateCachePolicy("test-policy", "a comment", 3600, 86400, 0)
				require.NoError(t, err)
				assert.NotEmpty(t, p.ID)
				assert.Equal(t, "test-policy", p.Name)
				assert.Equal(t, int64(3600), p.DefaultTTL)
				assert.Equal(t, int64(86400), p.MaxTTL)
				assert.Equal(t, int64(0), p.MinTTL)
			},
		},
		{
			name: "create_connection_function_success",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				fn, err := b.CreateConnectionFunction("my-fn", "fn comment")
				require.NoError(t, err)
				assert.NotEmpty(t, fn.ARN)
				assert.Equal(t, "my-fn", fn.Name)
				assert.Equal(t, "fn comment", fn.Comment)
			},
		},
		{
			name: "create_connection_group_success",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				g, err := b.CreateConnectionGroup("my-group", "group comment")
				require.NoError(t, err)
				assert.NotEmpty(t, g.ID)
				assert.NotEmpty(t, g.ARN)
				assert.Equal(t, "my-group", g.Name)
			},
		},
		{
			name: "create_continuous_deployment_policy_success",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				p, err := b.CreateContinuousDeploymentPolicy(true, "")
				require.NoError(t, err)
				assert.NotEmpty(t, p.ID)
				assert.True(t, p.Enabled)
			},
		},
		{
			name: "copy_distribution_success",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				src, err := b.CreateDistribution("ref-cpy-001", "src-dist", true,
					minimalDistConfig("ref-cpy-001", "src-dist", true))
				require.NoError(t, err)

				cp, err := b.CopyDistribution(src.ID, "copy-ref-001")
				require.NoError(t, err)
				assert.NotEqual(t, src.ID, cp.ID)
				assert.Equal(t, src.Comment, cp.Comment)
				assert.Equal(t, src.Enabled, cp.Enabled)
				assert.NotEmpty(t, cp.DomainName)
				assert.Equal(t, "Deployed", cp.Status)
			},
		},
		{
			name: "associate_alias_success",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				d, err := b.CreateDistribution("ref-aal-001", "alias-dist", true, nil)
				require.NoError(t, err)
				err = b.AssociateAlias(d.ID, "www.example.com")
				require.NoError(t, err)
			},
		},
		{
			name: "associate_distribution_web_acl_success",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				d, err := b.CreateDistribution("ref-awacl-001", "wacl-dist", true, nil)
				require.NoError(t, err)
				err = b.AssociateDistributionWebACL(d.ID, "arn:aws:wafv2:us-east-1:123:global/webacl/test")
				require.NoError(t, err)
			},
		},
		{
			name: "associate_distribution_tenant_web_acl_success",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				err := b.AssociateDistributionTenantWebACL("tenant-001", "acl-001")
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudfront.NewInMemoryBackend("123456789012", config.DefaultRegion)
			tt.run(t, b)
		})
	}
}

// TestInMemoryBackend_Reset verifies backend Reset() directly.
func TestInMemoryBackend_Reset(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend("123456789012", config.DefaultRegion)

	_, err := b.CreateDistribution("ref-br1", "a-dist", true, nil)
	require.NoError(t, err)

	b.Reset()

	assert.Empty(t, b.ListDistributions())
	assert.Empty(t, b.ListOAIs())
}

// TestInMemoryBackend_NewResourceTypesCRUD tests backend methods for new resource types.
func TestInMemoryBackend_NewResourceTypesCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run     func(*testing.T, *cloudfront.InMemoryBackend)
		name    string
		wantErr bool
	}{
		{
			name: "oac_duplicate_name",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateOriginAccessControl("dup", "", "s3", "always", "sigv4")
				require.NoError(t, err)
				_, err = b.CreateOriginAccessControl("dup", "", "s3", "always", "sigv4")
				require.Error(t, err)
			},
		},
		{
			name: "oac_delete_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				err := b.DeleteOriginAccessControl("DOESNOTEXIST")
				require.Error(t, err)
			},
		},
		{
			name: "rhp_duplicate_name",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateResponseHeadersPolicy("dup-rhp", "")
				require.NoError(t, err)
				_, err = b.CreateResponseHeadersPolicy("dup-rhp", "")
				require.Error(t, err)
			},
		},
		{
			name: "function_duplicate_name",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateFunction("dup-fn", "", "cloudfront-js-2.0", "code")
				require.NoError(t, err)
				_, err = b.CreateFunction("dup-fn", "", "cloudfront-js-2.0", "code")
				require.Error(t, err)
			},
		},
		{
			name: "function_publish_sets_live",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateFunction("live-fn", "", "cloudfront-js-2.0", "code")
				require.NoError(t, err)
				fn, err := b.PublishFunction("live-fn")
				require.NoError(t, err)
				assert.Equal(t, "LIVE", fn.Status)
			},
		},
		{
			name: "function_update_sets_development",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateFunction("upd-fn2", "", "cloudfront-js-2.0", "code")
				require.NoError(t, err)
				_, err = b.PublishFunction("upd-fn2")
				require.NoError(t, err)
				fn, err := b.UpdateFunction("upd-fn2", "new comment", "cloudfront-js-2.0", "new code")
				require.NoError(t, err)
				assert.Equal(t, "DEVELOPMENT", fn.Status)
			},
		},
		{
			name: "orp_duplicate_name",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateOriginRequestPolicy("dup-orp", "")
				require.NoError(t, err)
				_, err = b.CreateOriginRequestPolicy("dup-orp", "")
				require.Error(t, err)
			},
		},
		{
			name: "cache_policy_update_name_conflict",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateCachePolicy("p1", "", 86400, 31536000, 0)
				require.NoError(t, err)
				p2, err := b.CreateCachePolicy("p2", "", 86400, 31536000, 0)
				require.NoError(t, err)
				// Try to rename p2 to p1 (conflict)
				_, err = b.UpdateCachePolicy(p2.ID, "p1", "", 86400, 31536000, 0)
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudfront.NewInMemoryBackend("123456789012", "us-east-1")
			tt.run(t, b)
		})
	}
}
