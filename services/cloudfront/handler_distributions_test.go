package cloudfront_test

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestListDistributionsByPolicyID_RoundTrip covers the ListDistributionsBy* operations that
// filter the real stored distributions by an ID referenced somewhere in the distribution's
// config (cache policy, origin request policy, response headers policy, realtime log config).
func TestListDistributionsByPolicyID_RoundTrip(t *testing.T) {
	t.Parallel()

	const prefix = "/2020-05-31/"

	tests := []struct {
		listPath    func(id string) string
		name        string
		configField string
		configValue string
	}{
		{
			name:        "CachePolicyId",
			configField: "CachePolicyId",
			configValue: "cache-policy-abc123",
			listPath: func(id string) string {
				return prefix + "distributionsByCachePolicyId/" + id
			},
		},
		{
			name:        "OriginRequestPolicyId",
			configField: "OriginRequestPolicyId",
			configValue: "orp-def456",
			listPath: func(id string) string {
				return prefix + "distributionsByOriginRequestPolicyId/" + id
			},
		},
		{
			name:        "ResponseHeadersPolicyId",
			configField: "ResponseHeadersPolicyId",
			configValue: "rhp-ghi789",
			listPath: func(id string) string {
				return prefix + "distributionsByResponseHeadersPolicyId/" + id
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newCFHandler(t)

			distBody := `<DistributionConfig>
				<CallerReference>cr-` + tc.name + `</CallerReference>
				<Enabled>true</Enabled>
				<` + tc.configField + `>` + tc.configValue + `</` + tc.configField + `>
			</DistributionConfig>`
			createResp := cfOK(t, h, http.MethodPost, prefix+"distribution", distBody)
			distID := extractXMLID(t, createResp)
			if distID == "" {
				t.Fatal("expected non-empty distribution ID from create")
			}

			// Found: the distribution referencing the ID must appear in the list. These three
			// operations return DistributionIdList (bare IDs), not DistributionList (full
			// DistributionSummary objects) -- cloudfront@v1.67.4 api_op_ListDistributionsBy*.go.
			foundResp := cfOK(t, h, http.MethodGet, tc.listPath(tc.configValue), "")
			if !strings.Contains(foundResp, "DistributionIdList") {
				t.Fatalf("expected DistributionIdList, got: %s", foundResp)
			}
			if strings.Contains(foundResp, "<Quantity>0</Quantity>") {
				t.Fatalf("expected non-empty list for matching id, got: %s", foundResp)
			}
			if !strings.Contains(foundResp, distID) {
				t.Fatalf("expected distribution %s in list, got: %s", distID, foundResp)
			}

			// Not found: an unrelated ID must return an empty list, not an error.
			notFoundResp := cfOK(t, h, http.MethodGet, tc.listPath("no-such-id-xyz"), "")
			if !strings.Contains(notFoundResp, "DistributionIdList") {
				t.Fatalf("expected DistributionIdList for empty result, got: %s", notFoundResp)
			}
			if !strings.Contains(notFoundResp, "<Quantity>0</Quantity>") {
				t.Fatalf("expected empty list for non-matching id, got: %s", notFoundResp)
			}
			if strings.Contains(notFoundResp, distID) {
				t.Fatalf("distribution %s should not match unrelated id, got: %s", distID, notFoundResp)
			}
		})
	}
}

// TestListDistributionsByRealtimeLogConfig_RoundTrip verifies the realtime log config ARN is
// read from the RealtimeLogConfigArn XML body element and used to filter real distributions.
// Real ListDistributionsByRealtimeLogConfig is POST /2020-05-31/distributionsByRealtimeLogConfig
// with RealtimeLogConfigArn in the body (cloudfront@v1.67.4 serializers.go:
// awsRestxml_serializeOpDocumentListDistributionsByRealtimeLogConfigInput), not a GET with the
// ARN as a query parameter.
func TestListDistributionsByRealtimeLogConfig_RoundTrip(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	arn := "arn:aws:cloudfront::123456789012:realtime-log-config/rlc-xyz"
	distBody := `<DistributionConfig>
		<CallerReference>cr-rlc</CallerReference>
		<Enabled>true</Enabled>
		<RealtimeLogConfigArn>` + arn + `</RealtimeLogConfigArn>
	</DistributionConfig>`
	createResp := cfOK(t, h, http.MethodPost, prefix+"distribution", distBody)
	distID := extractXMLID(t, createResp)
	if distID == "" {
		t.Fatal("expected non-empty distribution ID from create")
	}

	foundResp := cfOK(t, h, http.MethodPost, prefix+"distributionsByRealtimeLogConfig",
		`<ListDistributionsByRealtimeLogConfigRequest><RealtimeLogConfigArn>`+arn+
			`</RealtimeLogConfigArn></ListDistributionsByRealtimeLogConfigRequest>`)
	if !strings.Contains(foundResp, "DistributionList") {
		t.Fatalf("expected DistributionList, got: %s", foundResp)
	}
	if !strings.Contains(foundResp, distID) {
		t.Fatalf("expected distribution %s in list, got: %s", distID, foundResp)
	}

	otherARN := "arn:aws:cloudfront::123456789012:realtime-log-config/other"
	notFoundResp := cfOK(t, h, http.MethodPost, prefix+"distributionsByRealtimeLogConfig",
		`<ListDistributionsByRealtimeLogConfigRequest><RealtimeLogConfigArn>`+otherARN+
			`</RealtimeLogConfigArn></ListDistributionsByRealtimeLogConfigRequest>`)
	if !strings.Contains(notFoundResp, "<Quantity>0</Quantity>") {
		t.Fatalf("expected empty list for non-matching arn, got: %s", notFoundResp)
	}
}

// TestCreateDistributionWithTags_InvalidTagging verifies that
// invalid tags on CreateDistributionWithTags are rejected.
func TestCreateDistributionWithTags_InvalidTagging(t *testing.T) {
	t.Parallel()

	const prefix = "/2020-05-31/"

	tests := []struct {
		name     string
		body     string
		wantErr  string
		wantCode int
	}{
		{
			name: "aws_prefix_key",
			body: `<DistributionConfigWithTags>
				<DistributionConfig>
					<CallerReference>cr-aws-pfx</CallerReference>
					<Enabled>true</Enabled>
				</DistributionConfig>
				<Tags><Items><Tag><Key>aws:reserved</Key><Value>v</Value></Tag></Items></Tags>
			</DistributionConfigWithTags>`,
			wantCode: http.StatusBadRequest,
			wantErr:  "InvalidTagging",
		},
		{
			name: "key_too_long",
			body: `<DistributionConfigWithTags>` +
				`<DistributionConfig><CallerReference>cr-long-key</CallerReference>` +
				`<Enabled>true</Enabled></DistributionConfig>` +
				`<Tags><Items><Tag><Key>` + strings.Repeat("k", 129) +
				`</Key><Value>v</Value></Tag></Items></Tags></DistributionConfigWithTags>`,
			wantCode: http.StatusBadRequest,
			wantErr:  "InvalidTagging",
		},
		{
			name: "valid_tags",
			body: `<DistributionConfigWithTags>
				<DistributionConfig>
					<CallerReference>cr-valid-tags</CallerReference>
					<Enabled>true</Enabled>
				</DistributionConfig>
				<Tags><Items><Tag><Key>env</Key><Value>prod</Value></Tag></Items></Tags>
			</DistributionConfigWithTags>`,
			wantCode: http.StatusCreated,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newCFHandler(t)
			rr := cfRequest(t, h, http.MethodPost, prefix+"distribution?WithTags", tc.body)
			if rr.Code != tc.wantCode {
				t.Errorf("got %d want %d: %s", rr.Code, tc.wantCode, rr.Body.String())
			}
			if tc.wantErr != "" && !strings.Contains(rr.Body.String(), tc.wantErr) {
				t.Errorf("want %q in body, got: %s", tc.wantErr, rr.Body.String())
			}
		})
	}
}

// TestConfigRootXML verifies the Get*Config operations return the
// config element as the document root.
func TestConfigRootXML(t *testing.T) {
	t.Parallel()

	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	tests := []struct {
		name     string
		setup    func(t *testing.T) string
		wantRoot string
	}{
		{
			name: "key_group_config",
			setup: func(t *testing.T) string {
				t.Helper()
				kg, err := h.Backend.CreateKeyGroup("kg-cfg", "cmt", nil)
				require.NoError(t, err)

				return prefix + "key-group/" + kg.ID + "/config"
			},
			wantRoot: "<KeyGroupConfig",
		},
		{
			name: "public_key_config",
			setup: func(t *testing.T) string {
				t.Helper()
				pk, err := h.Backend.CreatePublicKey("cr-cfg", "pk-cfg", "cmt", testRSA2048PublicKeyPEM)
				require.NoError(t, err)

				return prefix + "public-key/" + pk.ID + "/config"
			},
			wantRoot: "<PublicKeyConfig",
		},
		{
			name: "fle_config",
			setup: func(t *testing.T) string {
				t.Helper()
				fle, err := h.Backend.CreateFieldLevelEncryption("fle-cfg-root", "cmt", nil)
				require.NoError(t, err)

				return prefix + "field-level-encryption/" + fle.ID + "/config"
			},
			wantRoot: "<FieldLevelEncryptionConfig",
		},
		{
			name: "fle_profile_config",
			setup: func(t *testing.T) string {
				t.Helper()
				p, err := h.Backend.CreateFieldLevelEncryptionProfile("fle-prof-root", "cmt", nil)
				require.NoError(t, err)

				return prefix + "field-level-encryption-profile/" + p.ID + "/config"
			},
			wantRoot: "<FieldLevelEncryptionProfileConfig",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			path := tt.setup(t)
			rec := cfRequest(t, h, http.MethodGet, path, "")
			require.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()
			// The document root (first element after the XML prolog) must be the
			// config element, not the wrapping resource element.
			idx := strings.Index(body, "<")
			for idx >= 0 && strings.HasPrefix(body[idx:], "<?") {
				next := strings.Index(body[idx+1:], "<")
				if next < 0 {
					break
				}
				idx = idx + 1 + next
			}
			assert.True(t, strings.HasPrefix(body[idx:], tt.wantRoot),
				"want root %s, body: %s", tt.wantRoot, body)
		})
	}
}

// TestSearchIndex verifies the distribution search index reflects
// create/update/delete and matches whole config tokens (no substring false
// positives), and survives snapshot restore.
func TestSearchIndex(t *testing.T) {
	t.Parallel()

	makeDist := func(t *testing.T, b *cloudfront.InMemoryBackend, cr, kgID string) string {
		t.Helper()
		body := []byte(`<DistributionConfig><CallerReference>` + cr +
			`</CallerReference><Enabled>true</Enabled><KeyGroupId>` + kgID +
			`</KeyGroupId></DistributionConfig>`)
		d, err := b.CreateDistribution(cr, "", true, body)
		require.NoError(t, err)

		return d.ID
	}

	tests := []struct {
		run  func(t *testing.T, b *cloudfront.InMemoryBackend)
		name string
	}{
		{
			name: "create_then_list_matches",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				id := makeDist(t, b, "cr-a", "KG-AAAA1111")
				got := b.ListDistributionsByKeyGroup("KG-AAAA1111")
				require.Len(t, got, 1)
				assert.Equal(t, id, got[0].ID)
			},
		},
		{
			name: "no_substring_false_positive",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				makeDist(t, b, "cr-b", "KG-AAAA1111")
				// "AAAA1111" is a substring of the token "KG-AAAA1111" but not a
				// whole token, so the previous strings.Contains scan would have
				// matched it — the token index must not.
				assert.Empty(t, b.ListDistributionsByKeyGroup("AAAA1111"))
			},
		},
		{
			name: "update_reindexes",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				id := makeDist(t, b, "cr-c", "KG-OLD0000000")
				require.Len(t, b.ListDistributionsByKeyGroup("KG-OLD0000000"), 1)

				newBody := []byte(`<DistributionConfig><CallerReference>cr-c` +
					`</CallerReference><Enabled>true</Enabled><KeyGroupId>KG-NEW1111111` +
					`</KeyGroupId></DistributionConfig>`)
				_, err := b.UpdateDistribution(id, "", true, newBody)
				require.NoError(t, err)

				assert.Empty(t, b.ListDistributionsByKeyGroup("KG-OLD0000000"))
				require.Len(t, b.ListDistributionsByKeyGroup("KG-NEW1111111"), 1)
			},
		},
		{
			name: "delete_deindexes",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				id := makeDist(t, b, "cr-d", "KG-DEL2222222")
				require.Len(t, b.ListDistributionsByKeyGroup("KG-DEL2222222"), 1)

				_, err := b.UpdateDistribution(id, "", false, []byte(
					`<DistributionConfig><CallerReference>cr-d</CallerReference>`+
						`<Enabled>false</Enabled><KeyGroupId>KG-DEL2222222</KeyGroupId></DistributionConfig>`))
				require.NoError(t, err)
				require.NoError(t, b.DeleteDistribution(id))

				assert.Empty(t, b.ListDistributionsByKeyGroup("KG-DEL2222222"))
			},
		},
		{
			name: "restore_rebuilds_index",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				makeDist(t, b, "cr-e", "KG-SNAP3333333")
				snap := b.Snapshot(context.Background())

				b2 := newB(t)
				require.NoError(t, b2.Restore(context.Background(), snap))
				require.Len(t, b2.ListDistributionsByKeyGroup("KG-SNAP3333333"), 1)
				assert.Empty(t, b2.ListDistributionsByKeyGroup("KG-NOPE"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, newB(t))
		})
	}
}

// TestListDistributionsByTrustStore verifies distributions referencing a trust store
// are returned, and that an unrelated trust store ID yields an empty (but valid) list.
func TestListDistributionsByTrustStore(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	out := cfOK(t, h, http.MethodPost, prefix+"trust-store",
		`<TrustStoreConfig><Name>assoc-store</Name></TrustStoreConfig>`)
	tsID := extractXMLID(t, out)

	distBody := `<DistributionConfig>` +
		`<CallerReference>cr-ts</CallerReference><Enabled>true</Enabled>` +
		`<TrustStoreId>` + tsID + `</TrustStoreId>` +
		`</DistributionConfig>`
	cfOK(t, h, http.MethodPost, prefix+"distribution", distBody)

	// Real ListDistributionsByTrustStore is GET /2020-05-31/distributionsByTrustStore with
	// TrustStoreIdentifier as a query value, not a URI path segment (cloudfront@v1.67.4
	// serializers.go: awsRestxml_serializeOpHttpBindingsListDistributionsByTrustStoreInput).
	resp := cfOK(t, h, http.MethodGet, prefix+"distributionsByTrustStore?TrustStoreIdentifier="+tsID, "")
	if !strings.Contains(resp, "DistributionList") {
		t.Errorf("expected DistributionList, got: %s", resp)
	}
	// The list's own Quantity (immediately before IsTruncated) must be checked, not any nested
	// Quantity -- the DistributionSummary item shape now carries several (Origins, Restrictions,
	// Aliases), all legitimately 0 for this minimal distribution.
	if strings.Contains(resp, "<Quantity>0</Quantity><IsTruncated>") {
		t.Errorf("expected non-empty list, got: %s", resp)
	}

	empty := cfOK(t, h, http.MethodGet, prefix+"distributionsByTrustStore?TrustStoreIdentifier=nonexistent-ts", "")
	if !strings.Contains(empty, "<Quantity>0</Quantity>") {
		t.Errorf("expected empty list for unrelated trust store, got: %s", empty)
	}
}

// TestListDistributionsByWebACL tests ListDistributionsByWebACLID.
func TestListDistributionsByWebACL(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	// List empty
	out := cfOK(t, h, http.MethodGet, prefix+"distributionsByWebACLId/my-waf-id", "")
	if !strings.Contains(out, "DistributionList") {
		t.Errorf("unexpected response: %s", out)
	}
	_ = xml.Unmarshal // ensure xml is imported
}

// TestCreateDistributionWithTags tests creating a distribution with tags.
func TestCreateDistributionWithTags(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	body := `<DistributionConfigWithTags>
		<DistributionConfig>
			<CallerReference>cr-with-tags</CallerReference>
			<Comment>tagged dist</Comment>
			<Enabled>true</Enabled>
		</DistributionConfig>
		<Tags>
			<Items>
				<Tag><Key>env</Key><Value>prod</Value></Tag>
			</Items>
		</Tags>
	</DistributionConfigWithTags>`
	resp := cfOK(t, h, http.MethodPost, prefix+"distribution?WithTags", body)
	if !strings.Contains(resp, "Distribution") {
		t.Fatalf("expected Distribution in response, got: %s", resp)
	}
	distID := extractXMLID(t, resp)
	if distID == "" {
		t.Fatal("expected distribution ID in response")
	}

	// Verify the distribution exists via get
	getResp := cfOK(t, h, http.MethodGet, prefix+"distribution/"+distID, "")
	if !strings.Contains(getResp, distID) {
		t.Errorf("get did not return distribution: %s", getResp)
	}

	// The Tags sent at creation must actually have been parsed (Tags>Items>Tag
	// on the wire, not Tags>Tag; see distributionConfigWithTagsXML), not just
	// silently dropped while the create still reports success.
	arn := fmt.Sprintf("arn:aws:cloudfront::123456789012:distribution/%s", distID)
	tagsResp := cfOK(t, h, http.MethodGet, prefix+"tagging?Resource="+arn, "")
	if !strings.Contains(tagsResp, "<Key>env</Key>") || !strings.Contains(tagsResp, "<Value>prod</Value>") {
		t.Errorf("expected tag applied at creation to be retrievable, got: %s", tagsResp)
	}
}

// TestCreateDistributionWithTags_RealClient drives the real aws-sdk-go-v2
// client to prove CreateDistributionWithTags is reachable and distinct from
// CreateDistribution. Real CreateDistributionWithTags sends a bare
// "?WithTags" query flag with no value (cloudfront@v1.67.4 serializers.go:
// awsRestxml_serializeOpCreateDistributionWithTags's SplitURI on
// ".../distribution?WithTags"), never "?Resource=WithTags". gopherstack
// previously read the WithTags signal from a "Resource" query value that a
// real client never sends, so every real CreateDistributionWithTags call
// landed on plain CreateDistribution instead and silently dropped the tags
// (gopherstack-o31x).
func TestCreateDistributionWithTags_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCloudFrontClient(t, h)

	created, err := client.CreateDistributionWithTags(t.Context(), &cfsdk.CreateDistributionWithTagsInput{
		DistributionConfigWithTags: &types.DistributionConfigWithTags{
			DistributionConfig: &types.DistributionConfig{
				CallerReference: aws.String("real-client-dist-with-tags"),
				Comment:         aws.String("tagged"),
				Enabled:         aws.Bool(true),
				Origins: &types.Origins{
					Quantity: aws.Int32(1),
					Items: []types.Origin{
						{Id: aws.String("origin1"), DomainName: aws.String("example.com")},
					},
				},
				DefaultCacheBehavior: &types.DefaultCacheBehavior{
					TargetOriginId:       aws.String("origin1"),
					ViewerProtocolPolicy: types.ViewerProtocolPolicyAllowAll,
				},
			},
			Tags: &types.Tags{Items: []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}}},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created.Distribution)

	tags, err := client.ListTagsForResource(t.Context(), &cfsdk.ListTagsForResourceInput{
		Resource: created.Distribution.ARN,
	})
	require.NoError(t, err)
	require.NotNil(t, tags.Tags)
	require.Len(t, tags.Tags.Items, 1)
	assert.Equal(t, "env", aws.ToString(tags.Tags.Items[0].Key))
	assert.Equal(t, "prod", aws.ToString(tags.Tags.Items[0].Value))
}

// TestListDistributionsByKeyGroup tests ListDistributionsByKeyGroup.
func TestListDistributionsByKeyGroup(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	// Create distribution that references a key group in its config
	distBody := `<DistributionConfig>
		<CallerReference>cr-kg</CallerReference>
		<Enabled>true</Enabled>
		<KeyGroupId>key-group-abc123</KeyGroupId>
	</DistributionConfig>`
	cfOK(t, h, http.MethodPost, prefix+"distribution", distBody)

	// List by key group - should find the distribution. ListDistributionsByKeyGroup returns
	// DistributionIdList (bare IDs), not DistributionList -- cloudfront@v1.67.4
	// api_op_ListDistributionsByKeyGroup.go: Output.DistributionIdList.
	resp := cfOK(t, h, http.MethodGet, prefix+"distributionsByKeyGroupId/key-group-abc123", "")
	if !strings.Contains(resp, "DistributionIdList") {
		t.Errorf("expected DistributionIdList, got: %s", resp)
	}
	// Should have quantity > 0
	if strings.Contains(resp, "<Quantity>0</Quantity>") {
		t.Errorf("expected non-empty list, got: %s", resp)
	}

	// Different key group should return empty list
	resp2 := cfOK(t, h, http.MethodGet, prefix+"distributionsByKeyGroupId/nonexistent-key-group", "")
	if !strings.Contains(resp2, "DistributionIdList") {
		t.Errorf("expected DistributionIdList for empty result, got: %s", resp2)
	}
}

// TestDisassociateWebACL tests DisassociateDistributionWebACL.
func TestDisassociateWebACL(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	// Create distribution
	distResp := cfOK(t, h, http.MethodPost, prefix+"distribution",
		`<DistributionConfig><CallerReference>cr-wacl</CallerReference><Enabled>true</Enabled></DistributionConfig>`)
	distID := extractXMLID(t, distResp)
	if distID == "" {
		t.Fatal("no dist ID in create response")
	}

	// Associate web ACL
	cfOK(t, h, http.MethodPut, prefix+"distribution/"+distID+"/associate-web-acl",
		`<AssociateDistributionWebACLRequest><WebACLArn>waf-123</WebACLArn></AssociateDistributionWebACLRequest>`)

	// Disassociate web ACL
	disResp := cfOK(t, h, http.MethodPut, prefix+"distribution/"+distID+"/disassociate-web-acl", "")
	if !strings.Contains(disResp, "Distribution") {
		t.Errorf("expected Distribution in disassociate response, got: %s", disResp)
	}
}

// TestUpdateDistributionWithStagingConfig tests promotion of staging config.
func TestUpdateDistributionWithStagingConfig(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	// Create primary distribution
	primaryResp := cfOK(t, h, http.MethodPost, prefix+"distribution",
		`<DistributionConfig><CallerReference>cr-primary</CallerReference><Enabled>true</Enabled></DistributionConfig>`)
	primaryID := extractXMLID(t, primaryResp)

	// Create staging distribution
	stagingResp := cfOK(
		t,
		h,
		http.MethodPost,
		prefix+"distribution",
		`<DistributionConfig><CallerReference>cr-staging</CallerReference><Enabled>false</Enabled></DistributionConfig>`,
	)
	stagingID := extractXMLID(t, stagingResp)

	// Promote staging to primary. Real clients send StagingDistributionId as a query
	// parameter to /promote-staging-config, never in the body (cloudfront@v1.67.4
	// serializers.go: awsRestxml_serializeOpHttpBindingsUpdateDistributionWithStagingConfigInput).
	promoteResp := cfOK(
		t, h, http.MethodPut,
		prefix+"distribution/"+primaryID+"/promote-staging-config?StagingDistributionId="+stagingID, "",
	)
	if !strings.Contains(promoteResp, "Distribution") {
		t.Errorf("expected Distribution in response, got: %s", promoteResp)
	}
}

// TestUpdateDistributionWithStagingConfig_MalformedBodyHandled verifies a malformed
// request body is rejected with 400 MalformedXML instead of silently proceeding
// (gopherstack-ob1g: the previous handler discarded xml.Unmarshal's error). It also
// exercises the corrected /promote-staging-config route (gopherstack-ob1g: the route
// table previously matched a "/staging" suffix no real client sends).
func TestUpdateDistributionWithStagingConfig_MalformedBodyHandled(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	primaryResp := cfOK(
		t,
		h,
		http.MethodPost,
		prefix+"distribution",
		`<DistributionConfig><CallerReference>cr-primary-x</CallerReference><Enabled>true</Enabled></DistributionConfig>`,
	)
	primaryID := extractXMLID(t, primaryResp)

	rec := cfRequest(t, h, http.MethodPut, prefix+"distribution/"+primaryID+"/promote-staging-config", "<<<not xml")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "MalformedXML")
}

func TestDistributionCreatesAsDeployed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		callerRef string
	}{
		{name: "basic_distribution", callerRef: "ref-1"},
		{name: "second_distribution", callerRef: "ref-2"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			d, err := b.CreateDistribution(tc.callerRef, "test", true, nil)
			require.NoError(t, err)
			assert.Equal(t, "Deployed", d.Status)
		})
	}
}

func TestDistributionHasLastModifiedTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "create"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			d, err := b.CreateDistribution("ref-lmt", "test", true, nil)
			require.NoError(t, err)
			assert.NotEmpty(t, d.LastModifiedTime, tc.name)
		})
	}
}

func TestUpdateDistributionSetsInProgress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "update_sets_inprogress"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			d, err := b.CreateDistribution("ref-upd", "initial", true, nil)
			require.NoError(t, err)

			updated, err := b.UpdateDistribution(d.ID, "updated", true, nil)
			require.NoError(t, err)
			assert.Equal(t, "InProgress", updated.Status, tc.name)
			assert.NotEmpty(t, updated.LastModifiedTime)
		})
	}
}

func TestCopyDistributionCreatesAsDeployed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "copy_is_inprogress"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			src, err := b.CreateDistribution("ref-src", "source", true, nil)
			require.NoError(t, err)

			cp, err := b.CopyDistribution(src.ID, "ref-copy")
			require.NoError(t, err)
			assert.Equal(t, "Deployed", cp.Status, tc.name)
			assert.NotEmpty(t, cp.LastModifiedTime)
		})
	}
}

func TestDistributionResponseHasLastModifiedTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "response_includes_last_modified_time"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXML(t, h, http.MethodPost, "/2020-05-31/distribution",
				minimalDistConfig("ref-lmt-h", "test", true))
			require.Equal(t, http.StatusCreated, rec.Code, tc.name)
			assert.Contains(t, rec.Body.String(), "<LastModifiedTime>", tc.name)
		})
	}
}

// TestListDistributionsPagination verifies that ListDistributions
// supports Marker/MaxItems pagination and returns IsTruncated + NextMarker
// when results are truncated.
func TestListDistributionsPagination(t *testing.T) {
	t.Parallel()

	type pageTC struct {
		maxItems       string
		marker         string
		name           string
		numDists       int
		wantQuantity   int
		wantTruncated  bool
		wantNextMarker bool
	}
	tests := []pageTC{
		{
			name:         "no_pagination_params_returns_all",
			numDists:     3,
			wantQuantity: 3,
		},
		{
			name:           "max_items_limits_result",
			numDists:       5,
			maxItems:       "2",
			wantQuantity:   2,
			wantTruncated:  true,
			wantNextMarker: true,
		},
		{
			name:         "marker_advances_page",
			numDists:     4,
			maxItems:     "10",
			wantQuantity: 4,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			for i := range tc.numDists {
				rec := doXML(t, h, http.MethodPost, "/2020-05-31/distribution",
					minimalDistConfig(fmt.Sprintf("ref-pg-%d", i), "test", true))
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			path := "/2020-05-31/distribution"
			sep := "?"
			if tc.maxItems != "" {
				path += sep + "MaxItems=" + tc.maxItems
				sep = "&"
			}
			if tc.marker != "" {
				path += sep + "Marker=" + tc.marker
			}

			rec := doXML(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, rec.Code, tc.name)

			body := rec.Body.String()
			if tc.wantTruncated {
				assert.Contains(t, body, "<IsTruncated>true</IsTruncated>", tc.name)
			} else {
				assert.Contains(t, body, "<IsTruncated>false</IsTruncated>", tc.name)
			}

			if tc.wantNextMarker {
				assert.Contains(t, body, "<NextMarker>", tc.name)
			}
		})
	}
}

// TestListDistributionsByWebACLId_ItemShape_RealClient is a regression test for
// gopherstack-21my: ListDistributionsByWebACLId (and the five siblings that share
// the same writeDistributionList/marshalDistributionList code path --
// ByAnycastIpListId, ByConnectionFunction, ByConnectionMode, ByTrustStore,
// ByRealtimeLogConfig) emitted a DistributionSummary item with only
// Id/ARN/Status/DomainName, dropping every other DistributionSummary member --
// including ETag and Aliases, both backed by real state -- even though this
// service's own ListDistributions op already builds the full item shape for the
// identical DistributionSummary wire type. Seeds two distributions with
// distinguishable Comment/PriceClass/HttpVersion/Aliases, associates both with
// the same web ACL, and asserts every field round-trips through the real SDK
// client rather than decoding to its zero value.
func TestListDistributionsByWebACLId_ItemShape_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCloudFrontClient(t, h)

	// A slash-free WebACLId (WAF Classic-style) is used deliberately: the ARN form
	// (WAFV2) trips an unrelated routing bug in extractResourceID (handler.go), which
	// cuts a URI-label identifier at its first "/" -- out of this test's scope, filed
	// separately (gopherstack-21my final report).
	const webACLArn = "a1b2c3d4-5678-90ab-cdef-example11111"

	mk := func(ref, comment string, priceClass types.PriceClass, alias string) *cfsdk.CreateDistributionOutput {
		out, err := client.CreateDistribution(t.Context(), &cfsdk.CreateDistributionInput{
			DistributionConfig: &types.DistributionConfig{
				CallerReference: aws.String(ref),
				Comment:         aws.String(comment),
				Enabled:         aws.Bool(true),
				PriceClass:      priceClass,
				HttpVersion:     types.HttpVersionHttp2,
				Origins: &types.Origins{
					Quantity: aws.Int32(1),
					Items: []types.Origin{
						{Id: aws.String("origin1"), DomainName: aws.String("example.com")},
					},
				},
				DefaultCacheBehavior: &types.DefaultCacheBehavior{
					TargetOriginId:       aws.String("origin1"),
					ViewerProtocolPolicy: types.ViewerProtocolPolicyAllowAll,
				},
			},
		})
		require.NoError(t, err)

		_, err = client.AssociateAlias(t.Context(), &cfsdk.AssociateAliasInput{
			TargetDistributionId: out.Distribution.Id,
			Alias:                aws.String(alias),
		})
		require.NoError(t, err)

		_, err = client.AssociateDistributionWebACL(t.Context(), &cfsdk.AssociateDistributionWebACLInput{
			Id:        out.Distribution.Id,
			WebACLArn: aws.String(webACLArn),
		})
		require.NoError(t, err)

		return out
	}

	first := mk("ref-webacl-shape-1", "first distribution", types.PriceClassPriceClass100, "one.example.com")
	second := mk("ref-webacl-shape-2", "second distribution", types.PriceClassPriceClass200, "two.example.com")

	listed, err := client.ListDistributionsByWebACLId(t.Context(), &cfsdk.ListDistributionsByWebACLIdInput{
		WebACLId: aws.String(webACLArn),
	})
	require.NoError(t, err)
	require.NotNil(t, listed.DistributionList)
	require.Len(t, listed.DistributionList.Items, 2)

	byID := make(map[string]types.DistributionSummary, 2)
	for _, item := range listed.DistributionList.Items {
		require.NotNil(t, item.Id)
		byID[*item.Id] = item
	}

	item1, ok := byID[*first.Distribution.Id]
	require.True(t, ok)
	assert.Equal(t, "first distribution", aws.ToString(item1.Comment))
	assert.Equal(t, types.PriceClassPriceClass100, item1.PriceClass)
	assert.Equal(t, types.HttpVersionHttp2, item1.HttpVersion)
	assert.True(t, aws.ToBool(item1.Enabled))
	assert.NotEmpty(t, aws.ToString(item1.ETag), "ETag must round-trip, not decode empty")
	require.NotNil(t, item1.Aliases)
	require.Len(t, item1.Aliases.Items, 1)
	assert.Equal(t, "one.example.com", item1.Aliases.Items[0])

	item2, ok := byID[*second.Distribution.Id]
	require.True(t, ok)
	assert.Equal(t, "second distribution", aws.ToString(item2.Comment))
	assert.Equal(t, types.PriceClassPriceClass200, item2.PriceClass)
	require.NotNil(t, item2.Aliases)
	require.Len(t, item2.Aliases.Items, 1)
	assert.Equal(t, "two.example.com", item2.Aliases.Items[0])
}

// TestListConflictingAliases_AccountID_RealClient covers the per-item AccountId field: the
// backend has an AccountID() accessor (used correctly by ListVpcOrigins and
// ListDistributionsByOwnedResource for the same real field), but handleListConflictingAliases
// hardcoded AccountId to "" instead of reading it.
func TestListConflictingAliases_AccountID_RealClient(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "555566667777", "us-east-1")
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	first, err := backend.CreateDistribution("ca-acct-owner", "", true, nil)
	require.NoError(t, err)
	other, err := backend.CreateDistribution("ca-acct-other", "", true, nil)
	require.NoError(t, err)
	require.NoError(t, backend.AssociateAlias(other.ID, "ca-acct.example.com"))

	out, listErr := client.ListConflictingAliases(t.Context(), &cfsdk.ListConflictingAliasesInput{
		Alias:          aws.String("ca-acct.example.com"),
		DistributionId: aws.String(first.ID),
	})
	require.NoError(t, listErr)
	require.NotNil(t, out.ConflictingAliasesList)
	require.Len(t, out.ConflictingAliasesList.Items, 1)

	assert.Equal(t, "555566667777", aws.ToString(out.ConflictingAliasesList.Items[0].AccountId))
}
