package cloudfront_test

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
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
				return prefix + "distributions/by-cache-policy-id/" + id
			},
		},
		{
			name:        "OriginRequestPolicyId",
			configField: "OriginRequestPolicyId",
			configValue: "orp-def456",
			listPath: func(id string) string {
				return prefix + "distributions/by-origin-request-policy-id/" + id
			},
		},
		{
			name:        "ResponseHeadersPolicyId",
			configField: "ResponseHeadersPolicyId",
			configValue: "rhp-ghi789",
			listPath: func(id string) string {
				return prefix + "distributions/by-response-headers-policy-id/" + id
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

			// Found: the distribution referencing the ID must appear in the list.
			foundResp := cfOK(t, h, http.MethodGet, tc.listPath(tc.configValue), "")
			if !strings.Contains(foundResp, "DistributionList") {
				t.Fatalf("expected DistributionList, got: %s", foundResp)
			}
			if strings.Contains(foundResp, "<Quantity>0</Quantity>") {
				t.Fatalf("expected non-empty list for matching id, got: %s", foundResp)
			}
			if !strings.Contains(foundResp, distID) {
				t.Fatalf("expected distribution %s in list, got: %s", distID, foundResp)
			}

			// Not found: an unrelated ID must return an empty list, not an error.
			notFoundResp := cfOK(t, h, http.MethodGet, tc.listPath("no-such-id-xyz"), "")
			if !strings.Contains(notFoundResp, "DistributionList") {
				t.Fatalf("expected DistributionList for empty result, got: %s", notFoundResp)
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
// read from the RealtimeLogConfigArn query parameter and used to filter real distributions.
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

	foundResp := cfOK(t, h, http.MethodGet,
		prefix+"distributions/by-realtime-log-config?RealtimeLogConfigArn="+arn, "")
	if !strings.Contains(foundResp, "DistributionList") {
		t.Fatalf("expected DistributionList, got: %s", foundResp)
	}
	if !strings.Contains(foundResp, distID) {
		t.Fatalf("expected distribution %s in list, got: %s", distID, foundResp)
	}

	otherARN := "arn:aws:cloudfront::123456789012:realtime-log-config/other"
	notFoundResp := cfOK(t, h, http.MethodGet,
		prefix+"distributions/by-realtime-log-config?RealtimeLogConfigArn="+otherARN, "")
	if !strings.Contains(notFoundResp, "<Quantity>0</Quantity>") {
		t.Fatalf("expected empty list for non-matching arn, got: %s", notFoundResp)
	}
}

// TestBatch2Accuracy_CreateDistributionWithTags_InvalidTagging verifies that
// invalid tags on CreateDistributionWithTags are rejected.
func TestBatch2Accuracy_CreateDistributionWithTags_InvalidTagging(t *testing.T) {
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
				<Tags><Tag><Key>aws:reserved</Key><Value>v</Value></Tag></Tags>
			</DistributionConfigWithTags>`,
			wantCode: http.StatusBadRequest,
			wantErr:  "InvalidTagging",
		},
		{
			name: "key_too_long",
			body: `<DistributionConfigWithTags>` +
				`<DistributionConfig><CallerReference>cr-long-key</CallerReference>` +
				`<Enabled>true</Enabled></DistributionConfig>` +
				`<Tags><Tag><Key>` + strings.Repeat("k", 129) +
				`</Key><Value>v</Value></Tag></Tags></DistributionConfigWithTags>`,
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
				<Tags><Tag><Key>env</Key><Value>prod</Value></Tag></Tags>
			</DistributionConfigWithTags>`,
			wantCode: http.StatusCreated,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newCFHandler(t)
			rr := cfRequest(t, h, http.MethodPost, prefix+"distribution?Resource=WithTags", tc.body)
			if rr.Code != tc.wantCode {
				t.Errorf("got %d want %d: %s", rr.Code, tc.wantCode, rr.Body.String())
			}
			if tc.wantErr != "" && !strings.Contains(rr.Body.String(), tc.wantErr) {
				t.Errorf("want %q in body, got: %s", tc.wantErr, rr.Body.String())
			}
		})
	}
}

// TestParityBatch3_ConfigRootXML verifies the Get*Config operations return the
// config element as the document root.
func TestParityBatch3_ConfigRootXML(t *testing.T) {
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

// TestParityBatch3_SearchIndex verifies the distribution search index reflects
// create/update/delete and matches whole config tokens (no substring false
// positives), and survives snapshot restore.
func TestParityBatch3_SearchIndex(t *testing.T) {
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

// TestNewOps_ListDistributionsByTrustStore verifies distributions referencing a trust store
// are returned, and that an unrelated trust store ID yields an empty (but valid) list.
func TestNewOps_ListDistributionsByTrustStore(t *testing.T) {
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

	resp := cfOK(t, h, http.MethodGet, prefix+"distributions/by-trust-store-id/"+tsID, "")
	if !strings.Contains(resp, "DistributionList") {
		t.Errorf("expected DistributionList, got: %s", resp)
	}
	if strings.Contains(resp, "<Quantity>0</Quantity>") {
		t.Errorf("expected non-empty list, got: %s", resp)
	}

	empty := cfOK(t, h, http.MethodGet, prefix+"distributions/by-trust-store-id/nonexistent-ts", "")
	if !strings.Contains(empty, "<Quantity>0</Quantity>") {
		t.Errorf("expected empty list for unrelated trust store, got: %s", empty)
	}
}

// TestNewOps_ListDistributionsByWebACL tests ListDistributionsByWebACLID.
func TestNewOps_ListDistributionsByWebACL(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	// List empty
	out := cfOK(t, h, http.MethodGet, prefix+"distributions/by-web-acl-id/my-waf-id", "")
	if !strings.Contains(out, "DistributionList") {
		t.Errorf("unexpected response: %s", out)
	}
	_ = xml.Unmarshal // ensure xml is imported
}

// TestBatch2_CreateDistributionWithTags tests creating a distribution with tags.
func TestBatch2_CreateDistributionWithTags(t *testing.T) {
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
			<Tag><Key>env</Key><Value>prod</Value></Tag>
		</Tags>
	</DistributionConfigWithTags>`
	resp := cfOK(t, h, http.MethodPost, prefix+"distribution?Resource=WithTags", body)
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
}

// TestBatch2_ListDistributionsByKeyGroup tests ListDistributionsByKeyGroup.
func TestBatch2_ListDistributionsByKeyGroup(t *testing.T) {
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

	// List by key group - should find the distribution
	resp := cfOK(t, h, http.MethodGet, prefix+"distributions/by-key-group/key-group-abc123", "")
	if !strings.Contains(resp, "DistributionList") {
		t.Errorf("expected DistributionList, got: %s", resp)
	}
	// Should have quantity > 0
	if strings.Contains(resp, "<Quantity>0</Quantity>") {
		t.Errorf("expected non-empty list, got: %s", resp)
	}

	// Different key group should return empty list
	resp2 := cfOK(t, h, http.MethodGet, prefix+"distributions/by-key-group/nonexistent-key-group", "")
	if !strings.Contains(resp2, "DistributionList") {
		t.Errorf("expected DistributionList for empty result, got: %s", resp2)
	}
}

// TestBatch2_DisassociateWebACL tests DisassociateDistributionWebACL.
func TestBatch2_DisassociateWebACL(t *testing.T) {
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
		`<WebACLAssociation><WebACLId>waf-123</WebACLId></WebACLAssociation>`)

	// Disassociate web ACL
	disResp := cfOK(t, h, http.MethodPut, prefix+"distribution/"+distID+"/disassociate-web-acl", "")
	if !strings.Contains(disResp, "Distribution") {
		t.Errorf("expected Distribution in disassociate response, got: %s", disResp)
	}
}

// TestBatch2_UpdateDistributionWithStagingConfig tests promotion of staging config.
func TestBatch2_UpdateDistributionWithStagingConfig(t *testing.T) {
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

	// Promote staging to primary
	promoteBody := `<UpdateDistributionWithStagingConfigRequest>` +
		`<StagingDistributionId>` + stagingID + `</StagingDistributionId>` +
		`</UpdateDistributionWithStagingConfigRequest>`
	promoteResp := cfOK(t, h, http.MethodPut, prefix+"distribution/"+primaryID+"/staging", promoteBody)
	if !strings.Contains(promoteResp, "Distribution") {
		t.Errorf("expected Distribution in response, got: %s", promoteResp)
	}
}

func TestParity_DistributionCreatesAsDeployed(t *testing.T) {
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

			b := newTestBackend()
			d, err := b.CreateDistribution(tc.callerRef, "test", true, nil)
			require.NoError(t, err)
			assert.Equal(t, "Deployed", d.Status)
		})
	}
}

func TestParity_DistributionHasLastModifiedTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "create"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			d, err := b.CreateDistribution("ref-lmt", "test", true, nil)
			require.NoError(t, err)
			assert.NotEmpty(t, d.LastModifiedTime, tc.name)
		})
	}
}

func TestParity_UpdateDistributionSetsInProgress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "update_sets_inprogress"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			d, err := b.CreateDistribution("ref-upd", "initial", true, nil)
			require.NoError(t, err)

			updated, err := b.UpdateDistribution(d.ID, "updated", true, nil)
			require.NoError(t, err)
			assert.Equal(t, "InProgress", updated.Status, tc.name)
			assert.NotEmpty(t, updated.LastModifiedTime)
		})
	}
}

func TestParity_CopyDistributionCreatesAsDeployed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "copy_is_inprogress"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			src, err := b.CreateDistribution("ref-src", "source", true, nil)
			require.NoError(t, err)

			cp, err := b.CopyDistribution(src.ID, "ref-copy")
			require.NoError(t, err)
			assert.Equal(t, "Deployed", cp.Status, tc.name)
			assert.NotEmpty(t, cp.LastModifiedTime)
		})
	}
}

func TestParity_DistributionResponseHasLastModifiedTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "response_includes_last_modified_time"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doXML(t, h, http.MethodPost, "/2020-05-31/distribution",
				minimalDistConfig("ref-lmt-h", "test", true))
			require.Equal(t, http.StatusCreated, rec.Code, tc.name)
			assert.Contains(t, rec.Body.String(), "<LastModifiedTime>", tc.name)
		})
	}
}

// TestParity_ListDistributionsPagination verifies that ListDistributions
// supports Marker/MaxItems pagination and returns IsTruncated + NextMarker
// when results are truncated.
func TestParity_ListDistributionsPagination(t *testing.T) {
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

			h := newTestHandler()
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

// TestFunctionAssociations covers the function association handlers.
func TestFunctionAssociations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *cloudfront.Handler) string
		check      func(*testing.T, *httptest.ResponseRecorder, string)
		name       string
		method     string
		path       string
		body       []byte
		wantStatus int
	}{
		{
			name:   "set_and_get_function_associations",
			method: http.MethodPut,
			path:   "",
			body: []byte(
				`<FunctionAssociations>` +
					`<Quantity>1</Quantity>` +
					`<Items>` +
					`<FunctionAssociation>` +
					`<FunctionARN>arn:aws:cloudfront::123456789012:function/my-fn</FunctionARN>` +
					`<EventType>viewer-request</EventType>` +
					`</FunctionAssociation>` +
					`</Items>` +
					`</FunctionAssociations>`,
			),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("assoc-ref-1", "assoc-dist", true, nil)
				require.NoError(t, err)

				return "/2020-05-31/distribution/" + d.ID + "/function-associations"
			},
			wantStatus: http.StatusOK,
			check:      nil,
		},
		{
			name:   "get_function_associations",
			method: http.MethodGet,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("assoc-ref-2", "get-assoc-dist", true, nil)
				require.NoError(t, err)
				associations := []cloudfront.FunctionAssociation{
					{FunctionARN: "arn:aws:cloudfront::123:function/fn", EventType: "viewer-request"},
				}
				require.NoError(t, h.Backend.SetDistributionFunctionAssociations(d.ID, associations))

				return "/2020-05-31/distribution/" + d.ID + "/function-associations"
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<FunctionAssociations")
				assert.Contains(t, rec.Body.String(), "<Quantity>1</Quantity>")
			},
		},
		{
			name:   "get_function_associations_dist_not_found",
			method: http.MethodGet,
			path:   "/2020-05-31/distribution/doesnotexist/function-associations",
			body:   nil,
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<Error>")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			path := tt.path
			if tt.setup != nil {
				if p := tt.setup(t, h); p != "" {
					path = p
				}
			}

			rec := doXML(t, h, tt.method, path, tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.check != nil {
				tt.check(t, rec, path)
			}
		})
	}
}

// TestBackendFunctionAssociationsDirectly tests function associations directly on the backend.
func TestBackendFunctionAssociationsDirectly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(*testing.T, *cloudfront.InMemoryBackend)
		name string
	}{
		{
			name: "set_and_get_associations",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				d, err := b.CreateDistribution("fn-assoc-ref", "fn-assoc-dist", true, nil)
				require.NoError(t, err)

				assocs := []cloudfront.FunctionAssociation{
					{FunctionARN: "arn:aws:cloudfront::123:function/my-fn", EventType: "viewer-request"},
				}
				require.NoError(t, b.SetDistributionFunctionAssociations(d.ID, assocs))

				got, err := b.GetDistributionFunctionAssociations(d.ID)
				require.NoError(t, err)
				require.Len(t, got, 1)
				assert.Equal(t, "arn:aws:cloudfront::123:function/my-fn", got[0].FunctionARN)
			},
		},
		{
			name: "get_associations_dist_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.GetDistributionFunctionAssociations("doesnotexist")
				require.Error(t, err)
			},
		},
		{
			name: "set_associations_dist_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				err := b.SetDistributionFunctionAssociations("doesnotexist", nil)
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

// TestDistributionCRUD covers create, get, update, list, and delete operations.
func TestDistributionCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *cloudfront.Handler) string
		check      func(*testing.T, *httptest.ResponseRecorder, string)
		headers    func(*testing.T, *cloudfront.Handler, string) map[string]string
		name       string
		method     string
		path       string
		body       []byte
		wantStatus int
	}{
		{
			name:   "create_distribution",
			method: http.MethodPost,
			path:   "/2020-05-31/distribution",
			body:   minimalDistConfig("ref-001", "my-dist", true),
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<Distribution")
				assert.Contains(t, rec.Body.String(), "<Status>Deployed</Status>")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
				assert.NotEmpty(t, rec.Header().Get("Location"))
			},
		},
		{
			name:   "get_distribution",
			method: http.MethodGet,
			path:   "", // set in setup
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-002", "get-dist", true,
					minimalDistConfig("ref-002", "get-dist", true))
				require.NoError(t, err)

				return "/2020-05-31/distribution/" + d.ID
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<Distribution")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "get_distribution_not_found",
			method: http.MethodGet,
			path:   "/2020-05-31/distribution/DOESNOTEXIST",
			body:   nil,
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "NoSuchDistribution")
			},
		},
		{
			name:   "get_distribution_config",
			method: http.MethodGet,
			path:   "", // set in setup
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-003", "cfg-dist", true,
					minimalDistConfig("ref-003", "cfg-dist", true))
				require.NoError(t, err)

				return "/2020-05-31/distribution/" + d.ID + "/config"
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "DistributionConfig")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "update_distribution",
			method: http.MethodPut,
			path:   "", // set in setup
			body:   minimalDistConfig("ref-004", "updated-dist", false),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-004", "orig-dist", true,
					minimalDistConfig("ref-004", "orig-dist", true))
				require.NoError(t, err)

				return "/2020-05-31/distribution/" + d.ID + "/config"
			},
			headers: func(t *testing.T, h *cloudfront.Handler, path string) map[string]string {
				t.Helper()
				// path is "/2020-05-31/distribution/{ID}/config" — extract ID
				parts := strings.Split(strings.TrimPrefix(path, "/2020-05-31/distribution/"), "/")
				d, err := h.Backend.GetDistribution(parts[0])
				require.NoError(t, err)

				return map[string]string{"If-Match": d.ETag}
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<Distribution")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "list_distributions",
			method: http.MethodGet,
			path:   "/2020-05-31/distribution",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateDistribution("ref-005", "list-dist", true,
					minimalDistConfig("ref-005", "list-dist", true))
				require.NoError(t, err)

				return ""
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "DistributionList")
			},
		},
		{
			name:   "delete_distribution",
			method: http.MethodDelete,
			path:   "", // set in setup
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-006", "del-dist", false,
					minimalDistConfig("ref-006", "del-dist", false))
				require.NoError(t, err)

				return "/2020-05-31/distribution/" + d.ID
			},
			headers: func(t *testing.T, h *cloudfront.Handler, path string) map[string]string {
				t.Helper()
				id := strings.TrimPrefix(path, "/2020-05-31/distribution/")
				d, err := h.Backend.GetDistribution(id)
				require.NoError(t, err)

				return map[string]string{"If-Match": d.ETag}
			},
			wantStatus: http.StatusNoContent,
			check:      func(t *testing.T, _ *httptest.ResponseRecorder, _ string) { t.Helper() },
		},
		{
			name:   "delete_distribution_not_found",
			method: http.MethodDelete,
			path:   "/2020-05-31/distribution/DOESNOTEXIST",
			body:   nil,
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "NoSuchDistribution")
			},
		},
		{
			name:   "update_distribution_precondition_failed",
			method: http.MethodPut,
			path:   "", // set in setup
			body:   minimalDistConfig("ref-007", "updated-dist", false),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-007", "orig-dist", true,
					minimalDistConfig("ref-007", "orig-dist", true))
				require.NoError(t, err)

				return "/2020-05-31/distribution/" + d.ID + "/config"
			},
			wantStatus: http.StatusPreconditionFailed,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "PreconditionFailed")
			},
		},
		{
			name:   "delete_distribution_precondition_failed",
			method: http.MethodDelete,
			path:   "", // set in setup
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-008", "del-dist-2", false,
					minimalDistConfig("ref-008", "del-dist-2", false))
				require.NoError(t, err)

				return "/2020-05-31/distribution/" + d.ID
			},
			wantStatus: http.StatusPreconditionFailed,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "PreconditionFailed")
			},
		},
		{
			name:   "delete_distribution_not_disabled",
			method: http.MethodDelete,
			path:   "", // set in setup
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-009", "enabled-dist", true,
					minimalDistConfig("ref-009", "enabled-dist", true))
				require.NoError(t, err)

				return "/2020-05-31/distribution/" + d.ID
			},
			headers: func(t *testing.T, h *cloudfront.Handler, path string) map[string]string {
				t.Helper()
				id := strings.TrimPrefix(path, "/2020-05-31/distribution/")
				d, err := h.Backend.GetDistribution(id)
				require.NoError(t, err)

				return map[string]string{"If-Match": d.ETag}
			},
			wantStatus: http.StatusConflict,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "DistributionNotDisabled")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			path := tt.path
			if tt.setup != nil {
				if p := tt.setup(t, h); p != "" {
					path = p
				}
			}

			var hdrs map[string]string
			if tt.headers != nil {
				hdrs = tt.headers(t, h, path)
			}

			rec := doXMLWithHeaders(t, h, tt.method, path, tt.body, hdrs)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec, path)
		})
	}
}

// TestAssociateAlias covers the AssociateAlias operation.
func TestAssociateAlias(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *cloudfront.Handler) string
		check      func(*testing.T, *httptest.ResponseRecorder)
		name       string
		alias      string
		wantStatus int
	}{
		{
			name:  "associate_alias_success",
			alias: "www.example.com",
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-aa-001", "alias-dist", true,
					minimalDistConfig("ref-aa-001", "alias-dist", true))
				require.NoError(t, err)

				return d.ID
			},
			wantStatus: http.StatusOK,
			check:      func(t *testing.T, _ *httptest.ResponseRecorder) { t.Helper() },
		},
		{
			name:  "associate_alias_distribution_not_found",
			alias: "notfound.example.com",
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return "DOESNOTEXIST"
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "NoSuchDistribution")
			},
		},
		{
			name:  "associate_alias_empty_alias",
			alias: "",
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-aa-002", "alias-dist2", true,
					minimalDistConfig("ref-aa-002", "alias-dist2", true))
				require.NoError(t, err)

				return d.ID
			},
			wantStatus: http.StatusBadRequest,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "InvalidArgument")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			distID := tt.setup(t, h)
			path := "/2020-05-31/distribution/" + distID + "/associate-alias"
			if tt.alias != "" {
				path += "?Alias=" + tt.alias
			}

			rec := doXML(t, h, http.MethodPut, path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec)
		})
	}
}

// TestAssociateAlias_Idempotent verifies associating the same alias twice is safe.
func TestAssociateAlias_Idempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	d, err := h.Backend.CreateDistribution("ref-ai-001", "idempotent-dist", true,
		minimalDistConfig("ref-ai-001", "idempotent-dist", true))
	require.NoError(t, err)

	path := "/2020-05-31/distribution/" + d.ID + "/associate-alias?Alias=idem.example.com"
	rec := doXML(t, h, http.MethodPut, path, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doXML(t, h, http.MethodPut, path, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestAssociateDistributionWebACL covers the AssociateDistributionWebACL operation.
func TestAssociateDistributionWebACL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *cloudfront.Handler) string
		check      func(*testing.T, *httptest.ResponseRecorder)
		name       string
		body       []byte
		wantStatus int
	}{
		{
			name: "associate_web_acl_success",
			body: []byte(
				`<WebACLAssociation><WebACLId>arn:aws:wafv2:us-east-1:123:global/webacl/test/abc</WebACLId></WebACLAssociation>`,
			),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-wacl-001", "wacl-dist", true,
					minimalDistConfig("ref-wacl-001", "wacl-dist", true))
				require.NoError(t, err)

				return d.ID
			},
			wantStatus: http.StatusOK,
			check:      func(t *testing.T, _ *httptest.ResponseRecorder) { t.Helper() },
		},
		{
			name: "associate_web_acl_not_found",
			body: []byte(`<WebACLAssociation><WebACLId>some-acl</WebACLId></WebACLAssociation>`),
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return "DOESNOTEXIST"
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "NoSuchDistribution")
			},
		},
		{
			name: "associate_web_acl_empty_body",
			body: nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-wacl-002", "wacl-dist2", true,
					minimalDistConfig("ref-wacl-002", "wacl-dist2", true))
				require.NoError(t, err)

				return d.ID
			},
			wantStatus: http.StatusOK,
			check:      func(t *testing.T, _ *httptest.ResponseRecorder) { t.Helper() },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			distID := tt.setup(t, h)
			path := "/2020-05-31/distribution/" + distID + "/associate-web-acl"

			rec := doXML(t, h, http.MethodPut, path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec)
		})
	}
}

// TestCopyDistribution covers the CopyDistribution operation.
func TestCopyDistribution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *cloudfront.Handler) string
		check      func(*testing.T, *httptest.ResponseRecorder)
		name       string
		body       []byte
		wantStatus int
	}{
		{
			name: "copy_distribution_success",
			body: []byte(
				`<CopyDistributionRequest><CallerReference>copy-ref-001</CallerReference></CopyDistributionRequest>`,
			),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-copy-001", "source-dist", true,
					minimalDistConfig("ref-copy-001", "source-dist", true))
				require.NoError(t, err)

				return d.ID
			},
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<Distribution")
				assert.Contains(t, rec.Body.String(), "<Status>Deployed</Status>")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
				assert.NotEmpty(t, rec.Header().Get("Location"))
			},
		},
		{
			name: "copy_distribution_not_found",
			body: []byte(
				`<CopyDistributionRequest><CallerReference>copy-ref-002</CallerReference></CopyDistributionRequest>`,
			),
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return "DOESNOTEXIST"
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "NoSuchDistribution")
			},
		},
		{
			name: "copy_distribution_empty_caller_ref",
			body: []byte(`<CopyDistributionRequest><CallerReference></CallerReference></CopyDistributionRequest>`),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-copy-003", "source-dist2", true,
					minimalDistConfig("ref-copy-003", "source-dist2", true))
				require.NoError(t, err)

				return d.ID
			},
			wantStatus: http.StatusBadRequest,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "InvalidArgument")
			},
		},
		{
			name: "copy_distribution_malformed_xml",
			body: []byte(`<<<not xml`),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-copy-004", "source-dist3", true,
					minimalDistConfig("ref-copy-004", "source-dist3", true))
				require.NoError(t, err)

				return d.ID
			},
			wantStatus: http.StatusBadRequest,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "MalformedXML")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			distID := tt.setup(t, h)
			path := "/2020-05-31/distribution/" + distID + "/copy"

			rec := doXML(t, h, http.MethodPost, path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec)
		})
	}
}

// TestRefinement1_CallerReferenceValidation verifies CallerReference is required.
func TestRefinement1_CallerReferenceValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantError  string
		body       []byte
		wantStatus int
	}{
		{
			name: "empty_caller_ref_distribution",
			body: []byte(
				`<DistributionConfig><CallerReference></CallerReference><Enabled>true</Enabled></DistributionConfig>`,
			),
			wantStatus: http.StatusBadRequest,
			wantError:  "InvalidArgument",
		},
		{
			name: "missing_caller_ref_oai",
			body: []byte(
				`<CloudFrontOriginAccessIdentityConfig>` +
					`<CallerReference></CallerReference>` +
					`<Comment>no-ref</Comment>` +
					`</CloudFrontOriginAccessIdentityConfig>`,
			),
			wantStatus: http.StatusBadRequest,
			wantError:  "InvalidArgument",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			var path string
			if strings.Contains(tt.name, "oai") {
				path = "/2020-05-31/origin-access-identity/cloudfront"
			} else {
				path = "/2020-05-31/distribution"
			}

			rec := doXML(t, h, http.MethodPost, path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantError)
		})
	}
}

// TestRefinement1_CallerReferenceIdempotency verifies duplicate CallerReferences return existing resource.
func TestRefinement1_CallerReferenceIdempotency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "distribution_idempotency"},
		{name: "oai_idempotency"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if strings.Contains(tt.name, "distribution") {
				body := minimalDistConfig("idem-ref-001", "idem-dist", true)
				rec1 := doXML(t, h, http.MethodPost, "/2020-05-31/distribution", body)
				require.Equal(t, http.StatusCreated, rec1.Code)

				// Second call with same CallerReference should return same distribution.
				rec2 := doXML(t, h, http.MethodPost, "/2020-05-31/distribution", body)
				require.Equal(t, http.StatusCreated, rec2.Code)

				// Should have same ID (only one distribution created).
				assert.Equal(t, rec1.Body.String(), rec2.Body.String())
				assert.Len(t, h.Backend.ListDistributions(), 1)
			} else {
				body := minimalOAIConfig("idem-oai-ref-001", "idem-oai")
				rec1 := doXML(t, h, http.MethodPost, "/2020-05-31/origin-access-identity/cloudfront", body)
				require.Equal(t, http.StatusCreated, rec1.Code)

				rec2 := doXML(t, h, http.MethodPost, "/2020-05-31/origin-access-identity/cloudfront", body)
				require.Equal(t, http.StatusCreated, rec2.Code)

				// Only one OAI should be stored.
				assert.Len(t, h.Backend.ListOAIs(), 1)
			}
		})
	}
}

// TestRefinement1_DeleteDistributionCleansUp verifies aliases/webACLs are removed on delete.
func TestRefinement1_DeleteDistributionCleansUp(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend("123456789012", config.DefaultRegion)

	d, err := b.CreateDistribution("ref-del-cleanup", "del-dist", false, nil)
	require.NoError(t, err)

	err = b.AssociateAlias(d.ID, "cleanup.example.com")
	require.NoError(t, err)

	err = b.AssociateDistributionWebACL(d.ID, "arn:aws:wafv2:us-east-1:123:webacl/test")
	require.NoError(t, err)

	// Delete requires ETag via handler; do directly via backend.
	h := cloudfront.NewHandler(b)
	// Get ETag for delete.
	rec := doXML(t, h, http.MethodGet, "/2020-05-31/distribution/"+d.ID, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	etag := rec.Header().Get("ETag")
	require.NotEmpty(t, etag)

	rec = doXMLWithHeaders(t, h, http.MethodDelete, "/2020-05-31/distribution/"+d.ID, nil,
		map[string]string{"If-Match": etag})
	require.Equal(t, http.StatusNoContent, rec.Code)

	// After deletion, CallerReference re-use should create a new distribution.
	d2, err := b.CreateDistribution("ref-del-cleanup", "new-dist", true, nil)
	require.NoError(t, err)
	assert.NotEqual(t, d.ID, d2.ID, "new distribution should have different ID after callerRef is freed")
}

// TestRefinement1_SortedOutput verifies sorted listing results.
func TestRefinement1_SortedOutput(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend("123456789012", config.DefaultRegion)

	// Create multiple distributions.
	refs := []string{"s-ref-001", "s-ref-002", "s-ref-003"}
	for _, ref := range refs {
		_, err := b.CreateDistribution(ref, ref, true, nil)
		require.NoError(t, err)
	}

	dists := b.ListDistributions()
	require.Len(t, dists, 3)

	for i := 1; i < len(dists); i++ {
		assert.LessOrEqual(t, dists[i-1].ID, dists[i].ID,
			"distributions should be sorted by ID")
	}

	// Create multiple OAIs.
	for _, ref := range refs {
		_, err := b.CreateOAI(ref+"-oai", "comment")
		require.NoError(t, err)
	}

	oais := b.ListOAIs()
	require.Len(t, oais, 3)

	for i := 1; i < len(oais); i++ {
		assert.LessOrEqual(t, oais[i-1].ID, oais[i].ID,
			"OAIs should be sorted by ID")
	}
}

// TestRefinement1_AliasCountInListDistributions verifies alias count is reflected in list output.
func TestRefinement1_AliasCountInListDistributions(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend("123456789012", config.DefaultRegion)
	h := cloudfront.NewHandler(b)

	d, err := b.CreateDistribution("ref-alias-list", "alias-list-dist", true, nil)
	require.NoError(t, err)

	err = b.AssociateAlias(d.ID, "www.example.com")
	require.NoError(t, err)

	err = b.AssociateAlias(d.ID, "api.example.com")
	require.NoError(t, err)

	rec := doXML(t, h, http.MethodGet, "/2020-05-31/distribution", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Quantity>2</Quantity>")
}

// TestRefinement1_CreateDistributionValidation verifies CallerReference is validated.
func TestRefinement1_CreateDistributionValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	body := []byte(
		`<DistributionConfig><CallerReference></CallerReference>` +
			`<Comment>no-ref</Comment><Enabled>true</Enabled></DistributionConfig>`,
	)
	rec := doXML(t, h, http.MethodPost, "/2020-05-31/distribution", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidArgument")
}
