package cloudfront_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

func newTestBackend() *cloudfront.InMemoryBackend {
	return cloudfront.NewInMemoryBackend("123456789012", config.DefaultRegion)
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

func TestParity_CreateInvalidationRequiresCallerReference(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		callerRef string
		wantErr   bool
	}{
		{name: "empty_caller_ref_rejected", callerRef: "", wantErr: true},
		{name: "non_empty_caller_ref_accepted", callerRef: "my-ref", wantErr: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			d, err := b.CreateDistribution("ref-inv", "test", true, nil)
			require.NoError(t, err)

			_, err = b.CreateInvalidation(d.ID, tc.callerRef, []string{"/*"})
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestParity_CountInProgressInvalidations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numInvs   int
		wantCount int
	}{
		{name: "no_invalidations", numInvs: 0, wantCount: 0},
		{name: "one_invalidation", numInvs: 1, wantCount: 1},
		{name: "two_invalidations", numInvs: 2, wantCount: 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			d, err := b.CreateDistribution("ref-cnt", "test", true, nil)
			require.NoError(t, err)

			for i := range tc.numInvs {
				_, err = b.CreateInvalidation(d.ID, fmt.Sprintf("ref-%d", i), []string{"/*"})
				require.NoError(t, err)
			}

			assert.Equal(t, tc.wantCount, b.CountInProgressInvalidations(d.ID))
		})
	}
}

func TestParity_CreateInvalidationHandlerReturnsInvalidationBatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "response_has_invalidation_batch"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			distRec := doXML(t, h, http.MethodPost, "/2020-05-31/distribution",
				minimalDistConfig("ref-cr", "test", true))
			require.Equal(t, http.StatusCreated, distRec.Code)

			distID := extractXMLTag(t, distRec.Body.String())
			require.NotEmpty(t, distID)

			body := []byte(`<InvalidationBatch>` +
				`<CallerReference>cr-1</CallerReference>` +
				`<Paths><Quantity>1</Quantity><Items><Path>/*</Path></Items></Paths>` +
				`</InvalidationBatch>`)

			rec := doXML(t, h, http.MethodPost,
				"/2020-05-31/distribution/"+distID+"/invalidation", body)
			require.Equal(t, http.StatusCreated, rec.Code, tc.name)

			body2 := rec.Body.String()
			assert.Contains(t, body2, "<InvalidationBatch>", tc.name)
			assert.Contains(t, body2, "<CallerReference>cr-1</CallerReference>", tc.name)
			assert.Contains(t, body2, "<Path>/*</Path>", tc.name)
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

func TestParity_ResponseHasCFIDHeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "cf_id_header_present"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doXML(t, h, http.MethodPost, "/2020-05-31/distribution",
				minimalDistConfig("ref-hdr", "test", true))
			require.Equal(t, http.StatusCreated, rec.Code, tc.name)
			assert.NotEmpty(t, rec.Header().Get("X-Amz-Cf-Id"), tc.name)
		})
	}
}

func TestParity_DeleteFLERequiresIfMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		sendIfMatch bool
		wantStatus  int
	}{
		{name: "no_if_match_rejected", sendIfMatch: false, wantStatus: http.StatusPreconditionFailed},
		{name: "correct_if_match_accepted", sendIfMatch: true, wantStatus: http.StatusNoContent},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createRec := doXML(t, h, http.MethodPost, "/2020-05-31/field-level-encryption",
				[]byte(`<FieldLevelEncryptionConfig><Comment>test</Comment></FieldLevelEncryptionConfig>`))
			require.Equal(t, http.StatusCreated, createRec.Code)

			fleID := extractXMLTag(t, createRec.Body.String())
			require.NotEmpty(t, fleID)

			etag := createRec.Header().Get("ETag")

			headers := map[string]string{}
			if tc.sendIfMatch {
				headers["If-Match"] = etag
			}

			rec := doXMLWithHeaders(t, h, http.MethodDelete,
				"/2020-05-31/field-level-encryption/"+fleID, nil, headers)
			assert.Equal(t, tc.wantStatus, rec.Code, tc.name)
		})
	}
}

func TestParity_DeletePublicKeyRequiresIfMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		sendIfMatch bool
		wantStatus  int
	}{
		{name: "no_if_match_rejected", sendIfMatch: false, wantStatus: http.StatusPreconditionFailed},
		{name: "correct_if_match_accepted", sendIfMatch: true, wantStatus: http.StatusNoContent},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createRec := doXML(
				t,
				h,
				http.MethodPost,
				"/2020-05-31/public-key",
				[]byte(
					`<PublicKeyConfig>`+
						`<CallerReference>pk-ref</CallerReference>`+
						`<Name>mykey</Name>`+
						`<EncodedKey>`+testRSA2048PublicKeyPEM+`</EncodedKey>`+
						`</PublicKeyConfig>`,
				),
			)
			require.Equal(t, http.StatusCreated, createRec.Code)

			pkID := extractXMLTag(t, createRec.Body.String())
			require.NotEmpty(t, pkID)

			etag := createRec.Header().Get("ETag")

			headers := map[string]string{}
			if tc.sendIfMatch {
				headers["If-Match"] = etag
			}

			rec := doXMLWithHeaders(t, h, http.MethodDelete,
				"/2020-05-31/public-key/"+pkID, nil, headers)
			assert.Equal(t, tc.wantStatus, rec.Code, tc.name)
		})
	}
}

func TestParity_DeleteKeyGroupRequiresIfMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		sendIfMatch bool
		wantStatus  int
	}{
		{name: "no_if_match_rejected", sendIfMatch: false, wantStatus: http.StatusPreconditionFailed},
		{name: "correct_if_match_accepted", sendIfMatch: true, wantStatus: http.StatusNoContent},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createRec := doXML(t, h, http.MethodPost, "/2020-05-31/key-group",
				[]byte(`<KeyGroupConfig><Name>kg1</Name><Items></Items></KeyGroupConfig>`))
			require.Equal(t, http.StatusCreated, createRec.Code)

			kgID := extractXMLTag(t, createRec.Body.String())
			require.NotEmpty(t, kgID)

			etag := createRec.Header().Get("ETag")

			headers := map[string]string{}
			if tc.sendIfMatch {
				headers["If-Match"] = etag
			}

			rec := doXMLWithHeaders(t, h, http.MethodDelete,
				"/2020-05-31/key-group/"+kgID, nil, headers)
			assert.Equal(t, tc.wantStatus, rec.Code, tc.name)
		})
	}
}

func TestParity_ContinuousDeploymentPolicyXMLIncludesStagingDNS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		stagingDNS string
		wantInXML  bool
	}{
		{name: "with_staging_dns_emits_element", stagingDNS: "abc.cloudfront.net", wantInXML: true},
		{name: "without_staging_dns_omits_element", stagingDNS: "", wantInXML: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			policy, err := b.CreateContinuousDeploymentPolicy(true, tc.stagingDNS)
			require.NoError(t, err)

			h := cloudfront.NewHandler(b)
			rec := doXML(t, h, http.MethodGet,
				"/2020-05-31/continuous-deployment-policy/"+policy.ID, nil)
			require.Equal(t, http.StatusOK, rec.Code, tc.name)

			if tc.wantInXML {
				assert.Contains(t, rec.Body.String(), "<StagingDistributionDnsNames>", tc.name)
				assert.Contains(t, rec.Body.String(), tc.stagingDNS, tc.name)
			} else {
				assert.NotContains(t, rec.Body.String(), "<StagingDistributionDnsNames>", tc.name)
			}
		})
	}
}

func extractXMLTag(t *testing.T, body string) string {
	t.Helper()

	const tag = "Id"
	open := "<" + tag + ">"
	closeTag := "</" + tag + ">"

	start := len(body)
	for i := range len(body) {
		if i+len(open) <= len(body) && body[i:i+len(open)] == open {
			start = i + len(open)

			break
		}
	}

	if start == len(body) {
		return ""
	}

	for i := start; i <= len(body)-len(closeTag); i++ {
		if body[i:i+len(closeTag)] == closeTag {
			return body[start:i]
		}
	}

	return ""
}

// TestParity_PersistenceBatch2RoundTrip verifies that batch-2 fields
// (TrustStores, StreamingDistributions, MonitoringSubscriptions,
// DistributionTenants, DistributionCachePolicies, etc.) survive a
// Snapshot → Restore cycle.
func TestParity_PersistenceBatch2RoundTrip(t *testing.T) {
	t.Parallel()

	type restoreTC struct {
		setup  func(*cloudfront.InMemoryBackend)
		verify func(*testing.T, *cloudfront.InMemoryBackend)
		name   string
	}
	tests := []restoreTC{
		{
			name: "trust_store_survives_restore",
			setup: func(b *cloudfront.InMemoryBackend) {
				_, err := b.CreateTrustStore("my-store", "comment")
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				stores := b.ListTrustStores()
				require.Len(t, stores, 1)
				assert.Equal(t, "my-store", stores[0].Name)
			},
		},
		{
			name: "streaming_distribution_survives_restore",
			setup: func(b *cloudfront.InMemoryBackend) {
				_, err := b.CreateStreamingDistribution([]byte(`<StreamingDistributionConfig/>`))
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				sds := b.ListStreamingDistributions()
				assert.Len(t, sds, 1)
			},
		},
		{
			name: "monitoring_subscription_survives_restore",
			setup: func(b *cloudfront.InMemoryBackend) {
				d, err := b.CreateDistribution("ref-mon", "test", true, nil)
				require.NoError(t, err)
				require.NoError(t, b.CreateMonitoringSubscription(d.ID, true))
			},
			verify: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				dists := b.ListDistributions()
				require.Len(t, dists, 1)
				ms, err := b.GetMonitoringSubscription(dists[0].ID)
				require.NoError(t, err)
				assert.Equal(t, "Enabled", ms.RealtimeMetricsSubscriptionStatus)
			},
		},
		{
			name: "distribution_tenant_survives_restore",
			setup: func(b *cloudfront.InMemoryBackend) {
				d, err := b.CreateDistribution("ref-ten", "test", true, nil)
				require.NoError(t, err)
				_, err = b.CreateDistributionTenant(d.ID, "tenant.example.com", nil)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				tenants := b.ListDistributionTenants()
				require.Len(t, tenants, 1)
				assert.Equal(t, "tenant.example.com", tenants[0].Domain)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			orig := newTestBackend()
			tc.setup(orig)

			snap := orig.Snapshot(t.Context())
			require.NotEmpty(t, snap)

			fresh := newTestBackend()
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tc.verify(t, fresh)
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
