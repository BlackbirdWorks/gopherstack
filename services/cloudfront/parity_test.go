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

func TestParity_DistributionCreatesAsInProgress(t *testing.T) {
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
			assert.Equal(t, "InProgress", d.Status)
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

func TestParity_CopyDistributionCreatesAsInProgress(t *testing.T) {
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
			assert.Equal(t, "InProgress", cp.Status, tc.name)
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
