package acmpca_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acmpca"
)

func TestInMemoryBackend_TagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		name     string
		wantTags int
	}{
		{
			name:     "add and list tags",
			tags:     map[string]string{"env": "test", "team": "platform"},
			wantTags: 2,
		},
		{
			name:     "no tags",
			tags:     map[string]string{},
			wantTags: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			ca, err := b.CreateCertificateAuthority(
				context.Background(),
				"ROOT",
				acmpca.CertificateAuthorityConfiguration{
					Subject: acmpca.CertificateAuthoritySubject{CommonName: "Tag CA"},
				},
			)
			require.NoError(t, err)

			h := acmpca.NewHandler(b)

			if len(tt.tags) > 0 {
				h.SetTagsForTest(ca.ARN, tt.tags)
			}

			got := h.GetTagsForTest(ca.ARN)
			assert.Len(t, got, tt.wantTags)
		})
	}
}

// ---- Tag operations ----

func TestACMPCAHandler_TagAndUntagCA(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()
	caARN := createHandlerCA(t, h)

	// TagCertificateAuthority
	rec := doACMPCARequest(t, h, "TagCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": caARN,
		"Tags": []map[string]any{
			{"Key": "env", "Value": "test"},
			{"Key": "team", "Value": "infra"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// TagCertificateAuthority - malformed JSON should fail
	rec = doACMPCARequest(t, h, "TagCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": "nonexistent",
		"Tags": []map[string]any{
			{"Key": "env", "Value": "test"},
		},
	})
	assert.NotEqual(t, http.StatusOK, rec.Code)

	// ListTags
	rec = doACMPCARequest(t, h, "ListTags", map[string]any{
		"CertificateAuthorityArn": caARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseACMPCAResponse(t, rec)
	tags, _ := resp["Tags"].([]any)
	assert.Len(t, tags, 2)

	// ListTags - nonexistent CA
	rec = doACMPCARequest(t, h, "ListTags", map[string]any{
		"CertificateAuthorityArn": "nonexistent",
	})
	assert.NotEqual(t, http.StatusOK, rec.Code)

	// UntagCertificateAuthority
	rec = doACMPCARequest(t, h, "UntagCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": caARN,
		"Tags": []map[string]any{
			{"Key": "env", "Value": "test"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// UntagCertificateAuthority - nonexistent CA
	rec = doACMPCARequest(t, h, "UntagCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": "nonexistent",
		"Tags": []map[string]any{
			{"Key": "env", "Value": "test"},
		},
	})
	assert.NotEqual(t, http.StatusOK, rec.Code)

	// ListTags after untag
	rec = doACMPCARequest(t, h, "ListTags", map[string]any{
		"CertificateAuthorityArn": caARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp = parseACMPCAResponse(t, rec)
	tags, _ = resp["Tags"].([]any)
	assert.Len(t, tags, 1)
}

// TestACMPCA_CreateCertificateAuthority_WithTags verifies that tags
// provided at creation time are stored and retrievable via ListTags.
func TestACMPCA_CreateCertificateAuthority_WithTags(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()

	rec := doACMPCARequest(t, h, "CreateCertificateAuthority", map[string]any{
		"CertificateAuthorityConfiguration": map[string]any{
			"Subject":          map[string]any{"CommonName": "Tagged CA"},
			"KeyAlgorithm":     "EC_prime256v1",
			"SigningAlgorithm": "SHA256WITHECDSA",
		},
		"CertificateAuthorityType": "ROOT",
		"Tags": []map[string]string{
			{"Key": "env", "Value": "prod"},
			{"Key": "team", "Value": "security"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseACMPCAResponse(t, rec)
	caARN, _ := resp["CertificateAuthorityArn"].(string)
	require.NotEmpty(t, caARN)

	tagsRec := doACMPCARequest(t, h, "ListTags", map[string]any{
		"CertificateAuthorityArn": caARN,
	})
	require.Equal(t, http.StatusOK, tagsRec.Code)

	tagsResp := parseACMPCAResponse(t, tagsRec)
	tags, _ := tagsResp["Tags"].([]any)
	assert.Len(t, tags, 2)
}

// TestACMPCA_ListTagsForCertificateAuthority verifies that the
// ListTagsForCertificateAuthority alias works identically to ListTags.
func TestACMPCA_ListTagsForCertificateAuthority(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()
	caARN := createHandlerCA(t, h)

	h.SetTagsForTest(caARN, map[string]string{"k1": "v1"})

	for _, op := range []string{"ListTags", "ListTagsForCertificateAuthority"} {
		rec := doACMPCARequest(t, h, op, map[string]any{
			"CertificateAuthorityArn": caARN,
		})
		require.Equal(t, http.StatusOK, rec.Code, "op=%s", op)
		resp := parseACMPCAResponse(t, rec)
		tags, _ := resp["Tags"].([]any)
		assert.Len(t, tags, 1, "op=%s", op)
	}
}

// TestACMPCAHandler_TagCertificateAuthority_NotFound verifies that tagging a
// non-existent CA returns ResourceNotFoundException.
func TestACMPCAHandler_TagCertificateAuthority_NotFound(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()

	rec := doACMPCARequest(t, h, "TagCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": "arn:aws:acm-pca:us-east-1:000000000000:certificate-authority/nonexistent",
		"Tags":                    []map[string]string{{"Key": "env", "Value": "test"}},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	resp := parseACMPCAResponse(t, rec)
	assert.Equal(t, "ResourceNotFoundException", resp["__type"])
}
