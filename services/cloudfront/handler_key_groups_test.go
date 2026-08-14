package cloudfront_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestPublicKeyReferentialIntegrity verifies that a public key
// referenced by a key group or an FLE profile cannot be deleted, and that the
// reference must be removed first.
func TestPublicKeyReferentialIntegrity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *cloudfront.InMemoryBackend)
		name string
	}{
		{
			name: "in_use_by_key_group_blocks_delete",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				pk, err := b.CreatePublicKey("cr1", "pk1", "", testRSA2048PublicKeyPEM)
				require.NoError(t, err)
				_, err = b.CreateKeyGroup("kg1", "", []string{pk.ID})
				require.NoError(t, err)

				err = b.DeletePublicKey(pk.ID)
				require.Error(t, err)
				assert.ErrorIs(t, err, cloudfront.ErrPublicKeyInUse)
			},
		},
		{
			name: "removing_key_group_ref_allows_delete",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				pk, err := b.CreatePublicKey("cr2", "pk2", "", testRSA2048PublicKeyPEM)
				require.NoError(t, err)
				kg, err := b.CreateKeyGroup("kg2", "", []string{pk.ID})
				require.NoError(t, err)

				require.ErrorIs(t, b.DeletePublicKey(pk.ID), cloudfront.ErrPublicKeyInUse)

				_, err = b.UpdateKeyGroup(kg.ID, "kg2", "", nil)
				require.NoError(t, err)
				require.NoError(t, b.DeletePublicKey(pk.ID))
			},
		},
		{
			name: "in_use_by_fle_profile_blocks_delete",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				pk, err := b.CreatePublicKey("cr3", "pk3", "", testRSA2048PublicKeyPEM)
				require.NoError(t, err)
				_, err = b.CreateFieldLevelEncryptionProfile("prof3", "", []cloudfront.EncryptionEntity{
					{PublicKeyID: pk.ID, ProviderID: "prov", FieldPatterns: []string{"secret"}},
				})
				require.NoError(t, err)

				err = b.DeletePublicKey(pk.ID)
				require.ErrorIs(t, err, cloudfront.ErrPublicKeyInUse)
			},
		},
		{
			name: "unreferenced_public_key_deletes",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				pk, err := b.CreatePublicKey("cr4", "pk4", "", testRSA2048PublicKeyPEM)
				require.NoError(t, err)
				require.NoError(t, b.DeletePublicKey(pk.ID))
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

// TestPublicKeyInUseHTTP verifies the HTTP surface returns 409
// PublicKeyInUse when deleting a referenced public key.
func TestPublicKeyInUseHTTP(t *testing.T) {
	t.Parallel()

	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	createBody := `<PublicKeyConfig><CallerReference>cr-http</CallerReference>` +
		`<Name>pk-http</Name><EncodedKey>` + testRSA2048PublicKeyPEM +
		`</EncodedKey><Comment>c</Comment></PublicKeyConfig>`
	createRec := cfRequest(t, h, http.MethodPost, prefix+"public-key", createBody)
	require.Equal(t, http.StatusCreated, createRec.Code)
	pkID := extractXMLID(t, createRec.Body.String())
	require.NotEmpty(t, pkID)
	pkETag := createRec.Header().Get("ETag")

	kgBody := `<KeyGroupConfig><Name>kg-http</Name><Comment></Comment><Items>` +
		`<PublicKey>` + pkID + `</PublicKey></Items></KeyGroupConfig>`
	kgRec := cfRequest(t, h, http.MethodPost, prefix+"key-group", kgBody)
	require.Equal(t, http.StatusCreated, kgRec.Code)

	delRec := cfRequestWithBodyHeaders(t, h, http.MethodDelete, prefix+"public-key/"+pkID, "",
		map[string]string{"If-Match": pkETag})
	assert.Equal(t, http.StatusConflict, delRec.Code)
	assert.Contains(t, delRec.Body.String(), "PublicKeyInUse")
}

func TestDeletePublicKeyRequiresIfMatch(t *testing.T) {
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

			h := newTestHandler(t)
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

func TestDeleteKeyGroupRequiresIfMatch(t *testing.T) {
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

			h := newTestHandler(t)
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

// TestPublicKeyPEMValidation verifies PEM-encoded public keys are validated.
func TestPublicKeyPEMValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name: "valid_rsa_key_accepted",
			body: fmt.Sprintf(`<PublicKeyConfig>
				<CallerReference>ref-1</CallerReference>
				<Name>valid-key</Name>
				<EncodedKey>%s</EncodedKey>
			</PublicKeyConfig>`, testRSA2048PublicKeyPEM),
			wantCode: http.StatusCreated,
		},
		{
			name: "invalid_pem_rejected",
			body: `<PublicKeyConfig>
				<CallerReference>ref-2</CallerReference>
				<Name>bad-key</Name>
				<EncodedKey>not-a-pem-key</EncodedKey>
			</PublicKeyConfig>`,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newAuditBackend(t)
			h := cloudfront.NewHandler(b)
			rec := doReq(t, h, http.MethodPost, "/2020-05-31/public-key", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
		})
	}
}

// TestKeyGroupItemValidation verifies key groups reject items that aren't valid public key IDs.
func TestKeyGroupItemValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		items    []string
		wantCode int
	}{
		{
			name:     "nonexistent_key_id_rejected",
			items:    []string{"pk-doesnotexist"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newAuditBackend(t)
			h := cloudfront.NewHandler(b)

			var sb strings.Builder
			for _, id := range tt.items {
				fmt.Fprintf(&sb, "<PublicKey>%s</PublicKey>", id)
			}
			itemsXML := sb.String()

			body := fmt.Sprintf(`<KeyGroupConfig>
				<Name>test-kg</Name>
				<Items>%s</Items>
			</KeyGroupConfig>`, itemsXML)

			rec := doReq(t, h, http.MethodPost, "/2020-05-31/key-group", body)
			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
		})
	}
}

// TestPublicKeyCRUD covers the full Public Key lifecycle via the HTTP handler.
func TestPublicKeyCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*testing.T, *cloudfront.Handler) string
		check       func(*testing.T, *httptest.ResponseRecorder, string)
		headersFunc func(*testing.T, *cloudfront.Handler, string) map[string]string
		name        string
		method      string
		path        string
		body        []byte
		wantStatus  int
	}{
		{
			name:   "create_public_key",
			method: http.MethodPost,
			path:   "/2020-05-31/public-key",
			body: []byte(
				`<PublicKeyConfig>` +
					`<CallerReference>pk-ref-1</CallerReference>` +
					`<Name>my-public-key</Name>` +
					`<Comment>test</Comment>` +
					`<EncodedKey>` + testRSA2048PublicKeyPEM + `</EncodedKey>` +
					`</PublicKeyConfig>`,
			),
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<PublicKey")
				assert.NotEmpty(t, rec.Header().Get("Location"))
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "list_public_keys",
			method: http.MethodGet,
			path:   "/2020-05-31/public-key",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreatePublicKey("pk-list-ref", "list-pk", "comment", testRSA2048PublicKeyPEM)
				require.NoError(t, err)

				return ""
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<PublicKeyList")
				assert.Contains(t, rec.Body.String(), "<Quantity>1</Quantity>")
			},
		},
		{
			name:   "get_public_key",
			method: http.MethodGet,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				pk, err := h.Backend.CreatePublicKey("pk-get-ref", "get-pk", "comment", testRSA2048PublicKeyPEM)
				require.NoError(t, err)

				return "/2020-05-31/public-key/" + pk.ID
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<PublicKey")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "get_public_key_config",
			method: http.MethodGet,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				pk, err := h.Backend.CreatePublicKey(
					"pk-get-cfg-ref",
					"get-pk-config",
					"comment",
					testRSA2048PublicKeyPEM,
				)
				require.NoError(t, err)

				return "/2020-05-31/public-key/" + pk.ID + "/config"
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<PublicKey")
			},
		},
		{
			name:   "update_public_key",
			method: http.MethodPut,
			path:   "",
			body:   []byte(`<PublicKeyConfig><Comment>updated comment</Comment></PublicKeyConfig>`),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				pk, err := h.Backend.CreatePublicKey("pk-upd-ref", "upd-pk", "original", testRSA2048PublicKeyPEM)
				require.NoError(t, err)

				return "/2020-05-31/public-key/" + pk.ID + "/config"
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<PublicKey")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "delete_public_key",
			method: http.MethodDelete,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				pk, err := h.Backend.CreatePublicKey("pk-del-ref", "del-pk", "delete me", testRSA2048PublicKeyPEM)
				require.NoError(t, err)

				return "/2020-05-31/public-key/" + pk.ID
			},
			headersFunc: func(t *testing.T, h *cloudfront.Handler, path string) map[string]string {
				t.Helper()
				parts := strings.Split(strings.TrimRight(path, "/"), "/")
				id := parts[len(parts)-1]
				pk, err := h.Backend.GetPublicKey(id)
				require.NoError(t, err)

				return map[string]string{"If-Match": pk.ETag}
			},
			wantStatus: http.StatusNoContent,
			check:      nil,
		},
		{
			name:   "get_public_key_not_found",
			method: http.MethodGet,
			path:   "/2020-05-31/public-key/doesnotexist",
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

			h := newTestHandler(t)
			path := tt.path
			if tt.setup != nil {
				if p := tt.setup(t, h); p != "" {
					path = p
				}
			}

			var hdrs map[string]string
			if tt.headersFunc != nil {
				hdrs = tt.headersFunc(t, h, path)
			}
			rec := doXMLWithHeaders(t, h, tt.method, path, tt.body, hdrs)

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.check != nil {
				tt.check(t, rec, path)
			}
		})
	}
}

// TestUpdatePublicKey_RealClient drives the real aws-sdk-go-v2 client to prove
// UpdatePublicKey is reachable. Real UpdatePublicKey PUTs to
// /public-key/{Id}/config (cloudfront@v1.67.4 serializers.go:
// awsRestxml_serializeOpUpdatePublicKey's SplitURI); gopherstack previously
// bound UpdatePublicKey to the bare /public-key/{Id} path instead, so every
// real client call 404'd (gopherstack-o31x).
func TestUpdatePublicKey_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCloudFrontClient(t, h)

	created, err := client.CreatePublicKey(t.Context(), &cfsdk.CreatePublicKeyInput{
		PublicKeyConfig: &types.PublicKeyConfig{
			CallerReference: aws.String("real-client-pk"),
			EncodedKey:      aws.String(testRSA2048PublicKeyPEM),
			Name:            aws.String("real-client-pk"),
			Comment:         aws.String("original"),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created.PublicKey)

	updated, err := client.UpdatePublicKey(t.Context(), &cfsdk.UpdatePublicKeyInput{
		Id: created.PublicKey.Id,
		PublicKeyConfig: &types.PublicKeyConfig{
			CallerReference: created.PublicKey.PublicKeyConfig.CallerReference,
			EncodedKey:      created.PublicKey.PublicKeyConfig.EncodedKey,
			Name:            created.PublicKey.PublicKeyConfig.Name,
			Comment:         aws.String("updated"),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, updated.PublicKey)
	assert.Equal(t, "updated", aws.ToString(updated.PublicKey.PublicKeyConfig.Comment))
}

// TestListKeyGroups_RealClient drives ListKeyGroups through the real aws-sdk-go-v2 CloudFront
// client. The real deserializer (awsRestxml_deserializeDocumentKeyGroupSummary, case
// "KeyGroup") wraps a single nested KeyGroup child in each KeyGroupSummary; a KeyGroupSummary
// with Id/Name/Comment flattened directly onto it (the pre-fix shape, confirmed by
// hand-reverting) decodes to a KeyGroupSummary.KeyGroup that is nil for every item -- the right
// item count, entirely blank content.
func TestListKeyGroups_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateKeyGroup("real-client-kg", "kg comment", nil)
	require.NoError(t, err)

	client := newTestCloudFrontClient(t, h)

	listed, err := client.ListKeyGroups(t.Context(), &cfsdk.ListKeyGroupsInput{})
	require.NoError(t, err)
	require.NotNil(t, listed.KeyGroupList)
	require.Len(t, listed.KeyGroupList.Items, 1)
	require.NotNil(t, listed.KeyGroupList.Items[0].KeyGroup)
	assert.Equal(t, "real-client-kg", aws.ToString(listed.KeyGroupList.Items[0].KeyGroup.KeyGroupConfig.Name))
}

// TestKeyGroupCRUD covers the full Key Group lifecycle via the HTTP handler.
func TestKeyGroupCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*testing.T, *cloudfront.Handler) string
		check       func(*testing.T, *httptest.ResponseRecorder, string)
		headersFunc func(*testing.T, *cloudfront.Handler, string) map[string]string
		name        string
		method      string
		path        string
		body        []byte
		wantStatus  int
	}{
		{
			name:   "create_key_group",
			method: http.MethodPost,
			path:   "/2020-05-31/key-group",
			body: []byte(
				`<KeyGroupConfig><Name>my-key-group</Name><Comment>test</Comment><Items></Items></KeyGroupConfig>`,
			),
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<KeyGroup")
				assert.NotEmpty(t, rec.Header().Get("Location"))
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "list_key_groups",
			method: http.MethodGet,
			path:   "/2020-05-31/key-group",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateKeyGroup("list-key-group", "comment", nil)
				require.NoError(t, err)

				return ""
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<KeyGroupList")
				assert.Contains(t, rec.Body.String(), "<Quantity>1</Quantity>")
			},
		},
		{
			name:   "get_key_group",
			method: http.MethodGet,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				pk, err := h.Backend.CreatePublicKey("pk-kg-ref", "pk-for-kg", "", testRSA2048PublicKeyPEM)
				require.NoError(t, err)
				kg, err := h.Backend.CreateKeyGroup("get-key-group", "comment", []string{pk.ID})
				require.NoError(t, err)

				return "/2020-05-31/key-group/" + kg.ID
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<KeyGroup")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "get_key_group_config",
			method: http.MethodGet,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				kg, err := h.Backend.CreateKeyGroup("get-key-group-cfg", "comment", nil)
				require.NoError(t, err)

				return "/2020-05-31/key-group/" + kg.ID + "/config"
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<KeyGroup")
			},
		},
		{
			name:   "update_key_group",
			method: http.MethodPut,
			path:   "",
			body: []byte(
				`<KeyGroupConfig><Name>updated-kg</Name><Comment>updated</Comment><Items></Items></KeyGroupConfig>`,
			),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				kg, err := h.Backend.CreateKeyGroup("old-key-group", "original", nil)
				require.NoError(t, err)

				return "/2020-05-31/key-group/" + kg.ID
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<KeyGroup")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "delete_key_group",
			method: http.MethodDelete,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				kg, err := h.Backend.CreateKeyGroup("del-key-group", "delete me", nil)
				require.NoError(t, err)

				return "/2020-05-31/key-group/" + kg.ID
			},
			headersFunc: func(t *testing.T, h *cloudfront.Handler, path string) map[string]string {
				t.Helper()
				parts := strings.Split(strings.TrimRight(path, "/"), "/")
				id := parts[len(parts)-1]
				kg, err := h.Backend.GetKeyGroup(id)
				require.NoError(t, err)

				return map[string]string{"If-Match": kg.ETag}
			},
			wantStatus: http.StatusNoContent,
			check:      nil,
		},
		{
			name:   "get_key_group_not_found",
			method: http.MethodGet,
			path:   "/2020-05-31/key-group/doesnotexist",
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

			h := newTestHandler(t)
			path := tt.path
			if tt.setup != nil {
				if p := tt.setup(t, h); p != "" {
					path = p
				}
			}

			var hdrs map[string]string
			if tt.headersFunc != nil {
				hdrs = tt.headersFunc(t, h, path)
			}
			rec := doXMLWithHeaders(t, h, tt.method, path, tt.body, hdrs)

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.check != nil {
				tt.check(t, rec, path)
			}
		})
	}
}

// TestInMemoryBackend_PublicKey tests Public Key backend operations directly.
func TestInMemoryBackend_PublicKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(*testing.T, *cloudfront.InMemoryBackend)
		name string
	}{
		{
			name: "create_get_list_update_delete",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				pk, err := b.CreatePublicKey("pk-be-ref", "pk-be-name", "comment", testRSA2048PublicKeyPEM)
				require.NoError(t, err)
				assert.NotEmpty(t, pk.ID)

				got, err := b.GetPublicKey(pk.ID)
				require.NoError(t, err)
				assert.Equal(t, "comment", got.Comment)

				list := b.ListPublicKeys()
				assert.Len(t, list, 1)

				updated, err := b.UpdatePublicKey(pk.ID, "updated comment")
				require.NoError(t, err)
				assert.Equal(t, "updated comment", updated.Comment)

				require.NoError(t, b.DeletePublicKey(pk.ID))
				_, err = b.GetPublicKey(pk.ID)
				require.Error(t, err)
			},
		},
		{
			name: "get_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.GetPublicKey("doesnotexist")
				require.Error(t, err)
			},
		},
		{
			name: "update_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.UpdatePublicKey("doesnotexist", "comment")
				require.Error(t, err)
			},
		},
		{
			name: "delete_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				err := b.DeletePublicKey("doesnotexist")
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
			tt.run(t, b)
		})
	}
}

// TestInMemoryBackend_KeyGroup tests Key Group backend operations directly.
func TestInMemoryBackend_KeyGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(*testing.T, *cloudfront.InMemoryBackend)
		name string
	}{
		{
			name: "create_get_list_update_delete",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				pk1, err := b.CreatePublicKey("kg-pk1-ref", "kg-pk1", "", testRSA2048PublicKeyPEM)
				require.NoError(t, err)
				pk2, err := b.CreatePublicKey("kg-pk2-ref", "kg-pk2", "", testRSA2048PublicKeyPEM)
				require.NoError(t, err)

				kg, err := b.CreateKeyGroup("kg-name", "comment", []string{pk1.ID})
				require.NoError(t, err)
				assert.NotEmpty(t, kg.ID)

				got, err := b.GetKeyGroup(kg.ID)
				require.NoError(t, err)
				assert.Equal(t, "kg-name", got.Name)
				assert.Equal(t, []string{pk1.ID}, got.Items)

				list := b.ListKeyGroups()
				assert.Len(t, list, 1)

				updated, err := b.UpdateKeyGroup(kg.ID, "kg-name-new", "updated", []string{pk2.ID})
				require.NoError(t, err)
				assert.Equal(t, "updated", updated.Comment)

				require.NoError(t, b.DeleteKeyGroup(kg.ID))
				_, err = b.GetKeyGroup(kg.ID)
				require.Error(t, err)
			},
		},
		{
			name: "get_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.GetKeyGroup("doesnotexist")
				require.Error(t, err)
			},
		},
		{
			name: "update_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.UpdateKeyGroup("doesnotexist", "name", "comment", nil)
				require.Error(t, err)
			},
		},
		{
			name: "delete_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				err := b.DeleteKeyGroup("doesnotexist")
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
			tt.run(t, b)
		})
	}
}
