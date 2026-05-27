package cloudfront_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestFieldLevelEncryptionCRUD covers the full FLE lifecycle via the HTTP handler.
func TestFieldLevelEncryptionCRUD(t *testing.T) {
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
			name:   "create_fle",
			method: http.MethodPost,
			path:   "/2020-05-31/field-level-encryption",
			body: []byte(
				`<FieldLevelEncryptionConfig>` +
					`<Comment>test fle</Comment>` +
					`<QueryArgProfileConfig><QueryArgProfiles><Items>` +
					`<QueryArgProfile><Profile>my-fle-config</Profile></QueryArgProfile>` +
					`</Items></QueryArgProfiles></QueryArgProfileConfig>` +
					`</FieldLevelEncryptionConfig>`,
			),
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<FieldLevelEncryption")
				assert.NotEmpty(t, rec.Header().Get("Location"))
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "list_fle",
			method: http.MethodGet,
			path:   "/2020-05-31/field-level-encryption",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateFieldLevelEncryption("list-fle-cfg", "comment")
				require.NoError(t, err)

				return ""
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<FieldLevelEncryptionList")
				assert.Contains(t, rec.Body.String(), "<Quantity>1</Quantity>")
			},
		},
		{
			name:   "get_fle",
			method: http.MethodGet,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				fle, err := h.Backend.CreateFieldLevelEncryption("get-fle-cfg", "a comment")
				require.NoError(t, err)

				return "/2020-05-31/field-level-encryption/" + fle.ID
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<FieldLevelEncryption")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "get_fle_config",
			method: http.MethodGet,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				fle, err := h.Backend.CreateFieldLevelEncryption("get-fle-cfg2", "comment2")
				require.NoError(t, err)

				return "/2020-05-31/field-level-encryption/" + fle.ID + "/config"
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<FieldLevelEncryption")
			},
		},
		{
			name:   "update_fle",
			method: http.MethodPut,
			path:   "",
			body:   []byte(`<FieldLevelEncryptionConfig><Comment>updated</Comment></FieldLevelEncryptionConfig>`),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				fle, err := h.Backend.CreateFieldLevelEncryption("upd-fle-cfg", "original")
				require.NoError(t, err)

				return "/2020-05-31/field-level-encryption/" + fle.ID
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<FieldLevelEncryption")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "get_fle_not_found",
			method: http.MethodGet,
			path:   "/2020-05-31/field-level-encryption/doesnotexist",
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
		{
			name:   "delete_fle",
			method: http.MethodDelete,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				fle, err := h.Backend.CreateFieldLevelEncryption("del-fle-cfg", "delete me")
				require.NoError(t, err)

				return "/2020-05-31/field-level-encryption/" + fle.ID
			},
			wantStatus: http.StatusNoContent,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Empty(t, rec.Body.String())
			},
		},
		{
			name:   "delete_fle_not_found",
			method: http.MethodDelete,
			path:   "/2020-05-31/field-level-encryption/notexist",
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

// TestFieldLevelEncryptionProfileCRUD covers the full FLE Profile lifecycle via the HTTP handler.
func TestFieldLevelEncryptionProfileCRUD(t *testing.T) {
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
			name:   "create_fle_profile",
			method: http.MethodPost,
			path:   "/2020-05-31/field-level-encryption-profile",
			body: []byte(
				`<FieldLevelEncryptionProfileConfig>` +
					`<Name>my-profile</Name>` +
					`<Comment>test</Comment>` +
					`</FieldLevelEncryptionProfileConfig>`,
			),
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<FieldLevelEncryptionProfile")
				assert.NotEmpty(t, rec.Header().Get("Location"))
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "list_fle_profiles",
			method: http.MethodGet,
			path:   "/2020-05-31/field-level-encryption-profile",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateFieldLevelEncryptionProfile("list-fle-profile", "comment")
				require.NoError(t, err)

				return ""
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<FieldLevelEncryptionProfileList")
				assert.Contains(t, rec.Body.String(), "<Quantity>1</Quantity>")
			},
		},
		{
			name:   "get_fle_profile",
			method: http.MethodGet,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				p, err := h.Backend.CreateFieldLevelEncryptionProfile("get-fle-profile", "comment")
				require.NoError(t, err)

				return "/2020-05-31/field-level-encryption-profile/" + p.ID
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<FieldLevelEncryptionProfile")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "get_fle_profile_config",
			method: http.MethodGet,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				p, err := h.Backend.CreateFieldLevelEncryptionProfile("get-fle-profile2", "comment2")
				require.NoError(t, err)

				return "/2020-05-31/field-level-encryption-profile/" + p.ID + "/config"
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<FieldLevelEncryptionProfile")
			},
		},
		{
			name:   "update_fle_profile",
			method: http.MethodPut,
			path:   "",
			body: []byte(
				`<FieldLevelEncryptionProfileConfig>` +
					`<Name>upd-profile</Name>` +
					`<Comment>updated</Comment>` +
					`</FieldLevelEncryptionProfileConfig>`,
			),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				p, err := h.Backend.CreateFieldLevelEncryptionProfile("old-fle-profile", "original")
				require.NoError(t, err)

				return "/2020-05-31/field-level-encryption-profile/" + p.ID
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<FieldLevelEncryptionProfile")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "delete_fle_profile",
			method: http.MethodDelete,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				p, err := h.Backend.CreateFieldLevelEncryptionProfile("del-fle-profile", "delete me")
				require.NoError(t, err)

				return "/2020-05-31/field-level-encryption-profile/" + p.ID
			},
			wantStatus: http.StatusNoContent,
			check:      nil,
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

// TestPublicKeyCRUD covers the full Public Key lifecycle via the HTTP handler.
func TestPublicKeyCRUD(t *testing.T) {
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

// TestKeyGroupCRUD covers the full Key Group lifecycle via the HTTP handler.
func TestKeyGroupCRUD(t *testing.T) {
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

// TestRealtimeLogConfigCRUD covers the full Realtime Log Config lifecycle via the HTTP handler.
func TestRealtimeLogConfigCRUD(t *testing.T) {
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
			name:   "create_realtime_log_config",
			method: http.MethodPost,
			path:   "/2020-05-31/realtime-log-config",
			body: []byte(
				`<RealtimeLogConfig>` +
					`<Name>my-rt-log</Name>` +
					`<SamplingRate>100</SamplingRate>` +
					`<Fields><Field>timestamp</Field></Fields>` +
					`</RealtimeLogConfig>`,
			),
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<RealtimeLogConfig")
				assert.Contains(t, rec.Body.String(), "<ARN>")
			},
		},
		{
			name:   "list_realtime_log_configs",
			method: http.MethodGet,
			path:   "/2020-05-31/realtime-log-config",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateRealtimeLogConfig("list-rt-log", 50, []string{"ts"})
				require.NoError(t, err)

				return ""
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<RealtimeLogConfigs")
			},
		},
		{
			name:   "get_realtime_log_config",
			method: http.MethodGet,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateRealtimeLogConfig("get-rt-log", 75, []string{"ts"})
				require.NoError(t, err)

				return "/2020-05-31/realtime-log-config/get-rt-log"
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<RealtimeLogConfig")
			},
		},
		{
			name:   "update_realtime_log_config",
			method: http.MethodPut,
			path:   "",
			body: []byte(
				`<RealtimeLogConfig>` +
					`<SamplingRate>90</SamplingRate>` +
					`<Fields><Field>uri</Field></Fields>` +
					`</RealtimeLogConfig>`,
			),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateRealtimeLogConfig("upd-rt-log", 50, []string{"ts"})
				require.NoError(t, err)

				return "/2020-05-31/realtime-log-config/upd-rt-log"
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<RealtimeLogConfig")
			},
		},
		{
			name:   "delete_realtime_log_config",
			method: http.MethodDelete,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateRealtimeLogConfig("del-rt-log", 50, []string{"ts"})
				require.NoError(t, err)

				return "/2020-05-31/realtime-log-config/del-rt-log"
			},
			wantStatus: http.StatusNoContent,
			check:      nil,
		},
		{
			name:   "get_realtime_log_config_not_found",
			method: http.MethodGet,
			path:   "/2020-05-31/realtime-log-config/doesnotexist",
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

// TestKeyValueStoreCRUD covers the full Key Value Store lifecycle via the HTTP handler.
func TestKeyValueStoreCRUD(t *testing.T) {
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
			name:   "create_key_value_store",
			method: http.MethodPost,
			path:   "/2020-05-31/key-value-store",
			body:   []byte(`<KeyValueStoreRequest><Name>my-kvs</Name><Comment>test</Comment></KeyValueStoreRequest>`),
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<KeyValueStore")
				assert.NotEmpty(t, rec.Header().Get("Location"))
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "list_key_value_stores",
			method: http.MethodGet,
			path:   "/2020-05-31/key-value-store",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateKeyValueStore("list-kvs", "comment")
				require.NoError(t, err)

				return ""
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<KeyValueStoreList")
				assert.Contains(t, rec.Body.String(), "<Quantity>1</Quantity>")
			},
		},
		{
			name:   "get_key_value_store",
			method: http.MethodGet,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				kvs, err := h.Backend.CreateKeyValueStore("get-kvs", "comment")
				require.NoError(t, err)

				return "/2020-05-31/key-value-store/" + kvs.ID
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<KeyValueStore")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "delete_key_value_store",
			method: http.MethodDelete,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				kvs, err := h.Backend.CreateKeyValueStore("del-kvs", "delete me")
				require.NoError(t, err)

				return "/2020-05-31/key-value-store/" + kvs.ID
			},
			wantStatus: http.StatusNoContent,
			check:      nil,
		},
		{
			name:   "get_key_value_store_not_found",
			method: http.MethodGet,
			path:   "/2020-05-31/key-value-store/doesnotexist",
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

// TestVpcOriginCRUD covers the full VPC Origin lifecycle via the HTTP handler.
func TestVpcOriginCRUD(t *testing.T) {
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
			name:   "create_vpc_origin",
			method: http.MethodPost,
			path:   "/2020-05-31/vpc-origin",
			body: []byte(
				`<VpcOriginRequest>` +
					`<VpcOriginEndpointConfig><Name>my-vpc-origin</Name></VpcOriginEndpointConfig>` +
					`</VpcOriginRequest>`,
			),
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<VpcOrigin")
				assert.NotEmpty(t, rec.Header().Get("Location"))
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "list_vpc_origins",
			method: http.MethodGet,
			path:   "/2020-05-31/vpc-origin",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateVpcOrigin("list-vpc-origin")
				require.NoError(t, err)

				return ""
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<VpcOriginList")
			},
		},
		{
			name:   "get_vpc_origin",
			method: http.MethodGet,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				origin, err := h.Backend.CreateVpcOrigin("get-vpc-origin")
				require.NoError(t, err)

				return "/2020-05-31/vpc-origin/" + origin.ID
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<VpcOrigin")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "update_vpc_origin",
			method: http.MethodPut,
			path:   "",
			body: []byte(
				`<VpcOriginRequest>` +
					`<VpcOriginEndpointConfig><Name>updated-vpc-origin</Name></VpcOriginEndpointConfig>` +
					`</VpcOriginRequest>`,
			),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				origin, err := h.Backend.CreateVpcOrigin("old-vpc-origin")
				require.NoError(t, err)

				return "/2020-05-31/vpc-origin/" + origin.ID
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<VpcOrigin")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "delete_vpc_origin",
			method: http.MethodDelete,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				origin, err := h.Backend.CreateVpcOrigin("del-vpc-origin")
				require.NoError(t, err)

				return "/2020-05-31/vpc-origin/" + origin.ID
			},
			wantStatus: http.StatusNoContent,
			check:      nil,
		},
		{
			name:   "get_vpc_origin_not_found",
			method: http.MethodGet,
			path:   "/2020-05-31/vpc-origin/doesnotexist",
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

// TestContinuousDeploymentPolicyCRUD covers get, list, update, delete (create is already tested).
func TestContinuousDeploymentPolicyCRUD(t *testing.T) {
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
			name:   "list_continuous_deployment_policies",
			method: http.MethodGet,
			path:   "/2020-05-31/continuous-deployment-policy",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateContinuousDeploymentPolicy(true, "staging.example.com")
				require.NoError(t, err)

				return ""
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<ContinuousDeploymentPolicyList")
				assert.Contains(t, rec.Body.String(), "<Quantity>1</Quantity>")
			},
		},
		{
			name:   "get_continuous_deployment_policy",
			method: http.MethodGet,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				p, err := h.Backend.CreateContinuousDeploymentPolicy(true, "staging.example.com")
				require.NoError(t, err)

				return "/2020-05-31/continuous-deployment-policy/" + p.ID
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<ContinuousDeploymentPolicy")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "get_continuous_deployment_policy_config",
			method: http.MethodGet,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				p, err := h.Backend.CreateContinuousDeploymentPolicy(false, "")
				require.NoError(t, err)

				return "/2020-05-31/continuous-deployment-policy/" + p.ID + "/config"
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<ContinuousDeploymentPolicy")
			},
		},
		{
			name:   "update_continuous_deployment_policy",
			method: http.MethodPut,
			path:   "",
			body: []byte(
				`<ContinuousDeploymentPolicyConfig><Enabled>false</Enabled></ContinuousDeploymentPolicyConfig>`,
			),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				p, err := h.Backend.CreateContinuousDeploymentPolicy(true, "staging.example.com")
				require.NoError(t, err)

				return "/2020-05-31/continuous-deployment-policy/" + p.ID
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<ContinuousDeploymentPolicy")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "delete_continuous_deployment_policy",
			method: http.MethodDelete,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				p, err := h.Backend.CreateContinuousDeploymentPolicy(true, "staging.example.com")
				require.NoError(t, err)

				return "/2020-05-31/continuous-deployment-policy/" + p.ID
			},
			wantStatus: http.StatusNoContent,
			check:      nil,
		},
		{
			name:   "get_continuous_deployment_policy_not_found",
			method: http.MethodGet,
			path:   "/2020-05-31/continuous-deployment-policy/doesnotexist",
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

// TestNewDispatchRefactoring verifies the refactored dispatch helpers route all operations correctly.
func TestNewDispatchRefactoring(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *cloudfront.Handler) string
		name       string
		method     string
		path       string
		wantStatus int
	}{
		// dispatchGetDistributionAndCachePolicyOps
		{
			name:   "get_cache_policy_routed",
			method: http.MethodGet,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				p, err := h.Backend.CreateCachePolicy("route-cp", "c", 86400, 31536000, 0)
				require.NoError(t, err)

				return "/2020-05-31/cache-policy/" + p.ID
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "delete_cache_policy_routed",
			method: http.MethodDelete,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				p, err := h.Backend.CreateCachePolicy("del-route-cp", "c", 86400, 31536000, 0)
				require.NoError(t, err)

				return "/2020-05-31/cache-policy/" + p.ID
			},
			// 412 PreconditionFailed confirms route reached handleDeleteCachePolicy (missing If-Match)
			wantStatus: http.StatusPreconditionFailed,
		},
		// dispatchGetIdentityAndPolicyDeleteOps
		{
			name:   "get_oai_config_routed",
			method: http.MethodGet,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				oai, err := h.Backend.CreateOAI("route-oai-ref", "route oai")
				require.NoError(t, err)

				return "/2020-05-31/origin-access-identity/cloudfront/" + oai.ID + "/config"
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "delete_distribution_routed",
			method: http.MethodDelete,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("del-route-ref", "del-route-dist", false, nil)
				require.NoError(t, err)

				return "/2020-05-31/distribution/" + d.ID
			},
			// 412 PreconditionFailed confirms route reached handleDeleteDistribution (missing If-Match)
			wantStatus: http.StatusPreconditionFailed,
		},
		// dispatchUpdateDeletePolicyAndOAIOps
		{
			name:   "delete_oai_routed",
			method: http.MethodDelete,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				oai, err := h.Backend.CreateOAI("del-route-oai-ref", "del-route-oai")
				require.NoError(t, err)

				return "/2020-05-31/origin-access-identity/cloudfront/" + oai.ID
			},
			// 412 PreconditionFailed confirms route reached handleDeleteOAI (missing If-Match header)
			wantStatus: http.StatusPreconditionFailed,
		},
		// dispatchFieldLevelEncryptionOps
		{
			name:   "get_fle_routed",
			method: http.MethodGet,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				fle, err := h.Backend.CreateFieldLevelEncryption("route-fle-cfg", "comment")
				require.NoError(t, err)

				return "/2020-05-31/field-level-encryption/" + fle.ID
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "get_fle_profile_routed",
			method: http.MethodGet,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				p, err := h.Backend.CreateFieldLevelEncryptionProfile("route-fle-profile", "comment")
				require.NoError(t, err)

				return "/2020-05-31/field-level-encryption-profile/" + p.ID
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			path := ""
			if tt.setup != nil {
				path = tt.setup(t, h)
			}

			rec := doXML(t, h, tt.method, path, nil)

			assert.Equal(t, tt.wantStatus, rec.Code)
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

// TestBackendContinuousDeploymentPolicyDirectly tests CDP operations directly on the backend.
func TestBackendContinuousDeploymentPolicyDirectly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(*testing.T, *cloudfront.InMemoryBackend)
		name string
	}{
		{
			name: "get_cdp",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				p, err := b.CreateContinuousDeploymentPolicy(true, "staging.example.com")
				require.NoError(t, err)

				got, err := b.GetContinuousDeploymentPolicy(p.ID)
				require.NoError(t, err)
				assert.True(t, got.Enabled)
				assert.Equal(t, "staging.example.com", got.StagingDistributionDNS)
			},
		},
		{
			name: "list_cdp",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateContinuousDeploymentPolicy(true, "")
				require.NoError(t, err)
				_, err = b.CreateContinuousDeploymentPolicy(false, "")
				require.NoError(t, err)

				list := b.ListContinuousDeploymentPolicies()
				assert.Len(t, list, 2)
			},
		},
		{
			name: "update_cdp",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				p, err := b.CreateContinuousDeploymentPolicy(true, "staging.example.com")
				require.NoError(t, err)

				updated, err := b.UpdateContinuousDeploymentPolicy(p.ID, false, "new.example.com")
				require.NoError(t, err)
				assert.False(t, updated.Enabled)
				assert.Equal(t, "new.example.com", updated.StagingDistributionDNS)
			},
		},
		{
			name: "delete_cdp",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				p, err := b.CreateContinuousDeploymentPolicy(true, "")
				require.NoError(t, err)
				require.NoError(t, b.DeleteContinuousDeploymentPolicy(p.ID))

				_, err = b.GetContinuousDeploymentPolicy(p.ID)
				require.Error(t, err)
			},
		},
		{
			name: "get_cdp_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.GetContinuousDeploymentPolicy("doesnotexist")
				require.Error(t, err)
			},
		},
		{
			name: "update_cdp_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.UpdateContinuousDeploymentPolicy("doesnotexist", false, "")
				require.Error(t, err)
			},
		},
		{
			name: "delete_cdp_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				err := b.DeleteContinuousDeploymentPolicy("doesnotexist")
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

// TestBackendFLEDirectly tests FLE backend operations directly.
func TestBackendFLEDirectly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(*testing.T, *cloudfront.InMemoryBackend)
		name string
	}{
		{
			name: "create_get_list_update_delete",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				fle, err := b.CreateFieldLevelEncryption("fle-backend-test", "comment")
				require.NoError(t, err)
				assert.NotEmpty(t, fle.ID)

				got, err := b.GetFieldLevelEncryption(fle.ID)
				require.NoError(t, err)
				assert.Equal(t, "comment", got.Comment)

				list := b.ListFieldLevelEncryptions()
				assert.Len(t, list, 1)

				updated, err := b.UpdateFieldLevelEncryption(fle.ID, "fle-backend-test-new", "updated")
				require.NoError(t, err)
				assert.Equal(t, "updated", updated.Comment)

				require.NoError(t, b.DeleteFieldLevelEncryption(fle.ID))
				_, err = b.GetFieldLevelEncryption(fle.ID)
				require.Error(t, err)
			},
		},
		{
			name: "create_duplicate_name_fails",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateFieldLevelEncryption("dup-fle", "comment")
				require.NoError(t, err)
				_, err = b.CreateFieldLevelEncryption("dup-fle", "comment")
				require.Error(t, err)
			},
		},
		{
			name: "get_fle_profile_list_update_delete",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				p, err := b.CreateFieldLevelEncryptionProfile("profile-test", "comment")
				require.NoError(t, err)

				got, err := b.GetFieldLevelEncryptionProfile(p.ID)
				require.NoError(t, err)
				assert.Equal(t, "profile-test", got.Name)

				list := b.ListFieldLevelEncryptionProfiles()
				assert.Len(t, list, 1)

				updated, err := b.UpdateFieldLevelEncryptionProfile(p.ID, "profile-test-new", "updated")
				require.NoError(t, err)
				assert.Equal(t, "updated", updated.Comment)

				require.NoError(t, b.DeleteFieldLevelEncryptionProfile(p.ID))
				_, err = b.GetFieldLevelEncryptionProfile(p.ID)
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

// TestBackendPublicKeyDirectly tests Public Key backend operations directly.
func TestBackendPublicKeyDirectly(t *testing.T) {
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

			b := cloudfront.NewInMemoryBackend("123456789012", "us-east-1")
			tt.run(t, b)
		})
	}
}

// TestBackendKeyGroupDirectly tests Key Group backend operations directly.
func TestBackendKeyGroupDirectly(t *testing.T) {
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

			b := cloudfront.NewInMemoryBackend("123456789012", "us-east-1")
			tt.run(t, b)
		})
	}
}

// TestBackendRealtimeLogConfigDirectly tests Realtime Log Config backend operations directly.
func TestBackendRealtimeLogConfigDirectly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(*testing.T, *cloudfront.InMemoryBackend)
		name string
	}{
		{
			name: "create_get_list_update_delete",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				cfg, err := b.CreateRealtimeLogConfig("rl-cfg", 100, []string{"timestamp", "uri"})
				require.NoError(t, err)
				assert.NotEmpty(t, cfg.ARN)

				got, err := b.GetRealtimeLogConfig(cfg.ARN)
				require.NoError(t, err)
				assert.Equal(t, "rl-cfg", got.Name)
				assert.Equal(t, int64(100), got.SamplingRate)

				list := b.ListRealtimeLogConfigs()
				assert.Len(t, list, 1)

				updated, err := b.UpdateRealtimeLogConfig(cfg.ARN, 50, []string{"uri"})
				require.NoError(t, err)
				assert.Equal(t, int64(50), updated.SamplingRate)

				require.NoError(t, b.DeleteRealtimeLogConfig(cfg.ARN))
				_, err = b.GetRealtimeLogConfig(cfg.ARN)
				require.Error(t, err)
			},
		},
		{
			name: "get_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.GetRealtimeLogConfig("doesnotexist")
				require.Error(t, err)
			},
		},
		{
			name: "update_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.UpdateRealtimeLogConfig("doesnotexist", 0, nil)
				require.Error(t, err)
			},
		},
		{
			name: "delete_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				err := b.DeleteRealtimeLogConfig("doesnotexist")
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

// TestBackendKeyValueStoreDirectly tests Key Value Store backend operations directly.
func TestBackendKeyValueStoreDirectly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(*testing.T, *cloudfront.InMemoryBackend)
		name string
	}{
		{
			name: "create_get_list_delete",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				kvs, err := b.CreateKeyValueStore("kvs-name", "comment")
				require.NoError(t, err)
				assert.NotEmpty(t, kvs.ID)

				got, err := b.GetKeyValueStore(kvs.ID)
				require.NoError(t, err)
				assert.Equal(t, "kvs-name", got.Name)

				list := b.ListKeyValueStores()
				assert.Len(t, list, 1)

				require.NoError(t, b.DeleteKeyValueStore(kvs.ID))
				_, err = b.GetKeyValueStore(kvs.ID)
				require.Error(t, err)
			},
		},
		{
			name: "get_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.GetKeyValueStore("doesnotexist")
				require.Error(t, err)
			},
		},
		{
			name: "delete_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				err := b.DeleteKeyValueStore("doesnotexist")
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

// TestBackendVpcOriginDirectly tests VPC Origin backend operations directly.
func TestBackendVpcOriginDirectly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(*testing.T, *cloudfront.InMemoryBackend)
		name string
	}{
		{
			name: "create_get_list_update_delete",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				origin, err := b.CreateVpcOrigin("vpc-origin-name")
				require.NoError(t, err)
				assert.NotEmpty(t, origin.ID)

				got, err := b.GetVpcOrigin(origin.ID)
				require.NoError(t, err)
				assert.Equal(t, "vpc-origin-name", got.Name)

				list := b.ListVpcOrigins()
				assert.Len(t, list, 1)

				updated, err := b.UpdateVpcOrigin(origin.ID, "new-vpc-origin-name")
				require.NoError(t, err)
				assert.Equal(t, "new-vpc-origin-name", updated.Name)

				require.NoError(t, b.DeleteVpcOrigin(origin.ID))
				_, err = b.GetVpcOrigin(origin.ID)
				require.Error(t, err)
			},
		},
		{
			name: "get_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.GetVpcOrigin("doesnotexist")
				require.Error(t, err)
			},
		},
		{
			name: "update_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.UpdateVpcOrigin("doesnotexist", "name")
				require.Error(t, err)
			},
		},
		{
			name: "delete_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				err := b.DeleteVpcOrigin("doesnotexist")
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

// TestNewBackendNewResourcesPersistence verifies the new resource types survive snapshot/restore.
func TestNewBackendNewResourcesPersistence(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend("000000000000", "us-east-1")

	fle, err := b.CreateFieldLevelEncryption("persist-fle", "comment")
	require.NoError(t, err)

	fleP, err := b.CreateFieldLevelEncryptionProfile("persist-fle-profile", "comment")
	require.NoError(t, err)

	pk, err := b.CreatePublicKey("pk-persist-ref", "persist-pk", "comment", testRSA2048PublicKeyPEM)
	require.NoError(t, err)

	kg, err := b.CreateKeyGroup("persist-kg", "comment", []string{pk.ID})
	require.NoError(t, err)

	rl, err := b.CreateRealtimeLogConfig("persist-rl", 100, []string{"ts"})
	require.NoError(t, err)

	kvs, err := b.CreateKeyValueStore("persist-kvs", "comment")
	require.NoError(t, err)

	vpc, err := b.CreateVpcOrigin("persist-vpc")
	require.NoError(t, err)

	h := cloudfront.NewHandler(b)
	snap := h.Snapshot()
	require.NotEmpty(t, snap)

	b2 := cloudfront.NewInMemoryBackend("000000000000", "us-east-1")
	h2 := cloudfront.NewHandler(b2)
	require.NoError(t, h2.Restore(snap))

	_, err = b2.GetFieldLevelEncryption(fle.ID)
	require.NoError(t, err)

	_, err = b2.GetFieldLevelEncryptionProfile(fleP.ID)
	require.NoError(t, err)

	_, err = b2.GetPublicKey(pk.ID)
	require.NoError(t, err)

	_, err = b2.GetKeyGroup(kg.ID)
	require.NoError(t, err)

	_, err = b2.GetRealtimeLogConfig(rl.ARN)
	require.NoError(t, err)

	_, err = b2.GetKeyValueStore(kvs.ID)
	require.NoError(t, err)

	_, err = b2.GetVpcOrigin(vpc.ID)
	require.NoError(t, err)
}

// TestNewBackendPersistenceWithStrings verifies that persistence doesn't lose data on string fields.
func TestNewBackendPersistenceWithStrings(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend("000000000000", "us-east-1")

	pk, err := b.CreatePublicKey("str-ref", "str-pk", "pk-comment", testRSA2048PublicKeyPEM)
	require.NoError(t, err)

	h := cloudfront.NewHandler(b)
	snap := h.Snapshot()
	require.NotEmpty(t, snap)

	b2 := cloudfront.NewInMemoryBackend("000000000000", "us-east-1")
	h2 := cloudfront.NewHandler(b2)
	require.NoError(t, h2.Restore(snap))

	restored, err := b2.GetPublicKey(pk.ID)
	require.NoError(t, err)
	assert.Equal(t, "pk-comment", restored.Comment)
	assert.Equal(t, testRSA2048PublicKeyPEM, restored.EncodedKey)
	assert.Equal(t, "str-ref", restored.CallerReference)
}

// TestCFHandlerStringManipulations exercises path parsing paths with line-coverage gaps.
func TestCFHandlerStringManipulations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "unknown_resource_type",
			method:     http.MethodGet,
			path:       "/2020-05-31/totally-unknown-resource",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "post_to_realtime_log_config_root",
			method:     http.MethodPost,
			path:       "/2020-05-31/realtime-log-config",
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			var body []byte
			if strings.Contains(tt.path, "realtime-log-config") {
				body = []byte(
					`<RealtimeLogConfig><Name>test-rlc</Name><SamplingRate>100</SamplingRate></RealtimeLogConfig>`,
				)
			}

			rec := doXML(t, h, tt.method, tt.path, body)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
