package cloudfront_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// cfRequestWithBodyHeaders issues an HTTP request with a body and headers and
// returns the response recorder.
func cfRequestWithBodyHeaders(
	t *testing.T,
	h *cloudfront.Handler,
	method, path, body string,
	headers map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyReader io.Reader = bytes.NewReader(nil)
	if body != "" {
		bodyReader = bytes.NewBufferString(body)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bodyReader)
	req.Header.Set("Content-Type", "application/xml")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	rr := httptest.NewRecorder()
	c := e.NewContext(req, rr)
	if err := h.Handler()(c); err != nil {
		t.Fatalf("handler error: %v", err)
	}

	return rr
}

func newB(t *testing.T) *cloudfront.InMemoryBackend {
	t.Helper()

	return cloudfront.NewInMemoryBackend("123456789012", "us-east-1")
}

// TestParityBatch3_PublicKeyReferentialIntegrity verifies that a public key
// referenced by a key group or an FLE profile cannot be deleted, and that the
// reference must be removed first.
func TestParityBatch3_PublicKeyReferentialIntegrity(t *testing.T) {
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

// TestParityBatch3_FLEReferentialIntegrity verifies referential integrity
// between FLE configs, FLE profiles, and public keys.
func TestParityBatch3_FLEReferentialIntegrity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *cloudfront.InMemoryBackend)
		name string
	}{
		{
			name: "profile_with_unknown_public_key_rejected",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateFieldLevelEncryptionProfile("p", "", []cloudfront.EncryptionEntity{
					{PublicKeyID: "K-DOES-NOT-EXIST", ProviderID: "prov"},
				})
				require.ErrorIs(t, err, cloudfront.ErrPublicKeyNotFound)
			},
		},
		{
			name: "config_with_unknown_profile_rejected",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateFieldLevelEncryption("cfg", "", []cloudfront.FLEQueryArgProfile{
					{QueryArg: "q", ProfileID: "P-DOES-NOT-EXIST"},
				})
				require.ErrorIs(t, err, cloudfront.ErrFLEProfileNotFound)
			},
		},
		{
			name: "profile_in_use_by_config_blocks_delete",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				prof, err := b.CreateFieldLevelEncryptionProfile("prof", "", nil)
				require.NoError(t, err)
				_, err = b.CreateFieldLevelEncryption("cfg", "", []cloudfront.FLEQueryArgProfile{
					{QueryArg: "q", ProfileID: prof.ID},
				})
				require.NoError(t, err)

				err = b.DeleteFieldLevelEncryptionProfile(prof.ID)
				require.ErrorIs(t, err, cloudfront.ErrFLEProfileInUse)
			},
		},
		{
			name: "profile_delete_ok_after_config_drops_ref",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				prof, err := b.CreateFieldLevelEncryptionProfile("prof", "", nil)
				require.NoError(t, err)
				cfg, err := b.CreateFieldLevelEncryption("cfg", "", []cloudfront.FLEQueryArgProfile{
					{QueryArg: "q", ProfileID: prof.ID},
				})
				require.NoError(t, err)

				require.ErrorIs(t, b.DeleteFieldLevelEncryptionProfile(prof.ID), cloudfront.ErrFLEProfileInUse)

				_, err = b.UpdateFieldLevelEncryption(cfg.ID, cfg.Name, "", nil)
				require.NoError(t, err)
				require.NoError(t, b.DeleteFieldLevelEncryptionProfile(prof.ID))
			},
		},
		{
			name: "profile_roundtrips_entities",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				pk, err := b.CreatePublicKey("cr", "pk", "", testRSA2048PublicKeyPEM)
				require.NoError(t, err)
				prof, err := b.CreateFieldLevelEncryptionProfile("prof", "", []cloudfront.EncryptionEntity{
					{PublicKeyID: pk.ID, ProviderID: "prov", FieldPatterns: []string{"a", "b"}},
				})
				require.NoError(t, err)

				got, err := b.GetFieldLevelEncryptionProfile(prof.ID)
				require.NoError(t, err)
				require.Len(t, got.EncryptionEntities, 1)
				assert.Equal(t, pk.ID, got.EncryptionEntities[0].PublicKeyID)
				assert.Equal(t, []string{"a", "b"}, got.EncryptionEntities[0].FieldPatterns)
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

// TestParityBatch3_PublicKeyInUseHTTP verifies the HTTP surface returns 409
// PublicKeyInUse when deleting a referenced public key.
func TestParityBatch3_PublicKeyInUseHTTP(t *testing.T) {
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

// TestParityBatch3_KVSStatusAndTimestamp verifies KVS create/describe carry a
// READY status and a non-empty last-modified time.
func TestParityBatch3_KVSStatusAndTimestamp(t *testing.T) {
	t.Parallel()

	b := newB(t)
	kvs, err := b.CreateKeyValueStore("kvs-status", "c")
	require.NoError(t, err)
	assert.Equal(t, "READY", kvs.Status)
	assert.NotEmpty(t, kvs.LastModifiedTime)

	got, err := b.GetKeyValueStore(kvs.ID)
	require.NoError(t, err)
	assert.Equal(t, "READY", got.Status)
	assert.NotEmpty(t, got.LastModifiedTime)
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
