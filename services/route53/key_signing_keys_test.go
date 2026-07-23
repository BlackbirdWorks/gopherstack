package route53_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53"
)

func createKSKForOpsTest(t *testing.T, h *route53.Handler, zoneID, name string) {
	t.Helper()

	body := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<CreateKeySigningKeyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <CallerReference>ksk-ref-%s</CallerReference>
  <HostedZoneId>%s</HostedZoneId>
  <KeyManagementServiceArn>arn:aws:kms:us-east-1:123456789012:key/mrk-abc123</KeyManagementServiceArn>
  <Name>%s</Name>
  <Status>ACTIVE</Status>
</CreateKeySigningKeyRequest>`, name, zoneID, name)

	rec := send(t, h, http.MethodPost, "/2013-04-01/keysigningkey", body)
	require.Equal(t, http.StatusCreated, rec.Code)
}

// TestRoute53_DeactivateDeleteKeySigningKey covers DeactivateKeySigningKey
// and DeleteKeySigningKey.
func TestRoute53_DeactivateDeleteKeySigningKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *route53.Handler) (zoneID, kskName string)
		name         string
		method       string
		suffix       string
		wantContains []string
		wantCode     int
	}{
		{
			name: "deactivate_ksk_success",
			setup: func(t *testing.T, h *route53.Handler) (string, string) {
				t.Helper()

				zoneID := createZoneForOpsTest(t, h)
				kskName := "mykey"
				createKSKForOpsTest(t, h, zoneID, kskName)
				// Activate first
				rec := send(t, h, http.MethodPost,
					"/2013-04-01/keysigningkey/"+zoneID+"/"+kskName+"/activate", "")
				require.Equal(t, http.StatusOK, rec.Code)

				return zoneID, kskName
			},
			method:       http.MethodPost,
			suffix:       "/deactivate",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeactivateKeySigningKeyResponse", "INSYNC"},
		},
		{
			name: "deactivate_ksk_not_found",
			setup: func(t *testing.T, _ *route53.Handler) (string, string) {
				t.Helper()

				return "ZNONEXISTENT", "badkey"
			},
			method:   http.MethodPost,
			suffix:   "/deactivate",
			wantCode: http.StatusNotFound,
		},
		{
			name: "delete_ksk_success",
			setup: func(t *testing.T, h *route53.Handler) (string, string) {
				t.Helper()

				zoneID := createZoneForOpsTest(t, h)
				kskName := "delkey"
				createKSKForOpsTest(t, h, zoneID, kskName)
				// Must deactivate before delete (AWS requirement).
				rec := send(t, h, http.MethodPost,
					"/2013-04-01/keysigningkey/"+zoneID+"/"+kskName+"/deactivate", "")
				require.Equal(t, http.StatusOK, rec.Code)

				return zoneID, kskName
			},
			method:       http.MethodDelete,
			suffix:       "",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteKeySigningKeyResponse"},
		},
		{
			name: "delete_ksk_not_found",
			setup: func(t *testing.T, _ *route53.Handler) (string, string) {
				t.Helper()

				return "ZNONEXISTENT", "badkey"
			},
			method:   http.MethodDelete,
			suffix:   "",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			zoneID, kskName := tt.setup(t, h)
			path := "/2013-04-01/keysigningkey/" + zoneID + "/" + kskName + tt.suffix
			rec := send(t, h, tt.method, path, "")

			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestKeySigningKeyCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		kskNames  []string
		wantCount int
	}{
		{
			name:      "one_ksk",
			kskNames:  []string{"key1"},
			wantCount: 1,
		},
		{
			name:      "two_ksks",
			kskNames:  []string{"key1", "key2"},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()
			hz, err := b.CreateHostedZone("example.com", "ref-1", "", false, "")
			require.NoError(t, err)

			for _, name := range tt.kskNames {
				_, err = b.CreateKeySigningKey(
					hz.ID, "caller-"+name, name, "arn:aws:kms:us-east-1:123456789012:key/test-ksk", "",
				)
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantCount, route53.KeySigningKeyCount(b))
		})
	}
}

func TestDuplicateKSK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		kskName string
		wantErr bool
		second  bool
	}{
		{
			name:    "first_create_ok",
			kskName: "mykey",
			second:  false,
			wantErr: false,
		},
		{
			name:    "duplicate_returns_error",
			kskName: "mykey",
			second:  true,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()
			hz, err := b.CreateHostedZone("example.com", "ref-1", "", false, "")
			require.NoError(t, err)

			_, err = b.CreateKeySigningKey(
				hz.ID, "caller-1", tt.kskName, "arn:aws:kms:us-east-1:123456789012:key/test-ksk", "",
			)
			require.NoError(t, err)

			if !tt.second {
				return
			}

			_, err = b.CreateKeySigningKey(
				hz.ID, "caller-2", tt.kskName, "arn:aws:kms:us-east-1:123456789012:key/test-ksk", "",
			)
			if tt.wantErr {
				require.Error(t, err)
				// Real Route 53 returns KeySigningKeyAlreadyExists (409) for a
				// duplicate name within a hosted zone, per the CreateKeySigningKey
				// error list in the aws-sdk-go-v2 route53 model — not the generic
				// InvalidInput this test previously asserted.
				assert.ErrorIs(t, err, route53.ErrKeySigningKeyAlreadyExists)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestDeleteZone_CascadesKSK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantKSKCount  int
		deleteZone    bool
		kskCountAfter int
	}{
		{
			name:          "without_delete",
			deleteZone:    false,
			wantKSKCount:  1,
			kskCountAfter: 1,
		},
		{
			name:          "with_delete_cascades_ksk",
			deleteZone:    true,
			wantKSKCount:  1,
			kskCountAfter: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()
			hz, err := b.CreateHostedZone("example.com", "ref-1", "", false, "")
			require.NoError(t, err)

			_, err = b.CreateKeySigningKey(
				hz.ID, "caller-1", "key1", "arn:aws:kms:us-east-1:123456789012:key/test-ksk", "",
			)
			require.NoError(t, err)

			assert.Equal(t, tt.wantKSKCount, route53.KeySigningKeyCount(b))

			if tt.deleteZone {
				require.NoError(t, b.DeleteHostedZone(hz.ID))
			}

			assert.Equal(t, tt.kskCountAfter, route53.KeySigningKeyCount(b))
		})
	}
}

func TestExtractOperation_CreateKSK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		method     string
		wantOp     string
		createZone bool
	}{
		{
			name:   "create_ksk",
			path:   "/2013-04-01/keysigningkey",
			method: http.MethodPost,
			wantOp: "CreateKeySigningKey",
		},
		{
			name:   "get_method_unknown",
			path:   "/2013-04-01/keysigningkey",
			method: http.MethodGet,
			wantOp: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			rec := send(t, h, tt.method, tt.path, "")
			_ = rec

			op := extractOpFromPath(t, h, tt.method, tt.path)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

func TestExtractOperation_ActivateKSK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		method string
		wantOp string
	}{
		{
			name:   "activate_ksk",
			path:   "/2013-04-01/keysigningkey/ZXXX/mykey/activate",
			method: http.MethodPost,
			wantOp: "ActivateKeySigningKey",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			op := extractOpFromPath(t, h, tt.method, tt.path)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

func TestCreateKSK_FullFields(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	zoneID := createZoneForOpsTest(t, h)
	createKSKForOpsTest(t, h, zoneID, "test-ksk")

	// GetDNSSEC should return the KSK with full fields.
	rec := send(t, h, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID+"/dnssec", "")
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()

	assert.Contains(t, body, "ECDSAP256SHA256", "SigningAlgorithmMnemonic missing")
	assert.Contains(t, body, "SHA-256", "DigestAlgorithmMnemonic missing")
	assert.Contains(t, body, "PublicKey")
	assert.Contains(t, body, "DSRecord")
	assert.Contains(t, body, "KeyTag")
}

func TestDeleteKeySigningKey_RequiresInactive(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	kskBody := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<CreateKeySigningKeyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <CallerReference>ksk-ref-del</CallerReference>
  <HostedZoneId>%s</HostedZoneId>
  <KeyManagementServiceArn>arn:aws:kms:us-east-1:123456789012:key/mrk-abc</KeyManagementServiceArn>
  <Name>active-key</Name>
  <Status>ACTIVE</Status>
</CreateKeySigningKeyRequest>`, zoneID)

	rec = send(t, h, http.MethodPost, "/2013-04-01/keysigningkey", kskBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	// Delete without deactivating first — must fail. Real Route 53 has no
	// "KeySigningKeyNotInactive" error code; the wire error for this case is
	// InvalidKeySigningKeyStatus (confirmed against aws-sdk-go-v2's route53
	// error types and the botocore api-2.json model, both http 400).
	rec = send(t, h, http.MethodDelete,
		"/2013-04-01/keysigningkey/"+zoneID+"/active-key", "")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidKeySigningKeyStatus")

	// Deactivate then delete — must succeed.
	rec = send(t, h, http.MethodPost,
		"/2013-04-01/keysigningkey/"+zoneID+"/active-key/deactivate", "")
	require.Equal(t, http.StatusOK, rec.Code)

	rec = send(t, h, http.MethodDelete,
		"/2013-04-01/keysigningkey/"+zoneID+"/active-key", "")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteKeySigningKeyResponse")
}

func TestRoute53_CreateKeySigningKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *route53.Handler) string
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "create_ksk_success",
			setup: func(h *route53.Handler) string {
				rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
				require.Equal(t, http.StatusCreated, rec.Code)

				return extractZoneID(t, rec.Body.String())
			},
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateKeySigningKeyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HostedZoneId>PLACEHOLDER</HostedZoneId>
  <CallerReference>ksk-ref-1</CallerReference>
  <Name>mykey</Name>
  <KeyManagementServiceArn>arn:aws:kms:us-east-1:123456789012:key/abc123</KeyManagementServiceArn>
  <Status>INACTIVE</Status>
</CreateKeySigningKeyRequest>`,
			wantCode:     http.StatusCreated,
			wantContains: []string{"CreateKeySigningKeyResponse", "mykey", "INACTIVE"},
		},
		{
			name: "create_ksk_missing_name",
			setup: func(h *route53.Handler) string {
				rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
				require.Equal(t, http.StatusCreated, rec.Code)

				return extractZoneID(t, rec.Body.String())
			},
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateKeySigningKeyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HostedZoneId>PLACEHOLDER</HostedZoneId>
  <CallerReference>ksk-ref-2</CallerReference>
</CreateKeySigningKeyRequest>`,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "create_ksk_zone_not_found",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateKeySigningKeyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HostedZoneId>ZNONEXISTENT</HostedZoneId>
  <CallerReference>ksk-ref-3</CallerReference>
  <Name>mykey</Name>
</CreateKeySigningKeyRequest>`,
			wantCode: http.StatusNotFound,
		},
		{
			// Real AWS: KeyManagementServiceArn must be a well-formed KMS
			// customer managed key ARN or CreateKeySigningKey returns
			// InvalidKMSArn (400), confirmed against the CreateKeySigningKey
			// API reference's Errors section.
			name: "create_ksk_invalid_kms_arn",
			setup: func(h *route53.Handler) string {
				rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
				require.Equal(t, http.StatusCreated, rec.Code)

				return extractZoneID(t, rec.Body.String())
			},
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateKeySigningKeyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HostedZoneId>PLACEHOLDER</HostedZoneId>
  <CallerReference>ksk-ref-4</CallerReference>
  <Name>mykey</Name>
  <KeyManagementServiceArn>not-a-kms-arn</KeyManagementServiceArn>
  <Status>INACTIVE</Status>
</CreateKeySigningKeyRequest>`,
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidKMSArn"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)

			body := tt.body
			if tt.setup != nil {
				zoneID := tt.setup(h)
				body = strings.ReplaceAll(body, "PLACEHOLDER", zoneID)
			}

			rec := send(t, h, http.MethodPost, "/2013-04-01/keysigningkey", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestRoute53_ActivateKeySigningKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "activate_success",
			wantCode:     http.StatusOK,
			wantContains: []string{"ActivateKeySigningKeyResponse", "ACTIVE"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)

			// Create zone.
			rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
			require.Equal(t, http.StatusCreated, rec.Code)
			zoneID := extractZoneID(t, rec.Body.String())

			// Create KSK.
			kskBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateKeySigningKeyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HostedZoneId>` + zoneID + `</HostedZoneId>
  <CallerReference>ksk-ref-activate</CallerReference>
  <Name>testkey</Name>
  <KeyManagementServiceArn>arn:aws:kms:us-east-1:123456789012:key/test-ksk</KeyManagementServiceArn>
  <Status>INACTIVE</Status>
</CreateKeySigningKeyRequest>`
			kskRec := send(t, h, http.MethodPost, "/2013-04-01/keysigningkey", kskBody)
			require.Equal(t, http.StatusCreated, kskRec.Code)

			// Activate.
			got := send(t, h, http.MethodPost, "/2013-04-01/keysigningkey/"+zoneID+"/testkey/activate", "")
			assert.Equal(t, tt.wantCode, got.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, got.Body.String(), s)
			}
		})
	}
}

func TestRoute53_ActivateKeySigningKey_NotFound(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	rec := send(t, h, http.MethodPost, "/2013-04-01/keysigningkey/ZNONEXISTENT/nokey/activate", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
