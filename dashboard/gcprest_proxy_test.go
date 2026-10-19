package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/gcpkms"
	"github.com/blackbirdworks/gopherstack/services/gcppubsub"
	"github.com/blackbirdworks/gopherstack/services/gcpsecretmanager"
)

type gcpStep struct {
	method   string
	path     string
	body     string
	wantBody string
	wantCode int
}

func runGCPSteps(t *testing.T, h *dashboard.DashboardHandler, steps []gcpStep) {
	t.Helper()

	for i, s := range steps {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(s.method, s.path, strings.NewReader(s.body))
		req.Header.Set("Content-Type", "application/json")
		require.NoError(t, h.Handler()(echo.NewContext(req, rec)))

		assert.Equal(t, s.wantCode, rec.Code, "step %d %s %s: %s", i, s.method, s.path, rec.Body.String())
		assert.Contains(t, rec.Body.String(), s.wantBody, "step %d", i)
	}
}

func TestGCPPubSubProxy(t *testing.T) {
	t.Parallel()

	const (
		base      = "/dashboard/api/v1/gcp/pubsub/v1/projects/proj-local"
		topic     = base + "/topics/topic1"
		sub       = base + "/subscriptions/sub1"
		topicBody = `{"labels":{"env":"dev"},"messageRetentionDuration":"600s"}`
		subBody   = `{"topic":"projects/proj-local/topics/topic1","ackDeadlineSeconds":10}`
		snapBody  = `{"subscription":"projects/proj-local/subscriptions/sub1"}`
		snap      = base + "/snapshots/snap-one"
		pubBody   = `{"messages":[{"data":"aGk=","attributes":{"a":"b"}}]}`
		seekBody  = `{"snapshot":"projects/proj-local/snapshots/snap-one"}`
		patchBody = `{"topic":{"labels":{"env":"prod"}},"updateMask":"labels"}`
		root      = "/dashboard/api/v1/gcp/pubsub"
	)

	tests := []struct {
		name  string
		steps []gcpStep
	}{
		{"topic lifecycle", []gcpStep{
			{http.MethodPut, topic, topicBody, "projects/proj-local/topics/topic1", 200},
			{http.MethodPut, topic, `{}`, "ALREADY_EXISTS", http.StatusConflict},
			{http.MethodGet, base + "/topics", "", `"env":"dev"`, 200},
			{http.MethodPatch, topic, patchBody, `"prod"`, 200},
			{http.MethodDelete, topic, "", "{}", 200},
			{http.MethodGet, topic, "", "NOT_FOUND", http.StatusNotFound},
		}},
		{"publish pull ack seek", []gcpStep{
			{http.MethodPut, topic, `{}`, "topic1", 200},
			{http.MethodPut, sub, subBody, "subscriptions/sub1", 200},
			{http.MethodPost, topic + ":publish", pubBody, `"messageIds"`, 200},
			{http.MethodPost, sub + ":pull", `{"maxMessages":5}`, `"aGk="`, 200},
			{http.MethodPost, sub + ":pull", `{"maxMessages":5}`, "{}", 200},
			{http.MethodPut, snap, snapBody, "snap-one", 200},
			{http.MethodGet, base + "/snapshots", "", "snap-one", 200},
			{http.MethodPost, sub + ":seek", seekBody, "{}", 200},
			{http.MethodGet, topic + "/subscriptions", "", "sub1", 200},
			{http.MethodDelete, snap, "", "{}", 200},
			{http.MethodDelete, sub, "", "{}", 200},
		}},
		{"rejects", []gcpStep{
			{http.MethodGet, base + "/schemas", "", "unsupported", http.StatusNotFound},
			{http.MethodGet, root + "/v2/projects/proj-local/topics", "", "", http.StatusBadRequest},
			{http.MethodPut, topic, `{bad`, "invalid request body", http.StatusBadRequest},
			{http.MethodGet, base + "/topics/%2E%2E", "", "", http.StatusBadRequest},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := dashboard.NewHandler(dashboard.Config{
				GCPPubSubOps: gcppubsub.NewHandler(gcppubsub.NewInMemoryBackend()),
			})
			runGCPSteps(t, h, tt.steps)
		})
	}
}

func TestGCPSecretManagerProxy(t *testing.T) {
	t.Parallel()

	const (
		base       = "/dashboard/api/v1/gcp/secretmanager/v1/projects/proj-local/secrets"
		secret     = base + "/sub1"
		create     = base + "?secretId=sub1"
		aliasMask  = "?updateMask=versionAliases"
		secretBody = `{"replication":{"automatic":{}},"labels":{"k":"v"}}`
	)

	tests := []struct {
		name  string
		steps []gcpStep
	}{
		{"secret and versions", []gcpStep{
			{http.MethodPost, create, secretBody, "projects/proj-local/secrets/sub1", 200},
			{http.MethodPost, create, `{"replication":{"automatic":{}}}`, "ALREADY_EXISTS", 409},
			{http.MethodPost, secret + ":addVersion", `{"payload":{"data":"b25l"}}`, `versions/1"`, 200},
			{http.MethodPost, secret + ":addVersion", `{"payload":{"data":"dHdv"}}`, `versions/2"`, 200},
			{http.MethodGet, secret + "/versions", "", `versions/2"`, 200},
			{http.MethodGet, secret + "/versions/latest:access", "", `"dHdv"`, 200},
			{http.MethodPost, secret + "/versions/2:disable", "", "DISABLED", 200},
			{http.MethodGet, secret + "/versions/2:access", "", "FAILED_PRECONDITION", http.StatusBadRequest},
			{http.MethodPost, secret + "/versions/2:enable", "", "ENABLED", 200},
			{http.MethodPost, secret + "/versions/1:destroy", "", "DESTROYED", 200},
			{http.MethodPatch, secret + aliasMask, `{"versionAliases":{"prod":2}}`, `"prod"`, 200},
			{http.MethodGet, secret + ":getIamPolicy", "", "etag", 200},
			{http.MethodGet, base, "", `"secrets"`, 200},
			{http.MethodDelete, secret, "", "{}", 200},
			{http.MethodGet, secret, "", "NOT_FOUND", http.StatusNotFound},
		}},
		{"rejects", []gcpStep{
			{http.MethodGet, base + "/sub1:setIamPolicy", "", "unsupported", http.StatusNotFound},
			{http.MethodPatch, secret + "?updateMask=%00", `{}`, "", http.StatusBadRequest},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := dashboard.NewHandler(dashboard.Config{
				GCPSecretManagerOps: gcpsecretmanager.NewHandler(gcpsecretmanager.NewInMemoryBackend()),
			})
			runGCPSteps(t, h, tt.steps)
		})
	}
}

func TestGCPRESTProxyDisabled(t *testing.T) {
	t.Parallel()

	for _, p := range []string{
		"/dashboard/api/v1/gcp/pubsub/v1/projects/proj-local/topics",
		"/dashboard/api/v1/gcp/secretmanager/v1/projects/proj-local/secrets",
		"/dashboard/api/v1/gcp/kms/v1/projects/proj-local/locations/global/keyRings",
	} {
		h := dashboard.NewHandler(dashboard.Config{})
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, p, nil)
		require.NoError(t, h.Handler()(echo.NewContext(req, rec)))
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	}
}

func TestGCPKMSProxy(t *testing.T) {
	t.Parallel()

	const (
		loc    = "/dashboard/api/v1/gcp/kms/v1/projects/proj-local/locations/global"
		ring   = loc + "/keyRings/r1"
		sym    = ring + "/cryptoKeys/sym"
		asym   = ring + "/cryptoKeys/sig"
		mac    = ring + "/cryptoKeys/mac"
		symKey = `{"purpose":"ENCRYPT_DECRYPT"}`
		sigKey = `{"purpose":"ASYMMETRIC_SIGN","versionTemplate":{"algorithm":"EC_SIGN_P256_SHA256"}}`
		macKey = `{"purpose":"MAC","versionTemplate":{"algorithm":"HMAC_SHA256"}}`
	)

	tests := []struct {
		name  string
		steps []gcpStep
	}{
		{"ring key and crypto", []gcpStep{
			{http.MethodPost, loc + "/keyRings?keyRingId=r1", "", "keyRings/r1", 200},
			{http.MethodPost, loc + "/keyRings?keyRingId=r1", "", "ALREADY_EXISTS", http.StatusConflict},
			{http.MethodGet, loc + "/keyRings", "", `"keyRings"`, 200},
			{http.MethodGet, ring, "", "keyRings/r1", 200},
			{http.MethodPost, ring + "/cryptoKeys?cryptoKeyId=sym", symKey, "ENCRYPT_DECRYPT", 200},
			{http.MethodGet, ring + "/cryptoKeys", "", `"cryptoKeys"`, 200},
			{http.MethodGet, sym, "", `"primary"`, 200},
			{http.MethodPost, sym + ":encrypt", `{"plaintext":"aGk="}`, `"ciphertext"`, 200},
			{http.MethodPost, sym + ":decrypt", `{"ciphertext":"AAAA"}`, "INVALID_ARGUMENT", http.StatusBadRequest},
			{http.MethodGet, sym + "/cryptoKeyVersions", "", `cryptoKeyVersions/1`, 200},
			{http.MethodPost, sym + "/cryptoKeyVersions", `{}`, `cryptoKeyVersions/2`, 200},
			{http.MethodPost, sym + ":updatePrimaryVersion", `{"cryptoKeyVersionId":"2"}`, `cryptoKeyVersions/2`, 200},
			{http.MethodPatch, sym + "/cryptoKeyVersions/1?updateMask=state", `{"state":"DISABLED"}`, "DISABLED", 200},
			{http.MethodPost, sym + "/cryptoKeyVersions/1:destroy", "", "DESTROY_SCHEDULED", 200},
			{http.MethodPost, sym + "/cryptoKeyVersions/1:restore", "", "DISABLED", 200},
		}},
		{"sign public key mac", []gcpStep{
			{http.MethodPost, loc + "/keyRings?keyRingId=r1", "", "keyRings/r1", 200},
			{http.MethodPost, ring + "/cryptoKeys?cryptoKeyId=sig", sigKey, "EC_SIGN_P256_SHA256", 200},
			{http.MethodPost, asym + "/cryptoKeyVersions/1:asymmetricSign", `{"data":"aGk="}`, `"signature"`, 200},
			{http.MethodGet, asym + "/cryptoKeyVersions/1/publicKey", "", "BEGIN PUBLIC KEY", 200},
			{http.MethodPost, ring + "/cryptoKeys?cryptoKeyId=mac", macKey, "HMAC_SHA256", 200},
			{http.MethodPost, mac + "/cryptoKeyVersions/1:macSign", `{"data":"aGk="}`, `"mac"`, 200},
		}},
		{"rejects", []gcpStep{
			{http.MethodGet, loc + "/importJobs", "", "unsupported", http.StatusNotFound},
			{http.MethodGet, ring + "/cryptoKeys/x/cryptoKeyVersions/1/other", "", "unsupported", http.StatusNotFound},
			{http.MethodPost, ring + ":bogus", "", "unsupported", http.StatusNotFound},
			{http.MethodPost, sym + ":rawDecrypt", `{}`, "unsupported", http.StatusNotFound},
			{http.MethodPost, ring + "/cryptoKeys?cryptoKeyId=k", `{bad`, "invalid request body", http.StatusBadRequest},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := dashboard.NewHandler(dashboard.Config{
				GCPKMSOps: gcpkms.NewHandler(gcpkms.NewInMemoryBackend()),
			})
			runGCPSteps(t, h, tt.steps)
		})
	}
}
