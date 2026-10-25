package dashboard_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/ociidentity"
	"github.com/blackbirdworks/gopherstack/services/ociobjectstorage"
	"github.com/blackbirdworks/gopherstack/services/ocistreaming"
	"github.com/blackbirdworks/gopherstack/services/ocivault"
)

const (
	ociComp      = "ocid1.compartment.oc1..dashtest"
	ociSecretVal = "s3cr3t-value-42"
)

func TestOCIObjectStorageProxy(t *testing.T) {
	t.Parallel()

	const root = "/dashboard/api/v1/oci/objectstorage"

	seed := func(t *testing.T) *dashboard.DashboardHandler {
		t.Helper()

		oc := ociobjectstorage.NewHandler(ociobjectstorage.NewInMemoryBackend())
		ns := oc.Backend.Namespace()
		_, err := oc.Backend.CreateBucket(
			ns,
			ociobjectstorage.CreateBucketInput{Name: "bk", CompartmentID: ociComp},
			"u",
		)
		require.NoError(t, err)

		for name, body := range map[string]string{"docs/a.txt": "hello", "docs/b.txt": "world", "top.txt": "top"} {
			_, err = oc.Backend.PutObject(ns, "bk", name, []byte(body),
				ociobjectstorage.ObjectMeta{ContentType: "text/plain"}, ociobjectstorage.Preconds{}, "")
			require.NoError(t, err)
		}

		_, err = oc.Backend.CreateBucket(
			ns,
			ociobjectstorage.CreateBucketInput{Name: "empty", CompartmentID: ociComp},
			"u",
		)
		require.NoError(t, err)

		return dashboard.NewHandler(dashboard.Config{OCIObjectStorageOps: oc})
	}

	tests := []struct {
		name  string
		steps []gcpStep
	}{
		{"browse", []gcpStep{
			{http.MethodGet, root + "/namespace", "", `"namespace":"gopherstack"`, 200},
			{http.MethodGet, root + "/buckets", "", `"name":"bk"`, 200},
			{http.MethodGet, root + "/buckets/bk", "", `"approximateCount":3`, 200},
			{http.MethodGet, root + "/buckets/bk/objects?delimiter=%2F", "", `"prefixes":["docs/"]`, 200},
			{http.MethodGet, root + "/buckets/bk/objects?prefix=docs%2F&delimiter=%2F", "", `"name":"docs/a.txt"`, 200},
			{http.MethodGet, root + "/buckets/bk/object?name=docs%2Fa.txt", "", `"contentType":"text/plain"`, 200},
			{http.MethodGet, root + "/buckets/bk/object/content?name=docs%2Fa.txt", "", "hello", 200},
			{http.MethodGet, root + "/buckets/bk/pars", "", `"pars":[]`, 200},
			{http.MethodGet, root + "/buckets/bk/lifecycle", "", `"configured":false`, 200},
			{http.MethodGet, root + "/buckets/bk/retention", "", `"rules":[]`, 200},
		}},
		{"actions", []gcpStep{
			{http.MethodDelete, root + "/buckets/bk/object?name=top.txt", "", "", 204},
			{http.MethodGet, root + "/buckets/bk/object?name=top.txt", "", "ObjectNotFound", 404},
			{http.MethodDelete, root + "/buckets/bk", "", "BucketNotEmpty", 409},
			{http.MethodDelete, root + "/buckets/empty", "", "", 204},
			{http.MethodGet, root + "/buckets/empty", "", "BucketNotFound", 404},
		}},
		{"rejects", []gcpStep{
			{http.MethodGet, root + "/buckets/bk/object", "", "name is required", 400},
			{http.MethodDelete, root + "/buckets/bk/object", "", "name is required", 400},
			{http.MethodGet, root + "/buckets/nope/objects", "", "BucketNotFound", 404},
			{http.MethodGet, root + "/buckets/bk/object?name=nope", "", "NotFound", 404},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runGCPSteps(t, seed(t), tt.steps)
		})
	}

	t.Run("download uses the base name", func(t *testing.T) {
		t.Parallel()

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, root+"/buckets/bk/object/content?name=docs%2Fa.txt", nil)
		require.NoError(t, seed(t).Handler()(echo.NewContext(req, rec)))
		assert.Equal(t, "attachment; filename*=UTF-8''a.txt", rec.Header().Get("Content-Disposition"))
		assert.Equal(t, "nosniff", rec.Header().Get("X-Content-Type-Options"))
	})

	t.Run("unavailable", func(t *testing.T) {
		t.Parallel()

		runGCPSteps(t, dashboard.NewHandler(dashboard.Config{}), []gcpStep{
			{http.MethodGet, root + "/buckets", "", "UNAVAILABLE", http.StatusServiceUnavailable},
			{http.MethodDelete, root + "/buckets/bk", "", "UNAVAILABLE", http.StatusServiceUnavailable},
		})
	})
}

func TestOCIVaultProxy(t *testing.T) {
	t.Parallel()

	const root = "/dashboard/api/v1/oci/vault"

	type seeded struct {
		h                        *dashboard.DashboardHandler
		vaultID, keyID, secretID string
	}

	seed := func(t *testing.T) seeded {
		t.Helper()

		v := ocivault.NewHandler(ocivault.NewInMemoryBackend())
		vault, _, err := v.Backend.CreateVault(ocivault.CreateVaultIn{
			CompartmentID: ociComp, DisplayName: "vault-one", VaultType: "DEFAULT",
		}, "")
		require.NoError(t, err)

		key, _, err := v.Backend.CreateKey(vault.ID, ocivault.CreateKeyIn{
			CompartmentID: ociComp, DisplayName: "key-one", ProtectionMode: "SOFTWARE",
			KeyShape: ocivault.KeyShape{Algorithm: "AES", Length: 32},
		})
		require.NoError(t, err)

		secret, _, err := v.Backend.CreateSecret(ocivault.CreateSecretIn{
			CompartmentID: ociComp, VaultID: vault.ID, KeyID: key.ID, SecretName: "db-password",
			SecretContent: &ocivault.SecretContent{
				ContentType: "BASE64", Content: base64.StdEncoding.EncodeToString([]byte(ociSecretVal)),
			},
		})
		require.NoError(t, err)

		return seeded{
			h: dashboard.NewHandler(
				dashboard.Config{OCIVaultOps: v},
			), vaultID: vault.ID, keyID: key.ID, secretID: secret.ID,
		}
	}

	t.Run("browse", func(t *testing.T) {
		t.Parallel()

		s := seed(t)
		runGCPSteps(t, s.h, []gcpStep{
			{http.MethodGet, root + "/vaults", "", `"displayName":"vault-one"`, 200},
			{http.MethodGet, root + "/vaults/" + s.vaultID + "/keys", "", `"displayName":"key-one"`, 200},
			{http.MethodGet, root + "/vaults/" + s.vaultID + "/keys/" + s.keyID + "/versions", "", `"versions"`, 200},
			{http.MethodGet, root + "/secrets", "", `"secretName":"db-password"`, 200},
			{http.MethodGet, root + "/secrets?vaultId=" + s.vaultID, "", `"secretName":"db-password"`, 200},
			{http.MethodGet, root + "/secrets/" + s.secretID + "/versions", "", `"versionNumber":1`, 200},
		})
	})

	t.Run("content never listed", func(t *testing.T) {
		t.Parallel()

		s := seed(t)

		for _, p := range []string{
			"/vaults", "/vaults/" + s.vaultID + "/keys", "/secrets", "/secrets/" + s.secretID + "/versions",
		} {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, root+p, nil)
			require.NoError(t, s.h.Handler()(echo.NewContext(req, rec)))
			assert.Equal(t, http.StatusOK, rec.Code, p)
			assert.NotContains(t, rec.Body.String(), ociSecretVal, p)
			assert.NotContains(t, rec.Body.String(), base64.StdEncoding.EncodeToString([]byte(ociSecretVal)), p)
		}
	})

	t.Run("reveal", func(t *testing.T) {
		t.Parallel()

		s := seed(t)
		runGCPSteps(t, s.h, []gcpStep{
			{http.MethodPost, root + "/secrets/" + s.secretID + "/versions/1/reveal", "", ociSecretVal, 200},
			{http.MethodPost, root + "/secrets/" + s.secretID + "/versions/1/reveal", "", `"isText":true`, 200},
		})
	})

	t.Run("rejects", func(t *testing.T) {
		t.Parallel()

		s := seed(t)
		runGCPSteps(t, s.h, []gcpStep{
			{
				http.MethodPost,
				root + "/secrets/" + s.secretID + "/versions/0/reveal",
				"",
				"invalid version number",
				400,
			},
			{
				http.MethodPost,
				root + "/secrets/" + s.secretID + "/versions/abc/reveal",
				"",
				"invalid version number",
				400,
			},
			{
				http.MethodPost,
				root + "/secrets/" + s.secretID + "/versions/9/reveal",
				"",
				"NotAuthorizedOrNotFound",
				404,
			},
			{
				http.MethodPost,
				root + "/secrets/ocid1.vaultsecret.oc1..nope/versions/1/reveal",
				"",
				"NotAuthorizedOrNotFound",
				404,
			},
			{http.MethodGet, root + "/vaults/nope/keys/nope/versions", "", "NotAuthorizedOrNotFound", 404},
		})
	})

	t.Run("unavailable", func(t *testing.T) {
		t.Parallel()

		runGCPSteps(t, dashboard.NewHandler(dashboard.Config{}), []gcpStep{
			{http.MethodGet, root + "/vaults", "", "UNAVAILABLE", http.StatusServiceUnavailable},
			{http.MethodPost, root + "/secrets/x/versions/1/reveal", "", "UNAVAILABLE", http.StatusServiceUnavailable},
		})
	})
}

func TestOCIStreamingProxy(t *testing.T) {
	t.Parallel()

	const root = "/dashboard/api/v1/oci/streaming"

	seed := func(t *testing.T) (*dashboard.DashboardHandler, string) {
		t.Helper()

		st := ocistreaming.NewHandler(ocistreaming.NewInMemoryBackend())
		st.Backend.SetTransition(0)

		pool, _, _, err := st.Backend.CreatePool(ocistreaming.CreatePoolIn{Name: "pool-one", Compartment: ociComp}, "")
		require.NoError(t, err)

		parts := 2
		stream, _, _, err := st.Backend.CreateStream(ocistreaming.CreateStreamIn{
			Name: "events", Compartment: ociComp, PoolID: pool.ID, Partitions: &parts,
		}, "")
		require.NoError(t, err)

		st.Backend.Sweep()

		for i := range 5 {
			body := `{"messages":[{"key":"` + base64.StdEncoding.EncodeToString([]byte("k"+string(rune('0'+i)))) +
				`","value":"` + base64.StdEncoding.EncodeToString([]byte("event-"+string(rune('0'+i)))) + `"}]}`
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(
				http.MethodPost,
				"/20180418/streams/"+stream.ID+"/messages",
				strings.NewReader(body),
			)
			req.Header.Set("Content-Type", "application/json")
			require.NoError(t, st.Handler()(echo.NewContext(req, rec)))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		}

		return dashboard.NewHandler(dashboard.Config{OCIStreamingOps: st}), stream.ID
	}

	t.Run("browse", func(t *testing.T) {
		t.Parallel()

		h, id := seed(t)
		runGCPSteps(t, h, []gcpStep{
			{http.MethodGet, root + "/pools", "", `"name":"pool-one"`, 200},
			{http.MethodGet, root + "/streams", "", `"name":"events"`, 200},
			{http.MethodGet, root + "/streams/" + id, "", `"partitions":2`, 200},
			{http.MethodGet, root + "/streams/" + id + "/partitions", "", `"partitions":[{`, 200},
		})
	})

	t.Run("messages", func(t *testing.T) {
		t.Parallel()

		h, id := seed(t)
		total := 0

		for _, part := range []string{"0", "1"} {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, root+"/streams/"+id+"/messages?partition="+part+"&limit=50", nil)
			require.NoError(t, h.Handler()(echo.NewContext(req, rec)))
			require.Equal(t, http.StatusOK, rec.Code)
			total += strings.Count(rec.Body.String(), `"text":"event-`)
		}

		assert.Equal(t, 5, total)

		runGCPSteps(t, h, []gcpStep{
			{http.MethodGet, root + "/streams/" + id + "/messages?partition=0&limit=1", "", `"isText":true`, 200},
			{http.MethodGet, root + "/streams/" + id + "/messages?partition=0&offset=0", "", `"messages"`, 200},
		})
	})

	t.Run("rejects", func(t *testing.T) {
		t.Parallel()

		h, id := seed(t)
		runGCPSteps(t, h, []gcpStep{
			{http.MethodGet, root + "/streams/" + id + "/messages?partition=9", "", "does not exist", 400},
			{http.MethodGet, root + "/streams/" + id + "/messages?offset=-1", "", "invalid offset", 400},
			{http.MethodGet, root + "/streams/" + id + "/messages?offset=abc", "", "invalid offset", 400},
			{http.MethodGet, root + "/streams/nope/messages", "", "NotAuthorizedOrNotFound", 404},
			{http.MethodGet, root + "/streams/nope", "", "NotAuthorizedOrNotFound", 404},
		})
	})

	t.Run("unavailable", func(t *testing.T) {
		t.Parallel()

		runGCPSteps(t, dashboard.NewHandler(dashboard.Config{}), []gcpStep{
			{http.MethodGet, root + "/pools", "", "UNAVAILABLE", http.StatusServiceUnavailable},
			{http.MethodGet, root + "/streams/x/messages", "", "UNAVAILABLE", http.StatusServiceUnavailable},
		})
	})
}

func TestOCIIdentityProxy(t *testing.T) {
	t.Parallel()

	const root = "/dashboard/api/v1/oci/identity"

	seed := func(t *testing.T) (*dashboard.DashboardHandler, string) {
		t.Helper()

		id := ociidentity.NewHandler(ociidentity.NewInMemoryBackend())
		b := id.Backend

		comp, _, err := b.CreateCompartment(ociidentity.CreateCompartmentIn{
			CompartmentID: ociidentity.TenancyID, Name: "dev", Description: "development",
		})
		require.NoError(t, err)

		user, _, err := b.CreateUser(ociidentity.CreateUserIn{
			CompartmentID: ociidentity.TenancyID, Name: "alice", Description: "Alice", Email: "alice@example.com",
		})
		require.NoError(t, err)

		group, _, err := b.CreateGroup(
			ociidentity.CreateGroupIn{CompartmentID: ociidentity.TenancyID, Name: "admins", Description: "Admins"},
		)
		require.NoError(t, err)

		_, _, err = b.AddUserToGroup(user.ID, group.ID)
		require.NoError(t, err)

		_, _, err = b.CreatePolicy(ociidentity.CreatePolicyIn{
			CompartmentID: comp.ID, Name: "dev-policy", Description: "dev",
			Statements: []string{"Allow group admins to manage all-resources in compartment dev"},
		})
		require.NoError(t, err)

		key, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)

		der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
		require.NoError(t, err)

		pemKey := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
		_, _, err = b.UploadAPIKey(user.ID, pemKey)
		require.NoError(t, err)

		return dashboard.NewHandler(dashboard.Config{OCIIdentityOps: id}), user.ID
	}

	t.Run("browse", func(t *testing.T) {
		t.Parallel()

		h, user := seed(t)
		runGCPSteps(t, h, []gcpStep{
			{http.MethodGet, root + "/tenancy", "", `"name":"gopherstack"`, 200},
			{http.MethodGet, root + "/compartments", "", `"name":"dev"`, 200},
			{http.MethodGet, root + "/users", "", `"name":"alice"`, 200},
			{http.MethodGet, root + "/groups", "", `"name":"admins"`, 200},
			{http.MethodGet, root + "/memberships", "", `"memberships":[{`, 200},
			{http.MethodGet, root + "/policies", "", "manage all-resources", 200},
			{http.MethodGet, root + "/users/" + user + "/keys", "", `"fingerprint":"`, 200},
		})
	})

	t.Run("keys expose fingerprints only", func(t *testing.T) {
		t.Parallel()

		h, user := seed(t)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, root+"/users/"+user+"/keys", nil)
		require.NoError(t, h.Handler()(echo.NewContext(req, rec)))
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.NotContains(t, rec.Body.String(), "keyValue")
		assert.NotContains(t, rec.Body.String(), "BEGIN PUBLIC KEY")
	})

	t.Run("rejects", func(t *testing.T) {
		t.Parallel()

		h, _ := seed(t)
		runGCPSteps(t, h, []gcpStep{
			{http.MethodGet, root + "/users/ocid1.user.oc1..nope/keys", "", "NotAuthorizedOrNotFound", 404},
		})
	})

	t.Run("unavailable", func(t *testing.T) {
		t.Parallel()

		runGCPSteps(t, dashboard.NewHandler(dashboard.Config{}), []gcpStep{
			{http.MethodGet, root + "/compartments", "", "UNAVAILABLE", http.StatusServiceUnavailable},
			{http.MethodGet, root + "/users/x/keys", "", "UNAVAILABLE", http.StatusServiceUnavailable},
		})
	})
}
