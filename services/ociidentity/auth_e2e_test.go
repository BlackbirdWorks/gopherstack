package ociidentity_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/identity"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oss "github.com/blackbirdworks/gopherstack/services/ociobjectstorage"
)

// storageEnv is an enforcing Object Storage listener whose key registry is (optionally) linked to Identity's.
func storageEnv(t *testing.T, e *env, link bool) string {
	t.Helper()

	be := oss.NewInMemoryBackend()
	h := oss.NewHandler(be)
	h.Port = freePort(t)
	h.EnforceAuth = true

	if link {
		be.Auth.Link(e.backend.Auth)
	}

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	return fmt.Sprintf("http://localhost:%d", h.Port)
}

func storageCall(t *testing.T, host, user string, kp keyPair, fingerprint string) error {
	t.Helper()

	prov := common.NewRawConfigurationProvider(testTenancy, user, "us-ashburn-1", fingerprint, kp.privPEM, nil)
	c, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(prov)
	require.NoError(t, err)

	c.Host = host

	_, err = c.GetNamespace(t.Context(), objectstorage.GetNamespaceRequest{})

	return err
}

func TestUploadedKeyAuthenticatesOtherServices(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		link       bool
		prepare    func(t *testing.T, e *env, user identity.User, kp keyPair)
		signWith   func(good, other keyPair) keyPair
		wantStatus int
	}{
		{
			name: "uploaded key verifies", link: true, wantStatus: http.StatusOK,
			signWith: func(good, _ keyPair) keyPair { return good },
		},
		{
			name: "wrong private key rejected", link: true, wantStatus: http.StatusUnauthorized,
			signWith: func(good, other keyPair) keyPair { good.privPEM = other.privPEM; return good },
		},
		{
			name: "deleted key rejected", link: true, wantStatus: http.StatusUnauthorized,
			signWith: func(good, _ keyPair) keyPair { return good },
			prepare: func(t *testing.T, e *env, user identity.User, kp keyPair) {
				t.Helper()

				_, err := e.client.DeleteApiKey(t.Context(), identity.DeleteApiKeyRequest{
					UserId: user.Id, Fingerprint: new(kp.fprint),
				})
				require.NoError(t, err)
			},
		},
		{
			name: "deleted user rejected", link: true, wantStatus: http.StatusUnauthorized,
			signWith: func(good, _ keyPair) keyPair { return good },
			prepare: func(t *testing.T, e *env, user identity.User, _ keyPair) {
				t.Helper()

				_, err := e.client.DeleteUser(t.Context(), identity.DeleteUserRequest{UserId: user.Id})
				require.NoError(t, err)
			},
		},
		{
			name: "unlinked registry does not know the key", link: false, wantStatus: http.StatusUnauthorized,
			signWith: func(good, _ keyPair) keyPair { return good },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			host := storageEnv(t, e, tt.link)
			good, other := newKeyPair(t), newKeyPair(t)
			user := e.mkUser(t, "signer")

			_, err := e.client.UploadApiKey(t.Context(), identity.UploadApiKeyRequest{
				UserId: user.Id, CreateApiKeyDetails: identity.CreateApiKeyDetails{Key: new(good.pubPEM)},
			})
			require.NoError(t, err)

			if tt.prepare != nil {
				tt.prepare(t, e, user, good)
			}

			signer := tt.signWith(good, other)
			err = storageCall(t, host, *user.Id, signer, good.fprint)

			if tt.wantStatus == http.StatusOK {
				require.NoError(t, err)

				return
			}

			se := svcErr(t, err)
			assert.Equal(t, tt.wantStatus, se.GetHTTPStatusCode())
			assert.Equal(t, "NotAuthenticated", se.GetCode())
		})
	}
}

func TestAdminKeyEndpointStillRegisters(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	host := storageEnv(t, e, true)
	kp := newKeyPair(t)

	// A key registered through the shared admin endpoint on Identity authenticates other services too.
	fp, err := e.backend.Auth.Register(testTenancy, "ocid1.user.oc1..admin", kp.pubPEM)
	require.NoError(t, err)
	require.NoError(t, storageCall(t, host, "ocid1.user.oc1..admin", kp, fp))
}
