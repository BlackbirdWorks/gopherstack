package app

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ociidentitybackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
	ociobjectstoragebackend "github.com/blackbirdworks/gopherstack/services/ociobjectstorage"
)

func TestWireOCIIdentityKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wire    bool
		wantErr bool
	}{
		{"wired key authenticates object storage", true, false},
		{"unwired key is unknown", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			idH := ociidentitybackend.NewHandler(ociidentitybackend.NewInMemoryBackend())
			osH := ociobjectstoragebackend.NewHandler(ociobjectstoragebackend.NewInMemoryBackend())
			osH.EnforceAuth = true

			l, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)

			tcp, ok := l.Addr().(*net.TCPAddr)
			require.True(t, ok)
			require.NoError(t, l.Close())

			osH.Port = tcp.Port
			require.NoError(t, osH.StartWorker(t.Context()))
			t.Cleanup(func() { osH.Shutdown(context.Background()) })

			if tt.wire {
				wireOCIIdentityKeys(map[string]service.Registerable{"OCIIdentity": idH, "OCIObjectStorage": osH})
			}

			priv, err := rsa.GenerateKey(rand.Reader, 2048)
			require.NoError(t, err)

			der, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
			require.NoError(t, err)

			user, _, err := idH.Backend.CreateUser(ociidentitybackend.CreateUserIn{
				CompartmentID: ociidentitybackend.TenancyID, Name: "wired", Description: "d",
			})
			require.NoError(t, err)

			key, _, err := idH.Backend.UploadAPIKey(
				user.ID,
				string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})),
			)
			require.NoError(t, err)

			fp, err := ociauth.Fingerprint(&priv.PublicKey)
			require.NoError(t, err)
			require.Equal(t, fp, key.Fingerprint)

			privPEM := string(
				pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)}),
			)
			prov := common.NewRawConfigurationProvider(
				ociidentitybackend.TenancyID,
				user.ID,
				"us-ashburn-1",
				fp,
				privPEM,
				nil,
			)
			c, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(prov)
			require.NoError(t, err)

			c.Host = fmt.Sprintf("http://localhost:%d", tcp.Port)

			_, err = c.GetNamespace(t.Context(), objectstorage.GetNamespaceRequest{})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}
