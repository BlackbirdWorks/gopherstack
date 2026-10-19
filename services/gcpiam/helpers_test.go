package gcpiam_test

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"sync/atomic"
	"testing"
	"time"

	iam "cloud.google.com/go/iam/admin/apiv1"
	"cloud.google.com/go/iam/admin/apiv1/adminpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/services/gcpiam"
)

const project = "projects/test-project"

type clock struct{ ns atomic.Int64 }

func (c *clock) now() time.Time          { return time.Unix(0, c.ns.Load()).UTC() }
func (c *clock) advance(d time.Duration) { c.ns.Add(int64(d)) }

type env struct {
	client  *iam.IamClient
	raw     adminpb.IAMClient
	handler *gcpiam.Handler
	clk     *clock
}

func newEnv(t *testing.T) *env {
	t.Helper()

	h := gcpiam.NewHandler(gcpiam.NewInMemoryBackend())
	h.Port = 0

	clk := &clock{}
	clk.ns.Store(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())
	h.Backend.SetClock(clk.now)

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	client, err := iam.NewIamClient(t.Context(),
		option.WithEndpoint(h.Addr()),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	conn, err := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return &env{client: client, raw: adminpb.NewIAMClient(conn), handler: h, clk: clk}
}

func (e *env) account(t *testing.T, id string) *adminpb.ServiceAccount {
	t.Helper()

	sa, err := e.client.CreateServiceAccount(t.Context(), &adminpb.CreateServiceAccountRequest{
		Name:           project,
		AccountId:      id,
		ServiceAccount: &adminpb.ServiceAccount{DisplayName: "Display " + id, Description: "desc"},
	})
	require.NoError(t, err)

	return sa
}

func requireCode(t *testing.T, want codes.Code, err error) {
	t.Helper()

	require.Error(t, err)
	require.Equal(t, want, status.Code(err), err.Error())
}

func parsePublicKey(t *testing.T, key *adminpb.ServiceAccountKey) *rsa.PublicKey {
	t.Helper()

	block, _ := pem.Decode(key.GetPublicKeyData())
	require.NotNil(t, block)

	if block.Type == "CERTIFICATE" {
		cert, err := x509.ParseCertificate(block.Bytes)
		require.NoError(t, err)

		pub, ok := cert.PublicKey.(*rsa.PublicKey)
		require.True(t, ok)

		return pub
	}

	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	require.NoError(t, err)

	pub, ok := parsed.(*rsa.PublicKey)
	require.True(t, ok)

	return pub
}

func verifySHA256(pub *rsa.PublicKey, data, sig []byte) error {
	sum := sha256.Sum256(data)

	return rsa.VerifyPKCS1v15(pub, crypto.SHA256, sum[:], sig)
}
