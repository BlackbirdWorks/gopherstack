package gcpkms_test

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/gcpkms"
)

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	ring := src.ring(t, "r")
	sym := src.key(t, ring, "sym", kmspb.CryptoKey_ENCRYPT_DECRYPT, kmspb.CryptoKeyVersion_GOOGLE_SYMMETRIC_ENCRYPTION,
		func(k *kmspb.CryptoKey) { k.NextRotationTime = timestamppb.New(src.clk.now().Add(time.Hour)) })
	sig := src.key(t, ring, "sig", kmspb.CryptoKey_ASYMMETRIC_SIGN, kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256)
	sigVersion := sig.GetName() + "/cryptoKeyVersions/1"

	enc, err := src.client.Encrypt(
		t.Context(),
		&kmspb.EncryptRequest{Name: sym.GetName(), Plaintext: []byte("persist me")},
	)
	require.NoError(t, err)

	pk, err := src.client.GetPublicKey(t.Context(), &kmspb.GetPublicKeyRequest{Name: sigVersion})
	require.NoError(t, err)

	_, err = src.client.CreateImportJob(t.Context(), &kmspb.CreateImportJobRequest{
		Parent: ring, ImportJobId: "job", ImportJob: &kmspb.ImportJob{
			ImportMethod: kmspb.ImportJob_RSA_OAEP_3072_SHA256, ProtectionLevel: kmspb.ProtectionLevel_SOFTWARE,
		},
	})
	require.NoError(t, err)

	_, err = src.client.ResourceIAM(sym.GetName()).Policy(t.Context())
	require.NoError(t, err)

	_, err = src.handler.Backend.SetIamPolicy(&iampb.SetIamPolicyRequest{
		Resource: sym.GetName(),
		Policy:   &iampb.Policy{Bindings: []*iampb.Binding{{Role: "roles/x", Members: []string{"user:a@b.c"}}}},
	})
	require.NoError(t, err)

	data := src.handler.Snapshot(t.Context())
	require.NotEmpty(t, data)

	dst := newEnv(t)
	require.NoError(t, dst.handler.Restore(t.Context(), data))

	dec, err := dst.client.Decrypt(
		t.Context(),
		&kmspb.DecryptRequest{Name: sym.GetName(), Ciphertext: enc.GetCiphertext()},
	)
	require.NoError(t, err)
	assert.Equal(t, "persist me", string(dec.GetPlaintext()))

	pk2, err := dst.client.GetPublicKey(t.Context(), &kmspb.GetPublicKeyRequest{Name: sigVersion})
	require.NoError(t, err)
	assert.Equal(t, pk.GetPem(), pk2.GetPem(), "restored private key yields the same public key")

	got, err := dst.client.GetCryptoKey(t.Context(), &kmspb.GetCryptoKeyRequest{Name: sym.GetName()})
	require.NoError(t, err)
	assert.Equal(t, sym.GetName()+"/cryptoKeyVersions/1", got.GetPrimary().GetName())
	assert.NotNil(t, got.GetNextRotationTime())

	job, err := dst.client.GetImportJob(t.Context(), &kmspb.GetImportJobRequest{Name: ring + "/importJobs/job"})
	require.NoError(t, err)
	assert.NotEmpty(t, job.GetPublicKey().GetPem())

	pol, err := dst.client.ResourceIAM(sym.GetName()).Policy(t.Context())
	require.NoError(t, err)
	assert.Equal(t, []string{"user:a@b.c"}, pol.Members("roles/x"))

	next, err := dst.client.CreateCryptoKeyVersion(
		t.Context(),
		&kmspb.CreateCryptoKeyVersionRequest{Parent: sym.GetName()},
	)
	require.NoError(t, err)
	assert.Equal(t, sym.GetName()+"/cryptoKeyVersions/2", next.GetName())

	dst.handler.Reset()

	_, err = dst.client.GetKeyRing(t.Context(), &kmspb.GetKeyRingRequest{Name: ring})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestRestoreRejectsCorruptSnapshot(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	assert.Error(t, e.handler.Restore(t.Context(), []byte("not json")))
}

func TestProviderInit(t *testing.T) {
	t.Parallel()

	p := &gcpkms.Provider{}

	_, err := p.Init(nil)
	require.ErrorIs(t, err, gcpkms.ErrNilAppContext)

	reg, err := p.Init(&service.AppContext{})
	require.NoError(t, err)

	h, ok := reg.(*gcpkms.Handler)
	require.True(t, ok)
	assert.Equal(t, gcpkms.DefaultPort, h.Port)
	assert.Equal(t, "GCPKMS", h.Name())
	assert.Contains(t, h.GetSupportedOperations(), "AsymmetricSign")
	assert.False(t, h.RouteMatcher()(nil))
}

func TestStartWorkerPortConflict(t *testing.T) {
	t.Parallel()

	var lc net.ListenConfig

	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	t.Cleanup(func() { _ = l.Close() })

	_, portStr, err := net.SplitHostPort(l.Addr().String())
	require.NoError(t, err)

	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)

	h := gcpkms.NewHandler(gcpkms.NewInMemoryBackend())
	h.Port = port

	require.Error(t, h.StartWorker(t.Context()))
}
