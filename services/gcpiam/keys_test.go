package gcpiam_test

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"cloud.google.com/go/iam/admin/apiv1/adminpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	pkcs12 "software.sslmate.com/src/go-pkcs12"
)

type keyFile struct {
	Type         string `json:"type"`
	ProjectID    string `json:"project_id"`
	PrivateKeyID string `json:"private_key_id"`
	PrivateKey   string `json:"private_key"`
	ClientEmail  string `json:"client_email"`
	ClientID     string `json:"client_id"`
	TokenURI     string `json:"token_uri"`
}

func createKey(t *testing.T, e *env, sa *adminpb.ServiceAccount) (*adminpb.ServiceAccountKey, keyFile) {
	t.Helper()

	key, err := e.client.CreateServiceAccountKey(t.Context(), &adminpb.CreateServiceAccountKeyRequest{
		Name:           sa.GetName(),
		PrivateKeyType: adminpb.ServiceAccountPrivateKeyType_TYPE_GOOGLE_CREDENTIALS_FILE,
	})
	require.NoError(t, err)

	var kf keyFile
	require.NoError(t, json.Unmarshal(key.GetPrivateKeyData(), &kf))

	return key, kf
}

func parsePrivate(t *testing.T, pemStr string) *rsa.PrivateKey {
	t.Helper()

	block, _ := pem.Decode([]byte(pemStr))
	require.NotNil(t, block)
	require.Equal(t, "PRIVATE KEY", block.Type)

	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	require.NoError(t, err)

	priv, ok := parsed.(*rsa.PrivateKey)
	require.True(t, ok)

	return priv
}

func TestCreateKeyReturnsUsableCredentialsFile(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	sa := e.account(t, "key-sa")
	key, kf := createKey(t, e, sa)

	assert.Equal(t, "service_account", kf.Type)
	assert.Equal(t, "test-project", kf.ProjectID)
	assert.Equal(t, sa.GetEmail(), kf.ClientEmail)
	assert.Equal(t, sa.GetUniqueId(), kf.ClientID)
	assert.Equal(t, "https://oauth2.googleapis.com/token", kf.TokenURI)
	assert.Regexp(t, `^[0-9a-f]{40}$`, kf.PrivateKeyID)
	assert.Equal(t, sa.GetName()+"/keys/"+kf.PrivateKeyID, key.GetName())
	assert.Equal(t, adminpb.ServiceAccountKeyAlgorithm_KEY_ALG_RSA_2048, key.GetKeyAlgorithm())
	assert.Equal(t, adminpb.ServiceAccountPrivateKeyType_TYPE_GOOGLE_CREDENTIALS_FILE, key.GetPrivateKeyType())

	priv := parsePrivate(t, kf.PrivateKey)
	assert.Equal(t, 2048, priv.N.BitLen())

	msg := []byte("hello gopherstack")
	sum := sha256.Sum256(msg)
	sig, err := rsa.SignPKCS1v15(rand.Reader, priv, crypto.SHA256, sum[:])
	require.NoError(t, err)

	for _, pt := range []adminpb.ServiceAccountPublicKeyType{
		adminpb.ServiceAccountPublicKeyType_TYPE_X509_PEM_FILE,
		adminpb.ServiceAccountPublicKeyType_TYPE_RAW_PUBLIC_KEY,
	} {
		got, gerr := e.client.GetServiceAccountKey(
			t.Context(),
			&adminpb.GetServiceAccountKeyRequest{Name: key.GetName(), PublicKeyType: pt},
		)
		require.NoError(t, gerr)
		assert.Empty(t, got.GetPrivateKeyData())
		require.NoError(t, verifySHA256(parsePublicKey(t, got), msg, sig), pt.String())
	}

	plain, err := e.client.GetServiceAccountKey(t.Context(), &adminpb.GetServiceAccountKeyRequest{Name: key.GetName()})
	require.NoError(t, err)
	assert.Empty(t, plain.GetPublicKeyData())
	assert.Equal(t, adminpb.ListServiceAccountKeysRequest_USER_MANAGED, plain.GetKeyType())
	assert.Equal(t, 9999, plain.GetValidBeforeTime().AsTime().Year())
}

func TestKeyAlgorithmsAndFormats(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	sa := e.account(t, "fmt-sa")

	small, err := e.client.CreateServiceAccountKey(t.Context(), &adminpb.CreateServiceAccountKeyRequest{
		Name: sa.GetName(), KeyAlgorithm: adminpb.ServiceAccountKeyAlgorithm_KEY_ALG_RSA_1024,
	})
	require.NoError(t, err)
	assert.Equal(t, adminpb.ServiceAccountKeyAlgorithm_KEY_ALG_RSA_1024, small.GetKeyAlgorithm())

	var kf keyFile
	require.NoError(t, json.Unmarshal(small.GetPrivateKeyData(), &kf), "default type is the credentials file")
	assert.Equal(t, 1024, parsePrivate(t, kf.PrivateKey).N.BitLen())

	p12, err := e.client.CreateServiceAccountKey(t.Context(), &adminpb.CreateServiceAccountKeyRequest{
		Name: sa.GetName(), PrivateKeyType: adminpb.ServiceAccountPrivateKeyType_TYPE_PKCS12_FILE,
	})
	require.NoError(t, err)

	priv, cert, err := pkcs12.Decode(p12.GetPrivateKeyData(), "notasecret")
	require.NoError(t, err)
	assert.Equal(t, sa.GetEmail(), cert.Subject.CommonName)
	assert.NotNil(t, priv)
}

func TestKeyLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	sa := e.account(t, "life-key-sa")
	ctx := t.Context()
	k1, _ := createKey(t, e, sa)
	k2, _ := createKey(t, e, sa)

	list, err := e.client.ListServiceAccountKeys(ctx, &adminpb.ListServiceAccountKeysRequest{Name: sa.GetName()})
	require.NoError(t, err)
	require.Len(t, list.GetKeys(), 3, "two user-managed keys plus the system-managed key")

	for _, k := range list.GetKeys() {
		assert.Empty(t, k.GetPrivateKeyData())
		assert.Empty(t, k.GetPublicKeyData())
	}

	require.NoError(
		t,
		e.client.DisableServiceAccountKey(ctx, &adminpb.DisableServiceAccountKeyRequest{Name: k1.GetName()}),
	)

	got, err := e.client.GetServiceAccountKey(ctx, &adminpb.GetServiceAccountKeyRequest{Name: k1.GetName()})
	require.NoError(t, err)
	assert.True(t, got.GetDisabled())

	require.NoError(
		t,
		e.client.EnableServiceAccountKey(ctx, &adminpb.EnableServiceAccountKeyRequest{Name: k1.GetName()}),
	)
	require.NoError(
		t,
		e.client.DeleteServiceAccountKey(ctx, &adminpb.DeleteServiceAccountKeyRequest{Name: k1.GetName()}),
	)

	_, err = e.client.GetServiceAccountKey(ctx, &adminpb.GetServiceAccountKeyRequest{Name: k1.GetName()})
	requireCode(t, codes.NotFound, err)

	user, err := e.client.ListServiceAccountKeys(ctx, &adminpb.ListServiceAccountKeysRequest{
		Name:     sa.GetName(),
		KeyTypes: []adminpb.ListServiceAccountKeysRequest_KeyType{adminpb.ListServiceAccountKeysRequest_USER_MANAGED},
	})
	require.NoError(t, err)
	require.Len(t, user.GetKeys(), 1)
	assert.Equal(t, k2.GetName(), user.GetKeys()[0].GetName())

	_, err = e.client.ListServiceAccountKeys(ctx, &adminpb.ListServiceAccountKeysRequest{
		Name: sa.GetName(), KeyTypes: []adminpb.ListServiceAccountKeysRequest_KeyType{
			adminpb.ListServiceAccountKeysRequest_USER_MANAGED, adminpb.ListServiceAccountKeysRequest_USER_MANAGED,
		},
	})
	requireCode(t, codes.InvalidArgument, err)
}

func TestUserKeyQuotaAndSystemKeys(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	sa := e.account(t, "quota-sa")
	ctx := t.Context()

	for range 10 {
		_, err := e.client.CreateServiceAccountKey(ctx, &adminpb.CreateServiceAccountKeyRequest{
			Name: sa.GetName(), KeyAlgorithm: adminpb.ServiceAccountKeyAlgorithm_KEY_ALG_RSA_1024,
		})
		require.NoError(t, err)
	}

	_, err := e.client.CreateServiceAccountKey(ctx, &adminpb.CreateServiceAccountKeyRequest{Name: sa.GetName()})
	requireCode(t, codes.FailedPrecondition, err)

	sys, err := e.client.ListServiceAccountKeys(ctx, &adminpb.ListServiceAccountKeysRequest{
		Name:     sa.GetName(),
		KeyTypes: []adminpb.ListServiceAccountKeysRequest_KeyType{adminpb.ListServiceAccountKeysRequest_SYSTEM_MANAGED},
	})
	require.NoError(t, err)
	require.Len(t, sys.GetKeys(), 1)

	k := sys.GetKeys()[0]
	assert.Equal(t, adminpb.ListServiceAccountKeysRequest_SYSTEM_MANAGED, k.GetKeyType())

	requireCode(
		t,
		codes.FailedPrecondition,
		e.client.DeleteServiceAccountKey(ctx, &adminpb.DeleteServiceAccountKeyRequest{Name: k.GetName()}),
	)
	requireCode(
		t,
		codes.FailedPrecondition,
		e.client.DisableServiceAccountKey(ctx, &adminpb.DisableServiceAccountKeyRequest{Name: k.GetName()}),
	)

	all, err := e.client.ListServiceAccountKeys(ctx, &adminpb.ListServiceAccountKeysRequest{Name: sa.GetName()})
	require.NoError(t, err)
	assert.Len(t, all.GetKeys(), 11)
}

func TestUploadServiceAccountKey(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	sa := e.account(t, "upload-sa")
	ctx := t.Context()

	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(7),
		NotBefore:    time.Date(2029, 1, 1, 0, 0, 0, 0, time.UTC),
		NotAfter:     time.Date(2039, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	up, err := e.client.UploadServiceAccountKey(
		ctx,
		&adminpb.UploadServiceAccountKeyRequest{Name: sa.GetName(), PublicKeyData: certPEM},
	)
	require.NoError(t, err)
	assert.Equal(t, adminpb.ServiceAccountKeyOrigin_USER_PROVIDED, up.GetKeyOrigin())
	assert.Equal(t, 2039, up.GetValidBeforeTime().AsTime().Year())

	got, err := e.client.GetServiceAccountKey(ctx, &adminpb.GetServiceAccountKeyRequest{
		Name: up.GetName(), PublicKeyType: adminpb.ServiceAccountPublicKeyType_TYPE_RAW_PUBLIC_KEY,
	})
	require.NoError(t, err)
	assert.True(t, priv.PublicKey.Equal(parsePublicKey(t, got)))

	junk := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("junk")})

	for _, bad := range [][]byte{nil, []byte("not pem"), junk} {
		_, err = e.client.UploadServiceAccountKey(
			ctx,
			&adminpb.UploadServiceAccountKeyRequest{Name: sa.GetName(), PublicKeyData: bad},
		)
		requireCode(t, codes.InvalidArgument, err)
	}
}

func TestKeyErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	sa := e.account(t, "err-key-sa")

	tests := []struct {
		call func() error
		name string
		want codes.Code
	}{
		{func() error {
			_, err := e.client.GetServiceAccountKey(
				t.Context(),
				&adminpb.GetServiceAccountKeyRequest{Name: sa.GetName() + "/keys/ab"},
			)

			return err
		}, "get missing key", codes.NotFound},
		{func() error {
			_, err := e.client.GetServiceAccountKey(
				t.Context(),
				&adminpb.GetServiceAccountKeyRequest{Name: sa.GetName()},
			)

			return err
		}, "malformed key name", codes.InvalidArgument},
		{func() error {
			_, err := e.client.CreateServiceAccountKey(
				t.Context(),
				&adminpb.CreateServiceAccountKeyRequest{
					Name: project + "/serviceAccounts/x@test-project.iam.gserviceaccount.com",
				},
			)

			return err
		}, "create on missing account", codes.NotFound},
		{func() error {
			_, err := e.client.CreateServiceAccountKey(t.Context(), &adminpb.CreateServiceAccountKeyRequest{
				Name: sa.GetName(), PrivateKeyType: adminpb.ServiceAccountPrivateKeyType(99),
			})

			return err
		}, "unsupported private key type", codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			requireCode(t, tt.want, tt.call())
		})
	}
}
