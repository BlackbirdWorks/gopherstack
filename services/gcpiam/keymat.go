package gcpiam

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/url"
	"time"

	"cloud.google.com/go/iam/admin/apiv1/adminpb"
	pkcs12 "software.sslmate.com/src/go-pkcs12"
)

const (
	keyIDBytes       = 20
	serialBits       = 128
	pkcs12Password   = "notasecret"
	bitsSmall        = 1024
	bitsDefault      = 2048
	pemPrivate       = "PRIVATE KEY"
	pemPublic        = "PUBLIC KEY"
	pemCertificate   = "CERTIFICATE"
	credentialsType  = "service_account"
	authURI          = "https://accounts.google.com/o/oauth2/auth"
	tokenURI         = "https://oauth2.googleapis.com/token" //nolint:gosec // public OAuth endpoint, not a credential
	authCertsURL     = "https://www.googleapis.com/oauth2/v1/certs"
	clientCertPrefix = "https://www.googleapis.com/robot/v1/metadata/x509/"
	universeDomain   = "googleapis.com"
)

// farFuture is the validity end GCP reports for user-managed keys.
func farFuture() time.Time { return time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC) }

// credentialsFile is the JSON key file Google client libraries consume; field order matches Google's.
type credentialsFile struct {
	Type                    string `json:"type"`
	ProjectID               string `json:"project_id"`
	PrivateKeyID            string `json:"private_key_id"`
	PrivateKey              string `json:"private_key"`
	ClientEmail             string `json:"client_email"`
	ClientID                string `json:"client_id"`
	AuthURI                 string `json:"auth_uri"`
	TokenURI                string `json:"token_uri"`
	AuthProviderX509CertURL string `json:"auth_provider_x509_cert_url"`
	ClientX509CertURL       string `json:"client_x509_cert_url"`
	UniverseDomain          string `json:"universe_domain"`
}

func newKeyID() (string, error) {
	raw := make([]byte, keyIDBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", errInternal(err)
	}

	return hex.EncodeToString(raw), nil
}

// keyAlgorithm validates the requested algorithm; every generated key is bitsDefault RSA.
func keyAlgorithm(alg adminpb.ServiceAccountKeyAlgorithm) (adminpb.ServiceAccountKeyAlgorithm, error) {
	if alg == adminpb.ServiceAccountKeyAlgorithm_KEY_ALG_RSA_1024 {
		return alg, errInvalid("KEY_ALG_RSA_1024 is not supported for new keys; use KEY_ALG_RSA_2048.")
	}

	return adminpb.ServiceAccountKeyAlgorithm_KEY_ALG_RSA_2048, nil
}

// material is a generated RSA key with its self-signed X.509 certificate and PKIX public key.
type material struct {
	priv    *rsa.PrivateKey
	certPEM []byte
	pubDER  []byte
}

func generateMaterial(email string, from, until time.Time) (*material, error) {
	priv, err := rsa.GenerateKey(rand.Reader, bitsDefault)
	if err != nil {
		return nil, errInternal(err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), serialBits))
	if err != nil {
		return nil, errInternal(err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: email},
		NotBefore:    from,
		NotAfter:     until,
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		return nil, errInternal(err)
	}

	pub, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		return nil, errInternal(err)
	}

	return &material{
		priv:    priv,
		certPEM: pem.EncodeToMemory(&pem.Block{Type: pemCertificate, Bytes: der}),
		pubDER:  pub,
	}, nil
}

func (m *material) credentials(acct *adminpb.ServiceAccount, keyID string) ([]byte, error) {
	pk, err := x509.MarshalPKCS8PrivateKey(m.priv)
	if err != nil {
		return nil, errInternal(err)
	}

	out, err := json.MarshalIndent(credentialsFile{
		Type:                    credentialsType,
		ProjectID:               acct.GetProjectId(),
		PrivateKeyID:            keyID,
		PrivateKey:              string(pem.EncodeToMemory(&pem.Block{Type: pemPrivate, Bytes: pk})),
		ClientEmail:             acct.GetEmail(),
		ClientID:                acct.GetUniqueId(),
		AuthURI:                 authURI,
		TokenURI:                tokenURI,
		AuthProviderX509CertURL: authCertsURL,
		ClientX509CertURL:       clientCertPrefix + url.PathEscape(acct.GetEmail()),
		UniverseDomain:          universeDomain,
	}, "", "  ")
	if err != nil {
		return nil, errInternal(err)
	}

	return out, nil
}

func (m *material) pkcs12() ([]byte, error) {
	block, _ := pem.Decode(m.certPEM)

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, errInternal(err)
	}

	out, err := pkcs12.Modern.Encode(m.priv, cert, nil, pkcs12Password)
	if err != nil {
		return nil, errInternal(err)
	}

	return out, nil
}

func publicPEM(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: pemPublic, Bytes: der})
}
