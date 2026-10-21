package gcpsql

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha1" //nolint:gosec // the sslCerts API addresses certificates by SHA-1 fingerprint
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"math/big"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
)

const (
	caValidity        = 10 * 365 * 24 * time.Hour
	clientValidity    = 10 * 365 * 24 * time.Hour
	ephemeralValidity = time.Hour
	serialBits        = 62
	maxCertName       = 80
)

var errNoKey = errors.New("invalid public key")

func newServerCA(project, instance string, now time.Time) (string, string, *SslCert, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", "", nil, err //nolint:wrapcheck // stdlib error is self-describing
	}

	cn := "C=US,O=Google\\, Inc,CN=Google Cloud SQL Server CA,dnQualifier=" + uuid.NewString()
	tpl := &x509.Certificate{
		SerialNumber: serial(), Subject: pkix.Name{CommonName: cn}, NotBefore: now.Add(-time.Minute),
		NotAfter: now.Add(caValidity), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}

	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		return "", "", nil, err //nolint:wrapcheck // stdlib error is self-describing
	}

	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return "", "", nil, err //nolint:wrapcheck // stdlib error is self-describing
	}

	certPEM := pemOf("CERTIFICATE", der)
	info := certInfo(project, instance, der, certPEM, cn, tpl)

	return certPEM, pemOf("PRIVATE KEY", keyDER), info, nil
}

func serial() *big.Int {
	n, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), serialBits))

	return n
}

func pemOf(typ string, der []byte) string {
	return string(pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}))
}

func certInfo(project, instance string, der []byte, certPEM, cn string, tpl *x509.Certificate) *SslCert {
	fp := sha1.Sum(der) //nolint:gosec // fingerprint identifies the certificate, never authenticates

	return &SslCert{
		Kind: kindSslCert, CertSerialNumber: tpl.SerialNumber.String(), Cert: certPEM, CommonName: cn,
		Sha1Fingerprint: hex.EncodeToString(fp[:]), Instance: instance, CreateTime: stamp(tpl.NotBefore),
		ExpirationTime: stamp(tpl.NotAfter),
		SelfLink:       instanceLink(project, instance) + "/sslCerts/" + hex.EncodeToString(fp[:]),
	}
}

func (rec *instRec) signer() (*x509.Certificate, any, error) {
	block, _ := pem.Decode([]byte(rec.CACert))
	kblock, _ := pem.Decode([]byte(rec.CAKey))

	if block == nil || kblock == nil {
		return nil, nil, errNoKey
	}

	ca, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, nil, err //nolint:wrapcheck // stdlib error is self-describing
	}

	key, err := x509.ParsePKCS8PrivateKey(kblock.Bytes)

	return ca, key, err //nolint:wrapcheck // stdlib error is self-describing
}

// signClient issues a client certificate for pub under the instance CA.
func (rec *instRec) signClient(cn string, pub any, now time.Time, validity time.Duration) (*SslCert, error) {
	ca, key, err := rec.signer()
	if err != nil {
		return nil, err
	}

	tpl := &x509.Certificate{
		SerialNumber: serial(), Subject: pkix.Name{CommonName: cn}, NotBefore: now.Add(-time.Minute),
		NotAfter: now.Add(validity), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	der, err := x509.CreateCertificate(rand.Reader, tpl, ca, pub, key)
	if err != nil {
		return nil, err //nolint:wrapcheck // stdlib error is self-describing
	}

	return certInfo(rec.Inst.Project, rec.Inst.Name, der, pemOf("CERTIFICATE", der), cn, tpl), nil
}

func parsePublicKey(text string) (any, error) {
	block, _ := pem.Decode([]byte(text))
	if block == nil {
		return nil, errInvalid("public_key must be a PEM encoded public key.")
	}

	if pub, err := x509.ParsePKIXPublicKey(block.Bytes); err == nil {
		return pub, nil
	}

	if pub, err := x509.ParsePKCS1PublicKey(block.Bytes); err == nil {
		return pub, nil
	}

	return nil, errInvalid("public_key is not a valid RSA, ECDSA or Ed25519 public key.")
}

// CertInsertResult is the sslCerts.insert response payload.
type CertInsertResult struct {
	Operation *Operation
	CA        *SslCert
	Cert      *SslCert
	Key       string
}

// InsertSslCert issues a client certificate and private key.
func (b *InMemoryBackend) InsertSslCert(project, instance, commonName string) (*CertInsertResult, error) {
	if commonName == "" || len(commonName) > maxCertName {
		return nil, errInvalid("Invalid common name: %q.", commonName)
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err //nolint:wrapcheck // stdlib error is self-describing
	}

	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err //nolint:wrapcheck // stdlib error is self-describing
	}

	b.mu.Lock("InsertSslCert")
	defer b.mu.Unlock()

	rec, err := b.opRec(project, instance)
	if err != nil {
		return nil, err
	}

	cert, err := rec.signClient(commonName, &key.PublicKey, b.now(), clientValidity)
	if err != nil {
		return nil, err
	}

	rec.Certs[cert.Sha1Fingerprint] = &certRec{Cert: *cert}
	op := b.submitLocked(rec, project, "UPDATE", opMeta{}, func(context.Context) error { return nil })

	return &CertInsertResult{
		Operation: op,
		CA:        rec.Inst.ServerCaCert,
		Cert:      cert,
		Key:       pemOf("PRIVATE KEY", keyDER),
	}, nil
}

// GetSslCert returns a client certificate by fingerprint.
func (b *InMemoryBackend) GetSslCert(project, instance, fp string) (*SslCert, error) {
	b.mu.RLock("GetSslCert")
	defer b.mu.RUnlock()

	rec, err := b.lookupLocked(project, instance)
	if err != nil {
		return nil, err
	}

	c, ok := rec.Certs[fp]
	if !ok {
		return nil, errNotFound("The SSL certificate does not exist.")
	}

	cp := c.Cert

	return &cp, nil
}

// ListSslCerts lists client certificates by creation time.
func (b *InMemoryBackend) ListSslCerts(project, instance string) ([]*SslCert, error) {
	b.mu.RLock("ListSslCerts")
	defer b.mu.RUnlock()

	rec, err := b.lookupLocked(project, instance)
	if err != nil {
		return nil, err
	}

	out := make([]*SslCert, 0, len(rec.Certs))

	for _, c := range rec.Certs {
		cp := c.Cert
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].CommonName+out[i].Sha1Fingerprint < out[j].CommonName+out[j].Sha1Fingerprint
	})

	return out, nil
}

// DeleteSslCert revokes a client certificate.
func (b *InMemoryBackend) DeleteSslCert(project, instance, fp string) (*Operation, error) {
	b.mu.Lock("DeleteSslCert")
	defer b.mu.Unlock()

	rec, err := b.opRec(project, instance)
	if err != nil {
		return nil, err
	}

	if _, ok := rec.Certs[fp]; !ok {
		return nil, errNotFound("The SSL certificate does not exist.")
	}

	delete(rec.Certs, fp)

	return b.submitLocked(rec, project, "UPDATE", opMeta{}, func(context.Context) error { return nil }), nil
}

// EphemeralCert signs a short-lived client certificate for a caller supplied public key.
func (b *InMemoryBackend) EphemeralCert(project, instance, publicKey, validDuration string) (*SslCert, error) {
	pub, err := parsePublicKey(publicKey)
	if err != nil {
		return nil, err
	}

	validity := ephemeralValidity

	if validDuration != "" {
		if d, perr := time.ParseDuration(validDuration); perr == nil && d > 0 && d < ephemeralValidity {
			validity = d
		}
	}

	b.mu.RLock("EphemeralCert")
	defer b.mu.RUnlock()

	rec, err := b.lookupLocked(project, instance)
	if err != nil {
		return nil, err
	}

	cert, err := rec.signClient("cloudsql-ephemeral-"+strconv.FormatInt(b.now().Unix(), 10), pub, b.now(), validity)
	if err != nil {
		return nil, err
	}

	cert.Sha1Fingerprint = ""

	return cert, nil
}

// ConnectSettings is the connect.get response.
type ConnectSettings struct {
	Kind            string      `json:"kind"`
	ServerCaCert    *SslCert    `json:"serverCaCert,omitempty"`
	DatabaseVersion string      `json:"databaseVersion"`
	BackendType     string      `json:"backendType"`
	Region          string      `json:"region"`
	Endpoint        string      `json:"gopherstackEndpoint,omitempty"`
	IPAddresses     []IPMapping `json:"ipAddresses,omitempty"`
}

// GetConnectSettings returns what a client needs to reach the instance.
func (b *InMemoryBackend) GetConnectSettings(project, instance string) (*ConnectSettings, error) {
	in, err := b.GetInstance(project, instance)
	if err != nil {
		return nil, err
	}

	return &ConnectSettings{
		Kind: kindConnect, ServerCaCert: in.ServerCaCert, DatabaseVersion: in.DatabaseVersion,
		BackendType: in.BackendType, Region: in.Region, IPAddresses: in.IPAddresses, Endpoint: in.GopherstackEndpoint,
	}, nil
}
