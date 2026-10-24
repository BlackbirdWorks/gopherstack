package cloudhsmv2

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"
)

var (
	errHsmKeyUnreadable = errors.New("cloudhsmv2: stored HSM key is unreadable")
	errHsmKeyNotECDSA   = errors.New("cloudhsmv2: HSM key is not ECDSA")
)

const (
	maxCertChars = 5000
	pemCSR       = "CERTIFICATE REQUEST"
	pemCert      = "CERTIFICATE"
	pemKey       = "PRIVATE KEY"
	serialBits   = 120
	certYears    = 20
)

// newClusterCSR returns a PEM CSR signed by a fresh HSM key, plus that key as PKCS#8 PEM.
func newClusterCSR(clusterID string) (string, string, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", "", fmt.Errorf("cloudhsmv2: generate key: %w", err)
	}

	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject:            hsmSubject(clusterID),
		SignatureAlgorithm: x509.ECDSAWithSHA256,
	}, key)
	if err != nil {
		return "", "", fmt.Errorf("cloudhsmv2: create csr: %w", err)
	}

	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return "", "", fmt.Errorf("cloudhsmv2: marshal key: %w", err)
	}

	return string(pem.EncodeToMemory(&pem.Block{Type: pemCSR, Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: pemKey, Bytes: keyDER})), nil
}

func hsmSubject(clusterID string) pkix.Name {
	return pkix.Name{
		Country:            []string{"US"},
		Province:           []string{"CA"},
		Locality:           []string{"San Jose"},
		Organization:       []string{"Cavium Inc."},
		OrganizationalUnit: []string{"N3FIPS"},
		CommonName:         "HSM:" + clusterID,
	}
}

// hardwareCerts is the PEM chain CloudHSM reports once a cluster has an HSM.
type hardwareCerts struct{ Manufacturer, AWS, HSM string }

// issueHardwareCerts builds manufacturer (self-signed) -> AWS hardware -> HSM certificates;
// the HSM certificate certifies the cluster's HSM key.
func issueHardwareCerts(clusterID, hsmKeyPEM string, now time.Time) (hardwareCerts, error) {
	blk, _ := pem.Decode([]byte(hsmKeyPEM))
	if blk == nil {
		return hardwareCerts{}, errHsmKeyUnreadable
	}

	parsed, err := x509.ParsePKCS8PrivateKey(blk.Bytes)
	if err != nil {
		return hardwareCerts{}, fmt.Errorf("cloudhsmv2: parse HSM key: %w", err)
	}

	hsmKey, ok := parsed.(*ecdsa.PrivateKey)
	if !ok {
		return hardwareCerts{}, errHsmKeyNotECDSA
	}

	mfrKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return hardwareCerts{}, fmt.Errorf("cloudhsmv2: generate key: %w", err)
	}

	awsKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return hardwareCerts{}, fmt.Errorf("cloudhsmv2: generate key: %w", err)
	}

	mfrTmpl := certTemplate("Cavium Manufacturer CA", "Cavium Inc.", now, true)

	mfrDER, err := x509.CreateCertificate(rand.Reader, mfrTmpl, mfrTmpl, &mfrKey.PublicKey, mfrKey)
	if err != nil {
		return hardwareCerts{}, fmt.Errorf("cloudhsmv2: create manufacturer certificate: %w", err)
	}

	mfrCert, err := x509.ParseCertificate(mfrDER)
	if err != nil {
		return hardwareCerts{}, fmt.Errorf("cloudhsmv2: parse manufacturer certificate: %w", err)
	}

	awsTmpl := certTemplate("AWS CloudHSM Hardware", "Amazon Web Services", now, true)

	awsDER, err := x509.CreateCertificate(rand.Reader, awsTmpl, mfrCert, &awsKey.PublicKey, mfrKey)
	if err != nil {
		return hardwareCerts{}, fmt.Errorf("cloudhsmv2: create AWS hardware certificate: %w", err)
	}

	awsCert, err := x509.ParseCertificate(awsDER)
	if err != nil {
		return hardwareCerts{}, fmt.Errorf("cloudhsmv2: parse AWS hardware certificate: %w", err)
	}

	hsmTmpl := certTemplate("HSM:"+clusterID, "Cavium Inc.", now, false)
	hsmTmpl.Subject = hsmSubject(clusterID)

	hsmDER, err := x509.CreateCertificate(rand.Reader, hsmTmpl, awsCert, &hsmKey.PublicKey, awsKey)
	if err != nil {
		return hardwareCerts{}, fmt.Errorf("cloudhsmv2: create HSM certificate: %w", err)
	}

	enc := func(der []byte) string { return string(pem.EncodeToMemory(&pem.Block{Type: pemCert, Bytes: der})) }

	return hardwareCerts{Manufacturer: enc(mfrDER), AWS: enc(awsDER), HSM: enc(hsmDER)}, nil
}

func certTemplate(cn, org string, now time.Time, ca bool) *x509.Certificate {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), serialBits))
	if err != nil {
		serial = big.NewInt(now.UnixNano())
	}

	t := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: cn, Organization: []string{org}},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.AddDate(certYears, 0, 0),
		BasicConstraintsValid: true,
		IsCA:                  ca,
		KeyUsage:              x509.KeyUsageDigitalSignature,
	}
	if ca {
		t.KeyUsage |= x509.KeyUsageCertSign
	}

	return t
}

func parseCerts(label, data string) ([]*x509.Certificate, error) {
	if len(data) > maxCertChars {
		return nil, fail(ErrInvalidRequest, "%s must be at most %d characters", label, maxCertChars)
	}

	var certs []*x509.Certificate

	rest := []byte(data)

	for {
		var blk *pem.Block

		blk, rest = pem.Decode(rest)
		if blk == nil {
			break
		}

		if blk.Type != pemCert {
			return nil, fail(ErrInvalidRequest, "%s must contain only PEM certificates", label)
		}

		c, err := x509.ParseCertificate(blk.Bytes)
		if err != nil {
			return nil, fail(ErrInvalidRequest, "%s is not a valid certificate: %v", label, err)
		}

		certs = append(certs, c)
	}

	if len(certs) == 0 || strings.TrimSpace(string(rest)) != "" {
		return nil, fail(ErrInvalidRequest, "%s must be PEM-encoded certificates", label)
	}

	return certs, nil
}

// verifyClusterCert checks that signed certifies the CSR's key and chains to a self-signed trust anchor.
func verifyClusterCert(csrPEM, signedPEM, anchorPEM string, now time.Time) error {
	signed, err := parseCerts("SignedCert", signedPEM)
	if err != nil {
		return err
	}

	anchors, err := parseCerts("TrustAnchor", anchorPEM)
	if err != nil {
		return err
	}

	blk, _ := pem.Decode([]byte(csrPEM))
	if blk == nil {
		return fail(ErrInternalFailure, "stored cluster CSR is unreadable")
	}

	csr, err := x509.ParseCertificateRequest(blk.Bytes)
	if err != nil {
		return fail(ErrInternalFailure, "stored cluster CSR is unreadable")
	}

	pub, ok := csr.PublicKey.(*ecdsa.PublicKey)
	if !ok || !pub.Equal(signed[0].PublicKey) {
		return fail(ErrInvalidRequest, "SignedCert was not issued for this cluster's certificate signing request")
	}

	roots := x509.NewCertPool()

	for _, a := range anchors {
		if a.CheckSignatureFrom(a) != nil {
			return fail(ErrInvalidRequest, "TrustAnchor must be a self-signed root certificate")
		}

		roots.AddCert(a)
	}

	inter := x509.NewCertPool()
	for _, c := range signed[1:] {
		inter.AddCert(c)
	}

	_, err = signed[0].Verify(x509.VerifyOptions{
		Roots: roots, Intermediates: inter, CurrentTime: now,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
	})
	if err != nil {
		return fail(ErrInvalidRequest, "SignedCert does not chain to the TrustAnchor: %v", err)
	}

	return nil
}
