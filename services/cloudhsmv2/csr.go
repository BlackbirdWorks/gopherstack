package cloudhsmv2

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"strings"
	"time"
)

const (
	maxCertChars = 5000
	pemCSR       = "CERTIFICATE REQUEST"
	pemCert      = "CERTIFICATE"
)

// newClusterCSR returns a PEM CSR for a fresh key; the key is dropped because only the HSM would hold it.
func newClusterCSR(clusterID string) (string, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", fmt.Errorf("cloudhsmv2: generate key: %w", err)
	}

	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{
			Country:            []string{"US"},
			Province:           []string{"CA"},
			Locality:           []string{"San Jose"},
			Organization:       []string{"Cavium Inc."},
			OrganizationalUnit: []string{"N3FIPS"},
			CommonName:         "HSM:" + clusterID,
		},
		SignatureAlgorithm: x509.ECDSAWithSHA256,
	}, key)
	if err != nil {
		return "", fmt.Errorf("cloudhsmv2: create csr: %w", err)
	}

	return string(pem.EncodeToMemory(&pem.Block{Type: pemCSR, Bytes: der})), nil
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
