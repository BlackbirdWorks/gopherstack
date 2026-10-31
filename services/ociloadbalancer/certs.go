package ociloadbalancer

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"strings"
)

type certIn struct {
	Name       string `json:"certificateName"`
	Passphrase string `json:"passphrase"`
	PrivateKey string `json:"privateKey"`
	Public     string `json:"publicCertificate"`
	CA         string `json:"caCertificate"`
}

func countPEMCerts(data string) (int, bool) {
	rest, n := []byte(data), 0

	for {
		var blk *pem.Block

		blk, rest = pem.Decode(rest)
		if blk == nil {
			return n, true
		}

		if blk.Type != "CERTIFICATE" {
			return n, false
		}

		if _, err := x509.ParseCertificate(blk.Bytes); err != nil {
			return n, false
		}

		n++
	}
}

func caChain(ca string) [][]byte {
	rest, out := []byte(ca), [][]byte{}

	for {
		var blk *pem.Block

		blk, rest = pem.Decode(rest)
		if blk == nil {
			return out
		}

		out = append(out, blk.Bytes)
	}
}

func encryptedKey(key string) bool {
	blk, _ := pem.Decode([]byte(key))

	return blk != nil &&
		(blk.Type == "ENCRYPTED PRIVATE KEY" || strings.Contains(blk.Headers["Proc-Type"], "ENCRYPTED"))
}

// buildCertificate validates a certificate bundle. The returned TLS key pair (nil without a private key) is
// kept in memory for listener TLS termination; the private key is never stored in the resource or returned.
func buildCertificate(in *certIn) (*Certificate, *tls.Certificate, *APIError) {
	if err := checkName("certificate", in.Name); err != nil {
		return nil, nil, err
	}

	if in.Public == "" && in.CA == "" {
		return nil, nil, errMissing("publicCertificate or caCertificate is required")
	}

	if n, ok := countPEMCerts(in.CA); in.CA != "" && (!ok || n == 0) {
		return nil, nil, errParam("caCertificate is not a valid PEM certificate bundle")
	}

	if in.Public == "" {
		if in.PrivateKey != "" {
			return nil, nil, errParam("privateKey requires publicCertificate")
		}

		return &Certificate{Name: in.Name, CA: in.CA}, nil, nil
	}

	if n, ok := countPEMCerts(in.Public); !ok || n == 0 {
		return nil, nil, errParam("publicCertificate is not a valid PEM certificate")
	}

	cert := &Certificate{Name: in.Name, Public: in.Public, CA: in.CA}
	if in.PrivateKey == "" {
		return cert, nil, nil
	}

	if encryptedKey(in.PrivateKey) {
		return nil, nil, errUnsupported("passphrase-protected private keys")
	}

	pair, err := tls.X509KeyPair([]byte(in.Public), []byte(in.PrivateKey))
	if err != nil {
		return nil, nil, errParam("publicCertificate and privateKey are not a matching PEM pair: " + err.Error())
	}

	if len(pair.Certificate) == 1 {
		pair.Certificate = append(pair.Certificate, caChain(in.CA)...)
	}

	return cert, &pair, nil
}

func (b *InMemoryBackend) createCertificate(lb *LoadBalancer, in *certIn) *APIError {
	if _, dup := lb.Certificates[in.Name]; dup {
		return errConflict("certificate " + in.Name + " already exists")
	}

	cert, key, err := buildCertificate(in)
	if err != nil {
		return err
	}

	lb.Certificates[in.Name] = cert

	if key != nil {
		b.keys[lb.ID+"/"+in.Name] = key
	}

	return nil
}

func (b *InMemoryBackend) deleteCertificate(lb *LoadBalancer, name string) *APIError {
	if _, ok := lb.Certificates[name]; !ok {
		return errNotFound(name)
	}

	delete(lb.Certificates, name)

	return nil
}
