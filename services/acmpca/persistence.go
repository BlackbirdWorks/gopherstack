package acmpca

import (
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
)

var (
	errDecodePEM   = errors.New("failed to decode private key PEM")
	errNotECDSAKey = errors.New("private key is not *ecdsa.PrivateKey")
)

type caSnapshot struct {
	CertificateAuthority

	PrivKeyPEM string `json:"privKeyPEM,omitempty"`
}

func marshalPrivKey(key *ecdsa.PrivateKey) (string, error) {
	if key == nil {
		return "", nil
	}

	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return "", fmt.Errorf("marshal private key: %w", err)
	}

	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})), nil
}

func unmarshalPrivKey(pemStr string) (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		return nil, errDecodePEM
	}

	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse private key: %w", err)
	}

	ecKey, ok := key.(*ecdsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("%w: got %T", errNotECDSAKey, key)
	}

	return ecKey, nil
}

type backendSnapshot struct {
	CAs       map[string]*caSnapshot        `json:"cas"`
	Certs     map[string]*IssuedCertificate `json:"certs"`
	AccountID string                        `json:"accountID"`
	Region    string                        `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	cas := make(map[string]*caSnapshot, len(b.cas))
	for k, ca := range b.cas {
		snap := &caSnapshot{CertificateAuthority: *ca}
		snap.privKey = nil
		pemStr, err := marshalPrivKey(ca.privKey)
		if err == nil {
			snap.PrivKeyPEM = pemStr
		}
		cas[k] = snap
	}

	data, _ := json.Marshal(backendSnapshot{
		CAs:       cas,
		Certs:     b.certs,
		AccountID: b.accountID,
		Region:    b.region,
	})

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.CAs == nil {
		snap.CAs = make(map[string]*caSnapshot)
	}

	if snap.Certs == nil {
		snap.Certs = make(map[string]*IssuedCertificate)
	}

	cas := make(map[string]*CertificateAuthority, len(snap.CAs))
	for k, s := range snap.CAs {
		ca := s.CertificateAuthority

		if s.PrivKeyPEM != "" {
			privKey, err := unmarshalPrivKey(s.PrivKeyPEM)
			if err != nil {
				return fmt.Errorf("restore CA %s private key: %w", k, err)
			}

			ca.privKey = privKey
		}

		cas[k] = &ca
	}

	b.cas = cas
	b.certs = snap.Certs
	b.accountID = snap.AccountID
	b.region = snap.Region

	// Rebuild certsByCASerial index from restored certificates.
	b.certsByCASerial = make(map[string]string, len(b.certs))
	for certARN, cert := range b.certs {
		b.certsByCASerial[cert.CAARN+"#"+cert.Serial] = certARN
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte { return h.Backend.Snapshot() }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error { return h.Backend.Restore(data) }
