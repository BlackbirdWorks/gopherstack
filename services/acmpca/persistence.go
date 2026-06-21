package acmpca

import (
	"context"
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

// backendSnapshot mirrors the region-nested backend maps (outer key = region).
type backendSnapshot struct {
	CAs          map[string]map[string]*caSnapshot        `json:"cas"`
	Certs        map[string]map[string]*IssuedCertificate `json:"certs"`
	Permissions  map[string]map[string]*Permission        `json:"permissions"`
	AuditReports map[string]map[string]*AuditReport       `json:"auditReports"`
	Policies     map[string]map[string]string             `json:"policies"`
	AccountID    string                                   `json:"accountID"`
	Region       string                                   `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	cas := make(map[string]map[string]*caSnapshot, len(b.cas))
	for region, regionCAs := range b.cas {
		regionMap := make(map[string]*caSnapshot, len(regionCAs))
		for k, ca := range regionCAs {
			snap := &caSnapshot{CertificateAuthority: *ca}
			snap.privKey = nil
			pemStr, err := marshalPrivKey(ca.privKey)
			if err == nil {
				snap.PrivKeyPEM = pemStr
			}
			regionMap[k] = snap
		}
		cas[region] = regionMap
	}

	data, err := json.Marshal(backendSnapshot{
		CAs:          cas,
		Certs:        b.certs,
		Permissions:  b.permissions,
		AuditReports: b.auditReports,
		Policies:     b.policies,
		AccountID:    b.accountID,
		Region:       b.region,
	})
	if err != nil {
		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Certs == nil {
		snap.Certs = make(map[string]map[string]*IssuedCertificate)
	}

	if snap.Permissions == nil {
		snap.Permissions = make(map[string]map[string]*Permission)
	}

	if snap.AuditReports == nil {
		snap.AuditReports = make(map[string]map[string]*AuditReport)
	}

	if snap.Policies == nil {
		snap.Policies = make(map[string]map[string]string)
	}

	cas := make(map[string]map[string]*CertificateAuthority, len(snap.CAs))
	for region, regionCAs := range snap.CAs {
		regionMap := make(map[string]*CertificateAuthority, len(regionCAs))
		for k, s := range regionCAs {
			ca := s.CertificateAuthority

			if s.PrivKeyPEM != "" {
				privKey, err := unmarshalPrivKey(s.PrivKeyPEM)
				if err != nil {
					return fmt.Errorf("restore CA %s private key: %w", k, err)
				}

				ca.privKey = privKey
			}

			regionMap[k] = &ca
		}
		cas[region] = regionMap
	}

	b.cas = cas
	b.certs = snap.Certs
	b.permissions = snap.Permissions
	b.auditReports = snap.AuditReports
	b.policies = snap.Policies
	b.accountID = snap.AccountID
	b.region = snap.Region

	// Rebuild the per-region certsByCASerial index from restored certificates.
	b.certsByCASerial = make(map[string]map[string]string, len(b.certs))
	for region, regionCerts := range b.certs {
		idx := make(map[string]string, len(regionCerts))
		for certARN, cert := range regionCerts {
			idx[cert.CAARN+"#"+cert.Serial] = certARN
		}
		b.certsByCASerial[region] = idx
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
