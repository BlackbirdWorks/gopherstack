package directoryservice

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// RegisterCertificate registers a certificate. certData must be a PEM-encoded
// X.509 certificate, matching AWS's real CertificateData contract; CommonName
// and ExpiryDateTime are derived from the parsed certificate (not fabricated),
// mirroring how AWS Directory Service actually validates and reads the cert.
func (b *InMemoryBackend) RegisterCertificate(
	ctx context.Context,
	directoryID, certData, certType, ocspURL string,
) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("RegisterCertificate")
	defer b.mu.Unlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return "", ErrDirectoryNotFound
	}

	cert, parseErr := parseCertificatePEM(certData)
	if parseErr != nil {
		return "", ErrInvalidCertificate
	}

	id := fmt.Sprintf("c-%s", uuid.NewString()[:10])
	now := time.Now().UTC()
	b.certificatePut(&storedCertificate{
		region:             region,
		CertificateID:      id,
		DirectoryID:        directoryID,
		CertData:           certData,
		CommonName:         cert.Subject.CommonName,
		CertType:           certType,
		State:              "Registered",
		RegisteredDateTime: now,
		ExpiryDateTime:     cert.NotAfter,
		OCSPUrl:            ocspURL,
	})

	return id, nil
}

// parseCertificatePEM decodes a single PEM-encoded X.509 certificate block, as
// required by the real RegisterCertificate API contract (CertificateData is
// documented as "The certificate PEM string that needs to be registered.").
func parseCertificatePEM(certData string) (*x509.Certificate, error) {
	block, _ := pem.Decode([]byte(certData))
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, ErrInvalidCertificate
	}

	return x509.ParseCertificate(block.Bytes)
}

// DeregisterCertificate deregisters a certificate.
func (b *InMemoryBackend) DeregisterCertificate(ctx context.Context, directoryID, certID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeregisterCertificate")
	defer b.mu.Unlock()

	cert, ok := b.certificateGet(region, certID)
	if !ok || cert.DirectoryID != directoryID {
		return ErrCertNotFound
	}

	b.certificateDelete(region, certID)

	return nil
}

// ListCertificates returns certificates for a directory.
func (b *InMemoryBackend) ListCertificates(
	ctx context.Context,
	directoryID string,
	limit int32,
	nextToken string,
) ([]CertInfo, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListCertificates")
	defer b.mu.RUnlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return nil, "", ErrDirectoryNotFound
	}

	var ids []string
	for _, cert := range b.certificatesInRegion(region) {
		if cert.DirectoryID == directoryID {
			ids = append(ids, cert.CertificateID)
		}
	}
	sort.Strings(ids)

	start := 0
	if nextToken != "" {
		for i, id := range ids {
			if id == nextToken {
				start = i

				break
			}
		}
	}

	pageSize := int(limit)
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}

	end := min(start+pageSize, len(ids))
	result := make([]CertInfo, 0, end-start)
	for _, id := range ids[start:end] {
		cert, _ := b.certificateGet(region, id)
		result = append(result, CertInfo{
			CertificateID:  cert.CertificateID,
			CommonName:     cert.CommonName,
			CertType:       cert.CertType,
			State:          cert.State,
			ExpiryDateTime: cert.ExpiryDateTime,
		})
	}

	var outToken string
	if end < len(ids) {
		outToken = ids[end]
	}

	return result, outToken, nil
}

// DescribeCertificate returns details of a certificate.
func (b *InMemoryBackend) DescribeCertificate(ctx context.Context, directoryID, certID string) (*CertDetail, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeCertificate")
	defer b.mu.RUnlock()

	cert, ok := b.certificateGet(region, certID)
	if !ok || cert.DirectoryID != directoryID {
		return nil, ErrCertNotFound
	}

	return &CertDetail{
		CertificateID:      cert.CertificateID,
		DirectoryID:        cert.DirectoryID,
		CommonName:         cert.CommonName,
		CertType:           cert.CertType,
		State:              cert.State,
		CertData:           cert.CertData,
		RegisteredDateTime: cert.RegisteredDateTime,
		ExpiryDateTime:     cert.ExpiryDateTime,
		OCSPUrl:            cert.OCSPUrl,
	}, nil
}

// --- CA Enrollment Policy ---

// CaEnrollmentPolicyStatus* mirror aws-sdk-go-v2/service/directoryservice's
// types.CaEnrollmentPolicyStatus enum values (verified against types/enums.go).
const (
	CaEnrollmentPolicyStatusSuccess  = "Success"
	CaEnrollmentPolicyStatusDisabled = "Disabled"
)

// EnableCAEnrollmentPolicy enables CA enrollment policy, persisting the
// required PcaConnectorArn (this emulator cannot reach a real PCA connector,
// so the policy transitions straight to Success rather than modeling
// InProgress).
func (b *InMemoryBackend) EnableCAEnrollmentPolicy(ctx context.Context, directoryID, pcaConnectorArn string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("EnableCAEnrollmentPolicy")
	defer b.mu.Unlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return ErrDirectoryNotFound
	}

	b.caEnrollmentStore(region)[directoryID] = &CAEnrollmentPolicy{
		DirectoryID:         directoryID,
		Status:              CaEnrollmentPolicyStatusSuccess,
		PcaConnectorArn:     pcaConnectorArn,
		LastUpdatedDateTime: time.Now(),
	}

	return nil
}

// DisableCAEnrollmentPolicy disables CA enrollment policy, retaining the
// previously configured PcaConnectorArn (real AWS does not document clearing
// it on disable).
func (b *InMemoryBackend) DisableCAEnrollmentPolicy(ctx context.Context, directoryID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DisableCAEnrollmentPolicy")
	defer b.mu.Unlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return ErrDirectoryNotFound
	}

	store := b.caEnrollmentStore(region)

	policy, ok := store[directoryID]
	if !ok {
		policy = &CAEnrollmentPolicy{DirectoryID: directoryID}
		store[directoryID] = policy
	}

	policy.Status = CaEnrollmentPolicyStatusDisabled
	policy.LastUpdatedDateTime = time.Now()

	return nil
}

// DescribeCAEnrollmentPolicy returns CA enrollment policy for a directory. A
// directory that has never called EnableCAEnrollmentPolicy is Disabled with
// no PcaConnectorArn, mirroring real AWS's default (never-enrolled) state.
func (b *InMemoryBackend) DescribeCAEnrollmentPolicy(
	ctx context.Context,
	directoryID string,
) (*CAEnrollmentPolicy, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeCAEnrollmentPolicy")
	defer b.mu.RUnlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return nil, ErrDirectoryNotFound
	}

	if policy, ok := b.caEnrollmentStoreRO(region)[directoryID]; ok {
		clone := *policy

		return &clone, nil
	}

	return &CAEnrollmentPolicy{DirectoryID: directoryID, Status: CaEnrollmentPolicyStatusDisabled}, nil
}
