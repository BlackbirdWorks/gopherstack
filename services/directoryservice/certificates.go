package directoryservice

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// RegisterCertificate registers a certificate.
func (b *InMemoryBackend) RegisterCertificate(
	ctx context.Context,
	directoryID, certData, certType string,
) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("RegisterCertificate")
	defer b.mu.Unlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return "", ErrDirectoryNotFound
	}

	id := fmt.Sprintf("c-%s", uuid.NewString()[:10])
	now := time.Now().UTC()
	b.certificatePut(&storedCertificate{
		region:             region,
		CertificateID:      id,
		DirectoryID:        directoryID,
		CertData:           certData,
		CommonName:         "example.com",
		CertType:           certType,
		State:              "Registered",
		RegisteredDateTime: now,
		ExpiryDateTime:     now.Add(365 * 24 * time.Hour),
	})

	return id, nil
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
	}, nil
}

// --- CA Enrollment Policy ---

// EnableCAEnrollmentPolicy enables CA enrollment policy.
func (b *InMemoryBackend) EnableCAEnrollmentPolicy(ctx context.Context, directoryID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("EnableCAEnrollmentPolicy")
	defer b.mu.Unlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return ErrDirectoryNotFound
	}

	b.caEnrollmentStore(region)[directoryID] = true

	return nil
}

// DisableCAEnrollmentPolicy disables CA enrollment policy.
func (b *InMemoryBackend) DisableCAEnrollmentPolicy(ctx context.Context, directoryID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DisableCAEnrollmentPolicy")
	defer b.mu.Unlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return ErrDirectoryNotFound
	}

	b.caEnrollmentStore(region)[directoryID] = false

	return nil
}

// DescribeCAEnrollmentPolicy returns CA enrollment policy for a directory.
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

	enabled := b.caEnrollmentStore(region)[directoryID]

	return &CAEnrollmentPolicy{DirectoryID: directoryID, Enabled: enabled}, nil
}
