package iam

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// certIDPrefix is the AWS prefix for signing certificate IDs.
const certIDPrefix = "ASCA"

// certIDBytes is the number of random bytes for signing certificate ID suffix.
const certIDBytes = 8

// newSigningCertID generates a new unique signing certificate ID.
func newSigningCertID() string {
	b := make([]byte, certIDBytes)
	_, _ = rand.Read(b)

	return certIDPrefix + strings.ToUpper(hex.EncodeToString(b))
}

// UploadSigningCertificate stores a new X.509 signing certificate for a user.
func (b *InMemoryBackend) UploadSigningCertificate(userName, body string) (*SigningCertificate, error) {
	b.mu.Lock("UploadSigningCertificate")
	defer b.mu.Unlock()

	if _, exists := b.users.Get(userName); !exists {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	if body == "" {
		return nil, fmt.Errorf("%w: certificate body must not be empty", ErrMalformedPolicyDocument)
	}

	cert := SigningCertificate{
		CertificateID:   newSigningCertID(),
		UserName:        userName,
		CertificateBody: body,
		Status:          accessKeyStatusActive,
		UploadDate:      time.Now().UTC(),
	}

	b.signingCertificates.Put(&cert)

	return &cert, nil
}

// ListSigningCertificates returns a paginated list of signing certificates
// for the given user. If userName is empty, all certificates are returned
// (admin usage).
func (b *InMemoryBackend) ListSigningCertificates(
	userName, marker string, maxItems int,
) (page.Page[SigningCertificate], error) {
	b.mu.RLock("ListSigningCertificates")
	defer b.mu.RUnlock()

	if userName != "" {
		if _, exists := b.users.Get(userName); !exists {
			return page.Page[SigningCertificate]{}, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
		}
	}

	var result []SigningCertificate

	for _, cert := range b.signingCertificates.All() {
		if userName == "" || cert.UserName == userName {
			result = append(result, *cert)
		}
	}

	sort.Slice(result, func(i, j int) bool { return result[i].CertificateID < result[j].CertificateID })

	return page.New(result, marker, maxItems, iamDefaultMaxItems), nil
}

// UpdateSigningCertificate changes the status of a signing certificate (Active/Inactive).
func (b *InMemoryBackend) UpdateSigningCertificate(userName, certificateID, status string) error {
	b.mu.Lock("UpdateSigningCertificate")
	defer b.mu.Unlock()

	cert, exists := b.signingCertificates.Get(certificateID)
	if !exists || cert.UserName != userName {
		return fmt.Errorf(
			"%w: signing certificate %q not found for user %q",
			ErrAccessKeyNotFound, certificateID, userName,
		)
	}

	const inactive = "Inactive"

	if status != accessKeyStatusActive && status != inactive {
		return fmt.Errorf(
			"%w: invalid status %q; must be %s or Inactive",
			ErrInvalidAction, status, accessKeyStatusActive,
		)
	}

	cert.Status = status
	b.signingCertificates.Put(cert)

	return nil
}

// DeleteSigningCertificate removes a signing certificate.
func (b *InMemoryBackend) DeleteSigningCertificate(userName, certificateID string) error {
	b.mu.Lock("DeleteSigningCertificate")
	defer b.mu.Unlock()

	cert, exists := b.signingCertificates.Get(certificateID)
	if !exists || cert.UserName != userName {
		return fmt.Errorf(
			"%w: signing certificate %q not found for user %q",
			ErrAccessKeyNotFound, certificateID, userName,
		)
	}

	b.signingCertificates.Delete(certificateID)

	return nil
}
