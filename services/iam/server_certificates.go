package iam

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"
)

// serverCertIDPrefix is the AWS-style prefix for server certificate IDs.
const serverCertIDPrefix = "ASCA"

// serverCertIDBytes is the number of random bytes for the server certificate ID suffix.
const serverCertIDBytes = 8

func newServerCertID() string {
	b := make([]byte, serverCertIDBytes)
	_, _ = rand.Read(b)

	return serverCertIDPrefix + strings.ToUpper(hex.EncodeToString(b))
}

// UploadServerCertificate stores a new server certificate.
func (b *InMemoryBackend) UploadServerCertificate(name, path, certBody, certChain string) (*ServerCertificate, error) {
	b.mu.Lock("UploadServerCertificate")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: ServerCertificateName must not be empty", ErrInvalidInput)
	}

	if certBody == "" {
		return nil, fmt.Errorf("%w: CertificateBody must not be empty", ErrMalformedCertificate)
	}

	if _, exists := b.serverCertificates.Get(name); exists {
		return nil, fmt.Errorf("%w: server certificate %q already exists", ErrUserAlreadyExists, name)
	}

	normalizedPath := normPath(path)
	cert := ServerCertificate{
		UploadDate:            time.Now().UTC(),
		ServerCertificateName: name,
		ServerCertificateID:   newServerCertID(),
		Arn:                   "arn:aws:iam::" + b.accountID + ":server-certificate" + normalizedPath + name,
		Path:                  normalizedPath,
		CertificateBody:       certBody,
		CertificateChain:      certChain,
	}

	b.serverCertificates.Put(&cert)

	return &cert, nil
}

// GetServerCertificate retrieves a server certificate by name.
func (b *InMemoryBackend) GetServerCertificate(name string) (*ServerCertificate, error) {
	b.mu.RLock("GetServerCertificate")
	defer b.mu.RUnlock()

	cert, exists := b.serverCertificates.Get(name)
	if !exists {
		return nil, fmt.Errorf("%w: server certificate %q not found", ErrUserNotFound, name)
	}

	return cert, nil
}

// ListServerCertificates returns server certificates, filtered by path prefix if non-empty.
func (b *InMemoryBackend) ListServerCertificates(pathPrefix string) ([]ServerCertificate, error) {
	b.mu.RLock("ListServerCertificates")
	defer b.mu.RUnlock()

	var result []ServerCertificate

	for _, cert := range b.serverCertificates.All() {
		if pathPrefix == "" || strings.HasPrefix(cert.Path, pathPrefix) {
			result = append(result, *cert)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ServerCertificateName < result[j].ServerCertificateName
	})

	return result, nil
}

// UpdateServerCertificate renames a server certificate and/or changes its path.
func (b *InMemoryBackend) UpdateServerCertificate(name, newName, newPath string) error {
	b.mu.Lock("UpdateServerCertificate")
	defer b.mu.Unlock()

	cert, exists := b.serverCertificates.Get(name)
	if !exists {
		return fmt.Errorf("%w: server certificate %q not found", ErrUserNotFound, name)
	}

	if newName != "" && newName != name {
		if _, nameExists := b.serverCertificates.Get(newName); nameExists {
			return fmt.Errorf("%w: server certificate %q already exists", ErrUserAlreadyExists, newName)
		}

		// serverCertificates is keyed by ServerCertificateName, which is
		// changing here -- explicit Delete(old)+Put (rather than Put alone)
		// is required so the table's key stays in sync with the mutated
		// value, matching store.Table's key-is-a-pure-function-of-value
		// contract (see store_setup.go).
		b.serverCertificates.Delete(name)
		cert.ServerCertificateName = newName
		cert.Arn = "arn:aws:iam::" + b.accountID + ":server-certificate" + cert.Path + newName
	}

	if newPath != "" {
		normalizedPath := normPath(newPath)
		cert.Path = normalizedPath
		cert.Arn = "arn:aws:iam::" + b.accountID + ":server-certificate" + normalizedPath + cert.ServerCertificateName
	}

	b.serverCertificates.Put(cert)

	return nil
}

// DeleteServerCertificate removes a server certificate.
func (b *InMemoryBackend) DeleteServerCertificate(name string) error {
	b.mu.Lock("DeleteServerCertificate")
	defer b.mu.Unlock()

	if _, exists := b.serverCertificates.Get(name); !exists {
		return fmt.Errorf("%w: server certificate %q not found", ErrUserNotFound, name)
	}

	b.serverCertificates.Delete(name)

	return nil
}
