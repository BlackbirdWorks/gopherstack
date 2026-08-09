package apigateway

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// GenerateClientCertificate creates a new client certificate for mutual TLS.
func (b *InMemoryBackend) GenerateClientCertificate(input GenerateClientCertificateInput) (*ClientCertificate, error) {
	b.mu.Lock("GenerateClientCertificate")
	defer b.mu.Unlock()

	id := randomID(apiIDLength)
	now := time.Now()
	cert := &ClientCertificate{
		Tags:                  tags.FromMap("apigw.clientcert."+id+".tags", input.Tags),
		ClientCertificateID:   id,
		Description:           input.Description,
		PemEncodedCertificate: "-----BEGIN CERTIFICATE-----\nMIICpDCCAYwCCQDU...(mock)...\n-----END CERTIFICATE-----",
		CreatedDate:           unixEpochTime{now},
		ExpirationDate:        unixEpochTime{now.AddDate(0, 0, clientCertValidityDays)},
	}

	b.clientCertificates.Put(cert)

	return cert, nil
}

// GetClientCertificate returns a client certificate by ID.
func (b *InMemoryBackend) GetClientCertificate(id string) (*ClientCertificate, error) {
	b.mu.RLock("GetClientCertificate")
	defer b.mu.RUnlock()

	cert, ok := b.clientCertificates.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: client certificate %s not found", ErrNotFound, id)
	}

	return cert, nil
}

// GetClientCertificates returns all client certificates.
func (b *InMemoryBackend) GetClientCertificates() ([]ClientCertificate, error) {
	b.mu.RLock("GetClientCertificates")
	defer b.mu.RUnlock()

	all := b.clientCertificates.All()
	result := make([]ClientCertificate, 0, len(all))
	for _, c := range all {
		result = append(result, *c)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ClientCertificateID < result[j].ClientCertificateID
	})

	return result, nil
}

// DeleteClientCertificate removes a client certificate.
func (b *InMemoryBackend) DeleteClientCertificate(id string) error {
	b.mu.Lock("DeleteClientCertificate")
	defer b.mu.Unlock()

	if !b.clientCertificates.Delete(id) {
		return fmt.Errorf("%w: client certificate %s not found", ErrNotFound, id)
	}

	return nil
}

// UpdateClientCertificate updates the description of a client certificate.
func (b *InMemoryBackend) UpdateClientCertificate(input UpdateClientCertificateInput) (*ClientCertificate, error) {
	b.mu.Lock("UpdateClientCertificate")
	defer b.mu.Unlock()

	cert, ok := b.clientCertificates.Get(input.ClientCertificateID)
	if !ok {
		return nil, fmt.Errorf("%w: client certificate %s not found", ErrNotFound, input.ClientCertificateID)
	}

	cert.Description = input.Description

	return cert, nil
}
