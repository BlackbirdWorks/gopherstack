package elbv2

import (
	"fmt"
)

// AddListenerCertificates adds certificates to a listener.
func (b *InMemoryBackend) AddListenerCertificates(listenerArn string, certs []Certificate) error {
	b.mu.Lock("AddListenerCertificates")
	defer b.mu.Unlock()

	listener, ok := b.listeners.Get(listenerArn)
	if !ok {
		return ErrListenerNotFound
	}

	existing := make(map[string]bool, len(listener.Certificates))
	for _, c := range listener.Certificates {
		existing[c.CertificateArn] = true
	}

	for _, c := range certs {
		if !existing[c.CertificateArn] {
			listener.Certificates = append(listener.Certificates, c)
			existing[c.CertificateArn] = true
		}
	}

	return nil
}

// DescribeListenerCertificates returns certificates on a listener.
func (b *InMemoryBackend) DescribeListenerCertificates(listenerArn string) ([]Certificate, error) {
	b.mu.RLock("DescribeListenerCertificates")
	defer b.mu.RUnlock()

	listener, ok := b.listeners.Get(listenerArn)
	if !ok {
		return nil, ErrListenerNotFound
	}

	result := make([]Certificate, len(listener.Certificates))
	copy(result, listener.Certificates)

	return result, nil
}

// RemoveListenerCertificates removes certificate ARNs from a listener.
func (b *InMemoryBackend) RemoveListenerCertificates(listenerArn string, certArns []string) error {
	b.mu.Lock("RemoveListenerCertificates")
	defer b.mu.Unlock()

	listener, ok := b.listeners.Get(listenerArn)
	if !ok {
		return ErrListenerNotFound
	}

	remove := make(map[string]bool, len(certArns))
	for _, c := range certArns {
		remove[c] = true
	}

	remaining := make([]Certificate, 0, len(listener.Certificates))
	for _, c := range listener.Certificates {
		if !remove[c.CertificateArn] {
			remaining = append(remaining, c)
		}
	}

	if len(remaining) == 0 && (listener.Protocol == protoHTTPS || listener.Protocol == protoTLS) {
		return fmt.Errorf(
			"%w: Cannot remove the last certificate from an HTTPS/TLS listener",
			ErrInvalidParameter,
		)
	}

	listener.Certificates = remaining

	return nil
}
