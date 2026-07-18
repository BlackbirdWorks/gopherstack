package redshift

import "fmt"

// ---- HSM Client Certificate ----

// CreateHsmClientCertificate creates a new HSM client certificate.
func (b *InMemoryBackend) CreateHsmClientCertificate(
	id string,
	tagMap map[string]string,
) (*HsmClientCertificate, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: HsmClientCertificateIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateHsmClientCertificate")
	defer b.mu.Unlock()

	if _, exists := b.hsmClientCerts.Get(id); exists {
		return nil, fmt.Errorf("%w: certificate %s already exists", ErrHsmClientCertAlreadyExists, id)
	}

	pubKey := "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA" + id +
		"\n-----END PUBLIC KEY-----"
	cert := &HsmClientCertificate{
		HsmClientCertificateIdentifier: id,
		HsmClientCertificatePublicKey:  pubKey,
		Tags:                           tagMap,
	}
	b.hsmClientCerts.Put(cert)

	cp := *cert

	return &cp, nil
}

// DeleteHsmClientCertificate deletes the named HSM client certificate.
func (b *InMemoryBackend) DeleteHsmClientCertificate(id string) error {
	if id == "" {
		return fmt.Errorf("%w: HsmClientCertificateIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteHsmClientCertificate")
	defer b.mu.Unlock()

	if _, exists := b.hsmClientCerts.Get(id); !exists {
		return fmt.Errorf("%w: certificate %s not found", ErrHsmClientCertNotFound, id)
	}

	b.hsmClientCerts.Delete(id)

	return nil
}

// DescribeHsmClientCertificates returns HSM client certificates, optionally filtered by identifier.
func (b *InMemoryBackend) DescribeHsmClientCertificates(id string) ([]HsmClientCertificate, error) {
	b.mu.RLock("DescribeHsmClientCertificates")
	defer b.mu.RUnlock()

	if id != "" {
		c, exists := b.hsmClientCerts.Get(id)
		if !exists {
			return nil, fmt.Errorf("%w: certificate %s not found", ErrHsmClientCertNotFound, id)
		}

		cp := *c

		return []HsmClientCertificate{cp}, nil
	}

	result := make([]HsmClientCertificate, 0, b.hsmClientCerts.Len())

	for _, c := range b.hsmClientCerts.All() {
		result = append(result, *c)
	}

	return result, nil
}

// ---- HSM Configuration ----

// CreateHsmConfiguration creates a new HSM configuration.
func (b *InMemoryBackend) CreateHsmConfiguration(
	id, description, hsmIPAddress, hsmPartitionName string,
	tagMap map[string]string,
) (*HsmConfiguration, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: HsmConfigurationIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateHsmConfiguration")
	defer b.mu.Unlock()

	if _, exists := b.hsmConfigs.Get(id); exists {
		return nil, fmt.Errorf("%w: configuration %s already exists", ErrHsmConfigAlreadyExists, id)
	}

	cfg := &HsmConfiguration{
		HsmConfigurationIdentifier: id,
		Description:                description,
		HsmIPAddress:               hsmIPAddress,
		HsmPartitionName:           hsmPartitionName,
		Tags:                       tagMap,
	}
	b.hsmConfigs.Put(cfg)

	cp := *cfg

	return &cp, nil
}

// DeleteHsmConfiguration deletes the named HSM configuration.
func (b *InMemoryBackend) DeleteHsmConfiguration(id string) error {
	if id == "" {
		return fmt.Errorf("%w: HsmConfigurationIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteHsmConfiguration")
	defer b.mu.Unlock()

	if _, exists := b.hsmConfigs.Get(id); !exists {
		return fmt.Errorf("%w: configuration %s not found", ErrHsmConfigNotFound, id)
	}

	b.hsmConfigs.Delete(id)

	return nil
}

// DescribeHsmConfigurations returns HSM configurations, optionally filtered by identifier.
func (b *InMemoryBackend) DescribeHsmConfigurations(id string) ([]HsmConfiguration, error) {
	b.mu.RLock("DescribeHsmConfigurations")
	defer b.mu.RUnlock()

	if id != "" {
		c, exists := b.hsmConfigs.Get(id)
		if !exists {
			return nil, fmt.Errorf("%w: configuration %s not found", ErrHsmConfigNotFound, id)
		}

		cp := *c

		return []HsmConfiguration{cp}, nil
	}

	result := make([]HsmConfiguration, 0, b.hsmConfigs.Len())

	for _, c := range b.hsmConfigs.All() {
		result = append(result, *c)
	}

	return result, nil
}
