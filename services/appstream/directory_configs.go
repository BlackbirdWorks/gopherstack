package appstream

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

type storedDirectoryConfig struct {
	CreatedTime                          time.Time                      `json:"createdTime"`
	ServiceAccountCredentials            ServiceAccountCredentials      `json:"serviceAccountCredentials"`
	CertificateBasedAuthProperties       CertificateBasedAuthProperties `json:"certificateBasedAuthProperties"`
	DirectoryName                        string                         `json:"directoryName"`
	Arn                                  string                         `json:"arn"`
	OrganizationalUnitDistinguishedNames []string                       `json:"ouDNs"`
}

func (d *storedDirectoryConfig) toDirectoryConfig() *DirectoryConfig {
	ouDNs := make([]string, len(d.OrganizationalUnitDistinguishedNames)) //nolint:revive,staticcheck // existing issue.
	copy(ouDNs, d.OrganizationalUnitDistinguishedNames)

	return &DirectoryConfig{
		CreatedTime:                          d.CreatedTime,
		OrganizationalUnitDistinguishedNames: ouDNs,
		DirectoryName:                        d.DirectoryName,
		Arn:                                  d.Arn,
		ServiceAccountCredentials:            d.ServiceAccountCredentials,
		CertificateBasedAuthProperties:       d.CertificateBasedAuthProperties,
	}
}

func (b *InMemoryBackend) directoryConfigARN(name string) string {
	return arn.Build("appstream", b.region, b.accountID, fmt.Sprintf("directory-config/%s", name))
}

// CreateDirectoryConfig creates a new directory configuration.
func (b *InMemoryBackend) CreateDirectoryConfig(
	name string,
	ouDNs []string, //nolint:revive,staticcheck // existing issue.
	cred ServiceAccountCredentials,
	certAuth CertificateBasedAuthProperties,
) (*DirectoryConfig, error) {
	b.mu.Lock("CreateDirectoryConfig")
	defer b.mu.Unlock()

	if b.directoryConfigs.Has(name) {
		return nil, ErrAlreadyExists
	}

	arn := b.directoryConfigARN(name)
	dns := make([]string, len(ouDNs))
	copy(dns, ouDNs)

	dc := &storedDirectoryConfig{
		CreatedTime:                          time.Now().UTC(),
		OrganizationalUnitDistinguishedNames: dns,
		DirectoryName:                        name,
		Arn:                                  arn,
		ServiceAccountCredentials:            cred,
		CertificateBasedAuthProperties:       certAuth,
	}
	b.directoryConfigs.Put(dc)

	return dc.toDirectoryConfig(), nil
}

// DeleteDirectoryConfig removes a directory configuration.
func (b *InMemoryBackend) DeleteDirectoryConfig(name string) error {
	b.mu.Lock("DeleteDirectoryConfig")
	defer b.mu.Unlock()

	if !b.directoryConfigs.Has(name) {
		return ErrNotFound
	}

	b.directoryConfigs.Delete(name)

	return nil
}

// DescribeDirectoryConfigs returns directory configs, optionally filtered by name.
func (b *InMemoryBackend) DescribeDirectoryConfigs(names []string) ([]*DirectoryConfig, error) {
	b.mu.RLock("DescribeDirectoryConfigs")
	defer b.mu.RUnlock()

	if len(names) > 0 {
		var result []*DirectoryConfig

		for _, name := range names {
			dc, ok := b.directoryConfigs.Get(name)
			if !ok {
				return nil, ErrNotFound
			}

			result = append(result, dc.toDirectoryConfig())
		}

		return result, nil
	}

	result := make([]*DirectoryConfig, 0, b.directoryConfigs.Len())
	for _, dc := range b.directoryConfigs.All() {
		result = append(result, dc.toDirectoryConfig())
	}

	return result, nil
}

// UpdateDirectoryConfig updates the OUs, service account credentials, and/or
// certificate-based auth properties of a directory configuration. Each
// optional group is only overwritten when the caller supplies a non-empty
// value, matching this backend's existing "empty means unchanged" Update*
// convention.
func (b *InMemoryBackend) UpdateDirectoryConfig(
	name string,
	ouDNs []string, //nolint:revive,staticcheck // existing issue.
	cred ServiceAccountCredentials,
	certAuth CertificateBasedAuthProperties,
) (*DirectoryConfig, error) {
	b.mu.Lock("UpdateDirectoryConfig")
	defer b.mu.Unlock()

	dc, ok := b.directoryConfigs.Get(name)
	if !ok {
		return nil, ErrNotFound
	}

	if len(ouDNs) > 0 {
		dns := make([]string, len(ouDNs))
		copy(dns, ouDNs)
		dc.OrganizationalUnitDistinguishedNames = dns
	}

	if cred.AccountName != "" || cred.AccountPassword != "" {
		dc.ServiceAccountCredentials = cred
	}

	if certAuth.CertificateAuthorityArn != "" || certAuth.Status != "" {
		dc.CertificateBasedAuthProperties = certAuth
	}

	return dc.toDirectoryConfig(), nil
}
