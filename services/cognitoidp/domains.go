package cognitoidp

import "fmt"

// CreateUserPoolDomainFull creates a user pool domain with optional custom domain cert
// and managed login version. managedLoginVersion of 0 defaults to 1 (hosted UI
// classic) -- AWS's ManagedLoginVersion request field is optional and undocumented for
// its unset default, so this backend picks the legacy value, the only one that doesn't
// implicitly claim managed-login branding the caller never configured.
func (b *InMemoryBackend) CreateUserPoolDomainFull(
	userPoolID, domain, certificateArn string, managedLoginVersion int32,
) (*UserPoolDomain, error) {
	b.mu.Lock("CreateUserPoolDomainFull")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, exists := b.domains.Get(domain); exists {
		return nil, fmt.Errorf("%w: domain %q already exists", ErrAlreadyExists, domain)
	}

	// Custom domains get a CloudFront distribution domain; managed domains use the Cognito URL.
	cfDomain := domain + ".auth." + b.region + ".amazoncognito.com"
	if certificateArn != "" {
		cfDomain = "d" + randomAlphanumeric(cloudFrontDistIDLen) + ".cloudfront.net"
	}

	mlv := managedLoginVersion
	if mlv == 0 {
		mlv = managedLoginVersionClassic
	}

	d := &UserPoolDomain{
		Domain:                 domain,
		UserPoolID:             userPoolID,
		CloudFrontDistribution: cfDomain,
		CertificateArn:         certificateArn,
		Status:                 "ACTIVE",
		ManagedLoginVersion:    mlv,
		S3Bucket:               domainAssetsBucket(b.region),
		AWSAccountID:           b.accountID,
	}
	b.domains.Put(d)

	cp := *d

	return &cp, nil
}

// UpdateUserPoolDomainFull updates a domain's certificate ARN and/or managed login
// version and returns the resulting domain. managedLoginVersion of 0 leaves the
// existing stored value unchanged (AWS's ManagedLoginVersion request field is
// optional -- an update that omits it does not reset the domain's branding version).
func (b *InMemoryBackend) UpdateUserPoolDomainFull(
	userPoolID, domain, certificateArn string, managedLoginVersion int32,
) (*UserPoolDomain, error) {
	b.mu.Lock("UpdateUserPoolDomainFull")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	d, ok := b.domains.Get(domain)
	if !ok {
		return nil, fmt.Errorf("%w: domain %q not found", ErrUserPoolNotFound, domain)
	}

	if certificateArn != "" {
		d.CertificateArn = certificateArn
		d.CloudFrontDistribution = "d" + randomAlphanumeric(cloudFrontDistIDLen) + ".cloudfront.net"
	}

	if managedLoginVersion != 0 {
		d.ManagedLoginVersion = managedLoginVersion
	}

	cp := *d

	return &cp, nil
}

// CreateUserPoolDomain registers a domain for a user pool.
func (b *InMemoryBackend) CreateUserPoolDomain(userPoolID, domain string) (*UserPoolDomain, error) {
	b.mu.Lock("CreateUserPoolDomain")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, exists := b.domains.Get(domain); exists {
		return nil, fmt.Errorf("%w: domain %q already exists", ErrAlreadyExists, domain)
	}

	d := &UserPoolDomain{
		Domain:                 domain,
		UserPoolID:             userPoolID,
		CloudFrontDistribution: domain + ".auth." + b.region + ".amazoncognito.com",
		Status:                 "ACTIVE",
		ManagedLoginVersion:    managedLoginVersionClassic,
		S3Bucket:               domainAssetsBucket(b.region),
		AWSAccountID:           b.accountID,
	}
	b.domains.Put(d)

	cp := *d

	return &cp, nil
}

// DescribeUserPoolDomain returns domain details by domain name.
func (b *InMemoryBackend) DescribeUserPoolDomain(domain string) (*UserPoolDomain, error) {
	b.mu.RLock("DescribeUserPoolDomain")
	defer b.mu.RUnlock()

	d, ok := b.domains.Get(domain)
	if !ok {
		return nil, fmt.Errorf("%w: domain %q not found", ErrUserPoolNotFound, domain)
	}

	cp := *d

	return &cp, nil
}

// FindUserPoolDomain returns a domain by name, or nil if not found (no error).
// Use instead of DescribeUserPoolDomain when the caller treats "not found" as an empty result.
func (b *InMemoryBackend) FindUserPoolDomain(domain string) *UserPoolDomain {
	b.mu.RLock("FindUserPoolDomain")
	defer b.mu.RUnlock()

	d, _ := b.domains.Get(domain)
	if d == nil {
		return nil
	}

	cp := *d

	return &cp
}

// UpdateUserPoolDomain updates a domain (e.g., custom certificate). Returns the cloudfront domain.
func (b *InMemoryBackend) UpdateUserPoolDomain(userPoolID, domain string) (string, error) {
	b.mu.Lock("UpdateUserPoolDomain")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return "", fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	d, ok := b.domains.Get(domain)
	if !ok {
		return "", fmt.Errorf("%w: domain %q not found", ErrUserPoolNotFound, domain)
	}

	return d.CloudFrontDistribution, nil
}

// DeleteUserPoolDomain removes a domain from a user pool.
func (b *InMemoryBackend) DeleteUserPoolDomain(userPoolID, domain string) error {
	b.mu.Lock("DeleteUserPoolDomain")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.domains.Get(domain); !ok {
		return fmt.Errorf("%w: domain %q not found", ErrUserPoolNotFound, domain)
	}

	b.domains.Delete(domain)

	return nil
}

// managedLoginVersionClassic is AWS's documented value for hosted UI (classic)
// branding, the default when a domain's ManagedLoginVersion is never set.
const managedLoginVersionClassic = 1

// domainAssetsBucket synthesizes the S3 bucket name DescribeUserPoolDomain reports for
// a domain's static assets -- an AWS-internal implementation detail no caller can set
// or meaningfully validate, so this mirrors the existing CloudFrontDistribution
// synthesis above rather than leaving the field unpopulated.
func domainAssetsBucket(region string) string {
	return "aws-cognito-prod-" + region + "-assets"
}
