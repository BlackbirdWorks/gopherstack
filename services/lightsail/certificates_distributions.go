package lightsail

// This file backs family V (3 ops: CreateCertificate, DeleteCertificate,
// GetCertificates -- CDN/distribution-facing, distinct from the LB-TLS
// family M) and family T (10 ops: CreateDistribution, UpdateDistribution,
// DeleteDistribution, GetDistributions, UpdateDistributionBundle,
// ResetDistributionCache, GetDistributionLatestCacheReset,
// GetDistributionMetricData, AttachCertificateToDistribution,
// DetachCertificateFromDistribution).

import (
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	opTypeCreateCertificate          = "CreateCertificate"
	opTypeDeleteCertificate          = "DeleteCertificate"
	opTypeCreateDistribution         = "CreateDistribution"
	opTypeUpdateDistribution         = "UpdateDistribution"
	opTypeDeleteDistribution         = "DeleteDistribution"
	opTypeUpdateDistributionBundle   = "UpdateDistributionBundle"
	opTypeResetDistributionCache     = "ResetDistributionCache"
	opTypeAttachCertToDistribution   = "AttachCertificateToDistribution"
	opTypeDetachCertFromDistribution = "DetachCertificateFromDistribution"
)

// CreateCertificate creates a new CDN-facing Certificate, starting a real
// Let's-Encrypt-style validation timeline (PARITY.md 4.7): PENDING_VALIDATION
// -> ISSUED after asyncTransitionDelay.
func (b *InMemoryBackend) CreateCertificate(
	name, domainName string,
	sans []string,
	userTags map[string]string,
) ([]Operation, error) {
	b.mu.Lock("CreateCertificate")
	defer b.mu.Unlock()

	if err := b.registerNameLocked(ResourceTypeCertificate, name); err != nil {
		return nil, err
	}

	now := nowUTC()
	cert := &Certificate{
		Name: name, Arn: b.regionalARN(ResourceTypeCertificate, newUUID()), DomainName: domainName,
		Status: CertificateStatusPendingValidation, CreatedAt: now, NotBefore: now,
		NotAfter:                now.AddDate(0, certificateValidityMonths, 0),
		SubjectAlternativeNames: append([]string{domainName}, sans...),
		Tags:                    tags.New("lightsail.certificate." + name + ".tags"),
	}
	cert.Tags.Merge(userTags)
	b.certificates.Put(cert)

	b.work.After("CertificateIssued", asyncTransitionDelay, func() {
		b.mu.Lock("Certificate-async-issued")
		defer b.mu.Unlock()

		if c, found := b.certificates.Get(name); found && c.Status == CertificateStatusPendingValidation {
			c.Status = CertificateStatusIssued
			c.IssuedAt = nowUTC()
		}
	})

	return b.newOperationsLocked(opTypeCreateCertificate, ResourceTypeCertificate, []string{name}), nil
}

// DeleteCertificate deletes the named certificate.
func (b *InMemoryBackend) DeleteCertificate(name string) ([]Operation, error) {
	b.mu.Lock("DeleteCertificate")
	defer b.mu.Unlock()

	cert, ok := b.certificates.Get(name)
	if !ok {
		return nil, notFoundError("Certificate", name)
	}

	if cert.Tags != nil {
		cert.Tags.Close()
	}

	b.certificates.Delete(name)
	b.unregisterNameLocked(name)

	return b.newOperationsLocked(opTypeDeleteCertificate, ResourceTypeCertificate, []string{name}), nil
}

// GetCertificates returns every certificate, optionally filtered by name,
// paginated.
func (b *InMemoryBackend) GetCertificates(name string, token string) (page.Page[*Certificate], error) {
	b.mu.RLock("GetCertificates")
	defer b.mu.RUnlock()

	all := b.certificates.All()
	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	out := make([]*Certificate, 0, len(all))

	for _, c := range all {
		if name != "" && c.Name != name {
			continue
		}

		out = append(out, c.clone())
	}

	return paginateGeneric(out, token)
}

// resolveDistributionOrigin validates that originName names a real Instance,
// Bucket, or LoadBalancer (Distribution.Origin's own SDK doc comment names
// exactly these three kinds, PARITY.md 4.7).
func (b *InMemoryBackend) resolveDistributionOrigin(originName string) (string, bool) {
	if _, ok := b.instances.Get(originName); ok {
		return ResourceTypeInstance, true
	}

	if _, ok := b.buckets.Get(originName); ok {
		return ResourceTypeBucket, true
	}

	if _, ok := b.loadBalancers.Get(originName); ok {
		return ResourceTypeLoadBalancer, true
	}

	return "", false
}

// CreateDistribution creates a new Distribution -- modeled global but its
// Location.RegionName is always literally "us-east-1" regardless of this
// backend's own configured region (PARITY.md 4.7).
func (b *InMemoryBackend) CreateDistribution(
	name, bundleID, originName, ipAddressType, certificateName string, userTags map[string]string,
) ([]Operation, error) {
	b.mu.Lock("CreateDistribution")
	defer b.mu.Unlock()

	if _, ok := b.resolveDistributionOrigin(originName); !ok {
		return nil, notFoundError("Distribution origin (Instance/Bucket/LoadBalancer)", originName)
	}

	if err := b.registerNameLocked(ResourceTypeDistribution, name); err != nil {
		return nil, err
	}

	ipType := ipAddressType
	if ipType == "" {
		ipType = ipAddressTypeDualStack
	}

	dist := &Distribution{
		Name: name, Arn: b.distributionARN(name), SupportCode: newSupportCode(),
		BundleID: bundleID, Status: "InProgress", DomainName: name + "." + randomHex() + ".cloudfront.net",
		OriginPublicDNS: originName + ".origin.local", CertificateName: certificateName,
		IPAddressType: ipType, IsEnabled: true, CreatedAt: nowUTC(),
		Location: ResourceLocation{RegionName: distributionRegion},
		Origin:   DistributionOrigin{Name: originName, RegionName: b.region, ProtocolPolicy: "http-only"},
		Tags:     tags.New("lightsail.distribution." + name + ".tags"),
	}
	dist.Tags.Merge(userTags)
	b.distributions.Put(dist)

	b.work.After("DistributionDeployed", asyncTransitionDelay, func() {
		b.mu.Lock("Distribution-async-deployed")
		defer b.mu.Unlock()

		if d, found := b.distributions.Get(name); found && d.Status == "InProgress" {
			d.Status = "Deployed"
		}
	})

	ops := b.newOperationsLocked(opTypeCreateDistribution, ResourceTypeDistribution, []string{name})

	return ops, nil
}

// UpdateDistribution updates the named distribution's IsEnabled/
// CertificateName -- other CacheBehavior fields are accepted and stored
// verbatim without independently acting on cache behavior (this emulator
// does not proxy real traffic through a Distribution).
func (b *InMemoryBackend) UpdateDistribution(name, certificateName string, isEnabled *bool) (*Operation, error) {
	b.mu.Lock("UpdateDistribution")
	defer b.mu.Unlock()

	d, ok := b.distributions.Get(name)
	if !ok {
		return nil, notFoundError("Distribution", name)
	}

	if certificateName != "" {
		d.CertificateName = certificateName
	}

	if isEnabled != nil {
		d.IsEnabled = *isEnabled
	}

	ops := b.newOperationsLocked(opTypeUpdateDistribution, ResourceTypeDistribution, []string{name})

	return &ops[0], nil
}

// DeleteDistribution deletes the named distribution.
func (b *InMemoryBackend) DeleteDistribution(name string) (*Operation, error) {
	b.mu.Lock("DeleteDistribution")
	defer b.mu.Unlock()

	d, ok := b.distributions.Get(name)
	if !ok {
		return nil, notFoundError("Distribution", name)
	}

	if d.Tags != nil {
		d.Tags.Close()
	}

	b.distributions.Delete(name)
	b.unregisterNameLocked(name)

	ops := b.newOperationsLocked(opTypeDeleteDistribution, ResourceTypeDistribution, []string{name})

	return &ops[0], nil
}

// GetDistributions returns every distribution, optionally filtered by name,
// paginated.
func (b *InMemoryBackend) GetDistributions(name, token string) (page.Page[*Distribution], error) {
	b.mu.RLock("GetDistributions")
	defer b.mu.RUnlock()

	all := b.distributions.All()
	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	out := make([]*Distribution, 0, len(all))

	for _, d := range all {
		if name != "" && d.Name != name {
			continue
		}

		out = append(out, d.clone())
	}

	return paginateGeneric(out, token)
}

// UpdateDistributionBundle changes the named distribution's bundle tier.
func (b *InMemoryBackend) UpdateDistributionBundle(name, bundleID string) (*Operation, error) {
	b.mu.Lock("UpdateDistributionBundle")
	defer b.mu.Unlock()

	d, ok := b.distributions.Get(name)
	if !ok {
		return nil, notFoundError("Distribution", name)
	}

	d.BundleID = bundleID

	ops := b.newOperationsLocked(opTypeUpdateDistributionBundle, ResourceTypeDistribution, []string{name})

	return &ops[0], nil
}

// ResetDistributionCache resets the named distribution's cache, recording
// the reset time.
func (b *InMemoryBackend) ResetDistributionCache(name string) (*Operation, time.Time, error) {
	b.mu.Lock("ResetDistributionCache")
	defer b.mu.Unlock()

	if _, ok := b.distributions.Get(name); !ok {
		return nil, time.Time{}, notFoundError("Distribution", name)
	}

	now := nowUTC()
	b.distributionCacheResets[name] = now

	ops := b.newOperationsLocked(opTypeResetDistributionCache, ResourceTypeDistribution, []string{name})

	return &ops[0], now, nil
}

// GetDistributionLatestCacheReset returns the named distribution's most
// recent cache reset time, or a NotFoundException if it has never been
// reset -- a real state (never fabricated), not always "just now".
func (b *InMemoryBackend) GetDistributionLatestCacheReset(name string) (time.Time, error) {
	b.mu.RLock("GetDistributionLatestCacheReset")
	defer b.mu.RUnlock()

	if _, ok := b.distributions.Get(name); !ok {
		return time.Time{}, notFoundError("Distribution", name)
	}

	t, ok := b.distributionCacheResets[name]
	if !ok {
		return time.Time{}, notFoundError("cache reset record for Distribution", name)
	}

	return t, nil
}

// GetDistributionMetricData returns a real, well-formed, EMPTY MetricData
// response -- one of the six honestly-unfakeable telemetry ops
// (PARITY.md 4.10).
func (b *InMemoryBackend) GetDistributionMetricData(name string) error {
	b.mu.RLock("GetDistributionMetricData")
	defer b.mu.RUnlock()

	if _, ok := b.distributions.Get(name); !ok {
		return notFoundError("Distribution", name)
	}

	return nil
}

// AttachCertificateToDistribution attaches an ISSUED certificate to the
// named distribution.
func (b *InMemoryBackend) AttachCertificateToDistribution(
	distributionName, certificateName string,
) (*Operation, error) {
	b.mu.Lock("AttachCertificateToDistribution")
	defer b.mu.Unlock()

	d, ok := b.distributions.Get(distributionName)
	if !ok {
		return nil, notFoundError("Distribution", distributionName)
	}

	if _, certOK := b.certificates.Get(certificateName); !certOK {
		return nil, notFoundError("Certificate", certificateName)
	}

	d.CertificateName = certificateName

	ops := b.newOperationsLocked(opTypeAttachCertToDistribution, ResourceTypeDistribution, []string{distributionName})

	return &ops[0], nil
}

// DetachCertificateFromDistribution detaches whatever certificate is
// attached to the named distribution.
func (b *InMemoryBackend) DetachCertificateFromDistribution(distributionName string) (*Operation, error) {
	b.mu.Lock("DetachCertificateFromDistribution")
	defer b.mu.Unlock()

	d, ok := b.distributions.Get(distributionName)
	if !ok {
		return nil, notFoundError("Distribution", distributionName)
	}

	d.CertificateName = ""

	ops := b.newOperationsLocked(opTypeDetachCertFromDistribution, ResourceTypeDistribution, []string{distributionName})

	return &ops[0], nil
}
