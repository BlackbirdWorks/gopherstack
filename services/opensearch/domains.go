package opensearch

import (
	"fmt"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// CreateDomain creates a new OpenSearch domain.
func (b *InMemoryBackend) CreateDomain(input CreateDomainInput) (*Domain, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: DomainName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateDomain")
	defer b.mu.Unlock()

	// Finalise any domains whose deleting window has elapsed so the name frees up.
	b.purgeExpiredDomainsLocked()

	if b.domains.Has(input.Name) {
		return nil, fmt.Errorf("%w: domain %s already exists", ErrDomainAlreadyExists, input.Name)
	}

	if input.EngineVersion == "" {
		input.EngineVersion = defaultEngineVersion
	}

	domainARN := arn.Build("es", b.region, b.accountID, "domain/"+input.Name)
	endpoint := fmt.Sprintf("search-%s-%s.%s.es.amazonaws.com", input.Name, b.accountID, b.region)
	domainID := fmt.Sprintf("%s/%s", b.accountID, input.Name)

	if input.ClusterConfig.InstanceCount == 0 {
		input.ClusterConfig.InstanceCount = 1
	}

	if input.ClusterConfig.InstanceType == "" {
		input.ClusterConfig.InstanceType = instanceTypeT3Small
	}

	d := &Domain{
		Name:                        input.Name,
		ARN:                         domainARN,
		DomainID:                    domainID,
		EngineVersion:               input.EngineVersion,
		Endpoint:                    endpoint,
		Status:                      "Active",
		ClusterConfig:               input.ClusterConfig,
		Tags:                        tags.New("opensearch." + input.Name + ".tags"),
		EBSOptions:                  input.EBSOptions,
		SnapshotOptions:             input.SnapshotOptions,
		EncryptionAtRestOptions:     input.EncryptionAtRestOptions,
		NodeToNodeEncryptionOptions: input.NodeToNodeEncryptionOptions,
		DomainEndpointOptions:       input.DomainEndpointOptions,
		AdvancedSecurityOptions:     input.AdvancedSecurityOptions,
		VPCOptions:                  input.VPCOptions,
		CognitoOptions:              input.CognitoOptions,
		OffPeakWindowOptions:        input.OffPeakWindowOptions,
		IdentityCenterOptions:       input.IdentityCenterOptions,
		EnableSoftwareUpdateOptions: input.EnableSoftwareUpdateOptions,
		LogPublishingOptions:        input.LogPublishingOptions,
		AccessPolicies:              input.AccessPolicies,
	}

	if len(input.Tags) > 0 {
		d.Tags.Merge(input.Tags)
	}

	d.Created = true
	b.beginProcessing(d, dpsCreating)

	b.domains.Put(d)

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Register(endpoint)
	}

	cp := *d

	return &cp, nil
}

// DeleteDomain removes a domain by name and cleans up all associated resources.
//
// When processingDelay is 0 the domain (and all its scoped resources) is removed
// immediately and the returned status reports Deleted:true / Deleting. When a
// delay is configured the domain enters a real, observable Deleting window: it
// remains describable with Deleted:true until the window elapses, after which it
// is cascaded away lazily on the next write.
func (b *InMemoryBackend) DeleteDomain(name string) (*Domain, error) {
	b.mu.Lock("DeleteDomain")
	defer b.mu.Unlock()

	b.purgeExpiredDomainsLocked()

	d, exists := b.domains.Get(name)
	if !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, name)
	}

	d.Deleted = true
	b.beginProcessing(d, dpsDeleting)
	cp := *d

	if b.processingDelay == 0 {
		b.removeDomainLocked(name)
	}

	return &cp, nil
}

// DescribeDomain returns details about a domain. A domain whose deleting window
// has elapsed is reported as not found.
func (b *InMemoryBackend) DescribeDomain(name string) (*Domain, error) {
	b.mu.RLock("DescribeDomain")
	defer b.mu.RUnlock()

	d, exists := b.domains.Get(name)
	if !exists || deleteWindowElapsed(d, b.clock()) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, name)
	}

	cp := *d

	return &cp, nil
}

// ListDomainNames returns the names of all live domains in sorted order.
func (b *InMemoryBackend) ListDomainNames() []string {
	b.mu.RLock("ListDomainNames")
	defer b.mu.RUnlock()

	now := b.clock()
	names := make([]string, 0, b.domains.Len())

	for _, d := range b.domains.All() {
		if deleteWindowElapsed(d, now) {
			continue
		}

		names = append(names, d.Name)
	}

	slices.Sort(names)

	return names
}

// findDomainByARN returns the domain matching the given ARN, or nil if not found.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findDomainByARN(domainARN string) *Domain {
	matches := b.domainsByARN.Get(domainARN)
	if len(matches) == 0 {
		return nil
	}

	return matches[0]
}

// CancelServiceSoftwareUpdate cancels a pending service software update.
//
// AWS only permits cancellation while an update is actually scheduled
// (UpdateStatus == PENDING_UPDATE). When nothing is scheduled it rejects the
// request, so this mutates the stored software-update state and returns a
// ValidationException in that case rather than a canned success envelope.
func (b *InMemoryBackend) CancelServiceSoftwareUpdate(
	domainName string,
) (*ServiceSoftwareOptions, error) {
	if domainName == "" {
		return nil, fmt.Errorf("%w: DomainName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CancelServiceSoftwareUpdate")
	defer b.mu.Unlock()

	d, exists := b.domains.Get(domainName)
	if !exists || deleteWindowElapsed(d, b.clock()) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	if d.ServiceSoftware == nil || d.ServiceSoftware.UpdateStatus != sswStatusPendingUpdate {
		return nil, fmt.Errorf(
			"%w: domain %s has no service software update in a cancellable (PENDING_UPDATE) state",
			ErrValidation,
			domainName,
		)
	}

	// The scheduled install is cancelled, but the newer version remains
	// available, so the domain returns to the ELIGIBLE state.
	d.ServiceSoftware.UpdateStatus = sswStatusEligible
	d.ServiceSoftware.Cancellable = false
	d.ServiceSoftware.UpdateAvailable = true
	d.ServiceSoftware.NewVersion = defaultEngineVersion
	d.ServiceSoftware.Description = "Cancellation complete. A new version is available to install."

	// Clear the software-update processing window opened by StartServiceSoftwareUpdate.
	if d.ProcessingStatus == dpsUpdatingServiceSoftware {
		d.ProcessingStatus = ""
		d.ProcessingUntil = time.Time{}
	}

	cp := *d.ServiceSoftware

	return &cp, nil
}

// StartServiceSoftwareUpdate marks a domain as having a pending software update.
// StartServiceSoftwareUpdate schedules a service software update for the domain.
// scheduleAt must be one of "NOW", "TIMESTAMP", "OFF_PEAK_WINDOW", or "".
func (b *InMemoryBackend) StartServiceSoftwareUpdate(
	domainName, scheduleAt string,
) (*ServiceSoftwareOptions, error) {
	b.mu.Lock("StartServiceSoftwareUpdate")
	defer b.mu.Unlock()

	d, exists := b.domains.Get(domainName)
	if !exists || deleteWindowElapsed(d, b.clock()) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	desc := "A new service software version is ready to install."

	switch scheduleAt {
	case "OFF_PEAK_WINDOW":
		desc = "Service software update scheduled for the next off-peak window."
	case "TIMESTAMP":
		desc = "Service software update scheduled for the requested time."
	}

	// Record real, mutable software-update state so a subsequent
	// CancelServiceSoftwareUpdate has something concrete to act on.
	opts := &ServiceSoftwareOptions{
		CurrentVersion:  defaultEngineVersion,
		NewVersion:      defaultEngineVersion,
		UpdateAvailable: true,
		Cancellable:     true,
		UpdateStatus:    sswStatusPendingUpdate,
		Description:     desc,
	}
	d.ServiceSoftware = opts
	b.beginProcessing(d, dpsUpdatingServiceSoftware)

	cp := *opts

	return &cp, nil
}

// DescribeDomains returns a list of domains. If names is empty, all domains are returned.
// Missing names are silently skipped.
func (b *InMemoryBackend) DescribeDomains(names []string) ([]*Domain, error) {
	b.mu.RLock("DescribeDomains")
	defer b.mu.RUnlock()

	now := b.clock()

	if len(names) == 0 {
		out := make([]*Domain, 0, b.domains.Len())
		for _, d := range b.domains.All() {
			if deleteWindowElapsed(d, now) {
				continue
			}

			cp := *d
			out = append(out, &cp)
		}

		return out, nil
	}

	out := make([]*Domain, 0, len(names))

	for _, name := range names {
		d, exists := b.domains.Get(name)
		if !exists || deleteWindowElapsed(d, now) {
			continue
		}

		cp := *d
		out = append(out, &cp)
	}

	return out, nil
}

// AddDomainInternal seeds a domain directly for use in tests.
func (b *InMemoryBackend) AddDomainInternal(name, engineVersion string) {
	if engineVersion == "" {
		engineVersion = defaultEngineVersion
	}

	b.mu.Lock("AddDomainInternal")
	defer b.mu.Unlock()

	domainARN := arn.Build("es", b.region, b.accountID, "domain/"+name)
	endpoint := fmt.Sprintf("search-%s-%s.%s.es.amazonaws.com", name, b.accountID, b.region)
	b.domains.Put(&Domain{
		Name:          name,
		ARN:           domainARN,
		EngineVersion: engineVersion,
		Endpoint:      endpoint,
		Status:        domainStatusActive,
		ClusterConfig: ClusterConfig{InstanceType: instanceTypeT3Small, InstanceCount: 1},
		Tags:          tags.New("opensearch." + name + ".tags"),
	})
}
