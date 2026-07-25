package vpclattice

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// resolveServiceID resolves a service identifier (ID or ARN) to an ID.
func (b *InMemoryBackend) resolveServiceID(identifier string) (string, bool) {
	if svc, ok := b.services.Get(identifier); ok {
		return svc.ID, true
	}
	// check if it's an ARN
	for _, svc := range b.services.All() {
		if svc.ARN == identifier {
			return svc.ID, true
		}
	}

	return "", false
}

// ------- Service operations -------

// CreateService creates a new service.
func (b *InMemoryBackend) CreateService(
	ctx context.Context,
	name, authType, certificateArn, customDomainName string,
	tags map[string]string,
) (*Service, error) {
	if name == "" {
		return nil, ErrInvalidParameter
	}

	b.mu.Lock("CreateService")
	defer b.mu.Unlock()

	if len(b.servicesByName.Get(name)) > 0 {
		return nil, ErrAlreadyExists
	}

	now := time.Now().UTC()
	id := newID(idPrefixService)
	region := b.regionFor(ctx)
	svcARN := arn.Build(arnService, region, b.accountID, resourceService+"/"+id)

	if authType == "" {
		authType = authTypeNone
	}

	svc := &storedService{
		ARN:              svcARN,
		ID:               id,
		Name:             name,
		AuthType:         authType,
		CertificateArn:   certificateArn,
		CustomDomainName: customDomainName,
		DNSName:          id + ".vpc-lattice-svcs." + region + ".on.aws",
		HostedZoneID:     newHostedZoneID(),
		Status:           statusActive,
		Tags:             copyTags(tags),
		CreatedAt:        now,
		LastUpdatedAt:    now,
		Region:           region,
	}

	b.services.Put(svc)
	b.tags[svcARN] = copyTags(tags)

	return svc.toService(), nil
}

// GetService returns a service by ID or ARN.
func (b *InMemoryBackend) GetService(serviceID string) (*Service, error) {
	b.mu.RLock("GetService")
	defer b.mu.RUnlock()

	id, ok := b.resolveServiceID(serviceID)
	if !ok {
		return nil, ErrNotFound
	}

	svc, _ := b.services.Get(id)

	return svc.toService(), nil
}

// UpdateService updates a service.
func (b *InMemoryBackend) UpdateService(
	serviceID, authType, certificateArn string,
) (*Service, error) {
	b.mu.Lock("UpdateService")
	defer b.mu.Unlock()

	id, ok := b.resolveServiceID(serviceID)
	if !ok {
		return nil, ErrNotFound
	}

	svc, _ := b.services.Get(id)
	if authType != "" {
		svc.AuthType = authType
	}

	svc.CertificateArn = certificateArn
	svc.LastUpdatedAt = time.Now().UTC()

	return svc.toService(), nil
}

// DeleteService deletes a service. Real AWS rejects the delete with
// ConflictException while the service is still associated with a service
// network, and otherwise cascades the delete through the service's
// listeners, listener rules, resource policy, auth policy, and access log
// subscriptions -- see the DeleteService doc comment in
// aws-sdk-go-v2/service/vpclattice's api_op_DeleteService.go.
func (b *InMemoryBackend) DeleteService(serviceID string) (*Service, error) {
	b.mu.Lock("DeleteService")
	defer b.mu.Unlock()

	id, ok := b.resolveServiceID(serviceID)
	if !ok {
		return nil, ErrNotFound
	}

	for _, s := range b.snsas.All() {
		if s.ServiceID == id {
			return nil, ErrDependencyConflict
		}
	}

	svc, _ := b.services.Get(id)
	out := svc.toService()
	out.Status = statusDeleted

	b.deleteServiceDependents(svc.ARN, id)
	b.services.Delete(id)
	delete(b.tags, svc.ARN)

	return out, nil
}

// deleteServiceDependents removes the resources DeleteService cascades
// through: every listener on the service (and, transitively, their rules,
// via deleteListenerCascade), the resource policy, the auth policy, and any
// access log subscriptions attached directly to the service.
func (b *InMemoryBackend) deleteServiceDependents(serviceARN, serviceID string) {
	for _, l := range slices.Clone(b.listenersByService.Get(serviceID)) {
		b.deleteListenerCascade(l)
	}

	delete(b.authPolicies, serviceARN)
	delete(b.resourcePolicies, serviceARN)

	for _, a := range b.alss.All() {
		if a.ResourceARN == serviceARN {
			b.alss.Delete(a.ID)
			delete(b.tags, a.ARN)
		}
	}
}

// ListServices returns a paginated list of services.
func (b *InMemoryBackend) ListServices(
	ctx context.Context,
	maxResults int32,
	nextToken string,
) ([]*ServiceSummary, string, error) {
	b.mu.RLock("ListServices")
	defer b.mu.RUnlock()

	region := b.regionFor(ctx)
	all := make([]*ServiceSummary, 0, b.services.Len())

	for _, svc := range b.services.All() {
		if svc.Region != region {
			continue
		}

		all = append(all, svc.toSummary())
	}

	slices.SortFunc(all, func(a, b *ServiceSummary) int { return strings.Compare(a.ID, b.ID) })

	p := page.New(all, nextToken, int(maxResults), defaultMaxResults)

	return p.Data, p.Next, nil
}
