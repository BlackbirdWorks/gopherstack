package vpclattice

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// VpcEndpointRef is the part of an EC2 VPC endpoint that endpoint associations expose.
type VpcEndpointRef struct {
	CreatedAt time.Time
	ID        string
	VpcID     string
	OwnerID   string
	State     string
}

// EndpointDirectory resolves the EC2 VPC endpoints bound to Lattice resources; real AWS creates
// ResourceEndpointAssociations and ServiceNetworkVpcEndpointAssociations from EC2 CreateVpcEndpoint.
type EndpointDirectory interface {
	ServiceNetworkEndpoints(region, serviceNetworkARN string) []VpcEndpointRef
	ResourceConfigurationEndpoints(region, resourceConfigurationARN string) []VpcEndpointRef
	DisassociateResourceConfiguration(region, endpointID string) error
}

// SetEndpointDirectory wires the EC2 endpoint source; without it both association lists stay empty.
func (b *InMemoryBackend) SetEndpointDirectory(d EndpointDirectory) {
	b.mu.Lock("SetEndpointDirectory")
	defer b.mu.Unlock()

	b.endpoints = d
}

const (
	resourceEndpointAssociationPrefix = "rea-"
	serviceNetworkEndpointPrefix      = "snea-"
	vpcEndpointIDPrefix               = "vpce-"
)

func (b *InMemoryBackend) resourceEndpointAssociationARN(region, id string) string {
	return arn.Build("vpc-lattice", region, b.accountID, "resourceendpointassociation/"+id)
}

func associationSuffix(endpointID string) string {
	return strings.TrimPrefix(endpointID, vpcEndpointIDPrefix)
}

func (b *InMemoryBackend) resourceEndpointAssociationsFor(
	region string, rc *storedResourceConfiguration,
) []*ResourceEndpointAssociationSummary {
	if b.endpoints == nil {
		return nil
	}

	var out []*ResourceEndpointAssociationSummary

	for _, ep := range b.endpoints.ResourceConfigurationEndpoints(region, rc.ARN) {
		id := resourceEndpointAssociationPrefix + associationSuffix(ep.ID)
		out = append(out, &ResourceEndpointAssociationSummary{
			CreatedAt:                 ep.CreatedAt,
			ARN:                       b.resourceEndpointAssociationARN(region, id),
			ID:                        id,
			ResourceConfigurationARN:  rc.ARN,
			ResourceConfigurationID:   rc.ID,
			ResourceConfigurationName: rc.Name,
			VpcEndpointID:             ep.ID,
			VpcEndpointOwner:          ep.OwnerID,
		})
	}

	return out
}

func (f ResourceEndpointAssociationFilter) matches(a *ResourceEndpointAssociationSummary) bool {
	if id := f.ResourceEndpointAssociationIdentifier; id != "" && id != a.ID && id != a.ARN {
		return false
	}

	if f.VpcEndpointID != "" && f.VpcEndpointID != a.VpcEndpointID {
		return false
	}

	return f.VpcEndpointOwner == "" || f.VpcEndpointOwner == a.VpcEndpointOwner
}

// ListResourceEndpointAssociations lists the Resource VPC endpoints bound to a resource configuration.
func (b *InMemoryBackend) ListResourceEndpointAssociations(
	ctx context.Context,
	filter ResourceEndpointAssociationFilter,
	maxResults int32,
	nextToken string,
) ([]*ResourceEndpointAssociationSummary, string, error) {
	if filter.ResourceConfigurationIdentifier == "" {
		return nil, "", ErrInvalidParameter
	}

	b.mu.RLock("ListResourceEndpointAssociations")
	defer b.mu.RUnlock()

	rcID, ok := b.resolveResourceConfigurationID(filter.ResourceConfigurationIdentifier)
	if !ok {
		return nil, "", ErrNotFound
	}

	rc, _ := b.resourceConfigurations.Get(rcID)

	all := make([]*ResourceEndpointAssociationSummary, 0)

	for _, a := range b.resourceEndpointAssociationsFor(b.regionFor(ctx), rc) {
		if filter.matches(a) {
			all = append(all, a)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	p := page.New(all, nextToken, int(maxResults), defaultMaxResults)

	return p.Data, p.Next, nil
}

// DeleteResourceEndpointAssociation disassociates a resource configuration from its Resource VPC endpoint.
func (b *InMemoryBackend) DeleteResourceEndpointAssociation(
	ctx context.Context, id string,
) (*ResourceEndpointAssociationSummary, error) {
	b.mu.RLock("DeleteResourceEndpointAssociation")
	defer b.mu.RUnlock()

	region := b.regionFor(ctx)

	for _, rc := range b.resourceConfigurations.All() {
		for _, a := range b.resourceEndpointAssociationsFor(region, rc) {
			if a.ID != id && a.ARN != id {
				continue
			}

			if err := b.endpoints.DisassociateResourceConfiguration(region, a.VpcEndpointID); err != nil {
				return nil, err
			}

			return a, nil
		}
	}

	return nil, ErrNotFound
}

// ListServiceNetworkVpcEndpointAssociations lists the ServiceNetwork VPC endpoints bound to a service network.
func (b *InMemoryBackend) ListServiceNetworkVpcEndpointAssociations(
	ctx context.Context,
	serviceNetworkIdentifier string,
	maxResults int32,
	nextToken string,
) ([]*ServiceNetworkVpcEndpointAssociationSummary, string, error) {
	if serviceNetworkIdentifier == "" {
		return nil, "", ErrInvalidParameter
	}

	b.mu.RLock("ListServiceNetworkVpcEndpointAssociations")
	defer b.mu.RUnlock()

	snID, ok := b.resolveServiceNetworkID(serviceNetworkIdentifier)
	if !ok {
		return nil, "", ErrNotFound
	}

	sn, _ := b.serviceNetworks.Get(snID)

	all := make([]*ServiceNetworkVpcEndpointAssociationSummary, 0)

	if b.endpoints != nil {
		for _, ep := range b.endpoints.ServiceNetworkEndpoints(b.regionFor(ctx), sn.ARN) {
			all = append(all, &ServiceNetworkVpcEndpointAssociationSummary{
				CreatedAt:         ep.CreatedAt,
				ID:                serviceNetworkEndpointPrefix + associationSuffix(ep.ID),
				ServiceNetworkARN: sn.ARN,
				State:             ep.State,
				VpcEndpointID:     ep.ID,
				VpcID:             ep.VpcID,
				VpcEndpointOwner:  ep.OwnerID,
			})
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	p := page.New(all, nextToken, int(maxResults), defaultMaxResults)

	return p.Data, p.Next, nil
}
