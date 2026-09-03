package apprunner

import (
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// CreateVpcIngressConnection creates a new VPC ingress connection.
func (b *InMemoryBackend) CreateVpcIngressConnection(
	name, serviceArn, vpcID, vpcEndpointID string,
	tags map[string]string,
) (*VpcIngressConnection, error) {
	b.mu.Lock("CreateVpcIngressConnection")
	defer b.mu.Unlock()

	if existing := b.vicByName.Get(name); len(existing) > 0 {
		return nil, fmt.Errorf("vpc ingress connection %s already exists: %w", name, ErrAlreadyExists)
	}

	id := newID()
	vicArn := b.vicARN(name, id)
	now := time.Now().UTC()

	domainName := fmt.Sprintf("%s.%s.awsapprunner.com", id, b.region)

	vic := &storedVpcIngressConnection{
		VpcIngressConnectionArn:  vicArn,
		VpcIngressConnectionName: name,
		ServiceArn:               serviceArn,
		AccountID:                b.accountID,
		DomainName:               domainName,
		VpcID:                    vpcID,
		VpcEndpointID:            vpcEndpointID,
		Status:                   vicStatusAvailable,
		CreatedAt:                now,
	}

	b.vpcIngressConnections.Put(vic)

	if len(tags) > 0 {
		b.tags[vicArn] = make(map[string]string)
		maps.Copy(b.tags[vicArn], tags)
	}

	cp := vic.toVIC()

	return &cp, nil
}

// DescribeVpcIngressConnection returns a VPC ingress connection by ARN.
func (b *InMemoryBackend) DescribeVpcIngressConnection(vicArn string) (*VpcIngressConnection, error) {
	b.mu.RLock("DescribeVpcIngressConnection")
	defer b.mu.RUnlock()

	vic, ok := b.vpcIngressConnections.Get(vicArn)
	if !ok {
		return nil, fmt.Errorf("vpc ingress connection %s not found: %w", vicArn, ErrNotFound)
	}

	cp := vic.toVIC()

	return &cp, nil
}

// DeleteVpcIngressConnection deletes a VPC ingress connection.
func (b *InMemoryBackend) DeleteVpcIngressConnection(vicArn string) (*VpcIngressConnection, error) {
	b.mu.Lock("DeleteVpcIngressConnection")
	defer b.mu.Unlock()

	vic, ok := b.vpcIngressConnections.Get(vicArn)
	if !ok {
		return nil, fmt.Errorf("vpc ingress connection %s not found: %w", vicArn, ErrNotFound)
	}

	vic.Status = vicStatusDeleted
	vic.DeletedAt = time.Now().UTC()
	cp := vic.toVIC()

	b.vpcIngressConnections.Delete(vicArn)
	delete(b.tags, vicArn)

	return &cp, nil
}

// ListVpcIngressConnections returns VPC ingress connections with optional filters.
func (b *InMemoryBackend) ListVpcIngressConnections(
	serviceArnFilter, vpcEndpointIDFilter string,
	maxResults int32,
	nextToken string,
) ([]*VpcIngressConnectionSummary, string, error) {
	b.mu.RLock("ListVpcIngressConnections")
	defer b.mu.RUnlock()

	items := b.vpcIngressConnections.Snapshot()

	all := make([]*VpcIngressConnectionSummary, 0, len(items))
	for _, vic := range items {
		if serviceArnFilter != "" && vic.ServiceArn != serviceArnFilter {
			continue
		}
		if vpcEndpointIDFilter != "" && vic.VpcEndpointID != vpcEndpointIDFilter {
			continue
		}
		s := vic.toSummary()
		all = append(all, &s)
	}

	limit := int(maxResults)
	pg := page.New(all, nextToken, limit, defaultMaxResults)

	return pg.Data, pg.Next, nil
}

// UpdateVpcIngressConnection updates a VPC ingress connection's VPC config.
func (b *InMemoryBackend) UpdateVpcIngressConnection(
	vicArn, vpcID, vpcEndpointID string,
) (*VpcIngressConnection, error) {
	b.mu.Lock("UpdateVpcIngressConnection")
	defer b.mu.Unlock()

	vic, ok := b.vpcIngressConnections.Get(vicArn)
	if !ok {
		return nil, fmt.Errorf("vpc ingress connection %s not found: %w", vicArn, ErrNotFound)
	}

	if vpcID != "" {
		vic.VpcID = vpcID
	}

	if vpcEndpointID != "" {
		vic.VpcEndpointID = vpcEndpointID
	}

	cp := vic.toVIC()

	return &cp, nil
}
