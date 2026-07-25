package ecs

import (
	"fmt"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrExpressGatewayServiceNotFound is returned when an express gateway service does not exist.
var ErrExpressGatewayServiceNotFound = awserr.New(
	"ExpressGatewayServiceNotFoundException",
	awserr.ErrNotFound,
)

// ErrExpressGatewayServiceAlreadyExists is returned when an express gateway service already exists.
var ErrExpressGatewayServiceAlreadyExists = awserr.New(
	"ExpressGatewayServiceAlreadyExistsException", awserr.ErrAlreadyExists,
)

// UpdateExpressGatewayService updates an express gateway service.
func (b *InMemoryBackend) UpdateExpressGatewayService(
	input UpdateExpressGatewayServiceInput,
) (*ExpressGatewayService, error) {
	if input.ServiceArn == "" {
		return nil, fmt.Errorf("%w: serviceArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateExpressGatewayService")
	defer b.mu.Unlock()

	svc, ok := b.expressGatewayServices.Get(input.ServiceArn)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrExpressGatewayServiceNotFound, input.ServiceArn)
	}

	if input.ExecutionRoleArn != "" {
		svc.ExecutionRoleArn = input.ExecutionRoleArn
	}

	if input.InfrastructureRoleArn != "" {
		svc.InfrastructureRoleArn = input.InfrastructureRoleArn
	}

	out := *svc
	out.Tags = copyTags(b.resourceTags[resourceTagKey(svc.ServiceArn)])

	return &out, nil
}

// CreateExpressGatewayService creates a new express gateway service.
func (b *InMemoryBackend) CreateExpressGatewayService(
	input CreateExpressGatewayServiceInput,
) (*ExpressGatewayService, error) {
	if input.ExecutionRoleArn == "" {
		return nil, fmt.Errorf("%w: executionRoleArn is required", ErrInvalidParameter)
	}

	if input.InfrastructureRoleArn == "" {
		return nil, fmt.Errorf("%w: infrastructureRoleArn is required", ErrInvalidParameter)
	}

	clusterName := clusterKey(b.resolveCluster(input.Cluster))

	b.mu.Lock("CreateExpressGatewayService")
	defer b.mu.Unlock()

	serviceName := input.ServiceName
	if serviceName == "" {
		serviceName = "express-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	}

	serviceArn := fmt.Sprintf(
		"arn:aws:ecs:%s:%s:service/%s/%s", b.region, b.accountID, clusterName, serviceName,
	)

	if b.expressGatewayServices.Has(serviceArn) {
		return nil, fmt.Errorf("%w: %s", ErrExpressGatewayServiceAlreadyExists, serviceName)
	}

	svc := &ExpressGatewayService{
		CreatedAt:             time.Now(),
		ServiceArn:            serviceArn,
		ServiceName:           serviceName,
		Cluster:               clusterName,
		Status:                statusActive,
		ExecutionRoleArn:      input.ExecutionRoleArn,
		InfrastructureRoleArn: input.InfrastructureRoleArn,
		Tags:                  copyTags(input.Tags),
	}

	b.expressGatewayServices.Put(svc)

	// Mirror tags into the resourceTags side map so TagResource/UntagResource/
	// ListTagsForResource (and DescribeExpressGatewayService, which reads tags
	// from this map -- see DescribeExpressGatewayService below) see the same
	// tags applied at creation. Previously svc.Tags and resourceTags were two
	// independent, never-synchronized copies: a TagResource call on this ARN
	// silently never showed up on Describe, and creation-time tags were
	// invisible to ListTagsForResource.
	if len(input.Tags) > 0 {
		b.setResourceTagsLocked(serviceArn, input.Tags)
	}

	out := *svc
	out.Tags = copyTags(svc.Tags)

	return &out, nil
}

// DeleteExpressGatewayService deletes an express gateway service by ARN.
func (b *InMemoryBackend) DeleteExpressGatewayService(
	serviceArn string,
) (*ExpressGatewayService, error) {
	if serviceArn == "" {
		return nil, fmt.Errorf("%w: serviceArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteExpressGatewayService")
	defer b.mu.Unlock()

	svc, ok := b.expressGatewayServices.Get(serviceArn)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrExpressGatewayServiceNotFound, serviceArn)
	}

	b.expressGatewayServices.Delete(serviceArn)
	b.deleteResourceTagsLocked(serviceArn)

	out := *svc

	return &out, nil
}

// DescribeExpressGatewayService returns an express gateway service by ARN.
func (b *InMemoryBackend) DescribeExpressGatewayService(
	serviceArn string,
) (*ExpressGatewayService, error) {
	if serviceArn == "" {
		return nil, fmt.Errorf("%w: serviceArn is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeExpressGatewayService")
	defer b.mu.RUnlock()

	svc, ok := b.expressGatewayServices.Get(serviceArn)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrExpressGatewayServiceNotFound, serviceArn)
	}

	out := *svc
	// resourceTags (kept in sync by TagResource/UntagResource) is authoritative,
	// not the creation-time svc.Tags snapshot -- see CreateExpressGatewayService.
	out.Tags = copyTags(b.resourceTags[resourceTagKey(svc.ServiceArn)])

	return &out, nil
}
