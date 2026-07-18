package mediastore

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// containerNameRE is the allowed character set for container names.
var containerNameRE = regexp.MustCompile(`^[a-zA-Z0-9\-_]+$`)

// validateContainerName returns an error if the container name is invalid.
func validateContainerName(name string) error {
	if name == "" || len(name) > maxContainerNameLen || !containerNameRE.MatchString(name) {
		return ErrInvalidContainerName
	}

	return nil
}

// containerARN builds the ARN for a MediaStore container.
func containerARN(region, accountID, name string) string {
	return arn.Build("mediastore", region, accountID, "container/"+name)
}

// containerEndpoint returns the data plane endpoint for a container.
func containerEndpoint(name, region string) string {
	return fmt.Sprintf(defaultEndpointFormat, name, region)
}

// CreateContainer creates a new MediaStore container in the ctx region.
func (b *InMemoryBackend) CreateContainer(
	ctx context.Context,
	accountID, name string,
	tags map[string]string,
) (*Container, error) {
	if err := validateContainerName(name); err != nil {
		return nil, err
	}

	region := regionFromContext(ctx)

	b.mu.Lock("CreateContainer")
	defer b.mu.Unlock()

	tbl := b.containersStore(region)

	if tbl.Has(name) {
		return nil, ErrContainerAlreadyExists
	}

	now := time.Now().UTC()

	t := make(map[string]string)
	maps.Copy(t, tags)

	c := &Container{
		Name:         name,
		ARN:          containerARN(region, accountID, name),
		Endpoint:     containerEndpoint(name, region),
		Status:       containerStatusActive,
		CreationTime: &now,
		Tags:         t,
	}

	tbl.Put(c)

	return copyContainer(c), nil
}

// DeleteContainer removes a container from the ctx region.
func (b *InMemoryBackend) DeleteContainer(ctx context.Context, name string) error {
	region := regionFromContext(ctx)

	b.mu.Lock("DeleteContainer")
	defer b.mu.Unlock()

	tbl := b.containerRegion(region)

	if tbl == nil || !tbl.Has(name) {
		return ErrContainerNotFound
	}

	tbl.Delete(name)

	return nil
}

// DescribeContainer returns details about a container in the ctx region.
func (b *InMemoryBackend) DescribeContainer(ctx context.Context, name string) (*Container, error) {
	region := regionFromContext(ctx)

	b.mu.RLock("DescribeContainer")
	defer b.mu.RUnlock()

	c, exists := b.getContainer(region, name)
	if !exists {
		return nil, ErrContainerNotFound
	}

	return copyContainer(c), nil
}

// ListContainers returns paginated containers in the ctx region sorted by name.
func (b *InMemoryBackend) ListContainers(
	ctx context.Context,
	nextToken string,
	maxResults int,
) ([]*Container, string, error) {
	region := regionFromContext(ctx)

	b.mu.RLock("ListContainers")
	defer b.mu.RUnlock()

	tbl := b.containerRegion(region)

	var all []*Container
	if tbl != nil {
		items := tbl.All()
		all = make([]*Container, 0, len(items))

		for _, c := range items {
			all = append(all, copyContainer(c))
		}
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Name < all[j].Name
	})

	p := page.NewHMAC(all, nextToken, b.paginationSecret, maxResults, defaultListLimit)

	return p.Data, p.Next, nil
}

// PutContainerPolicy stores a container access policy.
func (b *InMemoryBackend) PutContainerPolicy(ctx context.Context, name, policy string) error {
	if !json.Valid([]byte(policy)) {
		return ErrInvalidPolicy
	}

	region := regionFromContext(ctx)

	b.mu.Lock("PutContainerPolicy")
	defer b.mu.Unlock()

	c, exists := b.getContainer(region, name)
	if !exists {
		return ErrContainerNotFound
	}

	c.ContainerPolicy = policy

	return nil
}

// GetContainerPolicy retrieves the container access policy.
func (b *InMemoryBackend) GetContainerPolicy(ctx context.Context, name string) (string, error) {
	region := regionFromContext(ctx)

	b.mu.RLock("GetContainerPolicy")
	defer b.mu.RUnlock()

	c, exists := b.getContainer(region, name)
	if !exists {
		return "", ErrContainerNotFound
	}

	if c.ContainerPolicy == "" {
		return "", ErrPolicyNotFound
	}

	return c.ContainerPolicy, nil
}

// DeleteContainerPolicy removes the container access policy.
func (b *InMemoryBackend) DeleteContainerPolicy(ctx context.Context, name string) error {
	region := regionFromContext(ctx)

	b.mu.Lock("DeleteContainerPolicy")
	defer b.mu.Unlock()

	c, exists := b.getContainer(region, name)
	if !exists {
		return ErrContainerNotFound
	}

	if c.ContainerPolicy == "" {
		return ErrPolicyNotFound
	}

	c.ContainerPolicy = ""

	return nil
}

// StartAccessLogging enables access logging for a container.
func (b *InMemoryBackend) StartAccessLogging(ctx context.Context, name string) error {
	region := regionFromContext(ctx)

	b.mu.Lock("StartAccessLogging")
	defer b.mu.Unlock()

	c, exists := b.getContainer(region, name)
	if !exists {
		return ErrContainerNotFound
	}

	c.AccessLoggingEnabled = true

	return nil
}

// StopAccessLogging disables access logging for a container.
func (b *InMemoryBackend) StopAccessLogging(ctx context.Context, name string) error {
	region := regionFromContext(ctx)

	b.mu.Lock("StopAccessLogging")
	defer b.mu.Unlock()

	c, exists := b.getContainer(region, name)
	if !exists {
		return ErrContainerNotFound
	}

	c.AccessLoggingEnabled = false

	return nil
}
