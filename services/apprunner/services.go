package apprunner

import (
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// CreateService creates a new App Runner service.
func (b *InMemoryBackend) CreateService(
	name, cpu, memory, imageURI string,
	tags map[string]string,
) (*Service, error) {
	b.mu.Lock("CreateService")
	defer b.mu.Unlock()

	if existing := b.byName.Get(name); len(existing) > 0 {
		return nil, fmt.Errorf("service %s already exists: %w", name, ErrAlreadyExists)
	}

	id := newID()
	svcArn := b.serviceARN(id)
	now := time.Now().UTC()

	if cpu == "" {
		cpu = defaultCPU
	}

	if memory == "" {
		memory = defaultMemory
	}

	svcTags := make(map[string]string)
	maps.Copy(svcTags, tags)

	svc := &storedService{
		ServiceArn:  svcArn,
		ServiceID:   id,
		ServiceName: name,
		ServiceURL:  buildServiceURL(id, b.region),
		Status:      statusRunning,
		CPU:         cpu,
		Memory:      memory,
		ImageURI:    imageURI,
		CreatedAt:   now,
		UpdatedAt:   now,
		Tags:        svcTags,
	}
	b.addOperation(svc, opTypeCreate)
	b.services.Put(svc)

	if len(svcTags) > 0 {
		b.tags[svcArn] = make(map[string]string)
		maps.Copy(b.tags[svcArn], svcTags)
	}

	cp := svc.toService()

	return &cp, nil
}

// DescribeService returns full service details.
func (b *InMemoryBackend) DescribeService(serviceArn string) (*Service, error) {
	b.mu.RLock("DescribeService")
	defer b.mu.RUnlock()

	svc, ok := b.services.Get(serviceArn)
	if !ok {
		return nil, fmt.Errorf("service %s not found: %w", serviceArn, ErrNotFound)
	}

	cp := svc.toService()

	return &cp, nil
}

// UpdateService updates a service's configuration.
func (b *InMemoryBackend) UpdateService(serviceArn, cpu, memory, imageURI string) (*Service, error) {
	b.mu.Lock("UpdateService")
	defer b.mu.Unlock()

	svc, ok := b.services.Get(serviceArn)
	if !ok {
		return nil, fmt.Errorf("service %s not found: %w", serviceArn, ErrNotFound)
	}

	if svc.Status != statusRunning {
		return nil, fmt.Errorf(
			"service %s cannot be updated in status %s: %w",
			serviceArn, svc.Status, ErrInvalidState,
		)
	}

	if cpu != "" {
		svc.CPU = cpu
	}

	if memory != "" {
		svc.Memory = memory
	}

	if imageURI != "" {
		svc.ImageURI = imageURI
	}

	svc.UpdatedAt = time.Now().UTC()
	b.addOperation(svc, opTypeUpdate)

	cp := svc.toService()

	return &cp, nil
}

// DeleteService marks a service as deleted and removes it from active lookup.
func (b *InMemoryBackend) DeleteService(serviceArn string) (*Service, error) {
	b.mu.Lock("DeleteService")
	defer b.mu.Unlock()

	svc, ok := b.services.Get(serviceArn)
	if !ok {
		return nil, fmt.Errorf("service %s not found: %w", serviceArn, ErrNotFound)
	}

	svc.Status = statusDeleted
	svc.UpdatedAt = time.Now().UTC()
	b.addOperation(svc, opTypeDelete)

	cp := svc.toService()

	b.services.Delete(serviceArn)
	delete(b.tags, serviceArn)

	return &cp, nil
}

// ListServices returns services sorted by ARN with pagination.
func (b *InMemoryBackend) ListServices(maxResults int32, nextToken string) ([]*ServiceSummary, string, error) {
	b.mu.RLock("ListServices")
	defer b.mu.RUnlock()

	items := b.services.Snapshot()

	all := make([]*ServiceSummary, 0, len(items))
	for _, svc := range items {
		s := svc.toSummary()
		all = append(all, &s)
	}

	limit := int(maxResults)
	pg := page.New(all, nextToken, limit, defaultMaxResults)

	return pg.Data, pg.Next, nil
}

// PauseService pauses a running service.
func (b *InMemoryBackend) PauseService(serviceArn string) (*Service, error) {
	b.mu.Lock("PauseService")
	defer b.mu.Unlock()

	svc, ok := b.services.Get(serviceArn)
	if !ok {
		return nil, fmt.Errorf("service %s not found: %w", serviceArn, ErrNotFound)
	}

	if svc.Status != statusRunning {
		return nil, fmt.Errorf(
			"service %s cannot be paused in status %s: %w",
			serviceArn, svc.Status, ErrInvalidState,
		)
	}

	svc.Status = statusPaused
	svc.UpdatedAt = time.Now().UTC()
	b.addOperation(svc, opTypePause)

	cp := svc.toService()

	return &cp, nil
}

// ResumeService resumes a paused service.
func (b *InMemoryBackend) ResumeService(serviceArn string) (*Service, error) {
	b.mu.Lock("ResumeService")
	defer b.mu.Unlock()

	svc, ok := b.services.Get(serviceArn)
	if !ok {
		return nil, fmt.Errorf("service %s not found: %w", serviceArn, ErrNotFound)
	}

	if svc.Status != statusPaused {
		return nil, fmt.Errorf(
			"service %s cannot be resumed in status %s: %w",
			serviceArn, svc.Status, ErrInvalidState,
		)
	}

	svc.Status = statusRunning
	svc.UpdatedAt = time.Now().UTC()
	b.addOperation(svc, opTypeResume)

	cp := svc.toService()

	return &cp, nil
}

// StartDeployment triggers a deployment for a service.
func (b *InMemoryBackend) StartDeployment(serviceArn string) (string, error) {
	b.mu.Lock("StartDeployment")
	defer b.mu.Unlock()

	svc, ok := b.services.Get(serviceArn)
	if !ok {
		return "", fmt.Errorf("service %s not found: %w", serviceArn, ErrNotFound)
	}

	if svc.Status != statusRunning {
		return "", fmt.Errorf(
			"service %s cannot start deployment in status %s: %w",
			serviceArn, svc.Status, ErrInvalidState,
		)
	}

	b.addOperation(svc, opTypeDeploy)
	opID := svc.Operations[len(svc.Operations)-1].ID

	return opID, nil
}
