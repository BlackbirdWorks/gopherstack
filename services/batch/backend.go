package batch

import (
	"fmt"
	"maps"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ClientException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ClientException", awserr.ErrAlreadyExists)
)

// ComputeEnvironment represents a Batch compute environment.
type ComputeEnvironment struct {
	Tags                   map[string]string `json:"tags,omitempty"`
	ComputeEnvironmentName string            `json:"computeEnvironmentName"`
	ComputeEnvironmentArn  string            `json:"computeEnvironmentArn"`
	Type                   string            `json:"type"`
	State                  string            `json:"state"`
	Status                 string            `json:"status"`
}

// ComputeEnvironmentOrder pairs a compute environment with its ordering in a job queue.
type ComputeEnvironmentOrder struct {
	ComputeEnvironment string `json:"computeEnvironment"`
	Order              int32  `json:"order"`
}

// JobQueue represents a Batch job queue.
type JobQueue struct {
	Tags                    map[string]string         `json:"tags,omitempty"`
	JobQueueName            string                    `json:"jobQueueName"`
	JobQueueArn             string                    `json:"jobQueueArn"`
	State                   string                    `json:"state"`
	Status                  string                    `json:"status"`
	ComputeEnvironmentOrder []ComputeEnvironmentOrder `json:"computeEnvironmentOrder,omitempty"`
	Priority                int32                     `json:"priority"`
}

// JobDefinition represents a Batch job definition.
type JobDefinition struct {
	Tags              map[string]string `json:"tags,omitempty"`
	JobDefinitionName string            `json:"jobDefinitionName"`
	JobDefinitionArn  string            `json:"jobDefinitionArn"`
	Type              string            `json:"type"`
	Status            string            `json:"status"`
	Revision          int32             `json:"revision"`
}

// InMemoryBackend stores AWS Batch state in memory.
type InMemoryBackend struct {
	computeEnvironments map[string]*ComputeEnvironment
	jobQueues           map[string]*JobQueue
	jobDefinitions      map[string]*JobDefinition
	jobDefRevisions     map[string]int32
	mu                  *lockmetrics.RWMutex
	accountID           string
	region              string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		computeEnvironments: make(map[string]*ComputeEnvironment),
		jobQueues:           make(map[string]*JobQueue),
		jobDefinitions:      make(map[string]*JobDefinition),
		jobDefRevisions:     make(map[string]int32),
		accountID:           accountID,
		region:              region,
		mu:                  lockmetrics.New("batch"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// lookupCEByNameOrARN returns a compute environment by name or ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) lookupCEByNameOrARN(nameOrARN string) (*ComputeEnvironment, bool) {
	if ce, ok := b.computeEnvironments[nameOrARN]; ok {
		return ce, true
	}

	for _, ce := range b.computeEnvironments {
		if ce.ComputeEnvironmentArn == nameOrARN {
			return ce, true
		}
	}

	return nil, false
}

// lookupJQByNameOrARN returns a job queue by name or ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) lookupJQByNameOrARN(nameOrARN string) (*JobQueue, bool) {
	if jq, ok := b.jobQueues[nameOrARN]; ok {
		return jq, true
	}

	for _, jq := range b.jobQueues {
		if jq.JobQueueArn == nameOrARN {
			return jq, true
		}
	}

	return nil, false
}

// CreateComputeEnvironment creates a new compute environment.
func (b *InMemoryBackend) CreateComputeEnvironment(
	name, ceType, state string,
	tags map[string]string,
) (*ComputeEnvironment, error) {
	b.mu.Lock("CreateComputeEnvironment")
	defer b.mu.Unlock()

	if _, ok := b.computeEnvironments[name]; ok {
		return nil, fmt.Errorf("%w: compute environment %s already exists", ErrAlreadyExists, name)
	}

	ceARN := arn.Build("batch", b.region, b.accountID, "compute-environment/"+name)

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	ce := &ComputeEnvironment{
		ComputeEnvironmentName: name,
		ComputeEnvironmentArn:  ceARN,
		Type:                   ceType,
		State:                  state,
		Status:                 "VALID",
		Tags:                   tagsCopy,
	}
	b.computeEnvironments[name] = ce
	cp := *ce

	return &cp, nil
}

// DescribeComputeEnvironments returns compute environments, optionally filtered by names/ARNs.
func (b *InMemoryBackend) DescribeComputeEnvironments(names []string) []*ComputeEnvironment {
	b.mu.RLock("DescribeComputeEnvironments")
	defer b.mu.RUnlock()

	if len(names) == 0 {
		list := make([]*ComputeEnvironment, 0, len(b.computeEnvironments))
		for _, ce := range b.computeEnvironments {
			cp := *ce
			list = append(list, &cp)
		}

		return list
	}

	list := make([]*ComputeEnvironment, 0, len(names))

	for _, nameOrARN := range names {
		if ce, ok := b.lookupCEByNameOrARN(nameOrARN); ok {
			cp := *ce
			list = append(list, &cp)
		}
	}

	return list
}

// UpdateComputeEnvironment updates the state of a compute environment.
func (b *InMemoryBackend) UpdateComputeEnvironment(nameOrARN, state string) (*ComputeEnvironment, error) {
	b.mu.Lock("UpdateComputeEnvironment")
	defer b.mu.Unlock()

	ce, ok := b.lookupCEByNameOrARN(nameOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: compute environment %s not found", ErrNotFound, nameOrARN)
	}

	if state != "" {
		ce.State = state
	}

	cp := *ce

	return &cp, nil
}

// DeleteComputeEnvironment removes a compute environment.
func (b *InMemoryBackend) DeleteComputeEnvironment(nameOrARN string) error {
	b.mu.Lock("DeleteComputeEnvironment")
	defer b.mu.Unlock()

	ce, ok := b.lookupCEByNameOrARN(nameOrARN)
	if !ok {
		return fmt.Errorf("%w: compute environment %s not found", ErrNotFound, nameOrARN)
	}

	delete(b.computeEnvironments, ce.ComputeEnvironmentName)

	return nil
}

// CreateJobQueue creates a new job queue.
func (b *InMemoryBackend) CreateJobQueue(
	name string,
	priority int32,
	state string,
	ceOrder []ComputeEnvironmentOrder,
	tags map[string]string,
) (*JobQueue, error) {
	b.mu.Lock("CreateJobQueue")
	defer b.mu.Unlock()

	if _, ok := b.jobQueues[name]; ok {
		return nil, fmt.Errorf("%w: job queue %s already exists", ErrAlreadyExists, name)
	}

	jqARN := arn.Build("batch", b.region, b.accountID, "job-queue/"+name)

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	orderCopy := make([]ComputeEnvironmentOrder, len(ceOrder))
	copy(orderCopy, ceOrder)

	jq := &JobQueue{
		JobQueueName:            name,
		JobQueueArn:             jqARN,
		State:                   state,
		Status:                  "VALID",
		Priority:                priority,
		ComputeEnvironmentOrder: orderCopy,
		Tags:                    tagsCopy,
	}
	b.jobQueues[name] = jq
	cp := *jq

	return &cp, nil
}

// DescribeJobQueues returns job queues, optionally filtered by names/ARNs.
func (b *InMemoryBackend) DescribeJobQueues(names []string) []*JobQueue {
	b.mu.RLock("DescribeJobQueues")
	defer b.mu.RUnlock()

	if len(names) == 0 {
		list := make([]*JobQueue, 0, len(b.jobQueues))
		for _, jq := range b.jobQueues {
			cp := *jq
			list = append(list, &cp)
		}

		return list
	}

	list := make([]*JobQueue, 0, len(names))

	for _, nameOrARN := range names {
		if jq, ok := b.lookupJQByNameOrARN(nameOrARN); ok {
			cp := *jq
			list = append(list, &cp)
		}
	}

	return list
}

// UpdateJobQueue updates a job queue's state and/or priority.
func (b *InMemoryBackend) UpdateJobQueue(nameOrARN string, priority *int32, state string) (*JobQueue, error) {
	b.mu.Lock("UpdateJobQueue")
	defer b.mu.Unlock()

	jq, ok := b.lookupJQByNameOrARN(nameOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: job queue %s not found", ErrNotFound, nameOrARN)
	}

	if state != "" {
		jq.State = state
	}

	if priority != nil {
		jq.Priority = *priority
	}

	cp := *jq

	return &cp, nil
}

// DeleteJobQueue removes a job queue.
func (b *InMemoryBackend) DeleteJobQueue(nameOrARN string) error {
	b.mu.Lock("DeleteJobQueue")
	defer b.mu.Unlock()

	jq, ok := b.lookupJQByNameOrARN(nameOrARN)
	if !ok {
		return fmt.Errorf("%w: job queue %s not found", ErrNotFound, nameOrARN)
	}

	delete(b.jobQueues, jq.JobQueueName)

	return nil
}

// RegisterJobDefinition registers a new job definition (or a new revision).
func (b *InMemoryBackend) RegisterJobDefinition(
	name, defType string,
	tags map[string]string,
) (*JobDefinition, error) {
	b.mu.Lock("RegisterJobDefinition")
	defer b.mu.Unlock()

	b.jobDefRevisions[name]++
	revision := b.jobDefRevisions[name]

	jdARN := arn.Build("batch", b.region, b.accountID, fmt.Sprintf("job-definition/%s:%d", name, revision))

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	jd := &JobDefinition{
		JobDefinitionName: name,
		JobDefinitionArn:  jdARN,
		Type:              defType,
		Status:            "ACTIVE",
		Revision:          revision,
		Tags:              tagsCopy,
	}
	b.jobDefinitions[jdARN] = jd
	cp := *jd

	return &cp, nil
}

// DescribeJobDefinitions returns job definitions, optionally filtered by names/ARNs.
func (b *InMemoryBackend) DescribeJobDefinitions(names []string) []*JobDefinition {
	b.mu.RLock("DescribeJobDefinitions")
	defer b.mu.RUnlock()

	if len(names) == 0 {
		list := make([]*JobDefinition, 0, len(b.jobDefinitions))
		for _, jd := range b.jobDefinitions {
			cp := *jd
			list = append(list, &cp)
		}

		return list
	}

	seen := make(map[string]bool)
	list := make([]*JobDefinition, 0, len(names))

	for _, nameOrARN := range names {
		if jd, ok := b.jobDefinitions[nameOrARN]; ok {
			if !seen[jd.JobDefinitionArn] {
				seen[jd.JobDefinitionArn] = true
				cp := *jd
				list = append(list, &cp)
			}

			continue
		}

		// Match by bare name (strip optional :revision suffix).
		baseName, _, _ := strings.Cut(nameOrARN, ":")

		for _, jd := range b.jobDefinitions {
			if jd.JobDefinitionName == baseName && !seen[jd.JobDefinitionArn] {
				seen[jd.JobDefinitionArn] = true
				cp := *jd
				list = append(list, &cp)
			}
		}
	}

	return list
}

// DeregisterJobDefinition marks a job definition as INACTIVE by ARN or name:revision.
func (b *InMemoryBackend) DeregisterJobDefinition(arnOrNameRev string) error {
	b.mu.Lock("DeregisterJobDefinition")
	defer b.mu.Unlock()

	// Try direct ARN lookup first.
	if jd, ok := b.jobDefinitions[arnOrNameRev]; ok {
		jd.Status = "INACTIVE"

		return nil
	}

	// Fall back to name:revision lookup (e.g. "my-job:3").
	for _, jd := range b.jobDefinitions {
		nameRev := fmt.Sprintf("%s:%d", jd.JobDefinitionName, jd.Revision)
		if nameRev == arnOrNameRev {
			jd.Status = "INACTIVE"

			return nil
		}
	}

	return fmt.Errorf("%w: job definition %s not found", ErrNotFound, arnOrNameRev)
}

// ListTagsForResource returns the tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if tags, ok := b.findTagsByARN(resourceARN); ok {
		out := make(map[string]string, len(tags))
		maps.Copy(out, tags)

		return out, nil
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// TagResource adds or updates tags on a resource identified by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	existing, ok := b.findTagsByARN(resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	if existing == nil {
		b.initTagsByARN(resourceARN)
		existing, _ = b.findTagsByARN(resourceARN)
	}

	maps.Copy(existing, tags)

	return nil
}

// UntagResource removes tags from a resource identified by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	existing, ok := b.findTagsByARN(resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	for _, k := range tagKeys {
		delete(existing, k)
	}

	return nil
}

// findTagsByARN looks up the tags map for a resource by ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findTagsByARN(resourceARN string) (map[string]string, bool) {
	for _, ce := range b.computeEnvironments {
		if ce.ComputeEnvironmentArn == resourceARN {
			return ce.Tags, true
		}
	}

	for _, jq := range b.jobQueues {
		if jq.JobQueueArn == resourceARN {
			return jq.Tags, true
		}
	}

	if jd, ok := b.jobDefinitions[resourceARN]; ok {
		return jd.Tags, true
	}

	return nil, false
}

// initTagsByARN ensures a resource has an initialised tags map.
// Caller must hold the write lock.
func (b *InMemoryBackend) initTagsByARN(resourceARN string) {
	for _, ce := range b.computeEnvironments {
		if ce.ComputeEnvironmentArn == resourceARN {
			ce.Tags = make(map[string]string)

			return
		}
	}

	for _, jq := range b.jobQueues {
		if jq.JobQueueArn == resourceARN {
			jq.Tags = make(map[string]string)

			return
		}
	}

	if jd, ok := b.jobDefinitions[resourceARN]; ok {
		jd.Tags = make(map[string]string)
	}
}
