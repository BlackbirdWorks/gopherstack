package batch

import (
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	statusValid = "VALID"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ClientException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ClientException", awserr.ErrAlreadyExists)
	// ErrValidation is returned when a request contains invalid parameters.
	ErrValidation = awserr.New("ClientException", awserr.ErrInvalidParameter)
)

const (
	jobDefStatusActive   = "ACTIVE"
	jobDefStatusInactive = "INACTIVE"

	jobStatusSubmitted = "SUBMITTED"
	jobStatusPending   = "PENDING"
	jobStatusRunnable  = "RUNNABLE"
	jobStatusStarting  = "STARTING"
	jobStatusRunning   = "RUNNING"
	jobStatusSucceeded = "SUCCEEDED"
	jobStatusFailed    = "FAILED"

	stateEnabled  = "ENABLED"
	stateDisabled = "DISABLED"

	resourceTypeReplenishable    = "REPLENISHABLE"
	resourceTypeNonReplenishable = "NON_REPLENISHABLE"

	msPerSecond = 1000.0

	maxJobNameLength       = 128
	maxCENameLength        = 128
	maxJobQueueNameLength  = 128
	defaultPaginationLimit = 100
)

// isValidCEType returns true if the given type is a supported compute environment type.
func isValidCEType(t string) bool {
	switch t {
	case "EC2", "SPOT", "FARGATE", "FARGATE_SPOT", "MANAGED", "UNMANAGED":
		return true
	}

	return false
}

// ComputeEnvironment represents a Batch compute environment.
type ComputeEnvironment struct {
	Tags                   map[string]string `json:"tags,omitempty"`
	ServiceRole            string            `json:"serviceRole,omitempty"`
	ComputeEnvironmentArn  string            `json:"computeEnvironmentArn"`
	Type                   string            `json:"type"`
	State                  string            `json:"state"`
	Status                 string            `json:"status"`
	ComputeEnvironmentName string            `json:"computeEnvironmentName"`
	InstanceRole           string            `json:"instanceRole,omitempty"`
	InstanceTypes          []string          `json:"instanceTypes,omitempty"`
	Subnets                []string          `json:"subnets,omitempty"`
	SecurityGroupIDs       []string          `json:"securityGroupIds,omitempty"`
	MinvCpus               int32             `json:"minvCpus,omitempty"`
	MaxvCpus               int32             `json:"maxvCpus,omitempty"`
	DesiredvCpus           int32             `json:"desiredvCpus,omitempty"`
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
	SchedulingPolicyArn     string                    `json:"schedulingPolicyArn,omitempty"`
	ComputeEnvironmentOrder []ComputeEnvironmentOrder `json:"computeEnvironmentOrder,omitempty"`
	Priority                int32                     `json:"priority"`
}

// ContainerProperties stores basic container configuration for a job definition.
type ContainerProperties struct {
	Image      string   `json:"image,omitempty"`
	JobRoleArn string   `json:"jobRoleArn,omitempty"`
	Command    []string `json:"command,omitempty"`
	Vcpus      int32    `json:"vcpus,omitempty"`
	Memory     int32    `json:"memory,omitempty"`
}

// JobDefinition represents a Batch job definition.
type JobDefinition struct {
	DeregisteredAt       *time.Time           `json:"deregisteredAt,omitempty"`
	Tags                 map[string]string    `json:"tags,omitempty"`
	Parameters           map[string]string    `json:"parameters,omitempty"`
	ContainerProperties  *ContainerProperties `json:"containerProperties,omitempty"`
	JobDefinitionName    string               `json:"jobDefinitionName"`
	JobDefinitionArn     string               `json:"jobDefinitionArn"`
	Type                 string               `json:"type"`
	Status               string               `json:"status"`
	PlatformCapabilities []string             `json:"platformCapabilities,omitempty"`
	Revision             int32                `json:"revision"`
	TimeoutSeconds       int32                `json:"timeoutSeconds,omitempty"`
}

// Job represents a submitted Batch job.
type Job struct {
	StoppedAt     *int64            `json:"stoppedAt,omitempty"`
	Tags          map[string]string `json:"tags,omitempty"`
	Parameters    map[string]string `json:"parameters,omitempty"`
	StartedAt     *int64            `json:"startedAt,omitempty"`
	JobID         string            `json:"jobId"`
	JobARN        string            `json:"jobArn"`
	JobName       string            `json:"jobName"`
	JobQueue      string            `json:"jobQueue"`
	JobDefinition string            `json:"jobDefinition"`
	Status        string            `json:"status"`
	StatusReason  string            `json:"statusReason,omitempty"`
	CreatedAt     int64             `json:"createdAt"`
}

// ConsumableResource represents a Batch consumable resource.
type ConsumableResource struct {
	Tags                   map[string]string `json:"tags,omitempty"`
	ConsumableResourceName string            `json:"consumableResourceName"`
	ConsumableResourceArn  string            `json:"consumableResourceArn"`
	ResourceType           string            `json:"resourceType,omitempty"`
	CreatedAt              int64             `json:"createdAt"`
	TotalQuantity          int64             `json:"totalQuantity"`
	AvailableQuantity      int64             `json:"availableQuantity"`
	InUseQuantity          int64             `json:"inUseQuantity"`
}

// SchedulingPolicy represents a Batch scheduling policy.
type SchedulingPolicy struct {
	Tags map[string]string `json:"tags,omitempty"`
	Arn  string            `json:"arn"`
	Name string            `json:"name"`
}

// ServiceEnvironment represents a Batch service environment.
type ServiceEnvironment struct {
	Tags                   map[string]string `json:"tags,omitempty"`
	ServiceEnvironmentName string            `json:"serviceEnvironmentName"`
	ServiceEnvironmentArn  string            `json:"serviceEnvironmentArn"`
	ServiceEnvironmentType string            `json:"serviceEnvironmentType"`
	State                  string            `json:"state"`
	Status                 string            `json:"status"`
}

// ServiceJob represents a Batch service job.
type ServiceJob struct {
	Tags               map[string]string `json:"tags,omitempty"`
	StartedAt          *int64            `json:"startedAt,omitempty"`
	StoppedAt          *int64            `json:"stoppedAt,omitempty"`
	ServiceJobID       string            `json:"serviceJobId"`
	ServiceJobArn      string            `json:"serviceJobArn"`
	ServiceJobName     string            `json:"serviceJobName"`
	ServiceEnvironment string            `json:"serviceEnvironment"`
	Status             string            `json:"status"`
	StatusReason       string            `json:"statusReason,omitempty"`
	CreatedAt          int64             `json:"createdAt"`
}

// JobQueueSnapshot represents the front-of-queue state for a job queue.
type JobQueueSnapshot struct {
	FrontOfQueue *FrontOfQueue `json:"frontOfQueue,omitempty"`
}

// FrontOfQueue holds jobs at the front of a job queue.
type FrontOfQueue struct {
	Jobs      []FrontOfQueueJob `json:"jobs,omitempty"`
	Timestamp float64           `json:"timestamp"`
}

// FrontOfQueueJob represents a single job at the front of a queue.
type FrontOfQueueJob struct {
	JobArn                 string  `json:"jobArn"`
	EarliestTimeAtPosition float64 `json:"earliestTimeAtPosition"`
}

// JobDependency represents a dependency between jobs.
type JobDependency struct {
	JobID string `json:"jobId,omitempty"`
	Type  string `json:"type,omitempty"`
}

// RetryStrategy configures automatic retry behavior.
type RetryStrategy struct {
	Attempts int32 `json:"attempts,omitempty"`
}

// JobTimeout configures the timeout for a job.
type JobTimeout struct {
	AttemptDurationSeconds int32 `json:"attemptDurationSeconds,omitempty"`
}

// InMemoryBackend stores AWS Batch state in memory.
type InMemoryBackend struct {
	computeEnvironments    map[string]*ComputeEnvironment
	jobQueues              map[string]*JobQueue
	jobDefinitions         map[string]*JobDefinition
	jobs                   map[string]*Job     // job ID → Job
	jobsByQueue            map[string][]string // queue ARN/name → []jobID
	jobDefRevisions        map[string]int32
	consumableResources    map[string]*ConsumableResource
	schedulingPolicies     map[string]*SchedulingPolicy // ARN → SchedulingPolicy
	serviceEnvironments    map[string]*ServiceEnvironment
	serviceJobs            map[string]*ServiceJob // serviceJobID → ServiceJob
	schedulingPolicyByName map[string]string      // name → ARN
	mu                     *lockmetrics.RWMutex
	accountID              string
	region                 string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		computeEnvironments:    make(map[string]*ComputeEnvironment),
		jobQueues:              make(map[string]*JobQueue),
		jobDefinitions:         make(map[string]*JobDefinition),
		jobs:                   make(map[string]*Job),
		jobsByQueue:            make(map[string][]string),
		jobDefRevisions:        make(map[string]int32),
		consumableResources:    make(map[string]*ConsumableResource),
		schedulingPolicies:     make(map[string]*SchedulingPolicy),
		serviceEnvironments:    make(map[string]*ServiceEnvironment),
		serviceJobs:            make(map[string]*ServiceJob),
		schedulingPolicyByName: make(map[string]string),
		accountID:              accountID,
		region:                 region,
		mu:                     lockmetrics.New("batch"),
	}
}

// Reset clears all state from the backend.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.computeEnvironments = make(map[string]*ComputeEnvironment)
	b.jobQueues = make(map[string]*JobQueue)
	b.jobDefinitions = make(map[string]*JobDefinition)
	b.jobs = make(map[string]*Job)
	b.jobsByQueue = make(map[string][]string)
	b.jobDefRevisions = make(map[string]int32)
	b.consumableResources = make(map[string]*ConsumableResource)
	b.schedulingPolicies = make(map[string]*SchedulingPolicy)
	b.serviceEnvironments = make(map[string]*ServiceEnvironment)
	b.serviceJobs = make(map[string]*ServiceJob)
	b.schedulingPolicyByName = make(map[string]string)
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
	serviceRole string,
	minvCpus, maxvCpus int32,
	instanceTypes, subnets, securityGroupIDs []string,
	instanceRole string,
) (*ComputeEnvironment, error) {
	b.mu.Lock("CreateComputeEnvironment")
	defer b.mu.Unlock()

	if len(name) == 0 || len(name) > maxCENameLength {
		return nil, fmt.Errorf(
			"%w: computeEnvironmentName must be between 1 and %d characters",
			ErrValidation, maxCENameLength,
		)
	}

	if state != "" && state != stateEnabled && state != stateDisabled {
		return nil, fmt.Errorf("%w: state must be %s or %s", ErrValidation, stateEnabled, stateDisabled)
	}

	if !isValidCEType(ceType) {
		return nil, fmt.Errorf("%w: invalid compute environment type %q", ErrValidation, ceType)
	}

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
		Status:                 statusValid,
		Tags:                   tagsCopy,
		ServiceRole:            serviceRole,
		MinvCpus:               minvCpus,
		MaxvCpus:               maxvCpus,
		InstanceTypes:          instanceTypes,
		Subnets:                subnets,
		SecurityGroupIDs:       securityGroupIDs,
		InstanceRole:           instanceRole,
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

		sort.Slice(list, func(i, j int) bool {
			return list[i].ComputeEnvironmentName < list[j].ComputeEnvironmentName
		})

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

// UpdateComputeEnvironment updates the state and/or service role of a compute environment.
func (b *InMemoryBackend) UpdateComputeEnvironment(nameOrARN, state, serviceRole string) (*ComputeEnvironment, error) {
	b.mu.Lock("UpdateComputeEnvironment")
	defer b.mu.Unlock()

	ce, ok := b.lookupCEByNameOrARN(nameOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: compute environment %s not found", ErrNotFound, nameOrARN)
	}

	if state != "" && state != stateEnabled && state != stateDisabled {
		return nil, fmt.Errorf("%w: state must be %s or %s", ErrValidation, stateEnabled, stateDisabled)
	}

	if state != "" {
		ce.State = state
	}

	if serviceRole != "" {
		ce.ServiceRole = serviceRole
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

	if ce.State != "DISABLED" {
		return fmt.Errorf(
			"%w: compute environment %s must be DISABLED before it can be deleted",
			ErrValidation,
			nameOrARN,
		)
	}

	// Check if referenced by any job queue.
	for _, jq := range b.jobQueues {
		for _, ceOrder := range jq.ComputeEnvironmentOrder {
			if ceOrder.ComputeEnvironment == ce.ComputeEnvironmentName ||
				ceOrder.ComputeEnvironment == ce.ComputeEnvironmentArn {
				return fmt.Errorf(
					"%w: compute environment %s is referenced by one or more job queues",
					ErrValidation,
					nameOrARN,
				)
			}
		}
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
	schedulingPolicyArn string,
) (*JobQueue, error) {
	b.mu.Lock("CreateJobQueue")
	defer b.mu.Unlock()

	if len(name) == 0 || len(name) > maxJobQueueNameLength {
		return nil, fmt.Errorf(
			"%w: jobQueueName must be between 1 and %d characters",
			ErrValidation, maxJobQueueNameLength,
		)
	}

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
		Status:                  statusValid,
		Priority:                priority,
		ComputeEnvironmentOrder: orderCopy,
		Tags:                    tagsCopy,
		SchedulingPolicyArn:     schedulingPolicyArn,
	}
	b.jobQueues[name] = jq
	cp := *jq

	return &cp, nil
}

// DescribeJobQueues returns job queues, optionally filtered by names/ARNs.
// When names are provided, all matching queues are returned without pagination.
// When names is empty, results are paginated using maxResults and nextToken.
func (b *InMemoryBackend) DescribeJobQueues(names []string, maxResults int32, nextToken string) ([]*JobQueue, string) {
	b.mu.RLock("DescribeJobQueues")
	defer b.mu.RUnlock()

	if len(names) > 0 {
		list := make([]*JobQueue, 0, len(names))

		for _, nameOrARN := range names {
			if jq, ok := b.lookupJQByNameOrARN(nameOrARN); ok {
				cp := *jq
				list = append(list, &cp)
			}
		}

		return list, ""
	}

	all := make([]*JobQueue, 0, len(b.jobQueues))
	for _, jq := range b.jobQueues {
		cp := *jq
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].JobQueueName < all[j].JobQueueName
	})

	limit := maxResults
	if limit <= 0 {
		limit = defaultPaginationLimit
	}

	offset := 0
	if nextToken != "" {
		if n, err := strconv.Atoi(nextToken); err == nil && n > 0 {
			offset = n
		}
	}

	if offset >= len(all) {
		return []*JobQueue{}, ""
	}

	end := min(offset+int(limit), len(all))

	page := all[offset:end]

	outToken := ""
	if end < len(all) {
		outToken = strconv.Itoa(end)
	}

	return page, outToken
}

// UpdateJobQueue updates a job queue's state, priority, and/or compute environment order.
func (b *InMemoryBackend) UpdateJobQueue(
	nameOrARN string,
	priority *int32,
	state string,
	ceOrder []ComputeEnvironmentOrder,
) (*JobQueue, error) {
	b.mu.Lock("UpdateJobQueue")
	defer b.mu.Unlock()

	jq, ok := b.lookupJQByNameOrARN(nameOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: job queue %s not found", ErrNotFound, nameOrARN)
	}

	if state != "" && state != stateEnabled && state != stateDisabled {
		return nil, fmt.Errorf("%w: state must be %s or %s", ErrValidation, stateEnabled, stateDisabled)
	}

	if state != "" {
		jq.State = state
	}

	if priority != nil {
		jq.Priority = *priority
	}

	if ceOrder != nil {
		orderCopy := make([]ComputeEnvironmentOrder, len(ceOrder))
		copy(orderCopy, ceOrder)
		jq.ComputeEnvironmentOrder = orderCopy
	}

	cp := *jq

	return &cp, nil
}

// DeleteJobQueue removes a job queue and all associated jobs.
// The queue must be in DISABLED state before deletion.
func (b *InMemoryBackend) DeleteJobQueue(nameOrARN string) error {
	b.mu.Lock("DeleteJobQueue")
	defer b.mu.Unlock()

	jq, ok := b.lookupJQByNameOrARN(nameOrARN)
	if !ok {
		return fmt.Errorf("%w: job queue %s not found", ErrNotFound, nameOrARN)
	}

	if jq.State != "DISABLED" {
		return fmt.Errorf("%w: job queue %s must be DISABLED before it can be deleted", ErrValidation, nameOrARN)
	}

	queueName := jq.JobQueueName

	// Clean up all jobs associated with this queue to prevent orphaned entries.
	for _, jobID := range b.jobsByQueue[queueName] {
		delete(b.jobs, jobID)
	}

	delete(b.jobsByQueue, queueName)
	delete(b.jobQueues, queueName)

	return nil
}

// RegisterJobDefinition registers a new job definition (or a new revision).
func (b *InMemoryBackend) RegisterJobDefinition(
	name, defType string,
	tags map[string]string,
	platformCapabilities []string,
	timeoutSeconds int32,
	containerProps *ContainerProperties,
	parameters map[string]string,
) (*JobDefinition, error) {
	b.mu.Lock("RegisterJobDefinition")
	defer b.mu.Unlock()

	b.jobDefRevisions[name]++
	revision := b.jobDefRevisions[name]

	jdARN := arn.Build("batch", b.region, b.accountID, fmt.Sprintf("job-definition/%s:%d", name, revision))

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	jd := &JobDefinition{
		JobDefinitionName:    name,
		JobDefinitionArn:     jdARN,
		Type:                 defType,
		Status:               jobDefStatusActive,
		Revision:             revision,
		Tags:                 tagsCopy,
		PlatformCapabilities: platformCapabilities,
		TimeoutSeconds:       timeoutSeconds,
		ContainerProperties:  containerProps,
		Parameters:           maps.Clone(parameters),
	}
	b.jobDefinitions[jdARN] = jd
	cp := *jd

	return &cp, nil
}

// DescribeJobDefinitions returns job definitions, optionally filtered by names/ARNs.
func (b *InMemoryBackend) DescribeJobDefinitions(names []string, status, jobDefinitionName string) []*JobDefinition {
	b.mu.RLock("DescribeJobDefinitions")
	defer b.mu.RUnlock()

	if len(names) == 0 {
		return b.describeAllJobDefinitions(status, jobDefinitionName)
	}

	return b.describeJobDefinitionsByNames(names, status)
}

func (b *InMemoryBackend) describeAllJobDefinitions(status, jobDefinitionName string) []*JobDefinition {
	list := make([]*JobDefinition, 0, len(b.jobDefinitions))

	for _, jd := range b.jobDefinitions {
		if status != "" && jd.Status != status {
			continue
		}

		if jobDefinitionName != "" && jd.JobDefinitionName != jobDefinitionName {
			continue
		}

		cp := *jd
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].Revision > list[j].Revision
	})

	return list
}

func (b *InMemoryBackend) describeJobDefinitionsByNames(names []string, status string) []*JobDefinition {
	seen := make(map[string]bool)
	list := make([]*JobDefinition, 0, len(names))

	for _, nameOrARN := range names {
		if jd, ok := b.jobDefinitions[nameOrARN]; ok {
			if !seen[jd.JobDefinitionArn] && (status == "" || jd.Status == status) {
				seen[jd.JobDefinitionArn] = true
				cp := *jd
				list = append(list, &cp)
			}

			continue
		}

		baseName, _, _ := strings.Cut(nameOrARN, ":")

		for _, jd := range b.jobDefinitions {
			if jd.JobDefinitionName == baseName && !seen[jd.JobDefinitionArn] && (status == "" || jd.Status == status) {
				seen[jd.JobDefinitionArn] = true
				cp := *jd
				list = append(list, &cp)
			}
		}
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].Revision > list[j].Revision
	})

	return list
}

// DeregisterJobDefinition marks a job definition as INACTIVE by ARN or name:revision.
// INACTIVE definitions remain visible in DescribeJobDefinitions (matching AWS behavior)
// and are swept by the janitor after the configured TTL.
func (b *InMemoryBackend) DeregisterJobDefinition(arnOrNameRev string) error {
	b.mu.Lock("DeregisterJobDefinition")
	defer b.mu.Unlock()

	now := time.Now()

	// Try direct ARN lookup first.
	if jd, ok := b.jobDefinitions[arnOrNameRev]; ok {
		jd.Status = jobDefStatusInactive
		jd.DeregisteredAt = &now

		return nil
	}

	// Fall back to name:revision lookup (e.g. "my-job:3").
	for _, jd := range b.jobDefinitions {
		nameRev := fmt.Sprintf("%s:%d", jd.JobDefinitionName, jd.Revision)
		if nameRev == arnOrNameRev {
			jd.Status = jobDefStatusInactive
			jd.DeregisteredAt = &now

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
	if tags, ok := b.findTagsInCoreResources(resourceARN); ok {
		return tags, true
	}

	return b.findTagsInPolicyResources(resourceARN)
}

func (b *InMemoryBackend) findTagsInCoreResources(resourceARN string) (map[string]string, bool) {
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

	for _, j := range b.jobs {
		if j.JobARN == resourceARN {
			return j.Tags, true
		}
	}

	for _, cr := range b.consumableResources {
		if cr.ConsumableResourceArn == resourceARN {
			return cr.Tags, true
		}
	}

	return nil, false
}

func (b *InMemoryBackend) findTagsInPolicyResources(resourceARN string) (map[string]string, bool) {
	for _, sp := range b.schedulingPolicies {
		if sp.Arn == resourceARN {
			return sp.Tags, true
		}
	}

	for _, se := range b.serviceEnvironments {
		if se.ServiceEnvironmentArn == resourceARN {
			return se.Tags, true
		}
	}

	for _, sj := range b.serviceJobs {
		if sj.ServiceJobArn == resourceARN {
			return sj.Tags, true
		}
	}

	return nil, false
}

// initTagsByARN ensures a resource has an initialised tags map.
// Caller must hold the write lock.
func (b *InMemoryBackend) initTagsByARN(resourceARN string) {
	if b.initTagsInCoreResources(resourceARN) {
		return
	}

	b.initTagsInPolicyResources(resourceARN)
}

func (b *InMemoryBackend) initTagsInCoreResources(resourceARN string) bool {
	for _, ce := range b.computeEnvironments {
		if ce.ComputeEnvironmentArn == resourceARN {
			ce.Tags = make(map[string]string)

			return true
		}
	}

	for _, jq := range b.jobQueues {
		if jq.JobQueueArn == resourceARN {
			jq.Tags = make(map[string]string)

			return true
		}
	}

	if jd, ok := b.jobDefinitions[resourceARN]; ok {
		jd.Tags = make(map[string]string)

		return true
	}

	for _, j := range b.jobs {
		if j.JobARN == resourceARN {
			j.Tags = make(map[string]string)

			return true
		}
	}

	for _, cr := range b.consumableResources {
		if cr.ConsumableResourceArn == resourceARN {
			cr.Tags = make(map[string]string)

			return true
		}
	}

	return false
}

func (b *InMemoryBackend) initTagsInPolicyResources(resourceARN string) {
	for _, sp := range b.schedulingPolicies {
		if sp.Arn == resourceARN {
			sp.Tags = make(map[string]string)

			return
		}
	}

	for _, se := range b.serviceEnvironments {
		if se.ServiceEnvironmentArn == resourceARN {
			se.Tags = make(map[string]string)

			return
		}
	}

	for _, sj := range b.serviceJobs {
		if sj.ServiceJobArn == resourceARN {
			sj.Tags = make(map[string]string)

			return
		}
	}
}

// SubmitJob submits a new job to the specified queue.
func (b *InMemoryBackend) SubmitJob(
	name, queue, jobDefinition string,
	tags map[string]string,
	parameters map[string]string,
	_ []JobDependency,
	_ *RetryStrategy,
	_ *JobTimeout,
) (*Job, error) {
	b.mu.Lock("SubmitJob")
	defer b.mu.Unlock()

	if len(name) == 0 || len(name) > maxJobNameLength {
		return nil, fmt.Errorf("%w: jobName must be between 1 and %d characters", ErrValidation, maxJobNameLength)
	}

	jq, ok := b.lookupJQByNameOrARN(queue)
	if !ok {
		return nil, fmt.Errorf("%w: job queue %s not found", ErrNotFound, queue)
	}

	if jq.State == stateDisabled {
		return nil, fmt.Errorf("%w: job queue %s is %s", ErrValidation, queue, stateDisabled)
	}

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	paramsCopy := maps.Clone(parameters)

	now := time.Now().UnixMilli()
	jobID := uuid.NewString()
	jobARN := arn.Build("batch", b.region, b.accountID, "job/"+jobID)

	j := &Job{
		JobID:   jobID,
		JobARN:  jobARN,
		JobName: name,
		// Always store the canonical queue name for consistency.
		JobQueue:      jq.JobQueueName,
		JobDefinition: jobDefinition,
		Status:        jobStatusSubmitted,
		CreatedAt:     now,
		Tags:          tagsCopy,
		Parameters:    paramsCopy,
	}
	b.jobs[jobID] = j
	b.jobsByQueue[jq.JobQueueName] = append(b.jobsByQueue[jq.JobQueueName], jobID)

	cp := *j
	cp.Tags = maps.Clone(j.Tags)

	return &cp, nil
}

// listAllJobs returns all jobs across all queues filtered by status.
func (b *InMemoryBackend) listAllJobs(status string) []*Job {
	all := make([]*Job, 0, len(b.jobs))

	for _, j := range b.jobs {
		if status != "" && j.Status != status {
			continue
		}

		cp := *j
		cp.Tags = maps.Clone(j.Tags)
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].CreatedAt < all[j].CreatedAt })

	return all
}

// listQueueJobs returns jobs in the given queue filtered by status.
func (b *InMemoryBackend) listQueueJobs(queue, status string) ([]*Job, error) {
	jq, ok := b.lookupJQByNameOrARN(queue)
	if !ok {
		return nil, fmt.Errorf("%w: job queue %s not found", ErrNotFound, queue)
	}

	ids := b.jobsByQueue[jq.JobQueueName]
	all := make([]*Job, 0, len(ids))

	for _, id := range ids {
		j, exists := b.jobs[id]
		if !exists {
			continue
		}

		if status != "" && j.Status != status {
			continue
		}

		cp := *j
		cp.Tags = maps.Clone(j.Tags)
		all = append(all, &cp)
	}

	return all, nil
}

// ListJobs returns job summaries for a queue, optionally filtered by status.
// Pagination is controlled via maxResults and nextToken (token encodes an integer offset).
func (b *InMemoryBackend) ListJobs(queue, status, nextToken string, maxResults int32) ([]*Job, string, error) {
	b.mu.RLock("ListJobs")
	defer b.mu.RUnlock()

	limit := maxResults
	if limit <= 0 {
		limit = defaultPaginationLimit
	}

	offset := 0
	if nextToken != "" {
		if n, err := strconv.Atoi(nextToken); err == nil && n > 0 {
			offset = n
		}
	}

	var (
		all []*Job
		err error
	)

	if queue == "" {
		all = b.listAllJobs(status)
	} else {
		all, err = b.listQueueJobs(queue, status)
		if err != nil {
			return nil, "", err
		}
	}

	if offset >= len(all) {
		return []*Job{}, "", nil
	}

	end := min(offset+int(limit), len(all))
	page := all[offset:end]

	outToken := ""
	if end < len(all) {
		outToken = strconv.Itoa(end)
	}

	return page, outToken, nil
}

// DescribeJobs returns full job details for the given job IDs.
func (b *InMemoryBackend) DescribeJobs(jobIDs []string) []*Job {
	b.mu.RLock("DescribeJobs")
	defer b.mu.RUnlock()

	out := make([]*Job, 0, len(jobIDs))

	for _, id := range jobIDs {
		j, ok := b.jobs[id]
		if !ok {
			// Try ARN lookup
			for _, jj := range b.jobs {
				if jj.JobARN == id {
					j = jj
					ok = true

					break
				}
			}
		}
		if !ok {
			continue
		}

		cp := *j
		cp.Tags = maps.Clone(j.Tags)
		out = append(out, &cp)
	}

	return out
}

// TerminateJob marks a job as FAILED with the given reason.
func (b *InMemoryBackend) TerminateJob(jobID, reason string) error {
	b.mu.Lock("TerminateJob")
	defer b.mu.Unlock()

	j, ok := b.jobs[jobID]
	if !ok {
		return fmt.Errorf("%w: job %s not found", ErrNotFound, jobID)
	}

	if j.Status == jobStatusSucceeded || j.Status == jobStatusFailed {
		return fmt.Errorf("%w: job %s is already in terminal state %s", ErrValidation, jobID, j.Status)
	}

	now := time.Now().UnixMilli()
	j.Status = jobStatusFailed
	j.StatusReason = reason
	j.StoppedAt = &now

	return nil
}

// CancelJob cancels a job in SUBMITTED, PENDING, or RUNNABLE state.
func (b *InMemoryBackend) CancelJob(jobID, reason string) error {
	b.mu.Lock("CancelJob")
	defer b.mu.Unlock()

	j, ok := b.jobs[jobID]
	if !ok {
		return fmt.Errorf("%w: job %s not found", ErrNotFound, jobID)
	}

	switch j.Status {
	case jobStatusSubmitted, jobStatusPending, jobStatusRunnable:
		now := time.Now().UnixMilli()
		j.Status = jobStatusFailed
		j.StatusReason = reason
		j.StoppedAt = &now

		return nil
	default:
		return fmt.Errorf("%w: cannot cancel job %s in %s state", ErrValidation, jobID, j.Status)
	}
}

// CreateConsumableResource creates a new consumable resource.
func (b *InMemoryBackend) CreateConsumableResource(
	name, resourceType string,
	totalQuantity int64,
	tags map[string]string,
) (*ConsumableResource, error) {
	b.mu.Lock("CreateConsumableResource")
	defer b.mu.Unlock()

	if _, ok := b.consumableResources[name]; ok {
		return nil, fmt.Errorf("%w: consumable resource %s already exists", ErrAlreadyExists, name)
	}

	if resourceType == "" {
		resourceType = resourceTypeReplenishable
	}

	if resourceType != resourceTypeReplenishable && resourceType != resourceTypeNonReplenishable {
		return nil, fmt.Errorf("%w: invalid resource type %s", ErrValidation, resourceType)
	}

	crARN := arn.Build("batch", b.region, b.accountID, "consumable-resource/"+name)

	cr := &ConsumableResource{
		ConsumableResourceName: name,
		ConsumableResourceArn:  crARN,
		ResourceType:           resourceType,
		TotalQuantity:          totalQuantity,
		AvailableQuantity:      totalQuantity,
		InUseQuantity:          0,
		CreatedAt:              time.Now().UnixMilli(),
		Tags:                   maps.Clone(tags),
	}
	b.consumableResources[name] = cr
	cp := *cr

	return &cp, nil
}

// DeleteConsumableResource removes a consumable resource by name or ARN.
func (b *InMemoryBackend) DeleteConsumableResource(nameOrARN string) error {
	b.mu.Lock("DeleteConsumableResource")
	defer b.mu.Unlock()

	cr, ok := b.lookupConsumableResourceByNameOrARN(nameOrARN)
	if !ok {
		return fmt.Errorf("%w: consumable resource %s not found", ErrNotFound, nameOrARN)
	}

	delete(b.consumableResources, cr.ConsumableResourceName)

	return nil
}

// DescribeConsumableResource returns details for a consumable resource identified by name or ARN.
func (b *InMemoryBackend) DescribeConsumableResource(nameOrARN string) (*ConsumableResource, error) {
	b.mu.RLock("DescribeConsumableResource")
	defer b.mu.RUnlock()

	cr, ok := b.lookupConsumableResourceByNameOrARN(nameOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: consumable resource %s not found", ErrNotFound, nameOrARN)
	}

	cp := *cr
	cp.Tags = maps.Clone(cr.Tags)

	return &cp, nil
}

// lookupConsumableResourceByNameOrARN returns a consumable resource by name or ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) lookupConsumableResourceByNameOrARN(nameOrARN string) (*ConsumableResource, bool) {
	if cr, ok := b.consumableResources[nameOrARN]; ok {
		return cr, true
	}

	for _, cr := range b.consumableResources {
		if cr.ConsumableResourceArn == nameOrARN {
			return cr, true
		}
	}

	return nil, false
}

// CreateSchedulingPolicy creates a new scheduling policy.
func (b *InMemoryBackend) CreateSchedulingPolicy(name string, tags map[string]string) (*SchedulingPolicy, error) {
	b.mu.Lock("CreateSchedulingPolicy")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	if _, ok := b.schedulingPolicyByName[name]; ok {
		return nil, fmt.Errorf("%w: scheduling policy %s already exists", ErrAlreadyExists, name)
	}

	policyARN := arn.Build("batch", b.region, b.accountID, "scheduling-policy/"+name)

	sp := &SchedulingPolicy{
		Arn:  policyARN,
		Name: name,
		Tags: maps.Clone(tags),
	}
	b.schedulingPolicies[policyARN] = sp
	b.schedulingPolicyByName[name] = policyARN
	cp := *sp

	return &cp, nil
}

// DeleteSchedulingPolicy removes a scheduling policy by ARN.
func (b *InMemoryBackend) DeleteSchedulingPolicy(policyARN string) error {
	b.mu.Lock("DeleteSchedulingPolicy")
	defer b.mu.Unlock()

	sp, ok := b.schedulingPolicies[policyARN]
	if !ok {
		return fmt.Errorf("%w: scheduling policy %s not found", ErrNotFound, policyARN)
	}

	delete(b.schedulingPolicyByName, sp.Name)
	delete(b.schedulingPolicies, policyARN)

	return nil
}

// CreateServiceEnvironment creates a new service environment.
func (b *InMemoryBackend) CreateServiceEnvironment(
	name, envType, state string,
	tags map[string]string,
) (*ServiceEnvironment, error) {
	b.mu.Lock("CreateServiceEnvironment")
	defer b.mu.Unlock()

	if _, ok := b.serviceEnvironments[name]; ok {
		return nil, fmt.Errorf("%w: service environment %s already exists", ErrAlreadyExists, name)
	}

	seARN := arn.Build("batch", b.region, b.accountID, "service-environment/"+name)

	if state == "" {
		state = stateEnabled
	}

	se := &ServiceEnvironment{
		ServiceEnvironmentName: name,
		ServiceEnvironmentArn:  seARN,
		ServiceEnvironmentType: envType,
		State:                  state,
		Status:                 statusValid,
		Tags:                   maps.Clone(tags),
	}
	b.serviceEnvironments[name] = se
	cp := *se

	return &cp, nil
}

// DeleteServiceEnvironment removes a service environment by name or ARN.
func (b *InMemoryBackend) DeleteServiceEnvironment(nameOrARN string) error {
	b.mu.Lock("DeleteServiceEnvironment")
	defer b.mu.Unlock()

	se, ok := b.lookupServiceEnvironmentByNameOrARN(nameOrARN)
	if !ok {
		return fmt.Errorf("%w: service environment %s not found", ErrNotFound, nameOrARN)
	}

	delete(b.serviceEnvironments, se.ServiceEnvironmentName)

	return nil
}

// lookupServiceEnvironmentByNameOrARN returns a service environment by name or ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) lookupServiceEnvironmentByNameOrARN(nameOrARN string) (*ServiceEnvironment, bool) {
	if se, ok := b.serviceEnvironments[nameOrARN]; ok {
		return se, true
	}

	for _, se := range b.serviceEnvironments {
		if se.ServiceEnvironmentArn == nameOrARN {
			return se, true
		}
	}

	return nil, false
}

// UpdateConsumableResource updates the quantity of a consumable resource.
func (b *InMemoryBackend) UpdateConsumableResource(
	nameOrARN, operation string,
	quantity int64,
) (*ConsumableResource, error) {
	b.mu.Lock("UpdateConsumableResource")
	defer b.mu.Unlock()

	cr, ok := b.lookupConsumableResourceByNameOrARN(nameOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: consumable resource %s not found", ErrNotFound, nameOrARN)
	}

	if quantity < 0 {
		return nil, fmt.Errorf("%w: quantity must be non-negative", ErrValidation)
	}

	if operation == "" {
		operation = "SET"
	}

	switch operation {
	case "SET":
		cr.TotalQuantity = quantity
		cr.AvailableQuantity = quantity
	case "ADD":
		cr.TotalQuantity += quantity
		cr.AvailableQuantity += quantity
	case "REMOVE":
		if quantity > cr.TotalQuantity {
			return nil, fmt.Errorf(
				"%w: cannot remove %d from total quantity %d",
				ErrValidation,
				quantity,
				cr.TotalQuantity,
			)
		}

		cr.TotalQuantity -= quantity
		cr.AvailableQuantity -= quantity
	default:
		return nil, fmt.Errorf("%w: unsupported operation %s", ErrValidation, operation)
	}

	cp := *cr
	cp.Tags = maps.Clone(cr.Tags)

	return &cp, nil
}

// ListConsumableResources returns all consumable resources sorted by name.
func (b *InMemoryBackend) ListConsumableResources() []*ConsumableResource {
	b.mu.RLock("ListConsumableResources")
	defer b.mu.RUnlock()

	list := make([]*ConsumableResource, 0, len(b.consumableResources))

	for _, cr := range b.consumableResources {
		cp := *cr
		cp.Tags = maps.Clone(cr.Tags)
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].ConsumableResourceName < list[j].ConsumableResourceName
	})

	return list
}

// ListSchedulingPolicies returns all scheduling policies sorted by ARN.
func (b *InMemoryBackend) ListSchedulingPolicies() []*SchedulingPolicy {
	b.mu.RLock("ListSchedulingPolicies")
	defer b.mu.RUnlock()

	list := make([]*SchedulingPolicy, 0, len(b.schedulingPolicies))

	for _, sp := range b.schedulingPolicies {
		cp := *sp
		cp.Tags = maps.Clone(sp.Tags)
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Arn < list[j].Arn })

	return list
}

// DescribeSchedulingPolicies returns scheduling policies, optionally filtered by ARNs.
func (b *InMemoryBackend) DescribeSchedulingPolicies(arns []string) []*SchedulingPolicy {
	b.mu.RLock("DescribeSchedulingPolicies")
	defer b.mu.RUnlock()

	if len(arns) == 0 {
		list := make([]*SchedulingPolicy, 0, len(b.schedulingPolicies))
		for _, sp := range b.schedulingPolicies {
			cp := *sp
			cp.Tags = maps.Clone(sp.Tags)
			list = append(list, &cp)
		}

		sort.Slice(list, func(i, j int) bool { return list[i].Arn < list[j].Arn })

		return list
	}

	list := make([]*SchedulingPolicy, 0, len(arns))

	for _, a := range arns {
		if sp, ok := b.schedulingPolicies[a]; ok {
			cp := *sp
			cp.Tags = maps.Clone(sp.Tags)
			list = append(list, &cp)
		}
	}

	return list
}

// DescribeServiceEnvironments returns service environments, optionally filtered by names/ARNs.
func (b *InMemoryBackend) DescribeServiceEnvironments(names []string) []*ServiceEnvironment {
	b.mu.RLock("DescribeServiceEnvironments")
	defer b.mu.RUnlock()

	if len(names) == 0 {
		list := make([]*ServiceEnvironment, 0, len(b.serviceEnvironments))
		for _, se := range b.serviceEnvironments {
			cp := *se
			cp.Tags = maps.Clone(se.Tags)
			list = append(list, &cp)
		}

		sort.Slice(list, func(i, j int) bool {
			return list[i].ServiceEnvironmentName < list[j].ServiceEnvironmentName
		})

		return list
	}

	list := make([]*ServiceEnvironment, 0, len(names))

	for _, nameOrARN := range names {
		if se, ok := b.lookupServiceEnvironmentByNameOrARN(nameOrARN); ok {
			cp := *se
			cp.Tags = maps.Clone(se.Tags)
			list = append(list, &cp)
		}
	}

	return list
}

// UpdateSchedulingPolicy performs a no-op update on the scheduling policy (verifies existence).
func (b *InMemoryBackend) UpdateSchedulingPolicy(policyARN string) error {
	b.mu.Lock("UpdateSchedulingPolicy")
	defer b.mu.Unlock()

	if _, ok := b.schedulingPolicies[policyARN]; !ok {
		return fmt.Errorf("%w: scheduling policy %s not found", ErrNotFound, policyARN)
	}

	return nil
}

// UpdateServiceEnvironment updates the state of a service environment.
func (b *InMemoryBackend) UpdateServiceEnvironment(nameOrARN, state string) (*ServiceEnvironment, error) {
	b.mu.Lock("UpdateServiceEnvironment")
	defer b.mu.Unlock()

	se, ok := b.lookupServiceEnvironmentByNameOrARN(nameOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: service environment %s not found", ErrNotFound, nameOrARN)
	}

	if state != "" {
		se.State = state
	}

	cp := *se
	cp.Tags = maps.Clone(se.Tags)

	return &cp, nil
}

// SubmitServiceJob creates a new service job in SUBMITTED status.
func (b *InMemoryBackend) SubmitServiceJob(name, serviceEnv string, tags map[string]string) (*ServiceJob, error) {
	b.mu.Lock("SubmitServiceJob")
	defer b.mu.Unlock()

	tagsCopy := maps.Clone(tags)
	now := time.Now().UnixMilli()
	jobID := uuid.NewString()
	jobARN := arn.Build("batch", b.region, b.accountID, "service-job/"+jobID)

	sj := &ServiceJob{
		ServiceJobID:       jobID,
		ServiceJobArn:      jobARN,
		ServiceJobName:     name,
		ServiceEnvironment: serviceEnv,
		Status:             jobStatusSubmitted,
		CreatedAt:          now,
		Tags:               tagsCopy,
	}
	b.serviceJobs[jobID] = sj
	cp := *sj

	return &cp, nil
}

// DescribeServiceJob returns a single service job by ID.
func (b *InMemoryBackend) DescribeServiceJob(serviceJobID string) (*ServiceJob, error) {
	b.mu.RLock("DescribeServiceJob")
	defer b.mu.RUnlock()

	sj, ok := b.serviceJobs[serviceJobID]
	if !ok {
		return nil, fmt.Errorf("%w: service job %s not found", ErrNotFound, serviceJobID)
	}

	cp := *sj
	cp.Tags = maps.Clone(sj.Tags)

	return &cp, nil
}

// ListServiceJobs returns service jobs, optionally filtered by service environment.
func (b *InMemoryBackend) ListServiceJobs(serviceEnv string) ([]*ServiceJob, error) {
	b.mu.RLock("ListServiceJobs")
	defer b.mu.RUnlock()

	list := make([]*ServiceJob, 0, len(b.serviceJobs))

	for _, sj := range b.serviceJobs {
		if serviceEnv != "" && sj.ServiceEnvironment != serviceEnv {
			continue
		}
		cp := *sj
		cp.Tags = maps.Clone(sj.Tags)
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].CreatedAt < list[j].CreatedAt })

	return list, nil
}

// TerminateServiceJob marks a service job as FAILED.
func (b *InMemoryBackend) TerminateServiceJob(serviceJobID, reason string) error {
	b.mu.Lock("TerminateServiceJob")
	defer b.mu.Unlock()

	sj, ok := b.serviceJobs[serviceJobID]
	if !ok {
		return fmt.Errorf("%w: service job %s not found", ErrNotFound, serviceJobID)
	}

	now := time.Now().UnixMilli()
	sj.Status = jobStatusFailed
	sj.StatusReason = reason
	sj.StoppedAt = &now

	return nil
}

// GetJobQueueSnapshot returns a snapshot of the front of a job queue.
func (b *InMemoryBackend) GetJobQueueSnapshot(jobQueue string) (*JobQueueSnapshot, error) {
	b.mu.RLock("GetJobQueueSnapshot")
	defer b.mu.RUnlock()

	jq, ok := b.lookupJQByNameOrARN(jobQueue)
	if !ok {
		return nil, fmt.Errorf("%w: job queue %s not found", ErrNotFound, jobQueue)
	}

	ids := b.jobsByQueue[jq.JobQueueName]
	runnableJobs := make([]*Job, 0, len(ids))

	for _, id := range ids {
		j, ok2 := b.jobs[id]
		if !ok2 {
			continue
		}
		if j.Status == jobStatusRunnable {
			runnableJobs = append(runnableJobs, j)
		}
	}

	sort.Slice(runnableJobs, func(i, j int) bool { return runnableJobs[i].CreatedAt < runnableJobs[j].CreatedAt })

	const maxFrontOfQueue = 100
	if len(runnableJobs) > maxFrontOfQueue {
		runnableJobs = runnableJobs[:maxFrontOfQueue]
	}

	foqJobs := make([]FrontOfQueueJob, 0, len(runnableJobs))
	now := float64(time.Now().UnixMilli()) / msPerSecond

	for _, j := range runnableJobs {
		foqJobs = append(foqJobs, FrontOfQueueJob{
			JobArn:                 j.JobARN,
			EarliestTimeAtPosition: float64(j.CreatedAt) / msPerSecond,
		})
	}

	return &JobQueueSnapshot{
		FrontOfQueue: &FrontOfQueue{
			Jobs:      foqJobs,
			Timestamp: now,
		},
	}, nil
}

// ListJobsByConsumableResource returns all jobs (AWS returns jobs using the given consumable resource).
func (b *InMemoryBackend) ListJobsByConsumableResource(_ string) ([]*Job, error) {
	b.mu.RLock("ListJobsByConsumableResource")
	defer b.mu.RUnlock()

	list := make([]*Job, 0, len(b.jobs))

	for _, j := range b.jobs {
		cp := *j
		cp.Tags = maps.Clone(j.Tags)
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].CreatedAt < list[j].CreatedAt })

	return list, nil
}
