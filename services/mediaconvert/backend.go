package mediaconvert

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	presetCustom = "CUSTOM"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrValidation is returned when request parameters fail validation.
	ErrValidation = awserr.New("BadRequestException", awserr.ErrInvalidParameter)
)

const (
	// statusActive is the active state for queues and presets.
	statusActive = "ACTIVE"
	// statusPaused is the paused state for queues.
	statusPaused = "PAUSED"
	// jobStatusSubmitted is the initial job state.
	jobStatusSubmitted = "SUBMITTED"
	// jobStatusProgressing is the in-progress job state.
	jobStatusProgressing = "PROGRESSING"
	// jobStatusComplete is the successfully finished job state.
	jobStatusComplete = "COMPLETE"
	// jobStatusError is the failed job state.
	jobStatusError = "ERROR"
	// jobStatusCanceled is the canceled job state.
	jobStatusCanceled = "CANCELED"
	// pricingPlanOnDemand is the default pricing plan.
	pricingPlanOnDemand = "ON_DEMAND"
	// jobPhaseProbing is the initial processing phase of a submitted job.
	jobPhaseProbing = "PROBING"
	// jobPhaseTranscoding is the second processing phase after probing.
	jobPhaseTranscoding = "TRANSCODING"
	// jobPhaseUploading is the third processing phase after transcoding.
	jobPhaseUploading = "UPLOADING"
	// priorityMin is the minimum allowed job/template priority.
	priorityMin = -50
	// priorityMax is the maximum allowed job/template priority.
	priorityMax = 50
	// orderAscending is the ascending list order value used by AWS MediaConvert.
	orderAscending = "ASCENDING"
	// deepCloneMaxDepth caps recursion in deepCloneValue to prevent stack overflows.
	deepCloneMaxDepth = 20
	// tokenTTL is how long a ClientRequestToken deduplication window lasts.
	tokenTTL = time.Minute
	// jobEngineVersionUsed is the fixed engine version reported on all jobs.
	jobEngineVersionUsed = "2017-08-29"
)

// epochSeconds converts a [time.Time] to a float64 Unix epoch seconds value,
// which is the format expected by the MediaConvert SDK for timestamp fields.
func epochSeconds(t time.Time) float64 {
	return float64(t.Unix())
}

// deepCloneMap returns a deep copy of a settings map, or nil if input is nil.
func deepCloneMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}

	cp := make(map[string]any, len(m))
	for k, v := range m {
		cp[k] = deepCloneValueAt(v, 0)
	}

	return cp
}

// deepCloneValue recursively clones a value, handling nested maps and slices.
// Kept for API compat; delegates to deepCloneValueAt at depth 0.
func deepCloneValue(v any) any {
	return deepCloneValueAt(v, 0)
}

// deepCloneValueAt clones v with a depth counter. When depth >= deepCloneMaxDepth,
// map and slice values are returned as nil to prevent unbounded recursion.
func deepCloneValueAt(v any, depth int) any {
	if depth >= deepCloneMaxDepth {
		switch v.(type) {
		case map[string]any, []any:
			return nil
		}

		return v
	}

	switch vt := v.(type) {
	case map[string]any:
		cp := make(map[string]any, len(vt))
		for k, val := range vt {
			cp[k] = deepCloneValueAt(val, depth+1)
		}

		return cp
	case []any:
		cp := make([]any, len(vt))
		for i, item := range vt {
			cp[i] = deepCloneValueAt(item, depth+1)
		}

		return cp
	default:
		return v
	}
}

// nonNilTagsCopy returns a copy of the given tags map; never returns nil.
func nonNilTagsCopy(tags map[string]string) map[string]string {
	cp := make(map[string]string, len(tags))
	maps.Copy(cp, tags)

	return cp
}

// ReservationPlan holds reservation plan details for a queue.
type ReservationPlan struct {
	ExpiresAt     float64 `json:"expiresAt,omitempty"`
	PurchasedAt   float64 `json:"purchasedAt,omitempty"`
	ReservedSlots int     `json:"reservedSlots,omitempty"`
	Status        string  `json:"status,omitempty"`
	Commitment    string  `json:"commitment,omitempty"`
	RenewalType   string  `json:"renewalType,omitempty"`
}

// Queue represents a MediaConvert queue.
type Queue struct {
	ReservationPlan  *ReservationPlan  `json:"reservationPlan,omitempty"`
	ServiceOverrides map[string]any    `json:"serviceOverrides,omitempty"`
	Tags             map[string]string `json:"tags,omitempty"`
	Arn              string            `json:"arn"`
	Name             string            `json:"name"`
	Description      string            `json:"description,omitempty"`
	PricingPlan      string            `json:"pricingPlan"`
	Status           string            `json:"status"`
	Type             string            `json:"type"`
	CreatedAt        float64           `json:"createdAt"`
	LastUpdated      float64           `json:"lastUpdated"`
	ProgressingJobsCount int           `json:"progressingJobsCount"`
	SubmittedJobsCount   int           `json:"submittedJobsCount"`
	ConcurrentJobs       int           `json:"concurrentJobs,omitempty"`
}

// JobTemplate represents a MediaConvert job template.
type JobTemplate struct {
	Settings    map[string]any    `json:"settings,omitempty"`
	Tags        map[string]string `json:"tags,omitempty"`
	Arn         string            `json:"arn"`
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	Category    string            `json:"category,omitempty"`
	Queue       string            `json:"queue,omitempty"`
	Type        string            `json:"type"`
	CreatedAt   float64           `json:"createdAt"`
	LastUpdated float64           `json:"lastUpdated"`
	Priority    int               `json:"priority"`
}

// JobTiming holds timing information for a MediaConvert job.
type JobTiming struct {
	SubmitTime float64 `json:"submitTime,omitempty"`
	StartTime  float64 `json:"startTime,omitempty"`
	FinishTime float64 `json:"finishTime,omitempty"`
}

// HopDestination represents a queue hop destination for a job.
type HopDestination struct {
	WaitMinutes int    `json:"waitMinutes,omitempty"`
	Priority    int    `json:"priority,omitempty"`
	Queue       string `json:"queue,omitempty"`
}

// QueueTransition records a queue change event for a job.
type QueueTransition struct {
	Timestamp        float64 `json:"timestamp,omitempty"`
	SourceQueue      string  `json:"sourceQueue,omitempty"`
	DestinationQueue string  `json:"destinationQueue,omitempty"`
}

// OutputDetail contains output-level detail for a completed job.
type OutputDetail struct {
	VideoDetails *VideoDetail `json:"videoDetails,omitempty"`
	DurationInMs int          `json:"durationInMs,omitempty"`
}

// VideoDetail holds video dimension details.
type VideoDetail struct {
	WidthInPx  int `json:"widthInPx,omitempty"`
	HeightInPx int `json:"heightInPx,omitempty"`
}

// OutputGroupDetail contains details for one output group.
type OutputGroupDetail struct {
	OutputDetails []OutputDetail `json:"outputDetails,omitempty"`
}

// JobMessages holds informational and warning messages for a job.
type JobMessages struct {
	Info    []string `json:"info,omitempty"`
	Warning []string `json:"warning,omitempty"`
}

// WarningGroup represents an aggregated warning count.
type WarningGroup struct {
	Count int    `json:"count,omitempty"`
	Code  string `json:"code,omitempty"`
}

// AccelerationSettings holds the requested acceleration mode.
type AccelerationSettings struct {
	Mode string `json:"mode,omitempty"`
}

// ShareDetails holds resource share token details.
type ShareDetails struct {
	ShareToken string  `json:"shareToken,omitempty"`
	SharedAt   float64 `json:"sharedAt,omitempty"`
}

// Job represents a MediaConvert transcoding job.
type Job struct {
	AccelerationSettings  *AccelerationSettings  `json:"accelerationSettings,omitempty"`
	Messages              *JobMessages           `json:"messages,omitempty"`
	LastShareDetails      *ShareDetails          `json:"lastShareDetails,omitempty"`
	Timing                *JobTiming             `json:"timing,omitempty"`
	Settings              map[string]any         `json:"settings,omitempty"`
	Tags                  map[string]string      `json:"tags,omitempty"`
	UserMetadata          map[string]string      `json:"userMetadata,omitempty"`
	OutputGroupDetails    []OutputGroupDetail    `json:"outputGroupDetails,omitempty"`
	QueueTransitions      []QueueTransition      `json:"queueTransitions,omitempty"`
	HopDestinations       []HopDestination       `json:"hopDestinations,omitempty"`
	Warnings              []WarningGroup         `json:"warnings,omitempty"`
	Arn                   string                 `json:"arn"`
	ID                    string                 `json:"id"`
	Queue                 string                 `json:"queue,omitempty"`
	QueueArn              string                 `json:"queueArn,omitempty"`
	Role                  string                 `json:"role"`
	Status                string                 `json:"status"`
	CurrentPhase          string                 `json:"currentPhase,omitempty"`
	JobTemplate           string                 `json:"jobTemplate,omitempty"`
	ErrorMessage          string                 `json:"errorMessage,omitempty"`
	BillingTagsSource     string                 `json:"billingTagsSource,omitempty"`
	AccelerationStatus    string                 `json:"accelerationStatus,omitempty"`
	StatusUpdateInterval  string                 `json:"statusUpdateInterval,omitempty"`
	SimulateReservedQueue string                 `json:"simulateReservedQueue,omitempty"`
	ClientRequestToken    string                 `json:"clientRequestToken,omitempty"`
	JobEngineVersionRequested string             `json:"jobEngineVersionRequested,omitempty"`
	JobEngineVersionUsed  string                 `json:"jobEngineVersionUsed,omitempty"`
	ShareStatus           string                 `json:"shareStatus,omitempty"`
	CreatedAt             float64                `json:"createdAt"`
	ErrorCode             int                    `json:"errorCode,omitempty"`
	JobPercentComplete    int                    `json:"jobPercentComplete"`
	Priority              int                    `json:"priority"`
	RetryCount            int                    `json:"retryCount"`
}

// Preset represents a MediaConvert output preset.
type Preset struct {
	Settings    map[string]any    `json:"settings,omitempty"`
	Tags        map[string]string `json:"tags,omitempty"`
	Arn         string            `json:"arn"`
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	Category    string            `json:"category,omitempty"`
	Type        string            `json:"type"`
	CreatedAt   float64           `json:"createdAt"`
	LastUpdated float64           `json:"lastUpdated"`
}

// Policy represents a MediaConvert account policy.
type Policy struct {
	HTTPInputs  string `json:"httpInputs,omitempty"`
	HTTPSInputs string `json:"httpsInputs,omitempty"`
	S3Inputs    string `json:"s3Inputs,omitempty"`
}

// jobsQuery stores the parameters of a StartJobsQuery call for deferred execution.
type jobsQuery struct {
	order      string
	filterList []map[string]any
	maxResults int
}

// tokenEntry records a ClientRequestToken for deduplication.
type tokenEntry struct {
	jobID     string
	createdAt time.Time
}

// queueJobCounter tracks active job counts for a single queue.
type queueJobCounter struct {
	submitted   int
	progressing int
}

// InMemoryBackend is the in-memory store for MediaConvert resources.
type InMemoryBackend struct {
	queries       map[string]*jobsQuery
	queues        map[string]*Queue
	jobTemplates  map[string]*JobTemplate
	jobs          map[string]*Job
	presets       map[string]*Preset
	tags          map[string]map[string]string
	certificates  map[string]struct{}
	queueCounters map[string]*queueJobCounter
	tokenIndex    map[string]*tokenEntry
	policy        *Policy
	mu            *lockmetrics.RWMutex
	accountID     string
	region        string
}

// NewInMemoryBackend creates a new in-memory MediaConvert backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		queries:       make(map[string]*jobsQuery),
		queues:        make(map[string]*Queue),
		jobTemplates:  make(map[string]*JobTemplate),
		jobs:          make(map[string]*Job),
		presets:       make(map[string]*Preset),
		tags:          make(map[string]map[string]string),
		certificates:  make(map[string]struct{}),
		queueCounters: make(map[string]*queueJobCounter),
		tokenIndex:    make(map[string]*tokenEntry),
		accountID:     accountID,
		region:        region,
		mu:            lockmetrics.New("mediaconvert"),
	}
}

// Region returns the region configured for this backend.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the account ID configured for this backend.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Reset clears all stored resources, resetting the backend to its initial state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.queries = make(map[string]*jobsQuery)
	b.queues = make(map[string]*Queue)
	b.jobTemplates = make(map[string]*JobTemplate)
	b.jobs = make(map[string]*Job)
	b.presets = make(map[string]*Preset)
	b.tags = make(map[string]map[string]string)
	b.certificates = make(map[string]struct{})
	b.queueCounters = make(map[string]*queueJobCounter)
	b.tokenIndex = make(map[string]*tokenEntry)
	b.policy = nil
}

// --- Internal seed helpers (used by tests) ---

// AddQueueInternal inserts a queue directly into the backend, bypassing business logic.
func (b *InMemoryBackend) AddQueueInternal(q *Queue) {
	b.mu.Lock("AddQueueInternal")
	defer b.mu.Unlock()

	b.queues[q.Name] = q
}

// AddJobTemplateInternal inserts a job template directly into the backend.
func (b *InMemoryBackend) AddJobTemplateInternal(jt *JobTemplate) {
	b.mu.Lock("AddJobTemplateInternal")
	defer b.mu.Unlock()

	b.jobTemplates[jt.Name] = jt
}

// AddJobInternal inserts a job directly into the backend.
func (b *InMemoryBackend) AddJobInternal(j *Job) {
	b.mu.Lock("AddJobInternal")
	defer b.mu.Unlock()

	b.jobs[j.ID] = j
}

// AddPresetInternal inserts a preset directly into the backend.
func (b *InMemoryBackend) AddPresetInternal(p *Preset) {
	b.mu.Lock("AddPresetInternal")
	defer b.mu.Unlock()

	b.presets[p.Name] = p
}

// CreateQueue creates a new MediaConvert queue.
func (b *InMemoryBackend) CreateQueue(
	name, description, pricingPlan, status string,
	tags map[string]string,
) (*Queue, error) {
	return b.CreateQueueFull(name, description, pricingPlan, status, tags, 0, nil, nil)
}

// CreateQueueFull creates a queue with all optional fields.
func (b *InMemoryBackend) CreateQueueFull(
	name, description, pricingPlan, status string,
	tags map[string]string,
	concurrentJobs int,
	reservationPlan *ReservationPlan,
	serviceOverrides map[string]any,
) (*Queue, error) {
	b.mu.Lock("CreateQueue")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	if _, ok := b.queues[name]; ok {
		return nil, fmt.Errorf("%w: queue %s already exists", ErrAlreadyExists, name)
	}

	if pricingPlan == "" {
		pricingPlan = pricingPlanOnDemand
	}

	if status == "" {
		status = statusActive
	}

	if status != statusActive && status != statusPaused {
		return nil, fmt.Errorf("%w: status must be ACTIVE or PAUSED", ErrValidation)
	}

	now := epochSeconds(time.Now())
	q := &Queue{
		Arn:              arn.Build("mediaconvert", b.region, b.accountID, "queues/"+name),
		Name:             name,
		Description:      description,
		PricingPlan:      pricingPlan,
		Status:           status,
		Type:             presetCustom,
		Tags:             nonNilTagsCopy(tags),
		CreatedAt:        now,
		LastUpdated:      now,
		ConcurrentJobs:   concurrentJobs,
		ReservationPlan:  cloneReservationPlan(reservationPlan),
		ServiceOverrides: deepCloneMap(serviceOverrides),
	}
	b.queues[name] = q
	b.initQueueCounterLocked(q.Arn)

	if len(tags) > 0 {
		b.storeTagsLocked(q.Arn, tags)
	}

	cp := cloneQueue(q)

	return cp, nil
}

// GetQueue returns a queue by name.
func (b *InMemoryBackend) GetQueue(name string) (*Queue, error) {
	b.mu.RLock("GetQueue")
	defer b.mu.RUnlock()

	q, ok := b.queues[name]
	if !ok {
		return nil, fmt.Errorf("%w: queue %s not found", ErrNotFound, name)
	}

	cp := cloneQueue(q)
	cp.ProgressingJobsCount, cp.SubmittedJobsCount = b.getQueueCounterLocked(q.Arn)

	return cp, nil
}

// ListQueues returns all queues sorted by name.
func (b *InMemoryBackend) ListQueues() []*Queue {
	b.mu.RLock("ListQueues")
	defer b.mu.RUnlock()

	list := make([]*Queue, 0, len(b.queues))
	for _, q := range b.queues {
		cp := cloneQueue(q)
		cp.ProgressingJobsCount, cp.SubmittedJobsCount = b.getQueueCounterLocked(q.Arn)
		list = append(list, cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// countJobsForQueueLocked counts PROGRESSING and SUBMITTED jobs for a queue ARN.
// Caller must hold at least a read lock. Kept as internal consistency-check method.
func (b *InMemoryBackend) countJobsForQueueLocked(queueArn string) (int, int) {
	var progressing, submitted int

	for _, j := range b.jobs {
		if j.QueueArn != queueArn {
			continue
		}

		switch j.Status {
		case jobStatusProgressing:
			progressing++
		case jobStatusSubmitted:
			submitted++
		}
	}

	return progressing, submitted
}

// initQueueCounterLocked creates a counter entry for queueArn if it does not exist.
// Caller must hold the write lock.
func (b *InMemoryBackend) initQueueCounterLocked(queueArn string) {
	if _, ok := b.queueCounters[queueArn]; !ok {
		b.queueCounters[queueArn] = &queueJobCounter{}
	}
}

// getQueueCounterLocked returns (progressing, submitted) counts for queueArn.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) getQueueCounterLocked(queueArn string) (int, int) {
	c, ok := b.queueCounters[queueArn]
	if !ok {
		return 0, 0
	}

	return c.progressing, c.submitted
}

// adjustQueueCounterLocked adds delta to the counter field corresponding to status.
// Caller must hold the write lock.
func (b *InMemoryBackend) adjustQueueCounterLocked(queueArn, status string, delta int) {
	if queueArn == "" {
		return
	}

	b.initQueueCounterLocked(queueArn)
	c := b.queueCounters[queueArn]

	switch status {
	case jobStatusSubmitted:
		c.submitted += delta
	case jobStatusProgressing:
		c.progressing += delta
	}
}

// UpdateQueue updates a queue's description and status.
func (b *InMemoryBackend) UpdateQueue(name, description, status string) (*Queue, error) {
	b.mu.Lock("UpdateQueue")
	defer b.mu.Unlock()

	q, ok := b.queues[name]
	if !ok {
		return nil, fmt.Errorf("%w: queue %s not found", ErrNotFound, name)
	}

	if status != "" && status != statusActive && status != statusPaused {
		return nil, fmt.Errorf("%w: status must be ACTIVE or PAUSED", ErrValidation)
	}

	if description != "" {
		q.Description = description
	}

	if status != "" {
		q.Status = status
	}

	q.LastUpdated = epochSeconds(time.Now())

	return cloneQueue(q), nil
}

// DeleteQueue removes a queue by name.
func (b *InMemoryBackend) DeleteQueue(name string) error {
	b.mu.Lock("DeleteQueue")
	defer b.mu.Unlock()

	q, ok := b.queues[name]
	if !ok {
		return fmt.Errorf("%w: queue %s not found", ErrNotFound, name)
	}
	delete(b.tags, q.Arn)
	delete(b.queueCounters, q.Arn)
	delete(b.queues, name)

	return nil
}

// resolveQueueLocked looks up a queue by name or ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) resolveQueueLocked(queue string) (*Queue, error) {
	if strings.HasPrefix(queue, "arn:") {
		for _, cq := range b.queues {
			if cq.Arn == queue {
				return cq, nil
			}
		}
	} else if q, ok := b.queues[queue]; ok {
		return q, nil
	}

	return nil, fmt.Errorf("%w: queue %s not found", ErrNotFound, queue)
}

// CreateJobTemplate creates a new MediaConvert job template.
func (b *InMemoryBackend) CreateJobTemplate(
	name, description, category, queue string,
	priority int,
	settings map[string]any,
	tags map[string]string,
) (*JobTemplate, error) {
	b.mu.Lock("CreateJobTemplate")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	if _, ok := b.jobTemplates[name]; ok {
		return nil, fmt.Errorf("%w: job template %s already exists", ErrAlreadyExists, name)
	}

	if priority < priorityMin || priority > priorityMax {
		return nil, fmt.Errorf("%w: priority must be between %d and %d", ErrValidation, priorityMin, priorityMax)
	}

	now := epochSeconds(time.Now())
	jt := &JobTemplate{
		Arn:         arn.Build("mediaconvert", b.region, b.accountID, "jobTemplates/"+name),
		Name:        name,
		Description: description,
		Category:    category,
		Queue:       queue,
		Priority:    priority,
		Settings:    deepCloneMap(settings),
		Tags:        nonNilTagsCopy(tags),
		Type:        presetCustom,
		CreatedAt:   now,
		LastUpdated: now,
	}
	b.jobTemplates[name] = jt

	if len(tags) > 0 {
		b.storeTagsLocked(jt.Arn, tags)
	}

	return cloneJobTemplate(jt), nil
}

// GetJobTemplate returns a job template by name.
func (b *InMemoryBackend) GetJobTemplate(name string) (*JobTemplate, error) {
	b.mu.RLock("GetJobTemplate")
	defer b.mu.RUnlock()

	jt, ok := b.jobTemplates[name]
	if !ok {
		return nil, fmt.Errorf("%w: job template %s not found", ErrNotFound, name)
	}

	return cloneJobTemplate(jt), nil
}

// ListJobTemplates returns all job templates sorted by name.
func (b *InMemoryBackend) ListJobTemplates() []*JobTemplate {
	b.mu.RLock("ListJobTemplates")
	defer b.mu.RUnlock()

	list := make([]*JobTemplate, 0, len(b.jobTemplates))
	for _, jt := range b.jobTemplates {
		list = append(list, cloneJobTemplate(jt))
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// UpdateJobTemplate updates a job template's description, category, queue, priority, and settings.
func (b *InMemoryBackend) UpdateJobTemplate(
	name, description, category, queue string,
	priority *int,
	settings map[string]any,
) (*JobTemplate, error) {
	b.mu.Lock("UpdateJobTemplate")
	defer b.mu.Unlock()

	jt, ok := b.jobTemplates[name]
	if !ok {
		return nil, fmt.Errorf("%w: job template %s not found", ErrNotFound, name)
	}

	if description != "" {
		jt.Description = description
	}

	if category != "" {
		jt.Category = category
	}

	if queue != "" {
		jt.Queue = queue
	}

	if priority != nil {
		if *priority < priorityMin || *priority > priorityMax {
			return nil, fmt.Errorf("%w: priority must be between %d and %d", ErrValidation, priorityMin, priorityMax)
		}

		jt.Priority = *priority
	}

	if settings != nil {
		jt.Settings = deepCloneMap(settings)
	}

	jt.LastUpdated = epochSeconds(time.Now())

	return cloneJobTemplate(jt), nil
}

// DeleteJobTemplate removes a job template by name.
func (b *InMemoryBackend) DeleteJobTemplate(name string) error {
	b.mu.Lock("DeleteJobTemplate")
	defer b.mu.Unlock()

	jt, ok := b.jobTemplates[name]
	if !ok {
		return fmt.Errorf("%w: job template %s not found", ErrNotFound, name)
	}
	delete(b.tags, jt.Arn)
	delete(b.jobTemplates, name)

	return nil
}

// CreateJob creates a new MediaConvert job.
func (b *InMemoryBackend) CreateJob(
	role, queue, jobTemplate string,
	settings map[string]any,
	tags map[string]string,
	userMetadata map[string]string,
	billingTagsSource string,
) (*Job, error) {
	return b.CreateJobFull(
		role, queue, jobTemplate,
		settings, tags, userMetadata,
		billingTagsSource, "", "", "", 0, nil,
	)
}

// CreateJobFull creates a new MediaConvert job with all optional fields.
func (b *InMemoryBackend) CreateJobFull(
	role, queue, jobTemplate string,
	settings map[string]any,
	tags map[string]string,
	userMetadata map[string]string,
	billingTagsSource, clientRequestToken, accelerationMode, jobEngineVersionReq string,
	priority int,
	hopDestinations []HopDestination,
) (*Job, error) {
	b.mu.Lock("CreateJobFull")
	defer b.mu.Unlock()

	if role == "" {
		return nil, fmt.Errorf("%w: role is required", ErrValidation)
	}

	// Dedup by ClientRequestToken.
	if clientRequestToken != "" {
		if entry, ok := b.tokenIndex[clientRequestToken]; ok {
			if time.Since(entry.createdAt) < tokenTTL {
				if existing, ok := b.jobs[entry.jobID]; ok {
					return cloneJob(existing), nil
				}
			}
		}
	}

	// Resolve queue ARN from queue name or ARN.
	queueArn := ""

	if queue != "" {
		resolved, err := b.resolveQueueLocked(queue)
		if err != nil {
			return nil, err
		}

		queueArn = resolved.Arn
	}

	accelStatus := "NOT_APPLICABLE"
	if accelerationMode == "ENABLED" {
		accelStatus = "PREFERRED"
	}

	var accelSettings *AccelerationSettings
	if accelerationMode != "" {
		accelSettings = &AccelerationSettings{Mode: accelerationMode}
	}

	now := epochSeconds(time.Now())
	id := generateJobID()

	var hopDests []HopDestination
	if len(hopDestinations) > 0 {
		hopDests = make([]HopDestination, len(hopDestinations))
		copy(hopDests, hopDestinations)
	}

	j := &Job{
		Arn:                       arn.Build("mediaconvert", b.region, b.accountID, "jobs/"+id),
		ID:                        id,
		Role:                      role,
		Queue:                     queue,
		QueueArn:                  queueArn,
		JobTemplate:               jobTemplate,
		Status:                    jobStatusSubmitted,
		CurrentPhase:              jobPhaseProbing,
		Settings:                  deepCloneMap(settings),
		Tags:                      nonNilTagsCopy(tags),
		UserMetadata:              nonNilTagsCopy(userMetadata),
		BillingTagsSource:         billingTagsSource,
		AccelerationStatus:        accelStatus,
		AccelerationSettings:      accelSettings,
		SimulateReservedQueue:     "DISABLED",
		StatusUpdateInterval:      "SECONDS_60",
		Timing:                    &JobTiming{SubmitTime: now},
		CreatedAt:                 now,
		Priority:                  priority,
		HopDestinations:           hopDests,
		ClientRequestToken:        clientRequestToken,
		JobEngineVersionRequested: jobEngineVersionReq,
		JobEngineVersionUsed:      jobEngineVersionUsed,
		Messages:                  &JobMessages{},
		Warnings:                  []WarningGroup{},
		ShareStatus:               "NOT_SHARED",
	}
	b.jobs[id] = j

	// Update queue counter for the new SUBMITTED job.
	b.adjustQueueCounterLocked(queueArn, jobStatusSubmitted, +1)

	// Record token for dedup.
	if clientRequestToken != "" {
		b.tokenIndex[clientRequestToken] = &tokenEntry{
			jobID:     id,
			createdAt: time.Now(),
		}
	}

	if len(tags) > 0 {
		b.storeTagsLocked(j.Arn, tags)
	}

	return cloneJob(j), nil
}

// GetJob returns a job by ID.
func (b *InMemoryBackend) GetJob(id string) (*Job, error) {
	b.mu.RLock("GetJob")
	defer b.mu.RUnlock()

	j, ok := b.jobs[id]
	if !ok {
		return nil, fmt.Errorf("%w: job %s not found", ErrNotFound, id)
	}

	return cloneJob(j), nil
}

// ListJobs returns all jobs sorted by creation time (newest first).
func (b *InMemoryBackend) ListJobs() []*Job {
	b.mu.RLock("ListJobs")
	defer b.mu.RUnlock()

	list := make([]*Job, 0, len(b.jobs))
	for _, j := range b.jobs {
		list = append(list, cloneJob(j))
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].CreatedAt > list[j].CreatedAt
	})

	return list
}

// CancelJob cancels a job by ID.
// Only SUBMITTED or PROGRESSING jobs can be canceled.
func (b *InMemoryBackend) CancelJob(id string) error {
	b.mu.Lock("CancelJob")
	defer b.mu.Unlock()

	j, ok := b.jobs[id]
	if !ok {
		return fmt.Errorf("%w: job %s not found", ErrNotFound, id)
	}

	if j.Status != jobStatusSubmitted && j.Status != jobStatusProgressing {
		return fmt.Errorf("%w: job %s cannot be canceled in status %s", ErrValidation, id, j.Status)
	}

	prevStatus := j.Status
	j.Status = jobStatusCanceled

	if j.Timing == nil {
		j.Timing = &JobTiming{}
	}

	j.Timing.FinishTime = epochSeconds(time.Now())

	// Update queue counter: remove from the previous status bucket.
	b.adjustQueueCounterLocked(j.QueueArn, prevStatus, -1)

	return nil
}

// UpdateJob updates a job's priority, queue, and hop destinations.
func (b *InMemoryBackend) UpdateJob(id, queue string, priority *int, hopDestinations []HopDestination) (*Job, error) {
	b.mu.Lock("UpdateJob")
	defer b.mu.Unlock()

	j, ok := b.jobs[id]
	if !ok {
		return nil, fmt.Errorf("%w: job %s not found", ErrNotFound, id)
	}

	if priority != nil {
		if *priority < priorityMin || *priority > priorityMax {
			return nil, fmt.Errorf("%w: priority must be between %d and %d", ErrValidation, priorityMin, priorityMax)
		}

		j.Priority = *priority
	}

	if queue != "" && queue != j.Queue {
		resolved, err := b.resolveQueueLocked(queue)
		if err != nil {
			return nil, err
		}

		// Record the queue transition.
		transition := QueueTransition{
			Timestamp:        epochSeconds(time.Now()),
			SourceQueue:      j.Queue,
			DestinationQueue: queue,
		}
		j.QueueTransitions = append(j.QueueTransitions, transition)

		// Update per-queue counters: move job from old queue to new queue.
		b.adjustQueueCounterLocked(j.QueueArn, j.Status, -1)

		j.Queue = queue
		j.QueueArn = resolved.Arn

		b.adjustQueueCounterLocked(j.QueueArn, j.Status, +1)
	}

	if hopDestinations != nil {
		dests := make([]HopDestination, len(hopDestinations))
		copy(dests, hopDestinations)
		j.HopDestinations = dests
	}

	return cloneJob(j), nil
}

// SweepExpiredTokens removes token entries that have exceeded the TTL.
// Called by the janitor; safe to call externally for testing.
func (b *InMemoryBackend) SweepExpiredTokens() {
	b.mu.Lock("SweepExpiredTokens")
	defer b.mu.Unlock()

	for token, entry := range b.tokenIndex {
		if time.Since(entry.createdAt) >= tokenTTL {
			delete(b.tokenIndex, token)
		}
	}
}

// AdvanceJobPhase advances job phase/status for the janitor.
// Returns true if any job was advanced.
func (b *InMemoryBackend) AdvanceJobPhase() bool {
	b.mu.Lock("AdvanceJobPhase")
	defer b.mu.Unlock()

	advanced := false
	now := epochSeconds(time.Now())

	for _, j := range b.jobs {
		if b.advanceOneJobLocked(j, now) {
			advanced = true
		}
	}

	return advanced
}

// advanceOneJobLocked advances the state machine for a single job.
// Caller must hold the write lock. Returns true if the job changed.
func (b *InMemoryBackend) advanceOneJobLocked(j *Job, now float64) bool {
	switch j.Status {
	case jobStatusSubmitted:
		return b.advanceSubmittedLocked(j, now)
	case jobStatusProgressing:
		return b.advanceProgressingLocked(j, now)
	}

	return false
}

// advanceSubmittedLocked transitions a SUBMITTED job to PROGRESSING/PROBING.
func (b *InMemoryBackend) advanceSubmittedLocked(j *Job, now float64) bool {
	// Decrement SUBMITTED counter, increment PROGRESSING counter.
	b.adjustQueueCounterLocked(j.QueueArn, jobStatusSubmitted, -1)
	b.adjustQueueCounterLocked(j.QueueArn, jobStatusProgressing, +1)

	j.Status = jobStatusProgressing
	j.CurrentPhase = jobPhaseProbing

	if j.Timing == nil {
		j.Timing = &JobTiming{}
	}

	j.Timing.StartTime = now

	return true
}

// advanceProgressingLocked advances phase within PROGRESSING, eventually to COMPLETE.
func (b *InMemoryBackend) advanceProgressingLocked(j *Job, now float64) bool {
	switch j.CurrentPhase {
	case jobPhaseProbing:
		j.CurrentPhase = jobPhaseTranscoding

		return true
	case jobPhaseTranscoding:
		j.CurrentPhase = jobPhaseUploading

		return true
	case jobPhaseUploading:
		return b.completeJobLocked(j, now)
	}

	return false
}

// completeJobLocked transitions a job to COMPLETE and fills output details.
func (b *InMemoryBackend) completeJobLocked(j *Job, now float64) bool {
	b.adjustQueueCounterLocked(j.QueueArn, jobStatusProgressing, -1)

	j.Status = jobStatusComplete
	j.CurrentPhase = ""
	j.JobPercentComplete = 100

	if j.Timing == nil {
		j.Timing = &JobTiming{}
	}

	j.Timing.FinishTime = now

	j.OutputGroupDetails = []OutputGroupDetail{
		{
			OutputDetails: []OutputDetail{
				{
					DurationInMs: 60000,
					VideoDetails: &VideoDetail{WidthInPx: 1920, HeightInPx: 1080},
				},
			},
		},
	}

	return true
}

// generateJobID generates a MediaConvert-style job ID.
func generateJobID() string {
	ts := time.Now().UnixMilli()
	suffix := uuid.NewString()[:13]

	return fmt.Sprintf("%d-%s", ts, suffix)
}

// GetTags returns a copy of tags for the given resource ARN.
func (b *InMemoryBackend) GetTags(resourceARN string) map[string]string {
	b.mu.RLock("GetTags")
	defer b.mu.RUnlock()

	t := b.tags[resourceARN]
	cp := make(map[string]string, len(t))

	maps.Copy(cp, t)

	return cp
}

// TagResource adds or updates tags for the given resource ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	b.storeTagsLocked(resourceARN, tags)
}

// UntagResource removes the specified tag keys from the resource ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	for _, k := range tagKeys {
		delete(b.tags[resourceARN], k)
	}

	if len(b.tags[resourceARN]) == 0 {
		delete(b.tags, resourceARN)
	}
}

// storeTagsLocked merges tags into the ARN tag map.
// Caller must hold the write lock.
func (b *InMemoryBackend) storeTagsLocked(resourceARN string, tags map[string]string) {
	if len(tags) == 0 {
		return
	}

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string, len(tags))
	}

	maps.Copy(b.tags[resourceARN], tags)
}

// CreatePreset creates a new MediaConvert output preset.
func (b *InMemoryBackend) CreatePreset(
	name, description, category string,
	settings map[string]any,
	tags map[string]string,
) (*Preset, error) {
	b.mu.Lock("CreatePreset")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	if _, ok := b.presets[name]; ok {
		return nil, fmt.Errorf("%w: preset %s already exists", ErrAlreadyExists, name)
	}

	now := epochSeconds(time.Now())
	p := &Preset{
		Arn:         arn.Build("mediaconvert", b.region, b.accountID, "presets/"+name),
		Name:        name,
		Description: description,
		Category:    category,
		Settings:    deepCloneMap(settings),
		Tags:        nonNilTagsCopy(tags),
		Type:        presetCustom,
		CreatedAt:   now,
		LastUpdated: now,
	}
	b.presets[name] = p

	if len(tags) > 0 {
		b.storeTagsLocked(p.Arn, tags)
	}

	return clonePreset(p), nil
}

// GetPreset returns a preset by name.
func (b *InMemoryBackend) GetPreset(name string) (*Preset, error) {
	b.mu.RLock("GetPreset")
	defer b.mu.RUnlock()

	p, ok := b.presets[name]
	if !ok {
		return nil, fmt.Errorf("%w: preset %s not found", ErrNotFound, name)
	}

	return clonePreset(p), nil
}

// ListPresets returns all presets sorted by name.
func (b *InMemoryBackend) ListPresets() []*Preset {
	b.mu.RLock("ListPresets")
	defer b.mu.RUnlock()

	list := make([]*Preset, 0, len(b.presets))
	for _, p := range b.presets {
		list = append(list, clonePreset(p))
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// UpdatePreset updates a preset's description, category, and settings.
func (b *InMemoryBackend) UpdatePreset(name, description, category string, settings map[string]any) (*Preset, error) {
	b.mu.Lock("UpdatePreset")
	defer b.mu.Unlock()

	p, ok := b.presets[name]
	if !ok {
		return nil, fmt.Errorf("%w: preset %s not found", ErrNotFound, name)
	}

	if description != "" {
		p.Description = description
	}

	if category != "" {
		p.Category = category
	}

	if settings != nil {
		p.Settings = deepCloneMap(settings)
	}

	p.LastUpdated = epochSeconds(time.Now())

	return clonePreset(p), nil
}

// DeletePreset removes a preset by name.
func (b *InMemoryBackend) DeletePreset(name string) error {
	b.mu.Lock("DeletePreset")
	defer b.mu.Unlock()

	p, ok := b.presets[name]
	if !ok {
		return fmt.Errorf("%w: preset %s not found", ErrNotFound, name)
	}
	delete(b.tags, p.Arn)
	delete(b.presets, name)

	return nil
}

// GetPolicy returns the current account policy, or nil if none has been set.
func (b *InMemoryBackend) GetPolicy() (*Policy, error) {
	b.mu.RLock("GetPolicy")
	defer b.mu.RUnlock()

	if b.policy == nil {
		return nil, fmt.Errorf("%w: no policy configured", ErrNotFound)
	}
	cp := *b.policy

	return &cp, nil
}

// PutPolicy sets the account policy.
func (b *InMemoryBackend) PutPolicy(httpInputs, httpsInputs, s3Inputs string) *Policy {
	b.mu.Lock("PutPolicy")
	defer b.mu.Unlock()

	b.policy = &Policy{
		HTTPInputs:  httpInputs,
		HTTPSInputs: httpsInputs,
		S3Inputs:    s3Inputs,
	}
	cp := *b.policy

	return &cp
}

// DeletePolicy removes the account policy.
func (b *InMemoryBackend) DeletePolicy() error {
	b.mu.Lock("DeletePolicy")
	defer b.mu.Unlock()

	if b.policy == nil {
		return fmt.Errorf("%w: no policy configured", ErrNotFound)
	}
	b.policy = nil

	return nil
}

// AssociateCertificate registers an ACM certificate ARN with this backend.
func (b *InMemoryBackend) AssociateCertificate(certARN string) error {
	b.mu.Lock("AssociateCertificate")
	defer b.mu.Unlock()

	if certARN == "" {
		return fmt.Errorf("%w: arn is required", ErrValidation)
	}

	if !strings.HasPrefix(certARN, "arn:") {
		return fmt.Errorf("%w: arn must start with 'arn:'", ErrValidation)
	}

	if _, ok := b.certificates[certARN]; ok {
		return fmt.Errorf("%w: certificate %s already associated", ErrAlreadyExists, certARN)
	}
	b.certificates[certARN] = struct{}{}

	return nil
}

// DisassociateCertificate removes an ACM certificate ARN association.
func (b *InMemoryBackend) DisassociateCertificate(certARN string) error {
	b.mu.Lock("DisassociateCertificate")
	defer b.mu.Unlock()

	if certARN == "" {
		return fmt.Errorf("%w: arn is required", ErrValidation)
	}

	if !strings.HasPrefix(certARN, "arn:") {
		return fmt.Errorf("%w: arn must start with 'arn:'", ErrValidation)
	}

	if _, ok := b.certificates[certARN]; !ok {
		return fmt.Errorf("%w: certificate %s not found", ErrNotFound, certARN)
	}
	delete(b.certificates, certARN)

	return nil
}

// StartJobsQuery stores a jobs query and returns a query ID for deferred retrieval.
// Filters use key-value pairs where key is a field name (e.g. "queue", "status")
// and values are the allowed values for that field.
func (b *InMemoryBackend) StartJobsQuery(filterList []map[string]any, maxResults int, order string) (string, error) {
	id := uuid.NewString()

	b.mu.Lock("StartJobsQuery")
	defer b.mu.Unlock()

	b.queries[id] = &jobsQuery{
		filterList: filterList,
		maxResults: maxResults,
		order:      order,
	}

	return id, nil
}

// GetJobsQueryResults returns jobs matching the stored query for the given ID.
// If the queryID is unknown (not from a prior StartJobsQuery call), returns empty.
func (b *InMemoryBackend) GetJobsQueryResults(queryID string) []*Job {
	b.mu.RLock("GetJobsQueryResults")
	defer b.mu.RUnlock()

	q, ok := b.queries[queryID]
	if !ok {
		return []*Job{}
	}

	list := make([]*Job, 0, len(b.jobs))

	for _, j := range b.jobs {
		if !jobMatchesFilters(j, q.filterList) {
			continue
		}

		list = append(list, cloneJob(j))
	}

	if q.order == orderAscending {
		sort.Slice(list, func(i, j int) bool { return list[i].CreatedAt < list[j].CreatedAt })
	} else {
		sort.Slice(list, func(i, j int) bool { return list[i].CreatedAt > list[j].CreatedAt })
	}

	if q.maxResults > 0 && len(list) > q.maxResults {
		list = list[:q.maxResults]
	}

	return list
}

// jobMatchesFilters returns true if the job satisfies all provided query filters.
// Each filter map must have a "key" and a "values" field; any value match passes.
func jobMatchesFilters(j *Job, filters []map[string]any) bool {
	for _, f := range filters {
		key, _ := f["key"].(string)
		vals, _ := f["values"].([]any)

		var jobVal string

		switch key {
		case "queue", "Queue":
			jobVal = j.Queue
		case "status", "Status":
			jobVal = j.Status
		default:
			continue
		}

		matched := false

		for _, v := range vals {
			if vs, ok := v.(string); ok && vs == jobVal {
				matched = true

				break
			}
		}

		if !matched {
			return false
		}
	}

	return true
}

// CreateResourceShare records a resource-share request for the given job ID.
// Sets ShareStatus = "SHARED" on the job and populates LastShareDetails.
func (b *InMemoryBackend) CreateResourceShare(jobID string) (string, error) {
	b.mu.Lock("CreateResourceShare")
	defer b.mu.Unlock()

	if jobID == "" {
		return "", fmt.Errorf("%w: jobId is required", ErrValidation)
	}

	j, ok := b.jobs[jobID]
	if !ok {
		return "", fmt.Errorf("%w: job %s not found", ErrNotFound, jobID)
	}

	token := uuid.NewString()
	j.ShareStatus = "SHARED"
	j.LastShareDetails = &ShareDetails{
		ShareToken: token,
		SharedAt:   epochSeconds(time.Now()),
	}

	return jobID, nil
}

// --- Deep clone helpers ---

func cloneReservationPlan(rp *ReservationPlan) *ReservationPlan {
	if rp == nil {
		return nil
	}

	cp := *rp

	return &cp
}

func cloneQueue(q *Queue) *Queue {
	cp := *q
	cp.Tags = nonNilTagsCopy(q.Tags)

	if q.ReservationPlan != nil {
		rp := *q.ReservationPlan
		cp.ReservationPlan = &rp
	}

	if q.ServiceOverrides != nil {
		cp.ServiceOverrides = deepCloneMap(q.ServiceOverrides)
	}

	return &cp
}

func cloneJobTemplate(jt *JobTemplate) *JobTemplate {
	cp := *jt
	cp.Settings = deepCloneMap(jt.Settings)
	cp.Tags = nonNilTagsCopy(jt.Tags)

	return &cp
}

func cloneJob(j *Job) *Job {
	cp := *j
	cp.Settings = deepCloneMap(j.Settings)
	cp.Tags = nonNilTagsCopy(j.Tags)
	cp.UserMetadata = nonNilTagsCopy(j.UserMetadata)

	if j.Timing != nil {
		t := *j.Timing
		cp.Timing = &t
	}

	if j.AccelerationSettings != nil {
		as := *j.AccelerationSettings
		cp.AccelerationSettings = &as
	}

	if j.Messages != nil {
		m := &JobMessages{}
		if len(j.Messages.Info) > 0 {
			m.Info = append([]string(nil), j.Messages.Info...)
		}

		if len(j.Messages.Warning) > 0 {
			m.Warning = append([]string(nil), j.Messages.Warning...)
		}

		cp.Messages = m
	}

	if len(j.Warnings) > 0 {
		cp.Warnings = append([]WarningGroup(nil), j.Warnings...)
	}

	if len(j.HopDestinations) > 0 {
		cp.HopDestinations = append([]HopDestination(nil), j.HopDestinations...)
	}

	if len(j.QueueTransitions) > 0 {
		cp.QueueTransitions = append([]QueueTransition(nil), j.QueueTransitions...)
	}

	if len(j.OutputGroupDetails) > 0 {
		cp.OutputGroupDetails = cloneOutputGroupDetails(j.OutputGroupDetails)
	}

	if j.LastShareDetails != nil {
		sd := *j.LastShareDetails
		cp.LastShareDetails = &sd
	}

	return &cp
}

func cloneOutputGroupDetails(details []OutputGroupDetail) []OutputGroupDetail {
	cp := make([]OutputGroupDetail, len(details))
	for i, d := range details {
		out := OutputGroupDetail{}

		if len(d.OutputDetails) > 0 {
			out.OutputDetails = make([]OutputDetail, len(d.OutputDetails))
			for j, od := range d.OutputDetails {
				cloned := OutputDetail{DurationInMs: od.DurationInMs}

				if od.VideoDetails != nil {
					vd := *od.VideoDetails
					cloned.VideoDetails = &vd
				}

				out.OutputDetails[j] = cloned
			}
		}

		cp[i] = out
	}

	return cp
}

func clonePreset(p *Preset) *Preset {
	cp := *p
	cp.Settings = deepCloneMap(p.Settings)
	cp.Tags = nonNilTagsCopy(p.Tags)

	return &cp
}
