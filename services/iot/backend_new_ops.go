package iot

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// ---------------------------------------------------------------------------
// Job
// ---------------------------------------------------------------------------

// JobStatus represents the status of a job.
type JobStatus string

const (
	JobStatusInProgress JobStatus = "IN_PROGRESS"
	JobStatusCompleted  JobStatus = "COMPLETED"
	JobStatusFailed     JobStatus = "FAILED"
	JobStatusCanceled   JobStatus = "CANCELED"
	JobStatusDeletion   JobStatus = "DELETION_IN_PROGRESS"
)

// JobExecutionStatus represents the status of a job execution on a thing.
type JobExecutionStatus string

const (
	JobExecQueued     JobExecutionStatus = "QUEUED"
	JobExecInProgress JobExecutionStatus = "IN_PROGRESS"
	JobExecSucceeded  JobExecutionStatus = "SUCCEEDED"
	JobExecFailed     JobExecutionStatus = "FAILED"
	JobExecCanceled   JobExecutionStatus = "CANCELED"
	JobExecRejected   JobExecutionStatus = "REJECTED"
	JobExecRemoved    JobExecutionStatus = "REMOVED"
)

// AbortConfig holds abort criteria for a job.
type AbortConfig struct {
	CriteriaList []AbortCriteria `json:"criteriaList,omitempty"`
}

// AbortCriteria is a single abort criterion.
type AbortCriteria struct {
	Action                    string  `json:"action,omitempty"`
	FailureType               string  `json:"failureType,omitempty"`
	MinNumberOfExecutedThings int     `json:"minNumberOfExecutedThings,omitempty"`
	ThresholdPercentage       float64 `json:"thresholdPercentage,omitempty"`
}

// JobExecutionsRolloutConfig holds rollout config for a job.
type JobExecutionsRolloutConfig struct {
	MaximumPerMinute int `json:"maximumPerMinute,omitempty"`
}

// TimeoutConfig holds timeout settings for a job.
type TimeoutConfig struct {
	InProgressTimeoutInMinutes int64 `json:"inProgressTimeoutInMinutes,omitempty"`
}

// Job represents an IoT job.
type Job struct {
	Tags                       map[string]string           `json:"tags,omitempty"`
	DocumentParameters         map[string]string           `json:"documentParameters,omitempty"`
	AbortConfig                *AbortConfig                `json:"abortConfig,omitempty"`
	JobExecutionsRolloutConfig *JobExecutionsRolloutConfig `json:"jobExecutionsRolloutConfig,omitempty"`
	TimeoutConfig              *TimeoutConfig              `json:"timeoutConfig,omitempty"`
	JobID                      string                      `json:"jobId"`
	JobARN                     string                      `json:"jobArn"`
	Description                string                      `json:"description,omitempty"`
	Document                   string                      `json:"document,omitempty"`
	DocumentSource             string                      `json:"documentSource,omitempty"`
	JobTemplateARN             string                      `json:"jobTemplateArn,omitempty"`
	Status                     JobStatus                   `json:"status"`
	TargetSelection            string                      `json:"targetSelection,omitempty"`
	Targets                    []string                    `json:"targets,omitempty"`
	CreatedAt                  float64                     `json:"createdAt,omitempty"`
	LastUpdatedAt              float64                     `json:"lastUpdatedAt,omitempty"`
	CompletedAt                float64                     `json:"completedAt,omitempty"`
}

// JobExecution represents a single job execution on a thing.
type JobExecution struct {
	JobID           string             `json:"jobId"`
	ThingName       string             `json:"thingName"`
	Status          JobExecutionStatus `json:"status"`
	ExecutionNumber int64              `json:"executionNumber,omitempty"`
	QueuedAt        float64            `json:"queuedAt,omitempty"`
	StartedAt       float64            `json:"startedAt,omitempty"`
	LastUpdatedAt   float64            `json:"lastUpdatedAt,omitempty"`
}

func cloneJob(j *Job) *Job {
	cp := *j
	cp.Targets = append([]string(nil), j.Targets...)
	if j.Tags != nil {
		cp.Tags = make(map[string]string, len(j.Tags))
		for k, v := range j.Tags {
			cp.Tags[k] = v
		}
	}
	return &cp
}

func (b *InMemoryBackend) jobARN(jobID string) string {
	return fmt.Sprintf("arn:aws:iot:%s:%s:job/%s", b.region, b.accountID, jobID)
}

// CreateJobInput holds input for CreateJob.
type CreateJobInput struct {
	Tags                       map[string]string           `json:"tags,omitempty"`
	DocumentParameters         map[string]string           `json:"documentParameters,omitempty"`
	AbortConfig                *AbortConfig                `json:"abortConfig,omitempty"`
	JobExecutionsRolloutConfig *JobExecutionsRolloutConfig `json:"jobExecutionsRolloutConfig,omitempty"`
	TimeoutConfig              *TimeoutConfig              `json:"timeoutConfig,omitempty"`
	JobID                      string                      `json:"jobId"`
	Description                string                      `json:"description,omitempty"`
	Document                   string                      `json:"document,omitempty"`
	DocumentSource             string                      `json:"documentSource,omitempty"`
	JobTemplateARN             string                      `json:"jobTemplateArn,omitempty"`
	TargetSelection            string                      `json:"targetSelection,omitempty"`
	Targets                    []string                    `json:"targets"`
}

func (b *InMemoryBackend) CreateJob(input *CreateJobInput) (*Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.jobs[input.JobID]; exists {
		return nil, fmt.Errorf("job %q already exists: %w", input.JobID, ErrAlreadyExists)
	}
	now := float64(time.Now().UnixMilli()) / 1000
	j := &Job{
		JobID:                      input.JobID,
		JobARN:                     b.jobARN(input.JobID),
		Description:                input.Description,
		Document:                   input.Document,
		DocumentSource:             input.DocumentSource,
		JobTemplateARN:             input.JobTemplateARN,
		TargetSelection:            input.TargetSelection,
		Targets:                    append([]string(nil), input.Targets...),
		AbortConfig:                input.AbortConfig,
		JobExecutionsRolloutConfig: input.JobExecutionsRolloutConfig,
		TimeoutConfig:              input.TimeoutConfig,
		Tags:                       input.Tags,
		DocumentParameters:         input.DocumentParameters,
		Status:                     JobStatusInProgress,
		CreatedAt:                  now,
		LastUpdatedAt:              now,
	}
	b.jobs[input.JobID] = j
	return cloneJob(j), nil
}

func (b *InMemoryBackend) DescribeJob(jobID string) (*Job, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	j, ok := b.jobs[jobID]
	if !ok {
		return nil, fmt.Errorf("job %q not found: %w", jobID, ErrResourceNotFound)
	}
	return cloneJob(j), nil
}

func (b *InMemoryBackend) ListJobs() []*Job {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*Job, 0, len(b.jobs))
	for _, k := range sortedKeys(b.jobs) {
		out = append(out, cloneJob(b.jobs[k]))
	}
	return out
}

func (b *InMemoryBackend) UpdateJob(jobID, description string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	j, ok := b.jobs[jobID]
	if !ok {
		return fmt.Errorf("job %q not found: %w", jobID, ErrResourceNotFound)
	}
	if description != "" {
		j.Description = description
	}
	j.LastUpdatedAt = float64(time.Now().UnixMilli()) / 1000
	return nil
}

func (b *InMemoryBackend) CancelJob(jobID, comment string) (*Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	j, ok := b.jobs[jobID]
	if !ok {
		return nil, fmt.Errorf("job %q not found: %w", jobID, ErrResourceNotFound)
	}
	j.Status = JobStatusCanceled
	j.LastUpdatedAt = float64(time.Now().UnixMilli()) / 1000
	return cloneJob(j), nil
}

func (b *InMemoryBackend) DeleteJob(jobID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.jobs[jobID]; !ok {
		return fmt.Errorf("job %q not found: %w", jobID, ErrResourceNotFound)
	}
	delete(b.jobs, jobID)
	// Remove executions.
	for k := range b.jobExecutions {
		if len(k) > len(jobID)+1 && k[:len(jobID)] == jobID {
			delete(b.jobExecutions, k)
		}
	}
	return nil
}

func (b *InMemoryBackend) GetJobDocument(jobID string) (string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	j, ok := b.jobs[jobID]
	if !ok {
		return "", fmt.Errorf("job %q not found: %w", jobID, ErrResourceNotFound)
	}
	return j.Document, nil
}

func jobExecKey(jobID, thingName string) string {
	return jobID + "|" + thingName
}

func (b *InMemoryBackend) DescribeJobExecution(jobID, thingName string) (*JobExecution, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	key := jobExecKey(jobID, thingName)
	exec, ok := b.jobExecutions[key]
	if !ok {
		return nil, fmt.Errorf(
			"job execution for job %q / thing %q not found: %w",
			jobID,
			thingName,
			ErrResourceNotFound,
		)
	}
	cp := *exec
	return &cp, nil
}

func (b *InMemoryBackend) CancelJobExecution(jobID, thingName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := jobExecKey(jobID, thingName)
	exec, ok := b.jobExecutions[key]
	if !ok {
		// Create a canceled execution record.
		b.jobExecutions[key] = &JobExecution{
			JobID:     jobID,
			ThingName: thingName,
			Status:    JobExecCanceled,
		}
		return nil
	}
	exec.Status = JobExecCanceled
	return nil
}

func (b *InMemoryBackend) DeleteJobExecution(jobID, thingName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := jobExecKey(jobID, thingName)
	delete(b.jobExecutions, key)
	return nil
}

// ---------------------------------------------------------------------------
// JobTemplate
// ---------------------------------------------------------------------------

// JobTemplate represents an IoT job template.
type JobTemplate struct {
	Tags                       map[string]string           `json:"tags,omitempty"`
	AbortConfig                *AbortConfig                `json:"abortConfig,omitempty"`
	JobExecutionsRolloutConfig *JobExecutionsRolloutConfig `json:"jobExecutionsRolloutConfig,omitempty"`
	TimeoutConfig              *TimeoutConfig              `json:"timeoutConfig,omitempty"`
	JobTemplateID              string                      `json:"jobTemplateId"`
	JobTemplateARN             string                      `json:"jobTemplateArn"`
	Description                string                      `json:"description,omitempty"`
	Document                   string                      `json:"document,omitempty"`
	DocumentSource             string                      `json:"documentSource,omitempty"`
	CreatedAt                  float64                     `json:"createdAt,omitempty"`
}

func cloneJobTemplate(jt *JobTemplate) *JobTemplate {
	cp := *jt
	return &cp
}

func (b *InMemoryBackend) jobTemplateARN(id string) string {
	return fmt.Sprintf("arn:aws:iot:%s:%s:jobtemplate/%s", b.region, b.accountID, id)
}

// CreateJobTemplateInput holds input for CreateJobTemplate.
type CreateJobTemplateInput struct {
	Tags                       map[string]string           `json:"tags,omitempty"`
	AbortConfig                *AbortConfig                `json:"abortConfig,omitempty"`
	JobExecutionsRolloutConfig *JobExecutionsRolloutConfig `json:"jobExecutionsRolloutConfig,omitempty"`
	TimeoutConfig              *TimeoutConfig              `json:"timeoutConfig,omitempty"`
	JobTemplateID              string                      `json:"jobTemplateId"`
	Description                string                      `json:"description,omitempty"`
	Document                   string                      `json:"document,omitempty"`
	DocumentSource             string                      `json:"documentSource,omitempty"`
	JobARN                     string                      `json:"jobArn,omitempty"`
}

func (b *InMemoryBackend) CreateJobTemplate(input *CreateJobTemplateInput) (*JobTemplate, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.jobTemplates[input.JobTemplateID]; exists {
		return nil, fmt.Errorf(
			"job template %q already exists: %w",
			input.JobTemplateID,
			ErrAlreadyExists,
		)
	}
	jt := &JobTemplate{
		JobTemplateID:              input.JobTemplateID,
		JobTemplateARN:             b.jobTemplateARN(input.JobTemplateID),
		Description:                input.Description,
		Document:                   input.Document,
		DocumentSource:             input.DocumentSource,
		AbortConfig:                input.AbortConfig,
		JobExecutionsRolloutConfig: input.JobExecutionsRolloutConfig,
		TimeoutConfig:              input.TimeoutConfig,
		Tags:                       input.Tags,
		CreatedAt:                  float64(time.Now().UnixMilli()) / 1000,
	}
	b.jobTemplates[input.JobTemplateID] = jt
	return cloneJobTemplate(jt), nil
}

func (b *InMemoryBackend) DescribeJobTemplate(id string) (*JobTemplate, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	jt, ok := b.jobTemplates[id]
	if !ok {
		return nil, fmt.Errorf("job template %q not found: %w", id, ErrResourceNotFound)
	}
	return cloneJobTemplate(jt), nil
}

func (b *InMemoryBackend) ListJobTemplates() []*JobTemplate {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*JobTemplate, 0, len(b.jobTemplates))
	for _, k := range sortedKeys(b.jobTemplates) {
		out = append(out, cloneJobTemplate(b.jobTemplates[k]))
	}
	return out
}

func (b *InMemoryBackend) DeleteJobTemplate(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.jobTemplates[id]; !ok {
		return fmt.Errorf("job template %q not found: %w", id, ErrResourceNotFound)
	}
	delete(b.jobTemplates, id)
	return nil
}

// ---------------------------------------------------------------------------
// RoleAlias
// ---------------------------------------------------------------------------

// RoleAlias represents an IoT role alias.
type RoleAlias struct {
	Tags                      map[string]string `json:"tags,omitempty"`
	RoleAlias                 string            `json:"roleAlias"`
	RoleAliasARN              string            `json:"roleAliasArn"`
	RoleARN                   string            `json:"roleArn"`
	Owner                     string            `json:"owner,omitempty"`
	CredentialDurationSeconds int               `json:"credentialDurationSeconds,omitempty"`
	CreationDate              float64           `json:"creationDate,omitempty"`
	LastModifiedDate          float64           `json:"lastModifiedDate,omitempty"`
}

func cloneRoleAlias(ra *RoleAlias) *RoleAlias {
	cp := *ra
	return &cp
}

func (b *InMemoryBackend) roleAliasARN(alias string) string {
	return fmt.Sprintf("arn:aws:iot:%s:%s:rolealias/%s", b.region, b.accountID, alias)
}

// CreateRoleAliasInput holds input for CreateRoleAlias.
type CreateRoleAliasInput struct {
	Tags                      map[string]string `json:"tags,omitempty"`
	RoleAlias                 string            `json:"roleAlias"`
	RoleARN                   string            `json:"roleArn"`
	CredentialDurationSeconds int               `json:"credentialDurationSeconds,omitempty"`
}

func (b *InMemoryBackend) CreateRoleAlias(input *CreateRoleAliasInput) (*RoleAlias, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.roleAliases[input.RoleAlias]; exists {
		return nil, fmt.Errorf(
			"role alias %q already exists: %w",
			input.RoleAlias,
			ErrAlreadyExists,
		)
	}
	now := float64(time.Now().UnixMilli()) / 1000
	ra := &RoleAlias{
		RoleAlias:                 input.RoleAlias,
		RoleAliasARN:              b.roleAliasARN(input.RoleAlias),
		RoleARN:                   input.RoleARN,
		CredentialDurationSeconds: input.CredentialDurationSeconds,
		Tags:                      input.Tags,
		CreationDate:              now,
		LastModifiedDate:          now,
	}
	if ra.CredentialDurationSeconds == 0 {
		ra.CredentialDurationSeconds = 3600
	}
	b.roleAliases[input.RoleAlias] = ra
	return cloneRoleAlias(ra), nil
}

func (b *InMemoryBackend) DescribeRoleAlias(alias string) (*RoleAlias, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ra, ok := b.roleAliases[alias]
	if !ok {
		return nil, fmt.Errorf("role alias %q not found: %w", alias, ErrResourceNotFound)
	}
	return cloneRoleAlias(ra), nil
}

func (b *InMemoryBackend) ListRoleAliases() []*RoleAlias {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*RoleAlias, 0, len(b.roleAliases))
	for _, k := range sortedKeys(b.roleAliases) {
		out = append(out, cloneRoleAlias(b.roleAliases[k]))
	}
	return out
}

func (b *InMemoryBackend) UpdateRoleAlias(
	alias, roleARN string,
	credDuration int,
) (*RoleAlias, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	ra, ok := b.roleAliases[alias]
	if !ok {
		return nil, fmt.Errorf("role alias %q not found: %w", alias, ErrResourceNotFound)
	}
	if roleARN != "" {
		ra.RoleARN = roleARN
	}
	if credDuration > 0 {
		ra.CredentialDurationSeconds = credDuration
	}
	ra.LastModifiedDate = float64(time.Now().UnixMilli()) / 1000
	return cloneRoleAlias(ra), nil
}

func (b *InMemoryBackend) DeleteRoleAlias(alias string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.roleAliases[alias]; !ok {
		return fmt.Errorf("role alias %q not found: %w", alias, ErrResourceNotFound)
	}
	delete(b.roleAliases, alias)
	return nil
}

// ---------------------------------------------------------------------------
// DomainConfiguration
// ---------------------------------------------------------------------------

// DomainConfiguration represents an IoT domain configuration.
type DomainConfiguration struct {
	Tags                      map[string]string `json:"tags,omitempty"`
	DomainConfigurationName   string            `json:"domainConfigurationName"`
	DomainConfigurationARN    string            `json:"domainConfigurationArn"`
	DomainName                string            `json:"domainName,omitempty"`
	ServiceType               string            `json:"serviceType,omitempty"`
	DomainConfigurationStatus string            `json:"domainConfigurationStatus"`
	DomainType                string            `json:"domainType,omitempty"`
	CreationDate              float64           `json:"creationDate,omitempty"`
	LastModifiedDate          float64           `json:"lastModifiedDate,omitempty"`
}

func cloneDomainConfig(dc *DomainConfiguration) *DomainConfiguration {
	cp := *dc
	return &cp
}

func (b *InMemoryBackend) domainConfigARN(name string) string {
	return fmt.Sprintf("arn:aws:iot:%s:%s:domainconfiguration/%s", b.region, b.accountID, name)
}

// CreateDomainConfigurationInput holds input for CreateDomainConfiguration.
type CreateDomainConfigurationInput struct {
	Tags                    map[string]string `json:"tags,omitempty"`
	DomainConfigurationName string            `json:"domainConfigurationName"`
	DomainName              string            `json:"domainName,omitempty"`
	ServiceType             string            `json:"serviceType,omitempty"`
}

func (b *InMemoryBackend) CreateDomainConfiguration(
	input *CreateDomainConfigurationInput,
) (*DomainConfiguration, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.domainConfigs[input.DomainConfigurationName]; exists {
		return nil, fmt.Errorf(
			"domain configuration %q already exists: %w",
			input.DomainConfigurationName,
			ErrAlreadyExists,
		)
	}
	now := float64(time.Now().UnixMilli()) / 1000
	dc := &DomainConfiguration{
		DomainConfigurationName:   input.DomainConfigurationName,
		DomainConfigurationARN:    b.domainConfigARN(input.DomainConfigurationName),
		DomainName:                input.DomainName,
		ServiceType:               input.ServiceType,
		DomainConfigurationStatus: "ENABLED",
		Tags:                      input.Tags,
		CreationDate:              now,
		LastModifiedDate:          now,
	}
	if dc.ServiceType == "" {
		dc.ServiceType = "DATA"
	}
	b.domainConfigs[input.DomainConfigurationName] = dc
	return cloneDomainConfig(dc), nil
}

func (b *InMemoryBackend) DescribeDomainConfiguration(name string) (*DomainConfiguration, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	dc, ok := b.domainConfigs[name]
	if !ok {
		return nil, fmt.Errorf("domain configuration %q not found: %w", name, ErrResourceNotFound)
	}
	return cloneDomainConfig(dc), nil
}

func (b *InMemoryBackend) ListDomainConfigurations() []*DomainConfiguration {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*DomainConfiguration, 0, len(b.domainConfigs))
	for _, k := range sortedKeys(b.domainConfigs) {
		out = append(out, cloneDomainConfig(b.domainConfigs[k]))
	}
	return out
}

func (b *InMemoryBackend) UpdateDomainConfiguration(
	name, status string,
) (*DomainConfiguration, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	dc, ok := b.domainConfigs[name]
	if !ok {
		return nil, fmt.Errorf("domain configuration %q not found: %w", name, ErrResourceNotFound)
	}
	if status != "" {
		dc.DomainConfigurationStatus = status
	}
	dc.LastModifiedDate = float64(time.Now().UnixMilli()) / 1000
	return cloneDomainConfig(dc), nil
}

func (b *InMemoryBackend) DeleteDomainConfiguration(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.domainConfigs[name]; !ok {
		return fmt.Errorf("domain configuration %q not found: %w", name, ErrResourceNotFound)
	}
	delete(b.domainConfigs, name)
	return nil
}

// ---------------------------------------------------------------------------
// ProvisioningTemplate
// ---------------------------------------------------------------------------

// ProvisioningTemplate represents an IoT fleet provisioning template.
type ProvisioningTemplate struct {
	Tags                map[string]string `json:"tags,omitempty"`
	TemplateName        string            `json:"templateName"`
	TemplateARN         string            `json:"templateArn"`
	Description         string            `json:"description,omitempty"`
	TemplateBody        string            `json:"templateBody,omitempty"`
	ProvisioningRoleARN string            `json:"provisioningRoleArn,omitempty"`
	TemplateType        string            `json:"type,omitempty"`
	Enabled             bool              `json:"enabled"`
	DefaultVersionID    int32             `json:"defaultVersionId,omitempty"`
	CreationDate        float64           `json:"creationDate,omitempty"`
	LastModifiedDate    float64           `json:"lastModifiedDate,omitempty"`
}

// ProvisioningTemplateVersion represents a version of a provisioning template.
type ProvisioningTemplateVersion struct {
	VersionID        int32   `json:"versionId"`
	TemplateBody     string  `json:"templateBody,omitempty"`
	CreationDate     float64 `json:"creationDate,omitempty"`
	IsDefaultVersion bool    `json:"isDefaultVersion"`
}

func cloneProvTemplate(pt *ProvisioningTemplate) *ProvisioningTemplate {
	cp := *pt
	return &cp
}

func (b *InMemoryBackend) provTemplateARN(name string) string {
	return fmt.Sprintf("arn:aws:iot:%s:%s:provisioningtemplate/%s", b.region, b.accountID, name)
}

// CreateProvisioningTemplateInput holds input for CreateProvisioningTemplate.
type CreateProvisioningTemplateInput struct {
	Tags                map[string]string `json:"tags,omitempty"`
	TemplateName        string            `json:"templateName"`
	Description         string            `json:"description,omitempty"`
	TemplateBody        string            `json:"templateBody,omitempty"`
	ProvisioningRoleARN string            `json:"provisioningRoleArn,omitempty"`
	Type                string            `json:"type,omitempty"`
	Enabled             bool              `json:"enabled"`
}

func (b *InMemoryBackend) CreateProvisioningTemplate(
	input *CreateProvisioningTemplateInput,
) (*ProvisioningTemplate, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.provTemplates[input.TemplateName]; exists {
		return nil, fmt.Errorf(
			"provisioning template %q already exists: %w",
			input.TemplateName,
			ErrAlreadyExists,
		)
	}
	now := float64(time.Now().UnixMilli()) / 1000
	pt := &ProvisioningTemplate{
		TemplateName:        input.TemplateName,
		TemplateARN:         b.provTemplateARN(input.TemplateName),
		Description:         input.Description,
		TemplateBody:        input.TemplateBody,
		ProvisioningRoleARN: input.ProvisioningRoleARN,
		TemplateType:        input.Type,
		Enabled:             input.Enabled,
		DefaultVersionID:    1,
		Tags:                input.Tags,
		CreationDate:        now,
		LastModifiedDate:    now,
	}
	b.provTemplates[input.TemplateName] = pt
	// Create initial version.
	b.provTemplateVersions[input.TemplateName] = []*ProvisioningTemplateVersion{
		{VersionID: 1, TemplateBody: input.TemplateBody, CreationDate: now, IsDefaultVersion: true},
	}
	return cloneProvTemplate(pt), nil
}

func (b *InMemoryBackend) DescribeProvisioningTemplate(name string) (*ProvisioningTemplate, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	pt, ok := b.provTemplates[name]
	if !ok {
		return nil, fmt.Errorf("provisioning template %q not found: %w", name, ErrResourceNotFound)
	}
	return cloneProvTemplate(pt), nil
}

func (b *InMemoryBackend) ListProvisioningTemplates() []*ProvisioningTemplate {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*ProvisioningTemplate, 0, len(b.provTemplates))
	for _, k := range sortedKeys(b.provTemplates) {
		out = append(out, cloneProvTemplate(b.provTemplates[k]))
	}
	return out
}

func (b *InMemoryBackend) UpdateProvisioningTemplate(
	name, description string,
	enabled *bool,
	provRoleARN string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	pt, ok := b.provTemplates[name]
	if !ok {
		return fmt.Errorf("provisioning template %q not found: %w", name, ErrResourceNotFound)
	}
	if description != "" {
		pt.Description = description
	}
	if enabled != nil {
		pt.Enabled = *enabled
	}
	if provRoleARN != "" {
		pt.ProvisioningRoleARN = provRoleARN
	}
	pt.LastModifiedDate = float64(time.Now().UnixMilli()) / 1000
	return nil
}

func (b *InMemoryBackend) DeleteProvisioningTemplate(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.provTemplates[name]; !ok {
		return fmt.Errorf("provisioning template %q not found: %w", name, ErrResourceNotFound)
	}
	delete(b.provTemplates, name)
	delete(b.provTemplateVersions, name)
	return nil
}

func (b *InMemoryBackend) CreateProvisioningTemplateVersion(
	name, body string,
) (*ProvisioningTemplateVersion, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.provTemplates[name]; !ok {
		return nil, fmt.Errorf("provisioning template %q not found: %w", name, ErrResourceNotFound)
	}
	versions := b.provTemplateVersions[name]
	newID := int32(len(versions) + 1)
	v := &ProvisioningTemplateVersion{
		VersionID:    newID,
		TemplateBody: body,
		CreationDate: float64(time.Now().UnixMilli()) / 1000,
	}
	b.provTemplateVersions[name] = append(versions, v)
	return v, nil
}

func (b *InMemoryBackend) ListProvisioningTemplateVersions(
	name string,
) ([]*ProvisioningTemplateVersion, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.provTemplates[name]; !ok {
		return nil, fmt.Errorf("provisioning template %q not found: %w", name, ErrResourceNotFound)
	}
	src := b.provTemplateVersions[name]
	out := make([]*ProvisioningTemplateVersion, len(src))
	copy(out, src)
	return out, nil
}

func (b *InMemoryBackend) DeleteProvisioningTemplateVersion(name string, versionID int32) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.provTemplates[name]; !ok {
		return fmt.Errorf("provisioning template %q not found: %w", name, ErrResourceNotFound)
	}
	versions := b.provTemplateVersions[name]
	for i, v := range versions {
		if v.VersionID == versionID {
			b.provTemplateVersions[name] = append(versions[:i], versions[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("version %d not found: %w", versionID, ErrResourceNotFound)
}

// ---------------------------------------------------------------------------
// Authorizer
// ---------------------------------------------------------------------------

// Authorizer represents an IoT authorizer.
type Authorizer struct {
	Tags                   map[string]string `json:"tags,omitempty"`
	TokenSigningPublicKeys map[string]string `json:"tokenSigningPublicKeys,omitempty"`
	AuthorizerName         string            `json:"authorizerName"`
	AuthorizerARN          string            `json:"authorizerArn"`
	AuthorizerFunctionARN  string            `json:"authorizerFunctionArn,omitempty"`
	TokenKeyName           string            `json:"tokenKeyName,omitempty"`
	Status                 string            `json:"status"`
	SigningDisabled        bool              `json:"signingDisabled"`
	EnableCachingForHTTP   bool              `json:"enableCachingForHttp"`
	CreationDate           float64           `json:"creationDate,omitempty"`
	LastModifiedDate       float64           `json:"lastModifiedDate,omitempty"`
}

func cloneAuthorizer(a *Authorizer) *Authorizer {
	cp := *a
	return &cp
}

func (b *InMemoryBackend) authorizerARN(name string) string {
	return fmt.Sprintf("arn:aws:iot:%s:%s:authorizer/%s", b.region, b.accountID, name)
}

// CreateAuthorizerInput holds input for CreateAuthorizer.
type CreateAuthorizerInput struct {
	Tags                   map[string]string `json:"tags,omitempty"`
	TokenSigningPublicKeys map[string]string `json:"tokenSigningPublicKeys,omitempty"`
	AuthorizerName         string            `json:"authorizerName"`
	AuthorizerFunctionARN  string            `json:"authorizerFunctionArn,omitempty"`
	TokenKeyName           string            `json:"tokenKeyName,omitempty"`
	Status                 string            `json:"status,omitempty"`
	SigningDisabled        bool              `json:"signingDisabled"`
	EnableCachingForHTTP   bool              `json:"enableCachingForHttp"`
}

func (b *InMemoryBackend) CreateAuthorizer(input *CreateAuthorizerInput) (*Authorizer, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.authorizers[input.AuthorizerName]; exists {
		return nil, fmt.Errorf(
			"authorizer %q already exists: %w",
			input.AuthorizerName,
			ErrAlreadyExists,
		)
	}
	now := float64(time.Now().UnixMilli()) / 1000
	a := &Authorizer{
		AuthorizerName:         input.AuthorizerName,
		AuthorizerARN:          b.authorizerARN(input.AuthorizerName),
		AuthorizerFunctionARN:  input.AuthorizerFunctionARN,
		TokenKeyName:           input.TokenKeyName,
		SigningDisabled:        input.SigningDisabled,
		EnableCachingForHTTP:   input.EnableCachingForHTTP,
		TokenSigningPublicKeys: input.TokenSigningPublicKeys,
		Status:                 input.Status,
		Tags:                   input.Tags,
		CreationDate:           now,
		LastModifiedDate:       now,
	}
	if a.Status == "" {
		a.Status = "ACTIVE"
	}
	b.authorizers[input.AuthorizerName] = a
	return cloneAuthorizer(a), nil
}

func (b *InMemoryBackend) DescribeAuthorizer(name string) (*Authorizer, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	a, ok := b.authorizers[name]
	if !ok {
		return nil, fmt.Errorf("authorizer %q not found: %w", name, ErrResourceNotFound)
	}
	return cloneAuthorizer(a), nil
}

func (b *InMemoryBackend) ListAuthorizers() []*Authorizer {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*Authorizer, 0, len(b.authorizers))
	for _, k := range sortedKeys(b.authorizers) {
		out = append(out, cloneAuthorizer(b.authorizers[k]))
	}
	return out
}

func (b *InMemoryBackend) UpdateAuthorizer(name, functionARN, status string) (*Authorizer, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	a, ok := b.authorizers[name]
	if !ok {
		return nil, fmt.Errorf("authorizer %q not found: %w", name, ErrResourceNotFound)
	}
	if functionARN != "" {
		a.AuthorizerFunctionARN = functionARN
	}
	if status != "" {
		a.Status = status
	}
	a.LastModifiedDate = float64(time.Now().UnixMilli()) / 1000
	return cloneAuthorizer(a), nil
}

func (b *InMemoryBackend) DeleteAuthorizer(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.authorizers[name]; !ok {
		return fmt.Errorf("authorizer %q not found: %w", name, ErrResourceNotFound)
	}
	delete(b.authorizers, name)
	return nil
}

// ---------------------------------------------------------------------------
// BillingGroup
// ---------------------------------------------------------------------------

// BillingGroupProperties holds description for a billing group.
type BillingGroupProperties struct {
	BillingGroupDescription string `json:"billingGroupDescription,omitempty"`
}

// BillingGroupMetadata holds metadata for a billing group.
type BillingGroupMetadata struct {
	CreationDate float64 `json:"creationDate,omitempty"`
}

// BillingGroup represents an IoT billing group.
type BillingGroup struct {
	Tags                   map[string]string      `json:"tags,omitempty"`
	BillingGroupName       string                 `json:"billingGroupName"`
	BillingGroupARN        string                 `json:"billingGroupArn"`
	BillingGroupID         string                 `json:"billingGroupId"`
	BillingGroupProperties BillingGroupProperties `json:"billingGroupProperties,omitempty"`
	BillingGroupMetadata   BillingGroupMetadata   `json:"billingGroupMetadata,omitempty"`
	Version                int64                  `json:"version"`
}

func cloneBillingGroup(bg *BillingGroup) *BillingGroup {
	cp := *bg
	return &cp
}

func (b *InMemoryBackend) billingGroupARN(name string) string {
	return fmt.Sprintf("arn:aws:iot:%s:%s:billinggroup/%s", b.region, b.accountID, name)
}

// CreateBillingGroupInput holds input for CreateBillingGroup.
type CreateBillingGroupInput struct {
	Tags                   map[string]string      `json:"tags,omitempty"`
	BillingGroupName       string                 `json:"billingGroupName"`
	BillingGroupProperties BillingGroupProperties `json:"billingGroupProperties,omitempty"`
}

func (b *InMemoryBackend) CreateBillingGroup(
	input *CreateBillingGroupInput,
) (*BillingGroup, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.billingGroups[input.BillingGroupName]; exists {
		return nil, fmt.Errorf(
			"billing group %q already exists: %w",
			input.BillingGroupName,
			ErrAlreadyExists,
		)
	}
	bg := &BillingGroup{
		BillingGroupName:       input.BillingGroupName,
		BillingGroupARN:        b.billingGroupARN(input.BillingGroupName),
		BillingGroupID:         uuid.NewString(),
		BillingGroupProperties: input.BillingGroupProperties,
		BillingGroupMetadata: BillingGroupMetadata{
			CreationDate: float64(time.Now().UnixMilli()) / 1000,
		},
		Tags:    input.Tags,
		Version: 1,
	}
	b.billingGroups[input.BillingGroupName] = bg
	return cloneBillingGroup(bg), nil
}

func (b *InMemoryBackend) DescribeBillingGroup(name string) (*BillingGroup, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	bg, ok := b.billingGroups[name]
	if !ok {
		return nil, fmt.Errorf("billing group %q not found: %w", name, ErrResourceNotFound)
	}
	return cloneBillingGroup(bg), nil
}

func (b *InMemoryBackend) ListBillingGroups() []*BillingGroup {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*BillingGroup, 0, len(b.billingGroups))
	for _, k := range sortedKeys(b.billingGroups) {
		out = append(out, cloneBillingGroup(b.billingGroups[k]))
	}
	return out
}

func (b *InMemoryBackend) UpdateBillingGroup(
	name string,
	props BillingGroupProperties,
) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	bg, ok := b.billingGroups[name]
	if !ok {
		return 0, fmt.Errorf("billing group %q not found: %w", name, ErrResourceNotFound)
	}
	bg.BillingGroupProperties = props
	bg.Version++
	return bg.Version, nil
}

func (b *InMemoryBackend) DeleteBillingGroup(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.billingGroups[name]; !ok {
		return fmt.Errorf("billing group %q not found: %w", name, ErrResourceNotFound)
	}
	delete(b.billingGroups, name)
	return nil
}

// ---------------------------------------------------------------------------
// ScheduledAudit
// ---------------------------------------------------------------------------

// ScheduledAudit represents an IoT scheduled audit.
type ScheduledAudit struct {
	Tags               map[string]string `json:"tags,omitempty"`
	TargetCheckNames   []string          `json:"targetCheckNames,omitempty"`
	ScheduledAuditName string            `json:"scheduledAuditName"`
	ScheduledAuditARN  string            `json:"scheduledAuditArn"`
	Frequency          string            `json:"frequency"`
	DayOfMonth         string            `json:"dayOfMonth,omitempty"`
	DayOfWeek          string            `json:"dayOfWeek,omitempty"`
}

func cloneScheduledAudit(sa *ScheduledAudit) *ScheduledAudit {
	cp := *sa
	cp.TargetCheckNames = append([]string(nil), sa.TargetCheckNames...)
	return &cp
}

func (b *InMemoryBackend) scheduledAuditARN(name string) string {
	return fmt.Sprintf("arn:aws:iot:%s:%s:scheduledaudit/%s", b.region, b.accountID, name)
}

// CreateScheduledAuditInput holds input for CreateScheduledAudit.
type CreateScheduledAuditInput struct {
	Tags               map[string]string `json:"tags,omitempty"`
	TargetCheckNames   []string          `json:"targetCheckNames,omitempty"`
	ScheduledAuditName string            `json:"scheduledAuditName"`
	Frequency          string            `json:"frequency"`
	DayOfMonth         string            `json:"dayOfMonth,omitempty"`
	DayOfWeek          string            `json:"dayOfWeek,omitempty"`
}

func (b *InMemoryBackend) CreateScheduledAudit(
	input *CreateScheduledAuditInput,
) (*ScheduledAudit, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.scheduledAudits[input.ScheduledAuditName]; exists {
		return nil, fmt.Errorf(
			"scheduled audit %q already exists: %w",
			input.ScheduledAuditName,
			ErrAlreadyExists,
		)
	}
	sa := &ScheduledAudit{
		ScheduledAuditName: input.ScheduledAuditName,
		ScheduledAuditARN:  b.scheduledAuditARN(input.ScheduledAuditName),
		Frequency:          input.Frequency,
		DayOfMonth:         input.DayOfMonth,
		DayOfWeek:          input.DayOfWeek,
		TargetCheckNames:   append([]string(nil), input.TargetCheckNames...),
		Tags:               input.Tags,
	}
	b.scheduledAudits[input.ScheduledAuditName] = sa
	return cloneScheduledAudit(sa), nil
}

func (b *InMemoryBackend) DescribeScheduledAudit(name string) (*ScheduledAudit, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	sa, ok := b.scheduledAudits[name]
	if !ok {
		return nil, fmt.Errorf("scheduled audit %q not found: %w", name, ErrResourceNotFound)
	}
	return cloneScheduledAudit(sa), nil
}

func (b *InMemoryBackend) ListScheduledAudits() []*ScheduledAudit {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*ScheduledAudit, 0, len(b.scheduledAudits))
	for _, k := range sortedKeys(b.scheduledAudits) {
		out = append(out, cloneScheduledAudit(b.scheduledAudits[k]))
	}
	return out
}

func (b *InMemoryBackend) UpdateScheduledAudit(
	name, frequency, dayOfMonth, dayOfWeek string,
	checks []string,
) (*ScheduledAudit, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	sa, ok := b.scheduledAudits[name]
	if !ok {
		return nil, fmt.Errorf("scheduled audit %q not found: %w", name, ErrResourceNotFound)
	}
	if frequency != "" {
		sa.Frequency = frequency
	}
	if dayOfMonth != "" {
		sa.DayOfMonth = dayOfMonth
	}
	if dayOfWeek != "" {
		sa.DayOfWeek = dayOfWeek
	}
	if len(checks) > 0 {
		sa.TargetCheckNames = append([]string(nil), checks...)
	}
	return cloneScheduledAudit(sa), nil
}

func (b *InMemoryBackend) DeleteScheduledAudit(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.scheduledAudits[name]; !ok {
		return fmt.Errorf("scheduled audit %q not found: %w", name, ErrResourceNotFound)
	}
	delete(b.scheduledAudits, name)
	return nil
}

// ---------------------------------------------------------------------------
// MitigationAction
// ---------------------------------------------------------------------------

// MitigationAction represents an IoT mitigation action.
type MitigationAction struct {
	Tags             map[string]string `json:"tags,omitempty"`
	ActionName       string            `json:"actionName"`
	ActionARN        string            `json:"actionArn"`
	ActionID         string            `json:"actionId"`
	RoleARN          string            `json:"roleArn,omitempty"`
	ActionParams     map[string]any    `json:"actionParams,omitempty"`
	CreationDate     float64           `json:"creationDate,omitempty"`
	LastModifiedDate float64           `json:"lastModifiedDate,omitempty"`
}

func cloneMitigationAction(ma *MitigationAction) *MitigationAction {
	cp := *ma
	return &cp
}

func (b *InMemoryBackend) mitigationActionARN(name string) string {
	return fmt.Sprintf("arn:aws:iot:%s:%s:mitigationaction/%s", b.region, b.accountID, name)
}

// CreateMitigationActionInput holds input for CreateMitigationAction.
type CreateMitigationActionInput struct {
	Tags         map[string]string `json:"tags,omitempty"`
	ActionName   string            `json:"actionName"`
	RoleARN      string            `json:"roleArn,omitempty"`
	ActionParams map[string]any    `json:"actionParams,omitempty"`
}

func (b *InMemoryBackend) CreateMitigationAction(
	input *CreateMitigationActionInput,
) (*MitigationAction, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.mitigationActions[input.ActionName]; exists {
		return nil, fmt.Errorf(
			"mitigation action %q already exists: %w",
			input.ActionName,
			ErrAlreadyExists,
		)
	}
	now := float64(time.Now().UnixMilli()) / 1000
	ma := &MitigationAction{
		ActionName:       input.ActionName,
		ActionARN:        b.mitigationActionARN(input.ActionName),
		ActionID:         uuid.NewString(),
		RoleARN:          input.RoleARN,
		ActionParams:     input.ActionParams,
		Tags:             input.Tags,
		CreationDate:     now,
		LastModifiedDate: now,
	}
	b.mitigationActions[input.ActionName] = ma
	return cloneMitigationAction(ma), nil
}

func (b *InMemoryBackend) DescribeMitigationAction(name string) (*MitigationAction, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ma, ok := b.mitigationActions[name]
	if !ok {
		return nil, fmt.Errorf("mitigation action %q not found: %w", name, ErrResourceNotFound)
	}
	return cloneMitigationAction(ma), nil
}

func (b *InMemoryBackend) ListMitigationActions() []*MitigationAction {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*MitigationAction, 0, len(b.mitigationActions))
	for _, k := range sortedKeys(b.mitigationActions) {
		out = append(out, cloneMitigationAction(b.mitigationActions[k]))
	}
	return out
}

func (b *InMemoryBackend) UpdateMitigationAction(
	name, roleARN string,
	params map[string]any,
) (*MitigationAction, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	ma, ok := b.mitigationActions[name]
	if !ok {
		return nil, fmt.Errorf("mitigation action %q not found: %w", name, ErrResourceNotFound)
	}
	if roleARN != "" {
		ma.RoleARN = roleARN
	}
	if params != nil {
		ma.ActionParams = params
	}
	ma.LastModifiedDate = float64(time.Now().UnixMilli()) / 1000
	return cloneMitigationAction(ma), nil
}

func (b *InMemoryBackend) DeleteMitigationAction(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.mitigationActions[name]; !ok {
		return fmt.Errorf("mitigation action %q not found: %w", name, ErrResourceNotFound)
	}
	delete(b.mitigationActions, name)
	return nil
}

// ---------------------------------------------------------------------------
// SecurityProfile
// ---------------------------------------------------------------------------

// SecurityProfile represents an IoT security profile.
type SecurityProfile struct {
	Tags                       map[string]string `json:"tags,omitempty"`
	SecurityProfileName        string            `json:"securityProfileName"`
	SecurityProfileARN         string            `json:"securityProfileArn"`
	SecurityProfileDescription string            `json:"securityProfileDescription,omitempty"`
	Version                    int64             `json:"version"`
	CreationDate               float64           `json:"creationDate,omitempty"`
	LastModifiedDate           float64           `json:"lastModifiedDate,omitempty"`
}

func cloneSecurityProfile(sp *SecurityProfile) *SecurityProfile {
	cp := *sp
	return &cp
}

func (b *InMemoryBackend) securityProfileARN(name string) string {
	return fmt.Sprintf("arn:aws:iot:%s:%s:securityprofile/%s", b.region, b.accountID, name)
}

// CreateSecurityProfileInput holds input for CreateSecurityProfile.
type CreateSecurityProfileInput struct {
	Tags                       map[string]string `json:"tags,omitempty"`
	SecurityProfileName        string            `json:"securityProfileName"`
	SecurityProfileDescription string            `json:"securityProfileDescription,omitempty"`
}

func (b *InMemoryBackend) CreateSecurityProfile(
	input *CreateSecurityProfileInput,
) (*SecurityProfile, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.securityProfiles[input.SecurityProfileName]; exists {
		return nil, fmt.Errorf(
			"security profile %q already exists: %w",
			input.SecurityProfileName,
			ErrAlreadyExists,
		)
	}
	now := float64(time.Now().UnixMilli()) / 1000
	sp := &SecurityProfile{
		SecurityProfileName:        input.SecurityProfileName,
		SecurityProfileARN:         b.securityProfileARN(input.SecurityProfileName),
		SecurityProfileDescription: input.SecurityProfileDescription,
		Tags:                       input.Tags,
		Version:                    1,
		CreationDate:               now,
		LastModifiedDate:           now,
	}
	b.securityProfiles[input.SecurityProfileName] = sp
	return cloneSecurityProfile(sp), nil
}

func (b *InMemoryBackend) DescribeSecurityProfile(name string) (*SecurityProfile, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	sp, ok := b.securityProfiles[name]
	if !ok {
		return nil, fmt.Errorf("security profile %q not found: %w", name, ErrResourceNotFound)
	}
	return cloneSecurityProfile(sp), nil
}

func (b *InMemoryBackend) ListSecurityProfiles() []*SecurityProfile {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*SecurityProfile, 0, len(b.securityProfiles))
	for _, k := range sortedKeys(b.securityProfiles) {
		out = append(out, cloneSecurityProfile(b.securityProfiles[k]))
	}
	return out
}

func (b *InMemoryBackend) UpdateSecurityProfile(
	name, description string,
) (*SecurityProfile, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	sp, ok := b.securityProfiles[name]
	if !ok {
		return nil, fmt.Errorf("security profile %q not found: %w", name, ErrResourceNotFound)
	}
	if description != "" {
		sp.SecurityProfileDescription = description
	}
	sp.Version++
	sp.LastModifiedDate = float64(time.Now().UnixMilli()) / 1000
	return cloneSecurityProfile(sp), nil
}

func (b *InMemoryBackend) DeleteSecurityProfile(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.securityProfiles[name]; !ok {
		return fmt.Errorf("security profile %q not found: %w", name, ErrResourceNotFound)
	}
	delete(b.securityProfiles, name)
	return nil
}

// ErrResourceNotFound is returned when a new-op resource is not found.
var ErrResourceNotFound = fmt.Errorf("ResourceNotFoundException")

// Ensure sort is used.
var _ = sort.Strings
