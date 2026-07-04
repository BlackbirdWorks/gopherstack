package iot

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"
)

// ---------------------------------------------------------------------------
// CACertificate
// ---------------------------------------------------------------------------

// CACertificate represents an IoT CA certificate.
type CACertificate struct {
	CertificateARN   string  `json:"certificateArn"`
	CertificateID    string  `json:"certificateId"`
	Status           string  `json:"status"`
	CertificatePem   string  `json:"certificatePem,omitempty"`
	OwnedBy          string  `json:"ownedBy,omitempty"`
	AutoRegistration string  `json:"autoRegistrationStatus,omitempty"`
	CreationDate     float64 `json:"creationDate,omitempty"`
	LastModifiedDate float64 `json:"lastModifiedDate,omitempty"`
}

func cloneCACert(c *CACertificate) *CACertificate {
	cp := *c

	return &cp
}

func (b *InMemoryBackend) caCertARN(id string) string {
	return fmt.Sprintf("arn:aws:iot:%s:%s:cacert/%s", b.region, b.accountID, id)
}

func (b *InMemoryBackend) RegisterCACertificate(pem, status string) (*CACertificate, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := uuid.NewString()[:12]
	now := float64(time.Now().Unix())
	ca := &CACertificate{
		CertificateID:    id,
		CertificateARN:   b.caCertARN(id),
		Status:           status,
		CertificatePem:   pem,
		CreationDate:     now,
		LastModifiedDate: now,
		OwnedBy:          b.accountID,
		AutoRegistration: "DISABLE",
	}
	if ca.Status == "" {
		ca.Status = "ACTIVE"
	}
	b.caCertificates[id] = ca

	return cloneCACert(ca), nil
}

func (b *InMemoryBackend) DescribeCACertificate(id string) (*CACertificate, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ca, ok := b.caCertificates[id]
	if !ok {
		return nil, fmt.Errorf("CA certificate %q not found: %w", id, ErrResourceNotFound)
	}

	return cloneCACert(ca), nil
}

func (b *InMemoryBackend) ListCACertificates() []*CACertificate {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*CACertificate, 0, len(b.caCertificates))
	for _, k := range sortedKeys(b.caCertificates) {
		out = append(out, cloneCACert(b.caCertificates[k]))
	}

	return out
}

func (b *InMemoryBackend) UpdateCACertificate(id, status string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	ca, ok := b.caCertificates[id]
	if !ok {
		return fmt.Errorf("CA certificate %q not found: %w", id, ErrResourceNotFound)
	}
	if status != "" {
		ca.Status = status
	}
	ca.LastModifiedDate = float64(time.Now().Unix())

	return nil
}

func (b *InMemoryBackend) DeleteCACertificate(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.caCertificates[id]; !ok {
		return fmt.Errorf("CA certificate %q not found: %w", id, ErrResourceNotFound)
	}
	delete(b.caCertificates, id)

	return nil
}

func (b *InMemoryBackend) ListCertificatesByCA(caID string) []*Certificate {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var out []*Certificate
	for _, k := range sortedKeys(b.certificates) {
		cert := b.certificates[k]
		if cert.CACertificateID == caID {
			cp := *cert
			out = append(out, &cp)
		}
	}

	return out
}

// ---------------------------------------------------------------------------
// Stream
// ---------------------------------------------------------------------------

// StreamFile holds file info for an IoT stream.
type StreamFile struct {
	S3Bucket string `json:"s3Bucket,omitempty"`
	S3Key    string `json:"s3Key,omitempty"`
	FileID   int32  `json:"fileId,omitempty"`
}

// IoTStream represents an IoT data stream.
//
//nolint:revive // IoTStream is intentional to maintain AWS API naming clarity
type IoTStream struct {
	Tags          map[string]string `json:"tags,omitempty"`
	StreamID      string            `json:"streamId"`
	StreamARN     string            `json:"streamArn"`
	Description   string            `json:"description,omitempty"`
	RoleARN       string            `json:"roleArn,omitempty"`
	Files         []StreamFile      `json:"files,omitempty"`
	CreatedAt     float64           `json:"createdAt,omitempty"`
	LastUpdated   float64           `json:"lastUpdatedAt,omitempty"`
	StreamVersion int32             `json:"streamVersion,omitempty"`
}

func cloneStream(s *IoTStream) *IoTStream {
	cp := *s
	cp.Files = append([]StreamFile(nil), s.Files...)

	return &cp
}

func (b *InMemoryBackend) streamARN(id string) string {
	return fmt.Sprintf("arn:aws:iot:%s:%s:stream/%s", b.region, b.accountID, id)
}

// CreateStreamInput holds input for CreateStream.
type CreateStreamInput struct {
	Tags        map[string]string `json:"tags,omitempty"`
	StreamID    string            `json:"streamId"`
	Description string            `json:"description,omitempty"`
	RoleARN     string            `json:"roleArn,omitempty"`
	Files       []StreamFile      `json:"files"`
}

func (b *InMemoryBackend) CreateStream(input *CreateStreamInput) (*IoTStream, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.streams[input.StreamID]; exists {
		return nil, fmt.Errorf("stream %q already exists: %w", input.StreamID, ErrAlreadyExists)
	}
	now := float64(time.Now().Unix())
	s := &IoTStream{
		StreamID:      input.StreamID,
		StreamARN:     b.streamARN(input.StreamID),
		Description:   input.Description,
		RoleARN:       input.RoleARN,
		Files:         append([]StreamFile(nil), input.Files...),
		Tags:          input.Tags,
		StreamVersion: 1,
		CreatedAt:     now,
		LastUpdated:   now,
	}
	b.streams[input.StreamID] = s

	return cloneStream(s), nil
}

func (b *InMemoryBackend) DescribeStream(id string) (*IoTStream, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	s, ok := b.streams[id]
	if !ok {
		return nil, fmt.Errorf("stream %q not found: %w", id, ErrResourceNotFound)
	}

	return cloneStream(s), nil
}

func (b *InMemoryBackend) ListStreams() []*IoTStream {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*IoTStream, 0, len(b.streams))
	for _, k := range sortedKeys(b.streams) {
		out = append(out, cloneStream(b.streams[k]))
	}

	return out
}

func (b *InMemoryBackend) UpdateStream(id, description, roleARN string, files []StreamFile) (*IoTStream, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	s, ok := b.streams[id]
	if !ok {
		return nil, fmt.Errorf("stream %q not found: %w", id, ErrResourceNotFound)
	}
	if description != "" {
		s.Description = description
	}
	if roleARN != "" {
		s.RoleARN = roleARN
	}
	if len(files) > 0 {
		s.Files = files
	}
	s.StreamVersion++
	s.LastUpdated = float64(time.Now().Unix())

	return cloneStream(s), nil
}

func (b *InMemoryBackend) DeleteStream(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.streams[id]; !ok {
		return fmt.Errorf("stream %q not found: %w", id, ErrResourceNotFound)
	}
	delete(b.streams, id)

	return nil
}

// ---------------------------------------------------------------------------
// FleetMetric
// ---------------------------------------------------------------------------

// FleetMetric represents an IoT fleet metric.
type FleetMetric struct {
	Tags         map[string]string `json:"tags,omitempty"`
	MetricName   string            `json:"metricName"`
	MetricARN    string            `json:"metricArn"`
	QueryString  string            `json:"queryString,omitempty"`
	IndexName    string            `json:"indexName,omitempty"`
	QueryVersion string            `json:"queryVersion,omitempty"`
	Description  string            `json:"description,omitempty"`
	Unit         string            `json:"unit,omitempty"`
	Period       int32             `json:"period,omitempty"`
	Version      int64             `json:"version"`
	CreationDate float64           `json:"creationDate,omitempty"`
	LastModified float64           `json:"lastModifiedDate,omitempty"`
}

func cloneFleetMetric(fm *FleetMetric) *FleetMetric {
	cp := *fm

	return &cp
}

func (b *InMemoryBackend) fleetMetricARN(name string) string {
	return fmt.Sprintf("arn:aws:iot:%s:%s:fleetmetric/%s", b.region, b.accountID, name)
}

// CreateFleetMetricInput holds input for CreateFleetMetric.
type CreateFleetMetricInput struct {
	Tags         map[string]string `json:"tags,omitempty"`
	MetricName   string            `json:"metricName"`
	QueryString  string            `json:"queryString,omitempty"`
	IndexName    string            `json:"indexName,omitempty"`
	QueryVersion string            `json:"queryVersion,omitempty"`
	Description  string            `json:"description,omitempty"`
	Unit         string            `json:"unit,omitempty"`
	Period       int32             `json:"period,omitempty"`
}

func (b *InMemoryBackend) CreateFleetMetric(input *CreateFleetMetricInput) (*FleetMetric, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.fleetMetrics[input.MetricName]; exists {
		return nil, fmt.Errorf("fleet metric %q already exists: %w", input.MetricName, ErrAlreadyExists)
	}
	now := float64(time.Now().Unix())
	fm := &FleetMetric{
		MetricName:   input.MetricName,
		MetricARN:    b.fleetMetricARN(input.MetricName),
		QueryString:  input.QueryString,
		IndexName:    input.IndexName,
		QueryVersion: input.QueryVersion,
		Description:  input.Description,
		Unit:         input.Unit,
		Period:       input.Period,
		Tags:         input.Tags,
		Version:      1,
		CreationDate: now,
		LastModified: now,
	}
	b.fleetMetrics[input.MetricName] = fm

	return cloneFleetMetric(fm), nil
}

func (b *InMemoryBackend) DescribeFleetMetric(name string) (*FleetMetric, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	fm, ok := b.fleetMetrics[name]
	if !ok {
		return nil, fmt.Errorf("fleet metric %q not found: %w", name, ErrResourceNotFound)
	}

	return cloneFleetMetric(fm), nil
}

func (b *InMemoryBackend) ListFleetMetrics() []*FleetMetric {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*FleetMetric, 0, len(b.fleetMetrics))
	for _, k := range sortedKeys(b.fleetMetrics) {
		out = append(out, cloneFleetMetric(b.fleetMetrics[k]))
	}

	return out
}

func (b *InMemoryBackend) UpdateFleetMetric(name, queryString, description string, period int32) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	fm, ok := b.fleetMetrics[name]
	if !ok {
		return fmt.Errorf("fleet metric %q not found: %w", name, ErrResourceNotFound)
	}
	if queryString != "" {
		fm.QueryString = queryString
	}
	if description != "" {
		fm.Description = description
	}
	if period > 0 {
		fm.Period = period
	}
	fm.Version++
	fm.LastModified = float64(time.Now().Unix())

	return nil
}

func (b *InMemoryBackend) DeleteFleetMetric(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.fleetMetrics[name]; !ok {
		return fmt.Errorf("fleet metric %q not found: %w", name, ErrResourceNotFound)
	}
	delete(b.fleetMetrics, name)

	return nil
}

// ---------------------------------------------------------------------------
// CustomMetric
// ---------------------------------------------------------------------------

// CustomMetric represents an IoT custom metric.
type CustomMetric struct {
	Tags             map[string]string `json:"tags,omitempty"`
	MetricName       string            `json:"metricName"`
	MetricARN        string            `json:"metricArn"`
	MetricType       string            `json:"metricType"`
	DisplayName      string            `json:"displayName,omitempty"`
	Version          int64             `json:"version"`
	CreationDate     float64           `json:"creationDate,omitempty"`
	LastModifiedDate float64           `json:"lastModifiedDate,omitempty"`
}

func cloneCustomMetric(cm *CustomMetric) *CustomMetric {
	cp := *cm

	return &cp
}

func (b *InMemoryBackend) customMetricARN(name string) string {
	return fmt.Sprintf("arn:aws:iot:%s:%s:custommetric/%s", b.region, b.accountID, name)
}

// CreateCustomMetricInput holds input for CreateCustomMetric.
type CreateCustomMetricInput struct {
	Tags               map[string]string `json:"tags,omitempty"`
	MetricName         string            `json:"metricName"`
	MetricType         string            `json:"metricType"`
	DisplayName        string            `json:"displayName,omitempty"`
	ClientRequestToken string            `json:"clientRequestToken,omitempty"`
}

func (b *InMemoryBackend) CreateCustomMetric(input *CreateCustomMetricInput) (*CustomMetric, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.customMetrics[input.MetricName]; exists {
		return nil, fmt.Errorf("custom metric %q already exists: %w", input.MetricName, ErrAlreadyExists)
	}
	now := float64(time.Now().Unix())
	cm := &CustomMetric{
		MetricName:       input.MetricName,
		MetricARN:        b.customMetricARN(input.MetricName),
		MetricType:       input.MetricType,
		DisplayName:      input.DisplayName,
		Tags:             input.Tags,
		Version:          1,
		CreationDate:     now,
		LastModifiedDate: now,
	}
	b.customMetrics[input.MetricName] = cm

	return cloneCustomMetric(cm), nil
}

func (b *InMemoryBackend) DescribeCustomMetric(name string) (*CustomMetric, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	cm, ok := b.customMetrics[name]
	if !ok {
		return nil, fmt.Errorf("custom metric %q not found: %w", name, ErrResourceNotFound)
	}

	return cloneCustomMetric(cm), nil
}

func (b *InMemoryBackend) ListCustomMetrics() []*CustomMetric {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*CustomMetric, 0, len(b.customMetrics))
	for _, k := range sortedKeys(b.customMetrics) {
		out = append(out, cloneCustomMetric(b.customMetrics[k]))
	}

	return out
}

func (b *InMemoryBackend) UpdateCustomMetric(name, displayName string) (*CustomMetric, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cm, ok := b.customMetrics[name]
	if !ok {
		return nil, fmt.Errorf("custom metric %q not found: %w", name, ErrResourceNotFound)
	}
	if displayName != "" {
		cm.DisplayName = displayName
	}
	cm.Version++
	cm.LastModifiedDate = float64(time.Now().Unix())

	return cloneCustomMetric(cm), nil
}

func (b *InMemoryBackend) DeleteCustomMetric(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.customMetrics[name]; !ok {
		return fmt.Errorf("custom metric %q not found: %w", name, ErrResourceNotFound)
	}
	delete(b.customMetrics, name)

	return nil
}

// ---------------------------------------------------------------------------
// Dimension
// ---------------------------------------------------------------------------

// Dimension represents an IoT dimension.
type Dimension struct {
	Tags             map[string]string `json:"tags,omitempty"`
	Name             string            `json:"name"`
	ARN              string            `json:"arn"`
	Type             string            `json:"type"`
	StringValues     []string          `json:"stringValues,omitempty"`
	CreationDate     float64           `json:"creationDate,omitempty"`
	LastModifiedDate float64           `json:"lastModifiedDate,omitempty"`
}

func cloneDimension(d *Dimension) *Dimension {
	cp := *d
	cp.StringValues = append([]string(nil), d.StringValues...)

	return &cp
}

func (b *InMemoryBackend) dimensionARN(name string) string {
	return fmt.Sprintf("arn:aws:iot:%s:%s:dimension/%s", b.region, b.accountID, name)
}

// CreateDimensionInput holds input for CreateDimension.
type CreateDimensionInput struct {
	Tags               map[string]string `json:"tags,omitempty"`
	Name               string            `json:"name"`
	Type               string            `json:"type"`
	ClientRequestToken string            `json:"clientRequestToken,omitempty"`
	StringValues       []string          `json:"stringValues"`
}

func (b *InMemoryBackend) CreateDimension(input *CreateDimensionInput) (*Dimension, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.dimensions[input.Name]; exists {
		return nil, fmt.Errorf("dimension %q already exists: %w", input.Name, ErrAlreadyExists)
	}
	now := float64(time.Now().Unix())
	d := &Dimension{
		Name:             input.Name,
		ARN:              b.dimensionARN(input.Name),
		Type:             input.Type,
		StringValues:     append([]string(nil), input.StringValues...),
		Tags:             input.Tags,
		CreationDate:     now,
		LastModifiedDate: now,
	}
	b.dimensions[input.Name] = d

	return cloneDimension(d), nil
}

func (b *InMemoryBackend) DescribeDimension(name string) (*Dimension, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	d, ok := b.dimensions[name]
	if !ok {
		return nil, fmt.Errorf("dimension %q not found: %w", name, ErrResourceNotFound)
	}

	return cloneDimension(d), nil
}

func (b *InMemoryBackend) ListDimensions() []*Dimension {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*Dimension, 0, len(b.dimensions))
	for _, k := range sortedKeys(b.dimensions) {
		out = append(out, cloneDimension(b.dimensions[k]))
	}

	return out
}

func (b *InMemoryBackend) UpdateDimension(name string, stringValues []string) (*Dimension, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	d, ok := b.dimensions[name]
	if !ok {
		return nil, fmt.Errorf("dimension %q not found: %w", name, ErrResourceNotFound)
	}
	if len(stringValues) > 0 {
		d.StringValues = append([]string(nil), stringValues...)
	}
	d.LastModifiedDate = float64(time.Now().Unix())

	return cloneDimension(d), nil
}

func (b *InMemoryBackend) DeleteDimension(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.dimensions[name]; !ok {
		return fmt.Errorf("dimension %q not found: %w", name, ErrResourceNotFound)
	}
	delete(b.dimensions, name)

	return nil
}

// ---------------------------------------------------------------------------
// Tags (TagResource / UntagResource / ListTagsForResource)
// ---------------------------------------------------------------------------

func (b *InMemoryBackend) TagResourceGeneric(resourceARN string, tags map[string]string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.resourceTags[resourceARN] == nil {
		b.resourceTags[resourceARN] = make(map[string]string)
	}
	maps.Copy(b.resourceTags[resourceARN], tags)

	return nil
}

func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if tags, ok := b.resourceTags[resourceARN]; ok {
		for _, k := range tagKeys {
			delete(tags, k)
		}
	}

	return nil
}

func (b *InMemoryBackend) ListTagsForResource(resourceARN string) map[string]string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	tags := b.resourceTags[resourceARN]
	if tags == nil {
		return map[string]string{}
	}
	out := make(map[string]string, len(tags))
	maps.Copy(out, tags)

	return out
}

// ---------------------------------------------------------------------------
// Audit Configuration
// ---------------------------------------------------------------------------

// AuditCheckConfig holds config for a single audit check.
type AuditCheckConfig struct {
	Enabled bool `json:"enabled"`
}

// AccountAuditConfiguration holds the account-level audit configuration.
type AccountAuditConfiguration struct {
	AuditCheckConfigurations map[string]*AuditCheckConfig `json:"auditCheckConfigurations,omitempty"`
	AuditNotificationTarget  any                          `json:"auditNotificationTargetConfigurations,omitempty"`
	RoleARN                  string                       `json:"roleArn,omitempty"`
}

func (b *InMemoryBackend) UpdateAccountAuditConfiguration(roleARN string, checks map[string]*AuditCheckConfig) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.auditConfiguration == nil {
		b.auditConfiguration = &AccountAuditConfiguration{}
	}
	if roleARN != "" {
		b.auditConfiguration.RoleARN = roleARN
	}
	if len(checks) > 0 {
		b.auditConfiguration.AuditCheckConfigurations = checks
	}

	return nil
}

func (b *InMemoryBackend) DescribeAccountAuditConfiguration() *AccountAuditConfiguration {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.auditConfiguration == nil {
		return &AccountAuditConfiguration{
			AuditCheckConfigurations: map[string]*AuditCheckConfig{},
		}
	}
	cp := *b.auditConfiguration

	return &cp
}

// DeleteAccountAuditConfiguration clears the account-level audit configuration,
// restoring it to its unconfigured state. It is idempotent: deleting an
// unconfigured account still succeeds, matching AWS IoT behavior.
func (b *InMemoryBackend) DeleteAccountAuditConfiguration() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.auditConfiguration = nil

	return nil
}

// ---------------------------------------------------------------------------
// Audit Task
// ---------------------------------------------------------------------------

// AuditTask represents an IoT audit task.
type AuditTask struct {
	TaskID             string  `json:"taskId"`
	TaskStatus         string  `json:"taskStatus"`
	TaskType           string  `json:"taskType"`
	ScheduledAuditName string  `json:"scheduledAuditName,omitempty"`
	TaskStartTime      float64 `json:"taskStartTime,omitempty"`
}

func (b *InMemoryBackend) StartOnDemandAuditTask(_ []string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := uuid.NewString()[:12]
	b.auditTasks[id] = string(JobStatusInProgress)
	b.auditTaskObjects[id] = &AuditTask{
		TaskID:        id,
		TaskStatus:    string(JobStatusInProgress),
		TaskType:      "ON_DEMAND_AUDIT_TASK",
		TaskStartTime: float64(time.Now().Unix()),
	}

	return id, nil
}

func (b *InMemoryBackend) DescribeAuditTask(taskID string) (*AuditTask, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	task, ok := b.auditTaskObjects[taskID]
	if !ok {
		return nil, fmt.Errorf("audit task %q not found: %w", taskID, ErrResourceNotFound)
	}
	cp := *task

	return &cp, nil
}

func (b *InMemoryBackend) ListAuditTasks(taskType string) []*AuditTask {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var out []*AuditTask
	keys := sortedKeys(b.auditTaskObjects)
	for _, k := range keys {
		task := b.auditTaskObjects[k]
		if taskType == "" || task.TaskType == taskType {
			cp := *task
			out = append(out, &cp)
		}
	}

	return out
}

// ---------------------------------------------------------------------------
// Misc: DetachThingPrincipal, CancelCertificateTransfer, RegistrationCode
//       ListThingGroupsForThing, ListThingsInBillingGroup
//       ListPrincipalPolicies, ListPolicyPrincipals, ListTargetsForPolicy
//       ListPrincipalThings, ListPrincipalThingsV2
//       ClearDefaultAuthorizer, SetDefaultAuthorizer
//       ListJobExecutionsForJob, ListJobExecutionsForThing
// ---------------------------------------------------------------------------

func (b *InMemoryBackend) DetachThingPrincipal(thingName, principal string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	principals := b.thingPrincipals[thingName]
	for i, p := range principals {
		if p == principal {
			b.thingPrincipals[thingName] = append(principals[:i], principals[i+1:]...)

			return nil
		}
	}

	return nil // AWS returns success even if not found
}

func (b *InMemoryBackend) CancelCertificateTransfer(certID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.certificateTransfers, certID)

	return nil
}

func (b *InMemoryBackend) GetRegistrationCode() string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.registrationCode == "" {
		b.mu.RUnlock()
		b.mu.Lock()
		if b.registrationCode == "" {
			b.registrationCode = uuid.NewString()
		}
		code := b.registrationCode
		b.mu.Unlock()
		b.mu.RLock()

		return code
	}

	return b.registrationCode
}

func (b *InMemoryBackend) DeleteRegistrationCode() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.registrationCode = ""

	return nil
}

func (b *InMemoryBackend) ListThingGroupsForThing(thingName string) []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var out []string
	for groupName, members := range b.thingGroupMembers {
		if slices.Contains(members, thingName) {
			out = append(out, groupName)
		}
	}
	sort.Strings(out)

	return out
}

func (b *InMemoryBackend) ListThingsInBillingGroup(groupName string) []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var out []string
	for thingName, bg := range b.thingBillingGroups {
		if bg == groupName {
			out = append(out, thingName)
		}
	}
	sort.Strings(out)

	return out
}

func (b *InMemoryBackend) RemoveThingFromBillingGroup(thingName, _ string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.thingBillingGroups, thingName)

	return nil
}

func (b *InMemoryBackend) ListPrincipalPolicies(principal string) []*Policy {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var out []*Policy
	for policyName, targets := range b.policyTargets {
		if slices.Contains(targets, principal) {
			if p, ok := b.policies[policyName]; ok {
				cp := *p
				out = append(out, &cp)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PolicyName < out[j].PolicyName })

	return out
}

func (b *InMemoryBackend) ListPolicyPrincipals(policyName string) []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := append([]string(nil), b.policyTargets[policyName]...)
	sort.Strings(out)

	return out
}

func (b *InMemoryBackend) ListTargetsForPolicy(policyName string) []string {
	return b.ListPolicyPrincipals(policyName)
}

func (b *InMemoryBackend) ListPrincipalThings(principal string) []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var out []string
	for thingName, principals := range b.thingPrincipals {
		if slices.Contains(principals, principal) {
			out = append(out, thingName)
		}
	}
	sort.Strings(out)

	return out
}

func (b *InMemoryBackend) GetEffectivePolicies(thingName, principal string) []*Policy {
	b.mu.RLock()
	defer b.mu.RUnlock()

	seen := map[string]bool{}
	var out []*Policy

	// Collect all principals for this thing.
	var principals []string
	if principal != "" {
		principals = append(principals, principal)
	}
	principals = append(principals, b.thingPrincipals[thingName]...)

	for policyName, targets := range b.policyTargets {
		for _, target := range targets {
			if slices.Contains(principals, target) && !seen[policyName] {
				if pol, ok := b.policies[policyName]; ok {
					cp := *pol
					out = append(out, &cp)
					seen[policyName] = true
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PolicyName < out[j].PolicyName })

	return out
}

func (b *InMemoryBackend) SetDefaultAuthorizer(authorizerName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.defaultAuthorizer = authorizerName

	return nil
}

func (b *InMemoryBackend) ClearDefaultAuthorizer() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.defaultAuthorizer = ""

	return nil
}

func (b *InMemoryBackend) DescribeDefaultAuthorizer() (string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.defaultAuthorizer == "" {
		return "", fmt.Errorf("no default authorizer set: %w", ErrResourceNotFound)
	}

	return b.defaultAuthorizer, nil
}

// ListJobExecutionsForJob returns summaries of all executions for a job.
func (b *InMemoryBackend) ListJobExecutionsForJob(jobID string) []*JobExecution {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var out []*JobExecution
	for k, exec := range b.jobExecutions {
		_ = k
		if exec.JobID == jobID {
			cp := *exec
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ThingName < out[j].ThingName })

	return out
}

// ListJobExecutionsForThing returns summaries of all executions for a thing.
func (b *InMemoryBackend) ListJobExecutionsForThing(thingName string) []*JobExecution {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var out []*JobExecution
	for k, exec := range b.jobExecutions {
		_ = k
		if exec.ThingName == thingName {
			cp := *exec
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].JobID < out[j].JobID })

	return out
}
