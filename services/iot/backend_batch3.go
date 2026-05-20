package iot

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ---------------------------------------------------------------------------
// OTAUpdate
// ---------------------------------------------------------------------------

// OTAUpdate represents an AWS IoT OTA update.
type OTAUpdate struct {
	OTAUpdateARN     string   `json:"otaUpdateArn"`
	OTAUpdateID      string   `json:"otaUpdateId"`
	Description      string   `json:"description,omitempty"`
	RoleARN          string   `json:"roleArn,omitempty"`
	Status           string   `json:"otaUpdateStatus"`
	Files            []any    `json:"files,omitempty"`
	Targets          []string `json:"targets,omitempty"`
	CreationDate     float64  `json:"creationDate,omitempty"`
	LastModifiedDate float64  `json:"lastModifiedDate,omitempty"`
}

func cloneOTAUpdate(o *OTAUpdate) *OTAUpdate {
	cp := *o
	cp.Targets = append([]string(nil), o.Targets...)
	cp.Files = append([]any(nil), o.Files...)

	return &cp
}

func (b *InMemoryBackend) otaARN(id string) string {
	return fmt.Sprintf("arn:aws:iot:%s:%s:otaupdate/%s", b.region, b.accountID, id)
}

func (b *InMemoryBackend) CreateOTAUpdate(
	id, description, roleARN string,
	targets []string,
	files []any,
) (*OTAUpdate, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.otaUpdates[id]; exists {
		return nil, fmt.Errorf("OTA update %q already exists: %w", id, ErrAlreadyExists)
	}
	now := float64(time.Now().Unix())
	o := &OTAUpdate{
		OTAUpdateID:      id,
		OTAUpdateARN:     b.otaARN(id),
		Description:      description,
		RoleARN:          roleARN,
		Targets:          append([]string(nil), targets...),
		Files:            append([]any(nil), files...),
		Status:           "CREATE_COMPLETE",
		CreationDate:     now,
		LastModifiedDate: now,
	}
	b.otaUpdates[id] = o

	return cloneOTAUpdate(o), nil
}

func (b *InMemoryBackend) GetOTAUpdate(id string) (*OTAUpdate, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	o, ok := b.otaUpdates[id]
	if !ok {
		return nil, fmt.Errorf("OTA update %q not found: %w", id, ErrResourceNotFound)
	}

	return cloneOTAUpdate(o), nil
}

func (b *InMemoryBackend) DeleteOTAUpdate(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.otaUpdates[id]; !ok {
		return fmt.Errorf("OTA update %q not found: %w", id, ErrResourceNotFound)
	}
	delete(b.otaUpdates, id)

	return nil
}

func (b *InMemoryBackend) ListOTAUpdates() []*OTAUpdate {
	b.mu.RLock()
	defer b.mu.RUnlock()

	keys := sortedKeys(b.otaUpdates)
	out := make([]*OTAUpdate, 0, len(keys))
	for _, k := range keys {
		out = append(out, cloneOTAUpdate(b.otaUpdates[k]))
	}

	return out
}

// ---------------------------------------------------------------------------
// IoTPackage
// ---------------------------------------------------------------------------

// IoTPackage represents an AWS IoT software package.
//
//nolint:revive // IoTPackage is intentional to avoid conflict with the builtin package keyword
type IoTPackage struct {
	Tags               map[string]string `json:"tags,omitempty"`
	PackageARN         string            `json:"packageArn"`
	PackageName        string            `json:"packageName"`
	Description        string            `json:"description,omitempty"`
	DefaultVersionName string            `json:"defaultVersionName,omitempty"`
	CreationDate       float64           `json:"creationDate,omitempty"`
	LastModifiedDate   float64           `json:"lastModifiedDate,omitempty"`
}

func cloneIoTPackage(p *IoTPackage) *IoTPackage {
	cp := *p
	cp.Tags = make(map[string]string, len(p.Tags))
	maps.Copy(cp.Tags, p.Tags)

	return &cp
}

func (b *InMemoryBackend) packageARN(name string) string {
	return fmt.Sprintf("arn:aws:iot:%s:%s:package/%s", b.region, b.accountID, name)
}

func (b *InMemoryBackend) CreateIoTPackage(name, description string, tags map[string]string) (*IoTPackage, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.iotPackages[name]; exists {
		return nil, fmt.Errorf("package %q already exists: %w", name, ErrAlreadyExists)
	}
	now := float64(time.Now().Unix())
	p := &IoTPackage{
		PackageName:      name,
		PackageARN:       b.packageARN(name),
		Description:      description,
		Tags:             make(map[string]string),
		CreationDate:     now,
		LastModifiedDate: now,
	}
	maps.Copy(p.Tags, tags)
	b.iotPackages[name] = p

	return cloneIoTPackage(p), nil
}

func (b *InMemoryBackend) GetIoTPackage(name string) (*IoTPackage, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	p, ok := b.iotPackages[name]
	if !ok {
		return nil, fmt.Errorf("package %q not found: %w", name, ErrResourceNotFound)
	}

	return cloneIoTPackage(p), nil
}

func (b *InMemoryBackend) UpdateIoTPackage(name, description, defaultVersionName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	p, ok := b.iotPackages[name]
	if !ok {
		return fmt.Errorf("package %q not found: %w", name, ErrResourceNotFound)
	}
	if description != "" {
		p.Description = description
	}
	if defaultVersionName != "" {
		p.DefaultVersionName = defaultVersionName
	}
	p.LastModifiedDate = float64(time.Now().Unix())

	return nil
}

func (b *InMemoryBackend) DeleteIoTPackage(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.iotPackages[name]; !ok {
		return fmt.Errorf("package %q not found: %w", name, ErrResourceNotFound)
	}
	delete(b.iotPackages, name)

	return nil
}

func (b *InMemoryBackend) ListIoTPackages() []*IoTPackage {
	b.mu.RLock()
	defer b.mu.RUnlock()

	keys := sortedKeys(b.iotPackages)
	out := make([]*IoTPackage, 0, len(keys))
	for _, k := range keys {
		out = append(out, cloneIoTPackage(b.iotPackages[k]))
	}

	return out
}

// ---------------------------------------------------------------------------
// IoTPackageVersion
// ---------------------------------------------------------------------------

// IoTPackageVersion represents a version of an AWS IoT software package.
//
//nolint:revive // IoTPackageVersion is intentional to avoid conflict with the builtin package keyword
type IoTPackageVersion struct {
	Tags              map[string]string `json:"tags,omitempty"`
	PackageVersionARN string            `json:"packageVersionArn"`
	PackageName       string            `json:"packageName"`
	VersionName       string            `json:"versionName"`
	Description       string            `json:"description,omitempty"`
	Status            string            `json:"status"`
	CreationDate      float64           `json:"creationDate,omitempty"`
	LastModifiedDate  float64           `json:"lastModifiedDate,omitempty"`
}

func cloneIoTPackageVersion(v *IoTPackageVersion) *IoTPackageVersion {
	cp := *v
	cp.Tags = make(map[string]string, len(v.Tags))
	maps.Copy(cp.Tags, v.Tags)

	return &cp
}

func (b *InMemoryBackend) packageVersionARN(packageName, versionName string) string {
	return fmt.Sprintf("arn:aws:iot:%s:%s:package/%s/version/%s",
		b.region, b.accountID, packageName, versionName)
}

func (b *InMemoryBackend) CreateIoTPackageVersion(
	packageName, versionName, description string,
	tags map[string]string,
) (*IoTPackageVersion, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.packageVersions2[packageName] == nil {
		b.packageVersions2[packageName] = make(map[string]*IoTPackageVersion)
	}
	if _, exists := b.packageVersions2[packageName][versionName]; exists {
		return nil, fmt.Errorf("package version %q/%q already exists: %w", packageName, versionName, ErrAlreadyExists)
	}
	now := float64(time.Now().Unix())
	v := &IoTPackageVersion{
		PackageVersionARN: b.packageVersionARN(packageName, versionName),
		PackageName:       packageName,
		VersionName:       versionName,
		Description:       description,
		Status:            "DRAFT",
		Tags:              make(map[string]string),
		CreationDate:      now,
		LastModifiedDate:  now,
	}
	maps.Copy(v.Tags, tags)
	b.packageVersions2[packageName][versionName] = v

	return cloneIoTPackageVersion(v), nil
}

func (b *InMemoryBackend) GetIoTPackageVersion(packageName, versionName string) (*IoTPackageVersion, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.packageVersions2[packageName] == nil {
		return nil, fmt.Errorf("package version %q/%q not found: %w", packageName, versionName, ErrResourceNotFound)
	}
	v, ok := b.packageVersions2[packageName][versionName]
	if !ok {
		return nil, fmt.Errorf("package version %q/%q not found: %w", packageName, versionName, ErrResourceNotFound)
	}

	return cloneIoTPackageVersion(v), nil
}

func (b *InMemoryBackend) UpdateIoTPackageVersion(packageName, versionName, description, status string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.packageVersions2[packageName] == nil {
		return fmt.Errorf("package version %q/%q not found: %w", packageName, versionName, ErrResourceNotFound)
	}
	v, ok := b.packageVersions2[packageName][versionName]
	if !ok {
		return fmt.Errorf("package version %q/%q not found: %w", packageName, versionName, ErrResourceNotFound)
	}
	if description != "" {
		v.Description = description
	}
	if status != "" {
		v.Status = status
	}
	v.LastModifiedDate = float64(time.Now().Unix())

	return nil
}

func (b *InMemoryBackend) DeleteIoTPackageVersion(packageName, versionName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.packageVersions2[packageName] == nil {
		return fmt.Errorf("package version %q/%q not found: %w", packageName, versionName, ErrResourceNotFound)
	}
	if _, ok := b.packageVersions2[packageName][versionName]; !ok {
		return fmt.Errorf("package version %q/%q not found: %w", packageName, versionName, ErrResourceNotFound)
	}
	delete(b.packageVersions2[packageName], versionName)

	return nil
}

func (b *InMemoryBackend) ListIoTPackageVersions(packageName string) []*IoTPackageVersion {
	b.mu.RLock()
	defer b.mu.RUnlock()

	m := b.packageVersions2[packageName]
	if m == nil {
		return []*IoTPackageVersion{}
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]*IoTPackageVersion, 0, len(keys))
	for _, k := range keys {
		out = append(out, cloneIoTPackageVersion(m[k]))
	}

	return out
}

// ---------------------------------------------------------------------------
// PackageConfiguration
// ---------------------------------------------------------------------------

// PackageConfiguration holds the software package configuration.
type PackageConfiguration struct {
	VersionUpdateByJobsConfig map[string]any `json:"versionUpdateByJobsConfig,omitempty"`
}

func (b *InMemoryBackend) GetPackageConfiguration() *PackageConfiguration {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.packageConfig == nil {
		return &PackageConfiguration{}
	}
	cp := *b.packageConfig

	return &cp
}

func (b *InMemoryBackend) UpdatePackageConfiguration(cfg map[string]any) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.packageConfig == nil {
		b.packageConfig = &PackageConfiguration{}
	}
	if cfg != nil {
		b.packageConfig.VersionUpdateByJobsConfig = cfg
	}

	return nil
}

// ---------------------------------------------------------------------------
// AuditSuppression
// ---------------------------------------------------------------------------

// AuditSuppression represents an AWS IoT audit suppression.
type AuditSuppression struct {
	ResourceIdentifier   map[string]any `json:"resourceIdentifier"`
	CheckName            string         `json:"checkName"`
	Description          string         `json:"description,omitempty"`
	ExpirationDate       float64        `json:"expirationDate,omitempty"`
	SuppressIndefinitely bool           `json:"suppressIndefinitely"`
}

func auditSuppressionKey(checkName string, resourceID map[string]any) string {
	// Use a stable key from checkName plus the first value found in the map.
	var sb strings.Builder
	sb.WriteString(checkName)
	for k, v := range resourceID {
		sb.WriteString("/" + k + "=" + fmt.Sprint(v))

		break
	}

	return sb.String()
}

func cloneAuditSuppression(s *AuditSuppression) *AuditSuppression {
	cp := *s
	cp.ResourceIdentifier = make(map[string]any, len(s.ResourceIdentifier))
	maps.Copy(cp.ResourceIdentifier, s.ResourceIdentifier)

	return &cp
}

func (b *InMemoryBackend) CreateAuditSuppression(
	checkName string,
	resourceID map[string]any,
	description string,
	suppressIndefinitely bool,
	expirationDate float64,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := auditSuppressionKey(checkName, resourceID)
	if _, exists := b.auditSuppressions[key]; exists {
		return fmt.Errorf("audit suppression %q already exists: %w", key, ErrAlreadyExists)
	}
	rid := make(map[string]any, len(resourceID))
	maps.Copy(rid, resourceID)
	b.auditSuppressions[key] = &AuditSuppression{
		CheckName:            checkName,
		ResourceIdentifier:   rid,
		Description:          description,
		SuppressIndefinitely: suppressIndefinitely,
		ExpirationDate:       expirationDate,
	}

	return nil
}

func (b *InMemoryBackend) DescribeAuditSuppression(
	checkName string,
	resourceID map[string]any,
) (*AuditSuppression, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	key := auditSuppressionKey(checkName, resourceID)
	s, ok := b.auditSuppressions[key]
	if !ok {
		return nil, fmt.Errorf("audit suppression %q not found: %w", key, ErrResourceNotFound)
	}

	return cloneAuditSuppression(s), nil
}

func (b *InMemoryBackend) UpdateAuditSuppression(
	checkName string,
	resourceID map[string]any,
	description string,
	suppressIndefinitely bool,
	expirationDate float64,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := auditSuppressionKey(checkName, resourceID)
	s, ok := b.auditSuppressions[key]
	if !ok {
		return fmt.Errorf("audit suppression %q not found: %w", key, ErrResourceNotFound)
	}
	if description != "" {
		s.Description = description
	}
	s.SuppressIndefinitely = suppressIndefinitely
	if expirationDate != 0 {
		s.ExpirationDate = expirationDate
	}

	return nil
}

func (b *InMemoryBackend) DeleteAuditSuppression(checkName string, resourceID map[string]any) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := auditSuppressionKey(checkName, resourceID)
	if _, ok := b.auditSuppressions[key]; !ok {
		return fmt.Errorf("audit suppression %q not found: %w", key, ErrResourceNotFound)
	}
	delete(b.auditSuppressions, key)

	return nil
}

func (b *InMemoryBackend) ListAuditSuppressions() []*AuditSuppression {
	b.mu.RLock()
	defer b.mu.RUnlock()

	keys := sortedKeys(b.auditSuppressions)
	out := make([]*AuditSuppression, 0, len(keys))
	for _, k := range keys {
		out = append(out, cloneAuditSuppression(b.auditSuppressions[k]))
	}

	return out
}

// ---------------------------------------------------------------------------
// AuditFinding
// ---------------------------------------------------------------------------

// AuditFinding represents an AWS IoT audit finding.
type AuditFinding struct {
	NonCompliantResource map[string]any `json:"nonCompliantResource,omitempty"`
	FindingID            string         `json:"findingId"`
	TaskID               string         `json:"taskId,omitempty"`
	CheckName            string         `json:"checkName"`
	Severity             string         `json:"severity"`
	FindingTime          float64        `json:"findingTime,omitempty"`
}

func cloneAuditFinding(f *AuditFinding) *AuditFinding {
	cp := *f
	cp.NonCompliantResource = make(map[string]any, len(f.NonCompliantResource))
	maps.Copy(cp.NonCompliantResource, f.NonCompliantResource)

	return &cp
}

func (b *InMemoryBackend) DescribeAuditFinding(findingID string) (*AuditFinding, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	f, ok := b.auditFindings[findingID]
	if !ok {
		return nil, fmt.Errorf("audit finding %q not found: %w", findingID, ErrResourceNotFound)
	}

	return cloneAuditFinding(f), nil
}

func (b *InMemoryBackend) ListAuditFindings() []*AuditFinding {
	b.mu.RLock()
	defer b.mu.RUnlock()

	keys := sortedKeys(b.auditFindings)
	out := make([]*AuditFinding, 0, len(keys))
	for _, k := range keys {
		out = append(out, cloneAuditFinding(b.auditFindings[k]))
	}

	return out
}

// ---------------------------------------------------------------------------
// V2 Logging
// ---------------------------------------------------------------------------

// V2LoggingOptions holds the account-level V2 logging configuration.
type V2LoggingOptions struct {
	DefaultLogLevel string `json:"defaultLogLevel"`
	RoleARN         string `json:"roleArn,omitempty"`
	DisableAllLogs  bool   `json:"disableAllLogs"`
}

// V2LoggingLevel holds a per-target logging level.
type V2LoggingLevel struct {
	Target   map[string]any `json:"logTarget"`
	LogLevel string         `json:"logLevel"`
}

func v2LogLevelKey(target map[string]any) string {
	targetType, _ := target["targetType"].(string)
	targetName, _ := target["targetName"].(string)

	return targetType + "/" + targetName
}

func cloneV2LogLevel(l *V2LoggingLevel) *V2LoggingLevel {
	cp := *l
	cp.Target = make(map[string]any, len(l.Target))
	maps.Copy(cp.Target, l.Target)

	return &cp
}

func (b *InMemoryBackend) GetV2LoggingOptions() *V2LoggingOptions {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.v2LoggingOptions == nil {
		return &V2LoggingOptions{DefaultLogLevel: "DISABLED"}
	}
	cp := *b.v2LoggingOptions

	return &cp
}

func (b *InMemoryBackend) SetV2LoggingOptions(roleARN, defaultLogLevel string, disableAllLogs bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.v2LoggingOptions = &V2LoggingOptions{
		RoleARN:         roleARN,
		DefaultLogLevel: defaultLogLevel,
		DisableAllLogs:  disableAllLogs,
	}

	return nil
}

func (b *InMemoryBackend) SetV2LoggingLevel(target map[string]any, logLevel string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := v2LogLevelKey(target)
	tgt := make(map[string]any, len(target))
	maps.Copy(tgt, target)
	b.v2LoggingLevels[key] = &V2LoggingLevel{Target: tgt, LogLevel: logLevel}

	return nil
}

func (b *InMemoryBackend) DeleteV2LoggingLevel(target map[string]any) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := v2LogLevelKey(target)
	if _, ok := b.v2LoggingLevels[key]; !ok {
		return fmt.Errorf("V2 logging level %q not found: %w", key, ErrResourceNotFound)
	}
	delete(b.v2LoggingLevels, key)

	return nil
}

func (b *InMemoryBackend) ListV2LoggingLevels() []*V2LoggingLevel {
	b.mu.RLock()
	defer b.mu.RUnlock()

	keys := sortedKeys(b.v2LoggingLevels)
	out := make([]*V2LoggingLevel, 0, len(keys))
	for _, k := range keys {
		out = append(out, cloneV2LogLevel(b.v2LoggingLevels[k]))
	}

	return out
}

// ---------------------------------------------------------------------------
// LoggingOptions
// ---------------------------------------------------------------------------

// LoggingOptions holds the IoT account-level logging configuration.
type LoggingOptions struct {
	RoleARN  string `json:"roleArn"`
	LogLevel string `json:"logLevel"`
}

func (b *InMemoryBackend) GetLoggingOptions() *LoggingOptions {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.loggingOptions == nil {
		return &LoggingOptions{LogLevel: "DISABLED"}
	}
	cp := *b.loggingOptions

	return &cp
}

func (b *InMemoryBackend) SetLoggingOptions(roleARN, logLevel string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.loggingOptions = &LoggingOptions{RoleARN: roleARN, LogLevel: logLevel}

	return nil
}

// ---------------------------------------------------------------------------
// ProvisioningClaim
// ---------------------------------------------------------------------------

// CreateProvisioningClaim returns a fake cert/key pair for the given template.
func (b *InMemoryBackend) CreateProvisioningClaim(templateName string) (string, string, string, error) {
	b.mu.RLock()
	_, ok := b.provTemplates[templateName]
	b.mu.RUnlock()

	if !ok {
		return "", "", "", fmt.Errorf("provisioning template %q not found: %w", templateName, ErrResourceNotFound)
	}

	return fakePEM, fakePublicKey, fakePrivateKey, nil
}

// ---------------------------------------------------------------------------
// CreateKeysAndCertificate
// ---------------------------------------------------------------------------

// CreateKeysAndCertificate creates a new certificate with generated fake keys.
func (b *InMemoryBackend) CreateKeysAndCertificate(setAsActive bool) (*Certificate, string, string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	status := certStatusInactive
	if setAsActive {
		status = certStatusActive
	}
	cert := b.newCertificate(fakePEM, status)
	b.certificates[cert.CertificateID] = cert
	cp := *cert

	return &cp, fakePublicKey, fakePrivateKey, nil
}

// ---------------------------------------------------------------------------
// TransferCertificate / RejectCertificateTransfer
// ---------------------------------------------------------------------------

// TransferCertificate initiates a certificate transfer to another account.
func (b *InMemoryBackend) TransferCertificate(certID, targetAccount string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	cert, ok := b.certificates[certID]
	if !ok {
		return fmt.Errorf("certificate %q not found: %w", certID, ErrCertificateNotFound)
	}
	cert.Status = "PENDING_TRANSFER"
	cert.LastModifiedAt = time.Now()
	b.certificateTransfers[certID] = targetAccount

	return nil
}

// RejectCertificateTransfer rejects a pending certificate transfer.
func (b *InMemoryBackend) RejectCertificateTransfer(certID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	cert, ok := b.certificates[certID]
	if !ok {
		return fmt.Errorf("certificate %q not found: %w", certID, ErrCertificateNotFound)
	}
	cert.Status = certStatusInactive
	cert.LastModifiedAt = time.Now()
	delete(b.certificateTransfers, certID)

	return nil
}

// ---------------------------------------------------------------------------
// EventConfigurations
// ---------------------------------------------------------------------------

// EventConfigurations holds the IoT event configurations.
type EventConfigurations struct {
	EventConfigurations map[string]*EventConfigEntry `json:"eventConfigurations"`
}

// EventConfigEntry holds the enabled flag for an event type.
type EventConfigEntry struct {
	Enabled bool `json:"Enabled"`
}

func (b *InMemoryBackend) DescribeEventConfigurations() *EventConfigurations {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.eventConfigurations == nil {
		return &EventConfigurations{EventConfigurations: map[string]*EventConfigEntry{}}
	}
	cp := EventConfigurations{
		EventConfigurations: make(map[string]*EventConfigEntry, len(b.eventConfigurations.EventConfigurations)),
	}
	for k, v := range b.eventConfigurations.EventConfigurations {
		e := *v
		cp.EventConfigurations[k] = &e
	}

	return &cp
}

func (b *InMemoryBackend) UpdateEventConfigurations(cfgs map[string]*EventConfigEntry) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.eventConfigurations == nil {
		b.eventConfigurations = &EventConfigurations{EventConfigurations: make(map[string]*EventConfigEntry)}
	}
	for k, v := range cfgs {
		e := *v
		b.eventConfigurations.EventConfigurations[k] = &e
	}

	return nil
}

// ---------------------------------------------------------------------------
// SecurityProfile targets (detach / list targets / list profiles for target)
// ---------------------------------------------------------------------------

// DetachSecurityProfile removes a target ARN from a security profile.
func (b *InMemoryBackend) DetachSecurityProfile(profileName, targetARN string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	targets := b.securityProfileTargets[profileName]
	filtered := make([]string, 0, len(targets))
	for _, t := range targets {
		if t != targetARN {
			filtered = append(filtered, t)
		}
	}
	b.securityProfileTargets[profileName] = filtered

	return nil
}

// ListTargetsForSecurityProfile returns the target ARNs attached to a security profile.
func (b *InMemoryBackend) ListTargetsForSecurityProfile(profileName string) []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	targets := b.securityProfileTargets[profileName]
	out := make([]string, len(targets))
	copy(out, targets)

	return out
}

// ListSecurityProfilesForTarget returns profile names attached to a target ARN.
func (b *InMemoryBackend) ListSecurityProfilesForTarget(targetARN string) []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var out []string
	for profileName, targets := range b.securityProfileTargets {
		if slices.Contains(targets, targetARN) {
			out = append(out, profileName)
		}
	}
	sort.Strings(out)

	return out
}

// ---------------------------------------------------------------------------
// DynamicThingGroup (reuses ThingGroup with IsDynamic=true)
// ---------------------------------------------------------------------------

// CreateDynamicThingGroup creates a dynamic thing group with a query string.
func (b *InMemoryBackend) CreateDynamicThingGroup(input *CreateThingGroupInput) (*ThingGroup, error) {
	if input.ThingGroupName == "" {
		return nil, fmt.Errorf("%w: ThingGroupName is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.thingGroups[input.ThingGroupName]; exists {
		return nil, fmt.Errorf("%w: dynamic thing group %q already exists", ErrAlreadyExists, input.ThingGroupName)
	}
	arn := fmt.Sprintf("arn:aws:iot:%s:%s:thinggroup/%s", b.region, b.accountID, input.ThingGroupName)
	id := uuid.NewString()
	attrs := make(map[string]string)
	if input.Attributes != nil {
		maps.Copy(attrs, input.Attributes)
	}
	tg := &ThingGroup{
		ThingGroupName:  input.ThingGroupName,
		ThingGroupARN:   arn,
		ThingGroupID:    id,
		Description:     input.Description,
		ParentGroupName: input.ParentGroupName,
		Attributes:      attrs,
		Members:         []string{},
		Version:         1,
		CreatedAt:       time.Now(),
		QueryString:     input.QueryString,
		IsDynamic:       true,
	}
	b.thingGroups[input.ThingGroupName] = tg
	b.thingGroupMembers[input.ThingGroupName] = []string{}

	return tg, nil
}

// DeleteDynamicThingGroup deletes a dynamic thing group.
func (b *InMemoryBackend) DeleteDynamicThingGroup(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.thingGroups[name]; !ok {
		return fmt.Errorf("%w: %s", ErrThingGroupNotFound, name)
	}
	delete(b.thingGroups, name)
	delete(b.thingGroupMembers, name)

	return nil
}

// UpdateDynamicThingGroup updates a dynamic thing group and returns the new version.
func (b *InMemoryBackend) UpdateDynamicThingGroup(input *UpdateThingGroupInput) (int64, error) {
	if input.ThingGroupName == "" {
		return 0, fmt.Errorf("%w: ThingGroupName is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	tg, ok := b.thingGroups[input.ThingGroupName]
	if !ok {
		return 0, fmt.Errorf("%w: %s", ErrThingGroupNotFound, input.ThingGroupName)
	}
	if input.Description != "" {
		tg.Description = input.Description
	}
	if input.QueryString != "" {
		tg.QueryString = input.QueryString
	}
	tg.Version++

	return tg.Version, nil
}

// ---------------------------------------------------------------------------
// IoTCommand
// ---------------------------------------------------------------------------

// IoTCommand represents an AWS IoT command.
//
//nolint:revive // IoTCommand is intentional to maintain AWS API naming clarity
type IoTCommand struct {
	Tags            map[string]string `json:"tags,omitempty"`
	Payload         map[string]any    `json:"payload,omitempty"`
	CommandARN      string            `json:"commandArn"`
	CommandID       string            `json:"commandId"`
	DisplayName     string            `json:"displayName,omitempty"`
	Description     string            `json:"description,omitempty"`
	Namespace       string            `json:"namespace,omitempty"`
	CreationDate    float64           `json:"creationDate,omitempty"`
	LastUpdated     float64           `json:"lastUpdatedAt,omitempty"`
	Deprecated      bool              `json:"deprecated"`
	PendingDeletion bool              `json:"pendingDeletion"`
}

func cloneIoTCommand(cmd *IoTCommand) *IoTCommand {
	cp := *cmd
	cp.Tags = make(map[string]string, len(cmd.Tags))
	maps.Copy(cp.Tags, cmd.Tags)
	cp.Payload = make(map[string]any, len(cmd.Payload))
	maps.Copy(cp.Payload, cmd.Payload)

	return &cp
}

func (b *InMemoryBackend) commandARN(id string) string {
	return fmt.Sprintf("arn:aws:iot:%s:%s:command/%s", b.region, b.accountID, id)
}

func (b *InMemoryBackend) CreateCommand(
	id, displayName, description, namespace string,
	payload map[string]any,
	tags map[string]string,
) (*IoTCommand, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.commands[id]; exists {
		return nil, fmt.Errorf("command %q already exists: %w", id, ErrAlreadyExists)
	}
	now := float64(time.Now().Unix())
	cmd := &IoTCommand{
		CommandID:    id,
		CommandARN:   b.commandARN(id),
		DisplayName:  displayName,
		Description:  description,
		Namespace:    namespace,
		Tags:         make(map[string]string),
		Payload:      make(map[string]any),
		CreationDate: now,
		LastUpdated:  now,
	}
	maps.Copy(cmd.Tags, tags)
	maps.Copy(cmd.Payload, payload)
	b.commands[id] = cmd

	return cloneIoTCommand(cmd), nil
}

func (b *InMemoryBackend) GetCommand(id string) (*IoTCommand, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	cmd, ok := b.commands[id]
	if !ok {
		return nil, fmt.Errorf("command %q not found: %w", id, ErrResourceNotFound)
	}

	return cloneIoTCommand(cmd), nil
}

func (b *InMemoryBackend) UpdateCommand(id, displayName, description string, deprecated bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	cmd, ok := b.commands[id]
	if !ok {
		return fmt.Errorf("command %q not found: %w", id, ErrResourceNotFound)
	}
	if displayName != "" {
		cmd.DisplayName = displayName
	}
	if description != "" {
		cmd.Description = description
	}
	cmd.Deprecated = deprecated
	cmd.LastUpdated = float64(time.Now().Unix())

	return nil
}

func (b *InMemoryBackend) DeleteCommand(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.commands[id]; !ok {
		return fmt.Errorf("command %q not found: %w", id, ErrResourceNotFound)
	}
	delete(b.commands, id)

	return nil
}

func (b *InMemoryBackend) ListCommands() []*IoTCommand {
	b.mu.RLock()
	defer b.mu.RUnlock()

	keys := sortedKeys(b.commands)
	out := make([]*IoTCommand, 0, len(keys))
	for _, k := range keys {
		out = append(out, cloneIoTCommand(b.commands[k]))
	}

	return out
}

// ---------------------------------------------------------------------------
// IoTCommandExecution
// ---------------------------------------------------------------------------

// IoTCommandExecution represents an execution of an IoT command.
//
//nolint:revive // IoTCommandExecution is intentional to maintain AWS API naming clarity
type IoTCommandExecution struct {
	CommandARN   string  `json:"commandArn"`
	ExecutionID  string  `json:"executionId"`
	ThingARN     string  `json:"thingArn"`
	Status       string  `json:"status"`
	CreationDate float64 `json:"createdAt,omitempty"`
}

func (b *InMemoryBackend) commandExecutionKey(commandID, executionID string) string {
	return commandID + "/" + executionID
}

func (b *InMemoryBackend) GetCommandExecution(commandID, executionID string) (*IoTCommandExecution, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	key := b.commandExecutionKey(commandID, executionID)
	ex, ok := b.commandExecutions[key]
	if !ok {
		return nil, fmt.Errorf("command execution %q/%q not found: %w", commandID, executionID, ErrResourceNotFound)
	}
	cp := *ex

	return &cp, nil
}

func (b *InMemoryBackend) ListCommandExecutions(commandID string) []*IoTCommandExecution {
	b.mu.RLock()
	defer b.mu.RUnlock()

	prefix := commandID + "/"
	var out []*IoTCommandExecution
	for k, ex := range b.commandExecutions {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			cp := *ex
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ExecutionID < out[j].ExecutionID })

	return out
}

// ---------------------------------------------------------------------------
// Fake key material
// ---------------------------------------------------------------------------

const fakePublicKey = `-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA2a2rwplBQLzHPZe5TNJG
pECOsDSZBBFKpbD+mBBpMBWMdQwCgYIKoZIzj0EAwIDSQADRgIhANXBFB+AAAAA
-----END PUBLIC KEY-----`

//nolint:gosec // fakePrivateKey is a test-only placeholder, not a real credential
const fakePrivateKey = `-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEA2a2rwplBQLzHPZe5TNJGpECOsDSZBBFKpbD+mBBpMBWMdQwC
gYIKoZIzj0EAwIDSQADRgIhANXBFB+AAAAA
-----END RSA PRIVATE KEY-----`
