package iam

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"maps"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// ---- SSH Public Keys ----

// SSHPublicKey represents an IAM SSH public key for a user.
type SSHPublicKey struct {
	UploadDate       time.Time `json:"UploadDate"`
	UserName         string    `json:"UserName,omitempty"`
	SSHPublicKeyID   string    `json:"SSHPublicKeyId,omitempty"`
	SSHPublicKeyBody string    `json:"SSHPublicKeyBody,omitempty"`
	Fingerprint      string    `json:"Fingerprint,omitempty"`
	Status           string    `json:"Status,omitempty"`
}

// ---- Access Advisor ----

// ServiceLastAccessedDetail tracks when a service was last accessed.
type ServiceLastAccessedDetail struct {
	ServiceName                string    `json:"ServiceName,omitempty"`
	ServiceNamespace           string    `json:"ServiceNamespace,omitempty"`
	LastAuthenticated          time.Time `json:"LastAuthenticated"`
	LastAuthenticatedArn       string    `json:"LastAuthenticatedArn,omitempty"`
	TotalAuthenticatedEntities int       `json:"TotalAuthenticatedEntities,omitempty"`
}

// accessAdvisorJob represents a pending/completed access advisor job.
type accessAdvisorJob struct {
	JobID     string    `json:"jobID,omitempty"`
	EntityARN string    `json:"entityARN,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
	Status    string    `json:"status,omitempty"` // IN_PROGRESS or COMPLETED
}

// ---- Comprehensive backend additions ----

// comprehensiveBackend stores SSH keys, MFA user links, access advisor jobs, and service-last-accessed data.
type comprehensiveBackend struct {
	// SSH public keys: keyID → SSHPublicKey
	sshPublicKeys map[string]SSHPublicKey

	// MFA device to user link: serialNumber → userName (empty = unassigned)
	mfaUserLinks map[string]string

	// Access advisor jobs: jobID → job
	accessAdvisorJobs map[string]*accessAdvisorJob

	// Service last accessed: entityARN → service namespace → detail
	serviceLastAccessed map[string]map[string]ServiceLastAccessedDetail

	// Org access report jobs: jobID → creation time
	orgReportJobs map[string]time.Time

	mu sync.Mutex
}

func newComprehensiveBackend() *comprehensiveBackend {
	return &comprehensiveBackend{
		sshPublicKeys:       make(map[string]SSHPublicKey),
		mfaUserLinks:        make(map[string]string),
		accessAdvisorJobs:   make(map[string]*accessAdvisorJob),
		serviceLastAccessed: make(map[string]map[string]ServiceLastAccessedDetail),
		orgReportJobs:       make(map[string]time.Time),
	}
}

// comprehensiveSnapshot is the serializable snapshot of comprehensiveBackend
// state, embedded in backendSnapshot.Comprehensive so SSH public keys, MFA
// user links, access advisor jobs, service-last-accessed data, and org report
// jobs survive a persistence restore rather than being silently dropped.
type comprehensiveSnapshot struct {
	SSHPublicKeys       map[string]SSHPublicKey                         `json:"sshPublicKeys,omitempty"`
	MFAUserLinks        map[string]string                               `json:"mfaUserLinks,omitempty"`
	AccessAdvisorJobs   map[string]*accessAdvisorJob                    `json:"accessAdvisorJobs,omitempty"`
	ServiceLastAccessed map[string]map[string]ServiceLastAccessedDetail `json:"serviceLastAccessed,omitempty"`
	OrgReportJobs       map[string]time.Time                            `json:"orgReportJobs,omitempty"`
}

// snapshot returns a deep copy of the comprehensive backend's state for
// persistence. It locks only c.mu (never b.mu), so callers must invoke it
// outside of any InMemoryBackend.mu critical section to avoid establishing a
// new nested-lock order.
func (c *comprehensiveBackend) snapshot() comprehensiveSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()

	return comprehensiveSnapshot{
		SSHPublicKeys:       maps.Clone(c.sshPublicKeys),
		MFAUserLinks:        maps.Clone(c.mfaUserLinks),
		AccessAdvisorJobs:   maps.Clone(c.accessAdvisorJobs),
		ServiceLastAccessed: maps.Clone(c.serviceLastAccessed),
		OrgReportJobs:       maps.Clone(c.orgReportJobs),
	}
}

// restore replaces the comprehensive backend's state from a snapshot. Like
// snapshot, it locks only c.mu and must be called outside of any
// InMemoryBackend.mu critical section.
func (c *comprehensiveBackend) restore(snap comprehensiveSnapshot) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.sshPublicKeys = snap.SSHPublicKeys
	if c.sshPublicKeys == nil {
		c.sshPublicKeys = make(map[string]SSHPublicKey)
	}

	c.mfaUserLinks = snap.MFAUserLinks
	if c.mfaUserLinks == nil {
		c.mfaUserLinks = make(map[string]string)
	}

	c.accessAdvisorJobs = snap.AccessAdvisorJobs
	if c.accessAdvisorJobs == nil {
		c.accessAdvisorJobs = make(map[string]*accessAdvisorJob)
	}

	c.serviceLastAccessed = snap.ServiceLastAccessed
	if c.serviceLastAccessed == nil {
		c.serviceLastAccessed = make(map[string]map[string]ServiceLastAccessedDetail)
	}

	c.orgReportJobs = snap.OrgReportJobs
	if c.orgReportJobs == nil {
		c.orgReportJobs = make(map[string]time.Time)
	}
}

// comp returns the comprehensiveBackend associated with this InMemoryBackend.
// It is always non-nil because NewInMemoryBackendWithConfig initialises it.
func (b *InMemoryBackend) comp() *comprehensiveBackend {
	if b.comprehensive == nil {
		b.comprehensive = newComprehensiveBackend()
	}

	return b.comprehensive
}

// ---- SSH public key methods ----

// sshFingerprintBytes is the number of bytes used to derive a fingerprint.
const sshFingerprintBytes = 8

// minSSHBodyLen is the minimum SSH public key body length for fingerprint derivation.
const minSSHBodyLen = 10

// UploadSSHPublicKey stores an SSH public key for a user.
func (b *InMemoryBackend) UploadSSHPublicKey(userName, body string) (*SSHPublicKey, error) {
	b.mu.RLock("UploadSSHPublicKey-check")
	_, exists := b.users[userName]
	b.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	keyID := strings.ToUpper(newID("apka"))
	fingerprint := computeSSHFingerprint(body)

	key := SSHPublicKey{
		SSHPublicKeyID:   keyID,
		UserName:         userName,
		SSHPublicKeyBody: body,
		Fingerprint:      fingerprint,
		Status:           accessKeyStatusActive,
		UploadDate:       time.Now().UTC(),
	}

	c := b.comp()
	c.mu.Lock()
	c.sshPublicKeys[keyID] = key
	c.mu.Unlock()

	return &key, nil
}

// GetSSHPublicKey retrieves an SSH public key by user name and key ID.
func (b *InMemoryBackend) GetSSHPublicKey(userName, keyID string) (*SSHPublicKey, error) {
	c := b.comp()
	c.mu.Lock()
	key, exists := c.sshPublicKeys[keyID]
	c.mu.Unlock()

	if !exists || key.UserName != userName {
		return nil, fmt.Errorf("%w: SSH public key %q not found for user %q", ErrAccessKeyNotFound, keyID, userName)
	}

	return &key, nil
}

// ListSSHPublicKeys returns all SSH public keys for a user.
func (b *InMemoryBackend) ListSSHPublicKeys(
	userName, marker string, maxItems int,
) (page.Page[SSHPublicKey], error) {
	b.mu.RLock("ListSSHPublicKeys-check")
	_, exists := b.users[userName]
	b.mu.RUnlock()

	if !exists {
		return page.Page[SSHPublicKey]{}, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	c := b.comp()
	c.mu.Lock()

	var keys []SSHPublicKey

	for _, k := range c.sshPublicKeys {
		if k.UserName == userName {
			keys = append(keys, k)
		}
	}

	c.mu.Unlock()

	sort.Slice(keys, func(i, j int) bool { return keys[i].SSHPublicKeyID < keys[j].SSHPublicKeyID })

	return page.New(keys, marker, maxItems, iamDefaultMaxItems), nil
}

// UpdateSSHPublicKey updates the status of an SSH public key.
func (b *InMemoryBackend) UpdateSSHPublicKey(userName, keyID, status string) error {
	c := b.comp()
	c.mu.Lock()
	defer c.mu.Unlock()

	key, exists := c.sshPublicKeys[keyID]
	if !exists || key.UserName != userName {
		return fmt.Errorf("%w: SSH public key %q not found for user %q", ErrAccessKeyNotFound, keyID, userName)
	}

	key.Status = status
	c.sshPublicKeys[keyID] = key

	return nil
}

// DeleteSSHPublicKey removes an SSH public key.
func (b *InMemoryBackend) DeleteSSHPublicKey(userName, keyID string) error {
	c := b.comp()
	c.mu.Lock()
	defer c.mu.Unlock()

	key, exists := c.sshPublicKeys[keyID]
	if !exists || key.UserName != userName {
		return fmt.Errorf("%w: SSH public key %q not found for user %q", ErrAccessKeyNotFound, keyID, userName)
	}

	delete(c.sshPublicKeys, keyID)

	return nil
}

// computeSSHFingerprint returns a placeholder fingerprint for an SSH public key body.
func computeSSHFingerprint(body string) string {
	if len(body) < minSSHBodyLen {
		return "aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99"
	}

	buf := make([]byte, sshFingerprintBytes)

	for i := range buf {
		buf[i] = body[i%len(body)]
	}

	parts := make([]string, sshFingerprintBytes)

	for i, b := range buf {
		parts[i] = fmt.Sprintf("%02x", b)
	}

	return strings.Join(parts, ":")
}

// ---- MFA device user linking ----

// EnableMFADevice links a virtual MFA device to a user.
// Returns an error if the device is already enabled (double-enable rejected).
func (b *InMemoryBackend) EnableMFADevice(userName, serialNumber, authCode1, authCode2 string) error {
	b.mu.RLock("EnableMFADevice-check")
	_, userExists := b.users[userName]
	dev, deviceExists := b.virtualMFADevices[serialNumber]
	b.mu.RUnlock()

	if !userExists {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	if !deviceExists {
		return fmt.Errorf("%w: virtual MFA device %q not found", ErrInvalidAction, serialNumber)
	}

	if dev.Status == MFAStatusEnabled {
		return fmt.Errorf(
			"%w: virtual MFA device %q is already enabled",
			ErrInvalidAction, serialNumber,
		)
	}

	if err := b.validateAuthCodes(authCode1, authCode2); err != nil {
		return err
	}

	c := b.comp()
	c.mu.Lock()
	c.mfaUserLinks[serialNumber] = userName
	c.mu.Unlock()

	// Update device status to Active.
	_ = b.setMFADeviceStatus(serialNumber, MFAStatusEnabled)

	return nil
}

// DeactivateMFADevice unlinks a virtual MFA device from a user.
// Returns an error if the device is not currently enabled.
func (b *InMemoryBackend) DeactivateMFADevice(userName, serialNumber string) error {
	b.mu.RLock("DeactivateMFADevice-check")
	_, userExists := b.users[userName]
	dev, deviceExists := b.virtualMFADevices[serialNumber]
	b.mu.RUnlock()

	if !userExists {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	if !deviceExists {
		return fmt.Errorf("%w: virtual MFA device %q not found", ErrInvalidAction, serialNumber)
	}

	if dev.Status != MFAStatusEnabled {
		return fmt.Errorf(
			"%w: virtual MFA device %q is not currently enabled",
			ErrInvalidAction, serialNumber,
		)
	}

	c := b.comp()
	c.mu.Lock()
	delete(c.mfaUserLinks, serialNumber)
	c.mu.Unlock()

	_ = b.setMFADeviceStatus(serialNumber, MFAStatusDeactivated)

	return nil
}

// GetMFADeviceOwner returns the user name that owns the given MFA device, or "".
func (b *InMemoryBackend) GetMFADeviceOwner(serialNumber string) string {
	c := b.comp()
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.mfaUserLinks[serialNumber]
}

// ListMFADevicesForUser returns all MFA devices assigned to a user.
func (b *InMemoryBackend) ListMFADevicesForUser(userName string) ([]VirtualMFADevice, error) {
	b.mu.RLock("ListMFADevicesForUser")
	defer b.mu.RUnlock()

	if _, exists := b.users[userName]; !exists {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	c := b.comp()
	c.mu.Lock()
	defer c.mu.Unlock()

	var devices []VirtualMFADevice

	for serial, owner := range c.mfaUserLinks {
		if owner == userName {
			if dev, ok := b.virtualMFADevices[serial]; ok {
				devices = append(devices, dev)
			}
		}
	}

	sort.Slice(devices, func(i, j int) bool { return devices[i].SerialNumber < devices[j].SerialNumber })

	return devices, nil
}

// GetVirtualMFADevice returns the virtual MFA device with the given serial number
// along with the user name it is currently assigned to (empty if unassigned).
// It returns ErrUserNotFound (mapped to NoSuchEntity) when no such device exists.
func (b *InMemoryBackend) GetVirtualMFADevice(serialNumber string) (VirtualMFADevice, string, error) {
	b.mu.RLock("GetVirtualMFADevice")
	dev, exists := b.virtualMFADevices[serialNumber]
	b.mu.RUnlock()

	if !exists {
		return VirtualMFADevice{}, "", fmt.Errorf("%w: virtual MFA device %q not found", ErrUserNotFound, serialNumber)
	}

	c := b.comp()
	c.mu.Lock()
	owner := c.mfaUserLinks[serialNumber]
	c.mu.Unlock()

	return dev, owner, nil
}

// ResyncMFADevice resynchronizes the named virtual MFA device for a user.
// AWS validates that the user exists and that the MFA device is associated
// with that user; the resync itself stores no additional state (no TOTP
// validation is performed in the mock). It returns ErrUserNotFound
// (NoSuchEntity) when the user or association is missing.
func (b *InMemoryBackend) ResyncMFADevice(userName, serialNumber, authCode1, authCode2 string) error {
	b.mu.RLock("ResyncMFADevice-check")
	_, userExists := b.users[userName]
	_, deviceExists := b.virtualMFADevices[serialNumber]
	b.mu.RUnlock()

	if !userExists {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	if !deviceExists {
		return fmt.Errorf("%w: virtual MFA device %q not found", ErrUserNotFound, serialNumber)
	}

	c := b.comp()
	c.mu.Lock()
	owner := c.mfaUserLinks[serialNumber]
	c.mu.Unlock()

	if owner != userName {
		return fmt.Errorf(
			"%w: virtual MFA device %q is not associated with user %q",
			ErrUserNotFound,
			serialNumber,
			userName,
		)
	}

	if err := b.validateAuthCodes(authCode1, authCode2); err != nil {
		return err
	}

	return nil
}

// ---- Access Advisor ----

// GenerateServiceLastAccessedDetailsForEntity creates a new access-advisor job for the given entity ARN.
func (b *InMemoryBackend) GenerateServiceLastAccessedDetailsForEntity(entityARN string) string {
	jobID := "sladjob-" + newID("")

	c := b.comp()
	c.mu.Lock()
	c.accessAdvisorJobs[jobID] = &accessAdvisorJob{
		JobID:     jobID,
		EntityARN: entityARN,
		CreatedAt: time.Now().UTC(),
		Status:    jobStatusCompleted, // Immediately complete in the mock
	}
	c.mu.Unlock()

	return jobID
}

// GetServiceLastAccessedDetails returns the access details for a given job ID.
// Returns job status and the list of service access details.
func (b *InMemoryBackend) GetServiceLastAccessedDetails(jobID string) (string, []ServiceLastAccessedDetail, error) {
	c := b.comp()
	c.mu.Lock()
	job, exists := c.accessAdvisorJobs[jobID]
	c.mu.Unlock()

	if !exists {
		// Return COMPLETED with empty list if job not found (permissive mock behavior).
		return jobStatusCompleted, []ServiceLastAccessedDetail{}, nil
	}

	c.mu.Lock()
	entityDetails := c.serviceLastAccessed[job.EntityARN]
	c.mu.Unlock()

	result := make([]ServiceLastAccessedDetail, 0, len(entityDetails))

	for _, d := range entityDetails {
		result = append(result, d)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ServiceNamespace < result[j].ServiceNamespace
	})

	return jobStatusCompleted, result, nil
}

// RecordServiceAccess records that an entity accessed a service.
func (b *InMemoryBackend) RecordServiceAccess(entityARN, serviceNamespace, serviceName string) {
	c := b.comp()
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.serviceLastAccessed[entityARN] == nil {
		c.serviceLastAccessed[entityARN] = make(map[string]ServiceLastAccessedDetail)
	}

	c.serviceLastAccessed[entityARN][serviceNamespace] = ServiceLastAccessedDetail{
		ServiceName:                serviceName,
		ServiceNamespace:           serviceNamespace,
		LastAuthenticated:          time.Now().UTC(),
		LastAuthenticatedArn:       entityARN,
		TotalAuthenticatedEntities: 1,
	}
}

// ---- Virtual MFA Device with seeds ----

// mfaSeedBytes is the number of random bytes used for MFA seed generation.
const mfaSeedBytes = 20

// mfaQRCodeBytes is the number of random bytes used for fake QR code PNG generation.
const mfaQRCodeBytes = 32

// CreateVirtualMFADeviceFull creates a virtual MFA device with QR code and seed data.
func (b *InMemoryBackend) CreateVirtualMFADeviceFull(
	virtualMFADeviceName, path string,
) (*VirtualMFADevice, error) {
	if virtualMFADeviceName == "" {
		return nil, fmt.Errorf("%w: VirtualMFADeviceName must not be empty", ErrInvalidAction)
	}

	p := normPath(path)
	serialNumber := arn.Build("iam", "", b.accountID, "mfa"+p+virtualMFADeviceName)

	b.mu.Lock("CreateVirtualMFADeviceFull")
	defer b.mu.Unlock()

	if _, exists := b.virtualMFADevices[serialNumber]; exists {
		return nil, fmt.Errorf("%w: virtual MFA device %q already exists", ErrUserAlreadyExists, virtualMFADeviceName)
	}

	// Generate a fake 20-byte seed and encode as base64 (mock base32 approximation).
	seedBytes := make([]byte, mfaSeedBytes)
	_, _ = rand.Read(seedBytes)
	base32Seed := base64.StdEncoding.EncodeToString(seedBytes)

	// Generate a minimal fake QR code PNG (random bytes base64-encoded).
	qrBytes := make([]byte, mfaQRCodeBytes)
	_, _ = rand.Read(qrBytes)
	qrPNG := base64.StdEncoding.EncodeToString(qrBytes)

	device := VirtualMFADevice{
		SerialNumber:         serialNumber,
		VirtualMFADeviceName: virtualMFADeviceName,
		Path:                 p,
		CreateDate:           time.Now().UTC(),
		Status:               MFAStatusNotAssigned,
		Base32StringSeed:     base32Seed,
		QRCodePNG:            qrPNG,
	}

	b.virtualMFADevices[serialNumber] = device

	return &device, nil
}

// ResetServiceSpecificCredentialFull resets a service-specific credential (regenerates password).
func (b *InMemoryBackend) ResetServiceSpecificCredentialFull(
	userName, credentialID string,
) (*ServiceSpecificCredential, error) {
	b.mu.Lock("ResetServiceSpecificCredential")
	defer b.mu.Unlock()

	if _, exists := b.users[userName]; !exists {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	cred, exists := b.serviceSpecificCreds[credentialID]
	if !exists || cred.UserName != userName {
		return nil, fmt.Errorf("%w: service-specific credential %q not found", ErrPolicyNotFound, credentialID)
	}

	// Generate a new password.
	cred.ServicePassword = newID("") + newID("")
	b.serviceSpecificCreds[credentialID] = cred

	return &cred, nil
}

// OIDCProviderExists reports whether an OIDC provider with the given issuer URL exists.
// The issuer URL may or may not have a trailing slash; both forms are checked.
// This method implements the sts.OIDCLookup interface.
func (b *InMemoryBackend) OIDCProviderExists(issuerURL string) bool {
	b.mu.RLock("OIDCProviderExists")
	defer b.mu.RUnlock()

	// Normalise the issuer URL to strip trailing slashes for comparison.
	normalised := strings.TrimRight(issuerURL, "/")

	for _, p := range b.oidcProviders {
		providerURL := strings.TrimRight(p.URL, "/")
		if providerURL == normalised {
			return true
		}
	}

	return false
}

// ResetComprehensiveBackend clears all comprehensive backend state.
// Called from InMemoryBackend.Reset().
func (b *InMemoryBackend) ResetComprehensiveBackend() {
	c := b.comp()
	c.mu.Lock()
	defer c.mu.Unlock()

	c.sshPublicKeys = make(map[string]SSHPublicKey)
	c.mfaUserLinks = make(map[string]string)
	c.accessAdvisorJobs = make(map[string]*accessAdvisorJob)
	c.serviceLastAccessed = make(map[string]map[string]ServiceLastAccessedDetail)
	c.orgReportJobs = make(map[string]time.Time)
}

// ---- Organizations Access Report ----

// GenerateOrganizationsAccessReport creates a new org access report job and returns its ID.
func (b *InMemoryBackend) GenerateOrganizationsAccessReport(_ string) string {
	jobID := "orgjob-" + newID("")
	now := time.Now().UTC()

	c := b.comp()
	c.mu.Lock()
	c.orgReportJobs[jobID] = now
	c.mu.Unlock()

	return jobID
}

// GetOrganizationsAccessReport retrieves the status of an org access report job.
func (b *InMemoryBackend) GetOrganizationsAccessReport(jobID string) (string, time.Time, bool) {
	c := b.comp()
	c.mu.Lock()
	createdAt, found := c.orgReportJobs[jobID]
	c.mu.Unlock()

	if !found {
		return "", time.Time{}, false
	}

	return jobStatusCompleted, createdAt, true
}

func (b *InMemoryBackend) validateAuthCodes(code1, code2 string) error {
	if len(code1) != 6 || len(code2) != 6 {
		return fmt.Errorf("%w: codes must be 6 digits", ErrInvalidAuthenticationCode)
	}
	for _, c := range code1 {
		if c < '0' || c > '9' {
			return fmt.Errorf("%w: codes must be numeric", ErrInvalidAuthenticationCode)
		}
	}
	for _, c := range code2 {
		if c < '0' || c > '9' {
			return fmt.Errorf("%w: codes must be numeric", ErrInvalidAuthenticationCode)
		}
	}
	if code1 == code2 {
		return fmt.Errorf("%w: codes must be distinct", ErrInvalidAuthenticationCode)
	}

	return nil
}
