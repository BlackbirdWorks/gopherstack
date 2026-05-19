package iam

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
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
	UserName         string    `json:"UserName"`
	SSHPublicKeyID   string    `json:"SSHPublicKeyId"`
	SSHPublicKeyBody string    `json:"SSHPublicKeyBody"`
	Fingerprint      string    `json:"Fingerprint"`
	Status           string    `json:"Status"`
}

// ---- Access Advisor ----

// ServiceLastAccessedDetail tracks when a service was last accessed.
type ServiceLastAccessedDetail struct {
	ServiceName                string    `json:"ServiceName"`
	ServiceNamespace           string    `json:"ServiceNamespace"`
	LastAuthenticated          time.Time `json:"LastAuthenticated"`
	LastAuthenticatedArn       string    `json:"LastAuthenticatedArn,omitempty"`
	TotalAuthenticatedEntities int       `json:"TotalAuthenticatedEntities"`
}

// accessAdvisorJob represents a pending/completed access advisor job.
type accessAdvisorJob struct {
	JobID     string
	EntityARN string
	CreatedAt time.Time
	Status    string // IN_PROGRESS or COMPLETED
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

	// Auth codes accepted as-is in the mock (no TOTP validation).
	_ = authCode1
	_ = authCode2

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
