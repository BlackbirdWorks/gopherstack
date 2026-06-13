package directoryservice

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrTrustNotFound is returned when a trust does not exist.
	ErrTrustNotFound = awserr.New(errEntityNotExistsException, awserr.ErrNotFound)
	// ErrCertNotFound is returned when a certificate does not exist.
	ErrCertNotFound = awserr.New(errEntityNotExistsException, awserr.ErrNotFound)
	// ErrConditionalForwarderNotFound is returned when a conditional forwarder does not exist.
	ErrConditionalForwarderNotFound = awserr.New(errEntityNotExistsException, awserr.ErrNotFound)
	// ErrSchemaExtensionNotFound is returned when a schema extension does not exist.
	ErrSchemaExtensionNotFound = awserr.New(errEntityNotExistsException, awserr.ErrNotFound)
	// ErrSharedDirectoryNotFound is returned when a shared directory does not exist.
	ErrSharedDirectoryNotFound = awserr.New(errEntityNotExistsException, awserr.ErrNotFound)
	// ErrAssessmentNotFound is returned when an AD assessment does not exist.
	ErrAssessmentNotFound = awserr.New(errEntityNotExistsException, awserr.ErrNotFound)
)

// --- Stored types for Appendix A ---

type storedIpRoute struct { //nolint:revive,staticcheck // existing issue.
	AddedDateTime time.Time `json:"addedDateTime"`
	DirectoryID   string    `json:"directoryId"`
	CidrIP        string    `json:"cidrIp"`
	Description   string    `json:"description"`
	IPRouteStatus string    `json:"ipRouteStatus"`
}

type storedRegion struct {
	LaunchTime  time.Time `json:"launchTime"`
	DirectoryID string    `json:"directoryId"`
	RegionName  string    `json:"regionName"`
	RegionType  string    `json:"regionType"`
	Status      string    `json:"status"`
}

type storedSchemaExtension struct {
	StartTime   time.Time `json:"startTime"`
	EndTime     time.Time `json:"endTime"`
	ExtensionID string    `json:"extensionId"`
	DirectoryID string    `json:"directoryId"`
	Description string    `json:"description"`
	Status      string    `json:"status"`
}

type storedConditionalForwarder struct {
	DirectoryID      string   `json:"directoryId"`
	RemoteDomainName string   `json:"remoteDomainName"`
	ReplicationScope string   `json:"replicationScope"`
	DNSIPAddrs       []string `json:"dnsIpAddrs"`
}

type storedLogSubscription struct {
	SubscriptionCreatedDateTime time.Time `json:"subscriptionCreatedDateTime"`
	DirectoryID                 string    `json:"directoryId"`
	LogGroupName                string    `json:"logGroupName"`
}

type storedEventTopic struct {
	CreatedDateTime time.Time `json:"createdDateTime"`
	DirectoryID     string    `json:"directoryId"`
	TopicName       string    `json:"topicName"`
	TopicARN        string    `json:"topicArn"`
	Status          string    `json:"status"`
}

type storedDomainController struct {
	LaunchTime       time.Time `json:"launchTime"`
	ControllerID     string    `json:"controllerId"`
	DirectoryID      string    `json:"directoryId"`
	Status           string    `json:"status"`
	AvailabilityZone string    `json:"availabilityZone"`
}

type storedTrust struct {
	CreatedDateTime      time.Time `json:"createdDateTime"`
	LastUpdatedDateTime  time.Time `json:"lastUpdatedDateTime"`
	StateLastUpdatedTime time.Time `json:"stateLastUpdatedDateTime"`
	TrustID              string    `json:"trustId"`
	DirectoryID          string    `json:"directoryId"`
	RemoteDomainName     string    `json:"remoteDomainName"`
	TrustDirection       string    `json:"trustDirection"`
	TrustType            string    `json:"trustType"`
	TrustState           string    `json:"trustState"`
	SelectiveAuth        string    `json:"selectiveAuth"`
	TrustStateReason     string    `json:"trustStateReason"`
}

type storedSharedDirectory struct {
	CreatedDateTime     time.Time `json:"createdDateTime"`
	LastUpdatedDateTime time.Time `json:"lastUpdatedDateTime"`
	SharedDirectoryID   string    `json:"sharedDirectoryId"`
	OwnerDirectoryID    string    `json:"ownerDirectoryId"`
	OwnerAccountID      string    `json:"ownerAccountId"`
	SharedAccountID     string    `json:"sharedAccountId"`
	ShareMethod         string    `json:"shareMethod"`
	ShareStatus         string    `json:"shareStatus"`
	ShareNotes          string    `json:"shareNotes"`
}

type storedCertificate struct {
	RegisteredDateTime time.Time `json:"registeredDateTime"`
	ExpiryDateTime     time.Time `json:"expiryDateTime"`
	CertificateID      string    `json:"certificateId"`
	DirectoryID        string    `json:"directoryId"`
	CertData           string    `json:"certData"`
	CommonName         string    `json:"commonName"`
	CertType           string    `json:"certType"`
	State              string    `json:"state"`
}

type storedLDAPSSetting struct {
	LastUpdatedDateTime       time.Time `json:"lastUpdatedDateTime"`
	CertificateExpiryDateTime time.Time `json:"certificateExpiryDateTime"`
	DirectoryID               string    `json:"directoryId"`
	LDAPSType                 string    `json:"ldapsType"`
	CertificateID             string    `json:"certificateId"`
	State                     string    `json:"state"`
}

type storedClientAuthSetting struct {
	LastUpdatedDateTime time.Time `json:"lastUpdatedDateTime"`
	DirectoryID         string    `json:"directoryId"`
	AuthType            string    `json:"authType"`
	Status              string    `json:"status"`
}

type storedRadiusSettings struct {
	DirectoryID            string   `json:"directoryId"`
	AuthenticationProtocol string   `json:"authenticationProtocol"`
	DisplayLabel           string   `json:"displayLabel"`
	SharedSecret           string   `json:"sharedSecret"`
	RadiusServers          []string `json:"radiusServers"`
	RadiusPort             int32    `json:"radiusPort"`
	RadiusRetries          int32    `json:"radiusRetries"`
	RadiusTimeout          int32    `json:"radiusTimeout"`
	UseSameUsername        bool     `json:"useSameUsername"`
}

type storedADAssessment struct {
	StartTime    time.Time `json:"startTime"`
	AssessmentID string    `json:"assessmentId"`
	DirectoryID  string    `json:"directoryId"`
	Status       string    `json:"status"`
	AssessType   string    `json:"assessmentType"`
	Region       string    `json:"region"`
}

type storedDirectorySetting struct {
	LastUpdatedDateTime time.Time `json:"lastUpdatedDateTime"`
	DirectoryID         string    `json:"directoryId"`
	Name                string    `json:"name"`
	AllowedValues       string    `json:"allowedValues"`
	AppliedValue        string    `json:"appliedValue"`
	RequestedValue      string    `json:"requestedValue"`
	Status              string    `json:"status"`
}

type storedUpdateInfo struct {
	StartTime           time.Time `json:"startTime"`
	LastUpdatedDateTime time.Time `json:"lastUpdatedDateTime"`
	DirectoryID         string    `json:"directoryId"`
	UpdateType          string    `json:"updateType"`
	Status              string    `json:"status"`
	NewValue            string    `json:"newValue"`
	PreviousValue       string    `json:"previousValue"`
	InitiatedBy         string    `json:"initiatedBy"`
	Region              string    `json:"region"`
}

type storedHybridADUpdate struct {
	RequestID   string `json:"requestId"`
	DirectoryID string `json:"directoryId"`
	Status      string `json:"status"`
}

// --- Backend state additions (new maps initialized in NewInMemoryBackend) ---

// IpRoute domain type.
type IpRoute struct { //nolint:revive,staticcheck // existing issue.
	DirectoryID string
	CidrIP      string
	Description string
	AddedTime   time.Time
	Status      string
}

// RegionDescription domain type.
type RegionDescription struct {
	LaunchTime  time.Time
	DirectoryID string
	RegionName  string
	RegionType  string
	Status      string
}

// SchemaExtension domain type.
type SchemaExtension struct {
	StartTime   time.Time
	EndTime     time.Time
	ExtensionID string
	DirectoryID string
	Description string
	Status      string
}

// ConditionalForwarder domain type.
type ConditionalForwarder struct {
	DirectoryID      string
	RemoteDomainName string
	ReplicationScope string
	DNSIPAddrs       []string
}

// LogSubscription domain type.
type LogSubscription struct {
	CreatedTime  time.Time
	DirectoryID  string
	LogGroupName string
}

// EventTopic domain type.
type EventTopic struct {
	CreatedDateTime time.Time
	DirectoryID     string
	TopicName       string
	TopicARN        string
	Status          string
}

// DomainController domain type.
type DomainController struct {
	LaunchTime       time.Time
	ControllerID     string
	DirectoryID      string
	Status           string
	AvailabilityZone string
}

// TrustInfo domain type.
type TrustInfo struct {
	CreatedDateTime      time.Time
	LastUpdatedDateTime  time.Time
	StateLastUpdatedTime time.Time
	TrustID              string
	DirectoryID          string
	RemoteDomainName     string
	TrustDirection       string
	TrustType            string
	TrustState           string
	SelectiveAuth        string
	TrustStateReason     string
}

// SharedDirInfo domain type.
type SharedDirInfo struct {
	CreatedDateTime     time.Time
	LastUpdatedDateTime time.Time
	SharedDirectoryID   string
	OwnerDirectoryID    string
	OwnerAccountID      string
	SharedAccountID     string
	ShareMethod         string
	ShareStatus         string
	ShareNotes          string
}

// CertInfo domain type (for list).
type CertInfo struct {
	ExpiryDateTime time.Time
	CertificateID  string
	CommonName     string
	CertType       string
	State          string
}

// CertDetail domain type (for describe).
type CertDetail struct {
	RegisteredDateTime time.Time
	ExpiryDateTime     time.Time
	CertificateID      string
	DirectoryID        string
	CommonName         string
	CertType           string
	State              string
	CertData           string
}

// LDAPSSetting domain type.
type LDAPSSetting struct {
	LastUpdatedDateTime       time.Time
	CertificateExpiryDateTime time.Time
	DirectoryID               string
	LDAPSType                 string
	CertificateID             string
	State                     string
}

// ClientAuthInfo domain type.
type ClientAuthInfo struct {
	LastUpdatedDateTime time.Time
	DirectoryID         string
	AuthType            string
	Status              string
}

// RadiusSettingsInput is used for Enable/Update RADIUS.
type RadiusSettingsInput struct {
	AuthenticationProtocol string
	DisplayLabel           string
	SharedSecret           string
	RadiusServers          []string
	RadiusPort             int32
	RadiusRetries          int32
	RadiusTimeout          int32
	UseSameUsername        bool
}

// RadiusInfo is returned by describe (currently inferred from directory).
type RadiusInfo struct {
	DirectoryID            string
	AuthenticationProtocol string
	RadiusServers          []string
	RadiusPort             int32
	RadiusRetries          int32
	RadiusTimeout          int32
	UseSameUsername        bool
}

// DirectoryDataAccessStatus domain type.
type DirectoryDataAccessStatus struct {
	DirectoryID string
	Enabled     bool
}

// CAEnrollmentPolicy domain type.
type CAEnrollmentPolicy struct {
	DirectoryID string
	Enabled     bool
}

// ADAssessmentInfo domain type.
type ADAssessmentInfo struct {
	StartTime    time.Time
	AssessmentID string
	DirectoryID  string
	Status       string
	AssessType   string
	Region       string
}

// DirectorySetting domain type.
type DirectorySetting struct {
	Name          string
	Value         string
	AllowedValues string
}

// SettingEntry domain type.
type SettingEntry struct {
	LastUpdatedDateTime time.Time
	DirectoryID         string
	Name                string
	AllowedValues       string
	AppliedValue        string
	RequestedValue      string
	Status              string
}

// UpdateInfoEntry domain type.
type UpdateInfoEntry struct {
	StartTime           time.Time
	LastUpdatedDateTime time.Time
	DirectoryID         string
	UpdateType          string
	Status              string
	NewValue            string
	PreviousValue       string
	InitiatedBy         string
	Region              string
}

// ComputerInfo is returned by CreateComputer.
type ComputerInfo struct {
	ComputerID   string
	ComputerName string
}

// HybridADUpdateEntry domain type.
type HybridADUpdateEntry struct {
	RequestID   string
	DirectoryID string
	Status      string
}

// --- IP Routes ---

// AddIpRoutes adds CIDR IP routes to a directory.
func (b *InMemoryBackend) AddIpRoutes( //nolint:revive,staticcheck // existing issue.
	directoryID string,
	routes []IpRoute,
) error {
	b.mu.Lock("AddIpRoutes")
	defer b.mu.Unlock()

	if _, ok := b.directories[directoryID]; !ok {
		return ErrDirectoryNotFound
	}

	now := time.Now().UTC()
	existing := b.ipRoutes[directoryID]
	existingSet := make(map[string]bool, len(existing))
	for _, r := range existing {
		existingSet[r.CidrIP] = true
	}

	for _, r := range routes {
		if !existingSet[r.CidrIP] {
			b.ipRoutes[directoryID] = append(b.ipRoutes[directoryID], storedIpRoute{
				DirectoryID:   directoryID,
				CidrIP:        r.CidrIP,
				Description:   r.Description,
				AddedDateTime: now,
				IPRouteStatus: "Added",
			})
		}
	}

	return nil
}

// RemoveIpRoutes removes CIDR IP routes from a directory.
func (b *InMemoryBackend) RemoveIpRoutes( //nolint:revive,staticcheck // existing issue.
	directoryID string,
	cidrIPs []string,
) error {
	b.mu.Lock("RemoveIpRoutes")
	defer b.mu.Unlock()

	if _, ok := b.directories[directoryID]; !ok {
		return ErrDirectoryNotFound
	}

	remove := make(map[string]bool, len(cidrIPs))
	for _, c := range cidrIPs {
		remove[c] = true
	}

	filtered := b.ipRoutes[directoryID][:0]
	for _, r := range b.ipRoutes[directoryID] {
		if !remove[r.CidrIP] {
			filtered = append(filtered, r)
		}
	}
	b.ipRoutes[directoryID] = filtered

	return nil
}

// ListIpRoutes returns IP routes for a directory.
func (b *InMemoryBackend) ListIpRoutes( //nolint:revive,staticcheck // existing issue.
	directoryID string,
	limit int32,
	nextToken string,
) ([]IpRoute, string, error) {
	b.mu.RLock("ListIpRoutes")
	defer b.mu.RUnlock()

	if _, ok := b.directories[directoryID]; !ok {
		return nil, "", ErrDirectoryNotFound
	}

	stored := b.ipRoutes[directoryID]
	sorted := make([]storedIpRoute, len(stored))
	copy(sorted, stored)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].CidrIP < sorted[j].CidrIP })

	start := 0
	if nextToken != "" {
		for i, r := range sorted {
			if r.CidrIP == nextToken {
				start = i

				break
			}
		}
	}

	pageSize := int(limit)
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}

	end := min(start+pageSize, len(sorted))
	result := make([]IpRoute, 0, end-start)
	for _, r := range sorted[start:end] {
		result = append(result, IpRoute{
			DirectoryID: r.DirectoryID,
			CidrIP:      r.CidrIP,
			Description: r.Description,
			AddedTime:   r.AddedDateTime,
			Status:      r.IPRouteStatus,
		})
	}

	var outToken string
	if end < len(sorted) {
		outToken = sorted[end].CidrIP
	}

	return result, outToken, nil
}

// --- Regions ---

// AddRegion adds a region to a directory.
func (b *InMemoryBackend) AddRegion(directoryID, regionName string) error {
	b.mu.Lock("AddRegion")
	defer b.mu.Unlock()

	if _, ok := b.directories[directoryID]; !ok {
		return ErrDirectoryNotFound
	}

	key := directoryID + ":" + regionName
	if _, exists := b.regions[key]; exists {
		return ErrAliasAlreadyExists
	}

	b.regions[key] = &storedRegion{
		DirectoryID: directoryID,
		RegionName:  regionName,
		RegionType:  "Additional",
		Status:      "Active",
		LaunchTime:  time.Now().UTC(),
	}

	return nil
}

// RemoveRegion removes a region from a directory.
func (b *InMemoryBackend) RemoveRegion(directoryID string) error {
	b.mu.Lock("RemoveRegion")
	defer b.mu.Unlock()

	if _, ok := b.directories[directoryID]; !ok {
		return ErrDirectoryNotFound
	}

	for key, r := range b.regions {
		if r.DirectoryID == directoryID {
			delete(b.regions, key)
		}
	}

	return nil
}

// DescribeRegions returns regions for a directory.
func (b *InMemoryBackend) DescribeRegions(
	directoryID, regionName, nextToken string, //nolint:revive // existing issue.
) ([]RegionDescription, string, error) {
	b.mu.RLock("DescribeRegions")
	defer b.mu.RUnlock()

	if _, ok := b.directories[directoryID]; !ok {
		return nil, "", ErrDirectoryNotFound
	}

	var all []storedRegion
	for _, r := range b.regions {
		if r.DirectoryID != directoryID {
			continue
		}
		if regionName != "" && r.RegionName != regionName {
			continue
		}
		all = append(all, *r)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].RegionName < all[j].RegionName })

	result := make([]RegionDescription, 0, len(all))
	for _, r := range all {
		result = append(result, RegionDescription(r))
	}

	return result, "", nil
}

// --- Schema Extensions ---

// StartSchemaExtension starts a schema extension.
func (b *InMemoryBackend) StartSchemaExtension(directoryID, description, _ string) (string, error) {
	b.mu.Lock("StartSchemaExtension")
	defer b.mu.Unlock()

	if _, ok := b.directories[directoryID]; !ok {
		return "", ErrDirectoryNotFound
	}

	id := fmt.Sprintf("e-%s", uuid.NewString()[:10])
	now := time.Now().UTC()
	b.schemaExtensions[id] = &storedSchemaExtension{
		ExtensionID: id,
		DirectoryID: directoryID,
		Description: description,
		Status:      "Completed",
		StartTime:   now,
		EndTime:     now,
	}

	return id, nil
}

// CancelSchemaExtension cancels a schema extension.
func (b *InMemoryBackend) CancelSchemaExtension(directoryID, schemaExtensionID string) error {
	b.mu.Lock("CancelSchemaExtension")
	defer b.mu.Unlock()

	ext, ok := b.schemaExtensions[schemaExtensionID]
	if !ok || ext.DirectoryID != directoryID {
		return ErrSchemaExtensionNotFound
	}

	ext.Status = "CancelInProgress"

	return nil
}

// ListSchemaExtensions returns schema extensions for a directory.
func (b *InMemoryBackend) ListSchemaExtensions(
	directoryID string,
	limit int32,
	nextToken string,
) ([]SchemaExtension, string, error) {
	b.mu.RLock("ListSchemaExtensions")
	defer b.mu.RUnlock()

	if _, ok := b.directories[directoryID]; !ok {
		return nil, "", ErrDirectoryNotFound
	}

	var all []storedSchemaExtension
	for _, e := range b.schemaExtensions {
		if e.DirectoryID == directoryID {
			all = append(all, *e)
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ExtensionID < all[j].ExtensionID })

	start := 0
	if nextToken != "" {
		for i, e := range all {
			if e.ExtensionID == nextToken {
				start = i

				break
			}
		}
	}

	pageSize := int(limit)
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}

	end := min(start+pageSize, len(all))
	result := make([]SchemaExtension, 0, end-start)
	for _, e := range all[start:end] {
		result = append(result, SchemaExtension(e))
	}

	var outToken string
	if end < len(all) {
		outToken = all[end].ExtensionID
	}

	return result, outToken, nil
}

// --- Conditional Forwarders ---

// CreateConditionalForwarder creates a conditional forwarder.
func (b *InMemoryBackend) CreateConditionalForwarder(directoryID, remoteDomainName string, dnsIPAddrs []string) error {
	b.mu.Lock("CreateConditionalForwarder")
	defer b.mu.Unlock()

	if _, ok := b.directories[directoryID]; !ok {
		return ErrDirectoryNotFound
	}

	key := directoryID + ":" + remoteDomainName
	if _, exists := b.conditionalForwarders[key]; exists {
		return ErrAliasAlreadyExists
	}

	b.conditionalForwarders[key] = &storedConditionalForwarder{
		DirectoryID:      directoryID,
		RemoteDomainName: remoteDomainName,
		DNSIPAddrs:       dnsIPAddrs,
		ReplicationScope: "Domain",
	}

	return nil
}

// UpdateConditionalForwarder updates a conditional forwarder.
func (b *InMemoryBackend) UpdateConditionalForwarder(directoryID, remoteDomainName string, dnsIPAddrs []string) error {
	b.mu.Lock("UpdateConditionalForwarder")
	defer b.mu.Unlock()

	key := directoryID + ":" + remoteDomainName
	fwd, ok := b.conditionalForwarders[key]
	if !ok {
		return ErrConditionalForwarderNotFound
	}

	fwd.DNSIPAddrs = dnsIPAddrs

	return nil
}

// DeleteConditionalForwarder deletes a conditional forwarder.
func (b *InMemoryBackend) DeleteConditionalForwarder(directoryID, remoteDomainName string) error {
	b.mu.Lock("DeleteConditionalForwarder")
	defer b.mu.Unlock()

	key := directoryID + ":" + remoteDomainName
	if _, ok := b.conditionalForwarders[key]; !ok {
		return ErrConditionalForwarderNotFound
	}

	delete(b.conditionalForwarders, key)

	return nil
}

// DescribeConditionalForwarders returns conditional forwarders for a directory.
func (b *InMemoryBackend) DescribeConditionalForwarders(
	directoryID string,
	remoteDomainNames []string,
) ([]ConditionalForwarder, error) {
	b.mu.RLock("DescribeConditionalForwarders")
	defer b.mu.RUnlock()

	if _, ok := b.directories[directoryID]; !ok {
		return nil, ErrDirectoryNotFound
	}

	filterSet := make(map[string]bool, len(remoteDomainNames))
	for _, d := range remoteDomainNames {
		filterSet[d] = true
	}

	var result []ConditionalForwarder
	for _, fwd := range b.conditionalForwarders {
		if fwd.DirectoryID != directoryID {
			continue
		}
		if len(filterSet) > 0 && !filterSet[fwd.RemoteDomainName] {
			continue
		}
		cp := make([]string, len(fwd.DNSIPAddrs))
		copy(cp, fwd.DNSIPAddrs)
		result = append(result, ConditionalForwarder{
			DirectoryID:      fwd.DirectoryID,
			RemoteDomainName: fwd.RemoteDomainName,
			DNSIPAddrs:       cp,
			ReplicationScope: fwd.ReplicationScope,
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].RemoteDomainName < result[j].RemoteDomainName })

	return result, nil
}

// --- Log Subscriptions ---

// CreateLogSubscription creates a log subscription.
func (b *InMemoryBackend) CreateLogSubscription(directoryID, logGroupName string) error {
	b.mu.Lock("CreateLogSubscription")
	defer b.mu.Unlock()

	if _, ok := b.directories[directoryID]; !ok {
		return ErrDirectoryNotFound
	}

	key := directoryID + ":" + logGroupName
	if _, exists := b.logSubscriptions[key]; exists {
		return ErrAliasAlreadyExists
	}

	b.logSubscriptions[key] = &storedLogSubscription{
		DirectoryID:                 directoryID,
		LogGroupName:                logGroupName,
		SubscriptionCreatedDateTime: time.Now().UTC(),
	}

	return nil
}

// DeleteLogSubscription deletes a log subscription.
func (b *InMemoryBackend) DeleteLogSubscription(directoryID string) error {
	b.mu.Lock("DeleteLogSubscription")
	defer b.mu.Unlock()

	if _, ok := b.directories[directoryID]; !ok {
		return ErrDirectoryNotFound
	}

	for key, sub := range b.logSubscriptions {
		if sub.DirectoryID == directoryID {
			delete(b.logSubscriptions, key)
		}
	}

	return nil
}

// ListLogSubscriptions returns log subscriptions.
func (b *InMemoryBackend) ListLogSubscriptions(
	directoryID string,
	limit int32,
	nextToken string, //nolint:revive // existing issue.
) ([]LogSubscription, string, error) {
	b.mu.RLock("ListLogSubscriptions")
	defer b.mu.RUnlock()

	var all []storedLogSubscription
	for _, sub := range b.logSubscriptions {
		if directoryID != "" && sub.DirectoryID != directoryID {
			continue
		}
		all = append(all, *sub)
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].DirectoryID == all[j].DirectoryID {
			return all[i].LogGroupName < all[j].LogGroupName
		}

		return all[i].DirectoryID < all[j].DirectoryID
	})

	start := 0
	pageSize := int(limit)
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}

	end := min(start+pageSize, len(all))
	result := make([]LogSubscription, 0, end-start)
	for _, sub := range all[start:end] {
		result = append(result, LogSubscription{
			DirectoryID:  sub.DirectoryID,
			LogGroupName: sub.LogGroupName,
			CreatedTime:  sub.SubscriptionCreatedDateTime,
		})
	}

	var outToken string
	if end < len(all) {
		outToken = all[end].DirectoryID + ":" + all[end].LogGroupName
	}

	return result, outToken, nil
}

// --- Event Topics ---

// RegisterEventTopic registers an event topic.
func (b *InMemoryBackend) RegisterEventTopic(directoryID, topicName string) error {
	b.mu.Lock("RegisterEventTopic")
	defer b.mu.Unlock()

	if _, ok := b.directories[directoryID]; !ok {
		return ErrDirectoryNotFound
	}

	key := directoryID + ":" + topicName
	if _, exists := b.eventTopics[key]; exists {
		return ErrAliasAlreadyExists
	}

	b.eventTopics[key] = &storedEventTopic{
		DirectoryID:     directoryID,
		TopicName:       topicName,
		TopicARN:        fmt.Sprintf("arn:aws:sns:%s:%s:%s", b.region, b.accountID, topicName),
		Status:          "Registered",
		CreatedDateTime: time.Now().UTC(),
	}

	return nil
}

// DeregisterEventTopic deregisters an event topic.
func (b *InMemoryBackend) DeregisterEventTopic(directoryID, topicName string) error {
	b.mu.Lock("DeregisterEventTopic")
	defer b.mu.Unlock()

	key := directoryID + ":" + topicName
	if _, ok := b.eventTopics[key]; !ok {
		return ErrDirectoryNotFound
	}

	delete(b.eventTopics, key)

	return nil
}

// DescribeEventTopics returns event topics for a directory.
func (b *InMemoryBackend) DescribeEventTopics(directoryID string, topicNames []string) ([]EventTopic, error) {
	b.mu.RLock("DescribeEventTopics")
	defer b.mu.RUnlock()

	filterSet := make(map[string]bool, len(topicNames))
	for _, n := range topicNames {
		filterSet[n] = true
	}

	var result []EventTopic
	for _, topic := range b.eventTopics {
		if directoryID != "" && topic.DirectoryID != directoryID {
			continue
		}
		if len(filterSet) > 0 && !filterSet[topic.TopicName] {
			continue
		}
		result = append(result, EventTopic{
			DirectoryID:     topic.DirectoryID,
			TopicName:       topic.TopicName,
			TopicARN:        topic.TopicARN,
			Status:          topic.Status,
			CreatedDateTime: topic.CreatedDateTime,
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].TopicName < result[j].TopicName })

	return result, nil
}

// --- Domain Controllers ---

// DescribeDomainControllers returns domain controllers for a directory.
func (b *InMemoryBackend) DescribeDomainControllers(
	directoryID string,
	domainControllerIDs []string,
	limit int32,
	nextToken string,
) ([]DomainController, string, error) {
	b.mu.RLock("DescribeDomainControllers")
	defer b.mu.RUnlock()

	if _, ok := b.directories[directoryID]; !ok {
		return nil, "", ErrDirectoryNotFound
	}

	filterSet := make(map[string]bool, len(domainControllerIDs))
	for _, id := range domainControllerIDs {
		filterSet[id] = true
	}

	var ids []string
	for id, dc := range b.domainControllers {
		if dc.DirectoryID != directoryID {
			continue
		}
		if len(filterSet) > 0 && !filterSet[id] {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)

	start := 0
	if nextToken != "" {
		for i, id := range ids {
			if id == nextToken {
				start = i

				break
			}
		}
	}

	pageSize := int(limit)
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}

	end := min(start+pageSize, len(ids))
	result := make([]DomainController, 0, end-start)
	for _, id := range ids[start:end] {
		dc := b.domainControllers[id]
		result = append(result, DomainController{
			ControllerID:     dc.ControllerID,
			DirectoryID:      dc.DirectoryID,
			Status:           dc.Status,
			AvailabilityZone: dc.AvailabilityZone,
			LaunchTime:       dc.LaunchTime,
		})
	}

	var outToken string
	if end < len(ids) {
		outToken = ids[end]
	}

	return result, outToken, nil
}

// UpdateNumberOfDomainControllers sets the desired domain controller count.
func (b *InMemoryBackend) UpdateNumberOfDomainControllers(directoryID string, desiredNumber int32) error {
	b.mu.Lock("UpdateNumberOfDomainControllers")
	defer b.mu.Unlock()

	if _, ok := b.directories[directoryID]; !ok {
		return ErrDirectoryNotFound
	}

	// Count current controllers.
	var current int32
	for _, dc := range b.domainControllers {
		if dc.DirectoryID == directoryID {
			current++
		}
	}

	// Add controllers if desired > current.
	for i := current; i < desiredNumber; i++ {
		id := fmt.Sprintf("dc-%s", uuid.NewString()[:10])
		b.domainControllers[id] = &storedDomainController{
			ControllerID:     id,
			DirectoryID:      directoryID,
			Status:           "Active",
			AvailabilityZone: "us-east-1a",
			LaunchTime:       time.Now().UTC(),
		}
	}

	// Remove controllers if desired < current.
	if desiredNumber < current {
		var toRemove []string
		for id, dc := range b.domainControllers {
			if dc.DirectoryID == directoryID {
				toRemove = append(toRemove, id)
			}
		}
		sort.Strings(toRemove)
		for i := int32(len(toRemove)) - 1; i >= desiredNumber; i-- { //nolint:gosec // existing issue.
			delete(b.domainControllers, toRemove[i])
		}
	}

	return nil
}

// --- Trusts ---

// CreateTrust creates a trust relationship.
func (b *InMemoryBackend) CreateTrust(
	directoryID, remoteDomainName, _, trustDirection, trustType string,
) (string, error) {
	b.mu.Lock("CreateTrust")
	defer b.mu.Unlock()

	if _, ok := b.directories[directoryID]; !ok {
		return "", ErrDirectoryNotFound
	}

	id := fmt.Sprintf("t-%s", uuid.NewString()[:10])
	now := time.Now().UTC()
	b.trusts[id] = &storedTrust{
		TrustID:              id,
		DirectoryID:          directoryID,
		RemoteDomainName:     remoteDomainName,
		TrustDirection:       trustDirection,
		TrustType:            trustType,
		TrustState:           "Created",
		SelectiveAuth:        "Disabled", //nolint:goconst // existing issue.
		CreatedDateTime:      now,
		LastUpdatedDateTime:  now,
		StateLastUpdatedTime: now,
	}

	return id, nil
}

// DeleteTrust deletes a trust relationship.
func (b *InMemoryBackend) DeleteTrust(trustID string) (string, error) {
	b.mu.Lock("DeleteTrust")
	defer b.mu.Unlock()

	if _, ok := b.trusts[trustID]; !ok {
		return "", ErrTrustNotFound
	}

	delete(b.trusts, trustID)

	return trustID, nil
}

// DescribeTrusts returns trusts for a directory.
func (b *InMemoryBackend) DescribeTrusts(
	directoryID string,
	trustIDs []string,
	limit int32,
	nextToken string,
) ([]TrustInfo, string, error) {
	b.mu.RLock("DescribeTrusts")
	defer b.mu.RUnlock()

	filterSet := make(map[string]bool, len(trustIDs))
	for _, id := range trustIDs {
		filterSet[id] = true
	}

	var ids []string
	for id, t := range b.trusts {
		if directoryID != "" && t.DirectoryID != directoryID {
			continue
		}
		if len(filterSet) > 0 && !filterSet[id] {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)

	start := 0
	if nextToken != "" {
		for i, id := range ids {
			if id == nextToken {
				start = i

				break
			}
		}
	}

	pageSize := int(limit)
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}

	end := min(start+pageSize, len(ids))
	result := make([]TrustInfo, 0, end-start)
	for _, id := range ids[start:end] {
		t := b.trusts[id]
		result = append(result, TrustInfo{
			TrustID:              t.TrustID,
			DirectoryID:          t.DirectoryID,
			RemoteDomainName:     t.RemoteDomainName,
			TrustDirection:       t.TrustDirection,
			TrustType:            t.TrustType,
			TrustState:           t.TrustState,
			SelectiveAuth:        t.SelectiveAuth,
			TrustStateReason:     t.TrustStateReason,
			CreatedDateTime:      t.CreatedDateTime,
			LastUpdatedDateTime:  t.LastUpdatedDateTime,
			StateLastUpdatedTime: t.StateLastUpdatedTime,
		})
	}

	var outToken string
	if end < len(ids) {
		outToken = ids[end]
	}

	return result, outToken, nil
}

// UpdateTrust updates a trust relationship.
func (b *InMemoryBackend) UpdateTrust(trustID, selectiveAuth string) (string, error) {
	b.mu.Lock("UpdateTrust")
	defer b.mu.Unlock()

	t, ok := b.trusts[trustID]
	if !ok {
		return "", ErrTrustNotFound
	}

	if selectiveAuth != "" {
		t.SelectiveAuth = selectiveAuth
	}
	t.LastUpdatedDateTime = time.Now().UTC()

	return trustID, nil
}

// VerifyTrust verifies a trust relationship.
func (b *InMemoryBackend) VerifyTrust(trustID string) (string, error) {
	b.mu.Lock("VerifyTrust")
	defer b.mu.Unlock()

	t, ok := b.trusts[trustID]
	if !ok {
		return "", ErrTrustNotFound
	}

	t.TrustState = "Verified"
	t.LastUpdatedDateTime = time.Now().UTC()
	t.StateLastUpdatedTime = time.Now().UTC()

	return trustID, nil
}

// --- Shared Directories ---

// ShareDirectory shares a directory.
func (b *InMemoryBackend) ShareDirectory(directoryID, shareMethod, shareNotes, targetID string) (string, error) {
	b.mu.Lock("ShareDirectory")
	defer b.mu.Unlock()

	if _, ok := b.directories[directoryID]; !ok {
		return "", ErrDirectoryNotFound
	}

	id := fmt.Sprintf("d-%s", uuid.NewString()[:10])
	now := time.Now().UTC()
	b.sharedDirectories[id] = &storedSharedDirectory{
		SharedDirectoryID:   id,
		OwnerDirectoryID:    directoryID,
		OwnerAccountID:      b.accountID,
		SharedAccountID:     targetID,
		ShareMethod:         shareMethod,
		ShareStatus:         "Shared",
		ShareNotes:          shareNotes,
		CreatedDateTime:     now,
		LastUpdatedDateTime: now,
	}

	return id, nil
}

// UnshareDirectory unshares a directory.
func (b *InMemoryBackend) UnshareDirectory(directoryID, targetID string) (string, error) {
	b.mu.Lock("UnshareDirectory")
	defer b.mu.Unlock()

	for id, sd := range b.sharedDirectories {
		if sd.OwnerDirectoryID == directoryID && sd.SharedAccountID == targetID {
			sd.ShareStatus = "Deleted"
			sd.LastUpdatedDateTime = time.Now().UTC()

			return id, nil
		}
	}

	return "", ErrSharedDirectoryNotFound
}

// AcceptSharedDirectory accepts a shared directory.
func (b *InMemoryBackend) AcceptSharedDirectory(sharedDirectoryID string) (string, error) {
	b.mu.Lock("AcceptSharedDirectory")
	defer b.mu.Unlock()

	sd, ok := b.sharedDirectories[sharedDirectoryID]
	if !ok {
		return "", ErrSharedDirectoryNotFound
	}

	sd.ShareStatus = "Shared"
	sd.LastUpdatedDateTime = time.Now().UTC()

	return sharedDirectoryID, nil
}

// RejectSharedDirectory rejects a shared directory.
func (b *InMemoryBackend) RejectSharedDirectory(sharedDirectoryID string) (string, error) {
	b.mu.Lock("RejectSharedDirectory")
	defer b.mu.Unlock()

	sd, ok := b.sharedDirectories[sharedDirectoryID]
	if !ok {
		return "", ErrSharedDirectoryNotFound
	}

	sd.ShareStatus = "RejectFailed"
	sd.LastUpdatedDateTime = time.Now().UTC()

	return sharedDirectoryID, nil
}

// DescribeSharedDirectories returns shared directories for an owner directory.
func (b *InMemoryBackend) DescribeSharedDirectories(
	ownerDirID string,
	sharedDirIDs []string,
	limit int32,
	nextToken string,
) ([]SharedDirInfo, string, error) {
	b.mu.RLock("DescribeSharedDirectories")
	defer b.mu.RUnlock()

	filterSet := make(map[string]bool, len(sharedDirIDs))
	for _, id := range sharedDirIDs {
		filterSet[id] = true
	}

	var ids []string
	for id, sd := range b.sharedDirectories {
		if ownerDirID != "" && sd.OwnerDirectoryID != ownerDirID {
			continue
		}
		if len(filterSet) > 0 && !filterSet[id] {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)

	start := 0
	if nextToken != "" {
		for i, id := range ids {
			if id == nextToken {
				start = i

				break
			}
		}
	}

	pageSize := int(limit)
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}

	end := min(start+pageSize, len(ids))
	result := make([]SharedDirInfo, 0, end-start)
	for _, id := range ids[start:end] {
		sd := b.sharedDirectories[id]
		result = append(result, SharedDirInfo{
			SharedDirectoryID:   sd.SharedDirectoryID,
			OwnerDirectoryID:    sd.OwnerDirectoryID,
			OwnerAccountID:      sd.OwnerAccountID,
			SharedAccountID:     sd.SharedAccountID,
			ShareMethod:         sd.ShareMethod,
			ShareStatus:         sd.ShareStatus,
			ShareNotes:          sd.ShareNotes,
			CreatedDateTime:     sd.CreatedDateTime,
			LastUpdatedDateTime: sd.LastUpdatedDateTime,
		})
	}

	var outToken string
	if end < len(ids) {
		outToken = ids[end]
	}

	return result, outToken, nil
}

// --- Certificates ---

// RegisterCertificate registers a certificate.
func (b *InMemoryBackend) RegisterCertificate(directoryID, certData, certType string) (string, error) {
	b.mu.Lock("RegisterCertificate")
	defer b.mu.Unlock()

	if _, ok := b.directories[directoryID]; !ok {
		return "", ErrDirectoryNotFound
	}

	id := fmt.Sprintf("c-%s", uuid.NewString()[:10])
	now := time.Now().UTC()
	b.certificates[id] = &storedCertificate{
		CertificateID:      id,
		DirectoryID:        directoryID,
		CertData:           certData,
		CommonName:         "example.com",
		CertType:           certType,
		State:              "Registered",
		RegisteredDateTime: now,
		ExpiryDateTime:     now.Add(365 * 24 * time.Hour),
	}

	return id, nil
}

// DeregisterCertificate deregisters a certificate.
func (b *InMemoryBackend) DeregisterCertificate(directoryID, certID string) error {
	b.mu.Lock("DeregisterCertificate")
	defer b.mu.Unlock()

	cert, ok := b.certificates[certID]
	if !ok || cert.DirectoryID != directoryID {
		return ErrCertNotFound
	}

	delete(b.certificates, certID)

	return nil
}

// ListCertificates returns certificates for a directory.
func (b *InMemoryBackend) ListCertificates(
	directoryID string,
	limit int32,
	nextToken string,
) ([]CertInfo, string, error) {
	b.mu.RLock("ListCertificates")
	defer b.mu.RUnlock()

	if _, ok := b.directories[directoryID]; !ok {
		return nil, "", ErrDirectoryNotFound
	}

	var ids []string
	for id, cert := range b.certificates {
		if cert.DirectoryID == directoryID {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)

	start := 0
	if nextToken != "" {
		for i, id := range ids {
			if id == nextToken {
				start = i

				break
			}
		}
	}

	pageSize := int(limit)
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}

	end := min(start+pageSize, len(ids))
	result := make([]CertInfo, 0, end-start)
	for _, id := range ids[start:end] {
		cert := b.certificates[id]
		result = append(result, CertInfo{
			CertificateID:  cert.CertificateID,
			CommonName:     cert.CommonName,
			CertType:       cert.CertType,
			State:          cert.State,
			ExpiryDateTime: cert.ExpiryDateTime,
		})
	}

	var outToken string
	if end < len(ids) {
		outToken = ids[end]
	}

	return result, outToken, nil
}

// DescribeCertificate returns details of a certificate.
func (b *InMemoryBackend) DescribeCertificate(directoryID, certID string) (*CertDetail, error) {
	b.mu.RLock("DescribeCertificate")
	defer b.mu.RUnlock()

	cert, ok := b.certificates[certID]
	if !ok || cert.DirectoryID != directoryID {
		return nil, ErrCertNotFound
	}

	return &CertDetail{
		CertificateID:      cert.CertificateID,
		DirectoryID:        cert.DirectoryID,
		CommonName:         cert.CommonName,
		CertType:           cert.CertType,
		State:              cert.State,
		CertData:           cert.CertData,
		RegisteredDateTime: cert.RegisteredDateTime,
		ExpiryDateTime:     cert.ExpiryDateTime,
	}, nil
}

// --- LDAPS ---

// EnableLDAPS enables LDAPS for a directory.
func (b *InMemoryBackend) EnableLDAPS(directoryID, ldapsType string) error {
	b.mu.Lock("EnableLDAPS")
	defer b.mu.Unlock()

	if _, ok := b.directories[directoryID]; !ok {
		return ErrDirectoryNotFound
	}

	key := directoryID + ":" + ldapsType
	now := time.Now().UTC()
	if existing, ok := b.ldapsSettings[key]; ok {
		existing.State = "Enabled" //nolint:goconst // existing issue.
		existing.LastUpdatedDateTime = now
	} else {
		b.ldapsSettings[key] = &storedLDAPSSetting{
			DirectoryID:               directoryID,
			LDAPSType:                 ldapsType,
			State:                     "Enabled",
			LastUpdatedDateTime:       now,
			CertificateExpiryDateTime: now.Add(365 * 24 * time.Hour),
		}
	}

	return nil
}

// DisableLDAPS disables LDAPS for a directory.
func (b *InMemoryBackend) DisableLDAPS(directoryID, ldapsType string) error {
	b.mu.Lock("DisableLDAPS")
	defer b.mu.Unlock()

	if _, ok := b.directories[directoryID]; !ok {
		return ErrDirectoryNotFound
	}

	key := directoryID + ":" + ldapsType
	if setting, ok := b.ldapsSettings[key]; ok {
		setting.State = "Disabled"
		setting.LastUpdatedDateTime = time.Now().UTC()
	}

	return nil
}

// DescribeLDAPSSettings returns LDAPS settings for a directory.
func (b *InMemoryBackend) DescribeLDAPSSettings(
	directoryID, ldapsType string,
	limit int32, //nolint:revive // existing issue.
	nextToken string, //nolint:revive // existing issue.
) ([]LDAPSSetting, string, error) {
	b.mu.RLock("DescribeLDAPSSettings")
	defer b.mu.RUnlock()

	if _, ok := b.directories[directoryID]; !ok {
		return nil, "", ErrDirectoryNotFound
	}

	var result []LDAPSSetting
	for _, s := range b.ldapsSettings {
		if s.DirectoryID != directoryID {
			continue
		}
		if ldapsType != "" && s.LDAPSType != ldapsType {
			continue
		}
		result = append(result, LDAPSSetting{
			DirectoryID:               s.DirectoryID,
			LDAPSType:                 s.LDAPSType,
			CertificateID:             s.CertificateID,
			State:                     s.State,
			LastUpdatedDateTime:       s.LastUpdatedDateTime,
			CertificateExpiryDateTime: s.CertificateExpiryDateTime,
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].LDAPSType < result[j].LDAPSType })

	return result, "", nil
}

// --- Client Authentication ---

// EnableClientAuthentication enables client authentication.
func (b *InMemoryBackend) EnableClientAuthentication(directoryID, authType string) error {
	b.mu.Lock("EnableClientAuthentication")
	defer b.mu.Unlock()

	if _, ok := b.directories[directoryID]; !ok {
		return ErrDirectoryNotFound
	}

	key := directoryID + ":" + authType
	now := time.Now().UTC()
	if existing, ok := b.clientAuthSettings[key]; ok {
		existing.Status = "Enabled"
		existing.LastUpdatedDateTime = now
	} else {
		b.clientAuthSettings[key] = &storedClientAuthSetting{
			DirectoryID:         directoryID,
			AuthType:            authType,
			Status:              "Enabled",
			LastUpdatedDateTime: now,
		}
	}

	return nil
}

// DisableClientAuthentication disables client authentication.
func (b *InMemoryBackend) DisableClientAuthentication(directoryID, authType string) error {
	b.mu.Lock("DisableClientAuthentication")
	defer b.mu.Unlock()

	if _, ok := b.directories[directoryID]; !ok {
		return ErrDirectoryNotFound
	}

	key := directoryID + ":" + authType
	now := time.Now().UTC()
	if existing, ok := b.clientAuthSettings[key]; ok {
		existing.Status = "Disabled"
		existing.LastUpdatedDateTime = now
	} else {
		b.clientAuthSettings[key] = &storedClientAuthSetting{
			DirectoryID:         directoryID,
			AuthType:            authType,
			Status:              "Disabled",
			LastUpdatedDateTime: now,
		}
	}

	return nil
}

// DescribeClientAuthenticationSettings returns client auth settings.
func (b *InMemoryBackend) DescribeClientAuthenticationSettings(
	directoryID, authType string,
	limit int32, //nolint:revive // existing issue.
	nextToken string, //nolint:revive // existing issue.
) ([]ClientAuthInfo, string, error) {
	b.mu.RLock("DescribeClientAuthenticationSettings")
	defer b.mu.RUnlock()

	if _, ok := b.directories[directoryID]; !ok {
		return nil, "", ErrDirectoryNotFound
	}

	var result []ClientAuthInfo
	for _, s := range b.clientAuthSettings {
		if s.DirectoryID != directoryID {
			continue
		}
		if authType != "" && s.AuthType != authType {
			continue
		}
		result = append(result, ClientAuthInfo{
			DirectoryID:         s.DirectoryID,
			AuthType:            s.AuthType,
			Status:              s.Status,
			LastUpdatedDateTime: s.LastUpdatedDateTime,
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].AuthType < result[j].AuthType })

	return result, "", nil
}

// --- RADIUS ---

// EnableRadius enables RADIUS for a directory.
func (b *InMemoryBackend) EnableRadius(directoryID string, settings RadiusSettingsInput) error {
	b.mu.Lock("EnableRadius")
	defer b.mu.Unlock()

	if _, ok := b.directories[directoryID]; !ok {
		return ErrDirectoryNotFound
	}

	servers := make([]string, len(settings.RadiusServers))
	copy(servers, settings.RadiusServers)
	b.radiusSettings[directoryID] = &storedRadiusSettings{
		DirectoryID:            directoryID,
		AuthenticationProtocol: settings.AuthenticationProtocol,
		DisplayLabel:           settings.DisplayLabel,
		RadiusServers:          servers,
		SharedSecret:           settings.SharedSecret,
		RadiusPort:             settings.RadiusPort,
		RadiusRetries:          settings.RadiusRetries,
		RadiusTimeout:          settings.RadiusTimeout,
		UseSameUsername:        settings.UseSameUsername,
	}

	return nil
}

// DisableRadius disables RADIUS for a directory.
func (b *InMemoryBackend) DisableRadius(directoryID string) error {
	b.mu.Lock("DisableRadius")
	defer b.mu.Unlock()

	if _, ok := b.directories[directoryID]; !ok {
		return ErrDirectoryNotFound
	}

	delete(b.radiusSettings, directoryID)

	return nil
}

// UpdateRadius updates RADIUS settings for a directory.
func (b *InMemoryBackend) UpdateRadius(directoryID string, settings RadiusSettingsInput) error {
	b.mu.Lock("UpdateRadius")
	defer b.mu.Unlock()

	if _, ok := b.directories[directoryID]; !ok {
		return ErrDirectoryNotFound
	}

	servers := make([]string, len(settings.RadiusServers))
	copy(servers, settings.RadiusServers)
	existing, ok := b.radiusSettings[directoryID]
	if !ok {
		b.radiusSettings[directoryID] = &storedRadiusSettings{}
		existing = b.radiusSettings[directoryID]
	}
	existing.DirectoryID = directoryID
	existing.AuthenticationProtocol = settings.AuthenticationProtocol
	existing.DisplayLabel = settings.DisplayLabel
	existing.RadiusServers = servers
	existing.SharedSecret = settings.SharedSecret
	existing.RadiusPort = settings.RadiusPort
	existing.RadiusRetries = settings.RadiusRetries
	existing.RadiusTimeout = settings.RadiusTimeout
	existing.UseSameUsername = settings.UseSameUsername

	return nil
}

// --- Directory Data Access ---

// EnableDirectoryDataAccess enables directory data access.
func (b *InMemoryBackend) EnableDirectoryDataAccess(directoryID string) error {
	b.mu.Lock("EnableDirectoryDataAccess")
	defer b.mu.Unlock()

	if _, ok := b.directories[directoryID]; !ok {
		return ErrDirectoryNotFound
	}

	b.dirDataAccess[directoryID] = true

	return nil
}

// DisableDirectoryDataAccess disables directory data access.
func (b *InMemoryBackend) DisableDirectoryDataAccess(directoryID string) error {
	b.mu.Lock("DisableDirectoryDataAccess")
	defer b.mu.Unlock()

	if _, ok := b.directories[directoryID]; !ok {
		return ErrDirectoryNotFound
	}

	b.dirDataAccess[directoryID] = false

	return nil
}

// DescribeDirectoryDataAccess returns data access status for a directory.
func (b *InMemoryBackend) DescribeDirectoryDataAccess(directoryID string) (*DirectoryDataAccessStatus, error) {
	b.mu.RLock("DescribeDirectoryDataAccess")
	defer b.mu.RUnlock()

	if _, ok := b.directories[directoryID]; !ok {
		return nil, ErrDirectoryNotFound
	}

	enabled := b.dirDataAccess[directoryID]

	return &DirectoryDataAccessStatus{DirectoryID: directoryID, Enabled: enabled}, nil
}

// --- CA Enrollment Policy ---

// EnableCAEnrollmentPolicy enables CA enrollment policy.
func (b *InMemoryBackend) EnableCAEnrollmentPolicy(directoryID string) error {
	b.mu.Lock("EnableCAEnrollmentPolicy")
	defer b.mu.Unlock()

	if _, ok := b.directories[directoryID]; !ok {
		return ErrDirectoryNotFound
	}

	b.caEnrollment[directoryID] = true

	return nil
}

// DisableCAEnrollmentPolicy disables CA enrollment policy.
func (b *InMemoryBackend) DisableCAEnrollmentPolicy(directoryID string) error {
	b.mu.Lock("DisableCAEnrollmentPolicy")
	defer b.mu.Unlock()

	if _, ok := b.directories[directoryID]; !ok {
		return ErrDirectoryNotFound
	}

	b.caEnrollment[directoryID] = false

	return nil
}

// DescribeCAEnrollmentPolicy returns CA enrollment policy for a directory.
func (b *InMemoryBackend) DescribeCAEnrollmentPolicy(directoryID string) (*CAEnrollmentPolicy, error) {
	b.mu.RLock("DescribeCAEnrollmentPolicy")
	defer b.mu.RUnlock()

	if _, ok := b.directories[directoryID]; !ok {
		return nil, ErrDirectoryNotFound
	}

	enabled := b.caEnrollment[directoryID]

	return &CAEnrollmentPolicy{DirectoryID: directoryID, Enabled: enabled}, nil
}

// --- AD Assessments ---

// StartADAssessment starts an AD assessment.
func (b *InMemoryBackend) StartADAssessment(directoryID string) (string, error) {
	b.mu.Lock("StartADAssessment")
	defer b.mu.Unlock()

	if _, ok := b.directories[directoryID]; !ok {
		return "", ErrDirectoryNotFound
	}

	id := fmt.Sprintf("a-%s", uuid.NewString()[:10])
	b.adAssessments[id] = &storedADAssessment{
		AssessmentID: id,
		DirectoryID:  directoryID,
		Status:       "Completed",
		AssessType:   "Operational",
		Region:       b.region,
		StartTime:    time.Now().UTC(),
	}

	return id, nil
}

// DeleteADAssessment deletes an AD assessment.
func (b *InMemoryBackend) DeleteADAssessment(directoryID, assessmentID string) error {
	b.mu.Lock("DeleteADAssessment")
	defer b.mu.Unlock()

	a, ok := b.adAssessments[assessmentID]
	if !ok || a.DirectoryID != directoryID {
		return ErrAssessmentNotFound
	}

	delete(b.adAssessments, assessmentID)

	return nil
}

// DescribeADAssessment returns details of an AD assessment.
func (b *InMemoryBackend) DescribeADAssessment(directoryID, assessmentID string) (*ADAssessmentInfo, error) {
	b.mu.RLock("DescribeADAssessment")
	defer b.mu.RUnlock()

	a, ok := b.adAssessments[assessmentID]
	if !ok || a.DirectoryID != directoryID {
		return nil, ErrAssessmentNotFound
	}

	return &ADAssessmentInfo{
		AssessmentID: a.AssessmentID,
		DirectoryID:  a.DirectoryID,
		Status:       a.Status,
		AssessType:   a.AssessType,
		Region:       a.Region,
		StartTime:    a.StartTime,
	}, nil
}

// ListADAssessments returns AD assessments for a directory.
func (b *InMemoryBackend) ListADAssessments(
	directoryID string,
	limit int32,
	nextToken string,
) ([]ADAssessmentInfo, string, error) {
	b.mu.RLock("ListADAssessments")
	defer b.mu.RUnlock()

	var ids []string
	for id, a := range b.adAssessments {
		if directoryID != "" && a.DirectoryID != directoryID {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)

	start := 0
	if nextToken != "" {
		for i, id := range ids {
			if id == nextToken {
				start = i

				break
			}
		}
	}

	pageSize := int(limit)
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}

	end := min(start+pageSize, len(ids))
	result := make([]ADAssessmentInfo, 0, end-start)
	for _, id := range ids[start:end] {
		a := b.adAssessments[id]
		result = append(result, ADAssessmentInfo{
			AssessmentID: a.AssessmentID,
			DirectoryID:  a.DirectoryID,
			Status:       a.Status,
			AssessType:   a.AssessType,
			Region:       a.Region,
			StartTime:    a.StartTime,
		})
	}

	var outToken string
	if end < len(ids) {
		outToken = ids[end]
	}

	return result, outToken, nil
}

// --- Hybrid AD ---

// CreateHybridAD creates a Hybrid AD directory (stored as MicrosoftAD type).
func (b *InMemoryBackend) CreateHybridAD(
	name, shortName, description, _ string,
	edition DirectoryEdition,
	tags []Tag,
) (*Directory, string, error) {
	b.mu.Lock("CreateHybridAD")
	defer b.mu.Unlock()

	if name == "" {
		return nil, "", ErrInvalidParameter
	}

	d := b.newStoredDirectory(name, shortName, description, DirectoryTypeMicrosoftAD, "", edition, nil, tags)
	b.directories[d.DirectoryID] = d
	b.aliases[d.Alias] = d.DirectoryID

	requestID := uuid.NewString()
	b.hybridADUpdates[requestID] = &storedHybridADUpdate{
		RequestID:   requestID,
		DirectoryID: d.DirectoryID,
		Status:      "Updated", //nolint:goconst // existing issue.
	}

	cp := d.toDirectory()

	return &cp, requestID, nil
}

// UpdateHybridAD updates a Hybrid AD directory.
func (b *InMemoryBackend) UpdateHybridAD(directoryID string) (string, error) {
	b.mu.Lock("UpdateHybridAD")
	defer b.mu.Unlock()

	if _, ok := b.directories[directoryID]; !ok {
		return "", ErrDirectoryNotFound
	}

	requestID := uuid.NewString()
	b.hybridADUpdates[requestID] = &storedHybridADUpdate{
		RequestID:   requestID,
		DirectoryID: directoryID,
		Status:      "Updated",
	}

	return requestID, nil
}

// DescribeHybridADUpdate returns hybrid AD update info for a directory.
func (b *InMemoryBackend) DescribeHybridADUpdate(directoryID string) ([]HybridADUpdateEntry, error) {
	b.mu.RLock("DescribeHybridADUpdate")
	defer b.mu.RUnlock()

	if _, ok := b.directories[directoryID]; !ok {
		return nil, ErrDirectoryNotFound
	}

	var result []HybridADUpdateEntry
	for _, u := range b.hybridADUpdates {
		if u.DirectoryID == directoryID {
			result = append(result, HybridADUpdateEntry{
				RequestID:   u.RequestID,
				DirectoryID: u.DirectoryID,
				Status:      u.Status,
			})
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].RequestID < result[j].RequestID })

	return result, nil
}

// --- Computer ---

// CreateComputer creates a computer account in a directory.
func (b *InMemoryBackend) CreateComputer(directoryID, computerName, _ string) (*ComputerInfo, error) {
	b.mu.RLock("CreateComputer")
	defer b.mu.RUnlock()

	if _, ok := b.directories[directoryID]; !ok {
		return nil, ErrDirectoryNotFound
	}

	if computerName == "" {
		return nil, ErrInvalidParameter
	}

	return &ComputerInfo{
		ComputerID:   fmt.Sprintf("CN=%s,OU=Computers", computerName),
		ComputerName: computerName,
	}, nil
}

// --- Settings ---

// UpdateSettings updates directory settings.
func (b *InMemoryBackend) UpdateSettings(directoryID string, settings []DirectorySetting) (string, error) {
	b.mu.Lock("UpdateSettings")
	defer b.mu.Unlock()

	if _, ok := b.directories[directoryID]; !ok {
		return "", ErrDirectoryNotFound
	}

	now := time.Now().UTC()
	existing := make(map[string]*storedDirectorySetting)
	for _, s := range b.dirSettings[directoryID] {
		existing[s.Name] = s
	}

	for _, s := range settings {
		if e, ok := existing[s.Name]; ok {
			e.RequestedValue = s.Value
			e.Status = "Requested"
			e.LastUpdatedDateTime = now
		} else {
			ns := &storedDirectorySetting{
				DirectoryID:         directoryID,
				Name:                s.Name,
				AllowedValues:       s.AllowedValues,
				RequestedValue:      s.Value,
				AppliedValue:        s.Value,
				Status:              "Updated",
				LastUpdatedDateTime: now,
			}
			b.dirSettings[directoryID] = append(b.dirSettings[directoryID], ns)
		}
	}

	return directoryID, nil
}

// DescribeSettings returns directory settings.
func (b *InMemoryBackend) DescribeSettings(
	directoryID, status, nextToken string, //nolint:revive // existing issue.
) ([]SettingEntry, string, error) {
	b.mu.RLock("DescribeSettings")
	defer b.mu.RUnlock()

	if _, ok := b.directories[directoryID]; !ok {
		return nil, "", ErrDirectoryNotFound
	}

	settings := b.dirSettings[directoryID]
	var filtered []storedDirectorySetting
	for _, s := range settings {
		if status != "" && s.Status != status {
			continue
		}
		filtered = append(filtered, *s)
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].Name < filtered[j].Name })

	result := make([]SettingEntry, 0, len(filtered))
	for _, s := range filtered {
		result = append(result, SettingEntry(s))
	}

	return result, "", nil
}

// UpdateDirectorySetup initiates a directory setup update.
func (b *InMemoryBackend) UpdateDirectorySetup(directoryID, updateType string, _ bool) error {
	b.mu.Lock("UpdateDirectorySetup")
	defer b.mu.Unlock()

	if _, ok := b.directories[directoryID]; !ok {
		return ErrDirectoryNotFound
	}

	now := time.Now().UTC()
	b.updateInfoEntries[directoryID] = append(b.updateInfoEntries[directoryID], &storedUpdateInfo{
		DirectoryID:         directoryID,
		UpdateType:          updateType,
		Status:              "Updated",
		StartTime:           now,
		LastUpdatedDateTime: now,
		Region:              b.region,
		InitiatedBy:         b.accountID,
	})

	return nil
}

// DescribeUpdateDirectory returns update info entries for a directory.
func (b *InMemoryBackend) DescribeUpdateDirectory(
	directoryID, updateType, nextToken string, //nolint:revive // existing issue.
) ([]UpdateInfoEntry, string, error) {
	b.mu.RLock("DescribeUpdateDirectory")
	defer b.mu.RUnlock()

	if _, ok := b.directories[directoryID]; !ok {
		return nil, "", ErrDirectoryNotFound
	}

	var result []UpdateInfoEntry
	for _, u := range b.updateInfoEntries[directoryID] {
		if updateType != "" && u.UpdateType != updateType {
			continue
		}
		result = append(result, UpdateInfoEntry{
			DirectoryID:         u.DirectoryID,
			UpdateType:          u.UpdateType,
			Status:              u.Status,
			NewValue:            u.NewValue,
			PreviousValue:       u.PreviousValue,
			InitiatedBy:         u.InitiatedBy,
			Region:              u.Region,
			StartTime:           u.StartTime,
			LastUpdatedDateTime: u.LastUpdatedDateTime,
		})
	}

	return result, "", nil
}

// --- Password Reset ---

// ResetUserPassword resets a user password.
func (b *InMemoryBackend) ResetUserPassword(directoryID, _, _ string) error {
	b.mu.RLock("ResetUserPassword")
	defer b.mu.RUnlock()

	if _, ok := b.directories[directoryID]; !ok {
		return ErrDirectoryNotFound
	}

	return nil
}

// --- ConnectDirectory ---

// ConnectDirectory creates an ADConnector directory.
func (b *InMemoryBackend) ConnectDirectory(
	name, shortName, description, _ string,
	size DirectorySize,
	tags []Tag,
) (*Directory, error) {
	b.mu.Lock("ConnectDirectory")
	defer b.mu.Unlock()

	if name == "" {
		return nil, ErrInvalidParameter
	}

	d := b.newStoredDirectory(name, shortName, description, DirectoryTypeADConnector, size, "", nil, tags)
	b.directories[d.DirectoryID] = d
	b.aliases[d.Alias] = d.DirectoryID

	cp := d.toDirectory()

	return &cp, nil
}
