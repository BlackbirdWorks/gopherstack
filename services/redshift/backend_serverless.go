package redshift

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// ---------------------------------------------------------------------------
// Sentinel errors — Redshift Serverless
// ---------------------------------------------------------------------------

var (
	// ErrNamespaceNotFound is returned when a serverless namespace does not exist.
	ErrNamespaceNotFound = errors.New("ResourceNotFoundException")
	// ErrNamespaceAlreadyExists is returned when a serverless namespace already exists.
	ErrNamespaceAlreadyExists = errors.New("ConflictException")
	// ErrWorkgroupNotFound is returned when a serverless workgroup does not exist.
	ErrWorkgroupNotFound = errors.New("ResourceNotFoundException")
	// ErrWorkgroupAlreadyExists is returned when a serverless workgroup already exists.
	ErrWorkgroupAlreadyExists = errors.New("ConflictException")
	// ErrServerlessSnapshotNotFound is returned when a serverless snapshot does not exist.
	ErrServerlessSnapshotNotFound = errors.New("ResourceNotFoundException")
	// ErrServerlessConflict is returned when a serverless resource already exists.
	ErrServerlessConflict = errors.New("ConflictException")
	// ErrUsageLimitSLNotFound is returned when a serverless usage limit does not exist.
	ErrUsageLimitSLNotFound = errors.New("ResourceNotFoundException")
	// ErrScheduledActionSLNotFound is returned when a serverless scheduled action does not exist.
	ErrScheduledActionSLNotFound = errors.New("ResourceNotFoundException")
)

// ---------------------------------------------------------------------------
// Serverless models
// ---------------------------------------------------------------------------

// Namespace represents a Redshift Serverless namespace.
type Namespace struct {
	CreationDate  time.Time `json:"creationDate"`
	NamespaceArn  string    `json:"namespaceArn"`
	NamespaceID   string    `json:"namespaceId"`
	NamespaceName string    `json:"namespaceName"`
	AdminUsername string    `json:"adminUsername,omitempty"`
	DBName        string    `json:"dbName,omitempty"`
	KmsKeyID      string    `json:"kmsKeyId,omitempty"`
	Status        string    `json:"status"`
	IamRoles      []string  `json:"iamRoles,omitempty"`
	LogExports    []string  `json:"logExports,omitempty"`
}

// Workgroup represents a Redshift Serverless workgroup.
type Workgroup struct {
	CreationDate     time.Time         `json:"creationDate"`
	WorkgroupArn     string            `json:"workgroupArn"`
	WorkgroupID      string            `json:"workgroupId"`
	WorkgroupName    string            `json:"workgroupName"`
	NamespaceName    string            `json:"namespaceName"`
	Status           string            `json:"status"`
	Endpoint         WorkgroupEndpoint `json:"endpoint"`
	SubnetIDs        []string          `json:"subnetIds,omitempty"`
	SecurityGroupIDs []string          `json:"securityGroupIds,omitempty"`
	BaseCapacity     int               `json:"baseCapacity"`
}

// WorkgroupEndpoint holds the endpoint address and port.
type WorkgroupEndpoint struct {
	Address string `json:"address"`
	Port    int    `json:"port"`
}

// ServerlessSnapshot represents a Redshift Serverless namespace snapshot.
type ServerlessSnapshot struct {
	SnapshotCreateTime        time.Time `json:"snapshotCreateTime"`
	SnapshotArn               string    `json:"snapshotArn"`
	SnapshotName              string    `json:"snapshotName"`
	NamespaceName             string    `json:"namespaceName"`
	NamespaceArn              string    `json:"namespaceArn"`
	Status                    string    `json:"status"`
	AdminUsername             string    `json:"adminUsername,omitempty"`
	AccountsWithRestoreAccess []string  `json:"accountsWithRestoreAccess,omitempty"`
}

// ServerlessUsageLimit represents a serverless usage limit.
type ServerlessUsageLimit struct {
	UsageLimitArn string `json:"usageLimitArn"`
	UsageLimitID  string `json:"usageLimitId"`
	ResourceArn   string `json:"resourceArn"`
	UsageType     string `json:"usageType"`
	Period        string `json:"period"`
	BreachAction  string `json:"breachAction"`
	Amount        int64  `json:"amount"`
}

// ServerlessScheduledAction represents a serverless scheduled action.
type ServerlessScheduledAction struct {
	ScheduledActionArn  string    `json:"scheduledActionArn"`
	ScheduledActionName string    `json:"scheduledActionName"`
	NamespaceName       string    `json:"namespaceName"`
	Schedule            string    `json:"schedule"`
	StartTime           time.Time `json:"startTime"`
	EndTime             time.Time `json:"endTime"`
	Status              string    `json:"status"`
	TargetAction        string    `json:"targetAction"`
}

// ---------------------------------------------------------------------------
// Status constants for serverless resources
// ---------------------------------------------------------------------------

const (
	slStatusAvailable = "AVAILABLE"
	slStatusActive    = dataShareStatusActive // reuse existing constant

	// Magic number constants for serverless operations.
	slIDHexBytes          = 8    // bytes for random resource IDs (produces 16-char hex)
	slEndpointHexBytes    = 6    // bytes for random endpoint suffix (produces 12-char hex)
	slDefaultBaseCapacity = 32   // default RPU if not specified
	slServerlessPort      = 5439 // default Redshift serverless port
	slCredTokenHexBytes   = 4    // bytes for credential token suffix
	slCredSecretHexBytes  = 20   // bytes for credential secret
	slCredExpiryMinutes   = 15   // credential TTL in minutes
	slDefaultPageSize     = 100  // default max results per page
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)

	return hex.EncodeToString(b)
}

func serverlessDefaultPageSize() int { return slDefaultPageSize }

// ---------------------------------------------------------------------------
// Namespace CRUD
// ---------------------------------------------------------------------------

// CreateNamespace creates a new Redshift Serverless namespace.
func (b *InMemoryBackend) CreateNamespace(
	namespaceName, adminUsername, dbName, kmsKeyID string,
	iamRoles, logExports []string,
) (*Namespace, error) {
	b.mu.Lock("CreateNamespace")
	defer b.mu.Unlock()

	if _, ok := b.slNamespaces[namespaceName]; ok {
		return nil, fmt.Errorf(
			"%w: namespace %q already exists",
			ErrNamespaceAlreadyExists,
			namespaceName,
		)
	}

	id := randomHex(slIDHexBytes)
	nsArn := arn.Build("redshift-serverless", b.region, b.accountID, "namespace/"+id)

	rolesCopy := make([]string, len(iamRoles))
	copy(rolesCopy, iamRoles)

	exportsCopy := make([]string, len(logExports))
	copy(exportsCopy, logExports)

	ns := &Namespace{
		CreationDate:  time.Now(),
		NamespaceArn:  nsArn,
		NamespaceID:   id,
		NamespaceName: namespaceName,
		AdminUsername: adminUsername,
		DBName:        dbName,
		KmsKeyID:      kmsKeyID,
		Status:        slStatusAvailable,
		IamRoles:      rolesCopy,
		LogExports:    exportsCopy,
	}
	b.slNamespaces[namespaceName] = ns
	b.slNamespaceIdx.insert(namespaceName)

	return cloneNamespace(ns), nil
}

// GetNamespace returns a Redshift Serverless namespace by name.
func (b *InMemoryBackend) GetNamespace(namespaceName string) (*Namespace, error) {
	b.mu.RLock("GetNamespace")
	defer b.mu.RUnlock()

	ns, ok := b.slNamespaces[namespaceName]
	if !ok {
		return nil, fmt.Errorf("%w: namespace %q not found", ErrNamespaceNotFound, namespaceName)
	}

	return cloneNamespace(ns), nil
}

// ListNamespaces returns all namespaces with pagination.
//
//nolint:dupl // pagination pattern is structurally identical across serverless resource types
func (b *InMemoryBackend) ListNamespaces(maxResults int, nextToken string) ([]*Namespace, string) {
	b.mu.RLock("ListNamespaces")
	defer b.mu.RUnlock()

	// Iterate the pre-sorted index so results are ordered without re-sorting.
	keys := b.slNamespaceIdx.ordered()
	list := make([]*Namespace, 0, len(keys))

	for _, name := range keys {
		if ns, ok := b.slNamespaces[name]; ok {
			list = append(list, cloneNamespace(ns))
		}
	}

	if maxResults <= 0 {
		maxResults = serverlessDefaultPageSize()
	}

	startIdx := 0
	if nextToken != "" {
		if n, err := strconv.Atoi(nextToken); err == nil {
			startIdx = n
		}
	}

	if startIdx >= len(list) {
		return []*Namespace{}, ""
	}

	end := startIdx + maxResults
	var outToken string

	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}

// UpdateNamespace updates a Redshift Serverless namespace.
func (b *InMemoryBackend) UpdateNamespace(
	namespaceName, adminUsername, dbName, kmsKeyID string,
	iamRoles, logExports []string,
) (*Namespace, error) {
	b.mu.Lock("UpdateNamespace")
	defer b.mu.Unlock()

	ns, ok := b.slNamespaces[namespaceName]
	if !ok {
		return nil, fmt.Errorf("%w: namespace %q not found", ErrNamespaceNotFound, namespaceName)
	}

	if adminUsername != "" {
		ns.AdminUsername = adminUsername
	}

	if dbName != "" {
		ns.DBName = dbName
	}

	if kmsKeyID != "" {
		ns.KmsKeyID = kmsKeyID
	}

	if iamRoles != nil {
		cp := make([]string, len(iamRoles))
		copy(cp, iamRoles)
		ns.IamRoles = cp
	}

	if logExports != nil {
		cp := make([]string, len(logExports))
		copy(cp, logExports)
		ns.LogExports = cp
	}

	return cloneNamespace(ns), nil
}

// DeleteNamespace deletes a Redshift Serverless namespace.
func (b *InMemoryBackend) DeleteNamespace(namespaceName string) (*Namespace, error) {
	b.mu.Lock("DeleteNamespace")
	defer b.mu.Unlock()

	ns, ok := b.slNamespaces[namespaceName]
	if !ok {
		return nil, fmt.Errorf("%w: namespace %q not found", ErrNamespaceNotFound, namespaceName)
	}

	cp := cloneNamespace(ns)
	delete(b.slNamespaces, namespaceName)
	b.slNamespaceIdx.remove(namespaceName)

	return cp, nil
}

func cloneNamespace(ns *Namespace) *Namespace {
	cp := *ns
	cp.IamRoles = make([]string, len(ns.IamRoles))
	copy(cp.IamRoles, ns.IamRoles)
	cp.LogExports = make([]string, len(ns.LogExports))
	copy(cp.LogExports, ns.LogExports)

	return &cp
}

// ---------------------------------------------------------------------------
// Workgroup CRUD
// ---------------------------------------------------------------------------

// CreateWorkgroup creates a new Redshift Serverless workgroup.
func (b *InMemoryBackend) CreateWorkgroup(
	workgroupName, namespaceName string,
	baseCapacity int,
	subnetIDs, securityGroupIDs []string,
) (*Workgroup, error) {
	b.mu.Lock("CreateWorkgroup")
	defer b.mu.Unlock()

	if _, ok := b.slWorkgroups[workgroupName]; ok {
		return nil, fmt.Errorf(
			"%w: workgroup %q already exists",
			ErrWorkgroupAlreadyExists,
			workgroupName,
		)
	}

	if _, ok := b.slNamespaces[namespaceName]; !ok {
		return nil, fmt.Errorf("%w: namespace %q not found", ErrNamespaceNotFound, namespaceName)
	}

	id := randomHex(slIDHexBytes)
	wgArn := arn.Build("redshift-serverless", b.region, b.accountID, "workgroup/"+id)

	if baseCapacity <= 0 {
		baseCapacity = slDefaultBaseCapacity
	}

	endpointAddr := fmt.Sprintf(
		"%s.%s.%s.redshift-serverless.amazonaws.com",
		workgroupName, randomHex(slEndpointHexBytes), b.region,
	)

	subnetsCopy := make([]string, len(subnetIDs))
	copy(subnetsCopy, subnetIDs)

	sgCopy := make([]string, len(securityGroupIDs))
	copy(sgCopy, securityGroupIDs)

	wg := &Workgroup{
		CreationDate:  time.Now(),
		WorkgroupArn:  wgArn,
		WorkgroupID:   id,
		WorkgroupName: workgroupName,
		NamespaceName: namespaceName,
		Status:        slStatusAvailable,
		BaseCapacity:  baseCapacity,
		Endpoint: WorkgroupEndpoint{
			Address: endpointAddr,
			Port:    slServerlessPort,
		},
		SubnetIDs:        subnetsCopy,
		SecurityGroupIDs: sgCopy,
	}
	b.slWorkgroups[workgroupName] = wg
	b.slWorkgroupIdx.insert(workgroupName)

	return cloneWorkgroup(wg), nil
}

// GetWorkgroup returns a Redshift Serverless workgroup by name.
func (b *InMemoryBackend) GetWorkgroup(workgroupName string) (*Workgroup, error) {
	b.mu.RLock("GetWorkgroup")
	defer b.mu.RUnlock()

	wg, ok := b.slWorkgroups[workgroupName]
	if !ok {
		return nil, fmt.Errorf("%w: workgroup %q not found", ErrWorkgroupNotFound, workgroupName)
	}

	return cloneWorkgroup(wg), nil
}

// ListWorkgroups returns all workgroups with pagination.
//
//nolint:dupl // pagination pattern is structurally identical across serverless resource types
func (b *InMemoryBackend) ListWorkgroups(maxResults int, nextToken string) ([]*Workgroup, string) {
	b.mu.RLock("ListWorkgroups")
	defer b.mu.RUnlock()

	// Iterate the pre-sorted index so results are ordered without re-sorting.
	keys := b.slWorkgroupIdx.ordered()
	list := make([]*Workgroup, 0, len(keys))

	for _, name := range keys {
		if wg, ok := b.slWorkgroups[name]; ok {
			list = append(list, cloneWorkgroup(wg))
		}
	}

	if maxResults <= 0 {
		maxResults = serverlessDefaultPageSize()
	}

	startIdx := 0
	if nextToken != "" {
		if n, err := strconv.Atoi(nextToken); err == nil {
			startIdx = n
		}
	}

	if startIdx >= len(list) {
		return []*Workgroup{}, ""
	}

	end := startIdx + maxResults
	var outToken string

	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}

// UpdateWorkgroup updates a Redshift Serverless workgroup.
func (b *InMemoryBackend) UpdateWorkgroup(
	workgroupName string,
	baseCapacity int,
	subnetIDs, securityGroupIDs []string,
) (*Workgroup, error) {
	b.mu.Lock("UpdateWorkgroup")
	defer b.mu.Unlock()

	wg, ok := b.slWorkgroups[workgroupName]
	if !ok {
		return nil, fmt.Errorf("%w: workgroup %q not found", ErrWorkgroupNotFound, workgroupName)
	}

	if baseCapacity > 0 {
		wg.BaseCapacity = baseCapacity
	}

	if subnetIDs != nil {
		cp := make([]string, len(subnetIDs))
		copy(cp, subnetIDs)
		wg.SubnetIDs = cp
	}

	if securityGroupIDs != nil {
		cp := make([]string, len(securityGroupIDs))
		copy(cp, securityGroupIDs)
		wg.SecurityGroupIDs = cp
	}

	return cloneWorkgroup(wg), nil
}

// DeleteWorkgroup deletes a Redshift Serverless workgroup.
func (b *InMemoryBackend) DeleteWorkgroup(workgroupName string) (*Workgroup, error) {
	b.mu.Lock("DeleteWorkgroup")
	defer b.mu.Unlock()

	wg, ok := b.slWorkgroups[workgroupName]
	if !ok {
		return nil, fmt.Errorf("%w: workgroup %q not found", ErrWorkgroupNotFound, workgroupName)
	}

	cp := cloneWorkgroup(wg)
	delete(b.slWorkgroups, workgroupName)
	b.slWorkgroupIdx.remove(workgroupName)

	return cp, nil
}

func cloneWorkgroup(wg *Workgroup) *Workgroup {
	cp := *wg
	cp.SubnetIDs = make([]string, len(wg.SubnetIDs))
	copy(cp.SubnetIDs, wg.SubnetIDs)
	cp.SecurityGroupIDs = make([]string, len(wg.SecurityGroupIDs))
	copy(cp.SecurityGroupIDs, wg.SecurityGroupIDs)

	return &cp
}

// ---------------------------------------------------------------------------
// Credentials
// ---------------------------------------------------------------------------

// GetCredentials returns temporary credentials for a serverless workgroup.
// Returns (dbUser, dbPassword, expiry, error).
func (b *InMemoryBackend) GetCredentials(
	workgroupName, dbName string,
) (string, string, string, error) {
	b.mu.RLock("GetCredentials")
	defer b.mu.RUnlock()

	wg, ok := b.slWorkgroups[workgroupName]
	if !ok {
		return "", "", "", fmt.Errorf(
			"%w: workgroup %q not found",
			ErrWorkgroupNotFound,
			workgroupName,
		)
	}

	resolvedDB := dbName
	if resolvedDB == "" {
		ns, nsOK := b.slNamespaces[wg.NamespaceName]
		if nsOK && ns.DBName != "" {
			resolvedDB = ns.DBName
		} else {
			resolvedDB = defaultDBName
		}
	}

	expiry := time.Now().Add(slCredExpiryMinutes * time.Minute).Format(time.RFC3339)
	dbUser := "IAMR:" + resolvedDB + ":" + strings.ToUpper(randomHex(slCredTokenHexBytes))

	return dbUser,
		randomHex(slCredSecretHexBytes),
		expiry,
		nil
}

// ---------------------------------------------------------------------------
// Serverless Snapshots
// ---------------------------------------------------------------------------

// CreateServerlessSnapshot creates a snapshot of a serverless namespace.
func (b *InMemoryBackend) CreateServerlessSnapshot(
	snapshotName, namespaceName string,
) (*ServerlessSnapshot, error) {
	b.mu.Lock("CreateServerlessSnapshot")
	defer b.mu.Unlock()

	ns, ok := b.slNamespaces[namespaceName]
	if !ok {
		return nil, fmt.Errorf("%w: namespace %q not found", ErrNamespaceNotFound, namespaceName)
	}

	if _, exists := b.slSnapshots[snapshotName]; exists {
		return nil, fmt.Errorf(
			"%w: snapshot %q already exists",
			ErrServerlessConflict,
			snapshotName,
		)
	}

	snapArn := arn.Build("redshift-serverless", b.region, b.accountID, "snapshot/"+snapshotName)

	snap := &ServerlessSnapshot{
		SnapshotCreateTime: time.Now(),
		SnapshotArn:        snapArn,
		SnapshotName:       snapshotName,
		NamespaceName:      namespaceName,
		NamespaceArn:       ns.NamespaceArn,
		Status:             slStatusAvailable,
		AdminUsername:      ns.AdminUsername,
	}
	b.slSnapshots[snapshotName] = snap
	b.slSnapshotIdx.insert(snapshotName)

	return cloneServerlessSnapshot(snap), nil
}

// GetServerlessSnapshot returns a serverless snapshot by name.
func (b *InMemoryBackend) GetServerlessSnapshot(snapshotName string) (*ServerlessSnapshot, error) {
	b.mu.RLock("GetServerlessSnapshot")
	defer b.mu.RUnlock()

	snap, ok := b.slSnapshots[snapshotName]
	if !ok {
		return nil, fmt.Errorf(
			"%w: snapshot %q not found",
			ErrServerlessSnapshotNotFound,
			snapshotName,
		)
	}

	return cloneServerlessSnapshot(snap), nil
}

// ListServerlessSnapshots returns snapshots, optionally filtered by namespace name.
//
//nolint:dupl // pagination pattern is structurally identical across serverless resource types
func (b *InMemoryBackend) ListServerlessSnapshots(
	namespaceName string,
	maxResults int,
	nextToken string,
) ([]*ServerlessSnapshot, string) {
	b.mu.RLock("ListServerlessSnapshots")
	defer b.mu.RUnlock()

	// Iterate the pre-sorted index so results are ordered without re-sorting.
	keys := b.slSnapshotIdx.ordered()
	list := make([]*ServerlessSnapshot, 0, len(keys))

	for _, name := range keys {
		snap, ok := b.slSnapshots[name]
		if !ok {
			continue
		}

		if namespaceName == "" || snap.NamespaceName == namespaceName {
			list = append(list, cloneServerlessSnapshot(snap))
		}
	}

	if maxResults <= 0 {
		maxResults = serverlessDefaultPageSize()
	}

	startIdx := 0
	if nextToken != "" {
		if n, err := strconv.Atoi(nextToken); err == nil {
			startIdx = n
		}
	}

	if startIdx >= len(list) {
		return []*ServerlessSnapshot{}, ""
	}

	end := startIdx + maxResults
	var outToken string

	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}

// DeleteServerlessSnapshot deletes a serverless snapshot.
func (b *InMemoryBackend) DeleteServerlessSnapshot(
	snapshotName string,
) (*ServerlessSnapshot, error) {
	b.mu.Lock("DeleteServerlessSnapshot")
	defer b.mu.Unlock()

	snap, ok := b.slSnapshots[snapshotName]
	if !ok {
		return nil, fmt.Errorf(
			"%w: snapshot %q not found",
			ErrServerlessSnapshotNotFound,
			snapshotName,
		)
	}

	cp := cloneServerlessSnapshot(snap)
	delete(b.slSnapshots, snapshotName)
	b.slSnapshotIdx.remove(snapshotName)

	return cp, nil
}

func cloneServerlessSnapshot(snap *ServerlessSnapshot) *ServerlessSnapshot {
	cp := *snap
	cp.AccountsWithRestoreAccess = make([]string, len(snap.AccountsWithRestoreAccess))
	copy(cp.AccountsWithRestoreAccess, snap.AccountsWithRestoreAccess)

	return &cp
}

// ---------------------------------------------------------------------------
// Serverless Usage Limits
// ---------------------------------------------------------------------------

// CreateServerlessUsageLimit creates a serverless usage limit.
func (b *InMemoryBackend) CreateServerlessUsageLimit(
	resourceArn, usageType, period, breachAction string,
	amount int64,
) (*ServerlessUsageLimit, error) {
	b.mu.Lock("CreateServerlessUsageLimit")
	defer b.mu.Unlock()

	id := randomHex(slIDHexBytes)
	ulArn := arn.Build("redshift-serverless", b.region, b.accountID, "usagelimit/"+id)

	ul := &ServerlessUsageLimit{
		UsageLimitArn: ulArn,
		UsageLimitID:  id,
		ResourceArn:   resourceArn,
		UsageType:     usageType,
		Amount:        amount,
		Period:        period,
		BreachAction:  breachAction,
	}
	b.slUsageLimits[id] = ul
	b.slUsageLimitIdx.insert(id)

	return cloneServerlessUsageLimit(ul), nil
}

// GetServerlessUsageLimit returns a serverless usage limit by ID.
func (b *InMemoryBackend) GetServerlessUsageLimit(
	usageLimitID string,
) (*ServerlessUsageLimit, error) {
	b.mu.RLock("GetServerlessUsageLimit")
	defer b.mu.RUnlock()

	ul, ok := b.slUsageLimits[usageLimitID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: usage limit %q not found",
			ErrUsageLimitSLNotFound,
			usageLimitID,
		)
	}

	return cloneServerlessUsageLimit(ul), nil
}

// ListServerlessUsageLimits returns all serverless usage limits.
//
//nolint:dupl // pagination pattern is structurally identical across serverless resource types
func (b *InMemoryBackend) ListServerlessUsageLimits(
	resourceArn string,
	maxResults int,
	nextToken string,
) ([]*ServerlessUsageLimit, string) {
	b.mu.RLock("ListServerlessUsageLimits")
	defer b.mu.RUnlock()

	// Iterate the pre-sorted index so results are ordered without re-sorting.
	keys := b.slUsageLimitIdx.ordered()
	list := make([]*ServerlessUsageLimit, 0, len(keys))

	for _, id := range keys {
		ul, ok := b.slUsageLimits[id]
		if !ok {
			continue
		}

		if resourceArn == "" || ul.ResourceArn == resourceArn {
			list = append(list, cloneServerlessUsageLimit(ul))
		}
	}

	if maxResults <= 0 {
		maxResults = serverlessDefaultPageSize()
	}

	startIdx := 0
	if nextToken != "" {
		if n, err := strconv.Atoi(nextToken); err == nil {
			startIdx = n
		}
	}

	if startIdx >= len(list) {
		return []*ServerlessUsageLimit{}, ""
	}

	end := startIdx + maxResults
	var outToken string

	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}

// UpdateServerlessUsageLimit updates a serverless usage limit.
func (b *InMemoryBackend) UpdateServerlessUsageLimit(
	usageLimitID, breachAction string,
	amount int64,
) (*ServerlessUsageLimit, error) {
	b.mu.Lock("UpdateServerlessUsageLimit")
	defer b.mu.Unlock()

	ul, ok := b.slUsageLimits[usageLimitID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: usage limit %q not found",
			ErrUsageLimitSLNotFound,
			usageLimitID,
		)
	}

	if amount > 0 {
		ul.Amount = amount
	}

	if breachAction != "" {
		ul.BreachAction = breachAction
	}

	return cloneServerlessUsageLimit(ul), nil
}

// DeleteServerlessUsageLimit deletes a serverless usage limit.
func (b *InMemoryBackend) DeleteServerlessUsageLimit(
	usageLimitID string,
) (*ServerlessUsageLimit, error) {
	b.mu.Lock("DeleteServerlessUsageLimit")
	defer b.mu.Unlock()

	ul, ok := b.slUsageLimits[usageLimitID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: usage limit %q not found",
			ErrUsageLimitSLNotFound,
			usageLimitID,
		)
	}

	cp := cloneServerlessUsageLimit(ul)
	delete(b.slUsageLimits, usageLimitID)
	b.slUsageLimitIdx.remove(usageLimitID)

	return cp, nil
}

func cloneServerlessUsageLimit(ul *ServerlessUsageLimit) *ServerlessUsageLimit {
	cp := *ul

	return &cp
}

// ---------------------------------------------------------------------------
// Serverless Scheduled Actions
// ---------------------------------------------------------------------------

// CreateServerlessScheduledAction creates a serverless scheduled action.
func (b *InMemoryBackend) CreateServerlessScheduledAction(
	scheduledActionName, namespaceName, schedule, targetAction string,
	startTime, endTime time.Time,
) (*ServerlessScheduledAction, error) {
	b.mu.Lock("CreateServerlessScheduledAction")
	defer b.mu.Unlock()

	if _, ok := b.slScheduledActions[scheduledActionName]; ok {
		return nil, fmt.Errorf(
			"%w: scheduled action %q already exists",
			ErrServerlessConflict,
			scheduledActionName,
		)
	}

	saArn := arn.Build(
		"redshift-serverless",
		b.region,
		b.accountID,
		"scheduledaction/"+scheduledActionName,
	)

	sa := &ServerlessScheduledAction{
		ScheduledActionArn:  saArn,
		ScheduledActionName: scheduledActionName,
		NamespaceName:       namespaceName,
		Schedule:            schedule,
		StartTime:           startTime,
		EndTime:             endTime,
		Status:              slStatusActive,
		TargetAction:        targetAction,
	}
	b.slScheduledActions[scheduledActionName] = sa
	b.slScheduledActionIdx.insert(scheduledActionName)

	return cloneServerlessScheduledAction(sa), nil
}

// GetServerlessScheduledAction returns a serverless scheduled action by name.
func (b *InMemoryBackend) GetServerlessScheduledAction(
	scheduledActionName string,
) (*ServerlessScheduledAction, error) {
	b.mu.RLock("GetServerlessScheduledAction")
	defer b.mu.RUnlock()

	sa, ok := b.slScheduledActions[scheduledActionName]
	if !ok {
		return nil, fmt.Errorf(
			"%w: scheduled action %q not found",
			ErrScheduledActionSLNotFound,
			scheduledActionName,
		)
	}

	return cloneServerlessScheduledAction(sa), nil
}

// ListServerlessScheduledActions returns all serverless scheduled actions.
//
//nolint:dupl // pagination pattern is structurally identical across serverless resource types
func (b *InMemoryBackend) ListServerlessScheduledActions(
	namespaceName string,
	maxResults int,
	nextToken string,
) ([]*ServerlessScheduledAction, string) {
	b.mu.RLock("ListServerlessScheduledActions")
	defer b.mu.RUnlock()

	// Iterate the pre-sorted index so results are ordered without re-sorting.
	keys := b.slScheduledActionIdx.ordered()
	list := make([]*ServerlessScheduledAction, 0, len(keys))

	for _, name := range keys {
		sa, ok := b.slScheduledActions[name]
		if !ok {
			continue
		}

		if namespaceName == "" || sa.NamespaceName == namespaceName {
			list = append(list, cloneServerlessScheduledAction(sa))
		}
	}

	if maxResults <= 0 {
		maxResults = serverlessDefaultPageSize()
	}

	startIdx := 0
	if nextToken != "" {
		if n, err := strconv.Atoi(nextToken); err == nil {
			startIdx = n
		}
	}

	if startIdx >= len(list) {
		return []*ServerlessScheduledAction{}, ""
	}

	end := startIdx + maxResults
	var outToken string

	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}

// UpdateServerlessScheduledAction updates a serverless scheduled action.
func (b *InMemoryBackend) UpdateServerlessScheduledAction(
	scheduledActionName, schedule, targetAction string,
	startTime, endTime time.Time,
) (*ServerlessScheduledAction, error) {
	b.mu.Lock("UpdateServerlessScheduledAction")
	defer b.mu.Unlock()

	sa, ok := b.slScheduledActions[scheduledActionName]
	if !ok {
		return nil, fmt.Errorf(
			"%w: scheduled action %q not found",
			ErrScheduledActionSLNotFound,
			scheduledActionName,
		)
	}

	if schedule != "" {
		sa.Schedule = schedule
	}

	if targetAction != "" {
		sa.TargetAction = targetAction
	}

	if !startTime.IsZero() {
		sa.StartTime = startTime
	}

	if !endTime.IsZero() {
		sa.EndTime = endTime
	}

	return cloneServerlessScheduledAction(sa), nil
}

// DeleteServerlessScheduledAction deletes a serverless scheduled action.
func (b *InMemoryBackend) DeleteServerlessScheduledAction(
	scheduledActionName string,
) (*ServerlessScheduledAction, error) {
	b.mu.Lock("DeleteServerlessScheduledAction")
	defer b.mu.Unlock()

	sa, ok := b.slScheduledActions[scheduledActionName]
	if !ok {
		return nil, fmt.Errorf(
			"%w: scheduled action %q not found",
			ErrScheduledActionSLNotFound,
			scheduledActionName,
		)
	}

	cp := cloneServerlessScheduledAction(sa)
	delete(b.slScheduledActions, scheduledActionName)
	b.slScheduledActionIdx.remove(scheduledActionName)

	return cp, nil
}

func cloneServerlessScheduledAction(sa *ServerlessScheduledAction) *ServerlessScheduledAction {
	cp := *sa

	return &cp
}
