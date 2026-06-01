package quicksight

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	errResourceNotFound  = "ResourceNotFoundException"
	errConflictException = "ConflictException"
	errValidation        = "InvalidParameterValueException"

	defaultNamespace         = "default"
	identityStoreQuickSight  = "QUICKSIGHT"
	statusCreationSuccessful = "CREATION_SUCCESSFUL"
	statusCreationInProgress = "CREATION_IN_PROGRESS"
	statusUpdateSuccessful   = "UPDATE_SUCCESSFUL"
	statusCreated            = "CREATED"
	statusDeleted            = "DELETED"
	statusRunning            = "RUNNING"
	statusCompleted          = "COMPLETED"
	statusCancelled          = "CANCELLED"

	defaultMaxResults = 100
)

var (
	// ErrNamespaceNotFound is returned when a namespace does not exist.
	ErrNamespaceNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrNamespaceAlreadyExists is returned when a namespace already exists.
	ErrNamespaceAlreadyExists = awserr.New(errConflictException, awserr.ErrAlreadyExists)
	// ErrGroupNotFound is returned when a group does not exist.
	ErrGroupNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrGroupAlreadyExists is returned when a group already exists.
	ErrGroupAlreadyExists = awserr.New(errConflictException, awserr.ErrAlreadyExists)
	// ErrGroupMemberNotFound is returned when a group member does not exist.
	ErrGroupMemberNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrGroupMemberAlreadyExists is returned when a group member already exists.
	ErrGroupMemberAlreadyExists = awserr.New(errConflictException, awserr.ErrAlreadyExists)
	// ErrUserNotFound is returned when a user does not exist.
	ErrUserNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrUserAlreadyExists is returned when a user already exists.
	ErrUserAlreadyExists = awserr.New(errConflictException, awserr.ErrAlreadyExists)
	// ErrDataSourceNotFound is returned when a data source does not exist.
	ErrDataSourceNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrDataSourceAlreadyExists is returned when a data source already exists.
	ErrDataSourceAlreadyExists = awserr.New(errConflictException, awserr.ErrAlreadyExists)
	// ErrDataSetNotFound is returned when a dataset does not exist.
	ErrDataSetNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrDataSetAlreadyExists is returned when a dataset already exists.
	ErrDataSetAlreadyExists = awserr.New(errConflictException, awserr.ErrAlreadyExists)
	// ErrIngestionNotFound is returned when an ingestion does not exist.
	ErrIngestionNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrIngestionAlreadyExists is returned when an ingestion already exists.
	ErrIngestionAlreadyExists = awserr.New(errConflictException, awserr.ErrAlreadyExists)
	// ErrDashboardNotFound is returned when a dashboard does not exist.
	ErrDashboardNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrDashboardAlreadyExists is returned when a dashboard already exists.
	ErrDashboardAlreadyExists = awserr.New(errConflictException, awserr.ErrAlreadyExists)
	// ErrAnalysisNotFound is returned when an analysis does not exist.
	ErrAnalysisNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrAnalysisAlreadyExists is returned when an analysis already exists.
	ErrAnalysisAlreadyExists = awserr.New(errConflictException, awserr.ErrAlreadyExists)
	// ErrValidation is returned on invalid input.
	ErrValidation = awserr.New(errValidation, awserr.ErrInvalidParameter)
	// ErrUnknownOperation is returned when the requested operation is not implemented.
	ErrUnknownOperation = errors.New("unknown operation")
)

type storedNamespace struct {
	Name           string `json:"name"`
	Arn            string `json:"arn"`
	CapacityRegion string `json:"capacityRegion"`
	Status         string `json:"status"`
	IdentityStore  string `json:"identityStore"`
}

func (n *storedNamespace) toNamespace() *Namespace {
	return &Namespace{
		Name:           n.Name,
		Arn:            n.Arn,
		CapacityRegion: n.CapacityRegion,
		CreationStatus: n.Status,
		IdentityStore:  n.IdentityStore,
	}
}

type storedGroup struct {
	GroupName   string `json:"groupName"`
	Arn         string `json:"arn"`
	Description string `json:"description"`
	Namespace   string `json:"namespace"`
	PrincipalID string `json:"principalId"`
}

func (g *storedGroup) toGroup() *Group {
	return &Group{
		GroupName:   g.GroupName,
		Arn:         g.Arn,
		Description: g.Description,
		Namespace:   g.Namespace,
		PrincipalID: g.PrincipalID,
	}
}

type storedUser struct {
	UserName     string `json:"userName"`
	Arn          string `json:"arn"`
	Email        string `json:"email"`
	Role         string `json:"role"`
	IdentityType string `json:"identityType"`
	Namespace    string `json:"namespace"`
	PrincipalID  string `json:"principalId"`
	SessionName  string `json:"sessionName"`
	Active       bool   `json:"active"`
}

func (u *storedUser) toUser() *User {
	return &User{
		UserName:     u.UserName,
		Arn:          u.Arn,
		Email:        u.Email,
		Role:         u.Role,
		IdentityType: u.IdentityType,
		Namespace:    u.Namespace,
		PrincipalID:  u.PrincipalID,
		SessionName:  u.SessionName,
		Active:       u.Active,
	}
}

type storedDataSource struct {
	CreatedTime     time.Time `json:"createdTime"`
	LastUpdatedTime time.Time `json:"lastUpdatedTime"`
	DataSourceID    string    `json:"dataSourceId"`
	Arn             string    `json:"arn"`
	Name            string    `json:"name"`
	Type            string    `json:"type"`
	Status          string    `json:"status"`
}

func (d *storedDataSource) toDataSource() *DataSource {
	return &DataSource{
		CreatedTime:     d.CreatedTime,
		LastUpdatedTime: d.LastUpdatedTime,
		DataSourceID:    d.DataSourceID,
		Arn:             d.Arn,
		Name:            d.Name,
		Type:            d.Type,
		Status:          d.Status,
	}
}

type storedDataSet struct {
	CreatedTime     time.Time `json:"createdTime"`
	LastUpdatedTime time.Time `json:"lastUpdatedTime"`
	DataSetID       string    `json:"dataSetId"`
	Arn             string    `json:"arn"`
	Name            string    `json:"name"`
	ImportMode      string    `json:"importMode"`
}

func (d *storedDataSet) toDataSet() *DataSet {
	return &DataSet{
		CreatedTime:     d.CreatedTime,
		LastUpdatedTime: d.LastUpdatedTime,
		DataSetID:       d.DataSetID,
		Arn:             d.Arn,
		Name:            d.Name,
		ImportMode:      d.ImportMode,
	}
}

type storedIngestion struct {
	CreatedTime     time.Time `json:"createdTime"`
	IngestionID     string    `json:"ingestionId"`
	Arn             string    `json:"arn"`
	DataSetID       string    `json:"dataSetId"`
	IngestionStatus string    `json:"ingestionStatus"`
}

func (i *storedIngestion) toIngestion() *Ingestion {
	return &Ingestion{
		CreatedTime:     i.CreatedTime,
		IngestionID:     i.IngestionID,
		Arn:             i.Arn,
		DataSetID:       i.DataSetID,
		IngestionStatus: i.IngestionStatus,
	}
}

type storedDashboard struct {
	CreatedTime     time.Time `json:"createdTime"`
	LastUpdatedTime time.Time `json:"lastUpdatedTime"`
	DashboardID     string    `json:"dashboardId"`
	Arn             string    `json:"arn"`
	Name            string    `json:"name"`
	Status          string    `json:"status"`
	VersionNumber   int64     `json:"versionNumber"`
}

func (d *storedDashboard) toDashboard() *Dashboard {
	return &Dashboard{
		CreatedTime:     d.CreatedTime,
		LastUpdatedTime: d.LastUpdatedTime,
		DashboardID:     d.DashboardID,
		Arn:             d.Arn,
		Name:            d.Name,
		Status:          d.Status,
		VersionNumber:   d.VersionNumber,
	}
}

type storedAnalysis struct {
	CreatedTime     time.Time `json:"createdTime"`
	LastUpdatedTime time.Time `json:"lastUpdatedTime"`
	AnalysisID      string    `json:"analysisId"`
	Arn             string    `json:"arn"`
	Name            string    `json:"name"`
	Status          string    `json:"status"`
}

func (a *storedAnalysis) toAnalysis() *Analysis {
	return &Analysis{
		CreatedTime:     a.CreatedTime,
		LastUpdatedTime: a.LastUpdatedTime,
		AnalysisID:      a.AnalysisID,
		Arn:             a.Arn,
		Name:            a.Name,
		Status:          a.Status,
	}
}

// state is the serializable snapshot of the backend.
type state struct {
	Namespaces   map[string]*storedNamespace  `json:"namespaces"`
	Groups       map[string]*storedGroup      `json:"groups"`
	GroupMembers map[string]bool              `json:"groupMembers"`
	Users        map[string]*storedUser       `json:"users"`
	DataSources  map[string]*storedDataSource `json:"dataSources"`
	DataSets     map[string]*storedDataSet    `json:"dataSets"`
	Ingestions   map[string]*storedIngestion  `json:"ingestions"`
	Dashboards   map[string]*storedDashboard  `json:"dashboards"`
	Analyses     map[string]*storedAnalysis   `json:"analyses"`
	Tags         map[string]map[string]string `json:"tags"`
}

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	mu           *lockmetrics.RWMutex
	namespaces   map[string]*storedNamespace
	groups       map[string]*storedGroup
	groupMembers map[string]bool
	users        map[string]*storedUser
	dataSources  map[string]*storedDataSource
	dataSets     map[string]*storedDataSet
	ingestions   map[string]*storedIngestion
	dashboards   map[string]*storedDashboard
	analyses     map[string]*storedAnalysis
	tags         map[string]map[string]string
	accountID    string
	region       string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		accountID:    accountID,
		region:       region,
		namespaces:   make(map[string]*storedNamespace),
		groups:       make(map[string]*storedGroup),
		groupMembers: make(map[string]bool),
		users:        make(map[string]*storedUser),
		dataSources:  make(map[string]*storedDataSource),
		dataSets:     make(map[string]*storedDataSet),
		ingestions:   make(map[string]*storedIngestion),
		dashboards:   make(map[string]*storedDashboard),
		analyses:     make(map[string]*storedAnalysis),
		tags:         make(map[string]map[string]string),
	}
	b.mu = lockmetrics.New("quicksight")

	// Pre-create the default namespace so basic operations work without explicit setup.
	b.namespaces[nsKey(accountID, defaultNamespace)] = &storedNamespace{
		Name:           defaultNamespace,
		Arn:            b.buildARN("namespace", defaultNamespace),
		CapacityRegion: region,
		Status:         statusCreationSuccessful,
		IdentityStore:  identityStoreQuickSight,
	}

	return b
}

// AccountID returns the configured account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the configured region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all stored state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.namespaces = make(map[string]*storedNamespace)
	b.groups = make(map[string]*storedGroup)
	b.groupMembers = make(map[string]bool)
	b.users = make(map[string]*storedUser)
	b.dataSources = make(map[string]*storedDataSource)
	b.dataSets = make(map[string]*storedDataSet)
	b.ingestions = make(map[string]*storedIngestion)
	b.dashboards = make(map[string]*storedDashboard)
	b.analyses = make(map[string]*storedAnalysis)
	b.tags = make(map[string]map[string]string)

	b.namespaces[nsKey(b.accountID, defaultNamespace)] = &storedNamespace{
		Name:           defaultNamespace,
		Arn:            b.buildARN("namespace", defaultNamespace),
		CapacityRegion: b.region,
		Status:         statusCreationSuccessful,
		IdentityStore:  identityStoreQuickSight,
	}
}

// Snapshot serializes backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	s := state{
		Namespaces:   b.namespaces,
		Groups:       b.groups,
		GroupMembers: b.groupMembers,
		Users:        b.users,
		DataSources:  b.dataSources,
		DataSets:     b.dataSets,
		Ingestions:   b.ingestions,
		Dashboards:   b.dashboards,
		Analyses:     b.analyses,
		Tags:         b.tags,
	}

	data, _ := json.Marshal(s)

	return data
}

// Restore deserializes backend state from JSON.
func (b *InMemoryBackend) Restore(data []byte) error {
	var s state
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("quicksight: restore: %w", err)
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.namespaces = s.Namespaces
	b.groups = s.Groups
	b.groupMembers = s.GroupMembers
	b.users = s.Users
	b.dataSources = s.DataSources
	b.dataSets = s.DataSets
	b.ingestions = s.Ingestions
	b.dashboards = s.Dashboards
	b.analyses = s.Analyses
	b.tags = s.Tags

	return nil
}

// ---- key helpers ----

func nsKey(accountID, namespace string) string {
	return accountID + "/" + namespace
}

func groupKey(accountID, namespace, groupName string) string {
	return accountID + "/" + namespace + "/" + groupName
}

func groupMemberKey(accountID, namespace, groupName, memberName string) string {
	return accountID + "/" + namespace + "/" + groupName + "/" + memberName
}

func userKey(accountID, namespace, userName string) string {
	return accountID + "/" + namespace + "/" + userName
}

func dataSourceKey(accountID, dataSourceID string) string {
	return accountID + "/" + dataSourceID
}

func dataSetKey(accountID, dataSetID string) string {
	return accountID + "/" + dataSetID
}

func ingestionKey(accountID, dataSetID, ingestionID string) string {
	return accountID + "/" + dataSetID + "/" + ingestionID
}

func dashboardKey(accountID, dashboardID string) string {
	return accountID + "/" + dashboardID
}

func analysisKey(accountID, analysisID string) string {
	return accountID + "/" + analysisID
}

// ---- ARN builder ----

func (b *InMemoryBackend) buildARN(resourceType, resourceID string) string {
	return fmt.Sprintf("arn:aws:quicksight:%s:%s:%s/%s", b.region, b.accountID, resourceType, resourceID)
}

// ---- Namespaces ----

func (b *InMemoryBackend) CreateNamespace(accountID, namespace, capacityRegion string) (*Namespace, error) {
	if namespace == "" {
		return nil, ErrValidation
	}

	if capacityRegion == "" {
		capacityRegion = b.region
	}

	b.mu.Lock("CreateNamespace")
	defer b.mu.Unlock()

	key := nsKey(accountID, namespace)
	if _, exists := b.namespaces[key]; exists {
		return nil, ErrNamespaceAlreadyExists
	}

	ns := &storedNamespace{
		Name:           namespace,
		Arn:            fmt.Sprintf("arn:aws:quicksight:%s:%s:namespace/%s", b.region, accountID, namespace),
		CapacityRegion: capacityRegion,
		Status:         statusCreationSuccessful,
		IdentityStore:  identityStoreQuickSight,
	}
	b.namespaces[key] = ns

	return ns.toNamespace(), nil
}

func (b *InMemoryBackend) DescribeNamespace(accountID, namespace string) (*Namespace, error) {
	b.mu.RLock("DescribeNamespace")
	defer b.mu.RUnlock()

	ns, ok := b.namespaces[nsKey(accountID, namespace)]
	if !ok {
		return nil, ErrNamespaceNotFound
	}

	return ns.toNamespace(), nil
}

func (b *InMemoryBackend) DeleteNamespace(accountID, namespace string) error {
	if namespace == defaultNamespace {
		return ErrValidation
	}

	b.mu.Lock("DeleteNamespace")
	defer b.mu.Unlock()

	key := nsKey(accountID, namespace)
	if _, ok := b.namespaces[key]; !ok {
		return ErrNamespaceNotFound
	}

	delete(b.namespaces, key)

	return nil
}

func (b *InMemoryBackend) ListNamespaces(
	accountID string,
	maxResults int32,
	nextToken string,
) ([]*Namespace, string, error) {
	b.mu.RLock("ListNamespaces")
	defer b.mu.RUnlock()

	var all []*storedNamespace
	prefix := accountID + "/"
	for k, ns := range b.namespaces {
		if strings.HasPrefix(k, prefix) {
			all = append(all, ns)
		}
	}

	result, next := paginateNamespaces(all, maxResults, nextToken)

	return result, next, nil
}

func paginateNamespaces(all []*storedNamespace, maxResults int32, nextToken string) ([]*Namespace, string) {
	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, ns := range all {
			if ns.Name == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].Name
	} else {
		end = len(all)
	}

	result := make([]*Namespace, 0, end-start)
	for _, ns := range all[start:end] {
		result = append(result, ns.toNamespace())
	}

	return result, next
}

// ---- Groups ----

func (b *InMemoryBackend) CreateGroup(accountID, namespace, groupName, description string) (*Group, error) {
	if groupName == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateGroup")
	defer b.mu.Unlock()

	if _, ok := b.namespaces[nsKey(accountID, namespace)]; !ok {
		return nil, ErrNamespaceNotFound
	}

	key := groupKey(accountID, namespace, groupName)
	if _, exists := b.groups[key]; exists {
		return nil, ErrGroupAlreadyExists
	}

	g := &storedGroup{
		GroupName:   groupName,
		Arn:         fmt.Sprintf("arn:aws:quicksight:%s:%s:group/%s/%s", b.region, accountID, namespace, groupName),
		Description: description,
		Namespace:   namespace,
		PrincipalID: uuid.New().String(),
	}
	b.groups[key] = g

	return g.toGroup(), nil
}

func (b *InMemoryBackend) DescribeGroup(accountID, namespace, groupName string) (*Group, error) {
	b.mu.RLock("DescribeGroup")
	defer b.mu.RUnlock()

	g, ok := b.groups[groupKey(accountID, namespace, groupName)]
	if !ok {
		return nil, ErrGroupNotFound
	}

	return g.toGroup(), nil
}

func (b *InMemoryBackend) UpdateGroup(accountID, namespace, groupName, description string) (*Group, error) {
	b.mu.Lock("UpdateGroup")
	defer b.mu.Unlock()

	key := groupKey(accountID, namespace, groupName)
	g, ok := b.groups[key]
	if !ok {
		return nil, ErrGroupNotFound
	}

	g.Description = description

	return g.toGroup(), nil
}

func (b *InMemoryBackend) DeleteGroup(accountID, namespace, groupName string) error {
	b.mu.Lock("DeleteGroup")
	defer b.mu.Unlock()

	key := groupKey(accountID, namespace, groupName)
	if _, ok := b.groups[key]; !ok {
		return ErrGroupNotFound
	}

	delete(b.groups, key)

	// Remove all memberships for this group.
	prefix := groupKey(accountID, namespace, groupName) + "/"
	for k := range b.groupMembers {
		if strings.HasPrefix(k, prefix) {
			delete(b.groupMembers, k)
		}
	}

	return nil
}

func (b *InMemoryBackend) ListGroups(
	accountID, namespace string,
	maxResults int32,
	nextToken string,
) ([]*Group, string, error) {
	b.mu.RLock("ListGroups")
	defer b.mu.RUnlock()

	prefix := groupKey(accountID, namespace, "") + "/"
	// prefix for groups in this namespace: "accountID/namespace/"
	nsPrefix := accountID + "/" + namespace + "/"
	var all []*storedGroup
	for k, g := range b.groups {
		if strings.HasPrefix(k, nsPrefix) && !strings.Contains(k[len(nsPrefix):], "/") {
			_ = prefix
			all = append(all, g)
		}
	}

	result, next := paginateGroups(all, maxResults, nextToken)

	return result, next, nil
}

func (b *InMemoryBackend) SearchGroups(
	accountID, namespace, query string,
	maxResults int32,
	nextToken string,
) ([]*Group, string, error) {
	b.mu.RLock("SearchGroups")
	defer b.mu.RUnlock()

	nsPrefix := accountID + "/" + namespace + "/"
	var all []*storedGroup
	for k, g := range b.groups {
		if strings.HasPrefix(k, nsPrefix) && !strings.Contains(k[len(nsPrefix):], "/") {
			if query == "" || strings.Contains(strings.ToLower(g.GroupName), strings.ToLower(query)) {
				all = append(all, g)
			}
		}
	}

	result, next := paginateGroups(all, maxResults, nextToken)

	return result, next, nil
}

func paginateGroups(all []*storedGroup, maxResults int32, nextToken string) ([]*Group, string) {
	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, g := range all {
			if g.GroupName == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].GroupName
	} else {
		end = len(all)
	}

	result := make([]*Group, 0, end-start)
	for _, g := range all[start:end] {
		result = append(result, g.toGroup())
	}

	return result, next
}

// ---- Group Memberships ----

func (b *InMemoryBackend) CreateGroupMembership(
	accountID, namespace, groupName, memberName string,
) (*GroupMember, error) {
	b.mu.Lock("CreateGroupMembership")
	defer b.mu.Unlock()

	if _, ok := b.groups[groupKey(accountID, namespace, groupName)]; !ok {
		return nil, ErrGroupNotFound
	}

	key := groupMemberKey(accountID, namespace, groupName, memberName)
	if b.groupMembers[key] {
		return nil, ErrGroupMemberAlreadyExists
	}

	b.groupMembers[key] = true

	return &GroupMember{
		MemberName: memberName,
		Arn:        fmt.Sprintf("arn:aws:quicksight:%s:%s:user/%s/%s", b.region, accountID, namespace, memberName),
	}, nil
}

func (b *InMemoryBackend) DescribeGroupMembership(
	accountID, namespace, groupName, memberName string,
) (*GroupMember, error) {
	b.mu.RLock("DescribeGroupMembership")
	defer b.mu.RUnlock()

	if !b.groupMembers[groupMemberKey(accountID, namespace, groupName, memberName)] {
		return nil, ErrGroupMemberNotFound
	}

	return &GroupMember{
		MemberName: memberName,
		Arn:        fmt.Sprintf("arn:aws:quicksight:%s:%s:user/%s/%s", b.region, accountID, namespace, memberName),
	}, nil
}

func (b *InMemoryBackend) DeleteGroupMembership(accountID, namespace, groupName, memberName string) error {
	b.mu.Lock("DeleteGroupMembership")
	defer b.mu.Unlock()

	key := groupMemberKey(accountID, namespace, groupName, memberName)
	if !b.groupMembers[key] {
		return ErrGroupMemberNotFound
	}

	delete(b.groupMembers, key)

	return nil
}

func (b *InMemoryBackend) ListGroupMemberships(
	accountID, namespace, groupName string,
	maxResults int32,
	nextToken string,
) ([]*GroupMember, string, error) {
	b.mu.RLock("ListGroupMemberships")
	defer b.mu.RUnlock()

	if _, ok := b.groups[groupKey(accountID, namespace, groupName)]; !ok {
		return nil, "", ErrGroupNotFound
	}

	prefix := groupMemberKey(accountID, namespace, groupName, "") + "/"
	_ = prefix
	fullPrefix := accountID + "/" + namespace + "/" + groupName + "/"
	var members []string
	for k := range b.groupMembers {
		if member, ok := strings.CutPrefix(k, fullPrefix); ok {
			members = append(members, member)
		}
	}

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, m := range members {
			if m == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(members) {
		next = members[end]
	} else {
		end = len(members)
	}

	result := make([]*GroupMember, 0, end-start)
	for _, m := range members[start:end] {
		result = append(result, &GroupMember{
			MemberName: m,
			Arn:        fmt.Sprintf("arn:aws:quicksight:%s:%s:user/%s/%s", b.region, accountID, namespace, m),
		})
	}

	return result, next, nil
}

// ---- Users ----

func (b *InMemoryBackend) RegisterUser(
	accountID, namespace, userName, email, role, identityType, sessionName string,
) (*User, error) {
	if userName == "" || email == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("RegisterUser")
	defer b.mu.Unlock()

	if _, ok := b.namespaces[nsKey(accountID, namespace)]; !ok {
		return nil, ErrNamespaceNotFound
	}

	key := userKey(accountID, namespace, userName)
	if _, exists := b.users[key]; exists {
		return nil, ErrUserAlreadyExists
	}

	if role == "" {
		role = "READER"
	}
	if identityType == "" {
		identityType = identityStoreQuickSight
	}

	u := &storedUser{
		UserName:     userName,
		Arn:          fmt.Sprintf("arn:aws:quicksight:%s:%s:user/%s/%s", b.region, accountID, namespace, userName),
		Email:        email,
		Role:         role,
		IdentityType: identityType,
		Namespace:    namespace,
		PrincipalID:  uuid.New().String(),
		SessionName:  sessionName,
		Active:       true,
	}
	b.users[key] = u

	return u.toUser(), nil
}

func (b *InMemoryBackend) DescribeUser(accountID, namespace, userName string) (*User, error) {
	b.mu.RLock("DescribeUser")
	defer b.mu.RUnlock()

	u, ok := b.users[userKey(accountID, namespace, userName)]
	if !ok {
		return nil, ErrUserNotFound
	}

	return u.toUser(), nil
}

func (b *InMemoryBackend) UpdateUser(accountID, namespace, userName, email, role string) (*User, error) {
	b.mu.Lock("UpdateUser")
	defer b.mu.Unlock()

	key := userKey(accountID, namespace, userName)
	u, ok := b.users[key]
	if !ok {
		return nil, ErrUserNotFound
	}

	if email != "" {
		u.Email = email
	}
	if role != "" {
		u.Role = role
	}

	return u.toUser(), nil
}

func (b *InMemoryBackend) DeleteUser(accountID, namespace, userName string) error {
	b.mu.Lock("DeleteUser")
	defer b.mu.Unlock()

	key := userKey(accountID, namespace, userName)
	if _, ok := b.users[key]; !ok {
		return ErrUserNotFound
	}

	delete(b.users, key)

	return nil
}

func (b *InMemoryBackend) DeleteUserByPrincipalID(accountID, namespace, principalID string) error {
	b.mu.Lock("DeleteUserByPrincipalID")
	defer b.mu.Unlock()

	prefix := accountID + "/" + namespace + "/"
	for k, u := range b.users {
		if strings.HasPrefix(k, prefix) && u.PrincipalID == principalID {
			delete(b.users, k)

			return nil
		}
	}

	return ErrUserNotFound
}

//nolint:dupl // list functions share structure but operate on different stored types
func (b *InMemoryBackend) ListUsers(
	accountID, namespace string,
	maxResults int32,
	nextToken string,
) ([]*User, string, error) {
	b.mu.RLock("ListUsers")
	defer b.mu.RUnlock()

	prefix := accountID + "/" + namespace + "/"
	var all []*storedUser
	for k, u := range b.users {
		if strings.HasPrefix(k, prefix) {
			all = append(all, u)
		}
	}

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, u := range all {
			if u.UserName == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].UserName
	} else {
		end = len(all)
	}

	result := make([]*User, 0, end-start)
	for _, u := range all[start:end] {
		result = append(result, u.toUser())
	}

	return result, next, nil
}

func (b *InMemoryBackend) ListUserGroups(
	accountID, namespace, userName string,
	maxResults int32,
	nextToken string,
) ([]*Group, string, error) {
	b.mu.RLock("ListUserGroups")
	defer b.mu.RUnlock()

	if _, ok := b.users[userKey(accountID, namespace, userName)]; !ok {
		return nil, "", ErrUserNotFound
	}

	nsPrefix := accountID + "/" + namespace + "/"
	var all []*storedGroup
	for gKey, g := range b.groups {
		if !strings.HasPrefix(gKey, nsPrefix) {
			continue
		}
		memberKey := groupMemberKey(accountID, namespace, g.GroupName, userName)
		if b.groupMembers[memberKey] {
			all = append(all, g)
		}
	}

	result, next := paginateGroups(all, maxResults, nextToken)

	return result, next, nil
}

// ---- DataSources ----

func (b *InMemoryBackend) CreateDataSource(
	accountID, dataSourceID, name, dsType string,
	tags map[string]string,
) (*DataSource, error) {
	if dataSourceID == "" || name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateDataSource")
	defer b.mu.Unlock()

	key := dataSourceKey(accountID, dataSourceID)
	if _, exists := b.dataSources[key]; exists {
		return nil, ErrDataSourceAlreadyExists
	}

	now := time.Now().UTC()
	ds := &storedDataSource{
		CreatedTime:     now,
		LastUpdatedTime: now,
		DataSourceID:    dataSourceID,
		Arn:             fmt.Sprintf("arn:aws:quicksight:%s:%s:datasource/%s", b.region, accountID, dataSourceID),
		Name:            name,
		Type:            dsType,
		Status:          statusCreationSuccessful,
	}
	b.dataSources[key] = ds

	if len(tags) > 0 {
		b.tags[ds.Arn] = maps.Clone(tags)
	}

	return ds.toDataSource(), nil
}

func (b *InMemoryBackend) DescribeDataSource(accountID, dataSourceID string) (*DataSource, error) {
	b.mu.RLock("DescribeDataSource")
	defer b.mu.RUnlock()

	ds, ok := b.dataSources[dataSourceKey(accountID, dataSourceID)]
	if !ok {
		return nil, ErrDataSourceNotFound
	}

	return ds.toDataSource(), nil
}

func (b *InMemoryBackend) UpdateDataSource(accountID, dataSourceID, name string) (*DataSource, error) {
	b.mu.Lock("UpdateDataSource")
	defer b.mu.Unlock()

	key := dataSourceKey(accountID, dataSourceID)
	ds, ok := b.dataSources[key]
	if !ok {
		return nil, ErrDataSourceNotFound
	}

	if name != "" {
		ds.Name = name
	}
	ds.LastUpdatedTime = time.Now().UTC()
	ds.Status = statusUpdateSuccessful

	return ds.toDataSource(), nil
}

func (b *InMemoryBackend) DeleteDataSource(accountID, dataSourceID string) error {
	b.mu.Lock("DeleteDataSource")
	defer b.mu.Unlock()

	key := dataSourceKey(accountID, dataSourceID)
	ds, ok := b.dataSources[key]
	if !ok {
		return ErrDataSourceNotFound
	}

	delete(b.tags, ds.Arn)
	delete(b.dataSources, key)

	return nil
}

//nolint:dupl // list functions share structure but operate on different stored types
func (b *InMemoryBackend) ListDataSources(
	accountID string,
	maxResults int32,
	nextToken string,
) ([]*DataSource, string, error) {
	b.mu.RLock("ListDataSources")
	defer b.mu.RUnlock()

	prefix := accountID + "/"
	var all []*storedDataSource
	for k, ds := range b.dataSources {
		if strings.HasPrefix(k, prefix) {
			all = append(all, ds)
		}
	}

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, ds := range all {
			if ds.DataSourceID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].DataSourceID
	} else {
		end = len(all)
	}

	result := make([]*DataSource, 0, end-start)
	for _, ds := range all[start:end] {
		result = append(result, ds.toDataSource())
	}

	return result, next, nil
}

// ---- DataSets ----

func (b *InMemoryBackend) CreateDataSet(
	accountID, dataSetID, name, importMode string,
	tags map[string]string,
) (*DataSet, error) {
	if dataSetID == "" || name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateDataSet")
	defer b.mu.Unlock()

	key := dataSetKey(accountID, dataSetID)
	if _, exists := b.dataSets[key]; exists {
		return nil, ErrDataSetAlreadyExists
	}

	if importMode == "" {
		importMode = "SPICE"
	}

	now := time.Now().UTC()
	ds := &storedDataSet{
		CreatedTime:     now,
		LastUpdatedTime: now,
		DataSetID:       dataSetID,
		Arn:             fmt.Sprintf("arn:aws:quicksight:%s:%s:dataset/%s", b.region, accountID, dataSetID),
		Name:            name,
		ImportMode:      importMode,
	}
	b.dataSets[key] = ds

	if len(tags) > 0 {
		b.tags[ds.Arn] = maps.Clone(tags)
	}

	return ds.toDataSet(), nil
}

func (b *InMemoryBackend) DescribeDataSet(accountID, dataSetID string) (*DataSet, error) {
	b.mu.RLock("DescribeDataSet")
	defer b.mu.RUnlock()

	ds, ok := b.dataSets[dataSetKey(accountID, dataSetID)]
	if !ok {
		return nil, ErrDataSetNotFound
	}

	return ds.toDataSet(), nil
}

func (b *InMemoryBackend) UpdateDataSet(accountID, dataSetID, name, importMode string) (*DataSet, error) {
	b.mu.Lock("UpdateDataSet")
	defer b.mu.Unlock()

	key := dataSetKey(accountID, dataSetID)
	ds, ok := b.dataSets[key]
	if !ok {
		return nil, ErrDataSetNotFound
	}

	if name != "" {
		ds.Name = name
	}
	if importMode != "" {
		ds.ImportMode = importMode
	}
	ds.LastUpdatedTime = time.Now().UTC()

	return ds.toDataSet(), nil
}

func (b *InMemoryBackend) DeleteDataSet(accountID, dataSetID string) error {
	b.mu.Lock("DeleteDataSet")
	defer b.mu.Unlock()

	key := dataSetKey(accountID, dataSetID)
	ds, ok := b.dataSets[key]
	if !ok {
		return ErrDataSetNotFound
	}

	delete(b.tags, ds.Arn)
	delete(b.dataSets, key)

	return nil
}

//nolint:dupl // list functions share structure but operate on different stored types
func (b *InMemoryBackend) ListDataSets(
	accountID string,
	maxResults int32,
	nextToken string,
) ([]*DataSet, string, error) {
	b.mu.RLock("ListDataSets")
	defer b.mu.RUnlock()

	prefix := accountID + "/"
	var all []*storedDataSet
	for k, ds := range b.dataSets {
		if strings.HasPrefix(k, prefix) {
			all = append(all, ds)
		}
	}

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, ds := range all {
			if ds.DataSetID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].DataSetID
	} else {
		end = len(all)
	}

	result := make([]*DataSet, 0, end-start)
	for _, ds := range all[start:end] {
		result = append(result, ds.toDataSet())
	}

	return result, next, nil
}

// ---- Ingestions ----

func (b *InMemoryBackend) CreateIngestion(accountID, dataSetID, ingestionID string) (*Ingestion, error) {
	b.mu.Lock("CreateIngestion")
	defer b.mu.Unlock()

	if _, ok := b.dataSets[dataSetKey(accountID, dataSetID)]; !ok {
		return nil, ErrDataSetNotFound
	}

	key := ingestionKey(accountID, dataSetID, ingestionID)
	if _, exists := b.ingestions[key]; exists {
		return nil, ErrIngestionAlreadyExists
	}

	ing := &storedIngestion{
		CreatedTime: time.Now().UTC(),
		IngestionID: ingestionID,
		Arn: fmt.Sprintf(
			"arn:aws:quicksight:%s:%s:dataset/%s/ingestion/%s",
			b.region,
			accountID,
			dataSetID,
			ingestionID,
		),
		DataSetID:       dataSetID,
		IngestionStatus: statusRunning,
	}
	b.ingestions[key] = ing

	return ing.toIngestion(), nil
}

func (b *InMemoryBackend) DescribeIngestion(accountID, dataSetID, ingestionID string) (*Ingestion, error) {
	b.mu.RLock("DescribeIngestion")
	defer b.mu.RUnlock()

	ing, ok := b.ingestions[ingestionKey(accountID, dataSetID, ingestionID)]
	if !ok {
		return nil, ErrIngestionNotFound
	}

	return ing.toIngestion(), nil
}

func (b *InMemoryBackend) CancelIngestion(accountID, dataSetID, ingestionID string) error {
	b.mu.Lock("CancelIngestion")
	defer b.mu.Unlock()

	key := ingestionKey(accountID, dataSetID, ingestionID)
	ing, ok := b.ingestions[key]
	if !ok {
		return ErrIngestionNotFound
	}

	ing.IngestionStatus = statusCancelled

	return nil
}

//nolint:dupl // list functions share structure but operate on different stored types
func (b *InMemoryBackend) ListIngestions(
	accountID, dataSetID string,
	maxResults int32,
	nextToken string,
) ([]*Ingestion, string, error) {
	b.mu.RLock("ListIngestions")
	defer b.mu.RUnlock()

	prefix := accountID + "/" + dataSetID + "/"
	var all []*storedIngestion
	for k, ing := range b.ingestions {
		if strings.HasPrefix(k, prefix) {
			all = append(all, ing)
		}
	}

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, ing := range all {
			if ing.IngestionID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].IngestionID
	} else {
		end = len(all)
	}

	result := make([]*Ingestion, 0, end-start)
	for _, ing := range all[start:end] {
		result = append(result, ing.toIngestion())
	}

	return result, next, nil
}

// ---- Dashboards ----

func (b *InMemoryBackend) CreateDashboard(
	accountID, dashboardID, name string,
	tags map[string]string,
) (*Dashboard, error) {
	if dashboardID == "" || name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateDashboard")
	defer b.mu.Unlock()

	key := dashboardKey(accountID, dashboardID)
	if _, exists := b.dashboards[key]; exists {
		return nil, ErrDashboardAlreadyExists
	}

	now := time.Now().UTC()
	d := &storedDashboard{
		CreatedTime:     now,
		LastUpdatedTime: now,
		DashboardID:     dashboardID,
		Arn:             fmt.Sprintf("arn:aws:quicksight:%s:%s:dashboard/%s", b.region, accountID, dashboardID),
		Name:            name,
		Status:          statusCreated,
		VersionNumber:   1,
	}
	b.dashboards[key] = d

	if len(tags) > 0 {
		b.tags[d.Arn] = maps.Clone(tags)
	}

	return d.toDashboard(), nil
}

func (b *InMemoryBackend) DescribeDashboard(accountID, dashboardID string) (*Dashboard, error) {
	b.mu.RLock("DescribeDashboard")
	defer b.mu.RUnlock()

	d, ok := b.dashboards[dashboardKey(accountID, dashboardID)]
	if !ok {
		return nil, ErrDashboardNotFound
	}

	return d.toDashboard(), nil
}

func (b *InMemoryBackend) UpdateDashboard(accountID, dashboardID, name string) (*Dashboard, error) {
	b.mu.Lock("UpdateDashboard")
	defer b.mu.Unlock()

	key := dashboardKey(accountID, dashboardID)
	d, ok := b.dashboards[key]
	if !ok {
		return nil, ErrDashboardNotFound
	}

	if name != "" {
		d.Name = name
	}
	d.LastUpdatedTime = time.Now().UTC()
	d.VersionNumber++

	return d.toDashboard(), nil
}

func (b *InMemoryBackend) DeleteDashboard(accountID, dashboardID string) error {
	b.mu.Lock("DeleteDashboard")
	defer b.mu.Unlock()

	key := dashboardKey(accountID, dashboardID)
	d, ok := b.dashboards[key]
	if !ok {
		return ErrDashboardNotFound
	}

	delete(b.tags, d.Arn)
	delete(b.dashboards, key)

	return nil
}

//nolint:dupl // list functions share structure but operate on different stored types
func (b *InMemoryBackend) ListDashboards(
	accountID string,
	maxResults int32,
	nextToken string,
) ([]*Dashboard, string, error) {
	b.mu.RLock("ListDashboards")
	defer b.mu.RUnlock()

	prefix := accountID + "/"
	var all []*storedDashboard
	for k, d := range b.dashboards {
		if strings.HasPrefix(k, prefix) {
			all = append(all, d)
		}
	}

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, d := range all {
			if d.DashboardID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].DashboardID
	} else {
		end = len(all)
	}

	result := make([]*Dashboard, 0, end-start)
	for _, d := range all[start:end] {
		result = append(result, d.toDashboard())
	}

	return result, next, nil
}

func (b *InMemoryBackend) ListDashboardVersions(
	accountID, dashboardID string,
	_ int32,
	_ string,
) ([]*DashboardVersion, string, error) {
	b.mu.RLock("ListDashboardVersions")
	defer b.mu.RUnlock()

	d, ok := b.dashboards[dashboardKey(accountID, dashboardID)]
	if !ok {
		return nil, "", ErrDashboardNotFound
	}

	versions := make([]*DashboardVersion, 0, d.VersionNumber)
	for i := int64(1); i <= d.VersionNumber; i++ {
		versions = append(versions, &DashboardVersion{
			CreatedTime:   d.CreatedTime,
			Arn:           fmt.Sprintf("%s/version/%d", d.Arn, i),
			Status:        statusCreated,
			VersionNumber: i,
		})
	}

	return versions, "", nil
}

// ---- Analyses ----

func (b *InMemoryBackend) CreateAnalysis(
	accountID, analysisID, name string,
	tags map[string]string,
) (*Analysis, error) {
	if analysisID == "" || name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateAnalysis")
	defer b.mu.Unlock()

	key := analysisKey(accountID, analysisID)
	if _, exists := b.analyses[key]; exists {
		return nil, ErrAnalysisAlreadyExists
	}

	now := time.Now().UTC()
	a := &storedAnalysis{
		CreatedTime:     now,
		LastUpdatedTime: now,
		AnalysisID:      analysisID,
		Arn:             fmt.Sprintf("arn:aws:quicksight:%s:%s:analysis/%s", b.region, accountID, analysisID),
		Name:            name,
		Status:          statusCreationSuccessful,
	}
	b.analyses[key] = a

	if len(tags) > 0 {
		b.tags[a.Arn] = maps.Clone(tags)
	}

	return a.toAnalysis(), nil
}

func (b *InMemoryBackend) DescribeAnalysis(accountID, analysisID string) (*Analysis, error) {
	b.mu.RLock("DescribeAnalysis")
	defer b.mu.RUnlock()

	a, ok := b.analyses[analysisKey(accountID, analysisID)]
	if !ok {
		return nil, ErrAnalysisNotFound
	}

	return a.toAnalysis(), nil
}

func (b *InMemoryBackend) UpdateAnalysis(accountID, analysisID, name string) (*Analysis, error) {
	b.mu.Lock("UpdateAnalysis")
	defer b.mu.Unlock()

	key := analysisKey(accountID, analysisID)
	a, ok := b.analyses[key]
	if !ok {
		return nil, ErrAnalysisNotFound
	}

	if name != "" {
		a.Name = name
	}
	a.LastUpdatedTime = time.Now().UTC()
	a.Status = statusUpdateSuccessful

	return a.toAnalysis(), nil
}

func (b *InMemoryBackend) DeleteAnalysis(accountID, analysisID string, forceDeleteWithoutRecovery bool) error {
	b.mu.Lock("DeleteAnalysis")
	defer b.mu.Unlock()

	key := analysisKey(accountID, analysisID)
	a, ok := b.analyses[key]
	if !ok {
		return ErrAnalysisNotFound
	}

	if forceDeleteWithoutRecovery {
		delete(b.tags, a.Arn)
		delete(b.analyses, key)
	} else {
		a.Status = statusDeleted
	}

	return nil
}

//nolint:dupl // list functions share structure but operate on different stored types
func (b *InMemoryBackend) ListAnalyses(
	accountID string,
	maxResults int32,
	nextToken string,
) ([]*Analysis, string, error) {
	b.mu.RLock("ListAnalyses")
	defer b.mu.RUnlock()

	prefix := accountID + "/"
	var all []*storedAnalysis
	for k, a := range b.analyses {
		if strings.HasPrefix(k, prefix) {
			all = append(all, a)
		}
	}

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, a := range all {
			if a.AnalysisID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].AnalysisID
	} else {
		end = len(all)
	}

	result := make([]*Analysis, 0, end-start)
	for _, a := range all[start:end] {
		result = append(result, a.toAnalysis())
	}

	return result, next, nil
}

func (b *InMemoryBackend) RestoreAnalysis(accountID, analysisID string) (*Analysis, error) {
	b.mu.Lock("RestoreAnalysis")
	defer b.mu.Unlock()

	key := analysisKey(accountID, analysisID)
	a, ok := b.analyses[key]
	if !ok {
		return nil, ErrAnalysisNotFound
	}

	a.Status = statusCreationSuccessful
	a.LastUpdatedTime = time.Now().UTC()

	return a.toAnalysis(), nil
}

// ---- Tags ----

func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}
	maps.Copy(b.tags[resourceARN], tags)

	return nil
}

func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if b.tags[resourceARN] == nil {
		return nil
	}
	for _, k := range tagKeys {
		delete(b.tags[resourceARN], k)
	}

	return nil
}

func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	result := make(map[string]string)
	if t := b.tags[resourceARN]; t != nil {
		maps.Copy(result, t)
	}

	return result, nil
}
