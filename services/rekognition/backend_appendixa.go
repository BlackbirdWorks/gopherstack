package rekognition

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	maxAsyncJobs         = 10_000
	maxMediaAnalysisJobs = 10_000
)

var (
	// ErrProjectNotFound is returned when a project does not exist.
	ErrProjectNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrProjectVersionNotFound is returned when a project version does not exist.
	ErrProjectVersionNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrDatasetNotFound is returned when a dataset does not exist.
	ErrDatasetNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrUserNotFound is returned when a user does not exist.
	ErrUserNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrLivenessSessionNotFound is returned when a liveness session does not exist.
	ErrLivenessSessionNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrAsyncJobNotFound is returned when an async job does not exist.
	ErrAsyncJobNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrMediaAnalysisJobNotFound is returned when a media analysis job does not exist.
	ErrMediaAnalysisJobNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
)

// storedProject holds a Rekognition Custom Labels project.
type storedProject struct {
	CreationTimestamp time.Time `json:"creationTimestamp"`
	ProjectARN        string    `json:"projectArn"`
	Status            string    `json:"status"`
}

func (p *storedProject) toProject() *Project {
	return &Project{
		CreationTimestamp: p.CreationTimestamp,
		ProjectARN:        p.ProjectARN,
		Status:            p.Status,
	}
}

// storedProjectVersion holds a model version within a project.
type storedProjectVersion struct {
	CreationTimestamp time.Time `json:"creationTimestamp"`
	ProjectVersionARN string    `json:"projectVersionArn"`
	ProjectARN        string    `json:"projectArn"`
	VersionName       string    `json:"versionName"`
	Status            string    `json:"status"`
	StatusMessage     string    `json:"statusMessage"`
	MinInferenceUnits int32     `json:"minInferenceUnits"`
}

func (v *storedProjectVersion) toProjectVersion() *ProjectVersion {
	return &ProjectVersion{
		CreationTimestamp: v.CreationTimestamp,
		ProjectVersionARN: v.ProjectVersionARN,
		ProjectARN:        v.ProjectARN,
		VersionName:       v.VersionName,
		Status:            v.Status,
		StatusMessage:     v.StatusMessage,
		MinInferenceUnits: v.MinInferenceUnits,
	}
}

// storedProjectPolicy holds a project policy.
type storedProjectPolicy struct {
	CreationTimestamp    time.Time `json:"creationTimestamp"`
	LastUpdatedTimestamp time.Time `json:"lastUpdatedTimestamp"`
	ProjectARN           string    `json:"projectArn"`
	PolicyName           string    `json:"policyName"`
	PolicyRevisionID     string    `json:"policyRevisionId"`
	PolicyDocument       string    `json:"policyDocument"`
}

func (p *storedProjectPolicy) toProjectPolicy() *ProjectPolicy {
	return &ProjectPolicy{
		CreationTimestamp:    p.CreationTimestamp,
		LastUpdatedTimestamp: p.LastUpdatedTimestamp,
		ProjectARN:           p.ProjectARN,
		PolicyName:           p.PolicyName,
		PolicyRevisionID:     p.PolicyRevisionID,
		PolicyDocument:       p.PolicyDocument,
	}
}

// storedDataset holds a Rekognition Custom Labels dataset.
type storedDataset struct {
	CreationTimestamp    time.Time `json:"creationTimestamp"`
	LastUpdatedTimestamp time.Time `json:"lastUpdatedTimestamp"`
	DatasetARN           string    `json:"datasetArn"`
	ProjectARN           string    `json:"projectArn"`
	DatasetType          string    `json:"datasetType"`
	Status               string    `json:"status"`
	StatusMessage        string    `json:"statusMessage"`
}

func (d *storedDataset) toDataset() *Dataset {
	return &Dataset{
		CreationTimestamp:    d.CreationTimestamp,
		LastUpdatedTimestamp: d.LastUpdatedTimestamp,
		DatasetARN:           d.DatasetARN,
		ProjectARN:           d.ProjectARN,
		DatasetType:          d.DatasetType,
		Status:               d.Status,
		StatusMessage:        d.StatusMessage,
	}
}

// storedUser holds a Rekognition user in a collection. CollectionID is
// additive (Phase 3.3): the nested map[string]map[string]*storedUser this
// table replaced implied CollectionID only via its outer key, so the field
// is added here to give the flattened table a composite key. storedUser is
// never marshaled to an AWS-facing response directly (see toUser), so no
// json:"-" hiding is needed -- unlike a live *T type that IS part of the AWS
// wire response shape.
type storedUser struct {
	CollectionID string   `json:"collectionId"`
	UserID       string   `json:"userId"`
	UserStatus   string   `json:"userStatus"`
	FaceIDs      []string `json:"faceIds"`
}

func (u *storedUser) toUser() *User {
	return &User{
		UserID:     u.UserID,
		UserStatus: u.UserStatus,
	}
}

// storedLivenessSession holds a face liveness session.
type storedLivenessSession struct {
	SessionID  string  `json:"sessionId"`
	Status     string  `json:"status"`
	Confidence float32 `json:"confidence"`
}

// storedAsyncJob holds an async video analysis job.
type storedAsyncJob struct {
	JobID        string `json:"jobId"`
	JobType      string `json:"jobType"`
	CollectionID string `json:"collectionId"`
	JobStatus    string `json:"jobStatus"`
	PollCount    int    `json:"pollCount"`
}

// storedMediaAnalysisJob holds a media analysis job.
type storedMediaAnalysisJob struct {
	CreationTimestamp time.Time `json:"creationTimestamp"`
	JobID             string    `json:"jobId"`
	JobName           string    `json:"jobName"`
	Status            string    `json:"status"`
}

func (j *storedMediaAnalysisJob) toMediaAnalysisJob() *MediaAnalysisJob {
	return &MediaAnalysisJob{
		CreationTimestamp: j.CreationTimestamp,
		JobID:             j.JobID,
		JobName:           j.JobName,
		Status:            j.Status,
	}
}

func (b *InMemoryBackend) projectARN(name string) string {
	return arn.Build("rekognition", b.region, b.accountID, fmt.Sprintf("project/%s", name))
}

func (b *InMemoryBackend) projectVersionARN(projectARN, versionName string) string {
	return fmt.Sprintf("%s/version/%s", projectARN, versionName)
}

func (b *InMemoryBackend) datasetARN(projectARN, datasetType string) string {
	return fmt.Sprintf("%s/dataset/%s/%s", projectARN, datasetType, uuid.NewString())
}

// =============================================================================
// Projects
// =============================================================================

// CreateProject creates a new Rekognition Custom Labels project.
func (b *InMemoryBackend) CreateProject(name string) (*Project, error) {
	b.mu.Lock("CreateProject")
	defer b.mu.Unlock()

	arn := b.projectARN(name)

	if b.projects.Has(arn) {
		return nil, ErrCollectionAlreadyExists
	}

	p := &storedProject{
		CreationTimestamp: time.Now(),
		ProjectARN:        arn,
		Status:            "CREATING",
	}
	b.projects.Put(p)

	return p.toProject(), nil
}

// DeleteProject deletes a project.
func (b *InMemoryBackend) DeleteProject(projectARN string) error {
	b.mu.Lock("DeleteProject")
	defer b.mu.Unlock()

	if !b.projects.Has(projectARN) {
		return ErrProjectNotFound
	}

	b.projects.Delete(projectARN)

	return nil
}

// DescribeProjects lists projects, optionally filtered by ARNs.
func (b *InMemoryBackend) DescribeProjects(
	projectARNs []string, maxResults int32, nextToken string,
) ([]*Project, string, error) {
	b.mu.RLock("DescribeProjects")
	defer b.mu.RUnlock()

	// store.Table.Snapshot returns items ordered by key (ProjectARN), ascending.
	items := b.projects.Snapshot()

	// Build a filter set if requested.
	filter := make(map[string]bool, len(projectARNs))
	for _, arn := range projectARNs {
		filter[arn] = true
	}

	// Apply nextToken offset.
	start := 0
	if nextToken != "" {
		for i, v := range items {
			if v.ProjectARN == nextToken {
				start = i

				break
			}
		}
	}

	const maxPerPage = 100
	limit := int32(maxPerPage)
	if maxResults > 0 && maxResults < limit {
		limit = maxResults
	}

	var result []*Project
	var outToken string
	count := int32(0)

	for i := start; i < len(items); i++ {
		v := items[i]
		if len(filter) > 0 && !filter[v.ProjectARN] {
			continue
		}

		if count >= limit {
			outToken = v.ProjectARN

			break
		}

		result = append(result, v.toProject())
		count++
	}

	return result, outToken, nil
}

// CreateProjectVersion creates a new model version within a project.
func (b *InMemoryBackend) CreateProjectVersion(projectARN, versionName string) (*ProjectVersion, error) {
	b.mu.Lock("CreateProjectVersion")
	defer b.mu.Unlock()

	if !b.projects.Has(projectARN) {
		return nil, ErrProjectNotFound
	}

	arn := b.projectVersionARN(projectARN, versionName)

	if b.projectVersions.Has(arn) {
		return nil, ErrCollectionAlreadyExists
	}

	v := &storedProjectVersion{
		CreationTimestamp: time.Now(),
		ProjectVersionARN: arn,
		ProjectARN:        projectARN,
		VersionName:       versionName,
		Status:            "TRAINING_IN_PROGRESS",
	}
	b.projectVersions.Put(v)

	return v.toProjectVersion(), nil
}

// DeleteProjectVersion deletes a project version.
func (b *InMemoryBackend) DeleteProjectVersion(projectVersionARN string) error {
	b.mu.Lock("DeleteProjectVersion")
	defer b.mu.Unlock()

	if !b.projectVersions.Has(projectVersionARN) {
		return ErrProjectVersionNotFound
	}

	b.projectVersions.Delete(projectVersionARN)

	return nil
}

// DescribeProjectVersions lists versions for a project, optionally filtered by version names.
func (b *InMemoryBackend) DescribeProjectVersions(
	projectARN string, versionNames []string, maxResults int32, nextToken string,
) ([]*ProjectVersion, string, error) {
	b.mu.RLock("DescribeProjectVersions")
	defer b.mu.RUnlock()

	// Collect and sort version ARN keys that belong to this project.
	keys := make([]string, 0)
	for _, v := range b.projectVersions.All() {
		if v.ProjectARN == projectARN {
			keys = append(keys, v.ProjectVersionARN)
		}
	}
	sort.Strings(keys)

	// Build a filter set if requested.
	filter := make(map[string]bool, len(versionNames))
	for _, name := range versionNames {
		filter[name] = true
	}

	start := 0
	if nextToken != "" {
		for i, k := range keys {
			if k == nextToken {
				start = i

				break
			}
		}
	}

	const maxPerPage = 100
	limit := int32(maxPerPage)
	if maxResults > 0 && maxResults < limit {
		limit = maxResults
	}

	var result []*ProjectVersion
	var outToken string
	count := int32(0)

	for i := start; i < len(keys); i++ {
		k := keys[i]
		v, _ := b.projectVersions.Get(k)

		if len(filter) > 0 && !filter[v.VersionName] {
			continue
		}

		if count >= limit {
			outToken = k

			break
		}

		result = append(result, v.toProjectVersion())
		count++
	}

	return result, outToken, nil
}

// CopyProjectVersion copies a project version to another project.
func (b *InMemoryBackend) CopyProjectVersion(
	sourceProjectVersionARN, destinationProjectARN, versionName string,
) (*ProjectVersion, error) {
	b.mu.Lock("CopyProjectVersion")
	defer b.mu.Unlock()

	src, exists := b.projectVersions.Get(sourceProjectVersionARN)
	if !exists {
		return nil, ErrProjectVersionNotFound
	}

	if !b.projects.Has(destinationProjectARN) {
		return nil, ErrProjectNotFound
	}

	name := versionName
	if name == "" {
		name = src.VersionName
	}

	newARN := b.projectVersionARN(destinationProjectARN, name)

	v := &storedProjectVersion{
		CreationTimestamp: time.Now(),
		ProjectVersionARN: newARN,
		ProjectARN:        destinationProjectARN,
		VersionName:       name,
		Status:            "COPYING_IN_PROGRESS",
	}
	b.projectVersions.Put(v)

	return v.toProjectVersion(), nil
}

// StartProjectVersion sets a project version status to RUNNING.
func (b *InMemoryBackend) StartProjectVersion(projectVersionARN string, minInferenceUnits int32) error {
	b.mu.Lock("StartProjectVersion")
	defer b.mu.Unlock()

	v, exists := b.projectVersions.Get(projectVersionARN)
	if !exists {
		return ErrProjectVersionNotFound
	}

	v.Status = processorRunning
	v.MinInferenceUnits = minInferenceUnits

	return nil
}

// StopProjectVersion sets a project version status to STOPPED.
func (b *InMemoryBackend) StopProjectVersion(projectVersionARN string) error {
	b.mu.Lock("StopProjectVersion")
	defer b.mu.Unlock()

	v, exists := b.projectVersions.Get(projectVersionARN)
	if !exists {
		return ErrProjectVersionNotFound
	}

	v.Status = processorStopped

	return nil
}

// ListProjectPolicies lists policies for a project.
func (b *InMemoryBackend) ListProjectPolicies(
	projectARN string, maxResults int32, nextToken string,
) ([]*ProjectPolicy, string, error) {
	b.mu.RLock("ListProjectPolicies")
	defer b.mu.RUnlock()

	// Index result slices are insertion-ordered, not sorted by PolicyName --
	// clone (per the Index.Get contract) and sort to match the original
	// nested-map's collections.SortedKeys(policyName) pagination order.
	group := slices.Clone(b.projectPoliciesByProject.Get(projectARN))
	slices.SortFunc(group, func(a, c *storedProjectPolicy) int { return strings.Compare(a.PolicyName, c.PolicyName) })

	start := 0
	if nextToken != "" {
		for i, p := range group {
			if p.PolicyName == nextToken {
				start = i

				break
			}
		}
	}

	const maxPerPage = 100
	limit := int32(maxPerPage)
	if maxResults > 0 && maxResults < limit {
		limit = maxResults
	}

	end := min(start+int(limit), len(group))

	result := make([]*ProjectPolicy, 0, end-start)
	for _, p := range group[start:end] {
		result = append(result, p.toProjectPolicy())
	}

	var outToken string
	if end < len(group) {
		outToken = group[end].PolicyName
	}

	return result, outToken, nil
}

// PutProjectPolicy creates or updates a project policy.
func (b *InMemoryBackend) PutProjectPolicy(
	projectARN, policyName, policyDocument, policyRevisionID string, //nolint:revive // existing issue.
) (string, error) {
	b.mu.Lock("PutProjectPolicy")
	defer b.mu.Unlock()

	if !b.projects.Has(projectARN) {
		return "", ErrProjectNotFound
	}

	now := time.Now()
	newRevID := uuid.NewString()

	key := projectPolicyKey(projectARN, policyName)

	existing, exists := b.projectPolicies.Get(key)
	if exists {
		existing.LastUpdatedTimestamp = now
		existing.PolicyDocument = policyDocument
		existing.PolicyRevisionID = newRevID
	} else {
		b.projectPolicies.Put(&storedProjectPolicy{
			CreationTimestamp:    now,
			LastUpdatedTimestamp: now,
			ProjectARN:           projectARN,
			PolicyName:           policyName,
			PolicyRevisionID:     newRevID,
			PolicyDocument:       policyDocument,
		})
	}

	return newRevID, nil
}

// DeleteProjectPolicy deletes a project policy.
func (b *InMemoryBackend) DeleteProjectPolicy(
	projectARN, policyName, policyRevisionID string, //nolint:revive // existing issue.
) error {
	b.mu.Lock("DeleteProjectPolicy")
	defer b.mu.Unlock()

	// Mirrors the original nested-map's two-level existence check: a project
	// with no policies at all (never an entry in the outer map) is
	// indistinguishable from one whose named policy is simply missing.
	if len(b.projectPoliciesByProject.Get(projectARN)) == 0 {
		return ErrProjectNotFound
	}

	key := projectPolicyKey(projectARN, policyName)
	if !b.projectPolicies.Has(key) {
		return ErrProjectNotFound
	}

	b.projectPolicies.Delete(key)

	return nil
}

// =============================================================================
// Datasets
// =============================================================================

// CreateDataset creates a new dataset.
func (b *InMemoryBackend) CreateDataset(projectARN, datasetType string) (*Dataset, error) {
	b.mu.Lock("CreateDataset")
	defer b.mu.Unlock()

	if !b.projects.Has(projectARN) {
		return nil, ErrProjectNotFound
	}

	arn := b.datasetARN(projectARN, datasetType)
	now := time.Now()

	ds := &storedDataset{
		CreationTimestamp:    now,
		LastUpdatedTimestamp: now,
		DatasetARN:           arn,
		ProjectARN:           projectARN,
		DatasetType:          datasetType,
		Status:               "CREATE_COMPLETE",
	}
	b.datasets.Put(ds)

	return ds.toDataset(), nil
}

// DeleteDataset deletes a dataset.
func (b *InMemoryBackend) DeleteDataset(datasetARN string) error {
	b.mu.Lock("DeleteDataset")
	defer b.mu.Unlock()

	if !b.datasets.Has(datasetARN) {
		return ErrDatasetNotFound
	}

	b.datasets.Delete(datasetARN)
	delete(b.datasetEntries, datasetARN)

	return nil
}

// DescribeDataset returns details about a dataset.
func (b *InMemoryBackend) DescribeDataset(datasetARN string) (*Dataset, error) {
	b.mu.RLock("DescribeDataset")
	defer b.mu.RUnlock()

	ds, exists := b.datasets.Get(datasetARN)
	if !exists {
		return nil, ErrDatasetNotFound
	}

	return ds.toDataset(), nil
}

// ListDatasetEntries returns a paginated list of dataset entries.
func (b *InMemoryBackend) ListDatasetEntries(
	datasetARN string, maxResults int32, nextToken string,
) ([]string, string, error) {
	b.mu.RLock("ListDatasetEntries")
	defer b.mu.RUnlock()

	if !b.datasets.Has(datasetARN) {
		return nil, "", ErrDatasetNotFound
	}

	entries := b.datasetEntries[datasetARN]

	start := 0
	if nextToken != "" {
		for i, e := range entries {
			if e == nextToken {
				start = i

				break
			}
		}
	}

	const maxPerPage = 100
	limit := int32(maxPerPage)
	if maxResults > 0 && maxResults < limit {
		limit = maxResults
	}

	end := min(start+int(limit), len(entries))

	result := make([]string, end-start)
	copy(result, entries[start:end])

	var outToken string
	if end < len(entries) {
		outToken = entries[end]
	}

	return result, outToken, nil
}

// datasetPaginationToken is the opaque pagination cursor for dataset label listing.
type datasetPaginationToken struct {
	Offset int `json:"o"`
}

// countLabelsFromEntry parses one JSON-lines entry and accumulates label counts.
func countLabelsFromEntry(entry string, counts map[string]int64) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(entry), &obj); err != nil {
		return
	}

	for key, val := range obj {
		const metaSuffix = "-metadata"
		if len(key) < len(metaSuffix) || key[len(key)-len(metaSuffix):] != metaSuffix {
			continue
		}

		countLabelsFromMeta(val, counts)
	}
}

// countLabelsFromMeta parses a -metadata block and increments label counts.
func countLabelsFromMeta(raw json.RawMessage, counts map[string]int64) {
	var meta map[string]json.RawMessage
	if err := json.Unmarshal(raw, &meta); err != nil {
		return
	}

	// Single-label: "class-name"
	if cn, ok := meta["class-name"]; ok {
		var name string
		if err := json.Unmarshal(cn, &name); err == nil && name != "" {
			counts[name]++
		}
	}

	// Multi-label: "class-map": {"LabelA": ..., "LabelB": ...}
	if cm, ok := meta["class-map"]; ok {
		var classMap map[string]json.RawMessage
		if err := json.Unmarshal(cm, &classMap); err == nil {
			for name := range classMap {
				counts[name]++
			}
		}
	}
}

// decodeDatasetPageToken decodes an opaque pagination token into an offset.
func decodeDatasetPageToken(token string) int {
	if token == "" {
		return 0
	}

	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0
	}

	var tok datasetPaginationToken
	if err = json.Unmarshal(decoded, &tok); err != nil || tok.Offset <= 0 {
		return 0
	}

	return tok.Offset
}

// ListDatasetLabels parses stored dataset entries and returns labels with occurrence counts.
func (b *InMemoryBackend) ListDatasetLabels(
	datasetARN string, maxResults int32, nextToken string,
) ([]*DatasetLabel, string, error) {
	b.mu.RLock("ListDatasetLabels")

	if !b.datasets.Has(datasetARN) {
		b.mu.RUnlock()

		return nil, "", ErrDatasetNotFound
	}

	// Clone entries under lock.
	src := b.datasetEntries[datasetARN]
	entries := make([]string, len(src))
	copy(entries, src)

	b.mu.RUnlock()

	// Parse entries and count label occurrences (best-effort).
	counts := make(map[string]int64)
	for _, entry := range entries {
		countLabelsFromEntry(entry, counts)
	}

	// Sort by label name.
	names := make([]string, 0, len(counts))
	for n := range counts {
		names = append(names, n)
	}

	sort.Strings(names)

	start := decodeDatasetPageToken(nextToken)

	const maxPerPage = 100
	limit := int(maxPerPage)

	if maxResults > 0 && int(maxResults) < limit {
		limit = int(maxResults)
	}

	end := min(start+limit, len(names))

	result := make([]*DatasetLabel, 0, end-start)
	for _, n := range names[start:end] {
		result = append(result, &DatasetLabel{
			LabelName:  n,
			EntryCount: counts[n],
		})
	}

	var outToken string

	if end < len(names) {
		tok, _ := json.Marshal(datasetPaginationToken{Offset: end})
		outToken = base64.RawURLEncoding.EncodeToString(tok)
	}

	return result, outToken, nil
}

// UpdateDatasetEntries appends changes to dataset entries.
func (b *InMemoryBackend) UpdateDatasetEntries(datasetARN string, changes []byte) error {
	b.mu.Lock("UpdateDatasetEntries")
	defer b.mu.Unlock()

	if !b.datasets.Has(datasetARN) {
		return ErrDatasetNotFound
	}

	b.datasetEntries[datasetARN] = append(b.datasetEntries[datasetARN], string(changes))

	return nil
}

// DistributeDatasetEntries validates datasets and marks them as UPDATE_IN_PROGRESS.
func (b *InMemoryBackend) DistributeDatasetEntries(datasets []DatasetDistribution) error {
	b.mu.Lock("DistributeDatasetEntries")
	defer b.mu.Unlock()

	for _, d := range datasets {
		ds, ok := b.datasets.Get(d.DatasetARN)
		if !ok {
			return ErrDatasetNotFound
		}

		ds.Status = "UPDATE_IN_PROGRESS"
		ds.LastUpdatedTimestamp = time.Now()
	}

	return nil
}

// =============================================================================
// Users
// =============================================================================

// CreateUser creates a user in a collection.
func (b *InMemoryBackend) CreateUser(collectionID, userID string) error {
	b.mu.Lock("CreateUser")
	defer b.mu.Unlock()

	if !b.collections.Has(collectionID) {
		return ErrCollectionNotFound
	}

	key := userKey(collectionID, userID)
	if b.users.Has(key) {
		return ErrCollectionAlreadyExists
	}

	b.users.Put(&storedUser{
		CollectionID: collectionID,
		UserID:       userID,
		UserStatus:   "ACTIVE",
		FaceIDs:      []string{},
	})

	return nil
}

// DeleteUser removes a user from a collection.
func (b *InMemoryBackend) DeleteUser(collectionID, userID string) error {
	b.mu.Lock("DeleteUser")
	defer b.mu.Unlock()

	if !b.collections.Has(collectionID) {
		return ErrCollectionNotFound
	}

	key := userKey(collectionID, userID)
	if !b.users.Has(key) {
		return ErrUserNotFound
	}

	b.users.Delete(key)

	return nil
}

// ListUsers returns a paginated list of users in a collection.
func (b *InMemoryBackend) ListUsers(
	collectionID string, maxResults int32, nextToken string,
) ([]*User, string, error) {
	b.mu.RLock("ListUsers")
	defer b.mu.RUnlock()

	if !b.collections.Has(collectionID) {
		return nil, "", ErrCollectionNotFound
	}

	// Index result slices are insertion-ordered, not sorted by UserID --
	// clone (per the Index.Get contract) and sort to match the original
	// nested-map's collections.SortedKeys(userID) pagination order.
	group := slices.Clone(b.usersByCollection.Get(collectionID))
	slices.SortFunc(group, func(a, c *storedUser) int { return strings.Compare(a.UserID, c.UserID) })

	start := 0
	if nextToken != "" {
		for i, u := range group {
			if u.UserID == nextToken {
				start = i

				break
			}
		}
	}

	const maxPerPage = 4096
	limit := int32(maxPerPage)
	if maxResults > 0 && maxResults < limit {
		limit = maxResults
	}

	end := min(start+int(limit), len(group))

	result := make([]*User, 0, end-start)
	for _, u := range group[start:end] {
		result = append(result, u.toUser())
	}

	var outToken string
	if end < len(group) {
		outToken = group[end].UserID
	}

	return result, outToken, nil
}

// AssociateFaces associates faces with a user.
func (b *InMemoryBackend) AssociateFaces(
	collectionID, userID string, faceIDs []string,
) ([]*AssociatedFace, []*UnsuccessfulFaceAssociation, error) {
	b.mu.Lock("AssociateFaces")
	defer b.mu.Unlock()

	if !b.collections.Has(collectionID) {
		return nil, nil, ErrCollectionNotFound
	}

	user, exists := b.users.Get(userKey(collectionID, userID))
	if !exists {
		return nil, nil, ErrUserNotFound
	}

	// Build a set of known face IDs in this collection.
	knownFaces := make(map[string]bool)
	for _, f := range b.facesByCollection.Get(collectionID) {
		knownFaces[f.FaceID] = true
	}

	var associated []*AssociatedFace
	var unsuccessful []*UnsuccessfulFaceAssociation

	for _, faceID := range faceIDs {
		if knownFaces[faceID] {
			user.FaceIDs = append(user.FaceIDs, faceID)
			associated = append(associated, &AssociatedFace{FaceID: faceID})
		} else {
			unsuccessful = append(unsuccessful, &UnsuccessfulFaceAssociation{
				FaceID:  faceID,
				Reasons: []string{"FACE_NOT_FOUND"},
			})
		}
	}

	return associated, unsuccessful, nil
}

// DisassociateFaces removes faces from a user.
func (b *InMemoryBackend) DisassociateFaces(
	collectionID, userID string, faceIDs []string,
) ([]*DisassociatedFace, []*UnsuccessfulFaceDisassociation, error) {
	b.mu.Lock("DisassociateFaces")
	defer b.mu.Unlock()

	if !b.collections.Has(collectionID) {
		return nil, nil, ErrCollectionNotFound
	}

	user, exists := b.users.Get(userKey(collectionID, userID))
	if !exists {
		return nil, nil, ErrUserNotFound
	}

	// Build a set of faces associated with this user.
	associated := make(map[string]bool, len(user.FaceIDs))
	for _, id := range user.FaceIDs {
		associated[id] = true
	}

	toRemove := make(map[string]bool, len(faceIDs))
	var disassociated []*DisassociatedFace
	var unsuccessful []*UnsuccessfulFaceDisassociation

	for _, faceID := range faceIDs {
		if associated[faceID] {
			toRemove[faceID] = true
			disassociated = append(disassociated, &DisassociatedFace{FaceID: faceID})
		} else {
			unsuccessful = append(unsuccessful, &UnsuccessfulFaceDisassociation{
				FaceID:  faceID,
				Reasons: []string{"FACE_NOT_FOUND"},
			})
		}
	}

	remaining := user.FaceIDs[:0]
	for _, id := range user.FaceIDs {
		if !toRemove[id] {
			remaining = append(remaining, id)
		}
	}
	user.FaceIDs = remaining

	return disassociated, unsuccessful, nil
}

// SearchUsers returns up to maxUsers users with a simulated similarity score.
func (b *InMemoryBackend) SearchUsers(collectionID, userID string, maxUsers int32) ([]*UserMatch, error) {
	b.mu.RLock("SearchUsers")
	defer b.mu.RUnlock()

	if !b.collections.Has(collectionID) {
		return nil, ErrCollectionNotFound
	}

	group := b.usersByCollection.Get(collectionID)
	if len(group) == 0 {
		return []*UserMatch{}, nil
	}

	limit := int(maxUsers)
	if limit <= 0 {
		limit = 5
	}

	var matches []*UserMatch
	for _, u := range group {
		if u.UserID == userID {
			continue
		}

		matches = append(matches, &UserMatch{
			User:       u.toUser(),
			Similarity: userSimilarity(userID, u),
		})

		if len(matches) >= limit {
			break
		}
	}

	return matches, nil
}

// SearchUsersByImage returns up to maxUsers users with a deterministic similarity
// score derived from the image reference and each candidate user's identity.
func (b *InMemoryBackend) SearchUsersByImage(
	collectionID string,
	maxUsers int32,
	imageKey string,
) ([]*UserMatch, error) {
	b.mu.RLock("SearchUsersByImage")
	defer b.mu.RUnlock()

	if !b.collections.Has(collectionID) {
		return nil, ErrCollectionNotFound
	}

	group := b.usersByCollection.Get(collectionID)
	if len(group) == 0 {
		return []*UserMatch{}, nil
	}

	limit := int(maxUsers)
	if limit <= 0 {
		limit = 5
	}

	var matches []*UserMatch
	for _, u := range group {
		matches = append(matches, &UserMatch{
			User:       u.toUser(),
			Similarity: userSimilarity(imageKey, u),
		})

		if len(matches) >= limit {
			break
		}
	}

	return matches, nil
}

// =============================================================================
// Face Liveness
// =============================================================================

// CreateFaceLivenessSession creates a new face liveness session.
func (b *InMemoryBackend) CreateFaceLivenessSession() (string, error) {
	b.mu.Lock("CreateFaceLivenessSession")
	defer b.mu.Unlock()

	sessionID := uuid.NewString()

	// Derive confidence from session ID hash: range 75.0-99.9
	var h uint32
	for _, c := range sessionID {
		h = h*31 + uint32(c) //nolint:mnd,gosec // hash multiplier; G115 safe: unicode codepoints are non-negative
	}

	confidence := float32(75.0) + float32(h%250)/10.0 //nolint:mnd // confidence range

	b.livenessSessions.Put(&storedLivenessSession{
		SessionID:  sessionID,
		Status:     "SUCCEEDED", //nolint:goconst // existing issue.
		Confidence: confidence,
	})

	return sessionID, nil
}

// GetFaceLivenessSessionResults returns the result of a liveness session.
func (b *InMemoryBackend) GetFaceLivenessSessionResults(sessionID string) (*LivenessSessionResult, error) {
	b.mu.RLock("GetFaceLivenessSessionResults")
	defer b.mu.RUnlock()

	session, exists := b.livenessSessions.Get(sessionID)
	if !exists {
		return nil, ErrLivenessSessionNotFound
	}

	return &LivenessSessionResult{
		SessionID:  session.SessionID,
		Status:     session.Status,
		Confidence: session.Confidence,
	}, nil
}

// =============================================================================
// Async Jobs
// =============================================================================

// evictOneIfAtCapacity deletes an arbitrary entry from t when it is already
// at (or over) max, mirroring the original map's "evict a random entry"
// eviction (Go map iteration order is unspecified, matching store.Table's
// own unspecified Range order).
func evictOneIfAtCapacity[V any](t *store.Table[V], maxLen int, keyFn func(*V) string) {
	if t.Len() < maxLen {
		return
	}

	t.Range(func(v *V) bool {
		t.Delete(keyFn(v))

		return false
	})
}

// StartAsyncJob creates a new async video analysis job.
func (b *InMemoryBackend) StartAsyncJob(jobType, collectionID string) (string, error) {
	b.mu.Lock("StartAsyncJob")
	defer b.mu.Unlock()

	evictOneIfAtCapacity(b.asyncJobs, maxAsyncJobs, asyncJobKeyFn)

	jobID := uuid.NewString()
	b.asyncJobs.Put(&storedAsyncJob{
		JobID:        jobID,
		JobType:      jobType,
		CollectionID: collectionID,
		JobStatus:    "IN_PROGRESS",
	})

	return jobID, nil
}

// GetAsyncJob returns an async job by ID, simulating state progression on each poll.
func (b *InMemoryBackend) GetAsyncJob(jobID string) (*AsyncJob, error) {
	b.mu.Lock("GetAsyncJob")
	defer b.mu.Unlock()

	job, exists := b.asyncJobs.Get(jobID)
	if !exists {
		return nil, ErrAsyncJobNotFound
	}

	switch job.PollCount {
	case 0:
		job.PollCount++

		return &AsyncJob{JobID: job.JobID, JobStatus: "IN_PROGRESS"}, nil
	case 1:
		job.PollCount++
		job.JobStatus = "SUCCEEDED"

		return &AsyncJob{JobID: job.JobID, JobStatus: "SUCCEEDED"}, nil
	default:
		return &AsyncJob{JobID: job.JobID, JobStatus: job.JobStatus}, nil
	}
}

// StartMediaAnalysisJob creates a new media analysis job.
func (b *InMemoryBackend) StartMediaAnalysisJob(jobName string) (string, error) {
	b.mu.Lock("StartMediaAnalysisJob")
	defer b.mu.Unlock()

	evictOneIfAtCapacity(b.mediaAnalysisJobs, maxMediaAnalysisJobs, mediaAnalysisJobKeyFn)

	jobID := uuid.NewString()
	b.mediaAnalysisJobs.Put(&storedMediaAnalysisJob{
		CreationTimestamp: time.Now(),
		JobID:             jobID,
		JobName:           jobName,
		Status:            "SUCCEEDED",
	})

	return jobID, nil
}

// GetMediaAnalysisJob returns a media analysis job by ID.
func (b *InMemoryBackend) GetMediaAnalysisJob(jobID string) (*MediaAnalysisJob, error) {
	b.mu.RLock("GetMediaAnalysisJob")
	defer b.mu.RUnlock()

	job, exists := b.mediaAnalysisJobs.Get(jobID)
	if !exists {
		return nil, ErrMediaAnalysisJobNotFound
	}

	return job.toMediaAnalysisJob(), nil
}

// ListMediaAnalysisJobs returns a paginated list of media analysis jobs.
func (b *InMemoryBackend) ListMediaAnalysisJobs(
	maxResults int32, nextToken string,
) ([]*MediaAnalysisJob, string, error) {
	b.mu.RLock("ListMediaAnalysisJobs")
	defer b.mu.RUnlock()

	const maxPerPage = 100

	result, outToken := paginateTable(
		b.mediaAnalysisJobs, maxResults, maxPerPage, nextToken,
		mediaAnalysisJobKeyFn, (*storedMediaAnalysisJob).toMediaAnalysisJob,
	)

	return result, outToken, nil
}
