package rekognition

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
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

// storedUser holds a Rekognition user in a collection.
type storedUser struct {
	UserID     string   `json:"userId"`
	UserStatus string   `json:"userStatus"`
	FaceIDs    []string `json:"faceIds"`
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

	if _, exists := b.projects[arn]; exists {
		return nil, ErrCollectionAlreadyExists
	}

	p := &storedProject{
		CreationTimestamp: time.Now(),
		ProjectARN:        arn,
		Status:            "CREATING",
	}
	b.projects[arn] = p

	return p.toProject(), nil
}

// DeleteProject deletes a project.
func (b *InMemoryBackend) DeleteProject(projectARN string) error {
	b.mu.Lock("DeleteProject")
	defer b.mu.Unlock()

	if _, exists := b.projects[projectARN]; !exists {
		return ErrProjectNotFound
	}

	delete(b.projects, projectARN)

	return nil
}

// DescribeProjects lists projects, optionally filtered by ARNs.
func (b *InMemoryBackend) DescribeProjects(
	projectARNs []string, maxResults int32, nextToken string,
) ([]*Project, string, error) {
	b.mu.RLock("DescribeProjects")
	defer b.mu.RUnlock()

	// Collect and sort all project ARN keys.
	keys := collections.SortedKeys(b.projects)

	// Build a filter set if requested.
	filter := make(map[string]bool, len(projectARNs))
	for _, arn := range projectARNs {
		filter[arn] = true
	}

	// Apply nextToken offset.
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

	var result []*Project
	var outToken string
	count := int32(0)

	for i := start; i < len(keys); i++ {
		k := keys[i]
		if len(filter) > 0 && !filter[k] {
			continue
		}

		if count >= limit {
			outToken = k

			break
		}

		result = append(result, b.projects[k].toProject())
		count++
	}

	return result, outToken, nil
}

// CreateProjectVersion creates a new model version within a project.
func (b *InMemoryBackend) CreateProjectVersion(projectARN, versionName string) (*ProjectVersion, error) {
	b.mu.Lock("CreateProjectVersion")
	defer b.mu.Unlock()

	if _, exists := b.projects[projectARN]; !exists {
		return nil, ErrProjectNotFound
	}

	arn := b.projectVersionARN(projectARN, versionName)

	if _, exists := b.projectVersions[arn]; exists {
		return nil, ErrCollectionAlreadyExists
	}

	v := &storedProjectVersion{
		CreationTimestamp: time.Now(),
		ProjectVersionARN: arn,
		ProjectARN:        projectARN,
		VersionName:       versionName,
		Status:            "TRAINING_IN_PROGRESS",
	}
	b.projectVersions[arn] = v

	return v.toProjectVersion(), nil
}

// DeleteProjectVersion deletes a project version.
func (b *InMemoryBackend) DeleteProjectVersion(projectVersionARN string) error {
	b.mu.Lock("DeleteProjectVersion")
	defer b.mu.Unlock()

	if _, exists := b.projectVersions[projectVersionARN]; !exists {
		return ErrProjectVersionNotFound
	}

	delete(b.projectVersions, projectVersionARN)

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
	for k, v := range b.projectVersions {
		if v.ProjectARN == projectARN {
			keys = append(keys, k)
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
		v := b.projectVersions[k]

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

	src, exists := b.projectVersions[sourceProjectVersionARN]
	if !exists {
		return nil, ErrProjectVersionNotFound
	}

	if _, exists := b.projects[destinationProjectARN]; !exists { //nolint:govet // existing issue.
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
	b.projectVersions[newARN] = v

	return v.toProjectVersion(), nil
}

// StartProjectVersion sets a project version status to RUNNING.
func (b *InMemoryBackend) StartProjectVersion(projectVersionARN string, minInferenceUnits int32) error {
	b.mu.Lock("StartProjectVersion")
	defer b.mu.Unlock()

	v, exists := b.projectVersions[projectVersionARN]
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

	v, exists := b.projectVersions[projectVersionARN]
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

	policyMap := b.projectPolicies[projectARN]

	keys := collections.SortedKeys(policyMap)

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

	end := min(start+int(limit), len(keys))

	result := make([]*ProjectPolicy, 0, end-start)
	for _, k := range keys[start:end] {
		result = append(result, policyMap[k].toProjectPolicy())
	}

	var outToken string
	if end < len(keys) {
		outToken = keys[end]
	}

	return result, outToken, nil
}

// PutProjectPolicy creates or updates a project policy.
func (b *InMemoryBackend) PutProjectPolicy(
	projectARN, policyName, policyDocument, policyRevisionID string, //nolint:revive // existing issue.
) (string, error) {
	b.mu.Lock("PutProjectPolicy")
	defer b.mu.Unlock()

	if _, exists := b.projects[projectARN]; !exists {
		return "", ErrProjectNotFound
	}

	if b.projectPolicies[projectARN] == nil {
		b.projectPolicies[projectARN] = make(map[string]*storedProjectPolicy)
	}

	now := time.Now()
	newRevID := uuid.NewString()

	existing, exists := b.projectPolicies[projectARN][policyName]
	if exists {
		existing.LastUpdatedTimestamp = now
		existing.PolicyDocument = policyDocument
		existing.PolicyRevisionID = newRevID
	} else {
		b.projectPolicies[projectARN][policyName] = &storedProjectPolicy{
			CreationTimestamp:    now,
			LastUpdatedTimestamp: now,
			ProjectARN:           projectARN,
			PolicyName:           policyName,
			PolicyRevisionID:     newRevID,
			PolicyDocument:       policyDocument,
		}
	}

	return newRevID, nil
}

// DeleteProjectPolicy deletes a project policy.
func (b *InMemoryBackend) DeleteProjectPolicy(
	projectARN, policyName, policyRevisionID string, //nolint:revive // existing issue.
) error {
	b.mu.Lock("DeleteProjectPolicy")
	defer b.mu.Unlock()

	policyMap, exists := b.projectPolicies[projectARN]
	if !exists {
		return ErrProjectNotFound
	}

	if _, exists := policyMap[policyName]; !exists { //nolint:govet // existing issue.
		return ErrProjectNotFound
	}

	delete(policyMap, policyName)

	return nil
}

// =============================================================================
// Datasets
// =============================================================================

// CreateDataset creates a new dataset.
func (b *InMemoryBackend) CreateDataset(projectARN, datasetType string) (*Dataset, error) {
	b.mu.Lock("CreateDataset")
	defer b.mu.Unlock()

	if _, exists := b.projects[projectARN]; !exists {
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
	b.datasets[arn] = ds

	return ds.toDataset(), nil
}

// DeleteDataset deletes a dataset.
func (b *InMemoryBackend) DeleteDataset(datasetARN string) error {
	b.mu.Lock("DeleteDataset")
	defer b.mu.Unlock()

	if _, exists := b.datasets[datasetARN]; !exists {
		return ErrDatasetNotFound
	}

	delete(b.datasets, datasetARN)
	delete(b.datasetEntries, datasetARN)

	return nil
}

// DescribeDataset returns details about a dataset.
func (b *InMemoryBackend) DescribeDataset(datasetARN string) (*Dataset, error) {
	b.mu.RLock("DescribeDataset")
	defer b.mu.RUnlock()

	ds, exists := b.datasets[datasetARN]
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

	if _, exists := b.datasets[datasetARN]; !exists {
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

// ListDatasetLabels returns an empty list of labels (not tracked by this mock).
func (b *InMemoryBackend) ListDatasetLabels(
	datasetARN string, maxResults int32, nextToken string, //nolint:revive // existing issue.
) ([]*DatasetLabel, string, error) {
	b.mu.RLock("ListDatasetLabels")
	defer b.mu.RUnlock()

	if _, exists := b.datasets[datasetARN]; !exists {
		return nil, "", ErrDatasetNotFound
	}

	return []*DatasetLabel{}, "", nil
}

// UpdateDatasetEntries appends changes to dataset entries.
func (b *InMemoryBackend) UpdateDatasetEntries(datasetARN string, changes []byte) error {
	b.mu.Lock("UpdateDatasetEntries")
	defer b.mu.Unlock()

	if _, exists := b.datasets[datasetARN]; !exists {
		return ErrDatasetNotFound
	}

	b.datasetEntries[datasetARN] = append(b.datasetEntries[datasetARN], string(changes))

	return nil
}

// DistributeDatasetEntries is a no-op for the in-memory backend.
func (b *InMemoryBackend) DistributeDatasetEntries(
	datasets []DatasetDistribution, //nolint:revive // existing issue.
) error {
	return nil
}

// =============================================================================
// Users
// =============================================================================

// CreateUser creates a user in a collection.
func (b *InMemoryBackend) CreateUser(collectionID, userID string) error {
	b.mu.Lock("CreateUser")
	defer b.mu.Unlock()

	if _, exists := b.collections[collectionID]; !exists {
		return ErrCollectionNotFound
	}

	if b.users[collectionID] == nil {
		b.users[collectionID] = make(map[string]*storedUser)
	}

	if _, exists := b.users[collectionID][userID]; exists {
		return ErrCollectionAlreadyExists
	}

	b.users[collectionID][userID] = &storedUser{
		UserID:     userID,
		UserStatus: "ACTIVE",
		FaceIDs:    []string{},
	}

	return nil
}

// DeleteUser removes a user from a collection.
func (b *InMemoryBackend) DeleteUser(collectionID, userID string) error {
	b.mu.Lock("DeleteUser")
	defer b.mu.Unlock()

	if _, exists := b.collections[collectionID]; !exists {
		return ErrCollectionNotFound
	}

	userMap := b.users[collectionID]
	if userMap == nil {
		return ErrUserNotFound
	}

	if _, exists := userMap[userID]; !exists {
		return ErrUserNotFound
	}

	delete(userMap, userID)

	return nil
}

// ListUsers returns a paginated list of users in a collection.
func (b *InMemoryBackend) ListUsers(
	collectionID string, maxResults int32, nextToken string,
) ([]*User, string, error) {
	b.mu.RLock("ListUsers")
	defer b.mu.RUnlock()

	if _, exists := b.collections[collectionID]; !exists {
		return nil, "", ErrCollectionNotFound
	}

	userMap := b.users[collectionID]

	keys := collections.SortedKeys(userMap)

	start := 0
	if nextToken != "" {
		for i, k := range keys {
			if k == nextToken {
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

	end := min(start+int(limit), len(keys))

	result := make([]*User, 0, end-start)
	for _, k := range keys[start:end] {
		result = append(result, userMap[k].toUser())
	}

	var outToken string
	if end < len(keys) {
		outToken = keys[end]
	}

	return result, outToken, nil
}

// AssociateFaces associates faces with a user.
func (b *InMemoryBackend) AssociateFaces(
	collectionID, userID string, faceIDs []string,
) ([]*AssociatedFace, []*UnsuccessfulFaceAssociation, error) {
	b.mu.Lock("AssociateFaces")
	defer b.mu.Unlock()

	if _, exists := b.collections[collectionID]; !exists {
		return nil, nil, ErrCollectionNotFound
	}

	userMap := b.users[collectionID]
	if userMap == nil {
		return nil, nil, ErrUserNotFound
	}

	user, exists := userMap[userID]
	if !exists {
		return nil, nil, ErrUserNotFound
	}

	// Build a set of known face IDs in this collection.
	knownFaces := make(map[string]bool)
	for _, f := range b.faces[collectionID] {
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

	if _, exists := b.collections[collectionID]; !exists {
		return nil, nil, ErrCollectionNotFound
	}

	userMap := b.users[collectionID]
	if userMap == nil {
		return nil, nil, ErrUserNotFound
	}

	user, exists := userMap[userID]
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

	if _, exists := b.collections[collectionID]; !exists {
		return nil, ErrCollectionNotFound
	}

	userMap := b.users[collectionID]
	if userMap == nil {
		return []*UserMatch{}, nil
	}

	limit := int(maxUsers)
	if limit <= 0 {
		limit = 5
	}

	var matches []*UserMatch
	for _, u := range userMap {
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

	if _, exists := b.collections[collectionID]; !exists {
		return nil, ErrCollectionNotFound
	}

	userMap := b.users[collectionID]
	if userMap == nil {
		return []*UserMatch{}, nil
	}

	limit := int(maxUsers)
	if limit <= 0 {
		limit = 5
	}

	var matches []*UserMatch
	for _, u := range userMap {
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
	b.livenessSessions[sessionID] = &storedLivenessSession{
		SessionID:  sessionID,
		Status:     "SUCCEEDED", //nolint:goconst // existing issue.
		Confidence: 99.0,        //nolint:mnd // existing issue.
	}

	return sessionID, nil
}

// GetFaceLivenessSessionResults returns the result of a liveness session.
func (b *InMemoryBackend) GetFaceLivenessSessionResults(sessionID string) (*LivenessSessionResult, error) {
	b.mu.RLock("GetFaceLivenessSessionResults")
	defer b.mu.RUnlock()

	session, exists := b.livenessSessions[sessionID]
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

// StartAsyncJob creates a new async video analysis job.
func (b *InMemoryBackend) StartAsyncJob(jobType, collectionID string) (string, error) {
	b.mu.Lock("StartAsyncJob")
	defer b.mu.Unlock()

	jobID := uuid.NewString()
	b.asyncJobs[jobID] = &storedAsyncJob{
		JobID:        jobID,
		JobType:      jobType,
		CollectionID: collectionID,
		JobStatus:    "SUCCEEDED",
	}

	return jobID, nil
}

// GetAsyncJob returns an async job by ID.
func (b *InMemoryBackend) GetAsyncJob(jobID string) (*AsyncJob, error) {
	b.mu.RLock("GetAsyncJob")
	defer b.mu.RUnlock()

	job, exists := b.asyncJobs[jobID]
	if !exists {
		return nil, ErrAsyncJobNotFound
	}

	return &AsyncJob{
		JobID:     job.JobID,
		JobStatus: job.JobStatus,
	}, nil
}

// StartMediaAnalysisJob creates a new media analysis job.
func (b *InMemoryBackend) StartMediaAnalysisJob(jobName string) (string, error) {
	b.mu.Lock("StartMediaAnalysisJob")
	defer b.mu.Unlock()

	jobID := uuid.NewString()
	b.mediaAnalysisJobs[jobID] = &storedMediaAnalysisJob{
		CreationTimestamp: time.Now(),
		JobID:             jobID,
		JobName:           jobName,
		Status:            "SUCCEEDED",
	}

	return jobID, nil
}

// GetMediaAnalysisJob returns a media analysis job by ID.
func (b *InMemoryBackend) GetMediaAnalysisJob(jobID string) (*MediaAnalysisJob, error) {
	b.mu.RLock("GetMediaAnalysisJob")
	defer b.mu.RUnlock()

	job, exists := b.mediaAnalysisJobs[jobID]
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

	keys := collections.SortedKeys(b.mediaAnalysisJobs)

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

	end := min(start+int(limit), len(keys))

	result := make([]*MediaAnalysisJob, 0, end-start)
	for _, k := range keys[start:end] {
		result = append(result, b.mediaAnalysisJobs[k].toMediaAnalysisJob())
	}

	var outToken string
	if end < len(keys) {
		outToken = keys[end]
	}

	return result, outToken, nil
}
