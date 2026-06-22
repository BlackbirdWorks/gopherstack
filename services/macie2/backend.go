package macie2

import (
	"context"
	"fmt"
	"maps"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

const (
	defaultFrequency    = "SIX_HOURS"
	statusEnabled       = "ENABLED"
	statusPaused        = "PAUSED"
	defaultMatchDist    = int32(50)
	defaultFindingScore = 5.0

	errResourceNotFound  = "ResourceNotFoundException"
	errConflictException = "ConflictException"
	errValidation        = "ValidationException"
	errMacieNotEnabled   = "AccessDeniedException"

	maxTagCount    = 50
	maxTagKeyLen   = 128
	maxTagValueLen = 256
	maxMatchDist   = int32(300)
	minMatchDist   = int32(1)
)

var (
	// ErrNotEnabled is returned when Macie is not enabled.
	ErrNotEnabled = awserr.New(errMacieNotEnabled, awserr.ErrNotFound)
	// ErrAllowListNotFound is returned when an allow list does not exist.
	ErrAllowListNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrAllowListAlreadyExists is returned when an allow list already exists.
	ErrAllowListAlreadyExists = awserr.New(errConflictException, awserr.ErrConflict)
	// ErrCustomDataIDNotFound is returned when a custom data identifier does not exist.
	ErrCustomDataIDNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrFindingsFilterNotFound is returned when a findings filter does not exist.
	ErrFindingsFilterNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrFindingNotFound is returned when a finding does not exist.
	ErrFindingNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrTaggedResourceNotFound is returned when a tag operation targets an unknown resource ARN.
	ErrTaggedResourceNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrValidation is returned on invalid input.
	ErrValidation = awserr.New(errValidation, awserr.ErrInvalidParameter)
)

// storedAllowList holds the allow list with all fields.
type storedAllowList struct {
	AllowListDetail
}

// storedCustomDataID holds the custom data identifier with internal fields.
type storedCustomDataID struct {
	CustomDataIdentifier
	Deleted bool `json:"deleted"`
}

// storedFindingsFilter holds the findings filter with all fields.
type storedFindingsFilter struct {
	FindingsFilterDetail
}

// storedFinding holds a Macie finding.
type storedFinding struct {
	Finding
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	mu              *lockmetrics.RWMutex
	allowLists      map[string]*storedAllowList      // id → allow list
	customDataIDs   map[string]*storedCustomDataID   // id → custom data identifier
	findingsFilters map[string]*storedFindingsFilter // id → findings filter
	findings        map[string]*storedFinding        // id → finding
	s3Buckets       map[string]*S3BucketMetadata     // bucketArn → bucket metadata
	tags            map[string]map[string]string     // resourceARN → tags
	session         *Session
	// Appendix A fields
	classificationJobs    map[string]*ClassificationJob             // jobID → job
	members               map[string]*Member                        // accountID → member
	invitations           map[string]*Invitation                    // invitationID → invitation
	administrator         *AdministratorAccount                     // this account's admin
	orgAdminAccounts      map[string]*OrgAdminAccount               // accountID → org admin
	orgConfig             *OrgConfig                                // org configuration
	autoDiscoveryConfig   *AutoDiscoveryConfig                      // automated discovery config
	autoDiscoveryAccounts map[string]*AutoDiscoveryAccount          // accountID → auto discovery
	classExportConfig     *ClassificationExportConfig               // export config
	classScopes           map[string]*ClassificationScope           // scopeID → scope
	findingsPubConfig     *FindingsPublicationConfig                // findings publication config
	resourceProfiles      map[string]*ResourceProfile               // resourceArn → profile
	resourceDetections    map[string][]ResourceProfileDetection     // resourceArn → detections
	revealConfig          *RevealConfiguration                      // reveal config
	sensitivityTemplates  map[string]*SensitivityInspectionTemplate // templateID → template
	accountID             string
	region                string
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:                    lockmetrics.New("macie2"),
		accountID:             accountID,
		region:                region,
		allowLists:            make(map[string]*storedAllowList),
		customDataIDs:         make(map[string]*storedCustomDataID),
		findingsFilters:       make(map[string]*storedFindingsFilter),
		findings:              make(map[string]*storedFinding),
		s3Buckets:             make(map[string]*S3BucketMetadata),
		tags:                  make(map[string]map[string]string),
		classificationJobs:    make(map[string]*ClassificationJob),
		members:               make(map[string]*Member),
		invitations:           make(map[string]*Invitation),
		orgAdminAccounts:      make(map[string]*OrgAdminAccount),
		orgConfig:             &OrgConfig{AutoEnable: false},
		autoDiscoveryConfig:   &AutoDiscoveryConfig{Status: "DISABLED"}, //nolint:goconst // existing issue.
		autoDiscoveryAccounts: make(map[string]*AutoDiscoveryAccount),
		classScopes:           make(map[string]*ClassificationScope),
		resourceProfiles:      make(map[string]*ResourceProfile),
		resourceDetections:    make(map[string][]ResourceProfileDetection),
		sensitivityTemplates:  make(map[string]*SensitivityInspectionTemplate),
	}
}

func (b *InMemoryBackend) allowListARN(id string) string {
	return arn.Build("macie2", b.region, b.accountID, fmt.Sprintf("allow-list/%s", id))
}

func (b *InMemoryBackend) customDataIDARN(id string) string {
	return arn.Build("macie2", b.region, b.accountID, fmt.Sprintf("custom-data-identifier/%s", id))
}

func (b *InMemoryBackend) findingsFilterARN(id string) string {
	return arn.Build("macie2", b.region, b.accountID, fmt.Sprintf("findings-filter/%s", id))
}

// GetSession returns the current Macie session (may be nil if not enabled).
func (b *InMemoryBackend) GetSession() *Session {
	b.mu.RLock("GetSession")
	defer b.mu.RUnlock()

	return b.session
}

// EnableMacie enables Macie for the account.
func (b *InMemoryBackend) EnableMacie(_, frequency, status string) error {
	b.mu.Lock("EnableMacie")
	defer b.mu.Unlock()

	if b.session != nil && b.session.Enabled {
		return ErrAllowListAlreadyExists // reuse conflict error for already-enabled
	}

	freq := frequency
	if freq == "" {
		freq = defaultFrequency
	}

	st := status
	if st == "" {
		st = statusEnabled
	}

	now := time.Now().UTC()
	b.session = &Session{
		CreatedAt:                  now,
		UpdatedAt:                  now,
		FindingPublishingFrequency: freq,
		ServiceRole: fmt.Sprintf(
			"arn:aws:iam::%s:role/aws-service-role/macie.amazonaws.com/AWSServiceRoleForAmazonMacie",
			b.accountID,
		),
		Status:  st,
		Enabled: true,
	}

	return nil
}

// DisableMacie disables Macie for the account.
func (b *InMemoryBackend) DisableMacie() error {
	b.mu.Lock("DisableMacie")
	defer b.mu.Unlock()

	b.session = nil

	return nil
}

// UpdateMacieSession updates the Macie session configuration.
func (b *InMemoryBackend) UpdateMacieSession(frequency, status string) error {
	b.mu.Lock("UpdateMacieSession")
	defer b.mu.Unlock()

	if b.session == nil {
		return nil
	}

	if frequency != "" {
		b.session.FindingPublishingFrequency = frequency
	}

	if status != "" {
		b.session.Status = status
	}

	b.session.UpdatedAt = time.Now().UTC()

	return nil
}

// CreateAllowList creates a new allow list.
func (b *InMemoryBackend) CreateAllowList(
	name, description string,
	criteria AllowListCriteria,
	tags map[string]string,
) (*AllowListSummary, error) {
	b.mu.Lock("CreateAllowList")
	defer b.mu.Unlock()

	id := uuid.New().String()
	now := time.Now().UTC()
	al := &storedAllowList{
		AllowListDetail: AllowListDetail{
			ID:          id,
			Arn:         b.allowListARN(id),
			Name:        name,
			Description: description,
			Criteria:    criteria,
			CreatedAt:   now,
			UpdatedAt:   now,
			Status:      AllowListStatus{Code: "OK"},
			Tags:        maps.Clone(tags),
		},
	}

	b.allowLists[id] = al
	if len(tags) > 0 {
		b.tags[al.Arn] = maps.Clone(tags)
	}

	return &AllowListSummary{
		Arn:         al.Arn,
		CreatedAt:   al.CreatedAt,
		Description: al.Description,
		ID:          al.ID,
		Name:        al.Name,
		UpdatedAt:   al.UpdatedAt,
		Tags:        al.Tags,
	}, nil
}

// GetAllowList retrieves an allow list by ID.
func (b *InMemoryBackend) GetAllowList(id string) (*AllowListDetail, error) {
	b.mu.RLock("GetAllowList")
	defer b.mu.RUnlock()

	al, ok := b.allowLists[id]
	if !ok {
		return nil, ErrAllowListNotFound
	}

	cp := al.AllowListDetail
	cp.Tags = maps.Clone(al.Tags)

	return &cp, nil
}

// UpdateAllowList updates an existing allow list.
func (b *InMemoryBackend) UpdateAllowList(
	id, name, description string,
	criteria AllowListCriteria,
) (*AllowListSummary, error) {
	b.mu.Lock("UpdateAllowList")
	defer b.mu.Unlock()

	al, ok := b.allowLists[id]
	if !ok {
		return nil, ErrAllowListNotFound
	}

	if name != "" {
		al.Name = name
	}

	al.Description = description
	al.Criteria = criteria
	al.UpdatedAt = time.Now().UTC()

	return &AllowListSummary{
		Arn:         al.Arn,
		CreatedAt:   al.CreatedAt,
		Description: al.Description,
		ID:          al.ID,
		Name:        al.Name,
		UpdatedAt:   al.UpdatedAt,
		Tags:        maps.Clone(al.Tags),
	}, nil
}

// DeleteAllowList deletes an allow list.
func (b *InMemoryBackend) DeleteAllowList(id string) error {
	b.mu.Lock("DeleteAllowList")
	defer b.mu.Unlock()

	if _, ok := b.allowLists[id]; !ok {
		return ErrAllowListNotFound
	}

	delete(b.allowLists, id)

	return nil
}

// ListAllowLists returns summaries of all allow lists.
func (b *InMemoryBackend) ListAllowLists() ([]*AllowListSummary, error) {
	b.mu.RLock("ListAllowLists")
	defer b.mu.RUnlock()

	result := make([]*AllowListSummary, 0, len(b.allowLists))

	for _, al := range b.allowLists {
		result = append(result, &AllowListSummary{
			Arn:         al.Arn,
			CreatedAt:   al.CreatedAt,
			Description: al.Description,
			ID:          al.ID,
			Name:        al.Name,
			UpdatedAt:   al.UpdatedAt,
			Tags:        maps.Clone(al.Tags),
		})
	}

	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })

	return result, nil
}

// CreateCustomDataIdentifier creates a new custom data identifier.
func (b *InMemoryBackend) CreateCustomDataIdentifier(
	name, description, regex string,
	ignoreWords, keywords []string,
	maxMatchDistance *int32,
	tags map[string]string,
) (string, error) {
	if _, compileErr := regexp.Compile(regex); compileErr != nil {
		return "", fmt.Errorf("%w: regex is invalid: %s", ErrValidation, compileErr.Error())
	}

	dist := defaultMatchDist
	if maxMatchDistance != nil {
		if *maxMatchDistance < minMatchDist || *maxMatchDistance > maxMatchDist {
			return "", fmt.Errorf(
				"%w: maximumMatchDistance must be between %d and %d",
				ErrValidation, minMatchDist, maxMatchDist,
			)
		}

		dist = *maxMatchDistance
	}

	b.mu.Lock("CreateCustomDataIdentifier")
	defer b.mu.Unlock()

	id := uuid.New().String()
	now := time.Now().UTC()

	cdi := &storedCustomDataID{
		CustomDataIdentifier: CustomDataIdentifier{
			ID:                   id,
			Arn:                  b.customDataIDARN(id),
			Name:                 name,
			Description:          description,
			Regex:                regex,
			IgnoreWords:          ignoreWords,
			Keywords:             keywords,
			MaximumMatchDistance: dist,
			CreatedAt:            now,
			Tags:                 maps.Clone(tags),
		},
	}

	b.customDataIDs[id] = cdi
	if len(tags) > 0 {
		b.tags[cdi.Arn] = maps.Clone(tags)
	}

	return id, nil
}

// GetCustomDataIdentifier retrieves a custom data identifier.
func (b *InMemoryBackend) GetCustomDataIdentifier(id string) (*CustomDataIdentifier, error) {
	b.mu.RLock("GetCustomDataIdentifier")
	defer b.mu.RUnlock()

	cdi, ok := b.customDataIDs[id]
	if !ok || cdi.Deleted {
		return nil, ErrCustomDataIDNotFound
	}

	cp := cdi.CustomDataIdentifier
	cp.Tags = maps.Clone(cdi.Tags)

	return &cp, nil
}

// DeleteCustomDataIdentifier soft-deletes a custom data identifier.
func (b *InMemoryBackend) DeleteCustomDataIdentifier(id string) error {
	b.mu.Lock("DeleteCustomDataIdentifier")
	defer b.mu.Unlock()

	cdi, ok := b.customDataIDs[id]
	if !ok {
		return ErrCustomDataIDNotFound
	}

	cdi.Deleted = true

	return nil
}

// ListCustomDataIdentifiers returns summaries of all non-deleted custom data identifiers.
func (b *InMemoryBackend) ListCustomDataIdentifiers() ([]*CustomDataIdentifierSummary, error) {
	b.mu.RLock("ListCustomDataIdentifiers")
	defer b.mu.RUnlock()

	result := make([]*CustomDataIdentifierSummary, 0, len(b.customDataIDs))

	for _, cdi := range b.customDataIDs {
		if cdi.Deleted {
			continue
		}

		result = append(result, &CustomDataIdentifierSummary{
			Arn:         cdi.Arn,
			CreatedAt:   cdi.CreatedAt,
			Description: cdi.Description,
			ID:          cdi.ID,
			Name:        cdi.Name,
		})
	}

	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })

	return result, nil
}

// TestCustomDataIdentifier tests a regex against sample text.
func (b *InMemoryBackend) TestCustomDataIdentifier(
	regex string,
	ignoreWords, keywords []string,
	maxMatchDistance *int32,
	sampleText string,
) (int32, error) {
	re, err := regexp.Compile(regex)
	if err != nil {
		return 0, ErrValidation
	}

	matches := re.FindAllStringIndex(sampleText, -1)
	if len(matches) == 0 {
		return 0, nil
	}

	dist := defaultMatchDist
	if maxMatchDistance != nil {
		dist = *maxMatchDistance
	}

	count := int32(0)

	for _, match := range matches {
		matchedText := sampleText[match[0]:match[1]]

		if containsIgnoreWord(matchedText, ignoreWords) {
			continue
		}

		if len(keywords) > 0 && !hasKeywordBefore(sampleText, match[0], keywords, int(dist)) {
			continue
		}

		count++
	}

	return count, nil
}

func containsIgnoreWord(text string, ignoreWords []string) bool {
	lower := strings.ToLower(text)

	for _, iw := range ignoreWords {
		if strings.Contains(lower, strings.ToLower(iw)) {
			return true
		}
	}

	return false
}

func hasKeywordBefore(text string, matchStart int, keywords []string, dist int) bool {
	start := max(matchStart-dist, 0)

	preceding := strings.ToLower(text[start:matchStart])

	for _, kw := range keywords {
		if strings.Contains(preceding, strings.ToLower(kw)) {
			return true
		}
	}

	return false
}

// CreateFindingsFilter creates a new findings filter.
func (b *InMemoryBackend) CreateFindingsFilter(
	name, description, action string,
	position *int32,
	criteria map[string]any,
	tags map[string]string,
) (*FindingsFilterSummary, error) {
	b.mu.Lock("CreateFindingsFilter")
	defer b.mu.Unlock()

	id := uuid.New().String()

	pos := int32(1)
	if position != nil {
		pos = *position
	}

	ff := &storedFindingsFilter{
		FindingsFilterDetail: FindingsFilterDetail{
			ID:              id,
			Arn:             b.findingsFilterARN(id),
			Name:            name,
			Description:     description,
			Action:          action,
			Position:        pos,
			FindingCriteria: criteria,
			Tags:            maps.Clone(tags),
		},
	}

	b.findingsFilters[id] = ff
	if len(tags) > 0 {
		b.tags[ff.Arn] = maps.Clone(tags)
	}

	return &FindingsFilterSummary{
		Action:      ff.Action,
		Arn:         ff.Arn,
		Description: ff.Description,
		ID:          ff.ID,
		Name:        ff.Name,
		Position:    ff.Position,
		Tags:        maps.Clone(ff.Tags),
	}, nil
}

// GetFindingsFilter retrieves a findings filter.
func (b *InMemoryBackend) GetFindingsFilter(id string) (*FindingsFilterDetail, error) {
	b.mu.RLock("GetFindingsFilter")
	defer b.mu.RUnlock()

	ff, ok := b.findingsFilters[id]
	if !ok {
		return nil, ErrFindingsFilterNotFound
	}

	cp := ff.FindingsFilterDetail
	cp.Tags = maps.Clone(ff.Tags)

	return &cp, nil
}

// UpdateFindingsFilter updates an existing findings filter.
func (b *InMemoryBackend) UpdateFindingsFilter(
	id, name, description, action string,
	position *int32,
	criteria map[string]any,
) (*FindingsFilterSummary, error) {
	b.mu.Lock("UpdateFindingsFilter")
	defer b.mu.Unlock()

	ff, ok := b.findingsFilters[id]
	if !ok {
		return nil, ErrFindingsFilterNotFound
	}

	if name != "" {
		ff.Name = name
	}

	ff.Description = description

	if action != "" {
		ff.Action = action
	}

	if position != nil {
		ff.Position = *position
	}

	if criteria != nil {
		ff.FindingCriteria = criteria
	}

	return &FindingsFilterSummary{
		Action:      ff.Action,
		Arn:         ff.Arn,
		Description: ff.Description,
		ID:          ff.ID,
		Name:        ff.Name,
		Position:    ff.Position,
		Tags:        maps.Clone(ff.Tags),
	}, nil
}

// DeleteFindingsFilter deletes a findings filter.
func (b *InMemoryBackend) DeleteFindingsFilter(id string) error {
	b.mu.Lock("DeleteFindingsFilter")
	defer b.mu.Unlock()

	if _, ok := b.findingsFilters[id]; !ok {
		return ErrFindingsFilterNotFound
	}

	delete(b.findingsFilters, id)

	return nil
}

// ListFindingsFilters returns summaries of all findings filters.
func (b *InMemoryBackend) ListFindingsFilters() ([]*FindingsFilterSummary, error) {
	b.mu.RLock("ListFindingsFilters")
	defer b.mu.RUnlock()

	result := make([]*FindingsFilterSummary, 0, len(b.findingsFilters))

	for _, ff := range b.findingsFilters {
		result = append(result, &FindingsFilterSummary{
			Action:      ff.Action,
			Arn:         ff.Arn,
			Description: ff.Description,
			ID:          ff.ID,
			Name:        ff.Name,
			Position:    ff.Position,
			Tags:        maps.Clone(ff.Tags),
		})
	}

	sort.Slice(result, func(i, j int) bool { return result[i].Position < result[j].Position })

	return result, nil
}

// GetFindings retrieves findings by ID.
func (b *InMemoryBackend) GetFindings(findingIDs []string) ([]*Finding, error) {
	b.mu.RLock("GetFindings")
	defer b.mu.RUnlock()

	result := make([]*Finding, 0, len(findingIDs))

	for _, id := range findingIDs {
		f, ok := b.findings[id]
		if !ok {
			return nil, ErrFindingNotFound
		}

		cp := f.Finding
		result = append(result, &cp)
	}

	return result, nil
}

// ListFindings returns finding IDs (optionally filtered).
func (b *InMemoryBackend) ListFindings(_ map[string]any, _ int, _ string) ([]string, string, error) {
	b.mu.RLock("ListFindings")
	defer b.mu.RUnlock()

	ids := collections.SortedKeys(b.findings)

	return ids, "", nil
}

// CreateSampleFindings creates sample findings.
func (b *InMemoryBackend) CreateSampleFindings(findingTypes []string) error {
	b.mu.Lock("CreateSampleFindings")
	defer b.mu.Unlock()

	types := findingTypes
	if len(types) == 0 {
		types = []string{"SensitiveData:S3Object/Personal"}
	}

	now := time.Now().UTC()

	for _, ft := range types {
		id := uuid.New().String()
		b.findings[id] = &storedFinding{
			Finding: Finding{
				AccountID:   b.accountID,
				Archived:    false,
				Category:    "SENSITIVE_DATA",
				CreatedAt:   now,
				Description: "Sample finding of type " + ft,
				ID:          id,
				Region:      b.region,
				Severity:    Severity{Description: "Medium", Score: defaultFindingScore},
				Title:       "Sample: " + ft,
				Type:        ft,
				UpdatedAt:   now,
			},
		}
	}

	return nil
}

// GetFindingStatistics returns statistics grouped by the given field.
func (b *InMemoryBackend) GetFindingStatistics(groupBy string, _ map[string]any) ([]FindingStatisticsGroup, error) {
	b.mu.RLock("GetFindingStatistics")
	defer b.mu.RUnlock()

	counts := make(map[string]int64)

	for _, f := range b.findings {
		var key string

		switch groupBy {
		case "type":
			key = f.Type
		case "severity.description":
			key = f.Severity.Description
		case "resourcesAffected.s3Bucket.name":
			key = "unknown-bucket"
		case "classificationDetails.jobId":
			key = "unknown-job"
		default:
			key = f.Type
		}

		counts[key]++
	}

	result := make([]FindingStatisticsGroup, 0, len(counts))

	for k, v := range counts {
		result = append(result, FindingStatisticsGroup{GroupKey: k, Count: v})
	}

	sort.Slice(result, func(i, j int) bool { return result[i].GroupKey < result[j].GroupKey })

	return result, nil
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	if err := validateTagInput(tags); err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.isKnownARN(resourceARN) {
		return fmt.Errorf("%w: %s", ErrTaggedResourceNotFound, resourceARN)
	}

	existing := b.tags[resourceARN]
	netNew := 0

	for k := range tags {
		if _, alreadySet := existing[k]; !alreadySet {
			netNew++
		}
	}

	if len(existing)+netNew > maxTagCount {
		return fmt.Errorf("%w: resource would exceed %d tag limit", ErrValidation, maxTagCount)
	}

	if existing == nil {
		b.tags[resourceARN] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceARN], tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if !b.isKnownARN(resourceARN) {
		return fmt.Errorf("%w: %s", ErrTaggedResourceNotFound, resourceARN)
	}

	for _, k := range tagKeys {
		delete(b.tags[resourceARN], k)
	}

	return nil
}

// ListTagsForResource returns the tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if !b.isKnownARN(resourceARN) {
		return nil, fmt.Errorf("%w: %s", ErrTaggedResourceNotFound, resourceARN)
	}

	if b.tags[resourceARN] == nil {
		return map[string]string{}, nil
	}

	return maps.Clone(b.tags[resourceARN]), nil
}

// isKnownARN reports whether arn refers to a live resource owned by this backend.
func (b *InMemoryBackend) isKnownARN(arnStr string) bool {
	prefix := arn.Build("macie2", b.region, b.accountID, "")
	if !strings.HasPrefix(arnStr, prefix) {
		return false
	}

	rest := strings.TrimPrefix(arnStr, prefix)

	resourceType, id, found := strings.Cut(rest, "/")
	if !found {
		return false
	}

	switch resourceType {
	case "allow-list":
		_, exists := b.allowLists[id]

		return exists
	case "custom-data-identifier":
		cdi, exists := b.customDataIDs[id]

		return exists && !cdi.Deleted
	case "findings-filter":
		_, exists := b.findingsFilters[id]

		return exists
	}

	return false
}

// AccountID returns the account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.session = nil
	b.allowLists = make(map[string]*storedAllowList)
	b.customDataIDs = make(map[string]*storedCustomDataID)
	b.findingsFilters = make(map[string]*storedFindingsFilter)
	b.findings = make(map[string]*storedFinding)
	b.s3Buckets = make(map[string]*S3BucketMetadata)
	b.tags = make(map[string]map[string]string)
	b.classificationJobs = make(map[string]*ClassificationJob)
	b.members = make(map[string]*Member)
	b.invitations = make(map[string]*Invitation)
	b.administrator = nil
	b.orgAdminAccounts = make(map[string]*OrgAdminAccount)
	b.orgConfig = &OrgConfig{AutoEnable: false}
	b.autoDiscoveryConfig = &AutoDiscoveryConfig{Status: "DISABLED"}
	b.autoDiscoveryAccounts = make(map[string]*AutoDiscoveryAccount)
	b.classExportConfig = nil
	b.classScopes = make(map[string]*ClassificationScope)
	b.findingsPubConfig = nil
	b.resourceProfiles = make(map[string]*ResourceProfile)
	b.resourceDetections = make(map[string][]ResourceProfileDetection)
	b.revealConfig = nil
	b.sensitivityTemplates = make(map[string]*SensitivityInspectionTemplate)
}

func validateTagInput(tags map[string]string) error {
	if len(tags) > maxTagCount {
		return fmt.Errorf("%w: tags must not exceed %d entries", ErrValidation, maxTagCount)
	}

	for k, v := range tags {
		if len(k) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key must not exceed %d characters", ErrValidation, maxTagKeyLen)
		}

		if len(v) > maxTagValueLen {
			return fmt.Errorf("%w: tag value must not exceed %d characters", ErrValidation, maxTagValueLen)
		}
	}

	return nil
}

type snapshot struct {
	Session               *Session                                  `json:"session,omitempty"`
	AllowLists            map[string]*storedAllowList               `json:"allowLists"`
	CustomDataIDs         map[string]*storedCustomDataID            `json:"customDataIds"`
	FindingsFilters       map[string]*storedFindingsFilter          `json:"findingsFilters"`
	Findings              map[string]*storedFinding                 `json:"findings"`
	S3Buckets             map[string]*S3BucketMetadata              `json:"s3Buckets"`
	Tags                  map[string]map[string]string              `json:"tags"`
	ClassificationJobs    map[string]*ClassificationJob             `json:"classificationJobs"`
	Members               map[string]*Member                        `json:"members"`
	Invitations           map[string]*Invitation                    `json:"invitations"`
	Administrator         *AdministratorAccount                     `json:"administrator,omitempty"`
	OrgAdminAccounts      map[string]*OrgAdminAccount               `json:"orgAdminAccounts"`
	OrgConfig             *OrgConfig                                `json:"orgConfig,omitempty"`
	AutoDiscoveryConfig   *AutoDiscoveryConfig                      `json:"autoDiscoveryConfig,omitempty"`
	AutoDiscoveryAccounts map[string]*AutoDiscoveryAccount          `json:"autoDiscoveryAccounts"`
	ClassExportConfig     *ClassificationExportConfig               `json:"classExportConfig,omitempty"`
	ClassScopes           map[string]*ClassificationScope           `json:"classScopes"`
	FindingsPubConfig     *FindingsPublicationConfig                `json:"findingsPubConfig,omitempty"`
	ResourceProfiles      map[string]*ResourceProfile               `json:"resourceProfiles"`
	ResourceDetections    map[string][]ResourceProfileDetection     `json:"resourceDetections"`
	RevealConfig          *RevealConfiguration                      `json:"revealConfig,omitempty"`
	SensitivityTemplates  map[string]*SensitivityInspectionTemplate `json:"sensitivityTemplates"`
}

// Snapshot serializes backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(ctx, "macie2", snapshot{
		Session:               b.session,
		AllowLists:            b.allowLists,
		CustomDataIDs:         b.customDataIDs,
		FindingsFilters:       b.findingsFilters,
		Findings:              b.findings,
		S3Buckets:             b.s3Buckets,
		Tags:                  b.tags,
		ClassificationJobs:    b.classificationJobs,
		Members:               b.members,
		Invitations:           b.invitations,
		Administrator:         b.administrator,
		OrgAdminAccounts:      b.orgAdminAccounts,
		OrgConfig:             b.orgConfig,
		AutoDiscoveryConfig:   b.autoDiscoveryConfig,
		AutoDiscoveryAccounts: b.autoDiscoveryAccounts,
		ClassExportConfig:     b.classExportConfig,
		ClassScopes:           b.classScopes,
		FindingsPubConfig:     b.findingsPubConfig,
		ResourceProfiles:      b.resourceProfiles,
		ResourceDetections:    b.resourceDetections,
		RevealConfig:          b.revealConfig,
		SensitivityTemplates:  b.sensitivityTemplates,
	})
}

// Restore deserializes backend state from JSON.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap snapshot
	if err := persistence.UnmarshalSnapshot(ctx, "macie2", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.session = snap.Session
	b.allowLists = snap.AllowLists
	b.customDataIDs = snap.CustomDataIDs
	b.findingsFilters = snap.FindingsFilters
	b.findings = snap.Findings
	b.s3Buckets = snap.S3Buckets
	b.tags = snap.Tags
	if b.s3Buckets == nil {
		b.s3Buckets = make(map[string]*S3BucketMetadata)
	}
	b.classificationJobs = snap.ClassificationJobs
	b.members = snap.Members
	b.invitations = snap.Invitations
	b.administrator = snap.Administrator
	b.orgAdminAccounts = snap.OrgAdminAccounts
	b.orgConfig = snap.OrgConfig
	b.autoDiscoveryConfig = snap.AutoDiscoveryConfig
	b.autoDiscoveryAccounts = snap.AutoDiscoveryAccounts
	b.classExportConfig = snap.ClassExportConfig
	b.classScopes = snap.ClassScopes
	b.findingsPubConfig = snap.FindingsPubConfig
	b.resourceProfiles = snap.ResourceProfiles
	b.resourceDetections = snap.ResourceDetections
	b.revealConfig = snap.RevealConfig
	b.sensitivityTemplates = snap.SensitivityTemplates

	return nil
}
