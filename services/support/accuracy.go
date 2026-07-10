package support

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const (
	defaultAccountID         = "000000000000"
	defaultCaseLanguage      = "en"
	defaultIssueType         = "technical"
	defaultSubmittedBy       = "customer"
	maxCCEmailAddresses      = 10
	maxCommunicationBodySize = 8000
	maxAttachmentsPerSet     = 3
	maxAttachmentSize        = 5 * 1024 * 1024
	defaultPageSize          = 100
	minPageSize              = 10
	maxPageSize              = 100
	recentCommunicationsSize = 5
)

var (
	// ErrAttachmentSetNotFound indicates an unknown staged attachment set.
	ErrAttachmentSetNotFound = awserr.New("AttachmentSetIdNotFound", awserr.ErrNotFound)
	// ErrAttachmentSetExpired indicates a staged attachment set passed its one-hour lifetime.
	ErrAttachmentSetExpired = awserr.New("AttachmentSetExpired", awserr.ErrInvalidParameter)
)

// CreateCaseOptions contains API-visible case creation data.
type CreateCaseOptions struct {
	Subject           string
	ServiceCode       string
	CategoryCode      string
	SeverityCode      string
	CommunicationBody string
	AttachmentSetID   string
	Language          string
	IssueType         string
	CCEmails          []string
}

// AddCommunicationOptions contains API-visible communication input data.
type AddCommunicationOptions struct {
	CaseID            string
	CommunicationBody string
	AttachmentSetID   string
	CCEmails          []string
}

// DescribeCasesOptions contains supported case query and pagination filters.
type DescribeCasesOptions struct {
	AfterTime             *time.Time
	BeforeTime            *time.Time
	DisplayID             string
	Language              string
	NextToken             string
	CaseIDs               []string
	MaxResults            int
	IncludeResolvedCases  bool
	IncludeCommunications bool
}

// DescribeCommunicationsOptions contains communication filters and pagination data.
type DescribeCommunicationsOptions struct {
	AfterTime  *time.Time
	BeforeTime *time.Time
	CaseID     string
	NextToken  string
	MaxResults int
}

// CreateCaseWithOptions stores an AWS-shaped support case and its initial communication.
func (b *InMemoryBackend) CreateCaseWithOptions(in CreateCaseOptions) (*Case, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	attachments, err := b.consumeAttachmentSetLocked(in.AttachmentSetID)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	b.nextDisplayID++
	cs := &Case{
		CaseID: fmt.Sprintf(
			"case-%s-%d-%s",
			defaultAccountID,
			now.Year(),
			strings.ReplaceAll(uuid.NewString(), "-", "")[:16],
		),
		DisplayID:    fmt.Sprintf("%010d", b.nextDisplayID),
		Subject:      in.Subject,
		Status:       caseStatusOpened,
		ServiceCode:  in.ServiceCode,
		CategoryCode: in.CategoryCode,
		SeverityCode: in.SeverityCode,
		Language:     defaultIfEmpty(in.Language, defaultCaseLanguage),
		IssueType:    defaultIfEmpty(in.IssueType, defaultIssueType),
		SubmittedBy:  defaultSubmittedBy,
		CCEmails:     append([]string(nil), in.CCEmails...),
		Body:         in.CommunicationBody,
		CreatedTime:  now,
	}
	b.cases.Put(cs)
	b.communications[cs.CaseID] = []Communication{
		newCommunication(cs.CaseID, in.CommunicationBody, in.CCEmails, attachments, now),
	}

	out := *cs
	out.CCEmails = append([]string(nil), cs.CCEmails...)

	return &out, nil
}

// DescribeCasesWithOptions returns ordered and paginated cases matching API filters.
func (b *InMemoryBackend) DescribeCasesWithOptions(in DescribeCasesOptions) ([]Case, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	all := b.cases.All()
	cases := make([]Case, 0, len(all))
	for _, cs := range all {
		if matchCase(cs, in) {
			cp := *cs
			cp.CCEmails = append([]string(nil), cs.CCEmails...)
			cases = append(cases, cp)
		}
	}
	sort.Slice(cases, func(i, j int) bool { return cases[i].CreatedTime.After(cases[j].CreatedTime) })

	start, err := pageOffset(in.NextToken)
	if err != nil {
		return nil, "", err
	}

	return paginate(cases, start, pageLimit(in.MaxResults))
}

// RecentCommunications returns up to five newest communications for DescribeCases.
func (b *InMemoryBackend) RecentCommunications(caseID string) ([]Communication, string) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	comms := cloneCommunications(b.communications[caseID])
	sortCommunications(comms)
	page, token, _ := paginate(comms, 0, recentCommunicationsSize)

	return page, token
}

// AddCommunicationWithOptions stores a customer response and reopens a resolved case.
func (b *InMemoryBackend) AddCommunicationWithOptions(in AddCommunicationOptions) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	cs, ok := b.cases.Get(in.CaseID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, in.CaseID)
	}
	attachments, err := b.consumeAttachmentSetLocked(in.AttachmentSetID)
	if err != nil {
		return err
	}
	if cs.Status == caseStatusResolved {
		cs.Status = "reopened"
		cs.ResolvedTime = nil
	}
	b.communications[in.CaseID] = append(
		b.communications[in.CaseID],
		newCommunication(in.CaseID, in.CommunicationBody, in.CCEmails, attachments, time.Now()),
	)

	return nil
}

// ResolveCaseWithStatus resolves a case and returns its real prior status.
func (b *InMemoryBackend) ResolveCaseWithStatus(caseID string) (string, *Case, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cs, ok := b.cases.Get(caseID)
	if !ok {
		return "", nil, fmt.Errorf("%w: %s", ErrNotFound, caseID)
	}
	if cs.Status == caseStatusResolved {
		return "", nil, fmt.Errorf("%w: %s", ErrAlreadyResolved, caseID)
	}
	initialStatus := cs.Status
	now := time.Now()
	cs.Status = caseStatusResolved
	cs.ResolvedTime = &now
	out := *cs

	return initialStatus, &out, nil
}

// DescribeCommunicationsWithOptions returns ordered communications matching API filters.
func (b *InMemoryBackend) DescribeCommunicationsWithOptions(
	in DescribeCommunicationsOptions,
) ([]Communication, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.cases.Has(in.CaseID) {
		return nil, "", fmt.Errorf("%w: %s", ErrNotFound, in.CaseID)
	}
	comms := make([]Communication, 0, len(b.communications[in.CaseID]))
	for _, comm := range b.communications[in.CaseID] {
		if in.AfterTime != nil && !comm.TimeCreated.After(*in.AfterTime) {
			continue
		}
		if in.BeforeTime != nil && !comm.TimeCreated.Before(*in.BeforeTime) {
			continue
		}
		comms = append(comms, cloneCommunication(comm))
	}
	sortCommunications(comms)
	start, err := pageOffset(in.NextToken)
	if err != nil {
		return nil, "", err
	}

	return paginate(comms, start, pageLimit(in.MaxResults))
}

// AddAttachmentsToSetWithAttachments stages uploaded files for single-use consumption.
func (b *InMemoryBackend) AddAttachmentsToSetWithAttachments(
	attachmentSetID string,
	attachments []Attachment,
) (string, time.Time, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(attachments) == 0 {
		return "", time.Time{}, fmt.Errorf("%w: attachments is required", ErrValidation)
	}
	if err := validateAttachments(attachments); err != nil {
		return "", time.Time{}, err
	}
	set, setID, err := b.attachmentSetForAppendLocked(attachmentSetID)
	if err != nil {
		return "", time.Time{}, err
	}
	if len(set.AttachmentIDs)+len(attachments) > maxAttachmentsPerSet {
		return "", time.Time{}, fmt.Errorf(
			"%w: maximum of %d attachments exceeded",
			ErrValidation,
			maxAttachmentsPerSet,
		)
	}
	for _, attachment := range attachments {
		attachment.AttachmentID = "att-" + uuid.NewString()
		cp := attachment
		cp.Data = append([]byte(nil), attachment.Data...)
		b.attachments.Put(&cp)
		set.AttachmentIDs = append(set.AttachmentIDs, cp.AttachmentID)
	}
	set.Expiry = time.Now().Add(time.Hour)

	return setID, set.Expiry, nil
}

func validateAttachments(attachments []Attachment) error {
	for _, attachment := range attachments {
		if attachment.FileName == "" || len(attachment.Data) == 0 || len(attachment.Data) > maxAttachmentSize {
			return fmt.Errorf("%w: invalid attachment", ErrValidation)
		}
	}

	return nil
}

func (b *InMemoryBackend) attachmentSetForAppendLocked(id string) (*AttachmentSet, string, error) {
	if id == "" {
		if b.attachmentSets.Len() >= maxAttachmentSets {
			// Evict the set with the earliest (soonest-expired) expiry.
			var oldestID string
			var oldestExpiry time.Time

			found := false

			b.attachmentSets.Range(func(s *AttachmentSet) bool {
				if !found || s.Expiry.Before(oldestExpiry) {
					oldestID = s.ID
					oldestExpiry = s.Expiry
					found = true
				}

				return true
			})

			b.attachmentSets.Delete(oldestID)
		}

		id = uuid.NewString()
		set := &AttachmentSet{ID: id}
		b.attachmentSets.Put(set)

		return set, id, nil
	}

	set, ok := b.attachmentSets.Get(id)
	if !ok {
		return nil, "", fmt.Errorf("%w: %s", ErrAttachmentSetNotFound, id)
	}

	if time.Now().After(set.Expiry) {
		return nil, "", fmt.Errorf("%w: %s", ErrAttachmentSetExpired, id)
	}

	return set, id, nil
}

func (b *InMemoryBackend) consumeAttachmentSetLocked(id string) ([]AttachmentRef, error) {
	if id == "" {
		return nil, nil
	}
	set, ok := b.attachmentSets.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrAttachmentSetNotFound, id)
	}
	if time.Now().After(set.Expiry) {
		return nil, fmt.Errorf("%w: %s", ErrAttachmentSetExpired, id)
	}
	refs := make([]AttachmentRef, 0, len(set.AttachmentIDs))
	for _, attachmentID := range set.AttachmentIDs {
		attachment, _ := b.attachments.Get(attachmentID)
		refs = append(refs, AttachmentRef{AttachmentID: attachmentID, FileName: attachment.FileName})
	}
	b.attachmentSets.Delete(id)

	return refs, nil
}

func validateCreateCase(in CreateCaseOptions) error {
	switch {
	case in.Subject == "":
		return fmt.Errorf("%w: subject is required", ErrValidation)
	case in.CommunicationBody == "":
		return fmt.Errorf("%w: communicationBody is required", ErrValidation)
	case len(in.CommunicationBody) > maxCommunicationBodySize:
		return fmt.Errorf("%w: communicationBody exceeds %d characters", ErrValidation, maxCommunicationBodySize)
	case len(in.CCEmails) > maxCCEmailAddresses:
		return fmt.Errorf("%w: too many ccEmailAddresses", ErrValidation)
	case in.Language != "" && !validLanguage(in.Language):
		return fmt.Errorf("%w: invalid language", ErrValidation)
	case in.IssueType != "" && !validIssueType(in.IssueType):
		return fmt.Errorf("%w: invalid issueType", ErrValidation)
	case in.SeverityCode != "" && !validSeverity(in.SeverityCode):
		return fmt.Errorf("%w: invalid severityCode", ErrValidation)
	}

	return nil
}

func validateCommunication(in AddCommunicationOptions) error {
	switch {
	case in.CaseID == "":
		return fmt.Errorf("%w: caseId is required", ErrValidation)
	case in.CommunicationBody == "":
		return fmt.Errorf("%w: communicationBody is required", ErrValidation)
	case len(in.CommunicationBody) > maxCommunicationBodySize:
		return fmt.Errorf("%w: communicationBody exceeds %d characters", ErrValidation, maxCommunicationBodySize)
	case len(in.CCEmails) > maxCCEmailAddresses:
		return fmt.Errorf("%w: too many ccEmailAddresses", ErrValidation)
	}

	return nil
}

func validLanguage(value string) bool {
	return value == "en" || value == "ja" || value == "zh" || value == "ko"
}

func validIssueType(value string) bool {
	return value == "technical" || value == "customer-service" || value == "account-and-billing"
}

func validSeverity(value string) bool {
	return value == severityLow || value == severityNormal || value == severityHigh ||
		value == severityUrgent || value == severityCritical
}

var (
	checkIDSetOnce sync.Once           //nolint:gochecknoglobals // sync.Once lazy init for static check ID set
	checkIDSet     map[string]struct{} //nolint:gochecknoglobals // populated exactly once by initCheckIDSet
)

func initCheckIDSet() {
	checkIDSetOnce.Do(func() {
		checks := trustedAdvisorChecks()
		checkIDSet = make(map[string]struct{}, len(checks))

		for _, c := range checks {
			checkIDSet[c.ID] = struct{}{}
		}
	})
}

func validCheckID(checkID string) bool {
	initCheckIDSet()
	_, ok := checkIDSet[checkID]

	return ok
}

func validateCheckIDs(checkIDs []string) error {
	if len(checkIDs) == 0 {
		return fmt.Errorf("%w: checkIds is required", ErrValidation)
	}
	for _, checkID := range checkIDs {
		if !validCheckID(checkID) {
			return fmt.Errorf("%w: invalid checkId %s", ErrValidation, checkID)
		}
	}

	return nil
}

func newCommunication(
	caseID string,
	body string,
	ccEmails []string,
	attachments []AttachmentRef,
	now time.Time,
) Communication {
	return Communication{
		CaseID:        caseID,
		Body:          body,
		SubmittedBy:   defaultSubmittedBy,
		TimeCreated:   now,
		CCEmails:      append([]string(nil), ccEmails...),
		AttachmentSet: append([]AttachmentRef(nil), attachments...),
	}
}

func matchCase(cs *Case, in DescribeCasesOptions) bool {
	if !in.IncludeResolvedCases && cs.Status == caseStatusResolved {
		return false
	}
	if in.DisplayID != "" && cs.DisplayID != in.DisplayID {
		return false
	}
	if in.Language != "" && cs.Language != in.Language {
		return false
	}
	if in.AfterTime != nil && !cs.CreatedTime.After(*in.AfterTime) {
		return false
	}
	if in.BeforeTime != nil && !cs.CreatedTime.Before(*in.BeforeTime) {
		return false
	}
	if len(in.CaseIDs) == 0 {
		return true
	}

	return slices.Contains(in.CaseIDs, cs.CaseID)
}

func cloneCommunication(comm Communication) Communication {
	comm.CCEmails = append([]string(nil), comm.CCEmails...)
	comm.AttachmentSet = append([]AttachmentRef(nil), comm.AttachmentSet...)

	return comm
}

func cloneCommunications(comms []Communication) []Communication {
	out := make([]Communication, 0, len(comms))
	for _, comm := range comms {
		out = append(out, cloneCommunication(comm))
	}

	return out
}

func sortCommunications(comms []Communication) {
	sort.Slice(comms, func(i, j int) bool { return comms[i].TimeCreated.After(comms[j].TimeCreated) })
}

func pageLimit(maxResults int) int {
	if maxResults == 0 {
		return defaultPageSize
	}

	return maxResults
}

func validatePageSize(maxResults int) error {
	if maxResults != 0 && (maxResults < minPageSize || maxResults > maxPageSize) {
		return fmt.Errorf("%w: maxResults must be between %d and %d", ErrValidation, minPageSize, maxPageSize)
	}

	return nil
}

type pageTokenJSON struct {
	Offset int `json:"o"`
}

func pageOffset(token string) (int, error) {
	if token == "" {
		return 0, nil
	}

	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid nextToken", ErrValidation)
	}

	var t pageTokenJSON
	if unmarshalErr := json.Unmarshal(decoded, &t); unmarshalErr != nil || t.Offset < 0 {
		return 0, fmt.Errorf("%w: invalid nextToken", ErrValidation)
	}

	return t.Offset, nil
}

func encodePageToken(offset int) string {
	data, _ := json.Marshal(pageTokenJSON{Offset: offset})

	return base64.RawURLEncoding.EncodeToString(data)
}

func paginate[T any](values []T, start, limit int) ([]T, string, error) {
	if start > len(values) {
		return nil, "", fmt.Errorf("%w: invalid nextToken", ErrValidation)
	}

	end := min(start+limit, len(values))
	nextToken := ""

	if end < len(values) {
		nextToken = encodePageToken(end)
	}

	return values[start:end], nextToken, nil
}

func defaultIfEmpty(value, fallback string) string {
	if value == "" {
		return fallback
	}

	return value
}
