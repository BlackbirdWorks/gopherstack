package codecommit

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	errRepoDoesNotExist             = "RepositoryDoesNotExistException"
	errApprovalRuleTemplateNotExist = "ApprovalRuleTemplateDoesNotExistException"

	prStatusOpen   = "OPEN"
	prStatusClosed = "CLOSED"

	// maxBatchGetRepositories is the AWS limit for BatchGetRepositories.
	maxBatchGetRepositories = 25
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New(errRepoDoesNotExist, awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("RepositoryNameExistsException", awserr.ErrConflict)
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("InvalidParameterException", awserr.ErrInvalidParameter)
	// ErrApprovalRuleTemplateNotFound is returned when an approval rule template is not found.
	ErrApprovalRuleTemplateNotFound = awserr.New(errApprovalRuleTemplateNotExist, awserr.ErrNotFound)
	// ErrApprovalRuleTemplateAlreadyExists is returned when an approval rule template already exists.
	ErrApprovalRuleTemplateAlreadyExists = awserr.New(
		"ApprovalRuleTemplateNameAlreadyExistsException",
		awserr.ErrConflict,
	)
	// ErrBranchNotFound is returned when a branch is not found.
	ErrBranchNotFound = awserr.New("BranchDoesNotExistException", awserr.ErrNotFound)
	// ErrBranchAlreadyExists is returned when a branch already exists.
	ErrBranchAlreadyExists = awserr.New("BranchNameExistsException", awserr.ErrConflict)
	// ErrCommitNotFound is returned when a commit is not found.
	ErrCommitNotFound = awserr.New("CommitDoesNotExistException", awserr.ErrNotFound)
	// ErrPullRequestNotFound is returned when a pull request is not found.
	ErrPullRequestNotFound = awserr.New("PullRequestDoesNotExistException", awserr.ErrNotFound)
	// ErrPullRequestAlreadyMerged is returned when a PR is already merged.
	ErrPullRequestAlreadyMerged = awserr.New("PullRequestAlreadyClosedException", awserr.ErrConflict)
	// ErrInvalidRepositoryName is returned when a repository name is invalid.
	ErrInvalidRepositoryName = awserr.New("InvalidRepositoryNameException", awserr.ErrInvalidParameter)
	// ErrMaxRepositoriesExceeded is returned when too many repositories are requested.
	ErrMaxRepositoriesExceeded = awserr.New("MaximumRepositoryNamesExceededException", awserr.ErrInvalidParameter)
)

// repoNameRe matches valid CodeCommit repository names: alphanumeric, _, -, .
var repoNameRe = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

// ValidateRepositoryName returns an error if name is not a valid CodeCommit repository name.
func ValidateRepositoryName(name string) error {
	if len(name) == 0 || len(name) > 100 {
		return fmt.Errorf("%w: repository name must be between 1 and 100 characters", ErrInvalidRepositoryName)
	}
	if !repoNameRe.MatchString(name) {
		return fmt.Errorf(
			"%w: repository name may only contain alphanumeric characters, underscores, hyphens, and periods",
			ErrInvalidRepositoryName,
		)
	}

	return nil
}

// ApprovalRuleTemplate represents an AWS CodeCommit approval rule template.
type ApprovalRuleTemplate struct {
	CreationDate                    time.Time `json:"creationDate"`
	LastModifiedDate                time.Time `json:"lastModifiedDate"`
	ApprovalRuleTemplateID          string    `json:"approvalRuleTemplateId"`
	ApprovalRuleTemplateName        string    `json:"approvalRuleTemplateName"`
	ApprovalRuleTemplateARN         string    `json:"approvalRuleTemplateArn"`
	ApprovalRuleTemplateContent     string    `json:"approvalRuleTemplateContent"`
	ApprovalRuleTemplateDescription string    `json:"approvalRuleTemplateDescription,omitempty"`
	LastModifiedUser                string    `json:"lastModifiedUser,omitempty"`
	RuleContentSha256               string    `json:"ruleContentSha256"`
}

// Branch represents a CodeCommit branch.
type Branch struct {
	BranchName     string `json:"branchName"`
	CommitID       string `json:"commitId"`
	RepositoryName string `json:"repositoryName"`
}

// Commit represents a CodeCommit commit.
type Commit struct {
	CommitID       string   `json:"commitId"`
	TreeID         string   `json:"treeId"`
	Message        string   `json:"message,omitempty"`
	AdditionalData string   `json:"additionalData,omitempty"`
	AuthorName     string   `json:"authorName,omitempty"`
	AuthorEmail    string   `json:"authorEmail,omitempty"`
	CommitterName  string   `json:"committerName,omitempty"`
	CommitterEmail string   `json:"committerEmail,omitempty"`
	RepositoryName string   `json:"repositoryName"`
	Parents        []string `json:"parents,omitempty"`
}

// PullRequestTarget represents a target for a pull request.
type PullRequestTarget struct {
	RepositoryName       string `json:"repositoryName"`
	SourceReference      string `json:"sourceReference"`
	DestinationReference string `json:"destinationReference,omitempty"`
	SourceCommit         string `json:"sourceCommit,omitempty"`
	DestinationCommit    string `json:"destinationCommit,omitempty"`
	MergeBase            string `json:"mergeBase,omitempty"`
}

// PullRequest represents a CodeCommit pull request.
type PullRequest struct {
	CreationDate       time.Time           `json:"creationDate"`
	LastActivityDate   time.Time           `json:"lastActivityDate"`
	PullRequestID      string              `json:"pullRequestId"`
	Title              string              `json:"title"`
	Description        string              `json:"description,omitempty"`
	AuthorARN          string              `json:"authorArn,omitempty"`
	PullRequestStatus  string              `json:"pullRequestStatus"`
	ClientRequestToken string              `json:"clientRequestToken,omitempty"`
	RevisionID         string              `json:"revisionId"`
	PullRequestTargets []PullRequestTarget `json:"pullRequestTargets"`
}

// Repository represents an AWS CodeCommit repository.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via TagResource / CreateRepository.
type Repository struct {
	CreationDate     time.Time  `json:"creationDate"`
	LastModifiedDate time.Time  `json:"lastModifiedDate"`
	Tags             *tags.Tags `json:"tags,omitempty"`
	RepositoryName   string     `json:"repositoryName"`
	RepositoryID     string     `json:"repositoryId"`
	ARN              string     `json:"arn"`
	Description      string     `json:"repositoryDescription,omitempty"`
	AccountID        string     `json:"accountId"`
	Region           string     `json:"-"`
	CloneURLHTTP     string     `json:"cloneUrlHttp"`
	CloneURLSSH      string     `json:"cloneUrlSsh"`
	KmsKeyID         string     `json:"kmsKeyId,omitempty"`
	DefaultBranch    string     `json:"defaultBranch,omitempty"`
}

// InMemoryBackend is the in-memory store for CodeCommit resources.
type InMemoryBackend struct {
	repositories          map[string]*Repository           // key: repositoryName
	repositoriesByARN     map[string]string                // key: ARN → repositoryName
	approvalRuleTemplates map[string]*ApprovalRuleTemplate // key: templateName
	// repoTemplateAssoc maps repositoryName -> set of templateNames
	repoTemplateAssoc map[string]map[string]struct{}
	// branches maps repositoryName -> branchName -> Branch
	branches map[string]map[string]*Branch
	// commits maps repositoryName -> commitId -> Commit
	commits map[string]map[string]*Commit
	// pullRequests maps pullRequestId -> PullRequest
	pullRequests map[string]*PullRequest
	// prApprovals maps prID -> userARN -> approvalState
	prApprovals map[string]map[string]string
	// prApprovalRules maps prID -> ruleName -> rule
	prApprovalRules map[string]map[string]*PullRequestApprovalRule
	// prOverrides maps prID -> overridden
	prOverrides map[string]bool
	// prOverriders maps prID -> overrider ARN
	prOverriders map[string]string
	// prEvents maps prID -> events
	prEvents map[string][]PullRequestEvent
	// comments maps commentID -> Comment
	comments map[string]*Comment
	// commentReactions maps commentID -> reactions
	commentReactions map[string][]Reaction
	// files maps repoName -> filePath -> File
	files map[string]map[string]*File
	// triggers maps repoName -> triggers
	triggers      map[string][]RepositoryTrigger
	mu            *lockmetrics.RWMutex
	accountID     string
	region        string
	nextPRCounter int
}

// NewInMemoryBackend creates a new in-memory CodeCommit backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		repositories:          make(map[string]*Repository),
		repositoriesByARN:     make(map[string]string),
		approvalRuleTemplates: make(map[string]*ApprovalRuleTemplate),
		repoTemplateAssoc:     make(map[string]map[string]struct{}),
		branches:              make(map[string]map[string]*Branch),
		commits:               make(map[string]map[string]*Commit),
		pullRequests:          make(map[string]*PullRequest),
		prApprovals:           make(map[string]map[string]string),
		prApprovalRules:       make(map[string]map[string]*PullRequestApprovalRule),
		prOverrides:           make(map[string]bool),
		prOverriders:          make(map[string]string),
		prEvents:              make(map[string][]PullRequestEvent),
		comments:              make(map[string]*Comment),
		commentReactions:      make(map[string][]Reaction),
		files:                 make(map[string]map[string]*File),
		triggers:              make(map[string][]RepositoryTrigger),
		accountID:             accountID,
		region:                region,
		mu:                    lockmetrics.New("codecommit"),
	}
}

// Reset clears all backend state, returning it to a pristine empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, r := range b.repositories {
		r.Tags.Close()
	}

	b.repositories = make(map[string]*Repository)
	b.repositoriesByARN = make(map[string]string)
	b.approvalRuleTemplates = make(map[string]*ApprovalRuleTemplate)
	b.repoTemplateAssoc = make(map[string]map[string]struct{})
	b.branches = make(map[string]map[string]*Branch)
	b.commits = make(map[string]map[string]*Commit)
	b.pullRequests = make(map[string]*PullRequest)
	b.prApprovals = make(map[string]map[string]string)
	b.prApprovalRules = make(map[string]map[string]*PullRequestApprovalRule)
	b.prOverrides = make(map[string]bool)
	b.prOverriders = make(map[string]string)
	b.prEvents = make(map[string][]PullRequestEvent)
	b.comments = make(map[string]*Comment)
	b.commentReactions = make(map[string][]Reaction)
	b.files = make(map[string]map[string]*File)
	b.triggers = make(map[string][]RepositoryTrigger)
	b.nextPRCounter = 0
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// CreateRepository creates a new CodeCommit repository.
func (b *InMemoryBackend) CreateRepository(name, description string, kv map[string]string) (*Repository, error) {
	b.mu.Lock("CreateRepository")
	defer b.mu.Unlock()

	if err := ValidateRepositoryName(name); err != nil {
		return nil, err
	}

	if _, ok := b.repositories[name]; ok {
		return nil, fmt.Errorf("%w: repository %s already exists", ErrAlreadyExists, name)
	}

	repoARN := arn.Build("codecommit", b.region, b.accountID, name)
	repoID := uuid.NewString()
	t := tags.New("codecommit.repository." + name + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}
	now := time.Now().UTC()
	r := &Repository{
		RepositoryName:   name,
		RepositoryID:     repoID,
		ARN:              repoARN,
		Description:      description,
		AccountID:        b.accountID,
		Region:           b.region,
		CreationDate:     now,
		LastModifiedDate: now,
		CloneURLHTTP:     fmt.Sprintf("https://git-codecommit.%s.amazonaws.com/v1/repos/%s", b.region, name),
		CloneURLSSH:      fmt.Sprintf("ssh://git-codecommit.%s.amazonaws.com/v1/repos/%s", b.region, name),
		Tags:             t,
	}
	b.repositories[name] = r
	b.repositoriesByARN[repoARN] = name
	cp := *r

	return &cp, nil
}

// GetRepository returns a repository by name.
func (b *InMemoryBackend) GetRepository(name string) (*Repository, error) {
	b.mu.RLock("GetRepository")
	defer b.mu.RUnlock()

	r, ok := b.repositories[name]
	if !ok {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, name)
	}
	cp := *r

	return &cp, nil
}

// DeleteRepository deletes a repository by name and cascades to branches, commits and
// template associations for that repository.
func (b *InMemoryBackend) DeleteRepository(name string) (*Repository, error) {
	b.mu.Lock("DeleteRepository")
	defer b.mu.Unlock()

	r, ok := b.repositories[name]
	if !ok {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, name)
	}
	cp := *r
	delete(b.repositories, name)
	delete(b.repositoriesByARN, r.ARN)
	r.Tags.Close()

	// Cascade: remove branches, commits, template-associations for this repo.
	delete(b.branches, name)
	delete(b.commits, name)
	delete(b.repoTemplateAssoc, name)

	return &cp, nil
}

// ListRepositories returns all repositories sorted by name.
func (b *InMemoryBackend) ListRepositories() []*Repository {
	b.mu.RLock("ListRepositories")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.repositories))
	for n := range b.repositories {
		names = append(names, n)
	}
	sort.Strings(names)

	list := make([]*Repository, 0, len(names))
	for _, n := range names {
		cp := *b.repositories[n]
		list = append(list, &cp)
	}

	return list
}

// TagResource adds or replaces tags on a repository by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, kv map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	name, ok := b.repositoriesByARN[resourceARN]
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}
	b.repositories[name].Tags.Merge(kv)

	return nil
}

// UntagResource removes tags from a repository by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	name, ok := b.repositoriesByARN[resourceARN]
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}
	b.repositories[name].Tags.DeleteKeys(tagKeys)

	return nil
}

// ListTagsForResource returns tags for a repository by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	name, ok := b.repositoriesByARN[resourceARN]
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	return b.repositories[name].Tags.Clone(), nil
}

// BatchGetRepositories returns repositories by name, splitting results into found/notFound.
// AWS enforces a maximum of 25 repository names per request.
func (b *InMemoryBackend) BatchGetRepositories(names []string) ([]*Repository, []string, error) {
	if len(names) > maxBatchGetRepositories {
		return nil, nil, fmt.Errorf(
			"%w: a maximum of %d repository names may be specified",
			ErrMaxRepositoriesExceeded,
			maxBatchGetRepositories,
		)
	}

	b.mu.RLock("BatchGetRepositories")
	defer b.mu.RUnlock()

	var found []*Repository
	var notFound []string

	for _, name := range names {
		r, ok := b.repositories[name]
		if !ok {
			notFound = append(notFound, name)

			continue
		}
		cp := *r
		found = append(found, &cp)
	}

	return found, notFound, nil
}

// CreateApprovalRuleTemplate creates a new approval rule template.
func (b *InMemoryBackend) CreateApprovalRuleTemplate(name, description, content string) (*ApprovalRuleTemplate, error) {
	b.mu.Lock("CreateApprovalRuleTemplate")
	defer b.mu.Unlock()

	if _, ok := b.approvalRuleTemplates[name]; ok {
		return nil, fmt.Errorf(
			"%w: approval rule template %s already exists",
			ErrApprovalRuleTemplateAlreadyExists,
			name,
		)
	}

	templateID := uuid.NewString()
	templateARN := arn.Build("codecommit", b.region, b.accountID, "approval-rule-template/"+name)
	now := time.Now().UTC()
	hash := sha256.Sum256([]byte(content))
	t := &ApprovalRuleTemplate{
		ApprovalRuleTemplateID:          templateID,
		ApprovalRuleTemplateName:        name,
		ApprovalRuleTemplateARN:         templateARN,
		ApprovalRuleTemplateContent:     content,
		ApprovalRuleTemplateDescription: description,
		CreationDate:                    now,
		LastModifiedDate:                now,
		RuleContentSha256:               hex.EncodeToString(hash[:]),
	}
	b.approvalRuleTemplates[name] = t
	cp := *t

	return &cp, nil
}

// AssociateApprovalRuleTemplateWithRepository associates an approval rule template with a repository.
func (b *InMemoryBackend) AssociateApprovalRuleTemplateWithRepository(templateName, repositoryName string) error {
	b.mu.Lock("AssociateApprovalRuleTemplateWithRepository")
	defer b.mu.Unlock()

	if _, ok := b.approvalRuleTemplates[templateName]; !ok {
		return fmt.Errorf("%w: approval rule template %s not found", ErrApprovalRuleTemplateNotFound, templateName)
	}

	if _, ok := b.repositories[repositoryName]; !ok {
		return fmt.Errorf("%w: repository %s not found", ErrNotFound, repositoryName)
	}

	if b.repoTemplateAssoc[repositoryName] == nil {
		b.repoTemplateAssoc[repositoryName] = make(map[string]struct{})
	}
	b.repoTemplateAssoc[repositoryName][templateName] = struct{}{}

	return nil
}

// DisassociateApprovalRuleTemplateFromRepository removes an approval rule template association from a repository.
func (b *InMemoryBackend) DisassociateApprovalRuleTemplateFromRepository(templateName, repositoryName string) error {
	b.mu.Lock("DisassociateApprovalRuleTemplateFromRepository")
	defer b.mu.Unlock()

	if _, ok := b.approvalRuleTemplates[templateName]; !ok {
		return fmt.Errorf("%w: approval rule template %s not found", ErrApprovalRuleTemplateNotFound, templateName)
	}

	if _, ok := b.repositories[repositoryName]; !ok {
		return fmt.Errorf("%w: repository %s not found", ErrNotFound, repositoryName)
	}

	if assoc, ok := b.repoTemplateAssoc[repositoryName]; ok {
		delete(assoc, templateName)
	}

	return nil
}

// BatchAssociateApprovalRuleTemplateWithRepositories associates an approval rule template with multiple repositories.
// Returns lists of associated and failed repository names.
func (b *InMemoryBackend) BatchAssociateApprovalRuleTemplateWithRepositories(
	templateName string,
	repositoryNames []string,
) ([]string, []BatchAssociationError) {
	b.mu.Lock("BatchAssociateApprovalRuleTemplateWithRepositories")
	defer b.mu.Unlock()

	var associated []string
	var errors []BatchAssociationError

	if _, ok := b.approvalRuleTemplates[templateName]; !ok {
		for _, name := range repositoryNames {
			errors = append(errors, BatchAssociationError{
				RepositoryName: name,
				ErrorCode:      errApprovalRuleTemplateNotExist,
				ErrorMessage:   fmt.Sprintf("approval rule template %s not found", templateName),
			})
		}

		return associated, errors
	}

	for _, name := range repositoryNames {
		if _, ok := b.repositories[name]; !ok {
			errors = append(errors, BatchAssociationError{
				RepositoryName: name,
				ErrorCode:      errRepoDoesNotExist,
				ErrorMessage:   fmt.Sprintf("repository %s not found", name),
			})

			continue
		}

		if b.repoTemplateAssoc[name] == nil {
			b.repoTemplateAssoc[name] = make(map[string]struct{})
		}
		b.repoTemplateAssoc[name][templateName] = struct{}{}
		associated = append(associated, name)
	}

	return associated, errors
}

// BatchDisassociateApprovalRuleTemplateFromRepositories removes associations between
// a template and multiple repositories.
func (b *InMemoryBackend) BatchDisassociateApprovalRuleTemplateFromRepositories(
	templateName string,
	repositoryNames []string,
) ([]string, []BatchAssociationError) {
	b.mu.Lock("BatchDisassociateApprovalRuleTemplateFromRepositories")
	defer b.mu.Unlock()

	var disassociated []string
	var errors []BatchAssociationError

	if _, ok := b.approvalRuleTemplates[templateName]; !ok {
		for _, name := range repositoryNames {
			errors = append(errors, BatchAssociationError{
				RepositoryName: name,
				ErrorCode:      errApprovalRuleTemplateNotExist,
				ErrorMessage:   fmt.Sprintf("approval rule template %s not found", templateName),
			})
		}

		return disassociated, errors
	}

	for _, name := range repositoryNames {
		if _, ok := b.repositories[name]; !ok {
			errors = append(errors, BatchAssociationError{
				RepositoryName: name,
				ErrorCode:      errRepoDoesNotExist,
				ErrorMessage:   fmt.Sprintf("repository %s not found", name),
			})

			continue
		}

		if assoc, ok := b.repoTemplateAssoc[name]; ok {
			delete(assoc, templateName)
		}
		disassociated = append(disassociated, name)
	}

	return disassociated, errors
}

// BatchAssociationError holds the error info for a single failed batch association.
type BatchAssociationError struct {
	RepositoryName string `json:"repositoryName"`
	ErrorCode      string `json:"errorCode"`
	ErrorMessage   string `json:"errorMessage"`
}

// CreateBranch creates a new branch in a repository.
func (b *InMemoryBackend) CreateBranch(repositoryName, branchName, commitID string) error {
	b.mu.Lock("CreateBranch")
	defer b.mu.Unlock()

	if _, ok := b.repositories[repositoryName]; !ok {
		return fmt.Errorf("%w: repository %s not found", ErrNotFound, repositoryName)
	}

	// Validate that the commitID exists in the repository.
	if repoCommits := b.commits[repositoryName]; repoCommits != nil {
		if _, ok := repoCommits[commitID]; !ok {
			return fmt.Errorf("%w: commit %s not found in repository %s", ErrCommitNotFound, commitID, repositoryName)
		}
	} else {
		return fmt.Errorf("%w: commit %s not found in repository %s", ErrCommitNotFound, commitID, repositoryName)
	}

	if b.branches[repositoryName] == nil {
		b.branches[repositoryName] = make(map[string]*Branch)
	}

	if _, ok := b.branches[repositoryName][branchName]; ok {
		return fmt.Errorf("%w: branch %s already exists", ErrBranchAlreadyExists, branchName)
	}

	b.branches[repositoryName][branchName] = &Branch{
		BranchName:     branchName,
		CommitID:       commitID,
		RepositoryName: repositoryName,
	}

	return nil
}

// CreateCommit creates a new commit in a repository, tracking parent commits from the
// current branch head.
func (b *InMemoryBackend) CreateCommit(
	repositoryName, branchName, authorName, authorEmail, message string,
) (*Commit, error) {
	b.mu.Lock("CreateCommit")
	defer b.mu.Unlock()

	if _, ok := b.repositories[repositoryName]; !ok {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repositoryName)
	}

	commitID := uuid.NewString()
	treeID := uuid.NewString()

	// Track parent commit: if the branch already has a head commit, record it as parent.
	var parents []string
	if branchName != "" {
		if repoBranches := b.branches[repositoryName]; repoBranches != nil {
			if existing, ok := repoBranches[branchName]; ok {
				parents = []string{existing.CommitID}
			}
		}
	}

	commit := &Commit{
		CommitID:       commitID,
		TreeID:         treeID,
		Message:        message,
		AuthorName:     authorName,
		AuthorEmail:    authorEmail,
		CommitterName:  authorName,
		CommitterEmail: authorEmail,
		RepositoryName: repositoryName,
		Parents:        parents,
	}

	if b.commits[repositoryName] == nil {
		b.commits[repositoryName] = make(map[string]*Commit)
	}
	b.commits[repositoryName][commitID] = commit

	// Update the branch tip to the new commit.
	if branchName != "" {
		if b.branches[repositoryName] == nil {
			b.branches[repositoryName] = make(map[string]*Branch)
		}
		b.branches[repositoryName][branchName] = &Branch{
			BranchName:     branchName,
			CommitID:       commitID,
			RepositoryName: repositoryName,
		}
	}

	cp := *commit
	if len(parents) > 0 {
		cp.Parents = make([]string, len(parents))
		copy(cp.Parents, parents)
	}

	return &cp, nil
}

// BatchGetCommits retrieves multiple commits by ID from a repository.
// Returns a 404 error if the repository does not exist.
func (b *InMemoryBackend) BatchGetCommits(
	repositoryName string,
	commitIDs []string,
) ([]*Commit, []BatchCommitError, error) {
	b.mu.RLock("BatchGetCommits")
	defer b.mu.RUnlock()

	if _, ok := b.repositories[repositoryName]; !ok {
		return nil, nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repositoryName)
	}

	found := make([]*Commit, 0, len(commitIDs))
	errors := make([]BatchCommitError, 0, len(commitIDs))

	repoCommits := b.commits[repositoryName]

	for _, id := range commitIDs {
		c, ok := repoCommits[id]
		if !ok {
			errors = append(errors, BatchCommitError{
				CommitID:     id,
				ErrorCode:    "CommitDoesNotExistException",
				ErrorMessage: fmt.Sprintf("commit %s not found", id),
			})

			continue
		}

		cp := *c
		if len(c.Parents) > 0 {
			cp.Parents = make([]string, len(c.Parents))
			copy(cp.Parents, c.Parents)
		}
		found = append(found, &cp)
	}

	return found, errors, nil
}

// BatchCommitError holds error information for a failed batch commit retrieval.
type BatchCommitError struct {
	CommitID     string `json:"commitId"`
	ErrorCode    string `json:"errorCode"`
	ErrorMessage string `json:"errorMessage"`
}

// BatchDescribeMergeConflictsResult holds the result of a merge conflict description.
type BatchDescribeMergeConflictsResult struct {
	DestinationCommitID string          `json:"destinationCommitId"`
	SourceCommitID      string          `json:"sourceCommitId"`
	BaseCommitID        string          `json:"baseCommitId,omitempty"`
	Conflicts           []MergeConflict `json:"conflicts"`
	Errors              []ConflictError `json:"errors,omitempty"`
}

// MergeConflict represents a single file conflict.
type MergeConflict struct {
	MergeHunks       []MergeHunk      `json:"mergeHunks,omitempty"`
	ConflictMetadata ConflictMetadata `json:"conflictMetadata"`
}

// ConflictMetadata holds metadata about a merge conflict.
type ConflictMetadata struct {
	FilePath          string           `json:"filePath"`
	NumberOfConflicts int              `json:"numberOfConflicts"`
	IsBinaryFile      FileBinaryStatus `json:"isBinaryFile"`
	ContentConflict   bool             `json:"contentConflict"`
}

// FileBinaryStatus holds whether each version of a file is binary.
type FileBinaryStatus struct {
	Source      bool `json:"source"`
	Destination bool `json:"destination"`
	Base        bool `json:"base"`
}

// MergeHunk represents a merge hunk.
type MergeHunk struct {
	Source      *MergeHunkDetail `json:"source,omitempty"`
	Destination *MergeHunkDetail `json:"destination,omitempty"`
	Base        *MergeHunkDetail `json:"base,omitempty"`
	IsConflict  bool             `json:"isConflict"`
}

// MergeHunkDetail represents details about a merge hunk.
type MergeHunkDetail struct {
	HunkContent string `json:"hunkContent"`
	StartLine   int    `json:"startLine"`
	EndLine     int    `json:"endLine"`
}

// ConflictError represents an error encountered while describing a conflict.
type ConflictError struct {
	FilePath     string `json:"filePath"`
	ErrorCode    string `json:"errorCode"`
	ErrorMessage string `json:"errorMessage"`
}

// BatchDescribeMergeConflicts describes merge conflicts between two commits.
// This is a stub implementation — it returns empty conflicts since the backend
// does not track file-level content.
func (b *InMemoryBackend) BatchDescribeMergeConflicts(
	repositoryName, destinationCommitSpecifier, sourceCommitSpecifier, _ string,
	filePaths []string,
) (*BatchDescribeMergeConflictsResult, error) {
	b.mu.RLock("BatchDescribeMergeConflicts")
	defer b.mu.RUnlock()

	if _, ok := b.repositories[repositoryName]; !ok {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repositoryName)
	}

	result := &BatchDescribeMergeConflictsResult{
		DestinationCommitID: destinationCommitSpecifier,
		SourceCommitID:      sourceCommitSpecifier,
		Conflicts:           []MergeConflict{},
	}

	if len(filePaths) > 0 {
		result.Conflicts = make([]MergeConflict, 0, len(filePaths))
		for _, fp := range filePaths {
			result.Conflicts = append(result.Conflicts, MergeConflict{
				ConflictMetadata: ConflictMetadata{
					FilePath:          fp,
					NumberOfConflicts: 0,
					ContentConflict:   false,
				},
				MergeHunks: []MergeHunk{},
			})
		}
	}

	return result, nil
}

// CreatePullRequest creates a new pull request.
func (b *InMemoryBackend) CreatePullRequest(
	title, description, clientRequestToken string,
	targets []PullRequestTarget,
) (*PullRequest, error) {
	b.mu.Lock("CreatePullRequest")
	defer b.mu.Unlock()

	b.nextPRCounter++
	prID := strconv.Itoa(b.nextPRCounter)
	now := time.Now().UTC()

	pr := &PullRequest{
		PullRequestID:      prID,
		Title:              title,
		Description:        description,
		PullRequestStatus:  prStatusOpen,
		CreationDate:       now,
		LastActivityDate:   now,
		ClientRequestToken: clientRequestToken,
		PullRequestTargets: targets,
		RevisionID:         uuid.NewString(),
	}
	b.pullRequests[prID] = pr
	cp := *pr

	// deep copy targets slice
	cp.PullRequestTargets = make([]PullRequestTarget, len(targets))
	copy(cp.PullRequestTargets, targets)

	return &cp, nil
}

// GetBranch returns a branch by repository and branch name.
func (b *InMemoryBackend) GetBranch(repositoryName, branchName string) (*Branch, error) {
	b.mu.RLock("GetBranch")
	defer b.mu.RUnlock()

	if _, ok := b.repositories[repositoryName]; !ok {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repositoryName)
	}

	br, ok := b.branches[repositoryName][branchName]
	if !ok {
		return nil, fmt.Errorf("%w: branch %s not found", ErrBranchNotFound, branchName)
	}

	cp := *br

	return &cp, nil
}

// DeleteBranch deletes a branch from a repository.
func (b *InMemoryBackend) DeleteBranch(repositoryName, branchName string) (*Branch, error) {
	b.mu.Lock("DeleteBranch")
	defer b.mu.Unlock()

	if _, ok := b.repositories[repositoryName]; !ok {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repositoryName)
	}

	br, ok := b.branches[repositoryName][branchName]
	if !ok {
		return nil, fmt.Errorf("%w: branch %s not found", ErrBranchNotFound, branchName)
	}

	cp := *br
	delete(b.branches[repositoryName], branchName)

	return &cp, nil
}

// GetCommit returns a commit by repository and commit ID.
func (b *InMemoryBackend) GetCommit(repositoryName, commitID string) (*Commit, error) {
	b.mu.RLock("GetCommit")
	defer b.mu.RUnlock()

	if _, ok := b.repositories[repositoryName]; !ok {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repositoryName)
	}

	c, ok := b.commits[repositoryName][commitID]
	if !ok {
		return nil, fmt.Errorf("%w: commit %s not found", ErrCommitNotFound, commitID)
	}

	cp := *c

	return &cp, nil
}

// ListBranches returns all branch names for a repository in sorted order.
func (b *InMemoryBackend) ListBranches(repositoryName string) ([]string, error) {
	b.mu.RLock("ListBranches")
	defer b.mu.RUnlock()

	if _, ok := b.repositories[repositoryName]; !ok {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repositoryName)
	}

	branches := b.branches[repositoryName]
	names := make([]string, 0, len(branches))
	for name := range branches {
		names = append(names, name)
	}

	sort.Strings(names)

	return names, nil
}

// GetPullRequest returns a pull request by ID.
func (b *InMemoryBackend) GetPullRequest(prID string) (*PullRequest, error) {
	b.mu.RLock("GetPullRequest")
	defer b.mu.RUnlock()

	pr, ok := b.pullRequests[prID]
	if !ok {
		return nil, fmt.Errorf("%w: pull request %s not found", ErrPullRequestNotFound, prID)
	}

	cp := *pr

	return &cp, nil
}

// ListPullRequests returns pull request IDs for a repository, optionally filtered by status.
// IDs are returned in numeric descending order (newest first), matching AWS behaviour.
func (b *InMemoryBackend) ListPullRequests(repositoryName, pullRequestStatus string) ([]string, error) {
	b.mu.RLock("ListPullRequests")
	defer b.mu.RUnlock()

	if _, ok := b.repositories[repositoryName]; !ok {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repositoryName)
	}

	ids := make([]string, 0, len(b.pullRequests))

	for id, pr := range b.pullRequests {
		if pullRequestStatus != "" && pr.PullRequestStatus != pullRequestStatus {
			continue
		}

		for _, t := range pr.PullRequestTargets {
			if t.RepositoryName == repositoryName {
				ids = append(ids, id)

				break
			}
		}
	}

	// Sort numerically descending (newest first) — AWS returns highest IDs first.
	sort.Slice(ids, func(i, j int) bool {
		ni, ei := strconv.Atoi(ids[i])
		nj, ej := strconv.Atoi(ids[j])
		if ei == nil && ej == nil {
			return ni > nj
		}

		return ids[i] > ids[j]
	})

	return ids, nil
}

// --- Seed helpers (for testing) ---

// AddRepositoryInternal seeds a Repository directly into the backend without
// going through normal validation.
func (b *InMemoryBackend) AddRepositoryInternal(r *Repository) {
	b.mu.Lock("AddRepositoryInternal")
	defer b.mu.Unlock()

	b.repositories[r.RepositoryName] = r
	b.repositoriesByARN[r.ARN] = r.RepositoryName
}

// AddApprovalRuleTemplateInternal seeds an ApprovalRuleTemplate directly.
func (b *InMemoryBackend) AddApprovalRuleTemplateInternal(t *ApprovalRuleTemplate) {
	b.mu.Lock("AddApprovalRuleTemplateInternal")
	defer b.mu.Unlock()

	b.approvalRuleTemplates[t.ApprovalRuleTemplateName] = t
}

// AddBranchInternal seeds a Branch directly into the backend.
func (b *InMemoryBackend) AddBranchInternal(repositoryName string, br *Branch) {
	b.mu.Lock("AddBranchInternal")
	defer b.mu.Unlock()

	if b.branches[repositoryName] == nil {
		b.branches[repositoryName] = make(map[string]*Branch)
	}
	b.branches[repositoryName][br.BranchName] = br
}

// AddCommitInternal seeds a Commit directly into the backend.
func (b *InMemoryBackend) AddCommitInternal(repositoryName string, c *Commit) {
	b.mu.Lock("AddCommitInternal")
	defer b.mu.Unlock()

	if b.commits[repositoryName] == nil {
		b.commits[repositoryName] = make(map[string]*Commit)
	}
	b.commits[repositoryName][c.CommitID] = c
}

// AddPullRequestInternal seeds a PullRequest directly into the backend.
func (b *InMemoryBackend) AddPullRequestInternal(pr *PullRequest) {
	b.mu.Lock("AddPullRequestInternal")
	defer b.mu.Unlock()

	b.pullRequests[pr.PullRequestID] = pr
}
