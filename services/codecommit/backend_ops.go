package codecommit

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// --- New models ---

// PullRequestApproval represents a user's approval state for a pull request.
type PullRequestApproval struct {
	UserARN       string `json:"userArn"`
	ApprovalState string `json:"approvalState"`
}

// PullRequestApprovalRule represents an approval rule on a pull request.
type PullRequestApprovalRule struct {
	RuleID              string `json:"approvalRuleId"`
	RuleName            string `json:"approvalRuleName"`
	ApprovalRuleContent string `json:"approvalRuleContent"`
}

// PullRequestEvent represents an event on a pull request.
type PullRequestEvent struct {
	PullRequestEventType string `json:"pullRequestEventType"`
	EventDate            string `json:"eventDate"`
}

// RuleEvaluation represents the evaluation of a pull request approval rule.
type RuleEvaluation struct {
	RuleName  string `json:"approvalRuleName"`
	Satisfied bool   `json:"satisfied"`
}

// Comment represents a CodeCommit comment.
type Comment struct {
	CommentID        string `json:"commentId"`
	Content          string `json:"content"`
	AuthorARN        string `json:"authorArn"`
	CreationDate     string `json:"creationDate"`
	LastModifiedDate string `json:"lastModifiedDate"`
	// InReplyTo links to parent comment for replies
	InReplyTo string `json:"inReplyTo,omitempty"`
	// PRid links comment to a pull request
	PRid string `json:"-"`
	// RepoName + AfterCommitID for commit comments
	RepoName      string `json:"-"`
	AfterCommitID string `json:"-"`
	Deleted       bool   `json:"deleted"`
}

// Reaction represents a reaction emoji by a user on a comment.
type Reaction struct {
	Emoji   string `json:"emoji"`
	UserARN string `json:"userArn"`
}

// File represents a file stored in CodeCommit.
type File struct {
	FilePath        string `json:"filePath"`
	CommitSpecifier string `json:"commitSpecifier"`
	BlobID          string `json:"blobId"`
	FileMode        string `json:"fileMode"`
	FileContent     []byte `json:"fileContent"`
}

// RepositoryTrigger represents a trigger on a repository.
type RepositoryTrigger struct {
	Name           string   `json:"name"`
	DestinationARN string   `json:"destinationArn"`
	Events         []string `json:"events"`
}

// FileDifference represents a file difference between two commits.
type FileDifference struct {
	AfterBlob  string `json:"afterBlob"`
	BeforeBlob string `json:"beforeBlob"`
	ChangeType string `json:"changeType"`
}

// --- Group 1: Repository CRUD edges ---

// UpdateRepositoryDescription sets the description of a repository.
func (b *InMemoryBackend) UpdateRepositoryDescription(name, desc string) error {
	b.mu.Lock("UpdateRepositoryDescription")
	defer b.mu.Unlock()

	r, ok := b.repositories[name]
	if !ok {
		return fmt.Errorf("%w: repository %s not found", ErrNotFound, name)
	}
	r.Description = desc
	r.LastModifiedDate = time.Now().UTC()

	return nil
}

// UpdateRepositoryName renames a repository from oldName to newName.
func (b *InMemoryBackend) UpdateRepositoryName(oldName, newName string) error {
	b.mu.Lock("UpdateRepositoryName")
	defer b.mu.Unlock()

	r, ok := b.repositories[oldName]
	if !ok {
		return fmt.Errorf("%w: repository %s not found", ErrNotFound, oldName)
	}

	if _, exists := b.repositories[newName]; exists {
		return fmt.Errorf("%w: repository %s already exists", ErrAlreadyExists, newName)
	}

	// Update name in struct
	r.RepositoryName = newName
	r.LastModifiedDate = time.Now().UTC()

	// Re-key maps
	delete(b.repositories, oldName)
	b.repositories[newName] = r
	b.repositoriesByARN[r.ARN] = newName

	// Migrate branches, commits, repoTemplateAssoc
	if branches, hasBranches := b.branches[oldName]; hasBranches {
		b.branches[newName] = branches
		delete(b.branches, oldName)
	}
	if commits, hasCommits := b.commits[oldName]; hasCommits {
		b.commits[newName] = commits
		delete(b.commits, oldName)
	}
	if assoc, hasAssoc := b.repoTemplateAssoc[oldName]; hasAssoc {
		b.repoTemplateAssoc[newName] = assoc
		delete(b.repoTemplateAssoc, oldName)
	}
	if files, hasFiles := b.files[oldName]; hasFiles {
		b.files[newName] = files
		delete(b.files, oldName)
	}
	if triggers, hasTriggers := b.triggers[oldName]; hasTriggers {
		b.triggers[newName] = triggers
		delete(b.triggers, oldName)
	}

	return nil
}

// UpdateRepositoryEncryptionKey sets the KMS key ID for a repository.
func (b *InMemoryBackend) UpdateRepositoryEncryptionKey(name, kmsKeyID string) error {
	b.mu.Lock("UpdateRepositoryEncryptionKey")
	defer b.mu.Unlock()

	r, ok := b.repositories[name]
	if !ok {
		return fmt.Errorf("%w: repository %s not found", ErrNotFound, name)
	}
	r.KmsKeyID = kmsKeyID
	r.LastModifiedDate = time.Now().UTC()

	return nil
}

// UpdateDefaultBranch sets the default branch for a repository.
// AWS requires the branch to exist in the repository.
func (b *InMemoryBackend) UpdateDefaultBranch(repoName, branchName string) error {
	b.mu.Lock("UpdateDefaultBranch")
	defer b.mu.Unlock()

	r, ok := b.repositories[repoName]
	if !ok {
		return fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}
	// Validate the branch exists.
	if repoBranches := b.branches[repoName]; repoBranches != nil {
		if _, found := repoBranches[branchName]; !found {
			return fmt.Errorf("%w: branch %s not found in repository %s", ErrBranchNotFound, branchName, repoName)
		}
	} else if branchName != "" {
		return fmt.Errorf(
			"%w: branch %s not found in repository %s (no branches exist)",
			ErrBranchNotFound, branchName, repoName,
		)
	}
	r.DefaultBranch = branchName
	r.LastModifiedDate = time.Now().UTC()

	return nil
}

// --- Group 2: Approval Rule Template CRUD ---

// DeleteApprovalRuleTemplate deletes an approval rule template by name.
func (b *InMemoryBackend) DeleteApprovalRuleTemplate(name string) error {
	b.mu.Lock("DeleteApprovalRuleTemplate")
	defer b.mu.Unlock()

	if _, ok := b.approvalRuleTemplates[name]; !ok {
		return fmt.Errorf("%w: approval rule template %s not found", ErrApprovalRuleTemplateNotFound, name)
	}
	delete(b.approvalRuleTemplates, name)

	return nil
}

// GetApprovalRuleTemplate retrieves an approval rule template by name.
func (b *InMemoryBackend) GetApprovalRuleTemplate(name string) (*ApprovalRuleTemplate, error) {
	b.mu.RLock("GetApprovalRuleTemplate")
	defer b.mu.RUnlock()

	t, ok := b.approvalRuleTemplates[name]
	if !ok {
		return nil, fmt.Errorf("%w: approval rule template %s not found", ErrApprovalRuleTemplateNotFound, name)
	}
	cp := *t

	return &cp, nil
}

// ListApprovalRuleTemplates returns all approval rule templates.
func (b *InMemoryBackend) ListApprovalRuleTemplates() []*ApprovalRuleTemplate {
	b.mu.RLock("ListApprovalRuleTemplates")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.approvalRuleTemplates))
	for n := range b.approvalRuleTemplates {
		names = append(names, n)
	}
	sort.Strings(names)

	list := make([]*ApprovalRuleTemplate, 0, len(names))
	for _, n := range names {
		cp := *b.approvalRuleTemplates[n]
		list = append(list, &cp)
	}

	return list
}

// UpdateApprovalRuleTemplateContent updates the content of an approval rule template.
func (b *InMemoryBackend) UpdateApprovalRuleTemplateContent(name, content string) error {
	b.mu.Lock("UpdateApprovalRuleTemplateContent")
	defer b.mu.Unlock()

	t, ok := b.approvalRuleTemplates[name]
	if !ok {
		return fmt.Errorf("%w: approval rule template %s not found", ErrApprovalRuleTemplateNotFound, name)
	}
	t.ApprovalRuleTemplateContent = content
	t.LastModifiedDate = time.Now().UTC()

	return nil
}

// UpdateApprovalRuleTemplateDescription updates the description of an approval rule template.
func (b *InMemoryBackend) UpdateApprovalRuleTemplateDescription(name, desc string) error {
	b.mu.Lock("UpdateApprovalRuleTemplateDescription")
	defer b.mu.Unlock()

	t, ok := b.approvalRuleTemplates[name]
	if !ok {
		return fmt.Errorf("%w: approval rule template %s not found", ErrApprovalRuleTemplateNotFound, name)
	}
	t.ApprovalRuleTemplateDescription = desc
	t.LastModifiedDate = time.Now().UTC()

	return nil
}

// UpdateApprovalRuleTemplateName renames an approval rule template.
func (b *InMemoryBackend) UpdateApprovalRuleTemplateName(oldName, newName string) error {
	b.mu.Lock("UpdateApprovalRuleTemplateName")
	defer b.mu.Unlock()

	t, ok := b.approvalRuleTemplates[oldName]
	if !ok {
		return fmt.Errorf("%w: approval rule template %s not found", ErrApprovalRuleTemplateNotFound, oldName)
	}
	if _, exists := b.approvalRuleTemplates[newName]; exists {
		return fmt.Errorf("%w: approval rule template %s already exists", ErrApprovalRuleTemplateAlreadyExists, newName)
	}
	t.ApprovalRuleTemplateName = newName
	t.LastModifiedDate = time.Now().UTC()
	delete(b.approvalRuleTemplates, oldName)
	b.approvalRuleTemplates[newName] = t

	// Update repoTemplateAssoc
	for _, assoc := range b.repoTemplateAssoc {
		if _, hasOld := assoc[oldName]; hasOld {
			delete(assoc, oldName)
			assoc[newName] = struct{}{}
		}
	}

	return nil
}

// --- Group 3: Template-Repository association edges ---

// ListAssociatedApprovalRuleTemplatesForRepository returns template names associated with a repository.
func (b *InMemoryBackend) ListAssociatedApprovalRuleTemplatesForRepository(repoName string) ([]string, error) {
	b.mu.RLock("ListAssociatedApprovalRuleTemplatesForRepository")
	defer b.mu.RUnlock()

	if _, ok := b.repositories[repoName]; !ok {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	assoc := b.repoTemplateAssoc[repoName]
	names := make([]string, 0, len(assoc))
	for n := range assoc {
		names = append(names, n)
	}
	sort.Strings(names)

	return names, nil
}

// ListRepositoriesForApprovalRuleTemplate returns repository names that have a given template associated.
func (b *InMemoryBackend) ListRepositoriesForApprovalRuleTemplate(templateName string) ([]string, error) {
	b.mu.RLock("ListRepositoriesForApprovalRuleTemplate")
	defer b.mu.RUnlock()

	if _, ok := b.approvalRuleTemplates[templateName]; !ok {
		return nil, fmt.Errorf(
			"%w: approval rule template %s not found",
			ErrApprovalRuleTemplateNotFound,
			templateName,
		)
	}

	var repos []string
	for repoName, assoc := range b.repoTemplateAssoc {
		if _, ok := assoc[templateName]; ok {
			repos = append(repos, repoName)
		}
	}
	sort.Strings(repos)

	return repos, nil
}

// --- Group 4: PullRequest operations ---

// GetPullRequestApprovalStates returns the approval states for a pull request.
func (b *InMemoryBackend) GetPullRequestApprovalStates(prID string) ([]PullRequestApproval, error) {
	b.mu.RLock("GetPullRequestApprovalStates")
	defer b.mu.RUnlock()

	if _, ok := b.pullRequests[prID]; !ok {
		return nil, fmt.Errorf("%w: pull request %s not found", ErrPullRequestNotFound, prID)
	}

	approvals := b.prApprovals[prID]
	result := make([]PullRequestApproval, 0, len(approvals))
	for userARN, state := range approvals {
		result = append(result, PullRequestApproval{UserARN: userARN, ApprovalState: state})
	}

	return result, nil
}

// GetPullRequestOverrideState returns whether a PR has been overridden and by whom.
func (b *InMemoryBackend) GetPullRequestOverrideState(prID string) (bool, string, error) {
	b.mu.RLock("GetPullRequestOverrideState")
	defer b.mu.RUnlock()

	if _, ok := b.pullRequests[prID]; !ok {
		return false, "", fmt.Errorf("%w: pull request %s not found", ErrPullRequestNotFound, prID)
	}

	overridden := b.prOverrides[prID]
	overrider := b.prOverriders[prID]

	return overridden, overrider, nil
}

// OverridePullRequestApprovalRules sets the override status for a pull request.
func (b *InMemoryBackend) OverridePullRequestApprovalRules(prID, overrideStatus, overriderARN string) error {
	b.mu.Lock("OverridePullRequestApprovalRules")
	defer b.mu.Unlock()

	if _, ok := b.pullRequests[prID]; !ok {
		return fmt.Errorf("%w: pull request %s not found", ErrPullRequestNotFound, prID)
	}

	b.prOverrides[prID] = overrideStatus == "OVERRIDE"
	b.prOverriders[prID] = overriderARN
	b.prEvents[prID] = append(b.prEvents[prID], PullRequestEvent{
		PullRequestEventType: "PULL_REQUEST_APPROVAL_RULE_OVERRIDDEN",
		EventDate:            time.Now().UTC().Format(time.RFC3339),
	})

	return nil
}

// UpdatePullRequestApprovalState sets the approval state for a user on a pull request.
// AWS rejects this operation on closed or merged pull requests.
func (b *InMemoryBackend) UpdatePullRequestApprovalState(prID, userARN, approvalState string) error {
	b.mu.Lock("UpdatePullRequestApprovalState")
	defer b.mu.Unlock()

	pr, ok := b.pullRequests[prID]
	if !ok {
		return fmt.Errorf("%w: pull request %s not found", ErrPullRequestNotFound, prID)
	}
	if pr.PullRequestStatus == prStatusMerged || pr.PullRequestStatus == prStatusClosed {
		return fmt.Errorf("%w: pull request %s is already closed", ErrPullRequestAlreadyMerged, prID)
	}

	if b.prApprovals[prID] == nil {
		b.prApprovals[prID] = make(map[string]string)
	}
	b.prApprovals[prID][userARN] = approvalState

	return nil
}

// UpdatePullRequestDescription updates the description of a pull request.
// AWS rejects this operation on closed or merged pull requests.
func (b *InMemoryBackend) UpdatePullRequestDescription(prID, desc string) error {
	b.mu.Lock("UpdatePullRequestDescription")
	defer b.mu.Unlock()

	pr, ok := b.pullRequests[prID]
	if !ok {
		return fmt.Errorf("%w: pull request %s not found", ErrPullRequestNotFound, prID)
	}
	if pr.PullRequestStatus == prStatusMerged || pr.PullRequestStatus == prStatusClosed {
		return fmt.Errorf("%w: pull request %s is already closed", ErrPullRequestAlreadyMerged, prID)
	}
	pr.Description = desc
	pr.LastActivityDate = time.Now().UTC()

	return nil
}

// UpdatePullRequestStatus updates the status of a pull request.
func (b *InMemoryBackend) UpdatePullRequestStatus(prID, status string) error {
	b.mu.Lock("UpdatePullRequestStatus")
	defer b.mu.Unlock()

	pr, ok := b.pullRequests[prID]
	if !ok {
		return fmt.Errorf("%w: pull request %s not found", ErrPullRequestNotFound, prID)
	}
	pr.PullRequestStatus = status
	pr.LastActivityDate = time.Now().UTC()

	return nil
}

// UpdatePullRequestTitle updates the title of a pull request.
// AWS rejects this operation on closed or merged pull requests.
func (b *InMemoryBackend) UpdatePullRequestTitle(prID, title string) error {
	b.mu.Lock("UpdatePullRequestTitle")
	defer b.mu.Unlock()

	pr, ok := b.pullRequests[prID]
	if !ok {
		return fmt.Errorf("%w: pull request %s not found", ErrPullRequestNotFound, prID)
	}
	if pr.PullRequestStatus == prStatusMerged || pr.PullRequestStatus == prStatusClosed {
		return fmt.Errorf("%w: pull request %s is already closed", ErrPullRequestAlreadyMerged, prID)
	}
	pr.Title = title
	pr.LastActivityDate = time.Now().UTC()

	return nil
}

// CreatePullRequestApprovalRule creates an approval rule on a pull request.
func (b *InMemoryBackend) CreatePullRequestApprovalRule(
	prID, ruleName, content string,
) (*PullRequestApprovalRule, error) {
	b.mu.Lock("CreatePullRequestApprovalRule")
	defer b.mu.Unlock()

	if _, ok := b.pullRequests[prID]; !ok {
		return nil, fmt.Errorf("%w: pull request %s not found", ErrPullRequestNotFound, prID)
	}

	if b.prApprovalRules[prID] == nil {
		b.prApprovalRules[prID] = make(map[string]*PullRequestApprovalRule)
	}

	rule := &PullRequestApprovalRule{
		RuleID:              uuid.NewString(),
		RuleName:            ruleName,
		ApprovalRuleContent: content,
	}
	b.prApprovalRules[prID][ruleName] = rule
	cp := *rule

	return &cp, nil
}

// DeletePullRequestApprovalRule deletes an approval rule from a pull request.
func (b *InMemoryBackend) DeletePullRequestApprovalRule(prID, ruleName string) error {
	b.mu.Lock("DeletePullRequestApprovalRule")
	defer b.mu.Unlock()

	if _, ok := b.pullRequests[prID]; !ok {
		return fmt.Errorf("%w: pull request %s not found", ErrPullRequestNotFound, prID)
	}

	if _, ok := b.prApprovalRules[prID][ruleName]; !ok {
		return fmt.Errorf("%w: approval rule %s not found on pull request %s", ErrNotFound, ruleName, prID)
	}
	delete(b.prApprovalRules[prID], ruleName)

	return nil
}

// UpdatePullRequestApprovalRuleContent updates the content of an approval rule on a pull request.
func (b *InMemoryBackend) UpdatePullRequestApprovalRuleContent(prID, ruleName, content string) error {
	b.mu.Lock("UpdatePullRequestApprovalRuleContent")
	defer b.mu.Unlock()

	if _, ok := b.pullRequests[prID]; !ok {
		return fmt.Errorf("%w: pull request %s not found", ErrPullRequestNotFound, prID)
	}

	rule, ok := b.prApprovalRules[prID][ruleName]
	if !ok {
		return fmt.Errorf("%w: approval rule %s not found on pull request %s", ErrNotFound, ruleName, prID)
	}
	rule.ApprovalRuleContent = content

	return nil
}

// DescribePullRequestEvents returns events for a pull request.
func (b *InMemoryBackend) DescribePullRequestEvents(prID string) ([]PullRequestEvent, error) {
	b.mu.RLock("DescribePullRequestEvents")
	defer b.mu.RUnlock()

	if _, ok := b.pullRequests[prID]; !ok {
		return nil, fmt.Errorf("%w: pull request %s not found", ErrPullRequestNotFound, prID)
	}

	events := b.prEvents[prID]
	if events == nil {
		return []PullRequestEvent{}, nil
	}
	result := make([]PullRequestEvent, len(events))
	copy(result, events)

	return result, nil
}

// EvaluatePullRequestApprovalRules evaluates all approval rules for a pull request.
func (b *InMemoryBackend) EvaluatePullRequestApprovalRules(prID string) ([]RuleEvaluation, error) {
	b.mu.RLock("EvaluatePullRequestApprovalRules")
	defer b.mu.RUnlock()

	if _, ok := b.pullRequests[prID]; !ok {
		return nil, fmt.Errorf("%w: pull request %s not found", ErrPullRequestNotFound, prID)
	}

	rules := b.prApprovalRules[prID]
	result := make([]RuleEvaluation, 0, len(rules))
	for _, rule := range rules {
		result = append(result, RuleEvaluation{RuleName: rule.RuleName, Satisfied: true})
	}

	return result, nil
}

// --- Group 5: Comments ---

func newCommentID() string {
	return uuid.NewString()
}

// PostCommentForComparedCommit creates a comment on a compared commit.
func (b *InMemoryBackend) PostCommentForComparedCommit(repoName, _, afterCommitID, content string) (*Comment, error) {
	b.mu.Lock("PostCommentForComparedCommit")
	defer b.mu.Unlock()

	if _, ok := b.repositories[repoName]; !ok {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	c := &Comment{
		CommentID:        newCommentID(),
		Content:          content,
		CreationDate:     now,
		LastModifiedDate: now,
		RepoName:         repoName,
		AfterCommitID:    afterCommitID,
	}
	b.comments[c.CommentID] = c
	cp := *c

	return &cp, nil
}

// PostCommentForPullRequest creates a comment on a pull request.
func (b *InMemoryBackend) PostCommentForPullRequest(prID, repoName, content string) (*Comment, error) {
	b.mu.Lock("PostCommentForPullRequest")
	defer b.mu.Unlock()

	if _, ok := b.pullRequests[prID]; !ok {
		return nil, fmt.Errorf("%w: pull request %s not found", ErrPullRequestNotFound, prID)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	c := &Comment{
		CommentID:        newCommentID(),
		Content:          content,
		CreationDate:     now,
		LastModifiedDate: now,
		PRid:             prID,
		RepoName:         repoName,
	}
	b.comments[c.CommentID] = c
	cp := *c

	return &cp, nil
}

// PostCommentReply creates a reply to an existing comment.
func (b *InMemoryBackend) PostCommentReply(inReplyTo, content string) (*Comment, error) {
	b.mu.Lock("PostCommentReply")
	defer b.mu.Unlock()

	if _, ok := b.comments[inReplyTo]; !ok {
		return nil, fmt.Errorf("%w: comment %s not found", ErrNotFound, inReplyTo)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	c := &Comment{
		CommentID:        newCommentID(),
		Content:          content,
		CreationDate:     now,
		LastModifiedDate: now,
		InReplyTo:        inReplyTo,
	}
	b.comments[c.CommentID] = c
	cp := *c

	return &cp, nil
}

// GetComment retrieves a comment by ID.
func (b *InMemoryBackend) GetComment(commentID string) (*Comment, error) {
	b.mu.RLock("GetComment")
	defer b.mu.RUnlock()

	c, ok := b.comments[commentID]
	if !ok {
		return nil, fmt.Errorf("%w: comment %s not found", ErrNotFound, commentID)
	}
	cp := *c

	return &cp, nil
}

// GetCommentReactions returns reactions for a comment.
func (b *InMemoryBackend) GetCommentReactions(commentID string) ([]Reaction, error) {
	b.mu.RLock("GetCommentReactions")
	defer b.mu.RUnlock()

	if _, ok := b.comments[commentID]; !ok {
		return nil, fmt.Errorf("%w: comment %s not found", ErrNotFound, commentID)
	}

	reactions := b.commentReactions[commentID]
	result := make([]Reaction, len(reactions))
	copy(result, reactions)

	return result, nil
}

// GetCommentsForComparedCommit returns comments for a compared commit.
func (b *InMemoryBackend) GetCommentsForComparedCommit(repoName, afterCommitID string) ([]*Comment, error) {
	b.mu.RLock("GetCommentsForComparedCommit")
	defer b.mu.RUnlock()

	if _, ok := b.repositories[repoName]; !ok {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	var result []*Comment
	for _, c := range b.comments {
		if c.RepoName == repoName && c.AfterCommitID == afterCommitID {
			cp := *c
			result = append(result, &cp)
		}
	}

	return result, nil
}

// GetCommentsForPullRequest returns comments for a pull request.
func (b *InMemoryBackend) GetCommentsForPullRequest(prID string) ([]*Comment, error) {
	b.mu.RLock("GetCommentsForPullRequest")
	defer b.mu.RUnlock()

	if _, ok := b.pullRequests[prID]; !ok {
		return nil, fmt.Errorf("%w: pull request %s not found", ErrPullRequestNotFound, prID)
	}

	var result []*Comment
	for _, c := range b.comments {
		if c.PRid == prID {
			cp := *c
			result = append(result, &cp)
		}
	}

	return result, nil
}

// PutCommentReaction adds a reaction to a comment.
func (b *InMemoryBackend) PutCommentReaction(commentID, emoji string) error {
	b.mu.Lock("PutCommentReaction")
	defer b.mu.Unlock()

	if _, ok := b.comments[commentID]; !ok {
		return fmt.Errorf("%w: comment %s not found", ErrNotFound, commentID)
	}
	b.commentReactions[commentID] = append(b.commentReactions[commentID], Reaction{Emoji: emoji})

	return nil
}

// UpdateComment updates the content of a comment.
func (b *InMemoryBackend) UpdateComment(commentID, content string) error {
	b.mu.Lock("UpdateComment")
	defer b.mu.Unlock()

	c, ok := b.comments[commentID]
	if !ok {
		return fmt.Errorf("%w: comment %s not found", ErrNotFound, commentID)
	}
	c.Content = content
	c.LastModifiedDate = time.Now().UTC().Format(time.RFC3339)

	return nil
}

// DeleteCommentContent marks a comment as deleted and clears its content.
func (b *InMemoryBackend) DeleteCommentContent(commentID string) error {
	b.mu.Lock("DeleteCommentContent")
	defer b.mu.Unlock()

	c, ok := b.comments[commentID]
	if !ok {
		return fmt.Errorf("%w: comment %s not found", ErrNotFound, commentID)
	}
	c.Deleted = true
	c.Content = ""
	c.LastModifiedDate = time.Now().UTC().Format(time.RFC3339)

	return nil
}

// --- Group 6: File/Blob ops ---

// PutFile stores a file and creates a commit.
func (b *InMemoryBackend) PutFile(repoName, branchName, filePath string, content []byte) (*Commit, error) {
	b.mu.Lock("PutFile")
	defer b.mu.Unlock()

	if _, ok := b.repositories[repoName]; !ok {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	blobID := uuid.NewString()
	if b.files[repoName] == nil {
		b.files[repoName] = make(map[string]*File)
	}
	b.files[repoName][filePath] = &File{
		FilePath:        filePath,
		CommitSpecifier: branchName,
		BlobID:          blobID,
		FileMode:        fileModeDefault,
		FileContent:     content,
	}

	commitID := uuid.NewString()
	treeID := uuid.NewString()
	now := time.Now().UTC()
	commit := &Commit{
		CommitID:       commitID,
		TreeID:         treeID,
		Message:        "Add " + filePath,
		RepositoryName: repoName,
		CreatedAt:      now,
	}
	if b.commits[repoName] == nil {
		b.commits[repoName] = make(map[string]*Commit)
	}
	b.commits[repoName][commitID] = commit

	// Update branch tip
	if branchName != "" {
		if b.branches[repoName] == nil {
			b.branches[repoName] = make(map[string]*Branch)
		}
		b.branches[repoName][branchName] = &Branch{
			BranchName:     branchName,
			CommitID:       commitID,
			RepositoryName: repoName,
		}
	}

	cp := *commit

	return &cp, nil
}

// GetFile retrieves a file by repository, commit specifier, and path.
func (b *InMemoryBackend) GetFile(repoName, _ /* commitSpecifier */, filePath string) (*File, error) {
	b.mu.RLock("GetFile")
	defer b.mu.RUnlock()

	if _, ok := b.repositories[repoName]; !ok {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	repoFiles := b.files[repoName]
	if repoFiles == nil {
		return nil, fmt.Errorf("%w: file %s not found", ErrNotFound, filePath)
	}

	f, ok := repoFiles[filePath]
	if !ok {
		return nil, fmt.Errorf("%w: file %s not found", ErrNotFound, filePath)
	}
	cp := *f

	return &cp, nil
}

// GetFolder lists file paths under a folder path.
func (b *InMemoryBackend) GetFolder(repoName, _ /* commitSpecifier */, folderPath string) ([]string, error) {
	b.mu.RLock("GetFolder")
	defer b.mu.RUnlock()

	if _, ok := b.repositories[repoName]; !ok {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	repoFiles := b.files[repoName]
	var paths []string
	prefix := folderPath
	if prefix != "" && prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}
	for fp := range repoFiles {
		if prefix == "" || fp == folderPath || len(fp) > len(prefix) && fp[:len(prefix)] == prefix {
			paths = append(paths, fp)
		}
	}
	sort.Strings(paths)

	return paths, nil
}

// GetFolderFiles returns file metadata (path, blobId, fileMode) for files under a folder path.
// This provides richer info than GetFolder for handler responses matching the AWS API shape.
func (b *InMemoryBackend) GetFolderFiles(repoName, _ /* commitSpecifier */, folderPath string) ([]*File, error) {
	b.mu.RLock("GetFolderFiles")
	defer b.mu.RUnlock()

	if _, ok := b.repositories[repoName]; !ok {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	repoFiles := b.files[repoName]
	var files []*File
	prefix := folderPath
	if prefix != "" && prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}
	for fp, f := range repoFiles {
		if prefix == "" || fp == folderPath || len(fp) > len(prefix) && fp[:len(prefix)] == prefix {
			cp := *f
			files = append(files, &cp)
		}
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].FilePath < files[j].FilePath
	})

	return files, nil
}

// DeleteFile removes a file and creates a delete commit.
func (b *InMemoryBackend) DeleteFile(repoName, branchName, filePath, _ /* parentCommitID */ string) (*Commit, error) {
	b.mu.Lock("DeleteFile")
	defer b.mu.Unlock()

	if _, ok := b.repositories[repoName]; !ok {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	if b.files[repoName] != nil {
		delete(b.files[repoName], filePath)
	}

	commitID := uuid.NewString()
	treeID := uuid.NewString()
	now := time.Now().UTC()
	commit := &Commit{
		CommitID:       commitID,
		TreeID:         treeID,
		Message:        "Delete " + filePath,
		RepositoryName: repoName,
		CreatedAt:      now,
	}
	if b.commits[repoName] == nil {
		b.commits[repoName] = make(map[string]*Commit)
	}
	b.commits[repoName][commitID] = commit

	// Update branch tip
	if branchName != "" {
		if b.branches[repoName] == nil {
			b.branches[repoName] = make(map[string]*Branch)
		}
		b.branches[repoName][branchName] = &Branch{
			BranchName:     branchName,
			CommitID:       commitID,
			RepositoryName: repoName,
		}
	}

	cp := *commit

	return &cp, nil
}

// --- Group 7: Triggers ---

// GetRepositoryTriggers returns triggers for a repository.
func (b *InMemoryBackend) GetRepositoryTriggers(repoName string) ([]RepositoryTrigger, error) {
	b.mu.RLock("GetRepositoryTriggers")
	defer b.mu.RUnlock()

	if _, ok := b.repositories[repoName]; !ok {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	triggers := b.triggers[repoName]
	result := make([]RepositoryTrigger, len(triggers))
	copy(result, triggers)

	return result, nil
}

// PutRepositoryTriggers replaces triggers for a repository.
func (b *InMemoryBackend) PutRepositoryTriggers(repoName string, triggers []RepositoryTrigger) error {
	b.mu.Lock("PutRepositoryTriggers")
	defer b.mu.Unlock()

	if _, ok := b.repositories[repoName]; !ok {
		return fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	b.triggers[repoName] = make([]RepositoryTrigger, len(triggers))
	copy(b.triggers[repoName], triggers)

	return nil
}

// TestRepositoryTriggers returns the names of triggers that succeeded.
func (b *InMemoryBackend) TestRepositoryTriggers(repoName string) ([]string, error) {
	b.mu.RLock("TestRepositoryTriggers")
	defer b.mu.RUnlock()

	if _, ok := b.repositories[repoName]; !ok {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	triggers := b.triggers[repoName]
	names := make([]string, 0, len(triggers))
	for _, t := range triggers {
		names = append(names, t.Name)
	}

	return names, nil
}

// --- Group 8: Merge ops ---

// MergePullRequestByFastForward merges a pull request by fast-forward strategy.
func (b *InMemoryBackend) MergePullRequestByFastForward(
	prID, _ /* repoName */, _ /* sourceRef */ string,
) (*PullRequest, error) {
	b.mu.Lock("MergePullRequestByFastForward")
	defer b.mu.Unlock()

	pr, ok := b.pullRequests[prID]
	if !ok {
		return nil, fmt.Errorf("%w: pull request %s not found", ErrPullRequestNotFound, prID)
	}
	if pr.PullRequestStatus == prStatusMerged || pr.PullRequestStatus == prStatusClosed {
		return nil, fmt.Errorf("%w: pull request %s is already closed", ErrPullRequestAlreadyMerged, prID)
	}
	pr.PullRequestStatus = prStatusMerged
	pr.LastActivityDate = time.Now().UTC()
	cp := *pr

	return &cp, nil
}

// MergePullRequestBySquash merges a pull request by squash strategy.
func (b *InMemoryBackend) MergePullRequestBySquash(
	prID, _ /* repoName */, _ /* sourceRef */ string,
) (*PullRequest, error) {
	b.mu.Lock("MergePullRequestBySquash")
	defer b.mu.Unlock()

	pr, ok := b.pullRequests[prID]
	if !ok {
		return nil, fmt.Errorf("%w: pull request %s not found", ErrPullRequestNotFound, prID)
	}
	if pr.PullRequestStatus == prStatusMerged || pr.PullRequestStatus == prStatusClosed {
		return nil, fmt.Errorf("%w: pull request %s is already closed", ErrPullRequestAlreadyMerged, prID)
	}
	pr.PullRequestStatus = prStatusMerged
	pr.LastActivityDate = time.Now().UTC()
	cp := *pr

	return &cp, nil
}

// MergePullRequestByThreeWay merges a pull request by three-way strategy.
func (b *InMemoryBackend) MergePullRequestByThreeWay(
	prID, _ /* repoName */, _ /* sourceRef */ string,
) (*PullRequest, error) {
	b.mu.Lock("MergePullRequestByThreeWay")
	defer b.mu.Unlock()

	pr, ok := b.pullRequests[prID]
	if !ok {
		return nil, fmt.Errorf("%w: pull request %s not found", ErrPullRequestNotFound, prID)
	}
	if pr.PullRequestStatus == prStatusMerged || pr.PullRequestStatus == prStatusClosed {
		return nil, fmt.Errorf("%w: pull request %s is already closed", ErrPullRequestAlreadyMerged, prID)
	}
	pr.PullRequestStatus = prStatusMerged
	pr.LastActivityDate = time.Now().UTC()
	cp := *pr

	return &cp, nil
}

// MergeBranchesByFastForward merges branches by fast-forward and creates a merge commit.
func (b *InMemoryBackend) MergeBranchesByFastForward(repoName, sourceRef, destinationRef string) (*Commit, error) {
	b.mu.Lock("MergeBranchesByFastForward")
	defer b.mu.Unlock()

	if _, ok := b.repositories[repoName]; !ok {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	commitID := uuid.NewString()
	treeID := uuid.NewString()
	now := time.Now().UTC()
	commit := &Commit{
		CommitID:       commitID,
		TreeID:         treeID,
		Message:        fmt.Sprintf("Merge %s into %s", sourceRef, destinationRef),
		RepositoryName: repoName,
		CreatedAt:      now,
	}
	if b.commits[repoName] == nil {
		b.commits[repoName] = make(map[string]*Commit)
	}
	b.commits[repoName][commitID] = commit

	// Update destination branch tip
	if b.branches[repoName] == nil {
		b.branches[repoName] = make(map[string]*Branch)
	}
	b.branches[repoName][destinationRef] = &Branch{
		BranchName:     destinationRef,
		CommitID:       commitID,
		RepositoryName: repoName,
	}

	cp := *commit

	return &cp, nil
}

// GetMergeOptions returns the available merge options for two branches.
func (b *InMemoryBackend) GetMergeOptions(
	repoName, _ /* sourceRef */, _ /* destinationRef */ string,
) ([]string, error) {
	b.mu.RLock("GetMergeOptions")
	defer b.mu.RUnlock()

	if _, ok := b.repositories[repoName]; !ok {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	return []string{"FAST_FORWARD_MERGE", "SQUASH_MERGE", "THREE_WAY_MERGE"}, nil
}

// --- Group 9: Misc ---

// GetBlob returns the content of a blob by blobID.
func (b *InMemoryBackend) GetBlob(repoName, blobID string) ([]byte, error) {
	b.mu.RLock("GetBlob")
	defer b.mu.RUnlock()

	if _, ok := b.repositories[repoName]; !ok {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	repoFiles := b.files[repoName]
	for _, f := range repoFiles {
		if f.BlobID == blobID {
			result := make([]byte, len(f.FileContent))
			copy(result, f.FileContent)

			return result, nil
		}
	}

	return nil, fmt.Errorf("%w: blob %s not found", ErrNotFound, blobID)
}

// ListFileCommitHistory returns all commits for a repository (simplified).
func (b *InMemoryBackend) ListFileCommitHistory(repoName, _ /* filePath */ string) ([]*Commit, error) {
	b.mu.RLock("ListFileCommitHistory")
	defer b.mu.RUnlock()

	if _, ok := b.repositories[repoName]; !ok {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	repoCommits := b.commits[repoName]
	result := make([]*Commit, 0, len(repoCommits))
	for _, c := range repoCommits {
		cp := *c
		result = append(result, &cp)
	}

	return result, nil
}

// CreateUnreferencedMergeCommit creates a new unreferenced merge commit.
func (b *InMemoryBackend) CreateUnreferencedMergeCommit(
	repoName, sourceCommitID, destinationCommitID string,
) (*Commit, error) {
	b.mu.Lock("CreateUnreferencedMergeCommit")
	defer b.mu.Unlock()

	if _, ok := b.repositories[repoName]; !ok {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	commitID := uuid.NewString()
	treeID := uuid.NewString()
	now := time.Now().UTC()
	commit := &Commit{
		CommitID:       commitID,
		TreeID:         treeID,
		Message:        "Unreferenced merge commit",
		RepositoryName: repoName,
		Parents:        []string{sourceCommitID, destinationCommitID},
		CreatedAt:      now,
	}
	if b.commits[repoName] == nil {
		b.commits[repoName] = make(map[string]*Commit)
	}
	b.commits[repoName][commitID] = commit
	cp := *commit
	cp.Parents = make([]string, len(commit.Parents))
	copy(cp.Parents, commit.Parents)

	return &cp, nil
}

// GetMergeCommit returns the first commit in a repository or an error.
func (b *InMemoryBackend) GetMergeCommit(
	repoName, _ /* sourceCommitSpecifier */, _ /* destinationCommitSpecifier */ string,
) (*Commit, error) {
	b.mu.RLock("GetMergeCommit")
	defer b.mu.RUnlock()

	if _, ok := b.repositories[repoName]; !ok {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	repoCommits := b.commits[repoName]
	for _, c := range repoCommits {
		cp := *c

		return &cp, nil
	}

	return nil, fmt.Errorf("%w: no commits found in repository %s", ErrCommitNotFound, repoName)
}

// GetMergeConflicts returns whether there are merge conflicts (always false).
func (b *InMemoryBackend) GetMergeConflicts(
	repoName, _ /* sourceCommitSpecifier */, _ /* destCommitSpecifier */, _ /* mergeOption */ string,
) (bool, error) {
	b.mu.RLock("GetMergeConflicts")
	defer b.mu.RUnlock()

	if _, ok := b.repositories[repoName]; !ok {
		return false, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	return false, nil
}

// GetDifferences returns file differences between beforeCommitSpecifier and afterCommitSpecifier.
// When beforeCommitSpecifier is empty, returns all files in afterCommitSpecifier as ADDed.
func (b *InMemoryBackend) GetDifferences(repoName, afterCommitSpecifier, _ string) ([]FileDifference, error) {
	b.mu.RLock("GetDifferences")
	defer b.mu.RUnlock()

	if _, ok := b.repositories[repoName]; !ok {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	repoFiles := b.files[repoName]
	if len(repoFiles) == 0 {
		return []FileDifference{}, nil
	}

	// Simplified diff: collect files associated with afterCommitSpecifier.
	// When before is empty, treat all files as ADDed.
	var diffs []FileDifference
	for _, f := range repoFiles {
		if afterCommitSpecifier == "" || f.CommitSpecifier == afterCommitSpecifier || afterCommitSpecifier == f.BlobID {
			diffs = append(diffs, FileDifference{
				AfterBlob:  f.BlobID,
				BeforeBlob: "",
				ChangeType: "A",
			})
		}
	}

	sort.Slice(diffs, func(i, j int) bool {
		return diffs[i].AfterBlob < diffs[j].AfterBlob
	})

	return diffs, nil
}
