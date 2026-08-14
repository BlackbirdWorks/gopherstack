package codecommit

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
	"github.com/google/uuid"
)

// CreateRepository creates a new CodeCommit repository.
func (b *InMemoryBackend) CreateRepository(name, description string, kv map[string]string) (*Repository, error) {
	b.mu.Lock("CreateRepository")
	defer b.mu.Unlock()

	if err := ValidateRepositoryName(name); err != nil {
		return nil, err
	}

	if b.repositories.Has(name) {
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
	b.repositories.Put(r)
	b.repositoriesByARN[repoARN] = name
	cp := *r

	return &cp, nil
}

// GetRepository returns a repository by name.
func (b *InMemoryBackend) GetRepository(name string) (*Repository, error) {
	b.mu.RLock("GetRepository")
	defer b.mu.RUnlock()

	r, ok := b.repositories.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, name)
	}
	cp := *r

	return &cp, nil
}

// DeleteRepository deletes a repository by name and cascades to branches, commits,
// template associations, files, triggers, and pull requests targeting this repository.
func (b *InMemoryBackend) DeleteRepository(name string) (*Repository, error) {
	b.mu.Lock("DeleteRepository")
	defer b.mu.Unlock()

	r, ok := b.repositories.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, name)
	}
	cp := *r
	b.repositories.Delete(name)
	delete(b.repositoriesByARN, r.ARN)
	r.Tags.Close()

	// Cascade: remove branches, commits, template-associations, files,
	// fileHistory, triggers.
	for _, br := range append([]*Branch{}, b.branchesByRepo.Get(name)...) {
		b.branches.Delete(branchKey(name, br.BranchName))
	}
	for _, c := range append([]*Commit{}, b.commitsByRepo.Get(name)...) {
		b.commits.Delete(commitKey(name, c.CommitID))
	}
	delete(b.repoTemplateAssoc, name)
	for _, f := range append([]*File{}, b.filesByRepo.Get(name)...) {
		b.files.Delete(fileKey(name, f.FilePath))
	}
	delete(b.fileHistory, name)
	delete(b.triggers, name)

	// Cascade: remove compared-commit comments (and their reactions) that
	// belong directly to this repository — these are not reachable through
	// any pull request, so they must be swept independently of the PR loop
	// below.
	b.deleteCommentsForRepo(name)

	// Cascade: remove pull requests that target this repository, and every
	// PR comment (+ reactions) that belongs to one of them.
	for _, pr := range b.pullRequests.All() {
		for _, t := range pr.PullRequestTargets {
			if t.RepositoryName == name {
				prID := pr.PullRequestID
				b.pullRequests.Delete(prID)
				delete(b.prApprovals, prID)

				for _, rule := range append([]*PullRequestApprovalRule{}, b.prApprovalRulesByPR.Get(prID)...) {
					b.prApprovalRules.Delete(prApprovalRuleKey(prID, rule.RuleName))
				}

				delete(b.prOverrides, prID)
				delete(b.prOverriders, prID)
				delete(b.prEvents, prID)
				b.deleteCommentsForPR(prID)

				break
			}
		}
	}

	return &cp, nil
}

// deleteCommentsForRepo removes every compared-commit comment (and its
// reactions) whose RepoName is repoName. comments has no secondary index (it
// is a "dirty" table — see store_setup.go), so this scans the full table;
// acceptable since it only runs on the already-O(n)-cascade DeleteRepository
// path. Caller must hold the write lock.
func (b *InMemoryBackend) deleteCommentsForRepo(repoName string) {
	for _, c := range b.comments.All() {
		if c.RepoName == repoName {
			b.comments.Delete(c.CommentID)
			delete(b.commentReactions, c.CommentID)
		}
	}
}

// deleteCommentsForPR removes every pull-request comment (and its reactions)
// whose PRid is prID. See deleteCommentsForRepo's doc for why this scans the
// full table. Caller must hold the write lock.
func (b *InMemoryBackend) deleteCommentsForPR(prID string) {
	for _, c := range b.comments.All() {
		if c.PRid == prID {
			b.comments.Delete(c.CommentID)
			delete(b.commentReactions, c.CommentID)
		}
	}
}

// ListRepositories returns all repositories sorted by name.
func (b *InMemoryBackend) ListRepositories() []*Repository {
	b.mu.RLock("ListRepositories")
	defer b.mu.RUnlock()

	snap := b.repositories.Snapshot()
	list := make([]*Repository, 0, len(snap))

	for _, r := range snap {
		cp := *r
		list = append(list, &cp)
	}

	return list
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
		r, ok := b.repositories.Get(name)
		if !ok {
			notFound = append(notFound, name)

			continue
		}
		cp := *r
		found = append(found, &cp)
	}

	return found, notFound, nil
}

// UpdateRepositoryDescription sets the description of a repository.
func (b *InMemoryBackend) UpdateRepositoryDescription(name, desc string) error {
	b.mu.Lock("UpdateRepositoryDescription")
	defer b.mu.Unlock()

	r, ok := b.repositories.Get(name)
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

	r, ok := b.repositories.Get(oldName)
	if !ok {
		return fmt.Errorf("%w: repository %s not found", ErrNotFound, oldName)
	}

	if b.repositories.Has(newName) {
		return fmt.Errorf("%w: repository %s already exists", ErrAlreadyExists, newName)
	}

	// Update name in struct
	r.RepositoryName = newName
	r.LastModifiedDate = time.Now().UTC()

	// Re-key
	b.repositories.Delete(oldName)
	b.repositories.Put(r)
	b.repositoriesByARN[r.ARN] = newName

	// Migrate branches, commits, files (composite-keyed tables: delete the old
	// key, update the entry's own repository field, re-insert under the new
	// key) and repoTemplateAssoc/triggers (plain maps keyed by repo name).
	for _, br := range append([]*Branch{}, b.branchesByRepo.Get(oldName)...) {
		b.branches.Delete(branchKey(oldName, br.BranchName))
		br.RepositoryName = newName
		b.branches.Put(br)
	}
	for _, c := range append([]*Commit{}, b.commitsByRepo.Get(oldName)...) {
		b.commits.Delete(commitKey(oldName, c.CommitID))
		c.RepositoryName = newName
		b.commits.Put(c)
	}
	if assoc, hasAssoc := b.repoTemplateAssoc[oldName]; hasAssoc {
		b.repoTemplateAssoc[newName] = assoc
		delete(b.repoTemplateAssoc, oldName)
	}
	for _, f := range append([]*File{}, b.filesByRepo.Get(oldName)...) {
		b.files.Delete(fileKey(oldName, f.FilePath))
		f.RepoName = newName
		b.files.Put(f)
	}
	if triggers, hasTriggers := b.triggers[oldName]; hasTriggers {
		b.triggers[newName] = triggers
		delete(b.triggers, oldName)
	}

	return nil
}

// UpdateRepositoryEncryptionKey sets the KMS key ID for a repository, returning
// the repository ID and the KMS key ID that was previously in effect (the real
// UpdateRepositoryEncryptionKeyOutput echoes RepositoryId, KmsKeyId and
// OriginalKmsKeyId -- api_op_UpdateRepositoryEncryptionKey.go:49).
func (b *InMemoryBackend) UpdateRepositoryEncryptionKey(
	name, kmsKeyID string,
) (string, string, error) {
	b.mu.Lock("UpdateRepositoryEncryptionKey")
	defer b.mu.Unlock()

	r, ok := b.repositories.Get(name)
	if !ok {
		return "", "", fmt.Errorf("%w: repository %s not found", ErrNotFound, name)
	}

	originalKmsKeyID := r.KmsKeyID
	r.KmsKeyID = kmsKeyID
	r.LastModifiedDate = time.Now().UTC()

	return r.RepositoryID, originalKmsKeyID, nil
}
