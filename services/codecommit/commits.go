package codecommit

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// pathMatchesFilter reports whether filePath matches an optional path
// filter: an empty filter matches everything; otherwise filePath must equal
// the filter exactly or lie under it as a directory prefix.
func pathMatchesFilter(filePath, filter string) bool {
	if filter == "" || filePath == filter {
		return true
	}

	prefix := filter
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	return strings.HasPrefix(filePath, prefix)
}

// recordFileHistory appends an entry to repoName/filePath's ordered
// (oldest-first) history, initializing the per-repo map on first use. Caller
// must hold the write lock.
func (b *InMemoryBackend) recordFileHistory(repoName, filePath, commitID, blobID string) {
	if b.fileHistory[repoName] == nil {
		b.fileHistory[repoName] = make(map[string][]FileHistoryEntry)
	}
	b.fileHistory[repoName][filePath] = append(
		b.fileHistory[repoName][filePath], FileHistoryEntry{CommitID: commitID, BlobID: blobID},
	)
}

// gitkeepFileName is the marker file real CodeCommit creates in a folder a
// deletion would otherwise leave empty, when KeepEmptyFolders is requested
// (e.g. api_op_CreateCommit.go: "If true, a .gitkeep file is created for
// empty folders.").
const gitkeepFileName = ".gitkeep"

// parentFolder returns the directory portion of filePath, or "" if filePath
// has no folder component (lives at the repository root).
func parentFolder(filePath string) string {
	idx := strings.LastIndexByte(filePath, '/')
	if idx < 0 {
		return ""
	}

	return filePath[:idx]
}

// folderHasFilesLocked reports whether any file in repoFiles still lives
// under folder (as an exact match or a "/"-prefixed descendant). Caller must
// hold at least the read lock.
func folderHasFilesLocked(repoFiles []*File, folder string) bool {
	for _, f := range repoFiles {
		if pathMatchesFilter(f.FilePath, folder) {
			return true
		}
	}

	return false
}

// keepEmptyFoldersLocked creates a .gitkeep marker under each folder left
// empty by deleteFiles, when keepEmptyFolders is true -- matching real
// CodeCommit's documented default (false: empty folders are deleted, i.e.
// left with no marker and so absent from GetFolder) versus true (a .gitkeep
// keeps the folder appearing). The marker is not reported back via
// blobIDsAdded: real CreateCommitOutput.FilesAdded/DeleteFileOutput report
// only the files the caller explicitly changed, not this internal
// side effect. Caller must hold the write lock.
func (b *InMemoryBackend) keepEmptyFoldersLocked(
	repoName, commitID string, deleteFiles []string, keepEmptyFolders bool,
) {
	if !keepEmptyFolders {
		return
	}

	repoFiles := b.filesByRepo.Get(repoName)

	seen := make(map[string]bool, len(deleteFiles))

	for _, fp := range deleteFiles {
		folder := parentFolder(fp)
		if folder == "" || seen[folder] {
			continue
		}

		seen[folder] = true

		if folderHasFilesLocked(repoFiles, folder) {
			continue
		}

		keepPath := folder + "/" + gitkeepFileName
		blobID := uuid.NewString()
		b.files.Put(&File{
			FilePath:        keepPath,
			CommitSpecifier: commitID,
			BlobID:          blobID,
			FileMode:        fileModeDefault,
			FileContent:     []byte{},
			RepoName:        repoName,
		})
		b.recordFileHistory(repoName, keepPath, commitID, blobID)
	}
}

// applyFileChanges applies put and delete file entries to the repository file store.
// It returns the blob ID assigned to each put file (blobIDsAdded) and the blob
// ID removed by each delete (blobIDsDeleted), both keyed by filePath, so
// callers can report AWS-accurate blobId values (CreateCommitOutput.filesAdded
// and .filesDeleted both carry a blobId per entry). Every put/delete is also
// recorded in fileHistory so ListFileCommitHistory reflects it. Caller must
// hold the write lock.
func (b *InMemoryBackend) applyFileChanges(
	repoName, commitID string, putFiles []PutFileEntry, deleteFiles []string, keepEmptyFolders bool,
) (map[string]string, map[string]string) {
	blobIDsAdded := make(map[string]string, len(putFiles))
	blobIDsDeleted := make(map[string]string, len(deleteFiles))

	for _, pf := range putFiles {
		fileMode := pf.FileMode
		if fileMode == "" {
			fileMode = fileModeDefault
		}
		blobID := uuid.NewString()
		b.files.Put(&File{
			FilePath:        pf.FilePath,
			CommitSpecifier: commitID,
			BlobID:          blobID,
			FileMode:        fileMode,
			FileContent:     pf.FileContent,
			RepoName:        repoName,
		})
		b.recordFileHistory(repoName, pf.FilePath, commitID, blobID)
		blobIDsAdded[pf.FilePath] = blobID
	}
	for _, fp := range deleteFiles {
		var removedBlobID string
		if existing, ok := b.files.Get(fileKey(repoName, fp)); ok {
			removedBlobID = existing.BlobID
		}
		b.files.Delete(fileKey(repoName, fp))
		b.recordFileHistory(repoName, fp, commitID, removedBlobID)
		blobIDsDeleted[fp] = removedBlobID
	}

	b.keepEmptyFoldersLocked(repoName, commitID, deleteFiles, keepEmptyFolders)

	return blobIDsAdded, blobIDsDeleted
}

// CreateCommit creates a new commit in a repository, tracking parent commits from the
// current branch head.
//
// parentCommitID must match the current branch tip when the branch already has commits;
// AWS returns ParentCommitIdRequiredException if omitted and ParentCommitIdOutdatedException
// if it does not match the current tip.
func (b *InMemoryBackend) CreateCommit(
	repositoryName, branchName, authorName, authorEmail, message, parentCommitID string,
	putFiles []PutFileEntry, deleteFiles []string, keepEmptyFolders bool,
) (*Commit, map[string]string, map[string]string, error) {
	b.mu.Lock("CreateCommit")
	defer b.mu.Unlock()

	if !b.repositories.Has(repositoryName) {
		return nil, nil, nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repositoryName)
	}

	// Determine current branch tip (if any).
	var currentTip string
	if branchName != "" {
		if existing, ok := b.branches.Get(branchKey(repositoryName, branchName)); ok {
			currentTip = existing.CommitID
		}
	}

	// Validate parentCommitId when provided — AWS returns ParentCommitIdOutdatedException
	// when the provided value does not match the current branch tip.
	// parentCommitId is optional; omitting it is allowed (no race detection in that case).
	if parentCommitID != "" && currentTip != "" && parentCommitID != currentTip {
		return nil, nil, nil, fmt.Errorf(
			"%w: parentCommitId %s does not match current branch tip %s",
			ErrParentCommitIDOutdated, parentCommitID, currentTip,
		)
	}

	// AWS rejects a commit whose putFiles entry has content identical to
	// what's already at that path with NoChangeException (CreateCommit's own
	// declared error set has no SameFileContentException; that's PutFile's),
	// checked before any mutation so a rejected commit leaves no partial state.
	for _, pf := range putFiles {
		if existing, ok := b.files.Get(fileKey(repositoryName, pf.FilePath)); ok &&
			bytes.Equal(existing.FileContent, pf.FileContent) {
			return nil, nil, nil, fmt.Errorf(
				"%w: file %s content is unchanged", ErrNoChange, pf.FilePath,
			)
		}
	}

	commitID := uuid.NewString()
	treeID := uuid.NewString()
	now := time.Now().UTC()

	// Track parent commit.
	var parents []string
	if currentTip != "" {
		parents = []string{currentTip}
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
		CreatedAt:      now,
	}

	b.commits.Put(commit)

	// Apply putFiles and deleteFiles to the file store.
	blobIDsAdded, blobIDsDeleted := b.applyFileChanges(
		repositoryName, commitID, putFiles, deleteFiles, keepEmptyFolders,
	)

	// Update the branch tip to the new commit.
	if branchName != "" {
		b.branches.Put(&Branch{
			BranchName:     branchName,
			CommitID:       commitID,
			RepositoryName: repositoryName,
		})
	}

	cp := *commit
	if len(parents) > 0 {
		cp.Parents = make([]string, len(parents))
		copy(cp.Parents, parents)
	}

	return &cp, blobIDsAdded, blobIDsDeleted, nil
}

// BatchGetCommits retrieves multiple commits by ID from a repository.
// Returns a 404 error if the repository does not exist.
func (b *InMemoryBackend) BatchGetCommits(
	repositoryName string,
	commitIDs []string,
) ([]*Commit, []BatchCommitError, error) {
	b.mu.RLock("BatchGetCommits")
	defer b.mu.RUnlock()

	if !b.repositories.Has(repositoryName) {
		return nil, nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repositoryName)
	}

	found := make([]*Commit, 0, len(commitIDs))
	errors := make([]BatchCommitError, 0, len(commitIDs))

	for _, id := range commitIDs {
		c, ok := b.commits.Get(commitKey(repositoryName, id))
		if !ok {
			// Not CommitDoesNotExistException: api-2.json types this field and
			// GetCommitInput.commitId identically as ObjectId (a raw SHA lookup),
			// distinct from the CommitId shape used by specifier-resolving ops
			// like CreateBranch (gopherstack-pfyr).
			errors = append(errors, BatchCommitError{
				CommitID:     id,
				ErrorCode:    "CommitIdDoesNotExistException",
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

// GetCommit returns a commit by repository and commit ID.
func (b *InMemoryBackend) GetCommit(repositoryName, commitID string) (*Commit, error) {
	b.mu.RLock("GetCommit")
	defer b.mu.RUnlock()

	if !b.repositories.Has(repositoryName) {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repositoryName)
	}

	c, ok := b.commits.Get(commitKey(repositoryName, commitID))
	if !ok {
		return nil, fmt.Errorf("%w: commit %s not found", ErrCommitIDNotFound, commitID)
	}

	cp := *c

	return &cp, nil
}

// GetDifferences returns file differences between beforeCommitSpecifier and afterCommitSpecifier.
// When beforeCommitSpecifier is empty, returns all files in afterCommitSpecifier as ADDed.
// getDifferencesDefaultMaxResults is applied when the caller does not supply
// a positive maxResults (mirrors the emulator-wide convention of a generous
// default page size; AWS does not publish an exact default for this op).
const getDifferencesDefaultMaxResults = 100

// GetDifferences returns a page of file differences between
// beforeCommitSpecifier and afterCommitSpecifier. When beforeCommitSpecifier
// is empty, returns all files in afterCommitSpecifier as ADDed. nextToken and
// maxResults implement AWS's cursor-based pagination for this op.
func (b *InMemoryBackend) GetDifferences(
	repoName, afterCommitSpecifier, _ /* beforeCommitSpecifier */, nextToken string, maxResults int,
	afterPath string,
) (page.Page[FileDifference], error) {
	if err := page.ValidateToken(nextToken); err != nil {
		return page.Page[FileDifference]{}, fmt.Errorf("%w: invalid NextToken", ErrInvalidContinuationToken)
	}

	b.mu.RLock("GetDifferences")
	defer b.mu.RUnlock()

	if !b.repositories.Has(repoName) {
		return page.Page[FileDifference]{}, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	repoFiles := b.filesByRepo.Get(repoName)

	// Simplified diff: collect files associated with afterCommitSpecifier.
	// When before is empty, treat all files as ADDed. AfterPath ("Limits the
	// results to this path. Can also be used to specify... a directory or
	// folder" -- api_op_GetDifferences.go) narrows to that exact file, or any
	// file under it as a directory prefix.
	var diffs []FileDifference
	for _, f := range repoFiles {
		if afterCommitSpecifier != "" && f.CommitSpecifier != afterCommitSpecifier && afterCommitSpecifier != f.BlobID {
			continue
		}

		if !pathMatchesFilter(f.FilePath, afterPath) {
			continue
		}

		mode := f.FileMode
		if mode == "" {
			mode = "100644"
		}
		diffs = append(diffs, FileDifference{
			AfterBlob:  &BlobInfo{BlobID: f.BlobID, Path: f.FilePath, Mode: mode},
			BeforeBlob: nil,
			ChangeType: "A",
		})
	}

	sort.Slice(diffs, func(i, j int) bool {
		pathI, pathJ := "", ""
		if diffs[i].AfterBlob != nil {
			pathI = diffs[i].AfterBlob.Path
		}
		if diffs[j].AfterBlob != nil {
			pathJ = diffs[j].AfterBlob.Path
		}

		return pathI < pathJ
	})

	return page.New(diffs, nextToken, maxResults, getDifferencesDefaultMaxResults), nil
}
