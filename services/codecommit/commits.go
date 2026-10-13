package codecommit

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

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
	before, _, ok := strings.CutLast(filePath, "/")
	if !ok {
		return ""
	}

	return before
}

// keepEmptyFoldersLocked adds a .gitkeep (tree and file view) to each folder a
// deletion emptied when keepEmptyFolders is set; it is not reported as added.
func (b *InMemoryBackend) keepEmptyFoldersLocked(
	repoName, commitID string, deleteFiles []string, keepEmptyFolders bool, tree map[string]TreeEntry,
) {
	if !keepEmptyFolders {
		return
	}

	seen := make(map[string]bool, len(deleteFiles))

	for _, fp := range deleteFiles {
		folder := parentFolder(fp)
		if folder == "" || seen[folder] {
			continue
		}

		seen[folder] = true

		if treeHasUnder(tree, folder) {
			continue
		}

		keepPath := folder + "/" + gitkeepFileName
		blobID := newObjectID()
		b.storeBlobLocked(repoName, blobID, []byte{})
		tree[keepPath] = TreeEntry{BlobID: blobID, Mode: fileModeDefault}
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
	tree map[string]TreeEntry,
) (map[string]string, map[string]string) {
	blobIDsAdded := make(map[string]string, len(putFiles))
	blobIDsDeleted := make(map[string]string, len(deleteFiles))

	for _, pf := range putFiles {
		fileMode := pf.FileMode
		if fileMode == "" {
			fileMode = fileModeDefault
		}
		blobID := newObjectID()
		b.files.Put(&File{
			FilePath:        pf.FilePath,
			CommitSpecifier: commitID,
			BlobID:          blobID,
			FileMode:        fileMode,
			FileContent:     pf.FileContent,
			RepoName:        repoName,
		})
		b.recordFileHistory(repoName, pf.FilePath, commitID, blobID)
		b.storeBlobLocked(repoName, blobID, pf.FileContent)
		tree[pf.FilePath] = TreeEntry{BlobID: blobID, Mode: fileMode}
		blobIDsAdded[pf.FilePath] = blobID
	}
	for _, fp := range deleteFiles {
		var removedBlobID string
		if existing, ok := b.files.Get(fileKey(repoName, fp)); ok {
			removedBlobID = existing.BlobID
		}
		b.files.Delete(fileKey(repoName, fp))
		delete(tree, fp)
		b.recordFileHistory(repoName, fp, commitID, removedBlobID)
		blobIDsDeleted[fp] = removedBlobID
	}

	b.keepEmptyFoldersLocked(repoName, commitID, deleteFiles, keepEmptyFolders, tree)

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
	res, err := b.CreateCommitDetailed(
		repositoryName, branchName, authorName, authorEmail, message, parentCommitID,
		putFiles, deleteFiles, nil, keepEmptyFolders,
	)
	if err != nil {
		return nil, nil, nil, err
	}

	added := make(map[string]string, len(res.Added)+len(res.Updated))
	maps.Copy(added, res.Added)
	maps.Copy(added, res.Updated)

	return res.Commit, added, res.Deleted, nil
}

// branchTipLocked returns the branch's current tip, rejecting an outdated parentCommitID
// (ParentCommitIdOutdatedException); parentCommitID is optional.
func (b *InMemoryBackend) branchTipLocked(repositoryName, branchName, parentCommitID string) (string, error) {
	var currentTip string

	if branchName != "" {
		if existing, ok := b.branches.Get(branchKey(repositoryName, branchName)); ok {
			currentTip = existing.CommitID
		}
	}

	if parentCommitID != "" && currentTip != "" && parentCommitID != currentTip {
		return "", fmt.Errorf(
			"%w: parentCommitId %s does not match current branch tip %s",
			ErrParentCommitIDOutdated, parentCommitID, currentTip,
		)
	}

	return currentTip, nil
}

// checkCommitChangesLocked rejects unchanged putFiles (NoChangeException, which is CreateCommit's
// own code; SameFileContentException is PutFile's) and setFileModes of missing files, before any mutation.
func (b *InMemoryBackend) checkCommitChangesLocked(
	repositoryName string, tree map[string]TreeEntry, putFiles []PutFileEntry, setFileModes []SetFileModeEntry,
) error {
	for _, pf := range putFiles {
		if b.treeFileContentEquals(repositoryName, tree, pf.FilePath, pf.FileContent) {
			return fmt.Errorf("%w: file %s content is unchanged", ErrNoChange, pf.FilePath)
		}
	}

	for _, sm := range setFileModes {
		if _, ok := tree[sm.FilePath]; !ok {
			return fmt.Errorf("%w: file %s does not exist", ErrFileNotFound, sm.FilePath)
		}
	}

	return nil
}

// CommitChanges is CreateCommit's result: blob IDs by path for added, updated and deleted files.
type CommitChanges struct {
	Commit  *Commit
	Added   map[string]string
	Updated map[string]string
	Deleted map[string]string
}

// CreateCommitDetailed is CreateCommit that also applies setFileModes and reports updated files separately.
func (b *InMemoryBackend) CreateCommitDetailed(
	repositoryName, branchName, authorName, authorEmail, message, parentCommitID string,
	putFiles []PutFileEntry, deleteFiles []string, setFileModes []SetFileModeEntry, keepEmptyFolders bool,
) (*CommitChanges, error) {
	b.mu.Lock("CreateCommit")
	defer b.mu.Unlock()

	if !b.repositories.Has(repositoryName) {
		return nil, fmt.Errorf("%w: %s does not exist", ErrNotFound, repositoryName)
	}

	currentTip, err := b.branchTipLocked(repositoryName, branchName, parentCommitID)
	if err != nil {
		return nil, err
	}

	tree := b.parentTreeLocked(repositoryName, currentTip)

	if err = b.checkCommitChangesLocked(repositoryName, tree, putFiles, setFileModes); err != nil {
		return nil, err
	}

	existing := make(map[string]bool, len(putFiles))
	for _, pf := range putFiles {
		_, existing[pf.FilePath] = tree[pf.FilePath]
	}

	commitID := newObjectID()
	treeID := newObjectID()
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
		repositoryName, commitID, putFiles, deleteFiles, keepEmptyFolders, tree,
	)
	updated := b.applySetFileModesLocked(repositoryName, commitID, setFileModes, tree)
	setCommitTree(commit, tree)

	added := make(map[string]string, len(blobIDsAdded))

	for path, blobID := range blobIDsAdded {
		if existing[path] {
			updated[path] = blobID
		} else {
			added[path] = blobID
		}
	}

	// Update the branch tip to the new commit.
	if branchName != "" {
		b.putBranchLocked(&Branch{
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

	return &CommitChanges{Commit: &cp, Added: added, Updated: updated, Deleted: blobIDsDeleted}, nil
}

// applySetFileModesLocked changes file modes in place, returning each touched path's blob ID.
func (b *InMemoryBackend) applySetFileModesLocked(
	repoName, commitID string, modes []SetFileModeEntry, tree map[string]TreeEntry,
) map[string]string {
	touched := make(map[string]string, len(modes))

	for _, sm := range modes {
		entry := tree[sm.FilePath]
		entry.Mode = sm.FileMode
		tree[sm.FilePath] = entry

		if f, ok := b.files.Get(fileKey(repoName, sm.FilePath)); ok {
			f.FileMode = sm.FileMode
		}

		b.recordFileHistory(repoName, sm.FilePath, commitID, entry.BlobID)
		touched[sm.FilePath] = entry.BlobID
	}

	return touched
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
		return nil, nil, fmt.Errorf("%w: %s does not exist", ErrNotFound, repositoryName)
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
		return nil, fmt.Errorf("%w: %s does not exist", ErrNotFound, repositoryName)
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
	repoName, afterCommitSpecifier, beforeCommitSpecifier, nextToken string, maxResults int,
	afterPath string,
) (page.Page[FileDifference], error) {
	if err := page.ValidateToken(nextToken); err != nil {
		return page.Page[FileDifference]{}, fmt.Errorf("%w: invalid NextToken", ErrInvalidContinuationToken)
	}

	b.mu.RLock("GetDifferences")
	defer b.mu.RUnlock()

	if !b.repositories.Has(repoName) {
		return page.Page[FileDifference]{}, fmt.Errorf("%w: %s does not exist", ErrNotFound, repoName)
	}

	if diffs, ok := b.treeDifferencesLocked(repoName, beforeCommitSpecifier, afterCommitSpecifier, afterPath); ok {
		return page.New(diffs, nextToken, maxResults, getDifferencesDefaultMaxResults), nil
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

// treeDifferencesLocked diffs the two commits' trees (all of after's files
// when before is empty); ok is false when either tree is unavailable.
func (b *InMemoryBackend) treeDifferencesLocked(repo, before, after, path string) ([]FileDifference, bool) {
	_, afterTree, ok := b.specTreeLocked(repo, after)
	if !ok {
		return nil, false
	}

	beforeTree := map[string]TreeEntry{}

	if before != "" {
		if _, beforeTree, ok = b.specTreeLocked(repo, before); !ok {
			return nil, false
		}
	}

	var diffs []FileDifference

	for _, p := range unionPaths(beforeTree, afterTree) {
		if !pathMatchesFilter(p, path) {
			continue
		}

		bt, hadBefore := beforeTree[p]
		at, hasAfter := afterTree[p]

		switch {
		case hadBefore && hasAfter && bt == at:
			continue
		case !hadBefore:
			diffs = append(diffs, FileDifference{AfterBlob: blobInfo(p, at), ChangeType: "A"})
		case !hasAfter:
			diffs = append(diffs, FileDifference{BeforeBlob: blobInfo(p, bt), ChangeType: "D"})
		default:
			diffs = append(
				diffs,
				FileDifference{BeforeBlob: blobInfo(p, bt), AfterBlob: blobInfo(p, at), ChangeType: "M"},
			)
		}
	}

	return diffs, true
}

func blobInfo(path string, e TreeEntry) *BlobInfo {
	return &BlobInfo{BlobID: e.BlobID, Path: path, Mode: modeOrDefault(e.Mode)}
}
