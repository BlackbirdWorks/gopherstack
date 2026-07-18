package codecommit

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// PutFile stores a file and creates a commit. It returns the new commit and
// the blob ID of the stored file content (AWS's PutFileOutput.BlobId is a
// required field, so callers must round-trip this into GetBlob).
func (b *InMemoryBackend) PutFile(repoName, branchName, filePath string, content []byte) (*Commit, string, error) {
	b.mu.Lock("PutFile")
	defer b.mu.Unlock()

	if !b.repositories.Has(repoName) {
		return nil, "", fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	blobID := uuid.NewString()
	b.files.Put(&File{
		FilePath:        filePath,
		CommitSpecifier: branchName,
		BlobID:          blobID,
		FileMode:        fileModeDefault,
		FileContent:     content,
		RepoName:        repoName,
	})

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
	b.commits.Put(commit)

	// Update branch tip
	if branchName != "" {
		b.branches.Put(&Branch{
			BranchName:     branchName,
			CommitID:       commitID,
			RepositoryName: repoName,
		})
	}

	cp := *commit

	return &cp, blobID, nil
}

// GetFile retrieves a file by repository, commit specifier, and path.
func (b *InMemoryBackend) GetFile(repoName, _ /* commitSpecifier */, filePath string) (*File, error) {
	b.mu.RLock("GetFile")
	defer b.mu.RUnlock()

	if !b.repositories.Has(repoName) {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	f, ok := b.files.Get(fileKey(repoName, filePath))
	if !ok {
		return nil, fmt.Errorf("%w: file %s not found", ErrFileNotFound, filePath)
	}
	cp := *f

	return &cp, nil
}

// GetFolder lists file paths under a folder path.
func (b *InMemoryBackend) GetFolder(repoName, _ /* commitSpecifier */, folderPath string) ([]string, error) {
	b.mu.RLock("GetFolder")
	defer b.mu.RUnlock()

	if !b.repositories.Has(repoName) {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	repoFiles := b.filesByRepo.Get(repoName)
	var paths []string
	prefix := folderPath
	if prefix != "" && prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}
	for _, f := range repoFiles {
		fp := f.FilePath
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

	if !b.repositories.Has(repoName) {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	repoFiles := b.filesByRepo.Get(repoName)
	var files []*File
	prefix := folderPath
	if prefix != "" && prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}
	for _, f := range repoFiles {
		fp := f.FilePath
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

// DeleteFile removes a file and creates a delete commit. It returns the new
// commit and the blob ID of the removed file (AWS's DeleteFileOutput.BlobId
// is a required field reporting the blob that was taken out of the tree).
// AWS rejects deletion of a path that does not exist with
// FileDoesNotExistException, so callers must not be able to fabricate a
// delete commit for a file that was never there.
func (b *InMemoryBackend) DeleteFile(
	repoName, branchName, filePath, _ /* parentCommitID */ string,
) (*Commit, string, error) {
	b.mu.Lock("DeleteFile")
	defer b.mu.Unlock()

	if !b.repositories.Has(repoName) {
		return nil, "", fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	existing, ok := b.files.Get(fileKey(repoName, filePath))
	if !ok {
		return nil, "", fmt.Errorf("%w: file %s not found", ErrFileNotFound, filePath)
	}
	blobID := existing.BlobID

	b.files.Delete(fileKey(repoName, filePath))

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
	b.commits.Put(commit)

	// Update branch tip
	if branchName != "" {
		b.branches.Put(&Branch{
			BranchName:     branchName,
			CommitID:       commitID,
			RepositoryName: repoName,
		})
	}

	cp := *commit

	return &cp, blobID, nil
}

// GetBlob returns the content of a blob by blobID.
func (b *InMemoryBackend) GetBlob(repoName, blobID string) ([]byte, error) {
	b.mu.RLock("GetBlob")
	defer b.mu.RUnlock()

	if !b.repositories.Has(repoName) {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	repoFiles := b.filesByRepo.Get(repoName)
	for _, f := range repoFiles {
		if f.BlobID == blobID {
			result := make([]byte, len(f.FileContent))
			copy(result, f.FileContent)

			return result, nil
		}
	}

	return nil, fmt.Errorf("%w: blob %s not found", ErrBlobNotFound, blobID)
}

// ListFileCommitHistory returns commits that touched the given filePath.
// When filePath is empty, all commits for the repository are returned.
func (b *InMemoryBackend) ListFileCommitHistory(repoName, filePath string) ([]*Commit, error) {
	b.mu.RLock("ListFileCommitHistory")
	defer b.mu.RUnlock()

	if !b.repositories.Has(repoName) {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	if filePath == "" {
		repoCommits := b.commitsByRepo.Get(repoName)
		result := make([]*Commit, 0, len(repoCommits))
		for _, c := range repoCommits {
			cp := *c
			cp.Parents = make([]string, len(c.Parents))
			copy(cp.Parents, c.Parents)
			result = append(result, &cp)
		}

		return result, nil
	}

	// Use fileHistory to find all commits that touched this file path.
	commitIDs, ok := b.fileHistory[repoName][filePath]
	if !ok || len(commitIDs) == 0 {
		return []*Commit{}, nil
	}

	result := make([]*Commit, 0, len(commitIDs))
	for _, commitID := range commitIDs {
		c, exists := b.commits.Get(commitKey(repoName, commitID))
		if !exists {
			continue
		}
		cp := *c
		cp.Parents = make([]string, len(c.Parents))
		copy(cp.Parents, c.Parents)
		result = append(result, &cp)
	}

	return result, nil
}
