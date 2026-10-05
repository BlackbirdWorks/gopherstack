package codecommit

import (
	"bytes"
	"maps"
	"strings"
)

func (b *InMemoryBackend) storeBlobLocked(repo, blobID string, content []byte) {
	if b.blobs[repo] == nil {
		b.blobs[repo] = make(map[string][]byte)
	}

	b.blobs[repo][blobID] = content
}

// blobContentLocked returns the blob's bytes, which callers must not mutate.
func (b *InMemoryBackend) blobContentLocked(repo, blobID string) ([]byte, bool) {
	if content, ok := b.blobs[repo][blobID]; ok {
		return content, true
	}

	for _, f := range b.filesByRepo.Get(repo) {
		if f.BlobID == blobID {
			return f.FileContent, true
		}
	}

	return nil, false
}

// treeOfLocked returns commitID's tree; a commit that predates trees reads as
// a snapshot of the repository's current files.
func (b *InMemoryBackend) treeOfLocked(repo, commitID string) map[string]TreeEntry {
	if c, ok := b.commits.Get(commitKey(repo, commitID)); ok && c.HasTree {
		return c.Tree
	}

	tree := make(map[string]TreeEntry)
	for _, f := range b.filesByRepo.Get(repo) {
		tree[f.FilePath] = TreeEntry{BlobID: f.BlobID, Mode: f.FileMode}
	}

	return tree
}

// parentTreeLocked returns a mutable copy of the tip's tree, empty when there is no tip.
func (b *InMemoryBackend) parentTreeLocked(repo, tip string) map[string]TreeEntry {
	if tip == "" {
		return make(map[string]TreeEntry)
	}

	return maps.Clone(b.treeOfLocked(repo, tip))
}

func setCommitTree(c *Commit, tree map[string]TreeEntry) {
	c.Tree = tree
	c.HasTree = true
}

func treeHasUnder(tree map[string]TreeEntry, folder string) bool {
	for p := range tree {
		if pathMatchesFilter(p, folder) {
			return true
		}
	}

	return false
}

// syncFilesToTreeLocked makes the repository's flat file view equal tree.
func (b *InMemoryBackend) syncFilesToTreeLocked(repo, commitID string, tree map[string]TreeEntry) {
	for _, f := range append([]*File(nil), b.filesByRepo.Get(repo)...) {
		if _, keep := tree[f.FilePath]; !keep {
			b.files.Delete(fileKey(repo, f.FilePath))
		}
	}

	for path, entry := range tree {
		if cur, ok := b.files.Get(fileKey(repo, path)); ok && cur.BlobID == entry.BlobID && cur.FileMode == entry.Mode {
			continue
		}

		content, _ := b.blobContentLocked(repo, entry.BlobID)
		b.files.Put(&File{
			FilePath: path, CommitSpecifier: commitID, BlobID: entry.BlobID,
			FileMode: entry.Mode, FileContent: content, RepoName: repo,
		})
	}
}

// recordTreeHistoryLocked appends history for every path whose entry differs between before and after.
func (b *InMemoryBackend) recordTreeHistoryLocked(repo, commitID string, before, after map[string]TreeEntry) {
	for path, entry := range after {
		if prev, ok := before[path]; !ok || prev.BlobID != entry.BlobID {
			b.recordFileHistory(repo, path, commitID, entry.BlobID)
		}
	}

	for path, prev := range before {
		if _, ok := after[path]; !ok {
			b.recordFileHistory(repo, path, commitID, prev.BlobID)
		}
	}
}

func parentDir(path string) string {
	if before, _, ok := strings.CutLast(path, "/"); ok {
		return before
	}

	return ""
}

// specTreeLocked returns the tree a commit specifier names, when it names a
// commit that carries one.
func (b *InMemoryBackend) specTreeLocked(repo, spec string) (string, map[string]TreeEntry, bool) {
	if spec == "" {
		return "", nil, false
	}

	id, err := b.resolveCommitSpecifier(repo, spec)
	if err != nil {
		return "", nil, false
	}

	if c, ok := b.commits.Get(commitKey(repo, id)); ok && c.HasTree {
		return id, c.Tree, true
	}

	return "", nil, false
}

func (b *InMemoryBackend) fileFromEntryLocked(repo, commitID, path string, e TreeEntry) *File {
	content, _ := b.blobContentLocked(repo, e.BlobID)

	return &File{
		FilePath: path, CommitSpecifier: commitID, BlobID: e.BlobID, FileMode: e.Mode,
		FileContent: bytes.Clone(content), RepoName: repo,
	}
}

// treeFileContentEquals reports whether tree holds path with exactly content.
func (b *InMemoryBackend) treeFileContentEquals(
	repo string,
	tree map[string]TreeEntry,
	path string,
	content []byte,
) bool {
	e, ok := tree[path]
	if !ok {
		return false
	}

	cur, _ := b.blobContentLocked(repo, e.BlobID)

	return bytes.Equal(cur, content)
}
