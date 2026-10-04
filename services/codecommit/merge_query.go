package codecommit

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	defaultMaxConflictFiles = 100
	defaultMaxMergeHunks    = 100
)

// MergeQuery carries the paging and conflict handling inputs of the read-only merge operations.
type MergeQuery struct {
	NextToken string
	Settings  MergeSettings
	MaxFiles  int
	MaxHunks  int
}

// MergeConflictsResult is GetMergeConflicts' output.
type MergeConflictsResult struct {
	SourceCommitID      string
	DestinationCommitID string
	BaseCommitID        string
	NextToken           string
	Conflicts           []ConflictMetadata
	Mergeable           bool
}

// DescribeMergeConflictsResult is DescribeMergeConflicts' output.
type DescribeMergeConflictsResult struct {
	SourceCommitID      string
	DestinationCommitID string
	BaseCommitID        string
	NextToken           string
	Hunks               []MergeHunk
	Metadata            ConflictMetadata
}

// MergeOptionsResult is GetMergeOptions' output.
type MergeOptionsResult struct {
	SourceCommitID      string
	DestinationCommitID string
	BaseCommitID        string
	Options             []string
}

// MergeCommitResult is GetMergeCommit's output.
type MergeCommitResult struct {
	SourceCommitID      string
	DestinationCommitID string
	BaseCommitID        string
	MergedCommitID      string
}

// prepareMergeLocked resolves both specifiers and evaluates the merge.
func (b *InMemoryBackend) prepareMergeLocked(
	repo, sourceSpec, destSpec string, s MergeSettings,
) (string, string, *mergeEvaluation, error) {
	if sourceSpec == "" || destSpec == "" {
		return "", "", nil, fmt.Errorf("%w: commit specifier is required", ErrCommitSpecifierRequired)
	}

	if !b.repositories.Has(repo) {
		return "", "", nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repo)
	}

	sourceID, err := b.resolveCommitSpecifier(repo, sourceSpec)
	if err != nil {
		return "", "", nil, err
	}

	destID, err := b.resolveCommitSpecifier(repo, destSpec)
	if err != nil {
		return "", "", nil, err
	}

	return sourceID, destID, b.evaluateMergeLocked(repo, sourceID, destID, s), nil
}

// GetMergeOptions lists the merge strategies available for the two commits.
func (b *InMemoryBackend) GetMergeOptions(
	repoName, sourceSpec, destSpec string, s MergeSettings,
) (*MergeOptionsResult, error) {
	b.mu.RLock("GetMergeOptions")
	defer b.mu.RUnlock()

	sourceID, destID, ev, err := b.prepareMergeLocked(repoName, sourceSpec, destSpec, s)
	if err != nil {
		return nil, err
	}

	var options []string

	if ev.baseID == destID {
		options = append(options, mergeOptionFastForward)
	}

	if len(ev.unresolved) == 0 {
		options = append(options, mergeOptionSquash, mergeOptionThreeWay)
	}

	return &MergeOptionsResult{
		Options: options, SourceCommitID: sourceID, DestinationCommitID: destID, BaseCommitID: ev.baseID,
	}, nil
}

// CreateUnreferencedMergeCommit creates, without moving any branch, the commit
// a squash or three-way merge of the two specifiers would produce.
func (b *InMemoryBackend) CreateUnreferencedMergeCommit(
	repoName, sourceSpec, destSpec, mergeOption, authorName, authorEmail, message string, s MergeSettings,
) (*Commit, error) {
	if mergeOption == mergeOptionFastForward {
		return nil, fmt.Errorf("%w: FAST_FORWARD_MERGE creates no merge commit", ErrInvalidMergeOption)
	}

	b.mu.Lock("CreateUnreferencedMergeCommit")
	defer b.mu.Unlock()

	sourceID, destID, ev, err := b.prepareMergeLocked(repoName, sourceSpec, destSpec, s)
	if err != nil {
		return nil, err
	}

	if len(ev.unresolved) > 0 {
		return nil, fmt.Errorf("%w: %d file(s) have unresolved conflicts", ErrManualMergeRequired, len(ev.unresolved))
	}

	if message == "" {
		message = "Unreferenced merge commit"
	}

	parents := []string{destID}
	if mergeOption == mergeOptionThreeWay {
		parents = append(parents, sourceID)
	}

	return cloneCommit(b.newMergeCommitLocked(repoName, parents, ev, message, authorName, authorEmail)), nil
}

// GetMergeCommit returns the merge commit of the two specifiers, creating the
// unreferenced three-way commit if absent; unresolved conflicts leave it empty.
func (b *InMemoryBackend) GetMergeCommit(
	repoName, sourceSpec, destSpec string, s MergeSettings,
) (*MergeCommitResult, error) {
	b.mu.Lock("GetMergeCommit")
	defer b.mu.Unlock()

	sourceID, destID, ev, err := b.prepareMergeLocked(repoName, sourceSpec, destSpec, s)
	if err != nil {
		return nil, err
	}

	out := &MergeCommitResult{SourceCommitID: sourceID, DestinationCommitID: destID, BaseCommitID: ev.baseID}
	if len(ev.unresolved) > 0 {
		return out, nil
	}

	parents := []string{destID, sourceID}
	for _, c := range b.commitsByRepo.Get(repoName) {
		if slices.Equal(c.Parents, parents) && b.treesEqualLocked(repoName, c, ev) {
			out.MergedCommitID = c.CommitID

			return out, nil
		}
	}

	out.MergedCommitID = b.newMergeCommitLocked(repoName, parents, ev, "Merge commit", "", "").CommitID

	return out, nil
}

// treesEqualLocked reports whether commit c's tree has ev's files, modes and contents.
func (b *InMemoryBackend) treesEqualLocked(repo string, c *Commit, ev *mergeEvaluation) bool {
	if !c.HasTree || len(c.Tree) != len(ev.tree) {
		return false
	}

	for path, want := range ev.tree {
		got, ok := c.Tree[path]
		if !ok || got.Mode != want.Mode {
			return false
		}

		gotContent, _ := b.blobContentLocked(repo, got.BlobID)

		wantContent, found := ev.newBlobs[want.BlobID]
		if !found {
			wantContent, _ = b.blobContentLocked(repo, want.BlobID)
		}

		if !bytes.Equal(gotContent, wantContent) {
			return false
		}
	}

	return true
}

func conflictResult(ev *mergeEvaluation, sourceID, destID string) *MergeConflictsResult {
	out := &MergeConflictsResult{
		SourceCommitID: sourceID, DestinationCommitID: destID, BaseCommitID: ev.baseID,
		Mergeable: len(ev.unresolved) == 0,
	}

	for _, fm := range ev.sortedUnresolved() {
		out.Conflicts = append(out.Conflicts, fm.meta)
	}

	return out
}

// GetMergeConflicts reports whether the two commits merge cleanly and lists the conflicting files.
func (b *InMemoryBackend) GetMergeConflicts(
	repoName, sourceSpec, destSpec, mergeOption string, q MergeQuery,
) (*MergeConflictsResult, error) {
	if err := page.ValidateToken(q.NextToken); err != nil {
		return nil, fmt.Errorf("%w: invalid NextToken", ErrInvalidContinuationToken)
	}

	b.mu.RLock("GetMergeConflicts")
	defer b.mu.RUnlock()

	sourceID, destID, ev, err := b.prepareMergeLocked(repoName, sourceSpec, destSpec, q.Settings)
	if err != nil {
		return nil, err
	}

	if mergeOption == mergeOptionFastForward {
		return &MergeConflictsResult{
			SourceCommitID: sourceID, DestinationCommitID: destID, BaseCommitID: ev.baseID,
			Mergeable: ev.baseID == destID,
		}, nil
	}

	out := conflictResult(ev, sourceID, destID)
	pg := page.New(out.Conflicts, q.NextToken, q.MaxFiles, defaultMaxConflictFiles)
	out.Conflicts, out.NextToken = pg.Data, pg.Next

	return out, nil
}

// BatchDescribeMergeConflicts describes the conflicts of the requested files,
// or of every conflicting file when none is requested.
func (b *InMemoryBackend) BatchDescribeMergeConflicts(
	repoName, destSpec, sourceSpec, mergeOption string, filePaths []string, q MergeQuery,
) (*BatchDescribeMergeConflictsResult, error) {
	if err := page.ValidateToken(q.NextToken); err != nil {
		return nil, fmt.Errorf("%w: invalid NextToken", ErrInvalidContinuationToken)
	}

	b.mu.RLock("BatchDescribeMergeConflicts")
	defer b.mu.RUnlock()

	sourceID, destID, ev, err := b.prepareMergeLocked(repoName, sourceSpec, destSpec, q.Settings)
	if err != nil {
		return nil, err
	}

	out := &BatchDescribeMergeConflictsResult{
		DestinationCommitID: destID, SourceCommitID: sourceID, BaseCommitID: ev.baseID,
		Conflicts: []MergeConflict{},
	}

	if mergeOption == mergeOptionFastForward {
		return out, nil
	}

	var files []*fileMerge

	if len(filePaths) == 0 {
		files = ev.sortedUnresolved()
	} else {
		files = ev.requestedFiles(filePaths, out)
	}

	pg := page.New(files, q.NextToken, q.MaxFiles, defaultMaxConflictFiles)
	out.NextToken = pg.Next

	for _, fm := range pg.Data {
		out.Conflicts = append(out.Conflicts, MergeConflict{
			ConflictMetadata: fm.meta, MergeHunks: limitHunks(fm.hunks, q.MaxHunks),
		})
	}

	return out, nil
}

// requestedFiles returns the merge results of the named paths, recording an
// error for each path no commit contains.
func (ev *mergeEvaluation) requestedFiles(paths []string, out *BatchDescribeMergeConflictsResult) []*fileMerge {
	var files []*fileMerge

	for _, p := range paths {
		fm, ok := ev.byPath[p]
		if !ok {
			out.Errors = append(out.Errors, ConflictError{
				ExceptionName: "FileDoesNotExistException", FilePath: p, Message: "file " + p + " not found",
			})

			continue
		}

		files = append(files, fm)
	}

	return files
}

func limitHunks(hunks []MergeHunk, limit int) []MergeHunk {
	if limit > 0 && len(hunks) > limit {
		hunks = hunks[:limit]
	}

	if hunks == nil {
		return []MergeHunk{}
	}

	return hunks
}

// DescribeMergeConflicts describes the conflict of one file, paging its hunks.
func (b *InMemoryBackend) DescribeMergeConflicts(
	repoName, destSpec, sourceSpec, mergeOption, filePath string, q MergeQuery,
) (*DescribeMergeConflictsResult, error) {
	if mergeOption == mergeOptionFastForward {
		return nil, fmt.Errorf("%w: FAST_FORWARD_MERGE has no conflicts to describe", ErrInvalidMergeOption)
	}

	if err := page.ValidateToken(q.NextToken); err != nil {
		return nil, fmt.Errorf("%w: invalid NextToken", ErrInvalidContinuationToken)
	}

	b.mu.RLock("DescribeMergeConflicts")
	defer b.mu.RUnlock()

	sourceID, destID, ev, err := b.prepareMergeLocked(repoName, sourceSpec, destSpec, q.Settings)
	if err != nil {
		return nil, err
	}

	fm, ok := ev.byPath[filePath]
	if !ok {
		return nil, fmt.Errorf("%w: file %s not found", ErrFileNotFound, filePath)
	}

	pg := page.New(fm.hunks, q.NextToken, q.MaxHunks, defaultMaxMergeHunks)

	return &DescribeMergeConflictsResult{
		SourceCommitID: sourceID, DestinationCommitID: destID, BaseCommitID: ev.baseID,
		Metadata: fm.meta, Hunks: pg.Data, NextToken: pg.Next,
	}, nil
}
