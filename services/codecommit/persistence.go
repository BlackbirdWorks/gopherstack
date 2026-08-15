package codecommit

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// codecommitSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to a DTO type, a registered table's value
// type, or backendSnapshot itself would make an older snapshot unsafe to
// decode as the current shape. Restore compares this against the persisted
// value and discards (registry.ResetAll, not a partial decode) any mismatch
// -- see Restore below. This is the first version: CodeCommit had no
// persistence at all before Phase 3.3, so there is no legacy snapshot shape
// to be compatible with -- any snapshot without a matching Version (including
// one with no version field, which decodes as 0) is discarded the same way
// any other incompatible snapshot is.
//
// Bumped 1 -> 2 when fileHistory's value type changed from []string (bare
// commit IDs) to []FileHistoryEntry (commit ID + blob ID pairs), needed to
// build AWS's FileVersion shape for ListFileCommitHistory.
const codecommitSnapshotVersion = 2

// commentSnapshot, fileSnapshot, and prApprovalRuleSnapshot are DTOs used
// ONLY for Snapshot/Restore. Each mirrors its live type field for field,
// except the identity field(s) normally tagged json:"-" on the live type
// (Comment.PRid/RepoName/AfterCommitID, File.RepoName,
// PullRequestApprovalRule.PRID) are given a real JSON tag here -- marshaling
// the live type directly would silently drop them, and unmarshaling into it
// would leave them permanently empty, corrupting the flat table's composite
// key (for File/PullRequestApprovalRule) or the RepoName/PRid-based filters
// GetCommentsForComparedCommit/GetCommentsForPullRequest depend on (for
// Comment) on restore. This is the same DTO technique services/apigateway
// and services/neptune use, applied here to comments/files/prApprovalRules
// -- the three "dirty" tables from store_setup.go's registerAllTables.
//
// CreationDate/LastModifiedDate are time.Time here (not string) to match
// Comment's own field type (gopherstack-gvkf: Comment used to store RFC3339
// strings, which the real wire deserializer rejects -- it wants epoch
// seconds). encoding/json already renders time.Time as an RFC3339-ish quoted
// string, the same shape these fields held on disk before, so old snapshots
// still decode -- no codecommitSnapshotVersion bump needed.
type commentSnapshot struct {
	CreationDate     time.Time `json:"creationDate"`
	LastModifiedDate time.Time `json:"lastModifiedDate"`
	CommentID        string    `json:"commentId"`
	Content          string    `json:"content"`
	AuthorARN        string    `json:"authorArn"`
	InReplyTo        string    `json:"inReplyTo,omitempty"`
	PRid             string    `json:"prId,omitempty"`
	RepoName         string    `json:"repoName,omitempty"`
	AfterCommitID    string    `json:"afterCommitId,omitempty"`
	Deleted          bool      `json:"deleted"`
}

func commentSnapshotKeyFn(v *commentSnapshot) string { return v.CommentID }

func toCommentSnapshot(c *Comment) *commentSnapshot {
	return &commentSnapshot{
		CommentID:        c.CommentID,
		Content:          c.Content,
		AuthorARN:        c.AuthorARN,
		CreationDate:     c.CreationDate,
		LastModifiedDate: c.LastModifiedDate,
		InReplyTo:        c.InReplyTo,
		PRid:             c.PRid,
		RepoName:         c.RepoName,
		AfterCommitID:    c.AfterCommitID,
		Deleted:          c.Deleted,
	}
}

func fromCommentSnapshot(s *commentSnapshot) *Comment {
	return &Comment{
		CommentID:        s.CommentID,
		Content:          s.Content,
		AuthorARN:        s.AuthorARN,
		CreationDate:     s.CreationDate,
		LastModifiedDate: s.LastModifiedDate,
		InReplyTo:        s.InReplyTo,
		PRid:             s.PRid,
		RepoName:         s.RepoName,
		AfterCommitID:    s.AfterCommitID,
		Deleted:          s.Deleted,
	}
}

type fileSnapshot struct {
	FilePath        string `json:"filePath"`
	CommitSpecifier string `json:"commitSpecifier"`
	BlobID          string `json:"blobId"`
	FileMode        string `json:"fileMode"`
	RepoName        string `json:"repoName"`
	FileContent     []byte `json:"fileContent"`
}

func fileSnapshotKeyFn(v *fileSnapshot) string { return fileKey(v.RepoName, v.FilePath) }

func toFileSnapshot(f *File) *fileSnapshot {
	return &fileSnapshot{
		FilePath:        f.FilePath,
		CommitSpecifier: f.CommitSpecifier,
		BlobID:          f.BlobID,
		FileMode:        f.FileMode,
		FileContent:     f.FileContent,
		RepoName:        f.RepoName,
	}
}

func fromFileSnapshot(s *fileSnapshot) *File {
	return &File{
		FilePath:        s.FilePath,
		CommitSpecifier: s.CommitSpecifier,
		BlobID:          s.BlobID,
		FileMode:        s.FileMode,
		FileContent:     s.FileContent,
		RepoName:        s.RepoName,
	}
}

type prApprovalRuleSnapshot struct {
	RuleID              string `json:"approvalRuleId"`
	RuleName            string `json:"approvalRuleName"`
	ApprovalRuleContent string `json:"approvalRuleContent"`
	PRID                string `json:"prId"`
}

func prApprovalRuleSnapshotKeyFn(v *prApprovalRuleSnapshot) string {
	return prApprovalRuleKey(v.PRID, v.RuleName)
}

func toPRApprovalRuleSnapshot(r *PullRequestApprovalRule) *prApprovalRuleSnapshot {
	return &prApprovalRuleSnapshot{
		RuleID:              r.RuleID,
		RuleName:            r.RuleName,
		ApprovalRuleContent: r.ApprovalRuleContent,
		PRID:                r.PRID,
	}
}

func fromPRApprovalRuleSnapshot(s *prApprovalRuleSnapshot) *PullRequestApprovalRule {
	return &PullRequestApprovalRule{
		RuleID:              s.RuleID,
		RuleName:            s.RuleName,
		ApprovalRuleContent: s.ApprovalRuleContent,
		PRID:                s.PRID,
	}
}

// buildPersistenceDTORegistry constructs the ephemeral DTO registry used by
// both Snapshot and Restore for the three "dirty" tables (comments, files,
// prApprovalRules). It is built fresh on every call, mirroring
// services/neptune's buildPersistenceDTORegistry.
func buildPersistenceDTORegistry() (
	*store.Registry,
	*store.Table[commentSnapshot],
	*store.Table[fileSnapshot],
	*store.Table[prApprovalRuleSnapshot],
) {
	dtoReg := store.NewRegistry()
	commentDTOs := store.Register(dtoReg, "comments", store.New(commentSnapshotKeyFn))
	fileDTOs := store.Register(dtoReg, "files", store.New(fileSnapshotKeyFn))
	prApprovalRuleDTOs := store.Register(dtoReg, "prApprovalRules", store.New(prApprovalRuleSnapshotKeyFn))

	return dtoReg, commentDTOs, fileDTOs, prApprovalRuleDTOs
}

// backendSnapshot is the top-level on-disk shape for the CodeCommit backend.
//
// Tables holds one JSON-encoded array per registered table name: the five
// "clean" tables on b.registry (repositories, approvalRuleTemplates,
// branches, commits, pullRequests) plus the three DTO tables built above for
// the "dirty" ones (comments, files, prApprovalRules) -- see
// store_setup.go's registerAllTables doc for the clean/dirty split.
//
// RepoTemplateAssoc, PRApprovals, PROverrides, PROverriders, PREvents,
// CommentReactions, FileHistory, and Triggers are the plain maps left
// unconverted by Phase 3.3 (see the field comments on InMemoryBackend in
// store.go): each holds a value with no identity of its own (a set, a bare
// scalar, or a slice), so there is nothing for store.Table to key on, and
// each is persisted here directly instead. RepositoriesByARN is NOT included
// here: it is a pure reverse-lookup cache, fully rebuildable from
// Repositories, and is rebuilt by rebuildRepositoriesByARN on Restore rather
// than persisted.
type backendSnapshot struct {
	Tables            map[string]json.RawMessage               `json:"tables"`
	RepoTemplateAssoc map[string]map[string]struct{}           `json:"repoTemplateAssoc"`
	PRApprovals       map[string]map[string]string             `json:"prApprovals"`
	PROverrides       map[string]bool                          `json:"prOverrides"`
	PROverriders      map[string]string                        `json:"prOverriders"`
	PREvents          map[string][]PullRequestEvent            `json:"prEvents"`
	CommentReactions  map[string][]Reaction                    `json:"commentReactions"`
	FileHistory       map[string]map[string][]FileHistoryEntry `json:"fileHistory"`
	Triggers          map[string][]RepositoryTrigger           `json:"triggers"`
	AccountID         string                                   `json:"accountId"`
	Region            string                                   `json:"region"`
	NextPRCounter     int                                      `json:"nextPrCounter"`
	Version           int                                      `json:"version"`
}

// Snapshot serializes current state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "codecommit: snapshot table marshal failed", "error", err)

		return nil
	}

	dtoReg, commentDTOs, fileDTOs, prApprovalRuleDTOs := buildPersistenceDTORegistry()

	for _, c := range b.comments.Snapshot() {
		commentDTOs.Put(toCommentSnapshot(c))
	}
	for _, f := range b.files.Snapshot() {
		fileDTOs.Put(toFileSnapshot(f))
	}
	for _, r := range b.prApprovalRules.Snapshot() {
		prApprovalRuleDTOs.Put(toPRApprovalRuleSnapshot(r))
	}

	dtoTables, err := dtoReg.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "codecommit: snapshot DTO table marshal failed", "error", err)

		return nil
	}
	maps.Copy(tables, dtoTables)

	s := backendSnapshot{
		Version:           codecommitSnapshotVersion,
		Tables:            tables,
		RepoTemplateAssoc: b.repoTemplateAssoc,
		PRApprovals:       b.prApprovals,
		PROverrides:       b.prOverrides,
		PROverriders:      b.prOverriders,
		PREvents:          b.prEvents,
		CommentReactions:  b.commentReactions,
		FileHistory:       b.fileHistory,
		Triggers:          b.triggers,
		AccountID:         b.accountID,
		Region:            b.region,
		NextPRCounter:     b.nextPRCounter,
	}

	return persistence.MarshalSnapshot(ctx, "codecommit", s)
}

// Restore deserializes state from JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var s backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "codecommit", data, &s); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if s.Version != codecommitSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"codecommit: discarding incompatible snapshot version, starting empty",
			"gotVersion", s.Version, "wantVersion", codecommitSnapshotVersion)

		b.registry.ResetAll()
		b.prApprovalRules.Reset()
		b.comments.Reset()
		b.files.Reset()
		b.repositoriesByARN = make(map[string]string)
		b.repoTemplateAssoc = make(map[string]map[string]struct{})
		b.prApprovals = make(map[string]map[string]string)
		b.prOverrides = make(map[string]bool)
		b.prOverriders = make(map[string]string)
		b.prEvents = make(map[string][]PullRequestEvent)
		b.commentReactions = make(map[string][]Reaction)
		b.fileHistory = make(map[string]map[string][]FileHistoryEntry)
		b.triggers = make(map[string][]RepositoryTrigger)
		b.nextPRCounter = 0

		return nil
	}

	if err := b.registry.RestoreAll(s.Tables); err != nil {
		return fmt.Errorf("codecommit: restore snapshot tables: %w", err)
	}

	if err := b.restoreDirtyTables(s.Tables); err != nil {
		return err
	}

	b.restorePlainMaps(&s)

	b.accountID = s.AccountID
	b.region = s.Region
	b.nextPRCounter = s.NextPRCounter

	// Repository.Region is never part of the wire API (json:"-") and is
	// always equal to the backend's own region (see CreateRepository) --
	// never anything else -- so it is cheaper and simpler to reassign it
	// here than to carry it through a DTO wrapper.
	for _, r := range b.repositories.All() {
		r.Region = b.region
	}

	b.rebuildRepositoriesByARN()

	return nil
}

// restoreDirtyTables rebuilds the three "dirty" tables (comments, files,
// prApprovalRules; see store_setup.go's registerAllTables doc) from tables
// via the ephemeral DTO registry, factored out of Restore to keep Restore's
// own cognitive complexity low. Callers must hold b.mu.Lock.
func (b *InMemoryBackend) restoreDirtyTables(tables map[string]json.RawMessage) error {
	dtoReg, commentDTOs, fileDTOs, prApprovalRuleDTOs := buildPersistenceDTORegistry()
	if err := dtoReg.RestoreAll(tables); err != nil {
		return fmt.Errorf("codecommit: restore snapshot DTO tables: %w", err)
	}

	b.comments.Reset()
	for _, c := range commentDTOs.Snapshot() {
		b.comments.Put(fromCommentSnapshot(c))
	}

	b.files.Reset()
	for _, f := range fileDTOs.Snapshot() {
		b.files.Put(fromFileSnapshot(f))
	}

	b.prApprovalRules.Reset()
	for _, r := range prApprovalRuleDTOs.Snapshot() {
		b.prApprovalRules.Put(fromPRApprovalRuleSnapshot(r))
	}

	return nil
}

// restorePlainMaps restores every plain (non-store.Table) persisted map from
// s, defaulting each to an empty map when absent from the snapshot (e.g. an
// older snapshot predating a field, though codecommitSnapshotVersion's own
// guard makes that unreachable today). Factored out of Restore to keep
// Restore's own cognitive complexity low. Callers must hold b.mu.Lock.
func (b *InMemoryBackend) restorePlainMaps(s *backendSnapshot) {
	b.repoTemplateAssoc = s.RepoTemplateAssoc
	if b.repoTemplateAssoc == nil {
		b.repoTemplateAssoc = make(map[string]map[string]struct{})
	}

	b.prApprovals = s.PRApprovals
	if b.prApprovals == nil {
		b.prApprovals = make(map[string]map[string]string)
	}

	b.prOverrides = s.PROverrides
	if b.prOverrides == nil {
		b.prOverrides = make(map[string]bool)
	}

	b.prOverriders = s.PROverriders
	if b.prOverriders == nil {
		b.prOverriders = make(map[string]string)
	}

	b.prEvents = s.PREvents
	if b.prEvents == nil {
		b.prEvents = make(map[string][]PullRequestEvent)
	}

	b.commentReactions = s.CommentReactions
	if b.commentReactions == nil {
		b.commentReactions = make(map[string][]Reaction)
	}

	b.fileHistory = s.FileHistory
	if b.fileHistory == nil {
		b.fileHistory = make(map[string]map[string][]FileHistoryEntry)
	}

	b.triggers = s.Triggers
	if b.triggers == nil {
		b.triggers = make(map[string][]RepositoryTrigger)
	}
}

// rebuildRepositoriesByARN rebuilds the repositoriesByARN reverse-lookup
// cache from the current repositories table. Call after any bulk restore of
// repositories. Callers must hold b.mu.
func (b *InMemoryBackend) rebuildRepositoriesByARN() {
	b.repositoriesByARN = make(map[string]string)

	for _, r := range b.repositories.All() {
		b.repositoriesByARN[r.ARN] = r.RepositoryName
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
