---
service: codecommit
sdk_module: aws-sdk-go-v2/service/codecommit@v1.33.10
last_audit_commit: aabde46b5
last_audit_date: 2026-07-23
overall: A            # 9 genuine bugs + 1 leak fixed this pass (see Notes); backend was already substantial
ops:
  CreateRepository: {wire: ok, errors: ok, state: ok, persist: ok}
  GetRepository: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRepository: {wire: ok, errors: ok, state: fixed, persist: ok, note: "cascades branches/commits/files/fileHistory/triggers/PRs/comments/commentReactions; comments and fileHistory were leaking past repo deletion before this pass (see Notes)"}
  ListRepositories: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetRepositories: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRepositoryDescription: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRepositoryName: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRepositoryEncryptionKey: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDefaultBranch: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateBranch: {wire: ok, errors: fixed, state: ok, persist: ok, note: "validateBranchName's BranchNameRequiredException/InvalidBranchNameException sentinels were missing from errCodeLookup (see Notes) and so were unreachable — both fell through to generic ValidationException"}
  GetBranch: {wire: ok, errors: ok, state: ok, persist: ok}
  ListBranches: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteBranch: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCommit: {wire: fixed, errors: fixed, state: fixed, persist: ok, note: "filesAdded[].blobId was hardcoded empty (fixed prior pass); this pass: filesDeleted[].blobId was omitted entirely (now the real removed blob id, matching filesAdded), and ParentCommitIdOutdatedException/ParentCommitIdRequiredException were unreachable (missing from errCodeLookup — see Notes)"}
  GetCommit: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetCommits: {wire: ok, errors: ok, state: ok, persist: ok}
  PutFile: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "blobId was hardcoded empty (fixed prior pass); this pass: File.CommitSpecifier stored branchName instead of the real commit id, so GetFile's commitId field after a PutFile returned the branch name — now the real commit id. Also never recorded fileHistory, so files written via PutFile (not CreateCommit) were invisible to ListFileCommitHistory — now recorded"}
  GetFile: {wire: ok, errors: fixed, state: ok, persist: ok, note: "not-found now FileDoesNotExistException, was RepositoryDoesNotExistException"}
  GetFolder: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteFile: {wire: fixed, errors: fixed, state: fixed, persist: ok, note: "deleting a non-existent path silently fabricated a commit before; now FileDoesNotExistException. blobId in response was hardcoded empty; now the removed file's real blob id. This pass: parentCommitId was accepted and silently ignored (documented gap) — now required and validated against the branch tip (ParentCommitIdRequiredException/ParentCommitIdOutdatedException), matching real AWS's DeleteFileInput.ParentCommitId (a required field per the SDK's validators.go). Also never recorded fileHistory — now recorded"}
  GetBlob: {wire: ok, errors: fixed, state: ok, persist: ok, note: "not-found now BlobIdDoesNotExistException, was RepositoryDoesNotExistException"}
  ListFileCommitHistory: {wire: fixed, errors: ok, state: fixed, persist: fixed, note: "revisionDag entries were raw Commit objects; real AWS's shape is FileVersion (blobId/path/commit/revisionChildren) — a real SDK client's FileVersion deserializer could not have read this response at all. Also added nextToken/maxResults pagination (was a documented deferred item) and fixed the underlying state gap: PutFile/DeleteFile never populated fileHistory (see those ops' notes), so single-file writes/deletes were invisible to this op even though it's the primary op AWS clients use to see a file's commit history"}
  CreateApprovalRuleTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  GetApprovalRuleTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteApprovalRuleTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  ListApprovalRuleTemplates: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateApprovalRuleTemplateContent: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateApprovalRuleTemplateDescription: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateApprovalRuleTemplateName: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateApprovalRuleTemplateWithRepository: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateApprovalRuleTemplateFromRepository: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchAssociateApprovalRuleTemplateWithRepositories: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchDisassociateApprovalRuleTemplateFromRepositories: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAssociatedApprovalRuleTemplatesForRepository: {wire: ok, errors: ok, state: ok, persist: ok}
  ListRepositoriesForApprovalRuleTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePullRequest: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPullRequest: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPullRequests: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePullRequestTitle: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePullRequestDescription: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePullRequestStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribePullRequestEvents: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePullRequestApprovalRule: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePullRequestApprovalRule: {wire: ok, errors: fixed, state: ok, persist: ok, note: "rule-not-found now ApprovalRuleDoesNotExistException, was RepositoryDoesNotExistException"}
  UpdatePullRequestApprovalRuleContent: {wire: ok, errors: fixed, state: ok, persist: ok, note: "rule-not-found now ApprovalRuleDoesNotExistException, was RepositoryDoesNotExistException"}
  UpdatePullRequestApprovalState: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPullRequestApprovalStates: {wire: ok, errors: ok, state: ok, persist: ok}
  EvaluatePullRequestApprovalRules: {wire: ok, errors: ok, state: ok, persist: ok}
  OverridePullRequestApprovalRules: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPullRequestOverrideState: {wire: ok, errors: ok, state: ok, persist: ok}
  MergePullRequestByFastForward: {wire: ok, errors: ok, state: ok, persist: ok}
  MergePullRequestBySquash: {wire: ok, errors: ok, state: ok, persist: ok, note: "status transition is real; content-level squash semantics are not modeled (see gaps)"}
  MergePullRequestByThreeWay: {wire: ok, errors: ok, state: ok, persist: ok, note: "status transition is real; content-level 3-way merge semantics are not modeled (see gaps)"}
  MergeBranchesByFastForward: {wire: ok, errors: ok, state: ok, persist: ok}
  MergeBranchesBySquash: {wire: partial, errors: ok, state: ok, persist: ok, note: "handler literally calls the fast-forward backend method (see gaps)"}
  MergeBranchesByThreeWay: {wire: partial, errors: ok, state: ok, persist: ok, note: "handler literally calls the fast-forward backend method (see gaps)"}
  CreateUnreferencedMergeCommit: {wire: ok, errors: ok, state: ok, persist: ok}
  GetMergeCommit: {wire: ok, errors: ok, state: ok, persist: ok}
  GetMergeConflicts: {wire: partial, errors: ok, state: n/a, persist: n/a, note: "always mergeable:true, conflicts:[] — no content-diff engine (see gaps)"}
  GetMergeOptions: {wire: ok, errors: ok, state: n/a, persist: n/a}
  DescribeMergeConflicts: {wire: fixed, errors: ok, state: fixed, persist: n/a, note: "was a disguised no-op that echoed the request and never checked the repository existed; now delegates to the same backend logic as BatchDescribeMergeConflicts with full validation"}
  BatchDescribeMergeConflicts: {wire: ok, errors: ok, state: partial, persist: n/a, note: "validates repo/params correctly; conflicts are always empty since files aren't diffed (see gaps, same root cause as GetMergeConflicts)"}
  PostCommentForComparedCommit: {wire: ok, errors: ok, state: ok, persist: ok}
  PostCommentForPullRequest: {wire: ok, errors: ok, state: ok, persist: ok}
  PostCommentReply: {wire: ok, errors: fixed, state: ok, persist: ok, note: "parent-not-found now CommentDoesNotExistException, was RepositoryDoesNotExistException"}
  GetComment: {wire: ok, errors: fixed, state: ok, persist: ok, note: "not-found now CommentDoesNotExistException, was RepositoryDoesNotExistException"}
  GetCommentReactions: {wire: ok, errors: fixed, state: ok, persist: ok}
  GetCommentsForComparedCommit: {wire: ok, errors: ok, state: ok, persist: ok}
  GetCommentsForPullRequest: {wire: ok, errors: ok, state: ok, persist: ok}
  PutCommentReaction: {wire: ok, errors: fixed, state: ok, persist: ok}
  UpdateComment: {wire: ok, errors: fixed, state: ok, persist: ok}
  DeleteCommentContent: {wire: ok, errors: fixed, state: ok, persist: ok}
  GetDifferences: {wire: fixed, errors: ok, state: ok, persist: n/a, note: "was a documented deferred item (nextToken/maxResults accepted but not enforced); now paginated via pkgs/page. Also fixed a wire-shape bug: this op is the one CodeCommit exception to lowercase pagination field names — both request and response use MaxResults/NextToken (capital), verified against the SDK's generated (de)serializers; the handler previously used lowercase and so real pagination requests/responses were silently no-ops"}
  GetRepositoryTriggers: {wire: ok, errors: ok, state: ok, persist: ok}
  PutRepositoryTriggers: {wire: ok, errors: ok, state: ok, persist: ok}
  TestRepositoryTriggers: {wire: ok, errors: ok, state: ok, persist: n/a, note: "always-succeed simulation; matches AWS's own TestRepositoryTriggers semantics (it doesn't invoke real destinations either)"}
families:
  approval_rule_template_crud: {status: ok, note: "Create/Get/Delete/List/Update* all verified against real SDK shapes"}
  pull_request_lifecycle: {status: ok, note: "create/list/get/update/status/events verified"}
  pull_request_approval: {status: ok, note: "rules, states, overrides, evaluation all mutate real backend state; 2 error-code fixes this pass"}
gaps:
  - "MergeBranchesBySquash/MergeBranchesByThreeWay handlers call the FastForward backend method verbatim (handler_merges.go handleMergeBranchesBySquash/handleMergeBranchesByThreeWay) — the merge *result* (a new commit + branch tip update) is real, but there's no content-level distinction between the three strategies. Root cause, confirmed this pass by re-reading the file model end to end: File is stored flatly, keyed only by repoName|filePath (fileKey in store_setup.go) — there is no per-branch or per-commit file tree at all, so there is no 'source branch version' vs 'destination branch version' of a file to even diff, let alone merge. Implementing real 3-way/squash merge semantics is not a bug fix but a full data-model rework (branch- or commit-scoped file trees) touching PutFile/DeleteFile/CreateCommit/GetFile/GetFolder/GetDifferences and every other file-reading op; out of scope for this pass. (bd: file follow-up)"
  - "GetMergeConflicts/BatchDescribeMergeConflicts/DescribeMergeConflicts never report a real conflict: mergeable is always true and conflicts/mergeHunks are always empty. Same root cause as the merge-strategy gap above (no per-branch file state to diff), re-confirmed this pass, not merely 'no content-diff engine' as previously stated — there is nothing to diff even in principle without a data-model change. (bd: file follow-up)"
  - "SameFileContentException/FilePathConflictsWithSubmodulePathException (ErrSameFileContent/ErrFilePathConflicts in errors.go) are declared and now correctly wired into errCodeLookup (this pass), but no backend path ever returns them — PutFile/CreateCommit never compare new content against the existing blob at a path, and submodules aren't modeled at all. Confirmed via grep: both sentinels are referenced nowhere outside their own declaration. Implementing the same-content check is plausible follow-up work (compare content on PutFile/CreateCommit's putFiles entries); submodule-path conflict detection has no submodule concept to build on. Neither is a currently-documented AWS op family in this file's ops list, so left as a noted gap rather than a fixed op. (bd: file follow-up)"
deferred: []
leaks: {status: clean, note: "no goroutines/janitors in this service; Reset/Snapshot/Restore cover all state including the 3 dirty tables (comments, files, prApprovalRules). Fixed this pass: DeleteRepository never cleaned up fileHistory[repoName], and never cascade-deleted comments (compared-commit comments by RepoName, PR comments by PRid) or their commentReactions — both are ghost-row leaks now closed (see Notes); locked by TestHandler_DeleteRepository_Cascade_FileHistory and TestHandler_DeleteRepository_Cascade_Comments."}
---

## Notes

Protocol: awsjson1.1, single POST endpoint, `X-Amz-Target: CodeCommit_20150413.<Op>`.
`RouteMatcher` checks the header prefix only (no body sniffing needed since it's a single
endpoint) — verified every op in `GetSupportedOperations()` has a `buildOps()` entry and
vice versa via `sdk_completeness_test.go`.

### Bugs fixed this pass (2026-07-12, HEAD 2ca17ef1)

1. **`DescribeMergeConflicts` was a disguised no-op** (`handler_ops.go`,
   `handleDescribeMergeConflicts`). It unmarshaled the request and echoed
   `destinationCommitSpecifier`/`sourceCommitSpecifier`/`filePath` straight back into the
   response with hardcoded `mergeHunks: []` and `numberOfConflicts: 0` — it never called
   into the backend, never validated required fields, and never checked the repository
   existed (a request against a nonexistent repository returned 200 OK). Its sibling
   `BatchDescribeMergeConflicts` did all of this correctly. Fixed by validating the same
   required fields (mirroring the batch handler) and delegating to
   `Backend.BatchDescribeMergeConflicts` with a single-element `filePaths`, translating the
   first (only) conflict entry into the single-file response shape.

2. **`PutFile`/`DeleteFile`/`CreateCommit` responses hardcoded `blobId: ""`** even though
   the backend generates and stores a real blob ID for every file write
   (`backend.go`'s `applyFileChanges`, `backend_ops.go`'s `PutFile`). AWS's
   `PutFileOutput.BlobId`, `DeleteFileOutput.BlobId`, and
   `CreateCommitOutput.filesAdded[].blobId` are all **required** response fields (verified
   against `aws-sdk-go-v2/service/codecommit@v1.33.10`'s generated types) — a client that
   calls `PutFile` then immediately `GetBlob(blobId)` (a common pattern to verify what was
   written) would get `BlobIdDoesNotExistException` against this emulator before this fix.
   `PutFile`/`DeleteFile`/`CreateCommit`/`applyFileChanges` now all return the real blob
   ID(s) they generate, and the handlers thread them into the response.

3. **`DeleteFile` never checked the file existed.** Deleting a path that was never added
   silently fabricated a "delete" commit and updated the branch tip, instead of returning
   AWS's `FileDoesNotExistException`. Fixed: `DeleteFile` now looks up the file first and
   returns `ErrFileNotFound` if absent (also lets it recover the real blob ID for the
   response — see #2).

4. **Six "not found" error paths returned the wrong AWS exception type.** `GetFile`,
   `GetBlob`, all 6 comment ops (`GetComment`, `GetCommentReactions`, `PostCommentReply`,
   `PutCommentReaction`, `UpdateComment`, `DeleteCommentContent`), and the 2 PR-approval-rule
   ops (`DeletePullRequestApprovalRule`, `UpdatePullRequestApprovalRuleContent`) all reused
   the generic `ErrNotFound` sentinel, which `handleError` maps to
   `RepositoryDoesNotExistException` — so e.g. `GetFile` on a missing path inside an
   *existing* repository returned "repository not found", which is both the wrong
   exception type (breaks SDK-side `errors.As`/type-switch handling) and a misleading
   message. Added dedicated sentinels (`ErrFileNotFound` →
   `FileDoesNotExistException`, `ErrBlobNotFound` → `BlobIdDoesNotExistException`,
   `ErrCommentNotFound` → `CommentDoesNotExistException`, `ErrApprovalRuleNotFound` →
   `ApprovalRuleDoesNotExistException`), all verified against real exception type names in
   `aws-sdk-go-v2/service/codecommit/types/errors.go`. `handleError`'s dispatch was
   refactored from an 18-branch `switch` (tripped `cyclop`) into a table
   (`errCodeLookup`) so future sentinel additions don't grow its cyclomatic complexity.

### Bugs fixed this pass (2026-07-23, HEAD aabde46b5)

No commits touched `services/codecommit/` between the prior audit (`2ca17ef1`) and this
one (`git log 2ca17ef1..HEAD -- services/codecommit/` is empty), so the prior pass's "ok"
entries were re-verified rather than re-derived from scratch; the bugs below are new
findings from field-diffing the file/commit/merge-conflict/pagination surface against
`aws-sdk-go-v2/service/codecommit@v1.33.10`'s generated (de)serializers.

1. **`ListFileCommitHistory`'s `revisionDag` was the wrong wire shape entirely.** Each
   entry was a flattened `Commit` map (`commitId`/`treeId`/`message`/...) instead of AWS's
   `FileVersion` shape (`blobId`/`path`/`commit`/`revisionChildren`, verified against
   `awsAwsjson11_deserializeDocumentFileVersion` in the SDK's `deserializers.go`) — a real
   SDK client's `FileVersion` deserializer would find no nested `commit` object and no
   `blobId`/`path` at all. Fixed by having the backend return `[]FileVersionEntry`
   (`models.go`) built from `fileHistory`, with `revisionChildren` computed as a linear
   chain (this backend has no branch-aware file versioning — see the `gaps` entry on merge
   conflicts for why) over the *unpaginated* history so a page boundary never truncates a
   still-valid child reference.

2. **`PutFile`/`DeleteFile` never recorded `fileHistory` at all** — only `CreateCommit`'s
   `applyFileChanges` did. Since `PutFile` is the primary single-file write path, any file
   added that way was invisible to `ListFileCommitHistory` when queried by `filePath`
   (silently fell through to the always-empty-history branch). Fixed: both ops now call the
   same `recordFileHistory` helper `CreateCommit` uses; `fileHistory`'s value type changed
   from `[]string` (bare commit IDs) to `[]FileHistoryEntry` (commit ID + blob ID pairs, the
   blob ID needed for bug #1's `FileVersion.BlobId`) — `codecommitSnapshotVersion` bumped
   `1 -> 2` since this changes a persisted table's value shape.

3. **`PutFile` stored the branch name, not the commit ID, in `File.CommitSpecifier`.**
   `GetFile`'s `commitId` response field is documented as "the full commit ID of the commit
   that contains the content" — after a `PutFile("repo", "main", ...)`, `GetFile` returned
   `"main"` in that field instead of a real commit ID. `CreateCommit`'s `applyFileChanges`
   already did this correctly (`CommitSpecifier: commitID`); `PutFile` now generates its
   commit ID before constructing the `File` row and uses it the same way.

4. **`GetDifferences` used lowercase pagination field names; AWS uses capitalized ones for
   this one op.** Verified against the SDK's generated
   `awsAwsjson11_serializeOpDocumentGetDifferencesInput` /
   `awsAwsjson11_deserializeOpDocumentGetDifferencesOutput`: `MaxResults`/`NextToken` (both
   request and response), unlike every other paginated op in this service which uses
   lowercase `maxResults`/`nextToken`. Since the handler used lowercase, a real SDK client's
   pagination requests were silent no-ops (always the first page). Fixed the field names and
   implemented real pagination via `pkgs/page` (closing the `GetDifferences pagination`
   deferred item); `ListFileCommitHistory` pagination (also previously deferred) was
   implemented the same way with the correct lowercase names for that op.

5. **`DeleteFile` accepted and silently ignored `parentCommitId`** (a documented gap).
   Real AWS's `DeleteFileInput.ParentCommitId` is a **required** field (verified via the
   SDK's `validateOpDeleteFileInput` in `validators.go`, which client-side rejects a nil
   value before ever sending a request) and must be the branch's current HEAD. Fixed:
   `DeleteFile` now returns `ParentCommitIdRequiredException` when empty and
   `ParentCommitIdOutdatedException` when non-empty but stale, mirroring the check
   `CreateCommit` already performs for its own `parentCommitId`.

6. **`CreateCommit`'s `filesDeleted` entries never carried a `blobId`**, unlike
   `filesAdded` (fixed in the prior pass). `FileMetadata.BlobId` (the type both arrays use)
   is optional but informative — mirroring the fix already applied to the standalone
   `DeleteFile` op (which does report the removed blob), `applyFileChanges` now also
   returns the blob ID each `deleteFiles` entry removed, and the handler threads it through.

7. **Six error sentinels were declared but missing from `errCodeLookup`, making them
   unreachable.** `ErrBranchNameRequired`, `ErrInvalidBranchName` (both actively returned by
   `validateBranchName`, used by `CreateBranch`), `ErrParentCommitIDRequired`,
   `ErrParentCommitIDOutdated` (returned by `CreateCommit`, and now `DeleteFile` — see #5),
   and the still-unused `ErrSameFileContent`/`ErrFilePathConflicts` (see `gaps`) were all
   absent from the table `handleError` looks up in `handler.go`. Every error using one of
   the four *active* sentinels fell through to a generic 400 `ValidationException` instead
   of its real, SDK-matching exception name — meaning `CreateCommit` with a stale
   `parentCommitId` has been returning the wrong exception type since before this pass, an
   `errCodeLookup` gap the prior pass's own refactor (which introduced the table) didn't
   catch because nothing exercised that path. All six now map to their real AWS exception
   name (still 400, matching this table's existing all-client-errors-are-400 convention).

8. **`DeleteRepository` leaked `fileHistory[repoName]` and every comment (+ reactions)
   belonging to the repository.** `fileHistory` was never touched by the cascade at all.
   Comments have no secondary index (`comments` is a "dirty" table — see the trap below) and
   `GetComment(commentId)` does a pure by-ID lookup with no repository check, so a comment
   survived its repository's deletion as a permanently-reachable ghost row; the same was
   true of pull-request comments when their PR was cascade-deleted. Fixed:
   `deleteCommentsForRepo`/`deleteCommentsForPR` helpers (`repositories.go`) sweep
   `comments`/`commentReactions` by `RepoName`/`PRid`, and `fileHistory[repoName]` is now
   `delete()`-d in the same cascade. Locked by
   `TestHandler_DeleteRepository_Cascade_{Comments,FileHistory}`.

### Traps for the next auditor

- `MergeBranchesBySquash`/`MergeBranchesByThreeWay` and the analogous
  `MergePullRequestBySquash`/`MergePullRequestByThreeWay` **look like** they implement
  distinct merge strategies but don't — this is a known, documented gap (see `gaps` above),
  not something introduced this pass. The root cause is now precisely identified (not just
  "no diff engine"): `File` has no per-branch or per-commit identity at all (flat
  `repoName|filePath` key, see `fileKey`), so there is no second version of a file to diff
  against in the first place. Don't re-flag without also proposing the branch/commit-scoped
  file-tree rework needed to fix it for real (large feature, not a bug fix).
- `BatchDescribeMergeConflicts`'s own doc comment says "stub implementation" — that refers
  to the fact it never finds real conflicts (no content diffing), not that it's unwired;
  it does validate real backend state (repo existence) and is exercised correctly by both
  `BatchDescribeMergeConflicts` and `DescribeMergeConflicts`.
- `TestRepositoryTriggers` always reporting every trigger as a `successfulExecution` is
  *correct* emulator behavior, matching real AWS (which also doesn't actually invoke SNS
  and always reports success in the common case) — do not "fix" this into a stub-detector
  false positive.
- Comment/File/PullRequestApprovalRule tables are the 3 "dirty" tables (not on
  `b.registry`) persisted via DTOs in `persistence.go` — if you add a field to `Comment`,
  `File`, or `PullRequestApprovalRule`, you must also update the matching DTO
  (`commentSnapshot`/`fileSnapshot`/`prApprovalRuleSnapshot`) or it silently won't persist.
- `fileHistory`'s value type is `[]FileHistoryEntry` (commit ID + blob ID), not the bare
  `[]string` of commit IDs it was before this pass — if you touch it again, remember
  `codecommitSnapshotVersion` must bump whenever its shape changes again (see the comment on
  the constant in `persistence.go`).
- `errCodeLookup` (`handler.go`) is checked by test coverage now (#7 above added tests for
  the four previously-unreachable-but-active entries), but there's no automated check that
  every sentinel declared in `errors.go` has a table entry — a new `awserr.New(...)`
  sentinel with no matching `errCodeLookup` row will silently fall through to generic
  `ValidationException` again. Diff `errors.go`'s `Err*` declarations against
  `errCodeLookup`'s `sentinel:` entries by hand if you add or suspect one.
