---
service: codecommit
sdk_module: aws-sdk-go-v2/service/codecommit@v1.33.10
last_audit_commit: 2ca17ef1
last_audit_date: 2026-07-12
overall: A            # 4 genuine bugs fixed this pass (see Notes); backend was already substantial
ops:
  CreateRepository: {wire: ok, errors: ok, state: ok, persist: ok}
  GetRepository: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRepository: {wire: ok, errors: ok, state: ok, persist: ok, note: cascades branches/commits/files/triggers/PRs}
  ListRepositories: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetRepositories: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRepositoryDescription: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRepositoryName: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRepositoryEncryptionKey: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDefaultBranch: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateBranch: {wire: ok, errors: ok, state: ok, persist: ok}
  GetBranch: {wire: ok, errors: ok, state: ok, persist: ok}
  ListBranches: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteBranch: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCommit: {wire: fixed, errors: ok, state: ok, persist: ok, note: "filesAdded[].blobId was hardcoded empty; now real per-file blob id"}
  GetCommit: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetCommits: {wire: ok, errors: ok, state: ok, persist: ok}
  PutFile: {wire: fixed, errors: ok, state: ok, persist: ok, note: "blobId was hardcoded empty; now the real generated blob id, round-trips through GetBlob"}
  GetFile: {wire: ok, errors: fixed, state: ok, persist: ok, note: "not-found now FileDoesNotExistException, was RepositoryDoesNotExistException"}
  GetFolder: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteFile: {wire: fixed, errors: fixed, state: fixed, persist: ok, note: "deleting a non-existent path silently fabricated a commit before; now FileDoesNotExistException. blobId in response was hardcoded empty; now the removed file's real blob id"}
  GetBlob: {wire: ok, errors: fixed, state: ok, persist: ok, note: "not-found now BlobIdDoesNotExistException, was RepositoryDoesNotExistException"}
  ListFileCommitHistory: {wire: ok, errors: ok, state: ok, persist: ok}
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
  GetDifferences: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetRepositoryTriggers: {wire: ok, errors: ok, state: ok, persist: ok}
  PutRepositoryTriggers: {wire: ok, errors: ok, state: ok, persist: ok}
  TestRepositoryTriggers: {wire: ok, errors: ok, state: ok, persist: n/a, note: "always-succeed simulation; matches AWS's own TestRepositoryTriggers semantics (it doesn't invoke real destinations either)"}
families:
  approval_rule_template_crud: {status: ok, note: "Create/Get/Delete/List/Update* all verified against real SDK shapes"}
  pull_request_lifecycle: {status: ok, note: "create/list/get/update/status/events verified"}
  pull_request_approval: {status: ok, note: "rules, states, overrides, evaluation all mutate real backend state; 2 error-code fixes this pass"}
gaps:
  - "MergeBranchesBySquash/MergeBranchesByThreeWay handlers call the FastForward backend method verbatim (handler_ops.go handleMergeBranchesBySquash/handleMergeBranchesByThreeWay) — the merge *result* (a new commit + branch tip update) is real, but there's no content-level distinction between the three strategies. Root cause: the backend has no tree/blob diff-merge engine. Implementing real 3-way/squash merge semantics is a substantial feature, not a bug fix; out of scope for this pass. (bd: file follow-up)"
  - "GetMergeConflicts/BatchDescribeMergeConflicts/DescribeMergeConflicts never report a real conflict: mergeable is always true and conflicts/mergeHunks are always empty, because the backend has no file-content diffing. This was true before this pass (backend.go's BatchDescribeMergeConflicts doc even says so) and DescribeMergeConflicts now correctly delegates to that same (still content-blind) logic instead of being a separate, less-validated stub. (bd: file follow-up)"
  - "DeleteFile ignores parentCommitId entirely (never validates it against the branch tip), unlike CreateCommit which enforces ParentCommitIdOutdatedException. Real AWS requires parentCommitId to be the current HEAD. Low-risk since DeleteFile is typically called right after GetBranch in real clients, but a race with a concurrent commit would go undetected here. (bd: file follow-up)"
deferred:
  - GetDifferences pagination (nextToken/maxResults accepted but not enforced — returns full result set every call)
  - ListFileCommitHistory pagination
leaks: {status: clean, note: "no goroutines/janitors in this service; Reset/Snapshot/Restore cover all state including the 3 dirty tables (comments, files, prApprovalRules)"}
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

### Traps for the next auditor

- `MergeBranchesBySquash`/`MergeBranchesByThreeWay` and the analogous
  `MergePullRequestBySquash`/`MergePullRequestByThreeWay` **look like** they implement
  distinct merge strategies but don't — this is a known, documented gap (see `gaps` above),
  not something introduced this pass. Don't re-flag without also proposing the
  diff/merge-conflict engine needed to fix it for real (large feature, not a bug fix).
- `BatchDescribeMergeConflicts`'s own doc comment says "stub implementation" — that refers
  to the fact it never finds real conflicts (no content diffing), not that it's unwired;
  it does validate real backend state (repo existence) and is exercised correctly by both
  `BatchDescribeMergeConflicts` and (after this pass) `DescribeMergeConflicts`.
- `TestRepositoryTriggers` always reporting every trigger as a `successfulExecution` is
  *correct* emulator behavior, matching real AWS (which also doesn't actually invoke SNS
  and always reports success in the common case) — do not "fix" this into a stub-detector
  false positive.
- Comment/File/PullRequestApprovalRule tables are the 3 "dirty" tables (not on
  `b.registry`) persisted via DTOs in `persistence.go` — if you add a field to `Comment`,
  `File`, or `PullRequestApprovalRule`, you must also update the matching DTO
  (`commentSnapshot`/`fileSnapshot`/`prApprovalRuleSnapshot`) or it silently won't persist.
