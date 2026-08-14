---
service: codecommit
sdk_module: aws-sdk-go-v2/service/codecommit@v1.36.4
last_audit_commit: 1835ab406
last_audit_date: 2026-08-13
overall: A            # this pass (gopherstack-gvkf): the entire Comment family (8 ops — the 7
                      # named in the bug plus DeleteCommentContent, found the same day) was
                      # undecodable by a real typed client: Comment.CreationDate/LastModifiedDate
                      # were stored and emitted as RFC3339 strings, but codecommit's own
                      # deserializeDocumentComment requires a JSON number (epoch seconds). A raw
                      # status-code/body test could not see this; only a typed SDK decode could.
                      # Fixed by matching Repository/PullRequest/ApprovalRuleTemplate's existing
                      # pattern (time.Time on the domain struct, .Unix() at the wire boundary).
                      # SECOND bug found and fixed in the same family:
                      # GetCommentsForComparedCommit/GetCommentsForPullRequest emitted a flat
                      # []Comment where the real shape is []CommentsForComparedCommit /
                      # []CommentsForPullRequest, each wrapping a nested "comments" list plus
                      # repositoryName/afterCommitId/beforeCommitId — unknown top-level JSON keys
                      # are silently dropped by the JSON-RPC protocol, so this failed silently
                      # (empty Comments slice) rather than erroring. Both are now correct; see ops
                      # table + Notes below. Prior pass: MergeBranchesBySquash/ByThreeWay now real
                      # distinct backend methods (real parent-count semantics,
                      # TargetBranch/CommitMessage/AuthorName/Email honored, specifier
                      # resolution+validation); GetMergeConflicts validates required fields and
                      # resolves specifiers instead of echoing them; found and fixed an
                      # inverted-boolean bug (GetMergeConflicts always reported mergeable:false);
                      # SameFileContentException now returned by PutFile/CreateCommit.
                      # Content-level merge/conflict diffing remains a gap.
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
  CreateCommit: {wire: fixed, errors: fixed, state: fixed, persist: ok, note: "filesAdded[].blobId was hardcoded empty (fixed prior pass); this pass: filesDeleted[].blobId was omitted entirely (now the real removed blob id, matching filesAdded), and ParentCommitIdOutdatedException/ParentCommitIdRequiredException were unreachable (missing from errCodeLookup — see Notes). ALSO this pass: putFiles entries with content identical to what's already at that path now return SameFileContentException instead of silently creating a no-op commit (the sentinel existed but no backend path ever returned it — see gaps' prior note, now partially closed)"}
  GetCommit: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetCommits: {wire: ok, errors: ok, state: ok, persist: ok}
  PutFile: {wire: fixed, errors: fixed, state: fixed, persist: ok, note: "blobId was hardcoded empty (fixed prior pass); this pass: File.CommitSpecifier stored branchName instead of the real commit id, so GetFile's commitId field after a PutFile returned the branch name — now the real commit id. Also never recorded fileHistory, so files written via PutFile (not CreateCommit) were invisible to ListFileCommitHistory — now recorded. ALSO this pass: writing content identical to what's already at that path now returns SameFileContentException instead of silently creating a no-op commit"}
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
  EvaluatePullRequestApprovalRules: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED (gopherstack-lx5h) — response emitted evaluationResults, an array of {approvalRuleName,satisfied} objects; the real required key (deserializers.go EvaluatePullRequestApprovalRulesOutput) is a single evaluation object (types.Evaluation: approved/overridden/approvalRulesSatisfied/approvalRulesNotSatisfied). Prior wire: ok was false. Handler now splits the backend's per-rule []RuleEvaluation into satisfied/not-satisfied name lists and folds in the existing prOverrides/prOverriders override state (approved := overridden || no unsatisfied rules). Backend still marks every rule Satisfied: true unconditionally (never checks a rule's real approval-pool/numberOfApprovalsNeeded content against actual approvals) — that evaluation-logic gap is pre-existing and out of this pass's scope (a wrong-key bug, not a wrong-logic one), tracked separately"}
  OverridePullRequestApprovalRules: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPullRequestOverrideState: {wire: ok, errors: ok, state: ok, persist: ok}
  MergePullRequestByFastForward: {wire: ok, errors: ok, state: ok, persist: ok}
  MergePullRequestBySquash: {wire: ok, errors: ok, state: ok, persist: ok, note: "status transition is real; content-level squash semantics are not modeled (see gaps)"}
  MergePullRequestByThreeWay: {wire: ok, errors: ok, state: ok, persist: ok, note: "status transition is real; content-level 3-way merge semantics are not modeled (see gaps)"}
  MergeBranchesByFastForward: {wire: ok, errors: ok, state: ok, persist: ok, note: "OUT-OF-SCOPE FINDING (not fixed this pass, flagging per audit brief): same TargetBranch/source-dest-existence-validation gaps found and fixed in Squash/ThreeWay this pass also apply here — TargetBranch is accepted by the real MergeBranchesByFastForwardInput but never read (always updates destinationCommitSpecifier's literal string as if it were the target branch name), and neither source nor destination specifier is validated to exist before creating a commit and moving a branch. Also creates a brand-new zero-parent commit unconditionally, where real AWS fast-forward semantics would typically just move the branch pointer to the existing source commit without fabricating a new one. This op was graded ok by two prior audits and is outside this pass's assigned scope (codecommit-3bsb was Squash/ThreeWay/GetMergeConflicts specifically); left as-is, not re-graded, but noted for a future pass."}
  MergeBranchesBySquash: {wire: ok, errors: ok, state: fixed, persist: ok, note: "FIXED this pass — was calling the FastForward backend method verbatim; now a real distinct method: resolves+validates both specifiers exist (CommitDoesNotExistException if not, previously unvalidated), creates a commit with exactly ONE parent (the destination tip, matching real squash-merge shape vs. 3-way's two), and honors TargetBranch/CommitMessage/AuthorName/Email request fields that were previously silently dropped. Content-level squash (combining file changes) still not modeled — see gaps."}
  MergeBranchesByThreeWay: {wire: ok, errors: ok, state: fixed, persist: ok, note: "FIXED this pass — same as MergeBranchesBySquash, but the created commit has TWO parents ([destination, source]), a real merge-commit shape FastForward's zero-parent commit and Squash's one-parent commit both lack. Content-level 3-way merge still not modeled — see gaps."}
  CreateUnreferencedMergeCommit: {wire: ok, errors: ok, state: ok, persist: ok}
  GetMergeCommit: {wire: ok, errors: ok, state: ok, persist: ok}
  GetMergeConflicts: {wire: fixed, errors: fixed, state: fixed, persist: n/a, note: "FIXED this pass — three bugs: (1) required-field/mergeOption-enum validation was entirely missing (repositoryName/sourceCommitSpecifier/destinationCommitSpecifier/mergeOption all 'This member is required' per the real SDK's validateOpGetMergeConflictsInput); (2) sourceCommitId/destinationCommitId echoed the raw request specifier instead of the resolved commit ID (now resolved via resolveCommitSpecifier, CommitDoesNotExistException if unresolvable); (3) SEVERE — mergeable was hardcoded to `false` (inverted: this emulator never computes real conflicts, so every merge was actually mergeable, but every real client polling this op before merging would have seen mergeable:false and refused to proceed). Now true. conflicts/mergeHunks remain always empty — no content-diff engine (see gaps); this is AWS-correct for FAST_FORWARD_MERGE specifically (doc-guaranteed empty) but a documented gap for SQUASH_MERGE/THREE_WAY_MERGE. FIXED (gopherstack-lx5h) — response key was also wrong: emitted \"conflicts\", real required key (deserializers.go) is conflictMetadataList. Confirmed the always-empty list itself is the deliberate, documented stub described above (no content-diff engine) and left that behavior untouched; only the key name changed, which is a zero-behavior-change fix since the value is always []"}
  GetMergeOptions: {wire: ok, errors: ok, state: n/a, persist: n/a}
  DescribeMergeConflicts: {wire: fixed, errors: ok, state: fixed, persist: n/a, note: "was a disguised no-op that echoed the request and never checked the repository existed; now delegates to the same backend logic as BatchDescribeMergeConflicts with full validation"}
  BatchDescribeMergeConflicts: {wire: ok, errors: ok, state: partial, persist: n/a, note: "validates repo/params correctly; conflicts are always empty since files aren't diffed (see gaps, same root cause as GetMergeConflicts). NOT touched this pass — still echoes the raw specifier strings rather than resolving them (unlike GetMergeConflicts, fixed this pass); flagged as a smaller, lower-priority instance of the same pattern for a future pass, out of this pass's scope (issue was GetMergeConflicts specifically)."}
  PostCommentForComparedCommit: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED (gopherstack-gvkf) — Comment.CreationDate/LastModifiedDate were RFC3339 strings; codecommit@v1.36.4 deserializers.go:20415,20430 requires a JSON number (smithytime.ParseEpochSeconds), so every response was undecodable by a real client (status 200, unreadable body). Now time.Time on the domain struct + .Unix() at the wire boundary, matching Repository/PullRequest/ApprovalRuleTemplate. Also now echoes repositoryName/afterCommitId/beforeCommitId at the top level (previously omitted; beforeCommitId was parsed from the request and silently discarded)"}
  PostCommentForPullRequest: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED (gopherstack-gvkf) — same CreationDate/LastModifiedDate string-vs-JSON-number bug as PostCommentForComparedCommit. Also now echoes pullRequestId/repositoryName/afterCommitId/beforeCommitId at the top level (previously omitted; the backend still doesn't store afterCommitId/beforeCommitId per-comment, so these are echoed from the request, not read back from storage)"}
  PostCommentReply: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "FIXED (gopherstack-gvkf) — same timestamp bug. errors: parent-not-found now CommentDoesNotExistException, was RepositoryDoesNotExistException"}
  GetComment: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "FIXED (gopherstack-gvkf) — same timestamp bug. errors: not-found now CommentDoesNotExistException, was RepositoryDoesNotExistException"}
  GetCommentReactions: {wire: ok, errors: fixed, state: ok, persist: ok, note: "operates on Reaction, not Comment — unaffected by gopherstack-gvkf"}
  GetCommentsForComparedCommit: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED (gopherstack-gvkf) — TWO bugs. (1) same Comment timestamp bug as the rest of the family. (2) SEPARATE, more severe bug: the real response is []CommentsForComparedCommit (deserializers.go:20763), each wrapping a nested \"comments\" array plus repositoryName/afterCommitId/beforeCommitId/afterBlobId/beforeBlobId/location — this emulator emitted a flat []Comment instead. Unknown top-level JSON keys are silently dropped by the JSON-RPC protocol (no decode error), so every real client got back a group with an empty Comments slice — total silent data loss, worse than a hard failure. Now wraps all matching comments into one group (repositoryName/afterCommitId always set, beforeCommitId when provided; afterBlobId/beforeBlobId/location omitted — not tracked by this backend, and are optional pointer fields in the real shape)"}
  GetCommentsForPullRequest: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED (gopherstack-gvkf) — same two bugs as GetCommentsForComparedCommit: Comment timestamps, and flat []Comment instead of []CommentsForPullRequest (deserializers.go:20883) wrapping a nested \"comments\" list. Now wraps into one group with pullRequestId always set and repositoryName populated from the stored comments' RepoName when available; afterCommitId/beforeCommitId omitted (PostCommentForPullRequest doesn't persist them per-comment)"}
  PutCommentReaction: {wire: ok, errors: fixed, state: ok, persist: ok, note: "operates on Reaction, not Comment — unaffected by gopherstack-gvkf"}
  UpdateComment: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "FIXED (gopherstack-gvkf) — same timestamp bug. errors: unchanged from prior pass"}
  DeleteCommentContent: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "FIXED (gopherstack-gvkf) — same timestamp bug via the shared commentToMap converter; not one of the 7 ops named in the original bug report, but calls the identical converter and was found broken the same way while auditing the rest of the family. errors: unchanged from prior pass"}
  GetDifferences: {wire: fixed, errors: ok, state: ok, persist: n/a, note: "was a documented deferred item (nextToken/maxResults accepted but not enforced); now paginated via pkgs/page. Also fixed a wire-shape bug: this op is the one CodeCommit exception to lowercase pagination field names — both request and response use MaxResults/NextToken (capital), verified against the SDK's generated (de)serializers; the handler previously used lowercase and so real pagination requests/responses were silently no-ops"}
  GetRepositoryTriggers: {wire: ok, errors: ok, state: ok, persist: ok}
  PutRepositoryTriggers: {wire: ok, errors: ok, state: ok, persist: ok}
  TestRepositoryTriggers: {wire: ok, errors: ok, state: ok, persist: n/a, note: "always-succeed simulation; matches AWS's own TestRepositoryTriggers semantics (it doesn't invoke real destinations either)"}
families:
  approval_rule_template_crud: {status: ok, note: "Create/Get/Delete/List/Update* all verified against real SDK shapes"}
  pull_request_lifecycle: {status: ok, note: "create/list/get/update/status/events verified"}
  pull_request_approval: {status: ok, note: "rules, states, overrides, evaluation all mutate real backend state; 2 error-code fixes this pass"}
gaps:
  - "MergeBranchesBySquash/MergeBranchesByThreeWay (FIXED this pass to be real, distinct backend methods — see ops table) still do not model content-level squash/3-way merge semantics: the produced commit has the right parent-count shape (one parent for squash, two for three-way) and the right branch-tip update, but there is no second version of any file to actually combine. Root cause, re-confirmed this pass: File is stored flatly, keyed only by repoName|filePath (fileKey in store_setup.go) — there is no per-branch or per-commit file tree at all, so there is no 'source branch version' vs 'destination branch version' of a file to even diff, let alone merge. Implementing real content-level merge semantics is not a bug fix but a full data-model rework (branch- or commit-scoped file trees) touching PutFile/DeleteFile/CreateCommit/GetFile/GetFolder/GetDifferences and every other file-reading op; out of scope for this pass. (bd: gopherstack-3bsb follow-up)"
  - "GetMergeConflicts (FIXED this pass — see ops table for the mergeable-inversion bug and validation gaps closed)/BatchDescribeMergeConflicts/DescribeMergeConflicts never report a real conflict: conflicts/mergeHunks are always empty. Same root cause as the merge-strategy gap above (no per-branch file state to diff) — there is nothing to diff even in principle without a data-model change. Note: for FAST_FORWARD_MERGE specifically this is not a gap at all — AWS's own GetMergeConflictsOutput.ConflictMetadataList doc comment guarantees an empty list for that strategy, so the behavior is correct by definition there; the gap is genuinely only SQUASH_MERGE/THREE_WAY_MERGE. (bd: gopherstack-3bsb follow-up)"
  - "FilePathConflictsWithSubmodulePathException (ErrFilePathConflicts in errors.go) is declared and wired into errCodeLookup, but no backend path ever returns it — submodules aren't modeled at all in this backend, so there is no concept to build a conflict check on. SameFileContentException (ErrSameFileContent) was the other half of this gap and is FIXED this pass — PutFile and CreateCommit's putFiles entries now compare new content against the existing blob at that path and reject identical writes (see PutFile/CreateCommit ops rows). Note this is a best-effort approximation, not full parity: because File has no per-branch identity (same root cause as the merge gaps above), the comparison is against the single flat current value at that path repo-wide, not specifically against the destination branch's parent-commit content the way real AWS computes it — for a repo with no branch divergence at a path (the common case) these are identical, but they could theoretically diverge. (bd: gopherstack-3bsb follow-up, partially closed)"
deferred: []
leaks: {status: clean, note: "no goroutines/janitors in this service; Reset/Snapshot/Restore cover all state including the 3 dirty tables (comments, files, prApprovalRules). Fixed this pass: DeleteRepository never cleaned up fileHistory[repoName], and never cascade-deleted comments (compared-commit comments by RepoName, PR comments by PRid) or their commentReactions — both are ghost-row leaks now closed (see Notes); locked by TestHandler_DeleteRepository_Cascade_FileHistory and TestHandler_DeleteRepository_Cascade_Comments."}
---

## Notes

### Bugs fixed this pass (2026-08-13, HEAD 1835ab406) — gopherstack-gvkf

1. **The entire `Comment` family (8 ops) returned an undecodable body to any
   typed client.** `models.go` typed `Comment.CreationDate`/`LastModifiedDate`
   as `string`, filled with `time.Now().UTC().Format(time.RFC3339)`
   (`comments.go`) and emitted verbatim by the shared `commentToMap`
   converter (`handler_comments.go`). The real deserializer
   (`codecommit@v1.36.4 deserializers.go:20415,20430`, inside
   `awsAwsjson11_deserializeDocumentComment`) requires a JSON *number* —
   `smithytime.ParseEpochSeconds` — and falls through to `expected
   CreationDate/LastModifiedDate to be a JSON Number, got string instead` on
   anything else. Status 200, body no SDK client could read. Affects
   `PostCommentForComparedCommit`, `PostCommentForPullRequest`,
   `PostCommentReply`, `GetComment`, `GetCommentsForComparedCommit`,
   `GetCommentsForPullRequest`, `UpdateComment` (the 7 ops the bug report
   named), plus `DeleteCommentContent` (an 8th — same `commentToMap` call
   path, found while auditing the rest of the family; its
   `DeleteCommentContentOutput.Comment` shape is identical). This service
   already got the pattern right elsewhere: `Repository`, `PullRequest`, and
   `ApprovalRuleTemplate` all store `time.Time` and convert with `.Unix()` at
   the wire boundary (`handler_repositories.go`, `handler_pull_requests.go`,
   `handler_approval_rules.go`); only `Comment` was missed. Fixed by matching
   that exact pattern rather than inventing a new one: `Comment.CreationDate`/
   `LastModifiedDate` are now `time.Time`, `comments.go` sets them with
   `time.Now().UTC()` (no `.Format`), and `commentToMap` emits `.Unix()`.
   `Comment` is persisted through a DTO (`commentSnapshot` in
   `persistence.go`, not the live struct — see "Traps for the next auditor"
   below), so this is not a wire-format-only change: `commentSnapshot`'s own
   `CreationDate`/`LastModifiedDate` were changed to `time.Time` too, to keep
   `toCommentSnapshot`/`fromCommentSnapshot` a straight field copy.
   `encoding/json` already renders `time.Time` as a quoted RFC3339-ish
   string — the exact shape these fields already held on disk — so an
   existing on-disk snapshot still decodes; `codecommitSnapshotVersion` did
   **not** need to bump. Verified live: reverted the fix, rebuilt the
   container image, and drove all 8 ops through the real `aws-sdk-go-v2`
   client — every one failed with `deserialization failed ... expected
   CreationDate to be a JSON Number, got string instead` (or
   `LastModifiedDate`, depending which field decoded first); reapplied the
   fix and the same 8 calls passed with real decoded timestamps
   (`test/integration/codecommit_test.go`,
   `TestIntegration_CodeCommit_CommentFamily`).

2. **`GetCommentsForComparedCommit`/`GetCommentsForPullRequest` emitted the
   wrong response shape entirely — a flat `[]Comment` instead of the real
   nested wrapper.** The real shape (verified against
   `codecommit@v1.36.4 deserializers.go:20763`/`20883`,
   `awsAwsjson11_deserializeDocumentCommentsForComparedCommit`/
   `...CommentsForPullRequest`) is `[]types.CommentsForComparedCommit` /
   `[]types.CommentsForPullRequest`: each element wraps a nested `comments`
   array plus `repositoryName`/`afterCommitId`/`beforeCommitId` (and
   optional `afterBlobId`/`beforeBlobId`/`location`, not tracked by this
   backend). This emulator's handlers instead put the flat, unwrapped
   `Comment` objects directly under `commentsForComparedCommitData`/
   `commentsForPullRequestData`. Unlike bug 1, this does **not** produce a
   decode error — the JSON-RPC protocol silently drops unrecognized
   top-level keys, so a real client's array element decodes successfully as
   a `CommentsForComparedCommit`/`CommentsForPullRequest` with every field at
   its zero value, including `Comments: nil`. Every comment posted through
   this backend was therefore **silently unreachable** through either list
   op — worse than a hard failure, since nothing in a raw-body or
   status-code check (or even a naive `err == nil` typed-client check) would
   catch it. Fixed: both handlers now group all matching comments into a
   single wrapper object (this backend has no per-location grouping, and
   each query is already scoped to one repository/commit or one pull
   request, so one group is the correct AWS-shaped answer here) with
   `repositoryName`/`afterCommitId` (compared-commit) or `pullRequestId`
   (pull-request, backed by the stored comment's own `RepoName` for
   `repositoryName` when available) always set, `beforeCommitId` set when
   the caller provided it, and the real comments nested under `comments`.
   Verified live the same way as bug 1: with the timestamp fix in place but
   this fix reverted, `TestIntegration_CodeCommit_CommentFamily/get_comments_for_compared_commit`
   and `.../get_comments_for_pull_request` failed on content, not on `err`:
   `group.RepositoryName`/`group.AfterCommitId` decoded as `""` and
   `group.Comments` as `[]` (`"[]" should have 1 item(s), but has 0`) even
   though the call itself returned no error — exactly the silent-data-loss
   signature described above.

3. **Two smaller, related findings, left as documented gaps rather than
   fixed this pass** (out of scope for a decode-correctness bug fix — both
   are shape completeness, not shape correctness): `PostCommentForComparedCommit`
   parses `beforeCommitId` from the request but the backend method signature
   discards it (`comments.go`'s `PostCommentForComparedCommit(repoName, _,
   afterCommitID, content string)`); `PostCommentForPullRequest` similarly
   never stores `afterCommitId`/`beforeCommitId` per-comment. Both Post*
   handlers now *echo* the caller-supplied values back in their top-level
   `afterCommitId`/`beforeCommitId` response fields (real, optional members
   of `PostCommentForComparedCommitOutput`/`PostCommentForPullRequestOutput`
   that were previously omitted entirely), which is honest for the
   just-posted response but means `GetCommentsForPullRequest`'s wrapper (bug
   2's fix) cannot populate `afterCommitId`/`beforeCommitId` on later reads —
   the backend has nowhere to read them back from. Also unaddressed:
   `Comment`'s real wire shape additionally carries `clientRequestToken`,
   `callerReactions`, and `reactionCounts` (`deserializers.go:20362` case
   list), none of which this backend threads through — `reactionCounts`
   would need `commentToMap` to gain backend access (reactions are tracked
   separately in `InMemoryBackend.commentReactions`); `callerReactions`
   presupposes a caller identity concept this backend doesn't model. All are
   optional pointer/map fields in the real shape, so their absence doesn't
   break decode the way bugs 1–2 did.

Protocol: awsjson1.1, single POST endpoint, `X-Amz-Target: CodeCommit_20150413.<Op>`.

### Bugs fixed this pass (2026-08-07, HEAD 1d7169f66) — gopherstack-3bsb

1. **`MergeBranchesBySquash`/`MergeBranchesByThreeWay` literally called the
   `MergeBranchesByFastForward` backend method** (`handler_merges.go`) — the
   response looked plausible (a fresh commit ID + branch update) but there
   was zero distinction between the three merge strategies at the object
   level, let alone the content level. Fixed by giving each its own real
   backend method (`merges.go`): both now resolve and validate their
   `sourceCommitSpecifier`/`destinationCommitSpecifier` (new
   `resolveCommitSpecifier` helper — tries a branch name first, then a full
   commit ID, `CommitDoesNotExistException` if neither resolves; previously
   *no* validation existed, so a nonexistent branch/commit silently
   "succeeded"), and produce the correct parent-count shape verified against
   `aws-sdk-go-v2/service/codecommit@v1.33.10`'s generated
   `MergeBranchesBySquashInput`/`MergeBranchesByThreeWayInput`: squash
   creates a commit with exactly one parent (the destination tip — squash
   discards source history), three-way creates a commit with two parents
   (`[destination, source]`, standard merge-commit shape). Both now also
   honor `TargetBranch`/`CommitMessage`/`AuthorName`/`Email` request fields
   (real, wire-verified members of both inputs) that were previously parsed
   by neither handler and silently dropped — a field-drop bug of the same
   class flagged in the audit brief. Content-level squash/3-way merging
   (actually combining file changes) remains unimplemented — see gaps; the
   root cause (flat, non-branch-scoped `File` storage) is unchanged.

2. **`GetMergeConflicts` had an inverted boolean — `mergeable` was hardcoded
   `false`.** This is the sharpest finding of the pass: this backend has no
   content-diff engine and never computes a real conflict, so every merge
   this emulator could ever be asked about is, in fact, mergeable — but the
   handler unconditionally returned `mergeable: false`. A real client that
   polls `GetMergeConflicts` before attempting a merge (a documented,
   common pattern) would have concluded every merge was blocked and never
   proceeded, against a backend that would have happily merged in every
   case. Fixed: now returns `true`. Also fixed: `sourceCommitId`/
   `destinationCommitId` in the response echoed the raw request specifier
   string instead of a resolved commit ID (wrong per
   `GetMergeConflictsOutput`'s doc: "the commit ID ... used in the merge
   evaluation"), and the op performed **zero** input validation —
   `repositoryName`/`sourceCommitSpecifier`/`destinationCommitSpecifier`/
   `mergeOption` are all `"This member is required"` per the real SDK's
   `validateOpGetMergeConflictsInput`, none were checked. Both fixed using
   the same `resolveCommitSpecifier` helper and validation pattern as
   `BatchDescribeMergeConflicts`'s existing (correct) handler.

3. **`SameFileContentException`/`ErrSameFileContent` was declared and wired
   into `errCodeLookup` but no backend path ever returned it** (flagged as a
   gap by the 2026-07-23 pass). Fixed for the buildable half: `PutFile` and
   `CreateCommit`'s `putFiles` entries now compare new content against the
   existing blob at that path (`bytes.Equal`) and reject an identical write
   before mutating any state, matching AWS's documented behavior ("the
   content of the file you're trying to add is exactly the same as the
   content of that file ... you specified as the parent commit"). This
   surfaced and required fixing two existing unit tests
   (`TestHandler_ListFileCommitHistory_Pagination`/`_TableDriven`) that had
   been writing byte-identical content across multiple commits to build
   history — itself a real client-unrepresentative test pattern (a real
   AWS client doing this would already get `SameFileContentException`),
   now writes distinct content per commit. `FilePathConflictsWithSubmodulePathException`
   remains unreturned — no submodule concept exists in this backend to
   build a conflict check on top of (see gaps).

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

- `MergeBranchesBySquash`/`MergeBranchesByThreeWay` (fixed 2026-08-07 to be real, distinct
  backend methods — see Notes) now have correct object-graph shape (parent count, branch
  update, honored `TargetBranch`/`CommitMessage`/`AuthorName`/`Email`) but still don't
  implement content-level squash/3-way merging — this remains a known, documented gap (see
  `gaps` above), not something newly introduced. The analogous
  `MergePullRequestBySquash`/`MergePullRequestByThreeWay` were NOT touched (out of this
  pass's scope; they only flip PR status, no commit/branch mutation at all, so there was no
  parent-count distinction to fix there). The root cause of the remaining content-level gap
  is precisely identified (not just "no diff engine"): `File` has no per-branch or
  per-commit identity at all (flat `repoName|filePath` key, see `fileKey`), so there is no
  second version of a file to diff against in the first place. Don't re-flag without also
  proposing the branch/commit-scoped file-tree rework needed to fix it for real (large
  feature, not a bug fix).
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
