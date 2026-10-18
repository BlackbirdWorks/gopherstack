---
service: codegurureviewer
sdk_module: aws-sdk-go-v2/service/codegurureviewer@v1.43.4
last_audit_commit: 8d07d8213
last_audit_date: 2026-10-18
overall: B            # all 14 SDK operations are backed by real state and verified through the SDK client in-package; B until an SDK-driven integration suite exists and the open items close.
ops:
  AssociateRepository: {wire: ok, errors: ok, state: ok, persist: ok, note: "exactly one Repository member; CodeCommit repo checked against the CodeCommit emulator, S3 bucket must start with codeguru-reviewer- and exist in the S3 emulator, Bitbucket/GHES need a ConnectionArn and are stored; Associating settles to Associated after the lifecycle window; duplicate live association is ConflictException; ClientRequestToken replays"}
  CreateCodeReview: {wire: ok, errors: ok, state: ok, persist: ok, note: "RepositoryAnalysis only (as the SDK documents); runs the rule set over the real files of the CodeCommit branch or the S3 .zip; Pending until the window passes, then Completed or Failed with StateReason; name unique per backend (ConflictException); association must be Associated"}
  DescribeCodeReview: {wire: ok, errors: ok, state: ok, persist: ok, note: "Metrics only once Completed"}
  DescribeRecommendationFeedback: {wire: ok, errors: ok, state: ok, persist: ok, note: "UserId defaults to the caller principal"}
  DescribeRepositoryAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateRepository: {wire: ok, errors: ok, state: ok, persist: ok, note: "Disassociating settles to Disassociated; the repository can be associated again afterwards"}
  ListCodeReviews: {wire: ok, errors: ok, state: ok, persist: ok, note: "Type required; filters ProviderTypes, States, RepositoryNames; newest first"}
  ListRecommendationFeedback: {wire: ok, errors: ok, state: ok, persist: ok, note: "filters UserIds and RecommendationIds"}
  ListRecommendations: {wire: ok, errors: ok, state: ok, persist: ok, note: "empty until the review is Completed; ordered by file, line, rule"}
  ListRepositoryAssociations: {wire: ok, errors: ok, state: ok, persist: ok, note: "filters ProviderType, State, Name, Owner; oldest first"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  PutRecommendationFeedback: {wire: ok, errors: ok, state: ok, persist: ok, note: "an empty Reactions list clears the caller's feedback; the recommendation must exist in the review"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "only repository associations are taggable"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  Analyzer: {status: ok, note: "Five deterministic rules, table-tested positive and negative: aws-hardcoded-access-key (any text file, AKIA/ASIA ids, masked in the description), unclosed-resource (Go os.Open/Create/OpenFile/net.Dial/sql.Open without a Close within 15 lines; Python name = open() with no name.close(); Java FileInputStream/FileReader/Socket... outside try-with-resources with no .close()), err-nil-misuse (Go if err == nil returning err), sql-string-concatenation (concatenation, Sprintf, f-string, template literal, .format), todo-fixme-density (3+ TODO/FIXME in a code file). Rules map to the Security and CodeQuality analysis types; RecommendationId is a sha256 of rule, path and lines, so the same code gives the same ids."}
  Sources: {status: ok, note: "CodeCommit: GetFolderFiles of the named branch on the emulator backend. S3: GetObject of SourceCodeArtifactsObjectKey, unzipped with size and file-count limits. Unreadable source fails the review with a StateReason instead of fabricating findings."}
  Tags: {status: ok, note: "50 tags per association, keys without the aws: prefix."}
gaps: []
items_still_open:
  - "PullRequest reviews: CreateCodeReview only creates RepositoryAnalysis reviews, so ListCodeReviews Type=PullRequest is always empty; the emulator has no provider webhook that opens a pull-request review"
  - "aws-codeguru-reviewer.yml: ConfigFileState reports Present/Absent by file presence; the contents (excluded paths) are not parsed, so SuppressedLinesOfCodeCount is always 0 and PresentWithErrors is never returned"
  - "ConnectionArn of Bitbucket/GitHub Enterprise Server associations is stored, not looked up in the CodeConnections/CodeStar Connections emulators"
  - "KMSKeyDetails.KMSKeyId is stored, not looked up in the KMS emulator"
  - "Error codes per operation (NotFoundException for associations, ResourceNotFoundException for reviews, feedback and tags) are an emulator choice; the SDK documents the exception set but not which operation raises which"
  - "ResourceGroupsTaggingAPI does not expose association tags"
structural_gaps:
  - "Reviews of Bitbucket, GitHub and GitHub Enterprise Server repositories fail with an explanatory StateReason: there is no emulated source host to clone from."
  - "CodeGuru's ML detectors cannot exist here; findings come from the five documented heuristics only, and MeteredLinesOfCodeCount counts non-blank lines of recognised code files."
deferred: []
leaks: {status: clean, note: "leak_main_test.go runs goleak over the package; there are no goroutines or timers."}
---

## Notes

Protocol is REST-JSON (SigV4 signing name `codeguru-reviewer`, matcher also accepts the `api/codegurureviewer` user-agent marker). Timestamps are epoch-seconds numbers (`awstime.EpochTime`). Association ARN: `arn:aws:codeguru-reviewer:<region>:<account>:association:<uuid>`; review ARN appends `:codereview:<uuid>`.

State is time-derived: records keep a settle time and the view settles Associating/Disassociating/Pending lazily, so a snapshot restored later reads the correct state. The window is 500ms by default (`--lifecycle-codegurureviewer` / `CODEGURUREVIEWER_LIFECYCLE_DELAY`). The analysis itself runs synchronously at CreateCodeReview against the source as it was at that moment.
