---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: codeartifact
sdk_module: aws-sdk-go-v2/service/codeartifact@v1.38.19   # version audited against
last_audit_commit: TBD-fill-in-after-commit                # this agent doesn't run git; main thread should set this on commit
last_audit_date: 2026-07-23
overall: A            # this pass: real package-group pattern-matching algorithm implemented,
                      # readme/dependency extraction implemented (npm package.json scope),
                      # UpdatePackageGroupOriginConfiguration/ListAllowedRepositoriesForGroup made real,
                      # a domain-delete package-group leak fixed. See ops table + gaps below.
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateDomain: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeDomain: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDomain: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDomains: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — maxResults/nextToken are JSON body fields (POST), not query params, unlike every other List op; was reading query only (always empty)"}
  CreateRepository: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — request body field renamed upstreamRepositories -> upstreams (real wire key)"}
  DescribeRepository: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRepository: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — request body field upstreamRepositories -> upstreams"}
  DeleteRepository: {wire: ok, errors: ok, state: ok, persist: ok}
  ListRepositories: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — maxResults/nextToken query params are max-results/next-token (kebab), not camelCase"}
  ListRepositoriesInDomain: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — same kebab-case pagination bug"}
  GetRepositoryEndpoint: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAuthorizationToken: {wire: partial, errors: ok, state: ok, persist: n/a, note: "token is a fabricated string (codeartifact-stub-token-<domain>), not a real signed/opaque credential — acceptable for an emulator since no downstream auth check consumes it, but flagged for awareness"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDomainPermissionsPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutDomainPermissionsPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDomainPermissionsPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetRepositoryPermissionsPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutRepositoryPermissionsPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRepositoryPermissionsPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED route-matcher bug — real path is plural /v1/repository/permissions/policies, DELETE-only; was sharing the singular /v1/repository/permissions/policy path with Get/Put, which real AWS does NOT serve DELETE on"}
  AssociateExternalConnection: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — query param externalConnection -> external-connection (kebab)"}
  DisassociateExternalConnection: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — same externalConnection -> external-connection"}
  CreatePackageGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribePackageGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — query param packageGroup -> package-group (kebab); was always empty for real clients -> spurious ValidationException"}
  DeletePackageGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — same packageGroup -> package-group"}
  UpdatePackageGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "unaffected: packageGroup is a JSON body field here (matches real wire), the (wrong) query fallback was dead code for real traffic but harmless"}
  ListPackageGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED pagination casing; createdTime was missing from response (real field, was tracked but never serialized) — now added"}
  ListSubPackageGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (this pass) — real parent/child hierarchy: a group's children are every OTHER domain group whose immediate (most-specific) proper-superset pattern is exactly this one, computed via package_group_pattern.go's isProperSubsetPattern; replaced the old string-prefix heuristic. Verified direct-children-only against the ListSubPackageGroups API reference (not the full descendant subtree)."}
  GetAssociatedPackageGroup: {wire: ok, errors: ok, state: partial, persist: n/a, note: "FIXED (this pass) — real most-specific-pattern matching (package_group_pattern.go) replaces the always-nil stub; response now includes associationType. state: only AWS's 'strong match' (exact) half of the algorithm is implemented, not the case-fold/separator-equivalence 'weak match' half, and this backend does not auto-create the implicit root '/*' group every real domain has — see gaps."}
  ListAssociatedPackages: {wire: ok, errors: ok, state: partial, persist: n/a, note: "FIXED (this pass) — real domain-wide matching: for each package (deduped by format/namespace/name across repos), computes its most-specific matching group and includes it only if that group is the requested pattern. Added pagination (max-results/next-token, kebab) and the associationType field ('STRONG', see GetAssociatedPackageGroup's note on scope). state gap: Preview flag (compute association without creating the group) not read/supported."}
  ListAllowedRepositoriesForGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (this pass) — the previously-unread required originRestrictionType query param (camelCase, NOT kebab — verified against serializers.go, an exception to this service's usual kebab-case query convention) is now read/validated and used to look up the real per-restriction-type AllowedRepositories list set via UpdatePackageGroupOriginConfiguration; added pagination. FIXED missing 404: real AWS 404s when the package group doesn't exist, this op never checked."}
  UpdatePackageGroupOriginConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (this pass) — request body now real (restrictions map[type]mode, addAllowedRepositories/removeAllowedRepositories []{originRestrictionType,repositoryName}), validated against the real 4-value mode / 3-value type enums; response now returns the real allowedRepositoryUpdates map[type]map[ADDED|REMOVED][]repoName shape (verified against the API reference's response syntax) plus the updated packageGroup with a real originConfiguration.restrictions block (mode/effectiveMode/repositoriesCount/inheritedFrom, resolved by walking the pattern-hierarchy's INHERIT chain up to the nearest explicit ancestor, defaulting to ALLOW at the top like real AWS's root group). FIXED missing repository-existence check on add/remove entries."}
  DescribePackage: {wire: ok, errors: ok, state: partial, persist: ok, note: "auto-creates a stub package on first Describe if absent (pre-existing behavior, not touched this pass — see gaps); now surfaces originConfiguration when set"}
  DeletePackage: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPackages: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED pagination casing"}
  PutPackageOriginConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED disguised no-op — backend built a Package literal but never called packages.Put (state was discarded); FIXED route-matcher bug — real op has no path of its own, it is POST on the shared /v1/package path (was GET/DELETE only, PUT on a nonexistent /v1/package/origin-configuration path); FIXED response shape — real output is flat {originConfiguration:{restrictions:{publish,upstream}}}, was wrapping in {package:...} and not reading the request body's restrictions at all"}
  DescribePackageVersion: {wire: ok, errors: ok, state: partial, persist: ok, note: "FIXED wire bug — publish-time field key is publishedTime, was publishedAt (real SDK deserializer never populated PublishedTime). auto-creates a stub version on first Describe if absent (pre-existing, not touched — see gaps)"}
  ListPackageVersions: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED pagination casing"}
  PublishPackageVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED wire bug — real response is FLAT {format,namespace,package,status,version,versionRevision,asset}, was nesting under packageVersionToMap with wrong field names (packageName not package, revision not versionRevision) and no asset field; FIXED disguised no-op — the uploaded asset's raw octet-stream body was discarded (Handler() only ever attempted a JSON decode, which fails silently on binary content) and the asset query param was never read; now stores the asset (name/size/sha256/content) on the PackageVersion and GetPackageVersionAsset/ListPackageVersionAssets serve it back; FIXED missing repository-existence check (real API 404s if the repo doesn't exist, this op never checked)"}
  DeletePackageVersions: {wire: ok, errors: ok, state: ok, persist: ok}
  CopyPackageVersions: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — query params sourceRepository/destinationRepository -> source-repository/destination-repository (kebab)"}
  DisposePackageVersions: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePackageVersionsStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPackageVersionAsset: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED disguised no-op — always returned empty 200 regardless of asset name/existence; now returns real stored content or 404 for an asset that was never published"}
  GetPackageVersionReadme: {wire: ok, errors: ok, state: partial, persist: n/a, note: "FIXED (this pass) — response is now the real flat shape (format/namespace/package/readme/version/versionRevision, verified against deserializers.go), and readme is populated for real when the caller published an asset literally named package.json whose JSON content has a readme field (npm convention). state gap: without such an asset, still returns empty (this backend doesn't unpack full tarballs/POMs) — see gaps."}
  ListPackageVersionAssets: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED disguised no-op — always returned [] regardless of what was published; now lists real stored AssetSummary entries (name/size/hashes)"}
  ListPackageVersionDependencies: {wire: ok, errors: ok, state: partial, persist: n/a, note: "FIXED (this pass) — response is now the real shape (dependencies[]/format/namespace/package/version, verified against deserializers.go), and dependencies are populated for real from a published package.json's dependencies/devDependencies/peerDependencies/optionalDependencies maps (dependencyType regular/dev/peer/optional per types.PackageDependency's doc comment). state gap: same package.json-only scope as GetPackageVersionReadme — see gaps."}
families:
  route_matcher: {status: ok, note: "Audited every op's path+method against aws-sdk-go-v2 serializers.go SplitURI/request.Method. Found and fixed 5 path/method bugs: DeleteRepositoryPermissionsPolicy (wrong shared path), GetAssociatedPackageGroup (wrong path), ListAssociatedPackages (wrong path), ListSubPackageGroups (wrong path), PutPackageOriginConfiguration (wrong path — real op has none, shares POST /v1/package). All other op paths/methods verified correct."}
  query_param_casing: {status: ok, note: "Audited every op's query-string parameter names against aws-sdk-go-v2 serializers.go SetQuery(...) calls. Found and fixed a service-wide pattern: List-op pagination (maxResults/nextToken) and several other params (packageGroup, externalConnection, sourceRepository/destinationRepository) use kebab-case on the wire (max-results, next-token, package-group, external-connection, source-repository, destination-repository) but the handler read camelCase query keys — meaning pagination and several ops were silently broken for any real AWS SDK client (worked only in unit tests that construct query strings by hand). ListDomains is the sole exception: its pagination is JSON-body, not query, distinguishing it from every other List op. This pass found one more exception: ListAllowedRepositoriesForGroup's originRestrictionType is genuinely camelCase on the wire (verified against serializers.go), unlike every other param in this family."}
  package_group_pattern_matching: {status: ok, note: "NEW (this pass) — implemented AWS's package-group pattern-matching algorithm (package_group_pattern.go): pattern parsing (format[/namespace[/name]] + $/~/ * suffix), matching, word-boundary prefix matching, and the specificity/subset ordering that defines the group hierarchy (parent/child, most-specific-match). Wired into GetAssociatedPackageGroup, ListAssociatedPackages, ListSubPackageGroups, and UpdatePackageGroupOriginConfiguration's INHERIT-chain resolution. Scope: implements AWS's 'strong match' (exact) half only, not the case-fold/dash-dot-underscore-equivalence 'weak match' half used for dependency-confusion protection, and does not auto-create the implicit root '/*' group — see gaps."}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "Package-group 'weak match' (case-folding, dash/dot/underscore-equivalence, confusable-character normalization used for dependency-confusion protection) is not implemented — only the 'strong' (exact) half of AWS's matching algorithm is (see package_group_pattern_matching family note). Every match this backend reports is therefore always associationType STRONG; real AWS's WEAK association type (and its 'block the package even though a broader group would allow it' side effect) never occurs here."
  - "This backend does not auto-create the implicit root package group ('/*') that real AWS attaches to every domain and forbids deleting. Adding it would change GetAssociatedPackageGroup/ListPackageGroups behavior on a domain with zero explicitly-created groups (several existing tests assert 'no groups yet' -> empty list / no match), so it was deliberately left out this pass rather than rewriting that test surface; flagged for a future pass."
  - "DescribePackage / DescribePackageVersion auto-create a stub record when the resource doesn't exist, instead of returning ResourceNotFoundException like real AWS. This is pre-existing, intentionally-documented behavior, reconfirmed this pass to be extremely load-bearing test-seeding infrastructure (60+ call sites across handler_package_versions_test.go, handler_package_versions_assets_test.go, persistence_test.go, handler_packages_test.go use GET as a seed operation), so ripping it out remains a large, independently-scoped migration — not touched this pass either. Real behavioral divergence from AWS."
  - "GetPackageVersionReadme / ListPackageVersionDependencies now parse real content from a published package.json asset (npm convention — see the ops table), but still return empty for any format/publish that doesn't include a standalone package.json asset (e.g. a real npm tarball, a Maven POM, or any non-npm format) — this backend's single-asset-per-call publish model doesn't unpack archives."
  - "GetAuthorizationToken returns a fabricated token string rather than any real credential material; acceptable since nothing validates it downstream, but flagged in case a future op starts checking it."
  - "domain-owner / cross-account query param is accepted by real AWS on nearly every op (for cross-account domain access) but is not read anywhere in this backend; single-account-only is assumed throughout."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - "Package-group 'weak match' dependency-confusion-protection semantics (see gaps above)"
  - "Root package-group auto-creation (see gaps above)"
  - "store_setup.go was read but not modified — no bugs found, not exhaustively re-audited"
leaks: {status: clean, note: "FIXED (this pass) — DeleteDomain never cascade-deleted the domain's package groups (ghost store rows) or closed their Tags (a pkgs/tags leak), despite deleting everything else the domain owned; every other resource path (repositories/packages/versions/policies/external-connections) was already covered by pre-existing cascade logic. Re-verified: no goroutines/janitors in this service; store.Table-backed state is snapshot/restored via existing Handler.Snapshot/Restore delegation to InMemoryBackend; new Restrictions/Assets/OriginConfig fields are plain JSON-tagged struct fields and round-trip automatically."}
---

## Notes

### 2026-07-23 pass: package-group pattern matching + readme/dependency extraction + a leak fix

The prior audit (2026-07-13) explicitly deferred the package-group pattern-matching algorithm as
"a real feature, not a quick wire fix — needs its own pass." That pass happened this round:
`package_group_pattern.go` implements AWS's documented pattern syntax/matching/hierarchy algorithm
(see [Package group definition syntax and matching
behavior](https://docs.aws.amazon.com/codeartifact/latest/ug/package-group-definition-syntax-matching-behavior.html)),
including the non-obvious parts — word-boundary prefix matching (`~`), the target-position/suffix
parsing model, and the proper-subset relation that defines "most specific match" and parent/child
hierarchy. It's covered by its own white-box test file
(`package_group_pattern_test.go`, `package codeartifact` with a `//nolint:testpackage`, same
convention as `isolation_test.go`) pinning every documented example pattern plus edge cases. This
powers real (non-stub) `GetAssociatedPackageGroup`, `ListAssociatedPackages`, `ListSubPackageGroups`,
and `UpdatePackageGroupOriginConfiguration`'s INHERIT-chain resolution — see the ops table for what
changed in each. Deliberately NOT implemented: the "weak match" half of the algorithm
(case-folding/separator-equivalence used for dependency-confusion protection) and the implicit
root `/*` group every real domain has — both are real, scoped, documented gaps (see `gaps:`), not
silently dropped.

`GetPackageVersionReadme`/`ListPackageVersionDependencies` also went from permanent stubs to real
extraction from a published `package.json` asset (npm convention) — see the ops table. This is
intentionally scoped to that one file name/format rather than attempting to unpack arbitrary
tarballs/POMs, which is out of reach for this backend's single-asset-per-call publish model.

**Leak found and fixed**: `DeleteDomain`'s cascade-delete never touched package groups — every
other owned resource (repositories, packages, versions, policies, external connections) was
cleaned up, but package groups (keyed by `domainName+pattern`, not by repository, so outside the
existing per-repository cascade loop) were left behind as ghost store rows with their `Tags` never
`Close()`d. Fixed by adding an explicit package-group cascade loop to `DeleteDomain` (see
`domains.go`).

**Traps for the next auditor (new this pass)**:
- `ListAllowedRepositoriesForGroup`'s `originRestrictionType` query param is genuinely camelCase
  on the wire (verified against `serializers.go`'s `SetQuery("originRestrictionType")`), unlike
  every other package-group query param in this service (which are kebab-case). Do not "fix" this
  to kebab-case.
- The package-group hierarchy (parent/child, effective-mode inheritance) is derived purely from
  pattern structure (`isProperSubsetPattern`/`specificityRank` in `package_group_pattern.go`), not
  from any stored parent pointer — a group's parent can change implicitly when a sibling group is
  created or deleted. This matches real AWS (the hierarchy is defined by pattern specificity, not
  an explicit tree), but means `DescribeOriginInfo` re-scans the domain's groups on every call
  rather than caching a parent reference.
- `DescribePackage`/`DescribePackageVersion`'s auto-create-on-Describe divergence was
  re-investigated (not just re-asserted) this pass specifically to see if it was now safe to remove
  — it is not: 60+ existing test call sites across five files rely on `GET .../package/version` as
  their primary seeding mechanism, not `PublishPackageVersion`. Removing it is real work (rewrite
  every one of those call sites to publish first) that deserves its own pass, not a footnote in this
  one.

Protocol: **restjson1**. Timestamps are epoch-seconds JSON numbers (`awstime`-style, hand-rolled
here via `epochSeconds`), not ISO8601 strings — this was already correct except for the
`publishedAt`→`publishedTime` key-name bug (see ops table).

**The big finding this pass**: query-string parameter casing. AWS's CodeArtifact Smithy model
uses kebab-case (`max-results`, `next-token`, `package-group`, `external-connection`,
`source-repository`, `destination-repository`) for httpQuery-bound members on most ops, but this
service's handlers read camelCase (`maxResults`, `nextToken`, `packageGroup`, ...). Every unit
test in this package constructs its own query strings by hand and matched the handler's (wrong)
camelCase expectation, so the bug was invisible to `go test` — it only breaks when driven by a
real `aws-sdk-go-v2` client, exactly the trap `parity-principles.md` rule 3 warns about
("unit tests are not parity proof"). `ListDomains` is the one op where pagination is a JSON body
field instead of a query param at all — an easy thing to miss if you pattern-match against the
other List ops.

**Route-matcher bugs** (rule explicitly named in the audit brief): 5 ops had incorrect path
and/or method wiring, meaning a real SDK request would never reach the intended handler (falling
through to `opUnknown` → 404 "unknown operation", or in `PutPackageOriginConfiguration`'s case,
being silently unroutable since `/v1/package/origin-configuration` never existed in the real API
at all — it's `POST /v1/package`, sharing a path with `DescribePackage`/`DeletePackage`).
Unit tests calling `h.Handler()(c)` directly with hand-built paths did not exercise
`RouteMatcher()`/`parseCodeArtifactPath` against the real paths, so this was invisible too.

**Disguised no-ops** (rule 4 / rule 1 no-stub rule): `PutPackageOriginConfiguration`'s backend
method built a `*Package` return value but never called `b.packages.Put(...)` — every call
looked like it succeeded but the origin configuration was never actually stored, so a subsequent
`DescribePackage` would never reflect it. `PublishPackageVersion`'s asset upload was silently
discarded twice over: the HTTP layer's blanket "try to JSON-decode every body" logic errors out
(and is swallowed) on binary octet-stream content, and even if it hadn't, the handler never read
the `asset` query param or passed the body through. `GetPackageVersionAsset`/
`ListPackageVersionAssets` were pure stubs returning empty regardless of what (if anything) had
been published. All four are fixed together as one coherent asset-storage feature (see ops table).

**Traps for the next auditor** (looks-wrong-but-correct):
- `UpdatePackageGroup`'s query-string fallback for `packageGroup` (still camelCase, technically
  wrong per the real wire format) is *harmless* dead code for real traffic: the real SDK always
  sends `packageGroup` in the JSON body for this op specifically (verified — it's the one
  package-group op where the identifier is a body field, not query), and the handler already
  falls back to the body value when the query lookup misses. Do not "fix" this to kebab-case;
  that would break nothing further but is pointless — leave it as documented.
- `DescribePackage`/`DescribePackageVersion` auto-creating a stub entry on first read is
  intentional pre-existing behavior (explicit comments: "stub entries are created on demand"),
  not a bug introduced this pass. It IS a real divergence from AWS (which 404s), logged as a gap,
  but ripping it out would be a large, risky behavioral change affecting many existing tests and
  was out of scope for this pass.
- `ListPackages` derives its package list by scanning `packageVersions`, not the `packages` table
  directly — this is intentional (a "package" only meaningfully exists once it has a version) and
  is why `PublishPackageVersion` inserts into both tables.
