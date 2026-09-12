# RouteMatcher over-claim sweep (gopherstack-op3e)

Built for gopherstack-op3e. Securityhub's entire findings/members op family
was unreachable over the real HTTP wire for an unknown length of time:
inspector2 and macie2 both claimed `/findings*` and `/members*`
unconditionally in their `RouteMatcher`, both register before securityhub in
`cli.go`'s `getServiceProviders` chain, and `pkgs/service/router.go` takes
the first matcher that returns true. `BatchImportFindings` got a 501 from
inspector2; `CreateMembers` got a 400 from macie2. Every unit test passed,
because unit tests call `h.Handler()` directly and never touch
`RouteMatcher` or the router (fixed in commit `a309b74fc`).

**A future pass should read this file, not rebuild it.** Regenerate the
candidate list via `go run ./cmd/routecollisions` (add `-json out.json` for
full per-service claim detail), diff against the "swept" tables below, and
update them — don't just discard this file. `*.py` scratch scripts are
gitignored here; `cmd/routecollisions` is the committed Go tool for exactly
the reason the sibling `_OVERWIDE_CANDIDATES.md`/`cmd/overwidecandidates`
and `_WRAPPER_KEY_SWEEP_REMAINDER.md`/`cmd/opcensus` pairs exist.

**Status after three passes: all 163 registered services triaged, 5 confirmed
bugs found and fixed** (the original securityhub/inspector2/macie2 one, three
more in the second pass — `apigateway`/`quicksight` on `/account/`,
`appconfigdata`/`omics` on `/configuration`, `inspector2`/`omics` on
`/configuration/` — plus `rolesanywhere`/`xray` on bare `/TagResource`,
`/UntagResource`, `/ListTagsForResource` in the third pass, gopherstack-h3p1;
see "Third pass" below), **6 tool false positives disproven by hand/by
driving the real router** (`batch`/`kafka` on bare `/v1/` and
`polly`/`appsync`/`batch` on `/v1/` from the first two passes, plus
`codeartifact`/`appsync` on `/v1/tags`+`/v1/domain`, `ecr`/`apigatewayv2`+
`appsync`+`sesv2` on `/v2`, `resourcegroups`/`backup` on `/resources/`, and
`vpclattice`/`macie2` on `/tags` from the third pass), and every remaining
candidate hand-verified clean via a real disambiguation mechanism.
`cmd/routecollisions` now chases predicate-function delegation and map/
route-table keys (gopherstack-h3p1) in addition to direct claims, extracting
real claims for 69 of 94 path-based services plus recognizing 16 more as
structurally immune (Query/EC2-protocol body match) — 85 of 94 tool-covered;
the remaining 9 (`resiliencehub`, `mgn`, `networkmanager`, plus 6 more) are
confirmed via the tool itself to have zero extractable "/"-prefixed literal
claims (AWS's own "RPC over REST" operation-name-shaped route tables, not a
tooling gap) or were hand-read for other reasons — see "Third pass" below.

## The question this asks (and the one it doesn't)

A prior sweep (gopherstack-k9bl) checked whether each service's
`RouteMatcher` accepts **its own** paths. This is the opposite question:
does it also accept paths that belong to **somebody else**? A generic noun
path (`/findings`, `/members`, `/tags`, `/policies`, `/channels`, `/v1/...`)
claimed unconditionally by an early-registered or higher-priority service
silently swallows every other service that legitimately serves the same
prefix — the router never falls through, so nothing errors loudly; the
victim's ops just 400/404/501 with the wrong service's error body forever,
and every one of the victim's own unit tests keeps passing because they
never go through the router.

## Method

1. Enumerated all 163 provider registrations in `cli.go`'s
   `getServiceProviders` chain (`getCoreServiceProviders` →
   `getRemainingServiceProviders` → `getLatestServiceProviders` →
   `getNewestServiceProviders` → `getMostRecentServiceProviders`), which
   fixes each service's registration order — the tiebreaker
   `pkgs/service/router.go`'s `sort.SliceStable` uses when two matchers
   share a `MatchPriority()`.
2. Found 162 services implementing `RouteMatcher() service.Matcher` (one
   dir, `s3tables`'s sibling or similar naming aside, has none — see the
   generator's stderr for any dir it skipped). Of those, **94 match on
   `c.Request().URL.Path`** (structurally at risk of this bug class) and
   **67 match on a header/`X-Amz-Target` prefix** (JSON-RPC-style services;
   structurally immune to path collisions, since AWS's own SDK never sends
   an ambiguous path for them).
3. `cmd/routecollisions` statically parses every path-based service's
   `RouteMatcher` body (`go/ast`, not the full router), resolves package
   consts (including `"/"+identifier` concatenation, package-level
   `[]string` tables like `securityHubOnlyPathPrefixes`/
   `onceRouteMatchPrefixes`, and `service.PriorityXxx[+N]` match-priority
   expressions), and extracts the literal path claims. It skips a claim
   found inside an exclusion branch (`if strings.HasPrefix(path, x) {
   return false }`, or a leading `!`) so a service's own careful carve-outs
   don't get reported as claims. **First pass: extracted claims for 44 of
   the 94 path-based services** (50 uncovered — see "Second pass" below,
   where those 50 were fully triaged: 9 more converted to real claims via a
   generator fix, 14 recognized as structurally immune, 27 hand-read).
   **After the second pass: 53 of 94 produce real extracted claims, 14 more
   are recognized-immune (67 of 94 tool-covered total), 27 hand-read.**
4. For every pair of claims across the covered services, ordered by
   effective router evaluation order (`MatchPriority()` descending, then
   registration order), it flags a candidate when the earlier-evaluated
   claim is a literal string-prefix of (or identical to) the later one —
   i.e. the earlier service would actually intercept the later service's
   request. First pass: **76 candidate pairs**; after the second pass'
   generator fixes, **87** (see `-json` output for the full per-claim
   detail). Every one was read by hand against the real source below.

## Confirmed bug (already fixed, commit `a309b74fc`)

securityhub's `/findings*` and `/members*` vs. inspector2 and macie2's
unconditional same-path claims. Fixed by gating inspector2/macie2's claims
on `isInspector2Request`/`isMacie2Request` (Authorization-header SigV4
signing-service check), mirroring securityhub's own pre-existing
`isSecurityHubRequest` pattern. Not re-verified in this pass (already has
its own hand-reverted round-trip test, `test/integration/
securityhub_findings_roundtrip_test.go`); this file exists because that fix
prompted the sweep, not because it needs redoing.

## Candidates checked this pass — all confirmed CLEAN

Every one of the 76 pairs the tool flagged falls into one of these already-
correctly-disambiguated clusters. "Guarded" in the tool's output is a coarse
per-service regex signal (`ExtractServiceFromRequest`/`isXRequest(`) and
**undercounts** two other real disambiguation mechanisms this pass had to
verify by hand — an ARN-embedded service check, and
`httputils.ScopedPrefixMatch`/a marker check the regex doesn't name. A
"guarded" bit alone was never trusted as proof; the actual source of each
claim was read.

- **`/tags` / `/tags/{arn}`** (accessanalyzer, amplify, eks, detective, dlm,
  appconfig, iotwireless, macie2, managedblockchain, iotanalytics,
  emrserverless, pipes, bedrockagent, networkmonitor — 60+ of the 76 pairs).
  This is the standard AWS tag-on-resource REST convention: the resource
  ARN is embedded in the path itself. Every one of these services either
  (a) checks the ARN's own embedded service segment before claiming (e.g.
  dlm's `isDLMResourceARN`, detective's
  `strings.HasPrefix(path[len(pathTagsPrefix):], "arn:aws:detective:")`,
  accessanalyzer/amplify identically), or (b) gates the whole claim on
  `httputils.ExtractServiceFromRequest`/`ScopedPrefixMatch` against its own
  SigV4 name. Read amplify, eks, detective, and dlm's actual `RouteMatcher`
  source directly to confirm (a); managedblockchain/iotwireless/
  bedrockagent/emrserverless/pipes/networkmonitor for (b).
- **`/channels`** (mediapackage vs. iotanalytics vs. mediatailor). All three
  gate on `httputils.ExtractServiceFromRequest(c.Request()) == <own
  sigV4Service>`. Documented k9bl precedent, re-verified here.
- **`/agents`, `/knowledgebases`, `/resourcepolicy`** (bedrock vs.
  bedrockagent). bedrockagent uses `baPriority = 87`, one tier above
  `PriorityPathVersioned` (85), specifically so it is evaluated before
  bedrock's `AgentsHandler` regardless of registration order, AND still
  gates on `ExtractServiceFromRequest` — its own code comments name this
  exact overlap. This is the one place in the repo that deliberately bumps
  `MatchPriority`, predating this campaign's no-priority-bump rule; it
  already works and was not touched.
- **`/v2/apis`** (appsync vs. apigatewayv2). appsync's claim is gated by
  `service.MatchesUserAgentMarker(c.Request().Header, "api/appsync")`, not
  a signing-service check the tool's regex names — read the source to catch
  this one.
- **`/applications`** (serverlessrepo vs. appconfig). Both SigV4-scoped
  (`ExtractServiceFromRequest`/`ScopedPrefixMatch`); appconfig's own comment
  cites gopherstack-ibeo as the issue that already fixed this exact overlap
  (also vs. emrserverless).
- **`/api/things/shadow/`, `/policies`** (iot vs. iotdataplane vs. dlm).
  iot's comments cite gopherstack-61i8 for this exact overlap; both claims
  are scoped to `svc == "" || svc == iotServiceName`.
- **`/v1/` bare prefix** (batch vs. kafka). Batch's `RouteMatcher` falls
  through to an unconditional `strings.HasPrefix(path, "/v1/")` after
  excluding only `/v1/clusters` and `/v1/configurations` — NOT kafka's other
  real `/v1/` paths (`/v1/kafka-versions`, `/v1/compatible-kafka-versions`,
  `/v1/vpc-connection[s]`, `/v1/operations/`). This looked like a live
  second bug by static reading alone. **Driven through the real router
  (not `h.Handler()`) via `TestIntegration_Kafka_ListKafkaVersions`
  (`test/integration/kafka_test.go`) against the current binary: it
  PASSED, unchanged.** Reading further, `kafkaMatchPriority =
  service.PriorityPathVersioned + 1` — kafka already bumps its own priority
  one tier above batch, with a comment explaining why (a *different*,
  already-fixed collision against AppSync's `/v1/tags`). That bump also
  happens to make kafka always win over batch's catch-all as a side effect.
  **No fix applied — this was a tool false positive, caught by verifying
  live before touching code, exactly as instructed.** The
  `TestIntegration_Kafka_ListKafkaVersions` test is kept as a permanent
  regression guard: `ListKafkaVersions` had zero coverage through the real
  router before this pass (only a `h.Handler()`-direct unit test existed).

## Second pass (gopherstack-op3e continued): the 50-service remainder

The 50 services below (from the prior pass's "remaining scope") were all
individually triaged this pass — every one either got a real extracted claim
out of an extended `cmd/routecollisions`, or was hand-read directly. **9
converted to tool coverage via a generator fix, 14 more recognized by the
tool as structurally immune (Query/EC2-protocol body match, not path-based
at all), and the remaining 27 were hand-read.** All 50 are now accounted
for. Three real, live-confirmed collisions were found and fixed; one
tool-flagged candidate among the newly-covered set was investigated and
disproven, the same as the batch/kafka false positive from the first pass.

### Tool extended: two generator fixes, `cmd/routecollisions`

1. **Second-argument `HasPrefix`/`CutPrefix` capture.** The original
   extractor only resolved a prefix identifier when a `path` local variable
   was the first argument (`HasPrefix(path, xxxPrefix)`); it silently missed
   the equally common repo idiom of inlining the request path as the first
   argument and naming the prefix constant as the second
   (`strings.HasPrefix(c.Request().URL.Path, xxxPathPrefix)`). Added
   `secondArgPrefixRe` + `scanSecondArgPrefixIdent` (`claims.go`) to resolve
   this shape against the package const table too. This alone converted 9 of
   the 50 to real extracted claims: appmesh, cloudfront,
   cloudfrontkeyvaluestore, mediaconvert, mq, polly, route53,
   sagemakerruntime, sesv2. All 9 were also hand-verified: single unique
   versioned or otherwise-unshared prefixes, no collisions found (mq's
   `brokersPath` is the one unconditional claim among them; see "mq" below).
2. **Query/EC2-protocol recognition.** EC2, IAM, RDS, DocDB, Neptune,
   Redshift, Autoscaling, ELB, ELBv2, ElasticBeanstalk, SES, SNS and STS
   don't claim a URL path at all — they match on POST + form-urlencoded
   Content-Type + an exact `Version` (or `Action`) value read from the body,
   the same wire convention EC2/IAM/RDS have always used. Structurally
   immune to this bug class the same way a header/`X-Amz-Target` match is:
   two Query-protocol services can only collide if their AWS API version
   strings happened to be identical, which they never are by construction.
   Added `queryProtocolContentTypeRe`/`queryProtocolVersionRe` (`types.go`)
   and an `Immune` field on `svcInfo` so these 14 services are now reported
   as recognized-immune rather than a false "no claims extracted" gap.
   `go run ./cmd/routecollisions` now reports both counts on its summary
   line.

Not extended (documented as a gap for the next pass rather than guessed at):
helper-function-body chasing (`isOmicsPath`, `isAPIGWTopLevelRESTPath`,
`matchesBackupPath`, and similar one-hop delegations to a predicate function
elsewhere in the package), and map/route-table key extraction (`account`'s
`operationNames`, `resourcegroups`'s `rgRESTPathOps`, `resiliencehub`'s
`routes()`, `networkmanager`'s `routeTable()`, `mgn`'s `dispatch()`). All of
these were read by hand this pass instead (see below) — every one turned out
to be either an operation-name-shaped route table (kebab-case or
PascalCase, e.g. `/create-app`, `/DescribeSourceServers` — AWS's own
"RPC-over-REST" convention for several newer services, structurally
unlikely to collide with a generic noun) or already SigV4/ARN-scoped. A
future pass extending the generator to chase these would likely reach
"tool-covered" status for most of the remaining 27 without new findings,
but the two real bugs found this pass (below) came from services the tool
*did* already reach (`apigateway`, `appconfigdata`) — extraction coverage
and bug-finding are not the same axis.

### Fixed this pass — three real, live-confirmed collisions

All three follow the exact shape gopherstack-op3e's title bug did: a generic
path claimed unconditionally by an earlier-evaluated (by priority, then
registration order) service, silently swallowing a second, legitimate
service's real operations. **Every one was reproduced live against the real
router before touching code, fixed with SigV4 Authorization-header scoping
(no `MatchPriority` bump), then hand-reverted to re-confirm the wrong-service
error and byte-identically restored** — see each service's own dedicated
`test/integration` regression test for the exact reproduction.

1. **`apigateway` vs. `quicksight` on `/account/`** (`services/apigateway/handler.go`,
   `isAPIGWTopLevelRESTPath`). API Gateway's own real API only ever emits
   the bare `/account` (confirmed against
   `aws-sdk-go-v2/service/apigateway@v1.42.4/serializers.go`'s two
   `SplitURI("/account")` calls, for `GetAccount`/`UpdateAccount` — no
   sub-path variant exists), but the matcher also accepted any
   `/account/...` prefix. API Gateway runs at `PriorityHeaderExact` (100),
   the highest tier in the router, so it always wins regardless of
   registration order. QuickSight's `CreateAccountSubscription`/
   `DescribeAccountSubscription`/`DeleteAccountSubscription` are real
   operations under the singular `/account/{AwsAccountId}` (QuickSight's tag
   and settings families use the plural `/accounts/...` instead, unaffected).
   **Confirmed live**: `GET /account/{id}` signed for QuickSight returned
   API Gateway's plain-text `404 not found` before the fix. **Fix**: since
   API Gateway's own operation table only ever handles the exact,
   no-sub-path case (`parseAPIGWAccountPath` requires `n == 1`), the over-claim
   was simply wrong against the real wire shape — narrowed to
   `path == "/account"` (no SigV4 gate needed; nothing legitimate was lost).
   Regression test: `test/integration/apigateway_quicksight_account_test.go`
   (full SDK round trip for both services).
2. **`appconfigdata` vs. `omics` on `/configuration`** (`services/appconfigdata/handler.go`).
   AppConfigData's `GetLatestConfiguration` is `GET /configuration`
   (`aws-sdk-go-v2/service/appconfigdata@v1.26.4/serializers.go:42`) — and
   Omics' `ListConfigurations`/`CreateConfiguration` independently use the
   *exact same* bare path (`aws-sdk-go-v2/service/omics@v1.49.5/serializers.go`,
   `type awsRestjson1_serializeOpListConfigurations`/`...CreateConfiguration`,
   `SplitURI("/configuration")`) — a genuine collision in AWS's own wire
   surface, normally disambiguated by hostname (`appconfigdata.*` vs.
   `omics.*`) since real AWS serves each from a distinct endpoint; gopherstack
   serves both from one host, so only SigV4 scoping can tell them apart here.
   AppConfigData registers at `MatchPriority` 86 (unconditional, no SigV4
   gate), Omics at 85 — AppConfigData always won. AppConfigData's real SigV4
   signing name is **`appconfig`**, not `appconfigdata` (confirmed live by
   inspecting the Authorization header a real `appconfigdata` SDK client
   sends — the SDK's `auth.go` overrides the default). **Confirmed live**:
   `GET /configuration` signed for Omics returned AppConfigData's
   `"ConfigurationToken is required"` 400 before the fix. **Fix**: gated the
   whole `RouteMatcher` on `httputils.ExtractServiceFromRequest(...) ==
   "appconfig"`, mirroring `securityhub`'s own pattern. Regression test:
   `test/integration/apigateway_quicksight_account_test.go`'s sibling isn't
   used here — see `test/integration/tag_routing_test.go`'s
   `TestIntegration_ConfigurationRouting_AppConfigData_CrossServiceIsolation`
   (RouteMatcher probe, not a full SDK round trip — see note below).
3. **`inspector2` vs. `omics` on `/configuration/`** (`services/inspector2/handler.go`,
   `ambiguousRouteMatchPrefixes`). Omics' `GetConfiguration`/
   `DeleteConfiguration` bind `/configuration/{name}`
   (`aws-sdk-go-v2/service/omics@v1.49.5/serializers.go`, `type
   awsRestjson1_serializeOpGetConfiguration`/`...DeleteConfiguration`,
   `SplitURI("/configuration/{name}")`); Inspector2's own `GetConfiguration`/
   `UpdateConfiguration` independently bind `/configuration/get` and
   `/configuration/update`. Both register/evaluate at `MatchPriority` 85
   (tied); Inspector2 registers first in `cli.go`, so it always won ties.
   `/configuration/` was in Inspector2's `onceRouteMatchPrefixes` table but
   *not* in `ambiguousRouteMatchPrefixes` (the map that gates a prefix behind
   `isInspector2Request`) — same exact mechanism as the original
   `/findings/`/`/members/` bug this file exists for, just a third prefix the
   original fix didn't catch. **Confirmed live**: `GET
   /configuration/testname` signed for Omics returned Inspector2's generic
   `501 NotImplementedException` before the fix. **Fix**: added
   `"/configuration/": true` to `ambiguousRouteMatchPrefixes` — a one-line
   diff using the exact mechanism already in place. Regression tests:
   `services/inspector2/handler_test.go`'s
   `TestRouteMatcher_FindingsMembersDisambiguation` (extended with
   `/configuration/*` cases) and
   `test/integration/tag_routing_test.go`'s
   `TestIntegration_ConfigurationRouting_Inspector2_CrossServiceIsolation`.

**Why two of the three regression tests drive `RouteMatcher()` directly
instead of a full SDK round trip through the live container**: Omics' own
SDK client unconditionally rewrites the request host to `"workflows-" +
host` for this entire operation family (`ListConfigurations`,
`CreateConfiguration`, `GetConfiguration`, `DeleteConfiguration`, and in fact
most of Omics' run/workflow surface —
`aws-sdk-go-v2/service/omics@v1.49.5/api_op_*.go`, `req.URL.Host =
"workflows-" + req.URL.Host`, confirmed by capturing the outgoing
Authorization/Host from a real client against a local `httptest.Server`).
gopherstack serves everything from one host with no `workflows-` virtual-host
routing implemented anywhere in `services/omics`, so a real Omics client for
these ops cannot reach the test container at all today — **a separate,
pre-existing, much larger structural gap** (most of Omics' real API surface
is unreachable via a stock SDK client, independent of anything in this
file). Filed as gopherstack-follow-up (see bd) rather than fixed here: it's
a wire/host-routing gap, not a `RouteMatcher`-vs-`RouteMatcher` collision,
and out of scope for this sweep. The two `/configuration` fixes above are
still real and were still confirmed live — just via a raw request built with
the router's actual `RouteMatcher()` and a crafted SigV4 Authorization header
(the established `matcherContext`/`sigV4Authorization` pattern already used
elsewhere in `test/integration/tag_routing_test.go` for exactly this
situation — a priority/SigV4 probe where the full end-to-end path isn't
available), not a guess.

### False positive disproven this pass

**`polly` vs. `appsync`/`batch` on `/v1/`.** After the second-argument
`HasPrefix` fix, the tool started flagging `polly`'s `/v1/` prefix as an
`UNGUARDED-WINNER` over several of appsync's `/v1/...` paths and batch's own
`/v1/` catch-all. Reading Polly's actual `RouteMatcher`
(`strings.HasPrefix(path, pollyPathPrefix) && parseRoute(method,
path).operation != opUnknown`) shows the second, AND'd condition the tool
can't see: `parseRoute` is an **exact-match allowlist** of exactly five full
paths (`/v1/speech`, `/v1/synthesisStream`, `/v1/synthesisTasks`,
`/v1/voices`, `/v1/lexicons`), none of which collide with anything appsync or
batch serve. Same root cause as the batch/kafka false positive from the
first pass (a tool that reads one `HasPrefix` in isolation, not the
multi-condition guard around it) — no fix needed, no test added (Polly's
`/v1/` allowlist already has full `Handler()`-level coverage; this isn't a
router-level gap the way `ListKafkaVersions` was).

### Hand-read this pass, confirmed clean (27 services)

**Superseded in part by the third pass** (gopherstack-h3p1, see above):
`account`, `backup`, `codeartifact`, `ecr`, `resourcegroups` (below), plus
`omics`, `elasticsearch`, `opensearch`, `mwaa` (elsewhere in this list), now
produce real extracted claims instead of relying on the hand-read below —
the mechanism and conclusions described here are unchanged (still correctly
"clean"), just now tool-confirmed rather than assumed.

**account, acm, acmpca, appstream, backup, codeartifact, cognitoidp, ecr,
elasticsearch, glacier, lakeformation, lambda, mediastoredata, mgn, mwaa,
networkmanager, omics, opensearch, personalize, quicksight, ram, rdsdata,
resiliencehub, resourcegroups, s3, sqs** (plus `apigateway` and
`appconfigdata`/`inspector2`'s own now-correctly-scoped claims, covered
above as the fixed side of a collision). Mechanism per service:

- **SigV4- or ARN-scoped already**: account (`ExtractServiceFromRequest ==
  "account"`), lakeformation, mwaa, ram, rdsdata (all whole-matcher SigV4
  gates); mgn, networkmanager, resiliencehub (ARN-scoped `/tags/` trio via
  `httputils.MatchesTaggedResourceARN`, everything else an
  operation-name-shaped route table — see below).
- **Header/`X-Amz-Target`-based, not path-based at all**: acm, acmpca,
  appstream (CBOR ops + target prefix), cognitoidp, ecr (registry mode
  gated by `/manifests/`/`/blobs/`/`/tags/list` markers specifically to
  avoid swallowing ApiGatewayV2's `/v2/apis` — pre-existing,
  gopherstack-61i8), personalize, resourcegroups (target prefix +
  exact-path map `rgRESTPathOps`, whose one `/resources/*` key,
  `/resources/search`, is a different literal from backup's own
  `/resources` — see below), s3 (catch-all at `PriorityCatchAll` = 0,
  always evaluated last by construction).
- **Operation-name-shaped route tables** (AWS's own "RPC over REST"
  convention: each op gets its own literal path, e.g. `/DescribeSourceServers`
  or `/create-app` — read `mgn`'s and `resiliencehub`'s own doc comments,
  which say so explicitly): mgn, resiliencehub. `networkmanager`'s
  `routeTable()` is real per-resource REST paths but every segment checked
  (`global-networks`, `resource-policy` (hyphenated — distinct from
  bedrock/bedrockagent's unhyphenated `/resourcepolicy`, already swept
  clean), ...) is specific and exact-segment-count matched, not a bare
  prefix.
- **Unique versioned or otherwise-unshared literal prefix** (no other
  claimant found anywhere in `services/`): appmesh (`/v20190125/`),
  cloudfront (`/2020-05-31/`), cloudfrontkeyvaluestore
  (`/key-value-stores/`, priority 87 alongside apigatewaymanagementapi's 87
  but a disjoint literal, already noted safe in its own comment),
  mediaconvert (`/2017-08-29/`), route53 (`/2013-04-01/`), sagemakerruntime
  (`/endpoints/`), sesv2 (`/v2/email/`), lambda (every real path is
  date-versioned, e.g. `/2015-03-31/functions`, at `PriorityHeaderPartial`
  = 95).
- **`glacier`**: matches `segs[1] == "vaults"|"policies"|"provisioned-capacity"`
  where `segs[0]` is an arbitrary AWS account ID — the real claimed shape is
  `/{accountId}/policies`, not a bare `/policies`, so it neither claims nor
  is claimed by iot/dlm's already-swept bare `/policies`.
- **`backup`**: the one service in this batch with genuinely unconditional,
  ungated path claims (`matchesBackupPath`, no SigV4/ARN check at all) —
  including bare `/resources` (exact) and `/resources/` (prefix), the same
  segment quicksight (`/resources/` prefix, SigV4-gated, wins ties at
  priority 86 > backup's 85) and resourcegroups (`/resources/search` exact
  key, header-based at priority 100) also use. Checked both directions by
  literal and priority: quicksight only claims the path when
  `isQuickSightRequest` passes, so it never swallows backup's traffic;
  resourcegroups' one `/resources/*` key is a different, longer literal
  (`/resources/search` ≠ backup's bare `/resources`) that backup's own
  claims don't reach either way. **No collision found, but backup's
  *mechanism* — a dozen-plus unconditional path prefixes with zero SigV4
  gating — is a standing risk pattern the next pass should keep an eye on
  if any new service registers below priority 85 and picks a literal under
  `/backup-jobs`, `/copy-jobs`, `/legal-holds`, `/audit-*`,
  `/restore-*`, `/scan/jobs`, `/tiering-configuration`, or
  `/logically-air-gapped-vaults`.**
- **`elasticsearch`/`opensearch`**: `pkgs/service/priorities.go`'s own doc
  comment flags these as `PriorityPathSubdomain` (82) specifically because
  they "could overlap with form-encoded services" — but the form-encoded
  (Query-protocol) services in this same batch (ec2/iam/rds/docdb/neptune/
  redshift/autoscaling/elb/elbv2/elasticbeanstalk/ses/sns/sts) claim **no
  path at all**, only a body `Version`/`Action` value, so there is no path
  literal for ES/OpenSearch to actually collide with regardless of
  priority tier. The doc comment's caution is about a hypothetical, not
  something this pass found evidence of.
- **`mq`**: `configurationsPath`/`tagsPath` are gated by `isMQRequest`
  (Authorization contains `/mq/`); `brokersPath` (`/v1/brokers`) is
  unconditional, but `mqMatchPriority` = `PriorityPathVersioned + 1` (86) —
  one tier above batch's 85, the exact same deliberate bump kafka already
  uses against batch (first pass, `services/_ROUTE_COLLISIONS.md`'s
  "`/v1/` bare prefix" entry) — so mq always wins the race regardless of
  registration order. Not re-verified live this pass since it reduces to
  the already-verified kafka mechanism, not a new one.
- **`codeartifact`**: `codeartifactMatchPriority` = `PriorityPathVersioned +
  1` (86), same bump mechanism, and batch's own `RouteMatcher` additionally
  hard-excludes codeartifact's exact `/v1/domain*`/`/v1/repositories*`/
  `/v1/authorization-token` literals by name — belt and suspenders, already
  correct.

## Third pass (gopherstack-h3p1): delegation chasing

`cmd/routecollisions` previously extracted claims only from a `RouteMatcher`
body's own text. This pass (see `cmd/routecollisions/delegation.go`) makes it
chase two delegation shapes documented as tooling debt at the end of the
second pass: a predicate-function call (`return isXPath(path)`) and a map/
route-table index (`operationNames[path]`, `h.routes()[key]`).

### How the chase works

`collectNamedBodies` (`main.go`) records every top-level func/method body and
package-level `var` composite-literal body in the package, keyed by name
(receiver type ignored — two receivers never share a method name in one
gopherstack service package). `chaseClaims` (`delegation.go`) then, starting
from `RouteMatcher`'s own body: extracts claims from the current body via
`extractClaimsForNode`, finds every name `referencedNames` sees called
(`ast.CallExpr`, both bare `isXPath(...)` and method-call
`h.routes()` forms) or indexed (`ast.IndexExpr`) in it, and — for each name
present in the named-body table and not already visited in this chase —
recurses into that body one level deeper. Depth is capped at
`chaseDepthLimit = 4` and a `visited` set (shared across the whole chase, not
per-branch) prevents cycles and redundant work on a helper reached from
multiple branches (e.g. `mergeRoutes`/`concatRoutes`). Three extraction
mechanisms run per chased body:

1. The existing text-based `extractClaims` (quoted literals, `"/"+ident`/
   `ident+"/"` concatenation, bare-identifier `==`/`HasPrefix`/`CutPrefix`/
   `range` context) — unchanged, just now also run on delegate bodies.
2. `scanMapKeyClaims` walks the body's AST for any map composite literal
   (however deeply nested — a func that directly `return`s
   `map[string]T{...}`, resiliencehub/mgn's shape, counts) and adds an exact
   claim per key: a literal string key directly, or an identifier key
   resolved against the const table (account's `operationNames` is keyed by
   consts, not string literals — this needed real AST inspection, not text
   matching, to resolve correctly).
3. `scanSliceLiteralIdentClaims` walks the body's AST for any `[]string{...}`
   composite literal — local (`prefixes := []string{...}` inside a helper,
   which the package-wide `sliceConsts` regex table never sees since it only
   matches a top-level `IDENT = []string{...}` var) or otherwise — and for
   each bare-identifier element resolving against a const, records **both**
   an exact claim for the value and a prefix claim for value+`"/"`. Both
   readings are needed because this repo's common idiom for such a table is
   `path == p || strings.HasPrefix(path, p+"/")` (omics' `isOmicsPath`): the
   loop variable `p` isn't itself const-resolvable, so the existing
   concatenation scan can't see the `+"/"` half on its own.

A fourth change fixes a false positive the chase would otherwise introduce:
`localConstTable` collects consts declared *local* to a chased body (e.g.
resourcegroups' `isResourceTagsPath`: `const prefix = "/resources/"` and
`const suffix = "/tags"`) into the const table for that call, and excludes
each such literal's own declaration text from the raw quoted-literal scan.
Without this, a local const used only in `strings.HasSuffix(path, suffix)`
produced a bogus standalone `"/tags"` claim merely from declaring the name —
never from an actual prefix check — because plain text scanning can't tell a
declaration from a comparison. With it, `suffix` (used only via `HasSuffix`,
a context the resolver never treats as claim-worthy) correctly contributes
no claim, while `prefix` (used via `HasPrefix(path, prefix)`) still resolves
to `/resources/` through the normal identifier-resolution path.

### Reproduces the two hand-found collisions automatically

Before this pass, `omics` produced **zero** extracted claims (`isOmicsPath`
was never chased), so `appconfigdata`/`omics` and `inspector2`/`omics` never
appeared in the tool's own candidate list — both were only found by hand
last pass. After this pass, `go run ./cmd/routecollisions` reproduces both
automatically:

```
appconfigdata            [prefix "/configuration"     prio=86 reg=50] shadows omics                    [exact "/configuration"     prio=85 reg=157]  (guarded/unguarded)
appconfigdata            [prefix "/configuration"     prio=86 reg=50] shadows omics                    [prefix "/configuration/"    prio=85 reg=157]  (guarded/unguarded)
inspector2               [exact "/configuration/"    prio=85 reg=85] shadows omics                    [prefix "/configuration/"    prio=85 reg=157]  (guarded/unguarded)
inspector2               [prefix "/configuration/"    prio=85 reg=85] shadows omics                    [prefix "/configuration/"    prio=85 reg=157]  (guarded/unguarded)
```

(`guarded/unguarded` — the winner, `appconfigdata`/`inspector2`, is
correctly recognized as SigV4/prefix-map-gated, matching the already-fixed
state from the second pass; the pairs surface for confirmation, not as new
bugs.)

### Now tool-covered instead of hand-read

`omics`, `backup`, `account`, `resourcegroups`, `codeartifact`,
`elasticsearch`, `opensearch`, `ecr`, and `mwaa` — 9 of the second pass's 27
hand-read services — now produce real extracted claims instead of relying on
a human having read the source. `resiliencehub`, `mgn`, and `networkmanager`
remain claim-free even after the chase, but that's now a *confirmed*
structural fact rather than an assumption: every literal key their route
tables use is a bare operation slug (`"POST create-app"`,
`"/DescribeSourceServers"`'s sibling shapes) with no leading `"/"`, so the
same `"/"`-prefix filter that has always applied to every other extraction
path here correctly finds nothing to claim.

### Fixed this pass — one real, newly-discovered collision

**`rolesanywhere` vs `xray` on bare `/TagResource`, `/UntagResource`,
`/ListTagsForResource`** (`services/rolesanywhere/handler.go`). Both
services' real AWS SDK clients emit these exact bare paths — confirmed
against `aws-sdk-go-v2/service/rolesanywhere@v1.26.3/serializers.go` and
`.../xray@v1.39.4/serializers.go`'s own `SplitURI("/TagResource")` etc:
`resourceArn` travels in the JSON body/query, never the path, for either
service (the standard AWS tagging-operation convention). RolesAnywhere
claimed all three unconditionally (`path == "/"+pathTagResource`, no SigV4
gate at all) while X-Ray's own real matcher indexes an equally unconditional
`xrayPaths` map — both `MatchPriority` 85, and RolesAnywhere registers first
in `cli.go`, so it always won the tie and silently swallowed every X-Ray
`TagResource`/`UntagResource`/`ListTagsForResource` request. Neither
delegation shape was previously chased, so RolesAnywhere had zero extracted
claims before this pass and the pair was invisible to the tool. **Fix**:
gated just those three ambiguous paths on
`httputils.ExtractServiceFromRequest(...) in {"", "rolesanywhere"}` — the
same `svc == "" || svc == ownServiceName` idiom `iot` already uses for its
own `/policies` and `/api/things/shadow/` disambiguation — leaving
RolesAnywhere's other (genuinely unique) paths unconditional. Regression
test: `services/rolesanywhere/handler_test.go`'s
`TestHandler_RouteMatcher_TagOpsCrossServiceIsolation`.

### False positives disproven this pass

All four follow the same root cause as Polly's `/v1/` false positive from
the first pass: the chase now reaches a real predicate/map, but that
predicate's condition is a **compound** one (an additional `&&`-ed check,
or an ARN-content narrowing) the tool's flat literal-overlap check can't
see — so the newly-surfaced over-claim needs the same by-hand disproof as
before, just now the tool tells you where to look instead of missing the
claim entirely.

- **`codeartifact` vs `appsync` on `/v1/tags`, `/v1/domain`**. CodeArtifact's
  real `ListTagsForResource` is `GET /v1/tags` with `resourceArn` as a query
  parameter (confirmed against
  `aws-sdk-go-v2/service/codeartifact@v1.41.4/serializers.go`,
  `SplitURI("/v1/tags")` + `encoder.SetQuery("resourceArn")`) — a genuine
  bare, no-ARN-in-path claim. But AppSync's own real matcher
  (`isAppSyncTagPath`, see its doc comment) requires an ARN *in the path*
  (`/v1/tags/{resourceArn}`) and explicitly does not claim bare `/v1/tags`;
  the tool's extracted AppSync claim is a `HasPrefix` over-approximation of
  that ARN-gated predicate. For `/v1/domain`: CodeArtifact's real prefix
  check is `HasPrefix(path, pathV1Domain+"/")` (note the trailing `/`), but
  the pre-existing `concatRightRe` extraction (unchanged this pass) drops
  the `+"/"` and records the bare `/v1/domain` — so the extracted claim is
  broader than the real one, and `/v1/domain` (without a following `/`) is
  not a prefix of AppSync's real `/v1/domainnames`. No fix needed.
- **`ecr` vs `apigatewayv2`/`appsync`/`sesv2` on `/v2`**. ECR's real registry
  gate (`isRegistryPath`) requires, after stripping the `/v2/` prefix, that
  the remainder contain `/manifests/` or `/blobs/` or end in `/tags/list` —
  none of which any of these three services' real `/v2/...` paths do — and
  is further gated on a `h.registryEnabled` runtime flag the tool has no way
  to see at all. The tool's claim (`HasPrefix`/`CutPrefix` on the bare `/v2`
  prefix identifier) is the same "sees one `HasPrefix`, not the `&&`-ed
  follow-up checks" gap Polly hit. No fix needed.
- **`resourcegroups` vs `backup` on `/resources/`**. resourcegroups'
  `isResourceTagsPath` requires the path to *also* end in `/tags` (and be
  longer than the combined prefix+suffix) — it only ever matches
  `/resources/{arn}/tags`, never a bare `/resources/...` sub-path, so it
  never reaches backup's real `/resources/*` traffic despite resourcegroups
  registering at the highest priority tier (`PriorityHeaderExact` = 100).
  No fix needed (backup's own unconditional, ungated `/resources` claims
  remain the standing risk pattern already flagged in the second pass — this
  pairing specifically is clean).
- **`vpclattice` vs `macie2` on `/tags`**. Both sides are actually
  ARN-scoped in reality — vpclattice's `isVPCLatticeTagPath` requires the
  ARN segment to contain `:vpc-lattice:`, macie2's own claim is the already-
  fixed `"/tags/arn:aws:macie2:"` exact prefix — but the tool's coarse
  `guarded` bit doesn't recognize either ARN-content check by name (same
  documented undercount as the `/tags/{arn}` cluster in "Candidates checked
  this pass" above), so it shows as `UNGUARDED-WINNER`. No fix needed.

The remaining `UNGUARDED-WINNER/unguarded` pairs in the current full run are
all the **same, already-documented-clean** clusters from the first two
passes — the ARN-embedded `/tags`/`/tags/{arn}` trio
(accessanalyzer/amplify/detective/dlm/eks/vpclattice — see "Candidates
checked this pass" above) and the AppSync `/v2/apis` User-Agent-marker case —
now also reached from newly-covered services (`mwaa`, `ecr`) landing on the
same already-verified conclusion, not new findings.

## Known tool limitations

- Coarse per-service `guarded` bit only recognizes
  `ExtractServiceFromRequest`/`isXRequest(` by name — misses
  `ScopedPrefixMatch`, ARN-embedding checks, and User-Agent marker checks
  (all real, all verified by hand this pass; see above).
- `MatchPriority()` resolution follows `service.PriorityXxx` and
  `service.PriorityXxx + N` (added this pass, after it produced a false
  "batch shadows kafka" candidate — see above) and local int/selector
  consts, but not a priority computed via a method call
  (`h.restRouter().MatchPriority()`, e.g. macie2) or any other indirection.
  Those show `prio=-1` in the JSON output and are ranked last by the
  generator's sort, which can misorder a pair's "winner"/"loser" labels —
  always confirm the real evaluation order by reading `MatchPriority()`'s
  actual implementation before trusting the label.
- Literal-overlap detection is purely textual (string-prefix / equality on
  the resolved claim), not control-flow aware beyond the exclusion-branch
  heuristic above — it does not understand ARN-content narrowing, method
  narrowing, or multi-condition guards. Every flagged pair still needs a
  human read of both services' actual `RouteMatcher` source, which is what
  "confirmed CLEAN" above records having done, not the tool's raw output.
  **Concretely bit this pass**: Polly's `/v1/` claim looked unguarded to the
  tool but is actually gated by a second, AND'd `parseRoute(...) !=
  opUnknown` exact-match condition the tool never sees — see "False positive
  disproven this pass" above.
- (Added second pass) `HasPrefix`/`CutPrefix` identifier resolution now
  covers the prefix identifier as either the first argument (`path` local
  variable form) or the second (`HasPrefix(c.Request().URL.Path,
  xxxPrefix)` inline form, `secondArgPrefixRe`/`scanSecondArgPrefixIdent` in
  `claims.go`) — but it still only resolves a **single** identifier per
  call; a `RouteMatcher` that builds its path expression through more than
  one level of indirection, or checks a slice/map of prefixes via a `for`
  loop over a *computed* (not literal) table, is not chased.
- (Added second pass) Query/EC2-protocol recognition
  (`queryProtocolContentTypeRe`/`queryProtocolVersionRe`) requires both a
  `Header.Get("Content-Type")` call and a `vals.Get("Version"/"Action")` or
  `strings.Contains(string(body), ...)` call to appear *anywhere* in the
  same `RouteMatcher` body — it does not verify they're the same check or in
  any particular order (found necessary because DocDB/Neptune insert a
  multi-line User-Agent-marker check and doc comment in between the two,
  and SNS/STS reference a local `snsContentType`/`contentTypeForm` constant
  rather than the literal `"application/x-www-form-urlencoded"` string). This
  is looser than ideal — a service with an unrelated `Contains(string(body),
  x)` call elsewhere in its matcher could be mislabeled `Immune` — but since
  `Immune` is only ever set when zero path claims were extracted, the
  failure mode is a cosmetic misclassification in the summary line, not a
  missed collision.
- (Added third pass, gopherstack-h3p1) Helper-function-body delegation
  (`return isXPath(path)`) and map/route-table literal-key extraction
  (`rgRESTPathOps["/resources/search"]`, `h.routes()["POST create-app"]`,
  and a local `[]string{...}` table of bare consts) are now chased — see
  "Third pass" above for the mechanism. This did not remove the "multi-
  condition guard" blind spot the batch/kafka and Polly false positives
  already documented; it just means that blind spot can now surface
  *through* a chased body too, not only a `RouteMatcher`'s own top-level
  code (see "False positives disproven this pass" in the Third pass section
  for four concrete instances: `codeartifact`/`appsync`,
  `ecr`/`apigatewayv2`+`appsync`+`sesv2`, `resourcegroups`/`backup`,
  `vpclattice`/`macie2`). The chase also still doesn't follow a local
  variable that isn't itself a `[]string{...}`/map composite literal at the
  call site — e.g. `slices.Contains(exacts, path)` where `exacts` was built
  some other way, or a value threaded through more than one variable
  reassignment — so a table built that way still needs a human read.
  `chaseDepthLimit = 4` bounds recursion depth; the deepest real chain found
  this pass (resiliencehub/mgn's route-table builders) is 3 hops, so there's
  one hop of headroom before a legitimately deeper delegation chain would
  need the limit raised.

## Unclaimed dispatcher paths (gopherstack-blzga, 2026-09-12)

The inverse question from every pass above: does a service's own **dispatcher**
(its `Handler()`/`handleREST`/`classifyPath`-style routing code) handle a path
family its `RouteMatcher` never claims? A path in that gap is unreachable over
the real HTTP wire — unit tests calling `h.Handler()` directly never see it,
only a real request through `pkgs/service.Router` does. Found twice already in
typed slice 16 (2026-09-12, predates this pass): omics' `isOmicsPath` had
`/s3accesspolicy/` baked with a trailing slash so the matcher required `//`
(S3AccessPolicy family unroutable), and opensearch defined
`openSearchDefaultAppSettingPath` but never added it to
`openSearchPathPrefixes` (Get/PutDefaultApplicationSetting unroutable).
Tracked as gopherstack-blzga.

### Checker design

`cmd/routecollisions -unclaimed` (new mode, `unclaimed.go`) reuses the
existing claim-extraction machinery but starts from the opposite end:

1. **Dispatch entry points.** `pkgData` now also collects every method
   literally named `Handler` (the `service.Service.Handler() echo.HandlerFunc`
   entry point every `Registerable` type must implement — a slice, since
   bedrock/redshift have more than one `Registerable` per package).
2. **Dispatch chase.** `chaseDispatchClaims` walks from each `Handler()` body
   the same call/index-following way `chaseClaims` (delegation.go) already
   does for `RouteMatcher`, but deeper (`dispatchChaseDepthLimit = 8` vs. 4 —
   real dispatch chains run `Handler → handleREST → classifyPath →
   classifyGET/POST/DELETE → an op-dispatch table`) and with two
   dispatch-specific differences from the matcher-side extraction:
   - **Comments are stripped** before scanning (`blankComments`,
     byte-length- and offset-preserving). A doc comment describing a path
     shape — extremely common in this repo — reads as a claim otherwise once
     an 8-hop chase runs through arbitrary handler code.
   - **Quoted literals require nearby comparison context**
     (`isDispatchComparisonContext`: `HasPrefix(`/`HasSuffix(`/`Contains(`/
     `CutPrefix(`/`==`/`case ` within 40 chars behind). Without this, a
     resource ID built by concatenation (route53's `"/hostedzone/" + hz.ID`)
     or a redirect `Location` header value reads as a route claim merely for
     being a bare `"/"`-prefixed quoted string anywhere in the reachable call
     graph. (Map-literal keys and local `[]string` slice-of-consts claims are
     still recorded unconditionally, same as the matcher side — those shapes
     are never anything but route tables in this codebase.)
3. **Coverage diff.** Dispatch claims not covered by any of the service's own
   `RouteMatcher` claims (string-prefix or exact match, same rule
   `literalsOverlap` uses) are reported, minimized to root literals (a hit
   that is itself a prefix-extension of another reported hit in the same
   service is dropped — fixing the shorter one covers the longer one too).
   Services whose only `RouteMatcher`(s) never reference `URL.Path` at all
   (header/`X-Amz-Target`-scoped — checked directly on the matcher body text,
   not the coarser `guarded` bit) are skipped as structurally immune, as are
   services whose matcher is path-based but extracts **zero** literal claims
   (the pre-existing "RPC-over-REST route table" tool limitation documented
   above — nothing to diff against, so nothing is reported rather than
   false-flagging the entire dispatcher).

Three shared-extraction gaps were fixed along the way (all purely additive —
verified against a full JSON diff that no previously-found `RouteMatcher`
claim was ever removed, only new ones added):

- **`range` over a package-level `[]string` of bare const identifiers**
  (`extractSliceConsts` in `main.go`, e.g. lambda's `lambdaPathPrefixes =
  []string{lambdaPathPrefix, lambda2017PathPrefix, ...}`) — previously only a
  slice literal's *quoted* elements resolved; a slice of *identifiers*
  resolved to nothing, so lambda's entire 19-entry dated-prefix table (and
  opensearch's, vpclattice's) was invisible to the matcher-side claims list
  too. Fixed by also resolving each comma-split element against the const
  table. (Confirmed via a full before/after JSON diff: lambda, opensearch,
  and vpclattice each gained many previously-missing claims; nothing else
  changed.)
- **Multi-value `switch` case clauses using bare const identifiers**
  (`scanSwitchCaseIdentClaims`, delegation.go, shared by both pipelines) —
  detective's `RouteMatcher` classifies its entire op set as `switch path {
  case pathGraph, pathInvitation, pathMembersList, ...: return true }`; no
  individual case value is preceded by `==`/`HasPrefix(path,`/`CutPrefix(
  path,`/`range ` (the only contexts `bareIdentRe` recognizes), and it isn't
  a map literal either, so detective's claims list previously collapsed to
  just its `/tags/` ARN guard. Fixed by walking every `*ast.CaseClause` for
  bare-identifier values resolved against consts.
- **`isExclusion` false-negatives on a guard that returns a boolean
  expression, not a literal `true`** — tried, **reverted**, kept as a
  documented limitation (see "Known tool limitations" below) because fixing
  it generically broke the primary report far worse than it helped: turning
  ~10 previously-invisible dispatch-side false positives into real matcher
  claims also turned every one of them into an **unconditional** `"/tags/"`
  claim in the *default* over-claim report (since the coarse `guarded` bit is
  package-wide, not per-claim — the fix loses exactly the ARN-scoping
  condition that made the pattern safe), inflating the collision-pair count
  from 204 to 321 with false `UNGUARDED-WINNER` alarms across every service
  using this repo's established `/tags/{arn}` guard idiom. Left alone; see
  below for how each affected service was instead verified safe by hand.

### Result: 151 candidate hits across 20 services, zero confirmed bugs

`go run ./cmd/routecollisions -unclaimed` (2026-09-12, post-fixes) reports
**151 unclaimed dispatcher path literals across 20 services**: apigateway,
backup, bedrock, bedrockagent, bedrockruntime, cloudfront, emrserverless,
inspector2, iot, iotanalytics, kafka, lambda, medialive, mediapackage,
mediatailor, mq, omics, route53, sagemakerruntime, scheduler. Every one was
hand-read against its own source (per-service source citations kept in the
session notes, not reproduced here) and falls into exactly two false-positive
classes — no new unreachable-path bug found (beyond the two already fixed
pre-pass, above):

**Class A — ARN/SigV4-scoped guard hidden by the `isExclusion` gap above.**
The matcher genuinely claims the path, conditionally: `if rest, ok :=
CutPrefix(path, "/tags/"); ok { return Contains(rest, ":omics:") }` (omics,
apigateway, bedrock, bedrockagent's `AgentsHandler`, detective, inspector2),
`if HasPrefix(p, configurationsPath) || HasPrefix(p, tagsPath) { return
isMQRequest(...) }` (mq), or the equivalent via
`httputils.ExtractServiceFromRequest(...) == "<svc>"` (backup's ARN-scoped
prefix concatenation `pathTags + "arn:aws:backup:"` inside a `[]string`
element — a *fourth*, unfixed shape: `const + "literal"` as a slice element
or switch-case value, not just a bare identifier — iotanalytics, kafka's
`isKafkaTagsPath`, scheduler). **Spot-verified live** via a real two-request
`service.NewRegistry`/`NewServiceRouter` test (scratch, not committed): mq's
`/v1/configurations` and `/v1/tags` are reachable when SigV4-scoped `mq` and
correctly fall through to the router's default 404 when scoped `kafka`.

**Class B — reachability-safe internal sub-dispatch.** The literal is
compared against an already-trimmed/split substring (`suffix`, `rest`,
`inner`, `parts[1]`, an already-stripped `path` local — never the raw
`c.Request().URL.Path`), or is a `HasSuffix`/`Contains` check reached only
after the service's own broad top-level prefix claim already matched
(route53's single `/2013-04-01/` claim covering `/hostedzone/`, `/change/`,
`/cidrblocks`, `/cidrlocations`, `/delegationset/`; cloudfront's single
`/2020-05-31/`; medialive's single `/prod/`; bedrockruntime's `/model/` +
`/guardrail/` covering `/invoke`/`/converse`; bedrock/bedrockagent's
`/agents/`+`/flows`+`/knowledgebases/` covering every `/actiongroups`,
`/agentaliases`, `/documents`, `/versions`, etc. sub-resource action word;
iot's large enumerated prefix list covering every `HasSuffix(path, "/x")`
sub-check reached only within an already-claimed `/jobs/`, `/things/`,
`/policies/`, etc.; sagemakerruntime's `/endpoints/` covering
`/invocations`/`/async-invocations`, with `/output` additionally not even a
path check — it's an S3 output-location URI string; mediapackage's
`/credentials` checked against `sub`, not `path`; mediatailor's bare
`parts[1] == "vodSource"` segment check within an already-claimed
`/sourceLocation/` path; lambda's durable-execution/URL-config suffix checks
within its own already-claimed function-path prefixes; backup's remaining
action words (`/access-policy`, `/disassociate`, `/index`,
`/mpaApprovalTeam`, `/vault-lock`, `/recovery-points`, `/restore-metadata`)
checked against an already-CutSuffix'd `rest`/`arn` or looked up in a local
`[]string` route table of known suffixes).

No fixes were made to any service's `RouteMatcher` — there was nothing to fix.
`go run ./cmd/paritylint` was not affected (no service source changed).

### A finding this pass's tool fixes surfaced, filed separately

The `scanSwitchCaseIdentClaims` fix (detective's real claims list, above) also
changed the **default** (non-`-unclaimed`) report: 69→71 services now show
extracted claims, 201→204 collision pairs. Diffed against the pre-fix
baseline (a throwaway `git worktree add --detach` at the prior commit) to
confirm the delta is genuine, not reordering: two new pairs are cosmetic
(`iot`/`iotwireless` and `lakeformation`/`rdsdata`, both `guarded/guarded`,
low risk, not investigated further here). The third is real and **confirmed
live**: detective's newly-visible exact claim on `/invitation` (priority 85)
literal-overlaps guardduty's own prefix claim on `/invitation` (priority -1),
neither guarded, and detective registers with the higher effective priority —
so detective wins the tie. A scratch two-service router test (`detective` +
`guardduty`, both real handlers, a `PUT /invitation` request SigV4-scoped
`guardduty`) confirms detective's handler answers it (`operation=
AcceptInvitation status=400`), never guardduty. This is a **pre-existing**
bug this pass's tool fix made visible, not introduced by it — same class as
the original securityhub/inspector2/macie2 incident this whole sweep exists
for. Filed as gopherstack-39710 rather than fixed here: it's a different bug
class (over-claim, not unclaimed-dispatcher-path) than gopherstack-blzga's
scope, and the fix (guard detective's `/invitation` case the same
SigV4-scoped way its own `/tags/` case already is) deserves its own
dedicated pass with a regression test, not a bundled fix.

**Correction (2026-09-12, resolving pass, see below):** the scratch repro's
`PUT /invitation` was a synthetic demonstration, not a real guardduty SDK
call — no guardduty op actually sends `PUT /invitation`. The real,
SDK-verified overlap at this exact path is detective's `AcceptInvitation`
(`PUT /invitation`) against guardduty's `ListInvitations` (`GET
/invitation`); the underlying bug (detective's path-only claim ignoring
method entirely) is the same either way. See "gopherstack-39710 resolved"
below.

### Known tool limitations (added this pass)

- `isExclusion` misreads a claim guarded by a runtime check that returns a
  boolean *expression* (`isMQRequest(...)`, `Contains(rest, ":omics:")`) as
  an exclusion carve-out, because the enclosing function's own unrelated
  trailing `return false` fallback is the first return-false-or-true text the
  unbounded lookahead window finds (there's no literal `return true`
  anywhere in the function to find first instead). See "Three shared-
  extraction gaps" above for why a generic fix was tried and reverted.
- `const + "literal"` concatenation as a `[]string` slice element or
  `switch`/`case` value (backup's `pathTags + "arn:aws:backup:"`) is not
  resolved by any extractor — only a bare identifier element/case value
  (`scanSliceLiteralIdentClaims`, `scanSwitchCaseIdentClaims`) or a
  `"/"+ident`/`ident+"/"` concatenation (`scanConcatLiterals`) is.
- The dispatch-side 40-char comparison-context window
  (`isDispatchComparisonContext`) is a text-proximity heuristic, not
  control-flow aware — an unrelated `==` or `HasPrefix(`/`HasSuffix(` earlier
  in the same statement or the previous line can make an unrelated literal
  read as "in comparison context" (seen once: route53's `z.Name == dnsName
  && strings.TrimPrefix(z.ID, "/hostedzone/")`, where the `==` compares a
  different pair of values than the literal a few characters later). Net
  effect skews toward over-reporting, the safer direction for a sweep whose
  every hit gets hand-verified anyway.
  need the limit raised.

### gopherstack-39710 resolved (2026-09-12): detective/guardduty `/invitation`

**Real overlap set**, cited from each pinned SDK's own `serializers.go`
(the only two ops of either service that bind the bare `/invitation` path
with no further segment):

- detective `AcceptInvitation`: `PUT /invitation`
  (`aws-sdk-go-v2/service/detective@v1.41.4/serializers.go:44`).
- guardduty `ListInvitations`: `GET /invitation`
  (`aws-sdk-go-v2/service/guardduty@v1.85.4/serializers.go:5486`).

No other guardduty op collides here: `AcceptInvitation` (the deprecated
legacy op) is `POST /detector/{DetectorId}/master` (serializers.go:144),
`AcceptAdministratorInvitation` is `PUT /detector/{DetectorId}/administrator`
(serializers.go:44), and `DeclineInvitations`/`DeleteInvitations`/
`GetInvitationsCount` all bind one segment deeper
(`/invitation/decline`/`/invitation/delete`/`/invitation/count`) — none of
which detective's `RouteMatcher` ever claimed.

**Fix**: `services/detective/handler.go`'s `RouteMatcher` pulled the
`pathInvitation` case out of its bare `switch path { case ... }` claim list
into its own SigV4-scoped branch, the same idiom `services/iot/handler.go`
already uses for its `/policies` guard —
`httputils.ExtractServiceFromRequest(c.Request())`, claiming the path only
when the scope is absent or `"detective"`, written with an explicit early
`return false` for a different present scope so `isExclusion`'s "return
false before return true" heuristic recognizes it as a real carve-out rather
than conservatively flagging it. No `MatchPriority` change on either side.

**Before** (pre-fix, `go run ./cmd/routecollisions`, 204 pairs total):

```
detective                [exact "/invitation"        prio=85 reg=143] shadows guardduty                [prefix "/invitation"        prio=-1 reg=84]  (UNGUARDED-WINNER/unguarded)
```

**After** (post-fix, 203 pairs total): the pair no longer appears in the
report at all.

Regression test: `TestInvitationRouting_CrossServiceIsolation`
(`services/detective/invitation_routing_cross_service_test.go`) — a real
`service.NewRegistry` + `service.NewServiceRouter` with both services' real
`Handler`s, driven by both services' real `aws-sdk-go-v2` clients against
one shared `httptest.Server`. Confirmed it fails against the pre-fix
`RouteMatcher` (reproduces the exact `InvalidInputException: unknown
operation` 400 the live repro above found) and passes with the fix; a
guardduty-signed `ListInvitations` now reaches guardduty, and a
detective-signed `AcceptInvitation` against an unknown graph still reaches
detective (`ResourceNotFoundException`, proving the guard didn't
overcorrect).

See `services/detective/PARITY.md` and `services/guardduty/PARITY.md`'s
matching 2026-09-12 entries for the full gate results.
