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

**Status after two passes: all 163 registered services triaged, 4 confirmed
bugs found and fixed** (the original securityhub/inspector2/macie2 one, plus
three more in the second pass's "Second pass" section below —
`apigateway`/`quicksight` on `/account/`, `appconfigdata`/`omics` on
`/configuration`, `inspector2`/`omics` on `/configuration/`), **2 tool false
positives disproven by driving the real router** (`batch`/`kafka` on bare
`/v1/`, first pass; `polly`/`appsync`/`batch` on `/v1/`, second pass), and
every remaining candidate hand-verified clean via a real disambiguation
mechanism. `cmd/routecollisions` now extracts claims for 53 of 94 path-based
services directly plus recognizes 14 more as structurally immune
(Query/EC2-protocol body match) — 67 of 94 tool-covered; the other 27 were
hand-read (helper-function delegation and route-table map keys the
extractor doesn't chase yet — see "Second pass" below for exactly which and
why).

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
- Helper-function-body delegation (`return isXPath(path)` to a predicate
  defined elsewhere in the package) and map/route-table literal-key
  extraction (`rgRESTPathOps["/resources/search"]`,
  `h.routes()["POST create-app"]`) are still not chased at all — every
  service relying on either shape (`omics`, `apigateway`, `backup`,
  `codeartifact`, `elasticsearch`, `opensearch`, `account`, `resourcegroups`,
  `resiliencehub`, `networkmanager`, `mgn`) was hand-read this pass instead
  (see "Second pass" above) rather than guessed at. A future pass could add
  this as a one-hop call-graph chase: collect every top-level func/method
  body and package-level `var`-composite-literal body in the package (not
  just `RouteMatcher`/`MatchPriority`), then when `RouteMatcher`'s body
  calls or indexes a name found in that table, recursively run
  `extractClaims` on its body text too (bounded depth + a visited set for
  cycle safety). Scoped out of this pass for time, not difficulty.
