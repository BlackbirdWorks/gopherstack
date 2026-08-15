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
   don't get reported as claims. **It extracted claims for 44 of the 94
   path-based services.** The other 50 are NOT yet covered — see
   "Remaining scope" below; their absence from the pair list below is a
   tooling gap, not a clean bill of health.
4. For every pair of claims across the 44 covered services, ordered by
   effective router evaluation order (`MatchPriority()` descending, then
   registration order), it flags a candidate when the earlier-evaluated
   claim is a literal string-prefix of (or identical to) the later one —
   i.e. the earlier service would actually intercept the later service's
   request. This produced **76 candidate pairs** (see `-json` output for
   the full per-claim detail); every one was read by hand against the
   real source below.

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

## Remaining scope — NOT yet swept

**50 path-based services never produced an extracted claim and were not
manually triaged this pass:** account, acm, acmpca, apigateway, appmesh,
appstream, autoscaling, backup, cloudfront, cloudfrontkeyvaluestore,
cloudwatch, codeartifact, cognitoidp, docdb, ec2, ecr, elasticbeanstalk,
elasticsearch, elb, elbv2, glacier, iam, lakeformation, lambda,
mediaconvert, mediastoredata, mgn, mq, mwaa, neptune, networkmanager, omics,
opensearch, personalize, polly, quicksight, ram, rds, rdsdata, redshift,
resiliencehub, resourcegroups, route53, s3, sagemakerruntime, ses, sesv2,
sns, sqs, sts.

Why the tool missed them (a future pass should extend the generator rather
than re-deriving this by hand): dispatch via a `map[string]service.
JSONOpFunc` keyed by exact path (ec2/ecr/rds-style consoles, not simple
`HasPrefix` chains the regex extractor follows); routing through a shared
sub-router/dispatch table in another file the per-directory scan didn't
walk; matching on the *method-string-hash* of a full literal path passed to
a switch (glacier/s3-style); or a `RouteMatcher` that delegates to a helper
function in a different file/package the const-and-body extraction doesn't
chase. None of this is evidence they're safe — some of the highest-risk
services for exactly this bug class are in this list (ec2, iam, s3, route53,
rds/rdsdata/redshift's REST-ish surfaces, and the four form-encoded/
path-subdomain-tier services from `pkgs/service/priorities.go`'s own doc
comment: docdb/neptune/redshift/opensearch/elasticsearch, which the comment
itself flags as "could overlap with form-encoded services"). **Start here.**

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
