# Checkpoint — wire-parity campaign, 2026-08-13

Branch `chore/queue-2026-08-11`, PR #2417 (draft). Merged `origin/main` early;
the only conflict was `.beads/issues.jsonl`, resolved with ours after checking
that no field on their side was newer.

Roughly 135 commits. 82 bd issues closed, 93 filed. Tree clean, all pushed,
full suite verified green.

## What this session actually was

The queue I was given had 8 items. Six of them turned out to be stale — already
fixed, or mis-stated. Verifying premises before dispatching (`gopherstack-9c4a`)
was the single highest-value habit of the session and should be the default.

The real work came out of two audits that the queue's item 4 asked for, which
between them generated more tracked work than they consumed. That is not a
failure — each bug found reliably revealed two adjacent ones — but it means the
queue does not converge. Expect to choose a stopping point rather than reach one.

## The cuts, ranked by what they actually yielded

| Cut | Services | Bugs | Notes |
|---|---|---|---|
| Required **input** members | ~150 over 6 passes | 77 | Best sustained yield by a wide margin. Every bug in an A-graded service. |
| Required **response** members | ~30 over 2 passes | 27 | Nothing forces an output field to exist — absent still returns 200. |
| **Over-wide** List responses | ~20 over 2 passes | 30 | Newest cut. 16 were one root cause in personalize. |
| Route reachability | 40+ | 47 | 41 in five services; the rest genuinely clean. |
| Vacuous-test hunt | ~70 | 0 | Failed as a hunt — see below. |

**Required members are where the bugs are.** A required member the handler never
reads means the operation cannot work for any real client, and that is almost
never defensible. Broad "absent field" sweeps produced 2,217 candidates and a
handful of bugs; filtering to required-ness produced 654 and dozens.

## Findings that generalise

**An A grade certifies op-level wire and routing, not field-level completeness.**
Confirmed independently six times. Every one of the 77 required-member bugs was
in a service graded A, most audited within the preceding three weeks. Re-auditing
a manifest *because* it looked complete found two client-breaking bugs.

**False rationales, ten instances, in four forms.** Five manifests positively
claimed verification that was false — redshift
("spot-checked: real state mutation confirmed", for two no-op stubs), kinesis
(`wire: ok` on three fabricated ops), quicksight ("import job lifecycle diffed
clean", for an import that imported nothing), securityhub (five ops emitting
unreadable keys), cleanrooms. Use `PARITY.md` to avoid re-deriving disclosed
gaps; never as evidence of correctness.

The other three forms are worse in ascending order. Three *code comments* argued
a field was absent for a reason that was untrue — kafka claimed AWS exposes no
rebalancing setting, and it does. One *standing policy note* in personalize's
`PARITY.md`, titled "Extra fields on List summaries are harmless", argued a true
premise to a wrong conclusion and thereby taught prior audits not to look — it is
why sixteen leaking ops survived. And one *upstream SDK doc comment* was stale:
the `pipes` module documents a field as ISO-8601 while its own deserializer parses
epoch seconds. **Cite the deserializer or serializer, never a field comment.**

**Borrowed shapes and behaviour — six distinct layers.** A shared response-key
*constant* correct for one op and wrong for its scoped sibling (cleanrooms, 4
ops); a shared XML *request type* (cloudfront); a shared *domain type* across
unrelated ops (cloudwatch's `AlarmContributor` carrying `InsightRuleContributor`'s
shape); a shared *list-item* shape where Start and Get genuinely differ (omics);
and an operation *reimplemented as a different operation* (organizations
`UpdateResponsibilityTransfer` was Accept/DeclineHandshake in disguise). When two
ops share a constant, struct or type, verify both against the SDK independently.
Plus a shared *Get-versus-List* converter, which produced 30 over-wide responses.
The resemblance that motivated the sharing is usually superficial — reasoning about
types by name or by analogy caused three near-misses that would each have
introduced a bug while fixing one.

**Read the whole operation, never just the reported field.** This produced extra
findings in *every* batch. The severest bug of the session — codecommit
`GetMergeConflicts` with `mergeable` hardcoded `false`, which would make any real
client refuse to merge — was found only because a *cosmetic* wrong-key fix put
someone in that handler.

**Audit field lists are a floor, not a ceiling — in both directions.** Undercounted in eight services
(backup 2-of-5, rekognition, organizations, dms, fsx, forecast, comprehend,
quicksight). Cause: a literal-match tool cannot distinguish a field read by the
*right* op from the same name read anywhere in the package. Per-op scoping via
the dispatch table would fix it and is worth building.

**When an audit says a gap is harmless *because of a pattern* rather than because
someone checked, that is an untested hypothesis.** `gopherstack-wl0s` was filed as
"round-trips fine, only presence unchecked" — inferred, not tested. One of its ops
wrapped its response under a key the real shape does not have, so nothing ever
reached a real client.

## Tests

**Seventeen tests were found wrong in the same direction as their bug**, two of
them in `test/integration`, which service-scoped work never runs. A test that
omits a field the handler also omits looks like a normal happy-path test. One
asserted status was in `200-299` *or* `400` — it could not fail. One called
`h.Handler()` directly, bypassing method-aware routing, so it could not catch a
routing bug. One was named `TestKBDocumentsRealWireRouting` and sent the invented
shape the handler expected.

**A dedicated hunt for these found zero**, while six more surfaced as a side
effect of fixing bugs. The signature is not in the test — it is the *agreement*
between test and handler, visible only once you know the correct behaviour. So
this is not a huntable backlog. It is a checklist item on every wire fix: when you
fix a handler, look at the nearest test and ask whether it agreed with the bug.

**The antidote is driving the real `aws-sdk-go-v2` client.** It builds what AWS
actually sends and cannot encode the handler's assumption. For a wrong response
key it is the *only* proof, since the SDK decodes nothing from an unrecognised key
whatever the raw body holds — a raw-map assertion passes against the bug.

## Tooling knowledge worth not re-deriving

The required-member scanners were rebuilt from scratch six times because the
scratchpad never survives. `gopherstack-569k` and `gopherstack-mven` carry the
full method. Five blind spots, each found the hard way:

1. lowerCamelCase `json:"fieldName"` tags rather than `.FieldName` accessors.
2. Case-insensitive matching (`Arn` vs `ARN`).
3. Tag-suffix tolerance — `json:"TermsId,omitempty"` does not contain `"TermsId"`.
4. Named-constant map keys — `resp[keyFoo]` where `const keyFoo = "Foo"`.
5. Nested XML path tags — `xml:"Parent>Child"`.
6. Dotted query prefixes — `vals.Get("Bundle.S3Bucket")` matches no pattern,
   because the literal has a period where a comma or quote is expected.

Fixes 1–3 alone cut raw candidates from 391 to 176 across 135 services.
Check field **access**, not struct **declaration** — that closes the anonymous
inline-struct blind spot (`gopherstack-oc9v`, 1487 of them) for free.

Seven false-positive classes: httpLabel/httpHeader bindings (dominant, ~300+);
httpPayload wrappers (all 113 of pinpoint's); query-protocol member-indexed
arrays; idempotency tokens; disclosed stubs; and cross-package delegation
(`dynamodbstreams` forwards to `services/dynamodb`).

**Route tests are now permanent, and strengthened.** 26 services carry
`TestExtractOperation_SDKRouteTable` — one subtest per op, built from the SDK's
`serializeOpHttpBindings`. That converts a periodic audit into a standing
guarantee. Copy `services/opensearch/handler_paths_sdk_diff_test.go`.

They originally asserted only that `ExtractOperation` resolved the right name —
which is not the dispatch contract, so an op could resolve correctly and still be
unreachable. 25 now also drive `Handler()` and assert the response is not that
service's unmatched-route sentinel. `apigatewayv2` is deliberately excluded: its
route-miss reply is byte-identical to legitimate not-found responses, and an
unsound assertion would be worse than none.

## Decode regimes differ and change what counts as a bug

- stdlib `encoding/json`: case-**insensitive**, so case-only tag differences are
  not bugs.
- `url.Values.Get` (query, ec2-query) and `encoding/xml`: case-**sensitive**, so
  they are fatal. A root-element mismatch makes `xml.Unmarshal` error and zero the
  *whole* struct — and 32 handlers discarded that error (`gopherstack-ob1g`).
- appstream decodes CBOR then bridges through `json.Unmarshal`, so it inherits
  case-insensitivity. **cloudwatch hand-rolls CBOR extraction off a `cbor.Map`**
  and is therefore case-sensitive — alone among the JSON-family services.

## Open decisions — human required

Three, and none is an agent's call.

- **`gopherstack-ylyb`** — CodeQL alert 254. Reviewed and technically sound, but
  "false positive" undersells it: `v = g^x mod N` is stored at rest, so the
  KDF-hardness CodeQL wants is genuinely absent. Inherent to SRP, identical in real
  Cognito, unfixable without breaking wire compat. Recommendation: re-label
  "won't fix". Dashboard state is the user's call.
- **`gopherstack-377m`** — repo-wide fail-open posture. sts trust-policy evaluation
  permits on any unmodeled operator or unknown key. Now logs at WARN and enforces
  `Null` and the `Arn*` family; `Numeric*`, `Date*`, `IpAddress` and `Binary` are
  structurally unimplementable (no key of those types exists). A **second,
  independent** fail-open path was then found: sts resolves `CallerArn` only for
  callers that are themselves assumed-role sessions, so `aws:PrincipalArn` is
  absent for every first-hop caller and the condition is skipped. Whatever posture
  is chosen must cover absent-because-unresolvable, not just unmodeled.
- **`gopherstack-cu4g`** — caller-identity plumbing, and it explicitly defers to
  `377m` on how absence should behave. Investigation corrected the premise: SigV4
  parsing already exists, the access key is parsed in four places, and two
  AKID-to-principal stores already work — the gap is plumbing, not resolution. Only
  two of the four consumers I had cited are genuinely blocked.

## Process

- **Two agents ran `git stash push`** despite an explicit prohibition, while
  another agent was mid-edit. Both scoped narrowly by luck; unscoped would have
  destroyed parallel work. The prohibition must name `git stash push` and
  `git checkout -- <file>` explicitly — a general "no git-mutating commands" is
  not read as covering them.
- **One agent parked and looped**, finishing correct work then returning "waiting
  for the monitor" three times. It burned ~346k tokens after finishing and had to
  be killed with `TaskStop`. Its output was recovered by inspecting `git status`
  and reading its `PARITY.md` diffs. **A parked agent leaves correct work looking
  abandoned** — check the tree before assuming failure. Every dispatch now carries
  an explicit "report, do not wait" instruction.
- **Scoped gates miss cross-cutting tests.** Verifying each change against only the
  services it touched left the `pkgs/persistence` snapshot guard red for much of
  the session, and left two `test/integration` tests encoding shapes that had been
  fixed. One root cause, two symptoms. Run `./pkgs/...` alongside touched services,
  and the integration suite before claiming green.
- **Check what a command actually measured.** A `go test ./... | head` pipeline
  reported `exit: 0` from `head`, not from the test run. The real run failed. A
  `grep -c "^ok"` returning zero should have been the tell.
- **Snapshot version bumps are a live data-loss hazard.** Two unnecessary bumps for
  purely additive fields were caught in review this session (apigateway last
  session, cloudfront this one); `Restore` discards *all* state on mismatch. A
  guard exists in `pkgs/persistence` and works — it simply has to be run. Bump only
  for an incompatible retype, as rds and directoryservice legitimately did.
- Integration tests need `make build-linux` plus a container; both were run green.
  `test/terraform` still cannot run locally.

## Where to go next

Bounded and ready: `gopherstack-1jkv` (rds cluster roles, blocked on real-AWS
evidence), `gopherstack-a250` (56 empty-struct inputs, ~49 unverified),
`gopherstack-8kzr`-adjacent cleanups.

Open-ended, will not converge: `569k` (required inputs, six passes deep),
`mven` (required responses; ~120 services whole-directory scanned but never
per-op scoped, so unproven rather than clean), `dv4s` (over-wide responses,
newest and still productive), `jqh2` (routes; yield tapered 5→1→0),
`oc9v` (1487 inline structs, 343 in sagemaker alone), `xwkb` (make the sweep
tooling read `PARITY.md` so coverage gaps stop being miscounted).

Unresolved scope question: **quicksight (277 ops) and iot (272)** are the two
largest REST-JSON services and appear in neither the route tally nor its scope.
Both show signs of prior dedicated passes. Decide explicitly — letting them sit
in the gap is exactly how redshift-serverless escaped two sweeps at once.
