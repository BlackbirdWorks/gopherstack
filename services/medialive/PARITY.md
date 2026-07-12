service: medialive
sdk_module: aws-sdk-go-v2/service/medialive@v1.97.2   # version audited against
last_audit_commit: c21fb0789c8b5b30c40cde9041657e44705ab173
last_audit_date: 2026-07-12
overall: A            # ~600 LOC of genuine fixes found (route table verified 100% clean;
                       # wire-shape casing bug found and fixed for the highest-traffic
                       # families named in scope; same bug class remains open for every
                       # other family -- see gaps)

# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
families:
  RouteMatcher:
    status: ok
    note: >
      Exhaustively diffed all 123 routed ops' (method, path) pairs against
      aws-sdk-go-v2/service/medialive@v1.97.2's serializers.go (every
      awsRestjson1_serializeOp*.HandleSerialize's httpbinding.SplitURI call +
      request.Method). classifyPath's op set matches the SDK's op set exactly
      (123/123), and every path-template + HTTP-method pair matches exactly,
      including tricky List-vs-Create collisions on shared collection paths
      (e.g. GET/POST /prod/channels, /prod/multiplexes, /prod/clusters) and
      sub-action paths (start/stop/accept/cancel/reboot/reject/transfer under
      /prod/inputDevices/{id}/..., channelClass/restartChannelPipelines/
      schedule under /prod/channels/{id}/..., monitor-deployment under
      /prod/signal-maps/{id}/...). No route-matcher bugs found in this
      service -- the class of bug that hit backup/eks/s3control/guardduty/
      cleanrooms/bedrockagent/iotwireless does not reproduce here.
  Channel:
    status: ok
    note: >
      FIXED this pass. CreateChannel/DescribeChannel/UpdateChannel/
      DeleteChannel/ListChannels/StartChannel/StopChannel/UpdateChannelClass/
      RestartChannelPipelines all had PascalCase JSON keys ("Arn"/"Id"/"Name"/
      "ChannelClass"/"RoleArn"/"State"/"Tags", wrapper key "Channel") that
      the real aws-sdk-go-v2 client's restjson1 deserializer -- which
      switches on an exact-case string per generated field ("arn"/"id"/
      "name"/... , wrapper key "channel") -- silently ignores, since Go's
      generic map decode into `map[string]interface{}` preserves the
      original key casing and the deserializer's `switch key { case "arn": }`
      never matches "Arn". Every real SDK client call against
      CreateChannel/DescribeChannel/etc. would have decoded to a
      **zero-value Channel struct** (empty Arn/Id/Name/State, ChannelClass="",
      etc.) despite gopherstack's HTTP response containing the right data.
      Existing unit tests didn't catch this because they parse the response
      as a raw `map[string]any` and assert on the same (wrong) PascalCase
      keys the handler emits -- a textbook "unit tests are not parity proof"
      case (parity-principles.md #3). Fixed by lowercasing channelOutput's
      json tags and the handleListChannels summary map + flipping keyChannel
      to "channel" (verified safe: only 3 call sites, all Channel family).
      Also added the wire's `pipelinesRunningCount` field (was completely
      absent), derived from State+ChannelClass (RUNNING => 2 for STANDARD /
      1 for SINGLE_PIPELINE, else 0) since AWS waiters/clients read it but
      it doesn't gate any real waiter's Retryable function (confirmed by
      reading ChannelCreatedWaiter/ChannelRunningWaiter, which only inspect
      State).
  Input:
    status: ok
    note: >
      Verified only, no changes needed. CreateInput/DescribeInput/
      UpdateInput/DeleteInput/ListInputs already used the correct lowerCamel
      wire keys and "input" wrapper -- this family was implemented correctly
      from the start (unlike Channel/Cluster/etc.), which is why the
      casing bug wasn't a blanket "core vs. parity" split; it tracks which
      individual PR happened to check against the real SDK.
  InputSecurityGroup:
    status: ok
    note: Verified only, no changes needed. Already correct lowerCamel wire keys ("arn"/"id"/"state"/"whitelistRules"/"cidr"), "securityGroup" wrapper.
  Multiplex:
    status: ok
    note: >
      FIXED this pass, same bug class as Channel. multiplexOutput's tags
      ("Tags"/"Arn"/"Id"/"Name"/"State"/"AvailabilityZones"/
      "MultiplexSettings" and the nested MultiplexSettings' "TransportStream
      Bitrate"/"TransportStreamId"/"TransportStreamReservedBitrate"/
      "MaximumVideoBufferDelayMilliseconds") were all PascalCase; fixed to
      lowerCamel, and the same fix applied to the CreateMultiplex/
      UpdateMultiplex REQUEST body parsing (extractMultiplexSettings read
      "MultiplexSettings"/"TransportStreamBitrate"/etc. -- a real SDK
      request would never populate those keys, so CreateMultiplex always
      silently ignored the caller's settings and stored zero values).
      Also added the wire's missing `pipelinesRunningCount` (RUNNING => 2,
      else 0 -- AWS Elemental multiplexes are always a 2-pipeline hitless
      pair) and `programCount` (len(programs), threaded through a new
      Multiplex.ProgramCount / MultiplexSummary.ProgramCount field computed
      in toMultiplex()/toSummary()).
  MultiplexProgram:
    status: ok
    note: >
      FIXED this pass, same bug class. multiplexProgramOutput's
      "ProgramName"/"ChannelId"/"MultiplexProgramSettings" (+ nested
      "ServiceDescriptor"/"ProviderName"/"ServiceName"/
      "PreferredChannelPipeline"/"ProgramNumber") were PascalCase; fixed to
      lowerCamel, plus the corresponding CreateMultiplexProgram/
      UpdateMultiplexProgram request-body parsing (extractMultiplexProgramSettings)
      and the "MultiplexProgram" wrapper key (now "multiplexProgram") and
      ListMultiplexPrograms summary keys.
  Tags:
    status: partial
    note: >
      Two independent bugs, one fixed fully and one fixed for its
      highest-traffic call site. (1) STATE BUG (fixed): CreateTags/
      DeleteTags/ListTagsForResource operated on a b.tags[ARN] map that was
      completely disconnected from the per-resource `.Tags` field that
      Channel/Input/InputSecurityGroup/Multiplex/InputDevice's own
      Describe/Create/List responses echo inline. Tags supplied at
      CreateChannel(tags=...) never appeared in ListTagsForResource, and
      tags added via CreateTags never appeared in DescribeChannel -- the two
      tag stores silently diverged. Fixed by adding
      `taggableResourceTags(arn)` (backend.go), which resolves an ARN to the
      live `.Tags` map of the actual Channel/Input/InputSecurityGroup/
      Multiplex/InputDevice resource (via a generic `findLiveTags` helper)
      and routes CreateTags/DeleteTags/ListTagsForResource through it,
      falling back to the legacy b.tags map for every other (out-of-scope)
      taggable resource family. New test
      TestAudit1_Tags_StaySyncedWithResource proves both directions.
      (2) WIRE BUG (partially fixed): ListTagsForResourceOutput's real key
      is lowercase "tags"; gopherstack used the shared `keyTags = "Tags"`
      constant. Fixed the ListTagsForResource handler specifically (uses a
      local "tags" literal now); the constant itself was intentionally left
      "Tags" because it's shared with every out-of-scope family's own
      Describe/Get responses (Cluster/SignalMap/CWTemplate/EBTemplate/
      Reservation/Network/...) -- flipping it would require fixing all of
      those in the same pass (see gaps below) or their tests would
      regress.
  InputDevice:
    status: partial
    note: >
      State/persistence untouched and already correct. Tag-store sync bug
      (see Tags above) fixed for InputDevice too (cheap, same generic
      helper) since DescribeInputDeviceOutput echoes tags inline in the
      real API. Wire-shape casing (inputDeviceOutput's "Tags"/"Arn"/"Id"/
      "Name"/"SerialNumber"/"MacAddress"/"Type"/"ConnectionState"/
      "DeviceSettingsSyncState"/"DeviceUpdateStatus") is NOT fixed this
      pass -- same bug class as Channel/Multiplex, deferred (see gaps).
      Route/method matching for all InputDevice sub-actions (accept/cancel/
      reboot/reject/start/stop/startInputDeviceMaintenanceWindow/transfer/
      thumbnailData) verified correct against the real serializer table.

# Families out of scope this pass -- deep route/error/state audit not
# performed beyond the global RouteMatcher sweep above and a wire-shape
# casing scan. Each of these families' Describe/Create/List handlers use
# the SAME shared PascalCase key constants (keyArn/keyID/keyName/keyState/
# keyTags/keyDescription/keyAlerts/keyActionName/keyScheduleActions/
# keySdiSource) plus family-specific PascalCase literal map keys
# ("ClusterType"/"InstanceRoleArn"/"GroupId"/"OfferingId"/"AssociatedClusterIds"/
# "NextToken"/... ) that do not match the real restjson1 wire (verified
# lowercase-first for every shape spot-checked: Cluster, Node,
# ChannelPlacementGroup, SignalMap, CloudWatchAlarmTemplate(Group),
# EventBridgeRuleTemplate(Group), Offering, Reservation, Network, SdiSource,
# Batch*, Schedule, ListTagsForResource). This means every op in every
# family below is CLIENT-BREAKING for a real aws-sdk-go-v2 caller today,
# exactly like Channel/Multiplex were before this pass's fix -- it just
# wasn't fixed in this pass because doing so completely (constants +
# ~40 family-specific literal keys + rewriting every assertion in
# handler_cluster_test.go/handler_new_ops_test.go/handler_parity_test.go/
# handler_inputdevice_test.go) is a materially larger, separately-reviewable
# change than the four families named as highest-traffic for this pass.
deferred:
  - Cluster / Node / ChannelPlacementGroup (wire casing gap; DescribeClusterOutput/DescribeNodeOutput have NO "tags" field in the real API even though CreateClusterInput accepts one -- clusterOutput's "Tags" field is spurious, tags for these must flow through ListTagsForResource only)
  - SignalMap / CloudWatchAlarmTemplate(Group) / EventBridgeRuleTemplate(Group) (wire casing gap; these DO echo "tags"/"createdAt"/"modifiedAt" inline in the real API -- gopherstack's models have neither the correct casing nor a CreatedAt/ModifiedAt field at all)
  - Offering / Reservation (wire casing gap)
  - Network / SdiSource (wire casing gap)
  - Batch (BatchStart/BatchStop/BatchDelete/BatchUpdateSchedule): wire casing gap, PLUS a real request-shape bug found but not fixed -- BatchStartInput/BatchStopInput have NO InputIds field in the real API (only channelIds+multiplexIds), so gopherstack's `extractStringSlice(body, "InputIds")` in handleBatchStart/handleBatchStop reads a key a real client never sends; conversely BatchDeleteInput has an InputSecurityGroupIds field gopherstack never parses at all
  - Schedule / Alerts / AccountConfiguration / Versions (wire casing gap)
  - InputDevice wire-shape casing (tag-store sync only was fixed, see families.InputDevice)
  - Deep state/error-code audit of Cluster, Node, SignalMap, Reservation/Offering purchase flow, Batch semantics beyond the wire-casing scan above (route matching for all of them was verified correct; op-by-op state-machine correctness was not re-verified this pass)

gaps:
  - Wire-shape PascalCase-vs-lowerCamel key casing bug affects every op in every family listed under `deferred` above -- same root cause and fix pattern as the Channel/Multiplex fix in this pass, just not yet applied. This is the single highest-value follow-up (bd: needs filing -- no existing bd issue found for this).
  - BatchStart/BatchStop wrongly parse an "InputIds" request field that doesn't exist in the real API (dead code, harmless no-op); BatchDelete is missing InputSecurityGroupIds parsing (real gap -- real clients that batch-delete an InputSecurityGroup get silently ignored) (bd: needs filing).
  - CloudWatchAlarmTemplateGroup/CloudWatchAlarmTemplate/EventBridgeRuleTemplateGroup/EventBridgeRuleTemplate/SignalMap models have no CreatedAt/ModifiedAt fields at all, even though the real API returns them as ISO8601 strings (`__timestampIso8601`, parsed via smithytime.ParseDateTime -- NOT epoch-seconds, unlike some other AWS JSON protocols) (bd: needs filing).

leaks: {status: clean, note: "No goroutines/janitors in this service. Tag-store fix in this pass removes a latent unbounded-growth leak in b.tags[ARN] for Channel/Input/InputSecurityGroup/Multiplex/InputDevice -- those five families' tags now live purely on the resource struct (garbage-collected with it on Delete) instead of also being duplicated into a per-ARN map that Delete never cleaned up."}

---

## Notes

**Wire protocol**: REST-JSON1 (`/prod/...` paths, JSON bodies, HTTP verbs GET/POST/PUT/DELETE/PATCH map 1:1 to List/Create/Update/Delete/Update-partial). No XML anywhere in this service.

**The casing bug, precisely**: aws-sdk-go-v2's restjson1 deserializers decode the
HTTP body into a `map[string]interface{}` via `encoding/json`, then dispatch
per-field with a Go `switch key { case "arn": ... }` -- an *exact string
match*, not a case-insensitive one (unlike a direct
`json.Unmarshal(body, &typedStruct)`, which Go's encoding/json *does*
case-fold). A response field emitted as `"Arn"` never matches `case "arn":`
and silently falls through to the `default: _, _ = key, value` branch,
leaving that field at its Go zero value in the decoded SDK type. This is
NOT caught by any unit test that decodes the response into `map[string]any`
and asserts on the same wrong key the handler emits -- both sides agree with
each other and disagree with reality. The only way to catch it is diffing
against the actual SDK deserializer source (which is what this pass did) or
an SDK-driven integration test.

**Verified-safe scope boundary**: this pass changed `keyChannel` from
"Channel" to "channel" because a full-file grep proved it has exactly 3
call sites, all Channel family. It deliberately did NOT touch keyArn/keyID/
keyName/keyState/keyTags/keyDescription/keyAlerts/keyActionName/
keyScheduleActions/keySdiSource, which are each shared between in-scope and
out-of-scope call sites -- flipping those would have silently "fixed" (by
luck of shared casing) some fields in Cluster/SignalMap/Offering/etc.
responses while leaving their *other*, family-specific PascalCase literal
keys wrong, i.e. moved those families from "fully wrong" to "confusingly,
inconsistently wrong" without actually making any of their ops real-client-
usable, while also breaking their currently-passing (if wrongly-premised)
tests. The follow-up sweep should fix a whole family (constants +
literals + tests) per PR, the same way this pass did for Channel/
Multiplex/MultiplexProgram.

**PipelinesRunningCount / ProgramCount semantics**: derived, not persisted.
Computed at read time from State (+ ChannelClass for Channel) or
len(Programs) (for Multiplex.ProgramCount) -- no new persisted field, no
snapshot-version bump needed. Verified this doesn't gate any real AWS SDK
waiter (`ChannelCreatedWaiter`/`ChannelRunningWaiter` only inspect `State`).

**ARN suffix convention** (for anyone extending `taggableResourceTags`):
each `*ARN(id string) string` builder in backend.go appends
`"<resourceType>:<id>"` as the ARN's resource segment (e.g. `channelARN`
appends `"channel:" + id`). `taggableResourceTags` does NOT parse this --
it does an O(n) linear scan of each candidate table's `.All()` comparing
`.ARN` fields directly, which is simpler and doesn't need to trust the ARN
format to stay parseable. Fine for an in-memory emulator; do not reach for
an ARN-parsing shortcut here without checking whether ID values can ever
contain the delimiter.
