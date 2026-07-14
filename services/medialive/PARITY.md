service: medialive
sdk_module: aws-sdk-go-v2/service/medialive@v1.97.2   # version audited against
last_audit_commit: 066717f8a6524d92673f3364ce570fdcbaefec1a
last_audit_date: 2026-07-12
overall: A            # Parity sweep 4: the wire-shape casing bug (PascalCase-vs-lowerCamel)
                       # that was fixed for Channel/Multiplex/MultiplexProgram/Tags in the
                       # prior pass is now fixed for EVERY remaining family in this service
                       # -- Cluster, Node, ChannelPlacementGroup, SignalMap,
                       # CloudWatchAlarmTemplate(Group), EventBridgeRuleTemplate(Group),
                       # Offering, Reservation, Network, SdiSource, Batch*, Schedule,
                       # Alerts/AccountConfiguration/Versions, and InputDevice's own
                       # output-struct casing. Also fixed: BatchStart/BatchStop parsing a
                       # nonexistent InputIds field (dead code) while BatchDelete was
                       # missing InputSecurityGroupIds parsing (real gap), and
                       # SignalMap/CloudWatchAlarmTemplate(Group)/EventBridgeRuleTemplate
                       # (Group) missing createdAt/modifiedAt entirely. medialive's
                       # wire-shape casing gap (this service's single largest outstanding
                       # parity issue as of the prior pass) is now closed.

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
      FIXED in a prior pass. CreateChannel/DescribeChannel/UpdateChannel/
      DeleteChannel/ListChannels/StartChannel/StopChannel/UpdateChannelClass/
      RestartChannelPipelines all had PascalCase JSON keys ("Arn"/"Id"/"Name"/
      "ChannelClass"/"RoleArn"/"State"/"Tags", wrapper key "Channel") that
      the real aws-sdk-go-v2 client's restjson1 deserializer -- which
      switches on an exact-case string per generated field ("arn"/"id"/
      "name"/... , wrapper key "channel") -- silently ignores. Fixed by
      lowercasing channelOutput's json tags and the handleListChannels
      summary map + flipping keyChannel to "channel". This pass (sweep 4)
      merged the shared keyArn/keyID/keyName/keyState/keyTags constants
      (previously PascalCase, kept separate from Channel's own wireArn/
      wireID/wireName/wireState) into a single lowerCamel set now that
      every family in the service uses the same casing -- see "Notes"
      below.
  Input:
    status: ok
    note: >
      Verified only, no changes needed. CreateInput/DescribeInput/
      UpdateInput/DeleteInput/ListInputs already used the correct lowerCamel
      wire keys and "input" wrapper.
  InputSecurityGroup:
    status: ok
    note: Verified only, no changes needed. Already correct lowerCamel wire keys ("arn"/"id"/"state"/"whitelistRules"/"cidr"), "securityGroup" wrapper.
  Multiplex:
    status: ok
    note: >
      FIXED in a prior pass, same bug class as Channel. multiplexOutput's
      tags and the nested MultiplexSettings were all PascalCase; fixed to
      lowerCamel, including the CreateMultiplex/UpdateMultiplex REQUEST body
      parsing. Also added the wire's missing `pipelinesRunningCount` and
      `programCount`.
  MultiplexProgram:
    status: ok
    note: >
      FIXED in a prior pass, same bug class. multiplexProgramOutput and its
      nested settings were PascalCase; fixed to lowerCamel, plus the
      corresponding request-body parsing and the "MultiplexProgram" wrapper
      key (now "multiplexProgram").
  Tags:
    status: partial
    note: >
      Two independent bugs from the prior pass, one fixed fully and one
      fixed for its highest-traffic call site at the time (now fully
      resolved, see below). (1) STATE BUG (fixed): CreateTags/DeleteTags/
      ListTagsForResource operated on a b.tags[ARN] map disconnected from
      the per-resource `.Tags` field. Fixed via `taggableResourceTags(arn)`
      (backend.go) + `findLiveTags` helper. (2) WIRE BUG (now fully fixed):
      ListTagsForResourceOutput's real key is lowercase "tags"; the prior
      pass fixed only the ListTagsForResource handler's literal (leaving
      the shared `keyTags` constant as "Tags" for other families). This
      pass (sweep 4) flipped `keyTags` itself to "tags" now that every
      family sharing it has been fixed to the same casing -- see Notes.
  InputDevice:
    status: ok
    note: >
      Tag-store sync bug (see Tags above) was fixed in a prior pass and NOT
      touched this pass. Wire-shape casing (inputDeviceOutput's field tags,
      the ListInputDevices "InputDevices" wrapper, the
      ListInputDeviceTransfers "InputDeviceTransfers" wrapper and its item
      keys, and "MaintenanceWindowActive") is FIXED this pass -- all
      lowerCamel now ("tags"/"arn"/"id"/"name"/"serialNumber"/"macAddress"/
      "type"/"connectionState"/"deviceSettingsSyncState"/
      "deviceUpdateStatus"/"maintenanceWindowActive", wrappers
      "inputDevices"/"inputDeviceTransfers"). Deliberately NOT touched this
      pass (out of scope per this sweep's task boundary): request-body
      parsing for ClaimDevice/UpdateInputDevice/TransferInputDevice (still
      reads PascalCase "Id"/"TargetCustomerId"/"TargetRegion"/
      "TransferMessage" -- verified these ARE the wrong casing vs the real
      TransferInputDeviceInput serializer, which uses "targetCustomerId"/
      "targetRegion"/"transferMessage"; tracked as a residual gap below).
      Route/method matching for all InputDevice sub-actions verified
      correct in the prior pass.
  Cluster:
    status: ok
    note: >
      FIXED this pass. clusterOutput's "Tags"/"Arn"/"Id"/"Name"/
      "ClusterType"/"InstanceRoleArn"/"State" were PascalCase; fixed to
      lowerCamel ("arn"/"id"/"name"/"clusterType"/"instanceRoleArn"/
      "state"). The real DescribeClusterOutput/CreateClusterOutput/
      UpdateClusterOutput have NO "tags" field at all (verified against the
      SDK deserializer) even though CreateClusterInput accepts tags --
      dropped Tags from clusterOutput entirely; Cluster tags now only
      surface via ListTagsForResource, matching AWS. Added the wire's
      "channelIds" field (real API field gopherstack never emitted at all)
      as a derived empty list -- gopherstack doesn't track cluster-channel
      association, tracked as a residual gap below. Request-body parsing
      fixed too: handleCreateCluster read "ClusterType"/"InstanceRoleArn"
      (PascalCase, never matches a real client's lowerCamel body) --
      CreateCluster silently ignored the caller's clusterType/
      instanceRoleArn before this fix. ListClusters' summary map and
      wrapper ("Clusters" -> "clusters") fixed too. ListClusterAlerts'
      synthetic alert also had entirely wrong field names ("AlertCode"/
      "AlertMessage"/"SetTime"/"ClearedTime" -- none of which exist on the
      real ClusterAlert shape); rewritten to the real field names (id/
      alertType/message/state/setTimestamp).
  Node:
    status: ok
    note: >
      FIXED this pass, same bug class as Cluster. nodeOutput's PascalCase
      keys fixed to lowerCamel ("arn"/"id"/"name"/"clusterId"/"role"/
      "state"/"connectionState"). Real DescribeNodeOutput/CreateNodeOutput/
      UpdateNodeOutput/UpdateNodeStateOutput have NO "tags" field either
      (same as Cluster) -- dropped. Added the wire's "channelPlacementGroups"
      field as a derived empty list (gopherstack doesn't track it; residual
      gap below). Request-body parsing fixed: handleCreateNode/
      handleUpdateNode read "Role" (should be "role"; CreateNodeInput's
      real field is lowerCamel), handleUpdateNodeState read "State" (should
      be "state"). ListNodes summary map and wrapper ("Nodes" -> "nodes")
      fixed too.
  ChannelPlacementGroup:
    status: ok
    note: >
      FIXED this pass, same bug class. toChannelPlacementGroupOutput's
      "ClusterId"/"Channels"/"Nodes" fixed to lowerCamel ("clusterId"/
      "channels"/"nodes"), verified against the real
      DescribeChannelPlacementGroupOutput shape (arn/channels/clusterId/id/
      name/nodes/state -- no tags field, matches gopherstack's model which
      never had one). Request-body parsing fixed: handleCreate/
      UpdateChannelPlacementGroup read "Nodes" (should be "nodes"). List
      wrapper ("ChannelPlacementGroups" -> "channelPlacementGroups") fixed.
  SignalMap:
    status: ok
    note: >
      FIXED this pass. toSignalMapOutput's PascalCase keys fixed to
      lowerCamel ("discoveryEntryPointArn"/"status"/
      "monitorDeploymentStatus"/"cloudWatchAlarmTemplateGroupIds"/
      "eventBridgeRuleTemplateGroupIds"). Added "createdAt"/"modifiedAt"
      (previously missing entirely from the model) -- confirmed ISO8601
      string wire form via smithytime.ParseDateTime in the SDK deserializer
      (NOT epoch-seconds); new SignalMap.CreatedAt/ModifiedAt time.Time
      fields, stamped on Create/StartUpdateSignalMap/
      StartMonitorDeployment/StartDeleteMonitorDeployment, rendered via
      new `formatISO8601` helper (RFC3339, omits empty on zero-value).
      Request-body parsing fixed: handleCreateSignalMap/
      handleStartUpdateSignalMap read "DiscoveryEntryPointArn"/
      "CloudWatchAlarmTemplateGroupIdentifiers"/
      "EventBridgeRuleTemplateGroupIdentifiers" (all PascalCase -- verified
      the real CreateSignalMapInput/StartUpdateSignalMapInput serializers
      use lowerCamel). ListSignalMaps wrapper ("SignalMaps" ->
      "signalMaps") fixed.
  CloudWatchAlarmTemplateGroup:
    status: ok
    note: >
      FIXED this pass. toCWAlarmTemplateGroupOutput's keys fixed to
      lowerCamel. Added "createdAt"/"modifiedAt" (ISO8601 strings, same
      confirmation as SignalMap). Note: the real Get/Create/Update
      responses have NO "templateCount" field -- only the List response's
      Summary shape does (verified against the SDK deserializer); List
      still reuses this same output function and so is still missing
      templateCount (tracked as a residual gap below -- would need a new
      backend method to count templates per group). List wrapper
      ("CloudWatchAlarmTemplateGroups" -> "cloudWatchAlarmTemplateGroups")
      fixed.
  CloudWatchAlarmTemplate:
    status: ok
    note: >
      FIXED this pass. toCWAlarmTemplateOutput's keys fixed to lowerCamel.
      Dropped "groupIdentifier" and "namespace" from the output entirely --
      neither is a real field on GetCloudWatchAlarmTemplateOutput/
      CreateCloudWatchAlarmTemplateOutput/UpdateCloudWatchAlarmTemplateOutput
      (verified against the SDK deserializer: only "groupId" is returned,
      no "namespace" at all). Added "createdAt"/"modifiedAt" (ISO8601).
      Request-body parsing fixed: extractCWAlarmTemplateFields read
      "GroupIdentifier"/"MetricName"/"Namespace"/"Statistic"/
      "ComparisonOperator"/"TargetResourceType"/"TreatMissingData"/
      "Threshold"/"EvaluationPeriods"/"DatapointsToAlarm"/"Period" (all
      PascalCase) -- CreateCloudWatchAlarmTemplate/
      UpdateCloudWatchAlarmTemplate silently ignored every one of these
      caller-supplied fields before this fix. List wrapper
      ("CloudWatchAlarmTemplates" -> "cloudWatchAlarmTemplates") fixed.
  EventBridgeRuleTemplateGroup:
    status: ok
    note: >
      FIXED this pass, same shape as CloudWatchAlarmTemplateGroup (no
      templateCount on Get/Create/Update, only on List's Summary shape --
      same residual gap, see CloudWatchAlarmTemplateGroup). Added
      "createdAt"/"modifiedAt". List wrapper
      ("EventBridgeRuleTemplateGroups" -> "eventBridgeRuleTemplateGroups")
      fixed.
  EventBridgeRuleTemplate:
    status: ok
    note: >
      FIXED this pass. toEBRuleTemplateOutput's keys fixed to lowerCamel
      ("groupId"/"eventType"/"eventTargets"). Dropped "groupIdentifier" --
      not a real field on this shape (only "groupId" is returned, verified
      against the SDK deserializer). Added "createdAt"/"modifiedAt". Note:
      the real List response's Summary shape returns "eventTargetCount"
      instead of the full "eventTargets" array -- gopherstack's List still
      reuses the Get/Create/Update shape and so over-returns the full
      target list instead of a count (tracked as a residual gap below).
      Request-body parsing fixed: handleCreate/UpdateEBRuleTemplate read
      "GroupIdentifier"/"EventType" (PascalCase) and extractEBTargets read
      "EventTargets" (PascalCase) -- all silently ignored caller input
      before this fix. List wrapper ("EventBridgeRuleTemplates" ->
      "eventBridgeRuleTemplates") fixed.
  Offering:
    status: ok
    note: >
      FIXED this pass. toOfferingOutput's keys fixed to lowerCamel. Added
      the wire's "region" field (real DescribeOfferingOutput/Offering HAS
      a region field; gopherstack's Offering model never tracked it) --
      new Offering.Region field, populated in seedOfferings from the
      backend's configured region. Confirmed the real shape has NO "name"
      and NO "tags" field (gopherstack's model never had either, so nothing
      to drop). List wrapper ("Offerings" -> "offerings") fixed.
  Reservation:
    status: ok
    note: >
      FIXED this pass, same bug class. toReservationOutput's keys fixed to
      lowerCamel. PurchaseOffering's response wrapper ("Reservation" ->
      "reservation") fixed. UpdateReservation's response was NOT wrapped at
      all before this fix -- the real UpdateReservationOutput wraps in
      "reservation" (verified against the SDK deserializer) while
      DescribeReservationOutput/DeleteReservationOutput do NOT wrap (bare
      top-level fields) -- fixed handleUpdateReservation to wrap, left
      Describe/Delete unwrapped (both already matched). Request-body
      parsing fixed: handlePurchaseOffering read "Count" (should be
      "count"). List wrapper ("Reservations" -> "reservations") fixed. Not
      added: the real "renewalSettings" field (gopherstack's Reservation
      model doesn't track renewal settings at all; tracked as a residual
      gap below since it's a new field, not a casing fix).
  Network:
    status: ok
    note: >
      FIXED this pass. toNetworkOutput's keys fixed to lowerCamel
      ("associatedClusterIds"/"ipPools"/"routes"). The nested IPPool/Route
      Go structs' own json tags were also PascalCase ("Cidr"/"Gateway") --
      fixed to lowerCamel since they're marshaled directly as nested
      objects. Request-body parsing fixed: extractIPPools/extractRoutes
      read "IpPools"/"Routes"/"Cidr"/"Gateway" (all PascalCase) -- silently
      ignored caller input before this fix. List wrapper ("Networks" ->
      "networks") fixed.
  SdiSource:
    status: ok
    note: >
      FIXED this pass. toSdiSourceOutput's keys fixed to lowerCamel
      ("type"/"mode"/"inputs"); "sdiSource" wrapper key already correct via
      the shared `keySdiSource` constant (now flipped from "SdiSource" to
      "sdiSource"). Request-body parsing fixed: handleCreate/
      UpdateSdiSource read "Type"/"Mode" (PascalCase). List wrapper
      ("SdiSources" -> "sdiSources") fixed.
  Batch:
    status: ok
    note: >
      FIXED this pass -- both the wire-casing gap AND the two non-casing
      bugs PARITY.md previously flagged. (1) CASING: toBatchResultOutput's
      wrapper ("Successful"/"Failed" -> "successful"/"failed") and item
      keys ("Code" -> "code", added "message") fixed. BatchUpdateSchedule's
      "Creates"/"Deletes"/"ScheduleActions"/"ActionName"/"ActionNames" all
      fixed to lowerCamel (request AND response). (2) REQUEST-SHAPE BUG
      (fixed): verified against api_op_BatchStart.go/api_op_BatchStop.go/
      api_op_BatchDelete.go and their serializers -- BatchStartInput/
      BatchStopInput have ONLY ChannelIds+MultiplexIds (wire:
      channelIds/multiplexIds), NO InputIds field at all.
      BatchStart/BatchStop's Go signatures changed from
      `(channelIDs, inputIDs, multiplexIDs []string)` to
      `(channelIDs, multiplexIDs []string)` -- the dead inputIDs parameter
      is gone, not just ignored. BatchDeleteInput DOES have all four:
      ChannelIds+InputIds+MultiplexIds+InputSecurityGroupIds (wire:
      channelIds/inputIds/multiplexIds/inputSecurityGroupIds) --
      handleBatchDelete now parses "inputSecurityGroupIds" (previously
      never parsed at all: a real client batch-deleting an
      InputSecurityGroup was silently ignored) and
      InMemoryBackend.BatchDelete gained a 4th parameter + a new
      `batchDeleteInputSecurityGroups` helper. New test
      TestBatch_DeleteInputSecurityGroups proves the fix end-to-end.
  Schedule:
    status: ok
    note: >
      FIXED this pass. DescribeSchedule's wrapper (keyScheduleActions:
      "ScheduleActions" -> "scheduleActions") and item key (keyActionName:
      "ActionName" -> "actionName") fixed via the shared constants (safe:
      grepped all call sites, all Batch/Schedule family, all in scope this
      pass).
  Alerts:
    status: ok
    note: >
      FIXED this pass. ListAlerts/ListMultiplexAlerts/ListClusterAlerts'
      wrapper (keyAlerts: "Alerts" -> "alerts") fixed via the shared
      constant. ListAlerts/ListMultiplexAlerts always return an empty list
      in this emulator (unchanged, no per-item casing to fix). See Cluster
      above for the ListClusterAlerts synthetic-alert field-name fix.
  AccountConfiguration:
    status: ok
    note: >
      FIXED this pass. Describe/UpdateAccountConfiguration's wrapper
      ("AccountConfiguration" -> "accountConfiguration") and inner key
      ("KmsKeyId" -> "kmsKeyId") fixed, both response AND request-body
      parsing (handleUpdateAccountConfiguration read the same wrong keys).
  Versions:
    status: ok
    note: >
      FIXED this pass. ListVersions' wrapper ("Versions" -> "versions") and
      item keys ("Version"/"ExpirationDate" -> "version"/"expirationDate")
      fixed. Also fixed a latent decode-breaking bug: expirationDate is
      __timestampIso8601 (smithytime.ParseDateTime) on the real wire, and
      gopherstack always emitted it as "" (channelEngineVersion has no
      real expiration data) -- a real SDK client would fail to parse ""
      as a timestamp. Now omits the key entirely when empty instead of
      emitting an unparseable "".
  Offering-Reservation-shared:
    status: ok
    note: See Offering and Reservation above.

# Families out of scope this pass (nothing left deferred at the family
# level -- every family named in the sweep-4 task is now `ok`). Remaining
# work is residual, sub-family gaps, listed below.
deferred: []

gaps:
  - InputDevice request-body parsing (ClaimDevice/UpdateInputDevice/TransferInputDevice)
    still reads PascalCase keys ("Id"/"TargetCustomerId"/"TargetRegion"/"TransferMessage")
    that don't match the real lowerCamel request shape -- verified wrong against the SDK
    serializer, deliberately left alone this pass per the task's InputDevice scope boundary
    (output-struct casing only). (bd: needs filing)
  - Cluster.ChannelIds / Node.ChannelPlacementGroups are real wire fields gopherstack now
    emits (matching the real shape) but always as an empty list -- gopherstack doesn't track
    cluster<->channel or node<->channel-placement-group association. Correctness gap, not a
    casing gap. (bd: needs filing)
  - CloudWatchAlarmTemplateGroup/EventBridgeRuleTemplateGroup List responses are missing
    "templateCount" (real Summary-shape field); CloudWatchAlarmTemplate/EventBridgeRuleTemplate
    Get/Create/Update responses already correctly omit it (verified not present on those
    shapes). Would need a new backend method to count templates per group. (bd: needs filing)
  - EventBridgeRuleTemplate List response returns the full "eventTargets" array per item
    instead of the real Summary shape's "eventTargetCount" integer. (bd: needs filing)
  - Reservation model doesn't track "renewalSettings" (a real field on
    DescribeReservationOutput/Reservation) at all -- PurchaseOffering/UpdateReservation accept
    but discard any renewalSettings the caller sends. New-field gap, not a casing gap. (bd:
    needs filing)
  - Deep state/error-code audit of Cluster, Node, SignalMap, Reservation/Offering purchase
    flow, Batch semantics beyond the wire-casing scope of this pass and sweep 4's fixes above
    was not re-performed (route matching for all of them was verified correct; op-by-op
    state-machine correctness beyond what this pass touched was not re-verified).

leaks: {status: clean, note: "No goroutines/janitors in this service. No new persisted maps added this pass -- CreatedAt/ModifiedAt/Region are plain fields on existing per-resource structs, garbage-collected with their owning resource on Delete same as every other field."}

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
leaving that field at its Go zero value in the decoded SDK type. Request
bodies have the mirror-image bug: a handler reading `body["GroupIdentifier"]`
when a real client always sends `"groupIdentifier"` silently no-ops on every
real caller's input. This pass (sweep 4) closed this bug class for every
remaining family in the service -- see the per-family notes above for the
specific keys fixed in each.

**Shared constants, now fully unified**: as of this pass, `keyArn`/`keyID`/
`keyName`/`keyState`/`keyTags`/`keyDescription`/`keyAlerts`/`keyActionName`/
`keyScheduleActions`/`keySdiSource` are ALL lowerCamel ("arn"/"id"/"name"/
"state"/"tags"/"description"/"alerts"/"actionName"/"scheduleActions"/
"sdiSource"), and the previously-separate `wireArn`/`wireID`/`wireName`/
`wireState` constants (introduced in the prior pass for Channel/Multiplex/
Input's already-correct casing) were deleted and their call sites repointed
at the now-identical `keyArn`/`keyID`/`keyName`/`keyState` -- there is no
longer a "these two constant sets differ" trap for future readers to fall
into, since every family in the service uses the same lowerCamel casing.
`keyMessage` ("Message", PascalCase, used only for the shared `respondErr`
error-response body) was deliberately left untouched -- error-response
casing was out of scope for this pass and no family's fix depended on it.

**Timestamp wire form, confirmed empirically this pass**: SignalMap/
CloudWatchAlarmTemplate(Group)/EventBridgeRuleTemplate(Group)'s
createdAt/modifiedAt, and ChannelAlert/ClusterAlert/MultiplexAlert's
setTimestamp/clearedTimestamp, and ListVersions' expirationDate are all
`__timestampIso8601`, deserialized via `smithytime.ParseDateTime` (grepped
directly in aws-sdk-go-v2/service/medialive@v1.97.2's deserializers.go for
every shape touched this pass) -- an ISO8601/RFC3339 string, NOT epoch
seconds (`pkgs/awstime.Epoch` does NOT apply here; that helper is for
services using the unixTimestamp wire form, which medialive's restjson1
protocol does not default to for these specific shapes). New helper
`formatISO8601` (handler.go) renders `time.Time` via `time.RFC3339`,
omitting the key when the time is zero-valued (a real SDK client would fail
to parse `""` as a timestamp -- this bit ListVersions' expirationDate before
this pass's fix).

**PipelinesRunningCount / ProgramCount semantics** (Channel/Multiplex,
prior pass, unchanged): derived, not persisted. Computed at read time from
State (+ ChannelClass for Channel) or len(Programs) (for
Multiplex.ProgramCount).

**ARN suffix convention** (for anyone extending `taggableResourceTags`):
each `*ARN(id string) string` builder in backend.go appends
`"<resourceType>:<id>"` as the ARN's resource segment. `taggableResourceTags`
does an O(n) linear scan of each candidate table's `.All()` comparing `.ARN`
fields directly.
