---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: iotwireless
sdk_module: aws-sdk-go-v2/service/iotwireless@v1.54.7   # version audited against
last_audit_commit: d1235ad5                              # HEAD when this manifest was written
last_audit_date: 2026-07-23
overall: A                # all 4 prior gaps + 9 deferred families field-diffed and fixed this pass
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  AssociateAwsAccountWithPartnerAccount: {wire: ok, errors: ok, state: ok, persist: ok, note: "was unreachable — routed PUT /partner-accounts/{id}, real op is POST /partner-accounts with Sidewalk.AmazonId in body; fixed"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "was unreachable — routed /tags/{arn} path segment, real op is bare POST /tags with resourceArn query param + []Tag body; fixed"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "same /tags routing fix as TagResource"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "same /tags routing fix; response now []Tag{Key,Value} not a bare map"}
  GetWirelessDevice: {wire: ok, errors: ok, state: ok, persist: ok, note: "ThingArn/ThingName were tracked by the backend but never surfaced — fixed; now also surfaces LoRaWAN/Sidewalk/Positioning"}
  GetWirelessGateway: {wire: ok, errors: ok, state: ok, persist: ok, note: "same ThingArn/ThingName fix as GetWirelessDevice; now also surfaces LoRaWAN"}
  StartBulkAssociateWirelessDeviceWithMulticastGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "was a no-op 204; now associates every device in the account/region with the group (emulates 'all qualifying devices' since there's no QueryString expression evaluator)"}
  StartBulkDisassociateWirelessDeviceFromMulticastGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "was a no-op 204; now clears the group's full device-association set"}
  DisassociateWirelessDeviceFromMulticastGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "routing silently discarded the {WirelessDeviceId} path segment, so calling it for any one device cleared ALL associations for the group; fixed via lastPathSegment() + per-device set removal"}
  DisassociateWirelessDeviceFromFuotaTask: {wire: ok, errors: ok, state: ok, persist: ok, note: "same discarded-path-segment bug as DisassociateWirelessDeviceFromMulticastGroup; fixed"}
  DisassociateMulticastGroupFromFuotaTask: {wire: ok, errors: ok, state: ok, persist: ok, note: "same discarded-path-segment bug; fixed"}
  StartFuotaTask: {wire: ok, errors: ok, state: ok, persist: ok, note: "was corrupting FirmwareUpdateRole by overwriting it with a fabricated status string to fake state; FuotaTask now has a real Status field, transitioned Pending -> FuotaSession_Waiting"}
families:
  WirelessDevice: {status: ok, note: "CRUD + Associate/Disassociate*, Deregister, statistics, data queue, test — all routes verified against real serializers.go SplitURI+Method; Tags wire shape fixed; LoRaWAN/Sidewalk/Positioning now stored and round-tripped (was gap); SendDataToWirelessDevice now captures TransmitMode (was silently dropped, queued messages always reported 0); DeleteWirelessDevice now cascade-cleans thing association, queued messages, and multicast/FUOTA group membership"}
  WirelessGateway: {status: ok, note: "CRUD + certificate/thing association, task, firmware/statistics — routes verified; Tags wire shape fixed; LoRaWAN now stored (GatewayEui/RfRegion/JoinEuiFilters/NetIdFilters/MaxEirp/SubBands/Beaconing) — Create nests it under LoRaWAN, Update's JoinEuiFilters/MaxEirp/NetIdFilters are top-level fields that merge into the same map; DeleteWirelessGateway now cascade-cleans thing/cert association and any pending gateway task"}
  DeviceProfile: {status: ok, note: "Tags wire shape fixed; LoRaWAN/Sidewalk now stored and returned on Get (types.LoRaWANDeviceProfile/SidewalkGetDeviceProfile were previously dropped entirely); List entries correctly narrowed to Arn/Id/Name only (types.DeviceProfile), matching real ListDeviceProfilesOutput"}
  ServiceProfile: {status: ok, note: "Tags wire shape fixed; LoRaWAN now stored and returned on Get (types.LoRaWANGetServiceProfileInfo); List entries correctly narrowed to Arn/Id/Name"}
  Destination: {status: ok, note: "Tags wire shape fixed; CRUD + Update routes verified; no CreatedAt field on the real wire shape (confirmed against GetDestinationOutput) so none added"}
  FuotaTask: {status: ok, note: "Tags wire shape fixed; Start/Update/Disassociate* verified. Field-diffed against GetFuotaTaskOutput and found genuinely missing: CreatedAt (epoch-seconds), Descriptor, FragmentIntervalMS, FragmentSizeBytes, RedundancyPercent, LoRaWAN, and a real Status field (StartFuotaTask was previously faking status by overwriting FirmwareUpdateRole) — all added. List entries correctly narrowed to Arn/Id/Name (types.FuotaTask); Get/List device+multicast-group associations upgraded from single-slot maps to real per-task sets (a task can have multiple of each) with cascade cleanup on delete"}
  MulticastGroup: {status: ok, note: "Tags wire shape fixed. Field-diffed against GetMulticastGroupOutput and found genuinely missing: CreatedAt (epoch-seconds), Description, LoRaWAN — all added. Bulk associate/disassociate now mutate real per-group device-association sets (was gap); per-device disassociate now uses the real path-segment device ID instead of clearing everything; DeleteMulticastGroup cascade-cleans its device-association set and its FUOTA-task associations"}
  NetworkAnalyzerConfiguration: {status: ok, note: "Tags wire shape fixed. Field-diffed against GetNetworkAnalyzerConfigurationOutput/CreateNetworkAnalyzerConfigurationInput and found genuinely missing: TraceContent (LogLevel/MulticastFrameInfo/WirelessDeviceFrameInfo) and MulticastGroups — both were accepted by nothing and always empty; now stored and round-tripped through Create/Get/Update"}
  PartnerAccount: {status: ok, note: "AssociateAwsAccountWithPartnerAccount route+wire rewritten (see ops); Get/Update/Disassociate/List were already correct (PartnerAccountId as path parameter); ListPartnerAccounts previously iterated a Go map with no sort (non-deterministic order across identical calls) — now sorted by AmazonId and paginated"}
  Tags (TagResource/UntagResource/ListTagsForResource): {status: ok, note: "route + wire shape rewritten; see ops"}
  GatewayTask / GatewayTaskDefinition: {status: ok, note: "was deferred; field-diffed against GetWirelessGatewayTaskOutput/CreateWirelessGatewayTaskDefinitionOutput/GetWirelessGatewayTaskDefinitionOutput/ListWirelessGatewayTaskDefinitionsOutput. Found and fixed: TaskCreatedAt was always empty (GatewayTask never recorded a creation time) — now set on CreateWirelessGatewayTask and formatted as an ISODateTimeString (this field is a *string on the wire, not a smithy timestamp, confirmed via the deserializer using plain string decode); CreateWirelessGatewayTaskDefinition's Update object (LoRaWAN current/update firmware version, UpdateDataRole, UpdateDataSource) was silently accepted and dropped — now stored and returned on Get; ListWirelessGatewayTaskDefinitions entries wrongly included Name/AutoCreateTasks — real types.UpdateWirelessGatewayTaskEntry carries only Arn/Id/LoRaWAN, fixed; CreateWirelessGatewayTaskOutput doesn't model WirelessGatewayId, trimmed the response to match"}
  WirelessDeviceImportTask / SingleWirelessDeviceImportTask: {status: ok, note: "was deferred; field-diffed against GetWirelessDeviceImportTaskOutput/StartSingleWirelessDeviceImportTaskOutput. Found and fixed: CreationTime was completely missing from Get/List responses — added, formatted as an ISODateTimeString (smithytime.ParseDateTime — a string, NOT epoch-seconds, unlike FuotaTask/MulticastGroup's CreatedAt which IS epoch-seconds; confirmed by reading both deserializer branches directly rather than assuming a single convention service-wide). SingleWirelessDeviceImportTask has no dedicated Get operation in the real API (confirmed: no api_op_GetSingleWirelessDeviceImportTask.go exists) so the current create-only implementation is already complete, not partial"}
  Position / PositionConfiguration / PositionEstimate / ResourcePosition: {status: ok, note: "was deferred; field-diffed against GetPositionOutput and found a real bug: Accuracy was modeled as a bare *float64, but the real shape is types.Accuracy{HorizontalAccuracy, VerticalAccuracy} — a client would have failed to deserialize this field entirely. Fixed to the correct object shape. GetPositionConfiguration/PutPositionConfiguration/ListPositionConfigurations/GetPositionEstimate/GetResourcePosition/UpdateResourcePosition field names confirmed correct via opaque-map echo (Solvers/Destination/GeoJsonPayload); ListPositionConfigurations now paginated"}
  EventConfiguration: {status: ok, note: "was deferred; field-diffed against GetEventConfigurationByResourceTypesOutput/GetResourceEventConfigurationOutput/ListEventConfigurationsOutput — ConnectionStatus/DeviceRegistrationState/Join/MessageDeliveryStatus/Proximity field names confirmed correct; opaque-map echo of each nested *XxxEventConfiguration sub-object is faithful since these are simple enable/disable objects. ListEventConfigurations now paginated"}
  LogLevels: {status: ok, note: "was deferred; field-diffed against GetLogLevelsByResourceTypesOutput/UpdateLogLevelsByResourceTypesInput and found a real bug: FuotaTaskLogOptions/WirelessDeviceLogOptions/WirelessGatewayLogOptions were accepted by UpdateLogLevelsByResourceTypes and silently dropped — GetLogLevelsByResourceTypes always echoed empty arrays regardless of what was set. Fixed: backend now has a real LogLevelsConfig carrying all four fields. Also trimmed GetResourceLogLevelOutput to just LogLevel — the prior response fabricated ResourceType/ResourceId fields that aren't in the real output shape"}
  MetricConfiguration / GetMetrics: {status: ok, note: "was deferred; field-diffed against GetMetricConfigurationOutput/GetMetricsOutput — SummaryMetric.Status and the per-query QueryId/QueryStatus/MetricName echo are field-name-correct. This backend doesn't ingest telemetry to aggregate real Values/Dimensions/Timestamps, so GetMetrics intentionally returns an empty Values array per query rather than fabricating aggregation results — this is a documented, deliberate partial-fidelity emulation, not a stub (no fabricated data is ever returned)"}
  ServiceEndpoint: {status: ok, note: "field-diffed against GetServiceEndpointOutput — ServiceType/ServiceEndpoint/ServerTrust match exactly; no changes needed"}
  SendDataToWirelessDevice / SendDataToMulticastGroup / QueuedMessages: {status: ok, note: "was deferred; field-diffed against SendDataToWirelessDeviceInput/DownlinkQueueMessage. Found and fixed: TransmitMode was accepted by SendDataToWirelessDeviceInput but never captured into the queued QueuedMessage, so every ListQueuedMessages entry reported TransmitMode 0 regardless of what was sent — now captured. DownlinkQueueMessage's MessageId/ReceivedAt/TransmitMode field names confirmed correct (LoRaWAN sub-object omitted, matching the no-fabrication principle: this backend has no router metadata to report). SendDataToMulticastGroup confirmed already minimal-and-correct: real AWS also returns only {MessageId}, and there is no reachable read-back API for multicast group sent data"}
  errors (global): {status: ok, note: "writeError now sets X-Amzn-Errortype header + __type body field derived from HTTP status (404->ResourceNotFoundException, 400->ValidationException, 403->AccessDeniedException, 409->ConflictException, 429->ThrottlingException, else->InternalServerException). Every error path in the service routes through writeError, so this is a single-point fix covering all ops."}
  pagination (List* ops): {status: ok, note: "was gap; every List* op (ListWirelessDevices, ListWirelessGateways, ListServiceProfiles, ListDeviceProfiles, ListDestinations, ListFuotaTasks, ListMulticastGroups, ListMulticastGroupsByFuotaTask, ListNetworkAnalyzerConfigurations, ListPositionConfigurations, ListEventConfigurations, ListPartnerAccounts, ListWirelessGatewayTaskDefinitions, ListWirelessDeviceImportTasks, ListQueuedMessages) now honors maxResults/nextToken via a shared paginateQuery helper (pkgs/page), against a deterministically sorted slice"}
  locking (InMemoryBackend): {status: ok, note: "was gap; InMemoryBackend.mu is now *lockmetrics.RWMutex (was a raw sync.RWMutex), matching the project's coarse-instrumented-lock convention. All ~110 Lock()/RLock() call sites across every <family>.go file were labeled with their enclosing method name as the metrics operation label"}
deferred: []                # none — every family from the prior pass was field-diffed this pass; see families above
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "LoRaWAN/Sidewalk/Update/TraceContent nested configuration objects (on WirelessDevice, WirelessGateway, DeviceProfile, ServiceProfile, MulticastGroup, FuotaTask, GatewayTaskDefinition, NetworkAnalyzerConfiguration) are stored as opaque map[string]any rather than individually typed and validated per nested sub-struct (ABP/OTAA session keys, FPorts, Beaconing, SubBands, etc.). This is real, non-fabricated state — whatever JSON object a client sends is persisted and echoed back verbatim, and update ops merge rather than wholesale-replace — but it does not validate required-subfield presence or enum values the way a hand-modeled struct would. Field names at the top level (LoRaWAN, Sidewalk, Update, TraceContent) are all confirmed correct against the SDK."
  - "ListWirelessDevices does not implement the DestinationName/DeviceProfileId/ServiceProfileId/FuotaTaskId/MulticastGroupId/WirelessDeviceType query-parameter filters that ListWirelessDevicesInput accepts — every call returns the full account/region device set (a completeness gap, not a wrong-data bug: each call's returned data is still accurate, just unfiltered). Note this is a real, reachable AWS filter capability (not the same class of gap as StartBulkAssociate's unfilterable QueryString, which has no structured representation at all) — worth a dedicated pass since ListFuotaTaskDeviceIDs/ListMulticastGroupDeviceIDs now exist and could back the FuotaTaskId/MulticastGroupId filters directly."
leaks: {status: clean, note: "no goroutines/janitors in this service; all state is plain in-memory maps/store.Table under the single mu *lockmetrics.RWMutex, released on Reset(). DeleteWirelessDevice/DeleteWirelessGateway/DeleteMulticastGroup/DeleteFuotaTask now cascade-clean every dependent association map (thing associations, queued messages, multicast/FUOTA membership sets, gateway tasks) so no ghost row survives a parent resource's deletion — this was NOT the case before this pass."}
---

## Notes

Freeform: AWS-behavior specifics worth remembering, and any "looks-wrong-but-correct"
traps so the next auditor doesn't re-flag them.

- **Protocol**: restjson1 (REST paths, not X-Amz-Target header dispatch). Field names on
  the wire are **PascalCase** ("Arn", "Id", "Name", ...) — this is unusual among AWS
  REST-JSON services (most use camelCase) but is IoT Wireless's real wire shape, confirmed
  against `aws-sdk-go-v2/service/iotwireless@v1.54.7`'s generated (de)serializers. Do not
  "fix" this to camelCase.

- **Tags wire shape**: every op that accepts/returns `Tags` on IoT Wireless — every
  `Create*`, `AssociateAwsAccountWithPartnerAccount`, `TagResource`,
  `ListTagsForResource` — uses `[]Tag{Key,Value}` (an array of key/value objects), **never**
  a bare `{"k":"v"}` JSON object map. Before this audit every Tags field in this handler was
  typed `map[string]string`, which fails to unmarshal a real client's `[{"Key":...}]` array
  (the whole request 400s, not just the Tags field — encoding/json aborts the struct decode
  on any field type mismatch it can't skip past cleanly here since the check is
  `if err != nil { return 400 }` immediately after Unmarshal). Fixed via
  `tags_wire.go`'s `tagKVsToMap`/`tagMapToKVs`, backed by `pkgs/tags.KV`. If you add a new Tags
  field to any future op, use `[]tags.KV`, not `map[string]string`.

- **`/tags` is a bare, fixed path** — `POST|GET|DELETE /tags`, never `/tags/{arn}`. The
  resource ARN travels as the `resourceArn` query parameter (confirmed via
  `awsRestjson1_serializeOpHttpBindingsTagResourceInput` etc., which call
  `encoder.SetQuery("resourceArn")`, never a URI path segment). `UntagResource`'s `tagKeys`
  is also a query parameter (repeated `tagKeys=a&tagKeys=b`), which was already handled
  correctly.

- **`AssociateAwsAccountWithPartnerAccount` is `POST /partner-accounts` with no path
  parameter** — the partner account ID is `Sidewalk.AmazonId` in the JSON body. This is the
  one partner-accounts op that does NOT bind `PartnerAccountId` as a `{PartnerAccountId}`
  URI segment; `GetPartnerAccount`/`UpdatePartnerAccount`/
  `DisassociateAwsAccountFromPartnerAccount` all do bind it as a path segment and were
  already correct before this audit.

- **`GetWirelessDevice`/`GetWirelessGateway` include `ThingArn`/`ThingName`** — derived from
  the IoT Thing ARN's last `/`-separated segment (`thingNameFromArn` in handler.go), since
  `AssociateWirelessDeviceWithThing`/`AssociateWirelessGatewayWithThing` requests only ever
  carry `ThingArn`, never `ThingName` — AWS derives the name itself. Neither the request nor
  the backend need to store ThingName separately.

- **Error responses need `X-Amzn-Errortype`** — aws-sdk-go-v2's REST-JSON error
  deserializer (`awsRestjson1_deserializeOpError*`) picks the modeled exception type
  (`ResourceNotFoundException`, `ValidationException`, ...) from the `X-Amzn-Errortype`
  response header first, falling back to a `code`/`__type` field in the JSON body
  (`restjson.GetErrorInfo`). Before this audit neither was ever set, so every error from
  this service — including plain 404s — deserialized into an untyped
  `smithy.GenericAPIError{Code: "UnknownError"}` client-side, breaking any
  `errors.As(err, &types.ResourceNotFoundException{})` handling (waiters, retries, most
  application code). Fixed centrally in `writeError`/`awsErrorType` in handler.go — every
  error path in this service already funneled through `writeError`, so this was a
  single-function fix with service-wide effect. Any new error path MUST use `writeError`,
  not a bare `c.JSON`/`WriteHeader`, or it will regress this fix.

- **CreateWirelessDevice/CreateWirelessGateway et al. return only `{Arn, Id}`** (or
  `{Arn, Name}` for name-keyed resources) — this is correct; real AWS's Create*Output shapes
  genuinely omit every other field (confirmed against `api_op_CreateWirelessDevice.go` etc.).
  Do not "fix" this to return the full resource — that would itself be a wire-shape bug in
  the other direction.

- **DeleteMulticastGroup et al. issuing 204 without touching state you'd expect** (e.g.
  `StartBulkAssociateWirelessDeviceWithMulticastGroup`) are intentionally left as documented
  gaps above, not silently "fixed" to fabricate bulk-task tracking that doesn't otherwise
  exist in this backend — see gaps.

- **Two different timestamp wire formats coexist in this service — read the deserializer,
  don't assume one convention.** `FuotaTask.CreatedAt` / `MulticastGroup.CreatedAt` are
  epoch-seconds JSON numbers (`smithytime.ParseEpochSeconds`, via `pkgs/awstime.Epoch`).
  `WirelessDeviceImportTask.CreationTime` / `GetWirelessGatewayTaskOutput.TaskCreatedAt` /
  `GetMulticastGroupSessionOutput.LoRaWAN.SessionStartTime` are ISO8601 **strings**
  (`smithytime.ParseDateTime`, plain `*string` on the Go SDK type, formatted here with
  `time.RFC3339`). This is genuinely per-field, not per-op or per-family — confirmed by
  reading each `awsRestjson1_deserializeOpDocument*Output` switch case directly. Do not
  "fix" one to match the other.

- **LoRaWAN/Sidewalk/Update/TraceContent nested config objects are stored as opaque
  `map[string]any`** (see `WirelessDevice.LoRaWAN`'s doc comment in models.go for the
  rationale), never as individually typed structs. `copyAnyMap`/`mergeAnyMap` (store.go)
  provide isolation-on-read and partial-merge-on-update semantics respectively — Update
  ops merge the request's top-level keys into the stored map rather than replacing it
  wholesale, matching real AWS's narrower `LoRaWANUpdateDevice`-style Update input shapes
  (which carry fewer fields than the Create shape, e.g. no `DevEui`). This is real,
  round-tripped client state, not fabrication — see gaps for the one thing it doesn't do
  (structural validation of the nested shape).

- **List entries are narrower than Get responses for several families** — real AWS list
  operations often return a stripped-down per-item type (e.g. `types.FuotaTask` /
  `types.DeviceProfile` / `types.ServiceProfile` carry only `Arn`/`Id`/`Name`, while
  `types.UpdateWirelessGatewayTaskEntry` carries only `Arn`/`Id`/`LoRaWAN`) even though the
  singular Get operation for the same resource returns many more fields. Each handler now
  uses a dedicated `*ListEntry`/`taskDefEntry` DTO for List responses, separate from the Get
  DTO — do not consolidate them, that would reintroduce over-inclusive list wire shapes.

- **`{WirelessDeviceId}`/`{MulticastGroupId}` trailing path segments are not carried by
  `parseIoTWirelessPath`'s `(op, resource) string` return** — that function only ever
  returns the top-level `{Id}` path parameter. Per-item disassociate handlers
  (`DisassociateWirelessDeviceFromMulticastGroup`, `DisassociateWirelessDeviceFromFuotaTask`,
  `DisassociateMulticastGroupFromFuotaTask`) recover the trailing sub-resource ID directly
  from the request URL via `lastPathSegment(c)` in routing dispatch (handler.go). Before
  this fix, calling disassociate for any one device/group silently cleared the *entire*
  association set for the parent resource, since the specific child ID was never read.

- **Association state (multicast-group↔device, FUOTA-task↔device, FUOTA-task↔multicast-group)
  is a set, not a single slot** — `map[string]map[string]bool` (`multicastGroupDevices`,
  `fuotaTaskDevices`, `fuotaTaskMulticast` in store.go), backing `ListMulticastGroupDeviceIDs`
  / `ListFuotaTaskDeviceIDs` / `ListMulticastGroupsByFuotaTask`. A prior
  `map[string]string` implementation silently dropped every association but the most
  recently added one for a given parent ID. `backendSnapshot`'s JSON shape for these three
  fields changed accordingly (object-of-arrays, not object-of-strings) —
  `iotwirelessSnapshotVersion` was bumped 1→2 so an old snapshot is cleanly discarded
  instead of partially misdecoded.
