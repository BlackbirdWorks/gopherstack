---
service: appintegrations
sdk_module: aws-sdk-go-v2/service/appintegrations@v1.52.0
last_audit_commit: 88c2622c4
last_audit_date: 2026-10-18
overall: B            # all 23 SDK operations are backed by real state and verified through the SDK client in-package; B until the open items below close.
ops:
  CreateApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "Namespace unique; ApplicationSourceConfig required; AuthType and ContactHandling scope enums checked; ClientToken replays"}
  CreateDataIntegration: {wire: ok, errors: ok, state: ok, persist: ok, note: "one DataIntegration per name; ScheduleConfig expression must be rate(...) or cron(...); ClientToken replays; lookups accept Id, ARN or name"}
  CreateDataIntegrationAssociation: {wire: ok, errors: ok, state: ok, persist: ok, note: "a DataIntegration takes one association (InvalidRequestException afterwards); ExecutionConfiguration mode needs its matching configuration"}
  CreateEventIntegration: {wire: ok, errors: ok, state: ok, persist: ok, note: "name and bus pattern-checked, EventFilter.Source must be aws.partner/...; bus existence checked against EventBridge when wired; ClientToken replays"}
  DeleteApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "ConflictException while associations exist unless force=true"}
  DeleteDataIntegration: {wire: ok, errors: ok, state: ok, persist: ok, note: "refused with InvalidRequestException while an association exists (code is an emulator choice, not in the SDK docs)"}
  DeleteEventIntegration: {wire: ok, errors: ok, state: ok, persist: ok, note: "refused with InvalidRequestException while an association exists (code is an emulator choice, not in the SDK docs)"}
  GetApplication: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDataIntegration: {wire: ok, errors: ok, state: ok, persist: ok}
  GetEventIntegration: {wire: ok, errors: ok, state: ok, persist: ok}
  ListApplicationAssociations: {wire: ok, errors: ok, state: ok, persist: ok, note: "associations are created through Connect CreateIntegrationAssociation (type APPLICATION)"}
  ListApplications: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDataIntegrationAssociations: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDataIntegrations: {wire: ok, errors: ok, state: ok, persist: ok}
  ListEventIntegrationAssociations: {wire: ok, errors: ok, state: ok, persist: ok, note: "associations are created through Connect CreateIntegrationAssociation (type EVENT) and own an EventBridge rule when EventBridge is wired"}
  ListEventIntegrations: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateApplication: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDataIntegration: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDataIntegrationAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateEventIntegration: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  EventIntegration: {status: ok, note: "Associations are owned by clients (Connect); each one creates an EventBridge rule on the integration bus matching the partner source and removes it on delete."}
  DataIntegration: {status: ok, note: "FileConfiguration, ObjectConfiguration and ScheduleConfig round-trip; association summaries carry ExecutionConfiguration."}
  Application: {status: ok, note: "Permissions, Publications, Subscriptions, IframeConfig, AuthConfig and ApplicationConfig round-trip; the SDK at this version has no separate permission operations."}
  Tags: {status: ok, note: "Event integrations, data integrations and applications are taggable by ARN; 200 tags, keys without the aws: prefix."}
gaps: []
items_still_open:
  - "DataIntegrationAssociation.LastExecutionStatus: never emitted because no AppFlow execution exists"
  - "KmsKey: stored as given, not looked up in the KMS emulator"
  - "CreateDataIntegrationAssociation duplicate: the code for a second association of one DataIntegration is not documented; the emulator uses InvalidRequestException"
  - "ResourceGroupsTaggingAPI and CloudFormation do not expose or provision AppIntegrations resources"
structural_gaps:
  - "No AppFlow data plane: data integrations never pull data, so executions, schedules and file configuration have no effect beyond being stored."
deferred: []
leaks: {status: clean, note: "leak_main_test.go runs goleak over the package; there are no goroutines or timers."}
---

## Notes

Protocol is REST-JSON (SigV4 signing name `app-integrations`); shapes and the exception set were read from the pinned SDK schemas and types. Timestamps are epoch-seconds numbers (`awstime.EpochTime`). ARNs are `arn:aws:app-integrations:<region>:<account>:event-integration/<name>`, `data-integration/<uuid>`, `application/<uuid>` and `...association/<uuid>`.

Resources live in one namespace keyed by name or ID, not per Region; the ARN keeps the creating Region. Pagination uses the opaque offset token with MaxResults 1-50.
