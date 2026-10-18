---
service: oam
sdk_module: aws-sdk-go-v2/service/oam@v1.31.4
last_audit_commit: 64eb45859
last_audit_date: 2026-10-18
overall: B            # all 15 SDK operations are backed by real state and driven through the real SDK client in-package; B until the open items below close.
ops:
  CreateLink: {wire: ok, errors: ok, state: ok, persist: ok, note: "sink policy evaluated with the IAM evaluator (principal account, aws:PrincipalOrgID, ForAllValues oam:ResourceTypes); LabelTemplate variables resolved; one link per source account, Region and sink (ConflictException)"}
  CreateSink: {wire: ok, errors: ok, state: ok, persist: ok, note: "name pattern [a-zA-Z0-9_.-]+; ConflictException on duplicate name; ServiceQuotaExceededException beyond one sink per account and Region"}
  DeleteLink: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteSink: {wire: ok, errors: ok, state: ok, persist: ok, note: "ConflictException while links are attached"}
  GetLink: {wire: ok, errors: ok, state: ok, persist: ok, note: "Tags only with IncludeTags"}
  GetSink: {wire: ok, errors: ok, state: ok, persist: ok, note: "lookup by ARN or ID; Tags only with IncludeTags"}
  GetSinkPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "ResourceNotFoundException when no policy is set"}
  ListAttachedLinks: {wire: ok, errors: ok, state: ok, persist: ok, note: "links of the caller's sink, from any source account"}
  ListLinks: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSinks: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "sink or link ARN of the caller account and Region"}
  PutSinkPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "Allow statements only, Principal required, actions must be oam: actions"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "50 tags, aws: prefix reserved"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateLink: {wire: ok, errors: ok, state: ok, persist: ok, note: "ResourceTypes replace the set; sink policy re-evaluated for oam:UpdateLink; LinkConfiguration kept when omitted"}
families:
  Sink: {status: ok, note: "Sinks belong to the monitoring account and Region; links to a sink are looked up by sink ARN across accounts."}
  Link: {status: ok, note: "Links belong to the source account and Region; filters are parsed (IN, NOT IN, LIKE, NOT LIKE joined by AND/OR) and must match the link's resource types."}
  SinkPolicy: {status: ok, note: "Statements are filtered by Principal and passed to services/iam EvaluatePolicies with oam:resourcetypes and aws:principalorgid in the condition context."}
items_still_open:
  - SinkPolicyDenied: a policy that does not allow the caller is reported as InvalidParameterException because OAM models no access-denied exception; the live error code is not verified
  - Quotas: the one sink per account and Region limit and the one link per source account, Region and sink limit follow AWS documentation and are not verified against the live API
  - LabelTemplate: without an Organizations directory $AccountName, $AccountEmail and $AccountEmailNoDomain all resolve to the account ID
  - CloudFormation: AWS::Oam::Sink and AWS::Oam::Link are not provisioned by the CloudFormation emulator
gaps: []
structural_gaps:
  - "No cross-account data plane: linked metrics, log groups and traces are not shared into the monitoring account because the emulated CloudWatch, Logs and X-Ray backends have no cross-account query path."
deferred: []
leaks: {status: clean, note: "leak_main_test.go runs goleak over the package; no goroutines or timers."}
---

## Notes

Protocol is restjson1: every operation is `POST /<Operation>` except the tag operations on `/tags/{ResourceArn}` (GET list, PUT tag, DELETE untag with repeated `tagKeys`). Routing matches on the `oam` SigV4 signing name or the SDK user agent plus the path shape, so it cannot swallow another service's `/tags/` requests.

Sink and link IDs are UUIDs; ARNs are `arn:aws:oam:<region>:<account>:sink/<id>` and `link/<id>`. Quota errors use HTTP 402.
