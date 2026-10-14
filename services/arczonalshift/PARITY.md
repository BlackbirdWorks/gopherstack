---
service: arczonalshift
sdk_module: aws-sdk-go-v2/service/arczonalshift@v1.34.1
last_audit_commit: a65519ce2
last_audit_date: 2026-10-13
overall: B
ops:
  StartZonalShift: {wire: ok, errors: ok, state: ok, persist: ok, note: "ExpiresIn 1m-72h; zone name or zone ID; ConflictException ZonalShiftAlreadyExists / SimultaneousZonalShiftsConflict; status ACTIVE until the expiry time passes, CANCELED on cancel, EXPIRED afterwards, all derived from the clock"}
  UpdateZonalShift: {wire: ok, errors: ok, state: ok, persist: ok}
  CancelZonalShift: {wire: ok, errors: ok, state: ok, persist: ok}
  ListZonalShifts: {wire: ok, errors: ok, state: ok, persist: ok}
  ListManagedResources: {wire: ok, errors: ok, state: ok, persist: ok, note: "ELBv2 Application and Network Load Balancers of the request region"}
  GetManagedResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateZonalAutoshiftConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePracticeRunConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePracticeRunConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePracticeRunConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  StartPracticeRun: {wire: ok, errors: ok, state: ok, persist: ok, note: "checks blocking and outcome alarms (CloudWatch), blocked dates and windows, allowed windows; runs for 30 minutes by default (--lifecycle-arczonalshift); outcome PENDING then SUCCEEDED, FAILED if an outcome alarm is in ALARM when it ends, INTERRUPTED when canceled"}
  CancelPracticeRun: {wire: ok, errors: ok, state: ok, persist: ok, note: "only practice runs; CancelZonalShift only customer shifts"}
  ListAutoshifts: {wire: ok, errors: ok, state: ok, persist: ok, note: "AWS initiates autoshifts from internal zone telemetry; this emulator never does, so the list is always empty after validating the status filter"}
  GetAutoshiftObserverNotificationStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAutoshiftObserverNotificationStatus: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  route_matching: {status: ok, note: "matches the SigV4 signing name or SDK user agent plus a known path shape"}
  errors: {status: ok, note: "REST-JSON error bodies carry both message and Message, X-Amzn-Errortype and the modeled reason"}
gaps: []
items_still_open:
  - StartZonalShift/ListManagedResources: Auto Scaling groups and EKS clusters are supported resource types in AWS but are not listed as managed resources here
structural_gaps:
  - ListAutoshifts always empty: autoshifts are initiated by AWS from internal Availability Zone health telemetry, which cannot exist in an emulator
deferred: []
leaks: {status: clean, note: "no goroutines or janitors; expiry and lifecycle are derived from the clock at read time"}
---

## Notes

- Protocol: REST-JSON (schema-serde client), lowerCamel members, epoch-seconds timestamps, SigV4 signing name `arc-zonal-shift`.
- Managed resources are the ELBv2 Application/Network Load Balancers of the request region, read live; their zones come from the load balancer and are reported as zone IDs (`use1-az1`).
- appliedStatus follows the documented precedence: customer zonal shift over autoshift over practice run. appliedWeights are 0 for zones with an applied shift away from them, otherwise 1.
- Practice-run outcome is settled the first time the run is observed after it ends.

