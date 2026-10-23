---
service: budgets
sdk_module: aws-sdk-go-v2/service/budgets@v1.53.1
last_audit_commit: 4ae40e79e
last_audit_date: 2026-10-18
overall: B            # all 26 SDK operations are backed by real state and exercised through the real SDK client; B until the open items below close.
ops:
  CreateBudget: {wire: ok, errors: ok, state: ok, persist: ok, note: "validates type/unit/limit/filters/auto-adjust; default TimePeriod is the current period start to 2087-06-15; CostTypes defaults filled; notifications, subscribers and tags created atomically"}
  CreateBudgetAction: {wire: ok, errors: ok, state: ok, persist: ok, note: "STANDBY on create; definition must match ActionType; role must be an IAM role ARN in the same account; max 10 per budget"}
  CreateNotification: {wire: ok, errors: ok, state: ok, persist: ok, note: "max 5 per budget; identity is type+operator+threshold+thresholdType"}
  CreateSubscriber: {wire: ok, errors: ok, state: ok, persist: ok, note: "max 11 per notification; SNS address must be a topic ARN, EMAIL a valid address"}
  DeleteBudget: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades to actions"}
  DeleteBudgetAction: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteNotification: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteSubscriber: {wire: ok, errors: ok, state: ok, persist: ok, note: "the last subscriber cannot be removed"}
  DescribeBudget: {wire: ok, errors: ok, state: ok, persist: ok, note: "CalculatedSpend derived from the Cost Explorer ledger; FilterExpression returned only with ShowFilterExpression"}
  DescribeBudgetAction: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeBudgetActionHistories: {wire: ok, errors: ok, state: ok, persist: ok, note: "newest first, TimePeriod filter"}
  DescribeBudgetActionsForAccount: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeBudgetActionsForBudget: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeBudgetNotificationsForAccount: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeBudgetPerformanceHistory: {wire: ok, errors: ok, state: ok, persist: ok, note: "one entry per period (monthly 13, daily 60, quarterly 4, annual 2) clipped to the budget start; actual from the ledger"}
  DescribeBudgets: {wire: ok, errors: ok, state: ok, persist: ok, note: "name-ordered pages, NextToken validated, MaxResults 1-1000"}
  DescribeNotificationsForBudget: {wire: ok, errors: ok, state: ok, persist: ok, note: "NotificationState OK/ALARM reflects the last evaluation"}
  DescribeSubscribersForNotification: {wire: ok, errors: ok, state: ok, persist: ok}
  ExecuteBudgetAction: {wire: ok, errors: ok, state: ok, persist: ok, note: "APPROVE (PENDING), RETRY (EXECUTION_FAILURE), REVERSE (EXECUTION_SUCCESS/REVERSE_FAILURE), RESET (back to STANDBY); runs the IAM/Organizations/SSM executor outside the lock"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "budget and action ARNs"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateBudget: {wire: ok, errors: ok, state: ok, persist: ok, note: "replaces the definition, keeps notifications, actions and tags"}
  UpdateBudgetAction: {wire: ok, errors: ok, state: ok, persist: ok, note: "partial update returning old and new; ResourceLockedException while in progress"}
  UpdateNotification: {wire: ok, errors: ok, state: ok, persist: ok, note: "keeps subscribers and state"}
  UpdateSubscriber: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  Budget: {status: ok, note: "Timestamps are epoch-seconds numbers (awsjson1.1 default). Wire types double as the persisted model."}
  Alerting: {status: ok, note: "Notifications are evaluated on budget writes, on describe calls and by a one-minute background evaluator; an OK to ALARM transition publishes once to SNS subscribers and emails EMAIL subscribers through SES."}
  Actions: {status: ok, note: "State machine STANDBY -> PENDING (manual) or EXECUTION_IN_PROGRESS (automatic) -> EXECUTION_SUCCESS/FAILURE -> REVERSE_* -> STANDBY; every transition appends an ActionHistory entry. Without a wired executor an execution ends EXECUTION_FAILURE with that reason in the history."}
  Spend: {status: partial, note: "COST/USAGE budgets read the Cost Explorer ledger (Service, Region, UsageType, LinkedAccount, TagKeyValue filters); forecast is a linear projection."}
  Tags: {status: ok, note: "Budgets and budget actions are taggable; unknown ARNs return NotFoundException."}
gaps: []
items_still_open:
  - CalculatedSpend: spend comes from the Cost Explorer ledger, which is synthetic seed data (90 days), not real usage
  - CalculatedSpend: RI/Savings Plans utilization and coverage budgets report 0 PERCENTAGE; no utilization source is wired
  - CostFilters: AZ, InstanceType, Operation, PurchaseType, UsageTypeGroup, etc. do not narrow spend (the ledger does not model them); FilterExpression is flattened to And-of-Dimensions only, Or/Not/Tags/CostCategories are ignored for spend
  - CostTypes: Include* flags exclude ledger entries by charge type (ce CostEntry.ChargeType, RECORD_TYPE-style values), but the synthetic seed ledger only holds Usage entries, so flags change spend only for entries added with RecordCharge; the flag-to-charge-type mapping (Subscription, Fee, Discount) is unverified
  - Alerting: topic policies granting budgets.amazonaws.com are not checked; evaluator cadence is 1 minute versus AWS several times a day
  - ExecuteBudgetAction: ExecutionRoleArn must exist in IAM when the action runs (a missing role fails the execution) but its trust policy is never evaluated or assumed; SSM actions start AWS-Stop*/Start* automation documents through the SSM backend
  - BillingViewArn: format-checked only; HealthStatus is always HEALTHY
  - AccessDeniedException, ThrottlingException, InternalErrorException, ExpiredNextTokenException, BillingViewHealthStatusException, ServiceQuotaExceededException: never raised
structural_gaps: []
deferred: []
leaks: {status: clean, note: "one worker.Group ticker bound to the janitor context; unit tests never start it"}
---

## Notes
Protocol awsjson1.1, X-Amz-Target prefix AWSBudgetServiceGateway. Budgets is a global service homed in us-east-1: ARNs are arn:aws:budgets::acct:budget/name and .../action/id.
An action key is acct/name/action/id, which is why budget names may not contain "/action/".
Percentage thresholds compare spend to the limit of the current period (planned limits resolved by period offset).
Wiring lives in internal/app/budgets_wiring.go (Cost Explorer ledger, SNS/SES, IAM/Organizations/SSM).
