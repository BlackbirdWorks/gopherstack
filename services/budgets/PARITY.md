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
  - CalculatedSpend: metering other services means editing their backends and ce Accrue (services/ce/spend.go:60), out of scope for this pass; spend is metered from emulated resources only for EC2 instances, EBS volumes, S3 storage and requests, Lambda and DynamoDB on-demand units; every other service contributes the 90-day synthetic seed catalogue
  - CalculatedSpend: RI_UTILIZATION and RI_COVERAGE are computed from EC2 reservations, but nothing seeds EC2 ReservedInstancesOfferings (services/ec2/reserved_instances.go:112) so no reservation can be purchased through the API (seeding the catalogue is an EC2 backend change); SAVINGS_PLANS_UTILIZATION and SAVINGS_PLANS_COVERAGE report 0 PERCENTAGE because no Savings Plans service exists
  - Alerting: topic policies granting budgets.amazonaws.com are not checked (evaluating a resource policy is IAM policy evaluation, out of scope); evaluator cadence is 1 minute versus the documented up to three updates a day 8 to 12 hours apart (cost-management userguide budgets-managing-costs), kept so alarms fire inside a test run
  - ExecuteBudgetAction: ExecutionRoleArn must exist in IAM when the action runs (a missing role fails the execution) but its trust policy is never evaluated or assumed (trust policy evaluation is IAM policy evaluation, out of scope); SSM actions start AWS-Stop*/Start* automation documents through the SSM backend
  - "AccessDeniedException is never raised (needs IAM policy evaluation, out of scope)"
structural_gaps: []
deferred: []
leaks: {status: clean, note: "one worker.Group ticker bound to the janitor context; unit tests never start it"}
---

## Emulator choices where the vendor is silent
- CostFilters CostCategory values are read as `<category name>$<category value>`, the form TagKeyValue documents (`user:Key$Value`); the SDK Budget.CostFilters doc names the CostCategory key but not its value syntax. FilterExpression CostCategories uses Key as the category name with the documented match options.
- CostTypes: Tax, Support, Credit, Refund and RIFee lines come from AWS billing (jurisdiction, support plan and contract dependent) and the vendor publishes no rule an emulated service could apply; the Include* flags act on any such row recorded through ce RecordCharge.
- ServiceQuotaExceededException: no per-account budget quota appears in the Cost Management user guide (budgets-managing-costs.html) or the pinned SDK operation docs, so none is enforced. ThrottlingException and InternalErrorException are never raised because the emulator has no rate limiter or internal fault source and a synthetic one would fabricate failures; chaos fault rules can inject them.
- FilterExpression nesting deeper than 32 levels is rejected with InvalidParameterException; the API reference states no depth limit, the bound only keeps validation and evaluation recursion finite. ProductAttributes keys and MatchOptions combinations, and a CostCategories Key, are validated as the SDK doc comments for ProductAttributeValues and CostCategoryValues describe them; a CostFilters CostCategory value with an empty name or value matches nothing.
- A NextToken is stamped with its issue time and expires after 24 hours with ExpiredNextTokenException; the API reference documents the exception (types/errors.go) but no lifetime.

## Notes
Protocol awsjson1.1, X-Amz-Target prefix AWSBudgetServiceGateway. Budgets is a global service homed in us-east-1: ARNs are arn:aws:budgets::acct:budget/name and .../action/id.
An action key is acct/name/action/id, which is why budget names may not contain "/action/".
Percentage thresholds compare spend to the limit of the current period (planned limits resolved by period offset).
Wiring lives in internal/app/budgets_wiring.go (Cost Explorer ledger, SNS/SES, IAM/Organizations/SSM).

## Ledger dimensions
FilterExpression CostCategories evaluates the ce cost category rules per ledger row (ce CostCategoryValue semantics: first matching rule, else the default value); ProductAttributes (feature, inferenceType, model, provider; keys case-sensitive per the SDK doc) match ce CostEntry.ProductAttributes; the CostFilters SubscriptionId key and the SUBSCRIPTION_ID dimension match CostEntry.SubscriptionID. Rows without the attribute match only ABSENT.
CostFilters AZ, InstanceType, Operation, PurchaseType, UsageTypeGroup, BillingEntity, InvoicingEntity and LegalEntityName narrow spend through ce CostEntry fields; FilterExpression Dimensions use the same fields (AZ, INSTANCE_TYPE, OPERATION, PURCHASE_TYPE, USAGE_TYPE_GROUP, BILLING_ENTITY, INVOICING_ENTITY, LEGAL_ENTITY_NAME, CHARGE_TYPE).
Include* flag to charge-type mapping follows the CostTypes field descriptions (API_budgets_CostTypes); the API reference gives no finer mapping.

## Metered spend
Actual spend for COST and USAGE budgets sums ce ledger rows, which now include rows metered from emulated resources (resource id, tags, AZ, instance type); CostFilters, TagKeyValue and FilterExpression Dimensions, Tags, And, Or and Not narrow them (internal/app/ce_metering_wiring_test.go).
RI_UTILIZATION is reserved instance hours covered by metered running instance hours over reserved hours in the period so far; RI_COVERAGE is covered hours over all metered instance hours. A zonal reservation matches its own AZ, a regional one any AZ; usage is day-granular. Without any active reservation the budget reports 0 PERCENTAGE.

## External effects that are recorded, not performed

- BillingViewArn: format-checked, and HealthStatus is HEALTHY; Create and UpdateBudget log "would resolve billing view and report its health" with the budget and view ARN because no billing view service exists under services/. BillingViewHealthStatusException is therefore never raised.
