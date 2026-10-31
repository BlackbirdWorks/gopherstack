---
service: ocinotifications
sdk_module: github.com/oracle/oci-go-sdk/v65@v65.126.2   # go.mod dependency (test-only): drives every operation in the service tests
last_audit_commit: 3a4fe760a
last_audit_date: 2026-10-27
overall: B
ops:
  CreateTopic: {wire: ok, errors: partial, state: ok, persist: ok, note: "200 with the topic and etag; CREATING then ACTIVE after the transition window on the injectable clock. Names are unique across live topics (409 Conflict), up to 256 characters. apiEndpoint is the base URL the client addressed, shortTopicId is generated."}
  GetTopic: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header; DELETING topics stay readable until the transition completes, then 404."}
  ListTopics: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; id, name, lifecycleState filters; sortBy TIMECREATED or LIFECYCLESTATE; bare array body, paged with opc-next-page."}
  UpdateTopic: {wire: ok, errors: ok, state: ok, persist: ok, note: "Description and tags; If-Match gives 412 NoEtagMatch; returns the topic with a new etag."}
  DeleteTopic: {wire: ok, errors: ok, state: ok, persist: ok, note: "202; DELETING then removed together with its subscriptions and deliveries."}
  ChangeTopicCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "202; moves the topic record only, its subscriptions keep their compartment."}
  CreateSubscription: {wire: ok, errors: partial, state: ok, persist: ok, note: "Protocols CUSTOM_HTTPS, EMAIL, HTTPS, ORACLE_FUNCTIONS, PAGERDUTY, SLACK, SMS with per-protocol endpoint checks (http: or https: URL, email address, E.164 number, function OCID, any PagerDuty key). EMAIL, HTTPS, CUSTOM_HTTPS, SLACK and SMS start PENDING and get a confirmation delivery; ORACLE_FUNCTIONS and PAGERDUTY start ACTIVE. Duplicate topic/protocol/endpoint is 409. The topic must be ACTIVE. Returns deliverPolicy as a JSON string, createdTime in epoch milliseconds and an etag."}
  GetSubscription: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header."}
  ListSubscriptions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required, topicId filter; summaries carry deliveryPolicy as an object; bare array body, paged."}
  UpdateSubscription: {wire: ok, errors: ok, state: ok, persist: ok, note: "deliveryPolicy.backoffRetryPolicy (EXPONENTIAL, maxRetryDuration 10000 to 86400000 ms) and tags; the response body is the update details shape the SDK declares, with the new etag header."}
  DeleteSubscription: {wire: ok, errors: ok, state: ok, persist: ok, note: "204; drops the subscription and its deliveries. If-Match gives 412."}
  ChangeSubscriptionCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "202."}
  GetConfirmSubscription: {wire: ok, errors: partial, state: ok, persist: ok, note: "Unsigned link (token and protocol query parameters, no signature needed even with enforcement on) that moves PENDING to ACTIVE; idempotent for an ACTIVE subscription. A wrong token or protocol is 400 InvalidParameter (code unverified). Returns the confirmation result with an unsubscribe URL."}
  GetUnsubscription: {wire: ok, errors: partial, state: ok, persist: ok, note: "Unsigned link that removes the subscription; the body is a JSON string with a confirmation sentence."}
  ResendSubscriptionConfirmation: {wire: ok, errors: ok, state: ok, persist: ok, note: "Only PENDING subscriptions (409 IncorrectState otherwise); supersedes earlier unsent confirmations and queues a new one with the same token."}
  PublishMessage: {wire: ok, errors: partial, state: ok, persist: ok, note: "body required, title optional, 64 KB limit; messageType header JSON (body must be valid JSON) or RAW_TEXT (default). Returns messageId and timeStamp and fans out to every ACTIVE subscription; PENDING ones receive nothing. The topic must be ACTIVE."}
families:
  auth: {status: ok, note: "pkgs/ociauth verifies draft-cavage signatures for API keys registered through POST /_gopherstack/oci/keys or OCI Identity; accept-any otherwise, --oci-notifications-enforce-auth rejects unsigned and unknown keys. The confirmation and unsubscription links carry tokens instead."}
  error_envelope: {status: partial, note: "Errors are {code,message} with opc-request-id; InvalidParameter, MissingParameter, NotAuthenticated, NotAuthorizedOrNotFound, NoEtagMatch and IncorrectState follow the OCI reference; Conflict for duplicate topics and subscriptions is unverified."}
  one_listener: {status: ok, note: "The control plane (topics) and data plane (subscriptions, publish) share one listener and the /20181201 base path, so one host serves both SDK clients."}
  https_delivery: {status: ok, note: "HTTPS and CUSTOM_HTTPS deliveries are POSTed through pkgs/httpclient with Content-Type, X-OCI-NS-MessageId, X-OCI-NS-TopicOcid, X-OCI-NS-TopicName, X-OCI-NS-MessageType and X-OCI-NS-UnsubscribeURL; confirmations add X-OCI-NS-ConfirmationURL and X-OCI-NS-SubscriptionId. Any 2xx acknowledges."}
  retry_policy: {status: ok, note: "Per the Oracle docs: an immediate retry, then exponential backoff of 1, 2, 4, 8, 16 and 32 minutes (32 repeats) until the subscription's maxRetryDuration (default 7200000 ms, 2 hours) from publication, then the message is DISCARDED. The dispatcher is event driven: it sleeps on a kick channel and a timer for the next due retry, both on the injectable clock and timer."}
  recorded_protocols: {status: ok, note: "EMAIL, SMS, SLACK and PAGERDUTY deliveries are recorded, not sent, with status RECORDED. GET /_gopherstack/oci/deliveries (optional topicId, subscriptionId) lists every delivery with its confirmationUrl, so a confirmation sent to an email or phone can be completed."}
  functions: {status: ok, note: "ORACLE_FUNCTIONS deliveries invoke OCI Functions in the same process (message as the payload) with the same retry policy; without the Functions backend they are RECORDED."}
  lifecycle: {status: ok, note: "Topic CREATING and DELETING last 2 seconds on the injectable backend clock; transitions are applied on every request and by a one-minute sweeper."}
  routing_isolation: {status: ok, note: "Own http.Server bound in StartWorker to --oci-notifications-port / OCI_NOTIFICATIONS_PORT (default 8157); never on the shared AWS router; cli.go reserves the port in the shared pool."}
  observability: {status: ok, note: "telemetry.WrapEchoHandler feeds ExtractOperation/ExtractResource; one lockmetrics.RWMutex guards all state; the delivery log keeps the newest 2000 finished entries."}
items_still_open:
  - "EMAIL, SMS, SLACK and PAGERDUTY messages are recorded but never sent; Slack formatting, PagerDuty incident creation and friendly email formatting are not modelled."
  - "Whether CUSTOM_HTTPS, SLACK, SMS and PAGERDUTY subscriptions start PENDING or ACTIVE is recalled from the documentation, not verified; the confirmation body and the X-OCI-NS-MessageType values are unverified."
  - "Signed deliveries (X-OCI-NS-Signature, X-OCI-NS-Timestamp, X-OCI-NS-SignatureVersion, X-OCI-NS-SigningCertURL) and the friendly-formatting setting are not implemented."
  - "The publish rate (60 messages per minute per topic) and per-subscription delivery rates are not enforced; SMS is not restricted to Announcements, Monitoring and Connector Hub messages."
  - "The maxRetryDuration range (10 s to 24 h), topic name rules, the status codes for DeleteSubscription (204) and DeleteTopic (202), and the error codes for bad confirmation tokens are unverified."
  - "Deleted topics and unsubscribed subscriptions disappear instead of passing through a DELETED state."
  - "Topic and subscription tagging is stored verbatim; opc-retry-token idempotency, IAM policy checks, resource principals and instance principals are not applied; only API-key signatures are verified."
---
