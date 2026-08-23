---
service: cloudfront
sdk_module: aws-sdk-go-v2/service/cloudfront@v1.67.4
sibling_sdk_modules: [aws-sdk-go-v2/service/cloudfrontkeyvaluestore@v1.15.4]  # KeyValueStore data-plane ops (GetKey/PutKey/DeleteKey/ListKeys/UpdateKeys/DescribeKeyValueStore) now live in services/cloudfrontkeyvaluestore (gopherstack-4ara, 2026-08-13) -- see that service's own PARITY.md
last_audit_commit:                                # unknown: gopherstack-o31x route-table audit pass ran without git access at write time, never backfilled -- gopherstack-33in
last_audit_date: 2026-08-14  # gopherstack-7185: response shapes of Create/Delete/Modify ops
                              # swept (the class prior passes only checked for List/Describe).
                              # 2 bugs found (DeleteVpcOrigin empty envelope, UpdateDomainAssociation
                              # wrong output key). See DeleteVpcOrigin/UpdateDomainAssociation op rows.
overall: A            # gopherstack-o31x: first FULL route diff of all 167 real cloudfront
                       # control-plane ops (method+path) against cloudfront@v1.67.4
                       # serializers.go, not just the ops other work happened to touch.
                       # Fixed the 3 known bare-vs-/config Update routes plus 21 further
                       # mismatches this diff surfaced: the entire ListDistributionsBy*
                       # family (12 ops) used a hyphenated path with no real-SDK counterpart;
                       # CreateDistributionWithTags/CreateStreamingDistributionWithTags read
                       # their WithTags flag from a "Resource" query key a real client never
                       # sets (real flag is a bare "?WithTags"); TagResource/UntagResource
                       # were disambiguated by HTTP method (POST/DELETE) when both are really
                       # POST, differing only by an "Operation=Tag|Untag" query value, so
                       # every real UntagResource call landed on TagResource instead; the
                       # monitoring-subscription trio used singular "distribution/" instead of
                       # the real plural "distributions/"; GetManagedCertificateDetails was
                       # nested under distribution-tenant instead of its own top-level
                       # "managed-certificate/{Identifier}"; DisassociateDistributionTenantWebACL
                       # had no route at all; and ListConnectionFunctions/ListConnectionGroups/
                       # GetDistributionTenantByDomain/GetConnectionGroupByRoutingEndpoint were
                       # each swapped with their List/Get sibling. See gopherstack-o31x and the
                       # "Full route-table audit" note below for the complete method and
                       # methodology. go build/vet/test -race/golangci-lint all pass clean.
                       # (gopherstack-a9t managed policies, gopherstack-na4 InUse guards,
                       # gopherstack-mzx CallerReference AlreadyExists), and found three
                       # NEW real wire bugs via field-diff against aws-sdk-go-v2 that were
                       # not previously flagged despite these families being marked "ok":
                       # (1) CachePolicy/OriginRequestPolicy/ResponseHeadersPolicy whitelist
                       # Items lists were silently dropped on parse (CachePolicy only) and on
                       # every read response (all three) -- see "Wire-shape fixes" note below;
                       # (2) UpdateOriginRequestPolicy was routed to require a "/config" URL
                       # suffix that no real SDK client ever sends (real wire is a bare-ID PUT),
                       # so every real UpdateOriginRequestPolicy call 404'd against this
                       # emulator; (3) CreateDistribution/CreateStreamingDistribution treated
                       # CallerReference reuse as unconditionally idempotent, when real AWS
                       # returns *AlreadyExists on ANY reuse regardless of content (stricter
                       # than the previously-filed gopherstack-mzx description, which assumed
                       # a content-comparison rule -- verified against the live API docs).
                       # go build/vet/test -race/golangci-lint all pass clean this pass.
ops:
  CreateDistribution: {wire: ok, errors: fixed, state: ok, persist: ok, note: "FIXED this pass: CallerReference reuse now ALWAYS returns DistributionAlreadyExists (was unconditionally idempotent); real API docs state this happens regardless of DistributionConfig content -- verified against the live CreateDistribution reference page, not just the SDK doc comment"}
  CreateDistributionWithTags: {wire: ok, errors: ok, state: fixed, persist: ok, note: "inherits the CreateDistribution CallerReference fix. FIXED 2026-08-13 (gopherstack-o31x): routing bug. Real request sends a bare \"?WithTags\" query flag with no value (serializers.go: awsRestxml_serializeOpCreateDistributionWithTags's SplitURI on \".../distribution?WithTags\"), never \"?Resource=WithTags\" -- gopherstack read the WithTags signal from a \"Resource\" query value a real client never sends, so every real CreateDistributionWithTags call silently landed on plain CreateDistribution instead (tags dropped, no error). Fixed by a new cfResourceParam helper (handler.go) that checks for the bare \"WithTags\" query key before falling back to \"Resource\". Same bug, same fix, for CreateStreamingDistributionWithTags (see its op row). Verified against the real aws-sdk-go-v2 client (TestCreateDistributionWithTags_RealClient) and confirmed to fail against the pre-fix shape by reverting by hand."}
  GetDistribution: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDistributionConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDistribution: {wire: ok, errors: ok, state: fixed, persist: ok, note: "If-Match/ETag enforced; validateQuantities added. FIXED this pass (gopherstack-k3fi): the InProgress status UpdateDistribution sets now really transitions back to Deployed on its own, via a b.work.After-scheduled async hop (distributions.go's scheduleDistributionDeployed) -- the same pkgs/worker idiom services/mgn/exportimport.go and services/outposts's order lifecycle use. The scheduled hop is re-armed on Restore (rearmPendingDistributionDeploysLocked) so a distribution restored mid-transition still reaches Deployed instead of sticking InProgress forever, unlike a bare timer that would only survive a process restart, not a Snapshot/Restore round trip. Scoped to Distribution only -- see deferred note below for the other 5 resource kinds with their own status semantics."}
  DeleteDistribution: {wire: ok, errors: ok, state: ok, persist: ok, note: "If-Match enforced; DistributionNotDisabled enforced"}
  ListDistributions: {wire: ok, errors: ok, state: ok, persist: ok}
  CopyDistribution: {wire: ok, errors: fixed, state: fixed, persist: ok, note: "FIXED this pass: did not track/enforce CallerReference uniqueness at all (distributionCallerRefs was never populated by CopyDistribution); now returns DistributionAlreadyExists on reuse, matching the real CopyDistribution error list"}
  CreateInvalidation: {wire: ok, errors: ok, state: ok, persist: ok, note: "validateQuantities added for Paths; background reconciler transitions InProgress->Completed"}
  GetInvalidation: {wire: ok, errors: ok, state: ok, persist: ok}
  ListInvalidations: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCachePolicy: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED this pass: request-parsing tags for whitelisted Headers/Cookies/QueryStrings used Headers>Header/Cookies>Cookie/QueryStrings>QueryString, which matches no real CloudFront wire path -- real is Headers>Items>Name (verified against the live CreateCachePolicy/UpdateCachePolicy request syntax); every whitelist/allExcept request silently lost its listed names on unmarshal. Also now returns CachePolicyAlreadyExists; validateQuantities added"}
  UpdateCachePolicy: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "same parse fix as Create; managed policies (Managed-CachingOptimized etc.) now return IllegalUpdate (400) instead of being silently rewritten; If-Match enforced; validateQuantities added"}
  DeleteCachePolicy: {wire: ok, errors: ok, state: fixed, persist: ok, note: "CachePolicyInUse guard via distribution config token index (prior pass); managed policies now return IllegalDelete (400) instead of being silently removed (this pass)"}
  GetCachePolicy / GetCachePolicyConfig / ListCachePolicies: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "FIXED this pass: every response previously omitted the Headers/Cookies/QueryStrings Items lists entirely (only a bare HeaderBehavior/CookieBehavior/QueryStringBehavior, no Quantity, no Items) and GetCachePolicyConfig omitted ParametersInCacheKeyAndForwardedToOrigin altogether -- a real client could never discover which names a policy actually whitelists. Managed-vs-custom Type=managed|custom filter added (gopherstack-a9t, closed) and List summaries now carry the correct <Type> element"}
  CreateOriginRequestPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "now returns OriginRequestPolicyAlreadyExists; validateQuantities added"}
  UpdateOriginRequestPolicy: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "FIXED this pass, CLIENT-BREAKING: routing required a PUT to .../origin-request-policy/{id}/config, but the real UpdateOriginRequestPolicy wire is a bare-ID PUT (.../origin-request-policy/{id}, no /config suffix, verified against the live API reference) -- every real SDK client's UpdateOriginRequestPolicy call 404'd with NoSuchOperation against this emulator. Managed policies now return IllegalUpdate (400)"}
  DeleteOriginRequestPolicy: {wire: ok, errors: ok, state: fixed, persist: ok, note: "OriginRequestPolicyInUse guard (prior pass); managed policies now return IllegalDelete (400) (this pass)"}
  GetOriginRequestPolicy / GetOriginRequestPolicyConfig / ListOriginRequestPolicies: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED this pass: same Items-list omission as CachePolicy -- orpResponseXML emitted only a bare Quantity, and GetOriginRequestPolicyConfig omitted HeadersConfig/CookiesConfig/QueryStringsConfig entirely. Type=managed|custom filter added"}
  CreateResponseHeadersPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "now returns ResponseHeadersPolicyAlreadyExists; validateQuantities added"}
  UpdateResponseHeadersPolicy: {wire: ok, errors: ok, state: fixed, persist: ok, note: "same as Create; managed policies now return IllegalUpdate (400)"}
  DeleteResponseHeadersPolicy: {wire: ok, errors: ok, state: fixed, persist: ok, note: "ResponseHeadersPolicyInUse guard (prior pass); managed policies now return IllegalDelete (400) (this pass)"}
  GetResponseHeadersPolicy / GetResponseHeadersPolicyConfig / ListResponseHeadersPolicies: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED this pass: CorsConfig's four list fields (AccessControlAllowOrigins/Headers/Methods, AccessControlExposeHeaders) and SecurityHeadersConfig's ContentTypeOptions/ContentSecurityPolicy were completely absent from every response even though the request parser already captured them. GetResponseHeadersPolicyConfig omitted the whole config body. Type=managed|custom filter added. STILL SIMPLIFIED (see items_still_open): XSSProtection is a single string field, not the real 4-field Override/Protection/ModeBlock/ReportUri struct, and STS/FrameOptions/ReferrerPolicy/ContentSecurityPolicy have no per-header Override flag modeled (only ContentTypeOptions does) -- not restructured this pass"}
  CreateOriginAccessControl: {wire: ok, errors: ok, state: ok, persist: ok, note: "now returns OriginAccessControlAlreadyExists; validateQuantities added"}
  UpdateOriginAccessControl: {wire: ok, errors: ok, state: ok, persist: ok, note: "same as above"}
  DeleteOriginAccessControl: {wire: ok, errors: ok, state: fixed, persist: ok, note: "FIXED this pass (gopherstack-na4): OriginAccessControlInUse guard added via the same token-index pattern as CachePolicy/Function; verified against a distribution whose Origin.OriginAccessControlId references it"}
  GetOriginAccessControl / GetOriginAccessControlConfig / ListOriginAccessControls: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCloudFrontOriginAccessIdentity: {wire: ok, errors: fixed, state: ok, persist: ok, note: "FIXED this pass: CallerReference reuse with an identical Comment is idempotent (correct, per the real CloudFrontOriginAccessIdentityConfig doc), but reuse with a DIFFERENT Comment previously still returned the existing OAI silently instead of CloudFrontOriginAccessIdentityAlreadyExists; validateQuantities added (harmless no-op for this shape)"}
  UpdateCloudFrontOriginAccessIdentity: {wire: ok, errors: ok, state: ok, persist: ok, note: "If-Match enforced"}
  DeleteCloudFrontOriginAccessIdentity: {wire: ok, errors: ok, state: fixed, persist: ok, note: "FIXED this pass (gopherstack-na4): CloudFrontOriginAccessIdentityInUse guard added, matching on the real S3OriginConfig.OriginAccessIdentity wire value \"origin-access-identity/cloudfront/{id}\" (not the bare ID) -- If-Match still enforced"}
  GetCloudFrontOriginAccessIdentity / Config / List: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateFunction: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED this pass: response was missing required FunctionMetadata.FunctionARN/CreatedTime/LastModifiedTime; now returns FunctionAlreadyExists (was DistributionAlreadyExists); validateQuantities added"}
  UpdateFunction: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same wire fix; If-Match enforced; validateQuantities added"}
  PublishFunction: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same wire fix; If-Match enforced; LastModifiedTime now bumped"}
  DeleteFunction: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW: FunctionInUse guard (keyed by FunctionARN, not name)"}
  GetFunction / DescribeFunction / ListFunctions: {wire: fixed, errors: ok, state: ok, persist: ok, note: "share the same FunctionMetadata fix"}
  TestFunction: {wire: fixed, errors: fixed, state: n/a, persist: n/a, note: "CORRECTED 2026-08-13 (gopherstack-3izo): the handler never read the request body at all -- it confirmed the function existed via GetFunction, then returned a hardcoded TestResult with empty FunctionExecutionLogs/FunctionErrorMessage/FunctionOutput regardless of the supplied EventObject (required, base64 body-XML, api_op_TestFunction.go:50, serializers.go:11847) or the function's own code, and never checked If-Match at all despite it being a second required member (api_op_TestFunction.go:56) -- every real client's test call got a successful-looking empty result no matter what it sent. Real execution is out of reach: gopherstack vendors no JavaScript engine (no goja/otto/v8 in go.mod), and the one existing precedent for this exact problem -- appsync's EvaluateCode (services/appsync/jseval.go) -- only covers a narrow return-expression DSL used by AppSync resolver mapping templates (~5 fixed patterns: object literals, context member paths, a handful of util.* helpers), not general-purpose ES5.1 code with loops/variables/string methods/regex that real CloudFront Functions (URL rewrites, header/cookie manipulation, redirects) actually use; a 'faithful subset' evaluator broad enough to be useful would silently misexecute on anything outside its subset and produce a FunctionOutput that looks real but isn't -- worse than an empty one. Lambda's approach (services/lambda/containers.go: real Docker containers running actual AWS runtime images) is genuine execution but is Lambda's own zip/bootstrap/runtime-API protocol, not applicable to CloudFront Functions' edge JS model. Chose the honest option: read and validate the request for real (If-Match checked against the function's current ETag -> InvalidIfMatchVersion if missing/mismatched, matching this op's own declared error, not the PreconditionFailed siblings use; EventObject required, base64-decoded, and validated as well-formed JSON -> InvalidArgument otherwise), then report the real declared TestFunctionFailed error (HTTP 500, 'the CloudFront function failed' per the API reference) for a well-formed request gopherstack cannot execute, instead of fabricating FunctionOutput/logs. One pre-existing test (TestCloudFrontFunctionCRUD/test_function) asserted the canned empty-success TestResult as correct with no If-Match header and no EventObject at all; corrected to expect TestFunctionFailed for a well-formed request. New TestTestFunction covers the full validation matrix (missing/wrong If-Match, missing/non-base64/non-JSON EventObject, unknown function, and the TestFunctionFailed structural-gap response) and fails against the pre-fix handler by reverting by hand."}
  TagResource / UntagResource / ListTagsForResource: {wire: ok, errors: ok, state: fixed, persist: ok, note: "FIXED 2026-08-13 (gopherstack-o31x): routing bug. Real TagResource and UntagResource are BOTH POST /2020-05-31/tagging, disambiguated only by an \"Operation=Tag\"/\"Operation=Untag\" query value (serializers.go: awsRestxml_serializeOp{Tag,Untag}Resource's SplitURI) -- UntagResource is never DELETE. gopherstack routed POST unconditionally to TagResource and DELETE to UntagResource, so every real UntagResource call (POST) landed on the TagResource handler instead, which then 400'd MalformedXML trying to unmarshal an UntagResource body (root TagKeys) as Tags. Fixed by threading the \"Operation\" query value through parseCFPath (new opParam parameter) and switching on it for POST /tagging; a bare POST with no recognized Operation value still defaults to TagResource for backward compatibility with hand-built requests. ListTagsForResource (GET) was unaffected. Verified against the real aws-sdk-go-v2 client (TestTagUntagResource_RealClient) and confirmed to fail against the pre-fix shape by reverting by hand. gopherstack-r80d (required-OUTPUT-member sweep): ListTagsForResourceOutput.Tags is the ONLY required output member in this service's entire 167-op SDK surface (every other op's Output has zero 'This member is required.' fields at struct depth 0) -- not a protocol-wide trait (route53, also REST-XML, has 108 required output fields across 58 ops), just how this particular Smithy model was authored. handleListTagsForResource always builds a non-nil Tags element (even when the tag set is empty), so the sole required member is correctly populated. Service is fully settled for this bug class."}
  AssociateAlias: {wire: ok, errors: ok, state: ok, persist: ok, families: cross-service}
  AssociateDistributionTenantWebACL: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED 2026-08-13 (gopherstack-4ara): request struct root was WebACLAssociation with a WebACLId field; the real root is AssociateDistributionTenantWebACLRequest with a WebACLArn field (an ARN, not an ID; serializers.go: awsRestxml_serializeOpDocumentAssociateDistributionTenantWebACLInput, cloudfront@v1.67.4). Unlike the PutResourcePolicy class of this bug, the handler's xml.Unmarshal error WAS checked (not discarded), so the actual failure mode was every real client's request 400ing MalformedXML outright, not a silent zero-value wipe that returns 200 -- confirmed against the real client both before and after the fix (TestAssociateDistributionTenantWebACL_RealClient, fails against the pre-fix shape by reverting by hand). Also fixed TestAssociateDistributionTenantWebACL, a pre-existing test whose hand-typed request body encoded the exact same invented WebACLAssociation/WebACLId shape the pre-fix handler expected, so it had been passing against broken code indefinitely."}
  AssociateDistributionWebACL: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED 2026-08-13 (gopherstack-bhhx): request struct root was WebACLAssociation with a WebACLId field (the same webACLAssociationXML shared type AssociateDistributionTenantWebACL used before its own gopherstack-4ara fix); the real root is AssociateDistributionWebACLRequest with a WebACLArn field (an ARN, not an ID; serializers.go:255, awsRestxml_serializeOpDocumentAssociateDistributionWebACLInput, cloudfront@v1.67.4) -- a DIFFERENT real root from the tenant sibling's AssociateDistributionTenantWebACLRequest despite an identical field shape, so this needed its own dedicated request type (associateDistributionWebACLRequestXML) rather than reusing either the old shared type or the tenant's dedicated one. Same failure-mode class as the tenant fix: the handler's xml.Unmarshal error WAS checked (not discarded), so real clients got a clean 400 MalformedXML rather than a silent zero-value wipe. Surveyed every other shared XML request/response type in this service for the same shared-type-different-real-root risk (invalidationBatchXML used by CreateInvalidation and CreateInvalidationForDistributionTenant, tagXML/tagsXML used by 7+ ops) -- all confirmed safe: the real SDK's own types.InvalidationBatch/types.Tags/types.Tag are themselves canonical shared types reused identically across those ops (types/types.go:6492,6521), unlike the WebACLAssociation/WebACLId shape which never existed on any real op's wire at all. Verified against the real aws-sdk-go-v2 client (TestAssociateDistributionWebACL in handler_distributions_lifecycle_test.go, driven with the real AssociateDistributionWebACLRequest/WebACLArn body, plus a negative case asserting the old WebACLAssociation/WebACLId body now 400s MalformedXML) and confirmed to fail against the pre-fix shape by reverting by hand. Also fixed TestAssociateDistributionWebACL and TestDisassociateWebACL, two pre-existing tests whose hand-typed request bodies encoded the exact same invented WebACLAssociation/WebACLId shape the pre-fix handler expected, so they had been passing against broken code indefinitely."}
  UpdateDomainAssociation: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED 2026-08-14 (gopherstack-7185): response shape bug. The real UpdateDomainAssociationOutput carries a single ResourceId field (whichever of DistributionId/DistributionTenantId was the target -- the union collapses on output) plus ETag as a response header, NOT the input-shaped DistributionId/DistributionTenantId pair (api_op_UpdateDomainAssociation.go:60-68, deserializers.go:23927-23938,23974) -- an input/output key split of the same class the omics CompleteMultipartReadSetUpload/RunCache bugs were, so checking the request side alone would have confirmed the wrong answer. The handler echoed back separate DistributionId/DistributionTenantId elements (neither matching the real ResourceId tag) and never set an ETag header at all, so a real client's ResourceId/ETag were always empty even though the reassignment genuinely happened. Fixed: DomainAssociationResult gained an ETag field (sourced from the target's own ETag) and a ResourceID() accessor collapsing the two IDs; the handler now emits <ResourceId> and sets the ETag header. Verified against the real aws-sdk-go-v2 client (TestUpdateDomainAssociation_RealClient) and confirmed to fail against the pre-fix shape by reverting by hand."}
  ListDistributionTenantsByCustomization: {wire: fixed, errors: ok, state: fixed, persist: n/a, note: "FIXED 2026-08-12 (gopherstack-difi): TWO wire bugs, the second more severe than the first. (1) WebACLArn was read from the query string via c.Request().URL.Query(); cloudfront@v1.67.4 serializers.go's HTTP-bindings serializer for this op returns nil (zero HTTP-bound fields), so WebACLArn/CertificateArn/Marker/MaxItems all serialize into the XML body -- the query-string read was always empty against a real client. (2) The route table matched GET /distribution-tenants/by-customization, but the real SDK sends POST /distribution-tenants-by-customization (one hyphenated segment, no slash) -- confirmed by probing the unfixed handler with a real-shaped request, which 404'd NoSuchOperation. Fixed both: request fields now parsed from the XML body (root ListDistributionTenantsByCustomizationRequest), and the route corrected to POST + the hyphenated path. CertificateArn filtering and Marker/MaxItems pagination, previously entirely unimplemented, are now real: CertificateArn matches TenantCertificateArn (the tenant's deterministic CloudFront-managed certificate ARN -- customer-supplied ACM certs via Customizations.Certificate.Arn are not modeled anywhere in this service's Create/UpdateDistributionTenant, so that half of real AWS's certificate model stays out of scope); Marker/MaxItems page through the ID-sorted tenant list the same way ListDistributions already does, with NextMarker returned as a sibling of DistributionTenantList per the real deserializer."}
  PutResourcePolicy: {wire: fixed, errors: fixed, state: fixed, persist: n/a, note: "FIXED 2026-08-13 (gopherstack-nfka): TWO stacked wire bugs. (1) The request struct tagged its policy field xml:\"Policy\" and its root xml:\"ResourcePolicy\"; the real request is root PutResourcePolicyRequest containing PolicyDocument (api_op_PutResourcePolicy.go:27-41, serializers.go:11515-11527) -- since encoding/xml's Unmarshal errors when the root element name doesn't match an XMLName tag, EVERY real client's body failed to parse at all (err was discarded), silently zeroing ResourceArn too, not just the policy text. (2) Routing matched method (GET/POST/DELETE) on a single shared \"resource-policy\" path, but the real SDK POSTs to three distinct RPC-style paths -- /put-resource-policy, /get-resource-policy, /delete-resource-policy -- confirmed by probing the unfixed handler with real-shaped requests, all three 404'd NoSuchOperation. Fixed both: root/field names corrected, ResourceArn parsed from the body (never a query string, matching serializeOpHttpBindings*Input which emits no HTTP bindings for any of the three ops), and routing split into three POST-only suffix matches. Also fixed the not-found error code: ErrResourcePolicyNotFound emitted the invented NoSuchResourcePolicy; the real declared code (deserializeOpError{Get,Put,Delete}ResourcePolicy) is EntityNotFound."}
  GetResourcePolicy: {wire: fixed, errors: ok, state: ok, persist: n/a, note: "Twin of the PutResourcePolicy bug: response element was xml:\"Policy\" instead of PolicyDocument, and ResourceArn was never echoed at all. Both request-side bugs (root-name mismatch discarding ResourceArn, routing) also applied -- see PutResourcePolicy row. Response now emits PolicyDocument and ResourceArn per GetResourcePolicyOutput."}
  DeleteResourcePolicy: {wire: fixed, errors: ok, state: ok, persist: n/a, note: "Same routing + body-vs-query-string bugs as Put/Get; DeleteResourcePolicyInput.ResourceArn now read from the body (root DeleteResourcePolicyRequest)."}
  CreateVpcOrigin: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED 2026-08-13 (gopherstack-nfka): request parsing captured only VpcOriginEndpointConfig.Name and Tags; the other three required members -- Arn (the ARN of the VPC interface endpoint or ALB this origin actually routes to, types/types.go:6989-6992), HTTPPort, HTTPSPort, and OriginProtocolPolicy -- were dropped entirely and never reached backend state. Now parsed, validated (InvalidArgument if any required member is empty/non-positive, matching the op's declared error set), stored, and echoed back inside VpcOriginEndpointConfig in the response (which is a sibling of the resource's own top-level Arn, not nested inside it -- confirmed via CreateVpcOriginOutput's httpPayload-bound VpcOrigin decode)."}
  UpdateVpcOrigin: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "CORRECTED 2026-08-13 (gopherstack-ob1g): the 2026-08-13 (gopherstack-nfka) fix above stopped one field short. UpdateVpcOriginInput's real root element IS VpcOriginEndpointConfig itself (serializers.go: awsRestxml_serializeOpUpdateVpcOrigin's payloadRoot.Local) -- there is no wrapping UpdateVpcOriginRequest element the way Create has one. The struct fixed that pass still used XMLName=\"UpdateVpcOriginRequest\" and nested fields one level under a VpcOriginEndpointConfig>Name-style path, so xml.Unmarshal still errored on the whole body for every real client and the error was discarded (_ = xml.Unmarshal(...)), silently no-opping every real UpdateVpcOrigin call end to end -- this survived because the existing tests hand-crafted bodies matching the same wrong root. Root and field nesting corrected; the unmarshal error is now handled (400 MalformedXML) instead of discarded. Verified against the real aws-sdk-go-v2 client (TestUpdateVpcOrigin_RealClient) and confirmed to fail against the pre-fix shape by reverting by hand."}
  DeleteVpcOrigin: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED 2026-08-14 (gopherstack-7185): empty-envelope bug, the class this issue was opened to find. Unlike every other Delete op in this service (all with a genuinely empty DeleteXOutput -- verified across all 23 real DeleteXOutput structs in the pinned SDK), DeleteVpcOriginOutput uniquely carries ETag (header) and VpcOrigin (body, the just-deleted resource) -- api_op_DeleteVpcOrigin.go:44-53. The handler answered with a bare 204 No Content, so a real client's out.VpcOrigin/out.ETag were always nil even though the delete genuinely happened. Fixed to return 200 with the deleted VpcOrigin body (reusing vpcOriginResponseXML) and the ETag header. Verified against the real aws-sdk-go-v2 client (TestDeleteVpcOrigin_RealClient) and confirmed to fail against the pre-fix shape by reverting by hand."}
  CreateRealtimeLogConfig: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "FIXED 2026-08-13 (gopherstack-nfka): THREE stacked wire bugs. (1) Request struct root was xml:\"RealtimeLogConfig\"; the real root is CreateRealtimeLogConfigRequest (api_op_CreateRealtimeLogConfig.go, serializers.go:2489-2609) -- same root-name-mismatch class of bug as PutResourcePolicy, so Name/Fields/SamplingRate were ALSO silently dropped for every real client, not just EndPoints. (2) EndPoints -- the required Kinesis destination (api_op_CreateRealtimeLogConfig.go:37-43) -- was never declared as a struct field at all; now parsed (list wrapped in <member>, matching serializers.go's awsRestxml_serializeDocumentEndPointList) and required (InvalidArgument if empty). (3) The response nested ARN/Name/etc directly under the root; CreateRealtimeLogConfigOutput is NOT httpPayload-bound (unlike VpcOrigin/Distribution) so the real deserializer looks for a child element literally named <RealtimeLogConfig> wrapping the fields (deserializers.go: awsRestxml_deserializeOpDocumentCreateRealtimeLogConfigOutput) -- the old flat response left output.RealtimeLogConfig nil for a real client even once (1) and (2) were fixed. All three verified against the real aws-sdk-go-v2 client via a round-trip test (TestRealtimeLogConfigCRUD_RealClient), and each fails against the pre-fix shape individually (confirmed by temporarily reverting each in turn)."}
  GetRealtimeLogConfig: {wire: fixed, errors: ok, state: ok, persist: ok, note: "Same response double-nesting bug as Create (fixed). ALSO a routing bug: this op is a POST to /2020-05-31/get-realtime-log-config carrying ARN or Name in the body (api_op_GetRealtimeLogConfig.go:33-42), not a GET to /realtime-log-config/{id}; the old route table 404'd NoSuchOperation for every real client. Now POSTs to the correct path and resolves by ARN or Name (preferring Name when both given, per the op's doc comment)."}
  UpdateRealtimeLogConfig: {wire: fixed, errors: ok, state: ok, persist: ok, note: "Same three bugs as Create (missing EndPoints, response nesting) plus the same routing bug as Get: real wire is PUT to the base /2020-05-31/realtime-log-config path with ARN/Name identifying the target in the body (api_op_UpdateRealtimeLogConfig.go:43-67), not a PUT to /realtime-log-config/{id}."}
  DeleteRealtimeLogConfig: {wire: fixed, errors: ok, state: ok, persist: ok, note: "Same routing bug as Get: real wire is POST to /2020-05-31/delete-realtime-log-config with ARN/Name in the body (api_op_DeleteRealtimeLogConfig.go), not a DELETE to /realtime-log-config/{id}."}
  UpdateTrustStore: {wire: fixed, errors: fixed, state: fixed, persist: ok, note: "FIXED 2026-08-13 (gopherstack-ob1g): TWO stacked wire bugs, same class as UpdateVpcOrigin above. (1) UpdateTrustStoreInput's real root is CaCertificatesBundleSource, containing CaCertificatesBundleS3Location>Bucket/Key/Region as its only children (serializers.go: awsRestxml_serializeOpUpdateTrustStore's payloadRoot.Local; types.go: CaCertificatesBundleSourceMemberCaCertificatesBundleS3Location) -- UpdateTrustStoreInput has NO Name or Comment member at all, so real AWS can never change either through this operation. The struct here used root TrustStoreConfig with Name/Comment/CertificateAuthorityCertificatesBundle fields, none of which exist on the real wire; xml.Unmarshal errored on the whole body for every real client and the error was discarded, silently no-opping the CA bundle update while ALSO exposing a Name/Comment-update capability real AWS doesn't have. (2) The unmarshal error was discarded (_ = xml.Unmarshal(...)); now handled (400 MalformedXML). Fix: request struct rebuilt to the real CaCertificatesBundleSource>CaCertificatesBundleS3Location shape (Region accepted on the wire but not persisted -- see deferred note), handler now always passes empty name/comment to the backend (never overwritten, matching real AWS), and the old TrustStoreConfig>CertificateAuthorityCertificatesBundle shape is still accepted for backward compatibility. Verified against the real aws-sdk-go-v2 client (TestUpdateTrustStore_RealClient, which reads back the applied bundle via a raw follow-up GET since the real TrustStore output shape has no field for the CA bundle at all) and confirmed to fail against the pre-fix shape by reverting by hand."}
  UpdateDistributionWithStagingConfig: {wire: fixed, errors: fixed, state: ok, persist: n/a, note: "FIXED 2026-08-13 (gopherstack-ob1g): routing bug found while hardening this handler's discarded xml.Unmarshal error. Real wire is PUT /2020-05-31/distribution/{Id}/promote-staging-config with StagingDistributionId as a QUERY parameter, never a body field (serializers.go: awsRestxml_serializeOpUpdateDistributionWithStagingConfig's SplitURI and awsRestxml_serializeOpHttpBindingsUpdateDistributionWithStagingConfigInput's SetQuery call). The route table matched a bare \"/staging\" suffix instead, so every real client's PUT 404'd as NoSuchOperation. Since real clients never send a body, the (now-fixed) discarded xml.Unmarshal error itself was latent rather than an active wipe for real traffic -- the route was the blocking bug. Fixed both: route corrected to the real path, and the unmarshal error is now handled instead of discarded, guarding the pre-existing body-based fallback path some callers may still use for backward compatibility."}
  ListDomainConflicts: {wire: fixed, errors: fixed, state: ok, persist: n/a, note: "FIXED 2026-08-13 (gopherstack-ob1g): routing bug found while hardening this handler's discarded xml.Unmarshal error. Real path is /2020-05-31/domain-conflicts (plural; serializers.go: awsRestxml_serializeOpListDomainConflicts's SplitURI); the route table matched the singular \"domain-conflict\", so every real client's POST 404'd as NoSuchOperation. Root/field names (ListDomainConflictsRequest>Domain) were already correct. Fixed both: route corrected to the plural path, and the unmarshal error is now handled instead of discarded. CORRECTED 2026-08-13 (gopherstack-3izo): that pass's 'Root/field names were already correct' verification only checked Domain -- it missed that ListDomainConflictsInput has a SECOND independently-required member, DomainControlValidationResource (a types.DistributionResourceId identifying the distribution or distribution tenant whose certificate validates control of the domain; api_op_ListDomainConflicts.go:73-77), which the request struct dropped entirely. Real AWS scopes the conflict check to that resource (excludes it from its own conflict list, since it legitimately holds the domain's cert); gopherstack ignored the scope and returned every conflict for the domain globally, including the resource itself when it was the one claiming the domain -- wrong, not merely incomplete. Fixed: DomainControlValidationResource now parsed (nested DistributionId/DistributionTenantId, exactly one required -> InvalidArgument otherwise), both required members validated (missing Domain or missing DomainControlValidationResource -> InvalidArgument), the referenced resource's existence checked (EntityNotFound if neither a real distribution nor tenant, matching this op's own declared error switch, not the per-resource-type NoSuchDistribution/NoSuchDistributionTenant codes other ops use), and findDomainConflicts extended to exclude that resource from the results. Two pre-existing tests (TestListDomainConflicts_RealConflicts, TestListDomainConflicts_TableDriven) never sent DomainControlValidationResource at all (one even used a nonexistent-on-the-real-wire ?Domain= query fallback) and so encoded the global-scope bug as correct; both corrected to send real bodies and now also cover the self-exclusion scoping and the new validation errors. All new/changed cases fail against the pre-fix handler by reverting by hand."}
  UpdatePublicKey: {wire: ok, errors: ok, state: fixed, persist: ok, note: "FIXED 2026-08-13 (gopherstack-o31x, filed by gopherstack-ob1g): real UpdatePublicKey PUTs to /2020-05-31/public-key/{Id}/config (serializers.go: awsRestxml_serializeOpUpdatePublicKey's SplitURI), not the bare /public-key/{Id} path -- every real client call 404'd. parseCFResourcePath's public-key call site (handler_paths.go: parseCFPublicKeyRealtimePath) had updateOp and updateConfigOp backwards (bound to the bare path, left the /config-suffixed PUT unmatched). Fixed by swapping which argument carries the real op. Existing tests asserting the wrong bare-ID path were updated to the real /config path, not preserved -- a test asserting a 404-producing route is negative value. Verified against the real aws-sdk-go-v2 client (TestUpdatePublicKey_RealClient) and confirmed to fail against the pre-fix shape by reverting by hand."}
  UpdateFieldLevelEncryptionConfig: {wire: ok, errors: ok, state: fixed, persist: ok, note: "FIXED 2026-08-13 (gopherstack-o31x, filed by gopherstack-ob1g): same bare-vs-/config bug as UpdatePublicKey. Real path is /2020-05-31/field-level-encryption/{Id}/config (serializers.go SplitURI). Fixed the same way (parseCFFieldLevelEncryptionPath's field-level-encryption call site); existing tests updated to the real path. Verified against the real aws-sdk-go-v2 client (TestUpdateFieldLevelEncryptionConfig_RealClient) and confirmed to fail against the pre-fix shape by reverting by hand."}
  UpdateFieldLevelEncryptionProfile: {wire: ok, errors: ok, state: fixed, persist: ok, note: "FIXED 2026-08-13 (gopherstack-o31x, filed by gopherstack-ob1g): same bare-vs-/config bug as UpdatePublicKey. Real path is /2020-05-31/field-level-encryption-profile/{Id}/config (serializers.go SplitURI). Fixed the same way (parseCFFieldLevelEncryptionPath's field-level-encryption-profile call site); existing tests updated to the real path. Verified against the real aws-sdk-go-v2 client (TestUpdateFieldLevelEncryptionProfile_RealClient) and confirmed to fail against the pre-fix shape by reverting by hand."}
families:
  list_distributions_by: {status: fixed, note: "FIXED 2026-08-13 (gopherstack-o31x): all 12 ListDistributionsBy* ops (AnycastIpListId, CachePolicyId, ConnectionFunction, ConnectionMode, KeyGroup, OriginRequestPolicyId, OwnedResource, RealtimeLogConfig, ResponseHeadersPolicyId, TrustStore, VpcOriginId, WebACLId) were routed on a hyphenated \"distributions/by-x-id/{id}\" path with no real-SDK counterpart at all -- every real client call 404'd NoSuchOperation. Real paths are a single camelCase segment with no hyphens, e.g. \"/2020-05-31/distributionsByCachePolicyId/{CachePolicyId}\" (serializers.go SplitURI, verified per-op individually, cloudfront@v1.67.4). Beyond the path shape, several ops also had the wrong ID SOURCE: ByConnectionFunction and ByTrustStore carry their identifier as a query value with no URI label at all (ConnectionFunctionIdentifier/TrustStoreIdentifier), not a path segment; ByConnectionMode and ByOwnedResource carry theirs as a URI label, not a query value (gopherstack previously had this backwards for all four); ByRealtimeLogConfig carries its ARN/Name in the XML body (POST, root ListDistributionsByRealtimeLogConfigRequest), not a query value. Fixed by rewriting parseCFDistributionsByPath (handler_paths.go) to the real per-op path shapes and dispatchStubsDistributionListBy (handler_dispatch.go) to read each op's identifier from its real source; deleted the now-fully-dead hyphenated-path fallback code in parseCFMiscPathSimple/parseCFMiscPathByDistribution that duplicated the wrong shape. Verified against the real aws-sdk-go-v2 client for ByConnectionMode (field-level round-trip, TestListDistributionsByConnectionMode_RealClient) and ByRealtimeLogConfig (TestListDistributionsByRealtimeLogConfig_RealClient); the other 10 are covered by TestExtractOperation_SDKRouteTable's exhaustive method+path diff against every real op (see 'Full route-table audit' note below) but not individually round-tripped through a real client due to this pass's time budget."}
  monitoring_subscription: {status: fixed, note: "FIXED 2026-08-13 (gopherstack-o31x): CreateMonitoringSubscription/GetMonitoringSubscription/DeleteMonitoringSubscription used the singular \"distribution/{Id}/monitoring-subscription\" path; the real path is PLURAL \"distributions/{DistributionId}/monitoring-subscription\" (serializers.go SplitURI, cloudfront@v1.67.4) -- unlike every other distribution sub-path in this service, which is singular. The singular-prefix guard in parseCFDistributionExtPath meant the plural path never even reached the trio's routing logic, so every real call 404'd. Fixed by splitting the trio into its own parseCFMonitoringSubscriptionPath (handler_paths.go) keyed on the plural prefix, and fixing extractMonitoringDistID (handler_monitoring.go) to trim the plural prefix too. Verified against the real aws-sdk-go-v2 client (TestMonitoringSubscription_RealClient, full Create/Get/Delete round trip) and confirmed to fail against the pre-fix shape by reverting by hand."}
  managed_certificate_details: {status: fixed, note: "FIXED 2026-08-13 (gopherstack-o31x): GetManagedCertificateDetails was routed as \"distribution-tenant/{Id}/managed-certificate-details\"; the real path is its own top-level \"/2020-05-31/managed-certificate/{Identifier}\" (serializers.go: awsRestxml_serializeOpGetManagedCertificateDetails's SplitURI), not nested under distribution-tenant at all -- every real client call 404'd. Fixed with a new parseCFManagedCertificatePath (handler_paths.go) and the matching dispatch-layer ID-extraction prefix (handler_dispatch.go); the two duplicate wrong-shape handlers in parseCFDistributionTenantExtOps and parseCFMiscPathByDistribution were removed. Verified against the real aws-sdk-go-v2 client (TestGetManagedCertificateDetails_RealClient) and confirmed to fail against the pre-fix shape by reverting by hand."}
  connection_group_function_swaps: {status: fixed, note: "FIXED 2026-08-13 (gopherstack-o31x): three swapped/wrong-shape routes in the connection-group and connection-function families. (1) GetConnectionGroupByRoutingEndpoint is really the bare GET \"connection-group\" (RoutingEndpoint as a query value); ListConnectionGroups is really POST to the plural \"connection-groups\"; gopherstack had these backwards (bare GET matched List, and a fictional \"connection-group-by-routing-endpoint\" literal path that no real client sends matched GetByRoutingEndpoint). (2) Same swap for GetDistributionTenantByDomain (bare GET \"distribution-tenant\", Domain as a \"?domain=\" query value) vs ListDistributionTenants (POST plural \"distribution-tenants\") -- the bare GET was routed to List instead. (3) ListConnectionFunctions is really POST to the plural \"connection-functions\"; gopherstack matched GET on the bare singular \"connection-function\", which no real client sends for List. All three confirmed by reading serializers.go's SplitURI per op (cloudfront@v1.67.4) and verified against the real aws-sdk-go-v2 client (TestGetConnectionGroupByRoutingEndpoint_RealClient, TestGetDistributionTenantByDomain_RealClient, TestListConnectionFunctions_RealClient); the GetConnectionGroupByRoutingEndpoint and GetDistributionTenantByDomain fixes were each confirmed to fail against the pre-fix shape by reverting by hand."}
  ListConnectionGroups: {wire: fixed, errors: ok, state: ok, persist: n/a, note: "FIXED 2026-08-13 (gopherstack-4ara): response wrapped items as <ConnectionGroupList><Items><ConnectionGroupSummary>...(plus a Quantity element); the real ListConnectionGroupsOutput has no Items/Quantity wrapper at all -- it is a direct <ConnectionGroups> element holding repeated <ConnectionGroupSummary> children (deserializers.go: awsRestxml_deserializeOpDocumentListConnectionGroupsOutput/awsRestxml_deserializeDocumentConnectionGroupSummaryList, cloudfront@v1.67.4). smithyxml's decoder only recognizes a direct <ConnectionGroups> child by name and silently skips anything else (including the old <Items> wrapper), so a real client always decoded an empty ConnectionGroups slice regardless of what gopherstack had stored -- worse than the 404 this op gave before gopherstack-o31x's routing fix made it reachable. Fixed by renaming the wrapper element and dropping the fabricated Quantity field the real output doesn't have. Verified against the real aws-sdk-go-v2 client (TestGetConnectionGroupByRoutingEndpoint_RealClient's List assertion, extended this pass to require the created group actually appears in the decoded list -- the SDK cannot populate a list from a wrong wrapper no matter what the raw XML holds, so this is the only test that can catch the bug); confirmed to fail against the pre-fix shape by reverting by hand."}
  ListConnectionFunctions: {wire: fixed, errors: ok, state: ok, persist: n/a, note: "FIXED 2026-08-13 (gopherstack-4ara): same bug class as ListConnectionGroups above -- response wrapped items as <ConnectionFunctionList><Items><ConnectionFunctionSummary>...(plus Quantity); real ListConnectionFunctionsOutput is a direct <ConnectionFunctions> element with no Items/Quantity wrapper (deserializers.go: awsRestxml_deserializeOpDocumentListConnectionFunctionsOutput, cloudfront@v1.67.4), so a real client always decoded an empty list. Fixed the same way. Verified against the real aws-sdk-go-v2 client (TestListConnectionFunctions_RealClient, extended this pass to require the created function actually appears in the decoded list); confirmed to fail against the pre-fix shape by reverting by hand."}
  disassociate_distribution_tenant_web_acl: {status: fixed, note: "FIXED 2026-08-13 (gopherstack-o31x): DisassociateDistributionTenantWebACL had no route at all -- only the Associate variant was wired in parseCFDistributionCorePath, even though the handler function and dispatch case already existed and were correctly implemented (handleDisassociateDistributionTenantWebACL, handler_dispatch.go's opDisassociateDistributionTenantWebACL case), unreachable purely for lack of a route match. Fixed by adding the \"/disassociate-web-acl\" suffix case alongside the existing \"/associate-web-acl\" one. Verified against the real aws-sdk-go-v2 client (TestDisassociateDistributionTenantWebACL_RealClient, which deliberately does not round-trip through Associate first -- see the AssociateDistributionTenantWebACL gap above)."}
  distribution_tenants_connection_groups: {status: ok, note: "CreateDistributionTenant/UpdateDistributionTenant now run validateQuantities; If-Match enforced on update/delete; audited, no new findings beyond the Quantity gap"}
  field_level_encryption: {status: ok, note: "Create/Update for config + profile now run validateQuantities and return the correct *AlreadyExists code (FieldLevelEncryptionConfigAlreadyExists / FieldLevelEncryptionProfileAlreadyExists) instead of DistributionAlreadyExists; FLEProfileInUse guard on profile delete pre-existed and is correct"}
  public_keys_key_groups: {status: ok, note: "CreatePublicKey/CreateKeyGroup/UpdateKeyGroup return PublicKeyAlreadyExists/KeyGroupAlreadyExists instead of DistributionAlreadyExists; PublicKeyInUse guard on public-key delete pre-existed and is correct; FIXED this pass (gopherstack-na4): DeleteKeyGroup now returns ResourceInUse (matching the real DeleteKeyGroup error list -- there is no dedicated KeyGroupInUse type) when the key group is referenced by a distribution's TrustedKeyGroups"}
  realtime_log_configs: {status: ok, note: "CreateRealtimeLogConfig returns RealtimeLogConfigAlreadyExists instead of DistributionAlreadyExists. See the CreateRealtimeLogConfig/GetRealtimeLogConfig/UpdateRealtimeLogConfig/DeleteRealtimeLogConfig op rows for the 2026-08-13 (gopherstack-nfka) wire and routing fixes -- this family note previously implied these ops were clean when they were not (missed by the 2026-07-23 audit)."}
  key_value_stores: {status: ok, note: "control-plane Create/Update run validateQuantities (no-op, shape has no Quantity/Items pairs). RESOLVED 2026-08-13 (gopherstack-4ara): the data-plane GetKey/PutKey/DeleteKey/ListKeys/UpdateKeys handlers previously living here were routed under this Handler's /2020-05-31/ RouteMatcher, which the real cloudfrontkeyvaluestore.Client never sends a request through -- structurally unreachable (see the removed 'gaps' entry below, and the Notes section's protocol paragraph). Removed from this service (handler_key_value_store.go, handler.go's op consts, handler_dispatch.go, handler_paths.go's parseCFKVSDataPlanePath) and reimplemented with correct routing/wire-shape in the new services/cloudfrontkeyvaluestore, wired via cli.go's wireCloudFrontKeyValueStore directly to this backend's keyValueStoreData/keyValueDataETags (the underlying state was always real; only the HTTP layer was wrong). This service's own control-plane CRUD (Create/Get/List/Delete/UpdateKeyValueStore) is unaffected and stays here. Persistence side-effect: keyValueStoreData/keyValueDataETags -- previously NOT in backendSnapshot at all -- are now persisted (cloudfrontSnapshotVersion bumped 1->2), and KeyValueStore gained a CreatedTime field (needed by the sibling service's DescribeKeyValueStore)."}
  vpc_origins: {status: ok, note: "Create/Update run validateQuantities (no-op for this shape). See the CreateVpcOrigin/UpdateVpcOrigin op rows for the 2026-08-13 (gopherstack-nfka) fix -- Arn/HTTPPort/HTTPSPort/OriginProtocolPolicy were previously dropped entirely, missed by the 2026-07-23 audit."}
  continuous_deployment_policy: {status: ok, note: "Create/Update run validateQuantities; If-Match already enforced"}
  invalidations_realtime_status: {status: ok, note: "background reconciler goroutine (runInvalidationReconciler) has a clean stopCh lifecycle via Close(); no leak"}
  monitoring_subscriptions_public_resource_policy_connection_groups: {status: fixed, note: "audited via handler_new_ops.go/handler_batch2.go dispatch; no Quantity/AlreadyExists-code issues found in these shapes. CORRECTION: this note previously claimed resource-policy was clean, but the 2026-07-23 audit missed that PutResourcePolicy's request never parsed at all against a real client (root/field name mismatch) and all three resource-policy ops were mis-routed (see PutResourcePolicy/GetResourcePolicy/DeleteResourcePolicy op rows, gopherstack-nfka, fixed 2026-08-13)."}
  managed_policies: {status: ok, note: "NEW this pass (gopherstack-a9t): 7 managed cache policies, 8 managed origin request policies, and 5 managed response headers policies seeded at backend construction/Reset/Restore with their real, permanent, verified-against-live-AWS-docs IDs and configs (see managed_policies.go's doc comment for the exact verification method and the deliberately-omitted Amplify-internal policies). Managed=true policies reject Update/Delete with IllegalUpdate/IllegalDelete (400); List* honors the real Type=managed|custom query filter and each summary carries the correct <Type> element"}
  streaming_distributions: {status: ok, note: "FIXED this pass: CreateStreamingDistribution treated non-empty CallerReference reuse as unconditionally idempotent; real AWS returns StreamingDistributionAlreadyExists on any reuse regardless of content (verified against the live CreateStreamingDistribution API reference, same rule as CreateDistribution). FIXED 2026-08-13 (gopherstack-o31x): CreateStreamingDistributionWithTags had the exact same WithTags-flag routing bug as CreateDistributionWithTags (real bare \"?WithTags\" query flag misread as \"Resource=WithTags\") -- see that op row for the fix. Verified via TestCreateStreamingDistributionWithTags_RealClient, confirmed to fail pre-fix by reverting by hand."}
gaps:
  # RESOLVED 2026-08-13 (gopherstack-4ara): the 5 CloudFront KeyValueStore data-plane ops
  # (GetKey/PutKey/DeleteKey/ListKeys/UpdateKeys), found structurally unreachable here by
  # gopherstack-o31x (see the "Full route-table audit" note below for how it was found),
  # are now implemented with correct routing/wire-shape in services/cloudfrontkeyvaluestore
  # -- see that service's own PARITY.md and the key_value_stores family note above.
  # gopherstack-o31x closed the previous pass's one open gap plus 21 further routing
  # mismatches the full 167-op diff surfaced beyond it -- see the FIXED op rows above
  # (CreateDistributionWithTags, CreateStreamingDistributionWithTags, TagResource,
  # UntagResource, UpdatePublicKey, UpdateFieldLevelEncryptionConfig,
  # UpdateFieldLevelEncryptionProfile, the whole ListDistributionsBy* family,
  # CreateMonitoringSubscription/GetMonitoringSubscription/DeleteMonitoringSubscription,
  # GetManagedCertificateDetails, DisassociateDistributionTenantWebACL,
  # GetDistributionTenantByDomain, GetConnectionGroupByRoutingEndpoint,
  # ListConnectionFunctions, ListConnectionGroups) and the "Full route-table audit" note
  # below for the complete list and methodology.
  # All three gaps filed by the pass before that are closed:
  #  - gopherstack-a9t (managed policies + Type filter): closed, see managed_policies family above.
  #  - gopherstack-na4 (OAI/OAC/KeyGroup delete InUse guards): closed, see the three
  #    "FIXED this pass (gopherstack-na4)" op rows above (DeleteOriginAccessControl,
  #    DeleteCloudFrontOriginAccessIdentity, DeleteKeyGroup via public_keys_key_groups family).
  #  - gopherstack-mzx (CallerReference AlreadyExists): closed, but the actual real-AWS rule
  #    is STRICTER than originally filed -- CreateDistribution/CreateStreamingDistribution
  #    always conflict on CallerReference reuse (content-independent), not just when content
  #    differs. CreateOAI genuinely IS content-comparison idempotent (the filed gap's
  #    assumption was correct for OAI specifically) and was fixed for the differing-content
  #    case. CopyDistribution didn't enforce CallerReference uniqueness at all and was also
  #    fixed. See the CreateDistribution/CopyDistribution/CreateStreamingDistribution/
  #    CreateCloudFrontOriginAccessIdentity op rows above for the exact behavior each has now.
deferred:
  - "Distribution status InProgress->Deployed transition timer: FIXED this pass (gopherstack-k3fi) for Distribution specifically -- see UpdateDistribution's op row above. The other 5 resource kinds with their own InProgress/Deployed-shaped status semantics (DistributionTenant, StreamingDistribution, ConnectionGroup/ConnectionFunction, AnycastIPList, TrustStore) still persist InProgress indefinitely; still deferred, now for a narrower, more honest reason -- extending the same worker.Group timer to each is straightforward but out of this pass's scope, not blocked on anything."
  - "Full per-op audit of DistributionConfig nested shape correctness (Origins/OriginGroups/CacheBehaviors/ViewerCertificate/Restrictions field-by-field) beyond the Quantity/Items validation and the pre-existing minimal-parse (RawConfig) model. This pass verified the specific sub-fields needed for the InUse-guard fixes (S3OriginConfig.OriginAccessIdentity path format, Origin.OriginAccessControlId, TrustedKeyGroups.Items) are correct, but a full field-by-field audit of the rest of DistributionConfig's ~60 nested types was not attempted -- RawConfig storage design predates this pass and was not restructured."
  - "ResponseHeadersPolicySecurityHeadersConfig is a flattened simplification of the real 5-sub-struct shape: XSSProtection is stored/emitted as a single string (matches only the real ReportUri sub-field) instead of the real ResponseHeadersPolicyXSSProtection{Override, Protection, ModeBlock, ReportUri} struct, and only ContentTypeOptions has a per-header Override flag modeled (STS/FrameOptions/ReferrerPolicy/ContentSecurityPolicy hardcode Override=false in every response, which happens to match every seeded managed policy's real Override:No default but is not read from request input for those four). Restructuring RHPSecurityHeaders to the full real shape is a breaking model change (cascades to persistence JSON tags and every existing test that constructs one) out of proportion to fix alongside this pass's other work; the CORS list fields and ContentTypeOptions/ContentSecurityPolicy value (the parts client code actually round-trips today) were fixed."
leaks: {status: clean, note: "runInvalidationReconciler goroutine has a proper stopCh + Close() lifecycle; no unbounded maps found. This pass added b.work (*pkgs/worker.Group), the mgn/outposts-style scheduled-timer idiom used by scheduleDistributionDeployed -- Close() now also calls b.work.Stop(), which cancels every pending timer and joins its goroutines, so nothing outlives the backend. seedManagedPoliciesLocked (prior pass) does no allocation beyond the fixed ~20-entry seed tables and is called only at construction/Reset/Restore, never per-request."}
---

## Notes

**gopherstack-bhhx (2026-08-13)**: fixed the `AssociateDistributionWebACL` gap `gopherstack-4ara`
confirmed but left open (see that entry below). Same bug class and same actual failure mode as the
tenant fix -- wrong request root/field (`WebACLAssociation`/`WebACLId` instead of the real
`AssociateDistributionWebACLRequest`/`WebACLArn`), `xml.Unmarshal` error checked rather than
discarded, so real clients got `400 MalformedXML` rather than a silent wipe -- but a DIFFERENT
real root than the tenant sibling's `AssociateDistributionTenantWebACLRequest`, confirmed against
`cloudfront@v1.67.4` `serializers.go:255`, so the fix needed its own dedicated request type rather
than reusing either the old shared type or the tenant op's. Surveyed every other XML type shared
across 2+ cloudfront ops for the same shared-type-different-real-root risk (`invalidationBatchXML`,
`tagXML`, `tagsXML`) -- all confirmed safe against the SDK's own canonical `types.InvalidationBatch`/
`types.Tags`/`types.Tag`, unlike `WebACLAssociation`/`WebACLId`, which never matched any real op's
wire. Verified against the real aws-sdk-go-v2 client and confirmed to fail against the pre-fix shape
by hand-reverting. Two pre-existing tests (`TestAssociateDistributionWebACL`, `TestDisassociateWebACL`)
encoded the same invented request shape the pre-fix handler expected and had been passing against
broken code indefinitely; both corrected to the real shape rather than preserved.

**gopherstack-4ara (2026-08-13)**: fixed the two wire-shape gaps `gopherstack-o31x` deliberately
left open (the KeyValueStore structural gap in the same issue was out of scope for that pass;
now RESOLVED in a follow-up pass the same day -- see the `key_value_stores` family note above
and services/cloudfrontkeyvaluestore/PARITY.md). (1) `AssociateDistributionTenantWebACL`'s request root/field
were wrong (`WebACLAssociation`/`WebACLId` instead of the real `AssociateDistributionTenantWebACLRequest`/
`WebACLArn`); the ACTUAL failure mode was every real client's request 400ing `MalformedXML`
outright, not the silent-200-with-empty-state pattern the filing bd issue described by analogy to
`PutResourcePolicy` -- gopherstack's `xml.Unmarshal` error here was checked, not discarded, unlike
the `PutResourcePolicy` precedent. The bug (every real call fails) was still real and still fixed;
only the exact mechanism differed from the filed premise, confirmed by driving the real
aws-sdk-go-v2 client both before and after the fix rather than trusting the filed description.
Also confirmed (not fixed here -- see the `gopherstack-bhhx` entry above for the follow-up fix)
that the non-tenant sibling `AssociateDistributionWebACL` shares the identical bug class, though
with a different real root name. (2) `ListConnectionGroups`/`ListConnectionFunctions` responses wrapped items under
an invented `<X><Items>` element with a fabricated `<Quantity>`; the real deserializers read a
direct `<ConnectionGroups>`/`<ConnectionFunctions>` element with no wrapper at all, so a real
client always decoded an empty list -- fixed by matching the real element names and dropping
`Quantity`. Both fixes verified against the real aws-sdk-go-v2 client, each confirmed to fail
against the pre-fix shape by hand-reverting. A pre-existing test, `TestAssociateDistributionTenantWebACL`,
encoded the exact same invented request shape the pre-fix handler expected and so had been passing
against broken code indefinitely; its body was corrected to the real shape rather than preserved.

**ETag/IfMatch** (proven, not touched this pass): Update/Delete for Distribution, CachePolicy,
OriginRequestPolicy, ResponseHeadersPolicy, OriginAccessControl, OAI, CloudFront Function,
ContinuousDeploymentPolicy, and DistributionTenant all require an `If-Match` header equal to
the resource's current ETag, else `412 PreconditionFailed`. This was already correct across
the board before this sweep; verified op-by-op, no gaps found.

**InconsistentQuantities (the headline fix this pass)**: CloudFront's wire format pairs a
caller-supplied `<Quantity>N</Quantity>` with an `<Items>...</Items>` list virtually
everywhere in the schema (57 distinct SDK types carry a `Quantity *int32` field). Real
AWS rejects a request where `N` disagrees with the actual number of items with
`InconsistentQuantities` (400). Before this pass, the emulator had **zero** occurrences of
this validation anywhere in the codebase -- `grep -rn InconsistentQuantities` was empty.
Root cause: `DistributionConfig` (and most other configs) is parsed into either a minimal
typed struct or stored as opaque `RawConfig` bytes; nothing ever re-derived the caller's
stated `Quantity` and compared it to the real list length, because Go slices don't need an
explicit count. Fix: `services/cloudfront/quantity_validation.go` adds a generic recursive
XML-tree walker (`validateQuantities`) that finds every `<X><Quantity>..</Quantity>
<Items>..</Items></X>` pairing in an arbitrary config body and flags a mismatch --
no per-resource schema modeling required, and provably safe against false positives
because it only fires when both `Quantity` and `Items` siblings are actually present
(verified against `KeyGroupConfig`/`PublicKeyConfig`/`RealtimeLogConfig`/`VpcOriginConfig`,
none of which use this pattern in the real SDK, via the smithy serializers). Wired into
all ~58 Create/Update body-parsing call sites across `handler.go`, `handler_batch2.go`,
and `handler_new_ops.go`.

**AlreadyExists error codes were all wrong (second major finding)**: `handleError`'s
`ErrAlreadyExists` sentinel had `code = "DistributionAlreadyExists"` and was reused
verbatim for CachePolicy, OriginRequestPolicy, ResponseHeadersPolicy, OriginAccessControl,
CloudFront Function, FieldLevelEncryptionConfig, FieldLevelEncryptionProfile, PublicKey,
KeyGroup, and RealtimeLogConfig name/CallerReference collisions -- i.e. creating a second
cache policy with a taken name returned the literal string `DistributionAlreadyExists`,
which is CloudFront's *distribution*-specific error code and was never even triggered by
an actual distribution collision (`CreateDistribution` doesn't use this sentinel at all;
it's fully idempotent on CallerReference, see gap above). Two existing tests
(`TestRefinement1_CachePolicyUniqueness`, `TestRefinement1_ErrorMapping`) asserted this
wrong code as if it were correct -- both fixed with justification comments pointing at the
real `aws-sdk-go-v2/service/cloudfront/types` error type names. Fix: 11 new distinct
sentinel errors (one per resource, matching the real SDK's dedicated error type where one
exists, falling back to the real generic `EntityAlreadyExists` where the SDK has no
resource-specific type -- e.g. Anycast IP lists, key value stores, trust stores). The
`handleError` switch (which had grown to cyclomatic complexity 23) was refactored into a
data-driven `errCodeMapping` table (pattern already established by EC2's `errCodeLookup`),
fixing a `cyclop` lint violation as a side effect.

**Function responses were missing FunctionARN/CreatedTime/LastModifiedTime (third
finding)**: `FunctionMetadata` requires `FunctionARN` and `LastModifiedTime` per the real
SDK (`CreatedTime`/`Stage` too). The emulator's `Function` backend struct *did* compute
and store an ARN (`b.functionARN(name)`) on create, but `functionResponseXML` (shared by
Create/Get/Describe/Publish/Update) and the inline `FunctionSummary` builder in
`handleListFunctions` never emitted it -- a real SDK caller had no way to get a function's
ARN back from any read operation, which makes attaching the function to a distribution's
`FunctionAssociations` (which require the ARN, not the name) impossible. Fixed by adding
`CreatedTime`/`LastModifiedTime` fields to `Function`, populating them on
Create/Update/Publish, and emitting all four `FunctionMetadata` fields from both XML
builders.

**InUse-on-delete guards (fourth finding)**: `DeleteCachePolicy`, `DeleteOriginRequestPolicy`,
`DeleteResponseHeadersPolicy`, and `DeleteFunction` had **no** check for whether the
resource was still referenced by a distribution -- real AWS returns `CachePolicyInUse` /
`OriginRequestPolicyInUse` / `ResponseHeadersPolicyInUse` / `FunctionInUse` (409) in that
case. (`PublicKeyInUse` and `FieldLevelEncryptionProfileInUse` already existed and are
correct -- not touched.) Fixed by adding `tokenReferencedByAnyDistribution` to
`backend_search_index.go`, reusing the pre-existing inverted token index that already
backs `ListDistributionsByCachePolicyID` etc. (built for the `ListDistributionsBy*`
control-plane ops) -- an O(1) check with no new scanning logic.

**gopherstack-na4 closed this pass: `KeyGroup`/`OAI`/`OriginAccessControl` InUse guards.**
`DeleteKeyGroup`, `DeleteOAI`, and `DeleteOriginAccessControl` had the same missing-guard gap
as the fourth finding above, deferred previously because each needed a slightly different
search token than the bare-ID case `tokenReferencedByAnyDistribution` already handled:
- `KeyGroup`: bare ID, referenced via `TrustedKeyGroups.Items` -- same pattern as
  CachePolicy, just a drop-in `tokenReferencedByAnyDistribution(id)` call. Returns
  `ResourceInUse` (409) on conflict: real `DeleteKeyGroup` has no dedicated `KeyGroupInUse`
  type, `ResourceInUse` is the actual documented error (verified against the live API
  reference), matching the existing `ErrKeyGroupNotFound` -> `NoSuchResource` precedent.
- `OAI`: referenced via `S3OriginConfig.OriginAccessIdentity`, whose real wire value is the
  literal path string `"origin-access-identity/cloudfront/{id}"`, not the bare ID (verified
  against the real `S3OriginConfig.OriginAccessIdentity` doc comment). Added
  `oaiReferencePath(id)` (also now shared by `oaiARN`) and check
  `tokenReferencedByAnyDistribution(oaiReferencePath(id))`. Returns
  `CloudFrontOriginAccessIdentityInUse` (409).
- `OriginAccessControl`: referenced via `Origin.OriginAccessControlId`, a bare ID like
  CachePolicyId -- same drop-in pattern as KeyGroup. Returns `OriginAccessControlInUse`
  (409).

All three verified end-to-end via `Test_ResourceInUse_BlocksDelete` in
`resource_in_use_test.go` (extended this pass): create the resource, attach it to a
distribution's raw config, assert delete is blocked with the correct code, disable+delete
the distribution, assert delete now succeeds.

**InconsistentQuantities trap for the next auditor**: don't add per-resource Quantity
validation by hand if you find a new Create/Update body-parsing handler missing the
`validateQuantities(body)` call -- just add the one-line call. The generic walker already
covers any shape with a `<Quantity>`/`<Items>` sibling pair; it is a no-op (returns nil)
for shapes that don't use the pattern, so it is always safe to add defensively.

**"Looks wrong but is correct" traps**:
- `ErrKeyGroupNotFound`'s wire code is `NoSuchResource`, not `NoSuchKeyGroup` -- this
  matches the real SDK (`types.NoSuchResource` is what CloudFront actually returns for a
  missing key group; there is no dedicated `NoSuchKeyGroup` type). Don't "fix" this.
- `ErrKeyValueStoreNotFound`/the new fallback `ErrAlreadyExists` both use `EntityNotFound`/
  `EntityAlreadyExists` -- also correct; the real SDK has no KVS-specific *NotFound/
  *AlreadyExists type either.
- `CreateAnycastIPList`/`CreateKeyValueStore`/`CreateTrustStore` intentionally still use the
  generic `ErrAlreadyExists` (now `EntityAlreadyExists`) sentinel rather than a dedicated
  one -- there is no `AnycastIpListAlreadyExists`/`KeyValueStoreAlreadyExists` type in
  `aws-sdk-go-v2/service/cloudfront/types@v1.60.2` to match; this is the AWS-accurate
  fallback, not an oversight.

**Protocol**: REST-XML throughout (control plane only, as of gopherstack-4ara 2026-08-13).
KeyValueStore's data plane (GetKey/PutKey/DeleteKey/ListKeys/UpdateKeys/DescribeKeyValueStore)
uses a genuinely separate REST-JSON protocol and SDK client (`cloudfrontkeyvaluestore`) with its
own unversioned path family (`/key-value-stores/...`, no `/2020-05-31/` prefix) -- it now lives
entirely in services/cloudfrontkeyvaluestore, not here. Do not re-add data-plane handlers to
this service; this Handler's RouteMatcher is anchored on `/2020-05-31/`, which the real
cloudfrontkeyvaluestore client never sends a request through, so anything added here would be
unreachable again (the exact bug gopherstack-4ara fixed).

---

## This pass's findings (2026-07-23 re-audit)

**Fifth finding: CachePolicy/OriginRequestPolicy/ResponseHeadersPolicy whitelist Items
lists, both directions.** Field-diffing these three families against the real SDK request
syntax (not just the Go struct field names, which matched) turned up the same bug class as
the second finding, but worse because it hit both parse AND serialize:
- **Parse (CachePolicy only)**: `cachePolicyHeadersConfigXML`/`CookiesConfigXML`/
  `QueryStringsConfigXML` used `xml:"Headers>Header"` / `"Cookies>Cookie"` /
  `"QueryStrings>QueryString"`. The real wire path (verified against the live
  `CreateCachePolicy`/`UpdateCachePolicy` request syntax) is `Headers>Items>Name` /
  `Cookies>Items>Name` / `QueryStrings>Items>Name`. Every whitelist/allExcept request a real
  SDK client sent had its listed names silently discarded on unmarshal -- `Headers` came back
  an empty slice with no error. (OriginRequestPolicy's parse-side tags were already correct;
  only its response side was broken -- see next bullet.)
- **Serialize (all three families, every read op)**: `cachePolicyResponseXML`,
  `orpResponseXML`, and `rhpResponseXML` either omitted the Items list entirely (emitting a
  bare `<Quantity>N</Quantity>` with no `<Items>`/no wrapper element at all) or, for
  `ResponseHeadersPolicy`'s CORS config, dropped all four list fields
  (`AccessControlAllowOrigins`/`AccessControlAllowHeaders`/`AccessControlAllowMethods`/
  `AccessControlExposeHeaders`) and two `SecurityHeadersConfig` fields
  (`ContentTypeOptions`, `ContentSecurityPolicy`) completely, even though the request parser
  already captured all of them correctly. `GetCachePolicyConfig`/
  `GetOriginRequestPolicyConfig`/`GetResponseHeadersPolicyConfig` omitted the entire nested
  config block (`ParametersInCacheKeyAndForwardedToOrigin`/`HeadersConfig+CookiesConfig+
  QueryStringsConfig`/`CorsConfig+SecurityHeadersConfig`) -- not just the lists. A real SDK
  caller had no way to discover which headers/cookies/query-strings/origins/methods a policy
  actually configures via any read op.

  Fix: added `xmlNameItems`/`xmlPluralItems` shared helpers (`handler_cache_policies.go`) and
  `cachePolicyConfigXMLBlock`/`orpConfigXMLBlock`/`rhpConfigXMLBlock` builders reused across
  each family's full response, config-only response, and List summary -- eliminating the
  triplicated, inconsistent hand-built XML that let the three call sites drift out of sync
  with each other in the first place. Locked in by
  `TestCachePolicyWhitelistItems_WireRoundTrip`,
  `TestOriginRequestPolicyWhitelistItems_WireRoundTrip`, and
  `TestResponseHeadersPolicyCORSItems_WireRoundTrip`.

**Sixth finding, CLIENT-BREAKING: `UpdateOriginRequestPolicy` routed to the wrong path.**
`parseCFOriginRequestPolicyPath` only matched `PUT` when the URL suffix ended in `/config`.
The real `UpdateOriginRequestPolicy` wire request is `PUT /2020-05-31/origin-request-policy/
{Id}` -- the bare-ID path, exactly like `UpdateCachePolicy` and `UpdateResponseHeadersPolicy`
(verified against the live API reference request syntax for all three). No real SDK client
ever sends `/config` on a PUT; `/config` is GET-only (`GetOriginRequestPolicyConfig`). Every
real `UpdateOriginRequestPolicy` call against this emulator 404'd with `NoSuchOperation:
unknown operation: Unknown`. An existing test (`TestOriginRequestPolicyCRUD/update_orp`) had
encoded this wrong path as correct and was fixed alongside the route.

**Seventh finding: `CreateDistribution`/`CopyDistribution`/`CreateStreamingDistribution`
CallerReference semantics.** The previously-filed gopherstack-mzx gap assumed a
content-comparison rule (idempotent if identical, conflict if different) by analogy with OAI.
Re-verified against the live API reference pages, not just the SDK's terser doc comments:
`CreateDistribution`'s docs state CallerReference reuse returns `DistributionAlreadyExists`
"regardless of the content of the DistributionConfig object" -- i.e. it NEVER treats reuse as
idempotent, even for byte-identical bodies. Same wording for `CreateStreamingDistribution`
(-> `StreamingDistributionAlreadyExists`) and `CopyDistribution` (which additionally wasn't
tracking CallerReference uniqueness at all before this pass -- `distributionCallerRefs` was
never populated by `CopyDistribution`). `CreateOAI` is the one family where the SDK doc's
content-comparison language is accurate and was implemented as such (identical `Comment` ->
idempotent return; different `Comment` -> `CloudFrontOriginAccessIdentityAlreadyExists`).
Existing tests asserting the old (wrong) always-idempotent behavior for Distribution and
StreamingDistribution were fixed: `TestCallerReferenceReuse` (renamed from
`TestCallerReferenceIdempotency`), `TestPersistenceRoundTrip_IndexesRebuilt`,
`TestStreamingDistributionSnapshotRestore`, `TestInMemoryBackend_StreamingDistribution`.

**Managed policies (gopherstack-a9t, closed)**: see the `managed_policies` family row above
and `managed_policies.go`'s doc comment for the full rationale, verification method, and the
deliberately-omitted Amplify-internal policy set. Every ID was cross-checked against the live
AWS documentation pages (not invented, not guessed) via `WebFetch`, since a wrong ID posing as
a real managed-policy ID would be worse than not seeding one at all.

---

## Discarded xml.Unmarshal errors sweep (2026-08-13, gopherstack-ob1g)

`encoding/xml` returns an error when a document's root element doesn't match the target
struct's `XMLName` tag, and leaves the struct **zeroed**, not partially filled -- so
`_ = xml.Unmarshal(body, &req)` with a wrong root silently discards the entire request and
proceeds on zero values, exactly the mechanism behind the `PutResourcePolicy`/
`CreateRealtimeLogConfig` bugs fixed by gopherstack-nfka (e5fbae252) above. This pass swept
every remaining non-test `_ = xml.Unmarshal(...)` call in `services/cloudfront/` (28
occurrences) and re-verified each struct's `XMLName` against the pinned SDK's serializer.

**Two more genuine whole-request wipes found and fixed** (each verified to fail against the
pre-fix code by reverting by hand, and covered by a real aws-sdk-go-v2 client round-trip
test): `UpdateVpcOrigin` (root was `UpdateVpcOriginRequest`, real root is
`VpcOriginEndpointConfig` itself -- see the corrected `UpdateVpcOrigin` op row above) and
`UpdateTrustStore` (root was `TrustStoreConfig` with Name/Comment fields that don't exist on
the real wire at all; real root is `CaCertificatesBundleSource` -- see the `UpdateTrustStore`
op row above).

**Two routing bugs found as a second layer behind hardening fixes**, per this pass's mandate
to check routability whenever touching one of these handlers (`UpdateDistributionWithStagingConfig`
matched a `/staging` suffix no real client sends; `ListDomainConflicts` matched the singular
`domain-conflict` instead of the real plural `domain-conflicts`) -- both fixed, see their op
rows above. **One more routing bug found but NOT fixed** (`UpdatePublicKey`/
`UpdateFieldLevelEncryptionConfig`/`UpdateFieldLevelEncryptionProfile` bound to the wrong path
shape) -- see `gaps` above for why it was left for a follow-up pass.

**The remaining 26 occurrences (23 in cloudfront: all but the two above; 3 in s3) had a
correct `XMLName` already** -- the discarded error was hardening, not a live bug, since no
real client body could ever hit a mismatched root there. Each now returns the service's
`MalformedXML` error (matching the pattern already established elsewhere in both codebases,
e.g. `handler_anycast_ip_lists.go`) instead of silently discarding the error, which is what
would have made the two genuine wipes above immediately findable instead of surviving three
prior audit passes. Covered by `TestXMLUnmarshalErrorHandled` (cloudfront) and
`TestXMLUnmarshalErrorHandled`/`TestRestoreObject_MalformedBodyHandled` (s3), each of which
fails against the pre-fix `_ = xml.Unmarshal(...)` form (spot-verified by reverting one
representative case, `CreateMonitoringSubscription`, by hand).

The matching s3 sweep (4 occurrences, one genuine wipe -- `GetBucketAbac`'s re-parse of its
own stored `PutBucketAbac` body used root `AbacConfiguration` where the real root is
`AbacStatus`, discovered to ALSO have a response-nesting bug once fixed: `GetBucketAbacOutput`
is httpPayload-bound so the response body itself must be the bare `AbacStatus` document, not
`AbacStatus` nested under an `AbacConfiguration` envelope -- the real deserializer function
that looks for a nested child is dead/unused generated code, not evidence of an envelope
shape) is recorded in `services/s3/PARITY.md`.

---

## Full route-table audit (2026-08-13, gopherstack-o31x)

cloudfront had produced eleven routing bugs across three prior passes (six in
gopherstack-nfka, two in gopherstack-ob1g, three filed-but-unfixed also in gopherstack-ob1g)
without ever getting a full diff of all its ops against the SDK -- only the ops other work
happened to touch were ever checked. A fleet-wide sweep (gopherstack-4nek) had to skip
cloudfront entirely because it was mid-edit at the time, and confirmed the fleet-wide finding
of zero mismatches held for the 76 services actually swept -- cloudfront was flagged as the
one confirmed hotspot and the right next target.

**Method**: extracted every real cloudfront op's method and path template from
`cloudfront@v1.67.4` serializers.go directly -- for each `awsRestxml_serializeOp<Op>`, its
`request.Method` assignment and the literal string passed to `httpbinding.SplitURI(...)`, both
in the same `HandleSerialize` function body. This is authoritative by construction: it's the
same code path the real SDK client runs to build a request, not a description of it. Extracted
167 ops this way (all of cloudfront's control-plane operations; excludes the 5 KeyValueStore
data-plane ops, which live in a structurally separate SDK client/protocol -- resolved
2026-08-13 in services/cloudfrontkeyvaluestore, see the `key_value_stores` family note above).

Then, instead of eyeballing the ~1000-line `handler_paths.go` route table by hand against that
list, built `TestExtractOperation_SDKRouteTable` (`handler_paths_sdk_diff_test.go`): a
table-driven test that builds a real `httptest.Request` for every one of the 167 extracted
(method, path) pairs and asserts `Handler.ExtractOperation` resolves it to the right op name.
Run against the pre-fix code (after Part 1's three known bugs were already fixed, but before
any of the fixes below), it reported exactly 21 mismatches -- either resolving to `"Unknown"`
(no route matched at all) or to a plausible-but-wrong sibling op. Every one of the 21 is now
fixed; see the op rows and family notes above for each (`list_distributions_by` accounts for
12, `monitoring_subscription` for 3, and one each for `GetManagedCertificateDetails`,
`DisassociateDistributionTenantWebACL`, `GetConnectionGroupByRoutingEndpoint`,
`ListConnectionGroups`, `GetDistributionTenantByDomain`, `ListConnectionFunctions`). Re-run
after all fixes, `TestExtractOperation_SDKRouteTable` reports zero mismatches across all 167
ops -- kept as a permanent regression test against this exact bug class recurring.

Two further bugs were NOT caught by the mechanical diff, because they don't manifest as a
wrong *op name* -- they're wrong signals feeding the SAME correct-looking dispatch: (1)
`CreateDistributionWithTags`/`CreateStreamingDistributionWithTags` silently resolved to their
non-tagged sibling because the WithTags flag was read from the wrong query key (a real client
never sends `Resource=WithTags`, only a bare `?WithTags`) -- found by writing a real-client
test and noticing tags never applied; (2) `TagResource`/`UntagResource` were disambiguated by
HTTP method instead of the real `Operation=Tag|Untag` query value, so `UntagResource` (always
POST on the real wire) landed on the `TagResource` handler -- found the same way. Both fixed;
see their op rows above.

**Verification**: every fix in this pass has a real `aws-sdk-go-v2` client test proving
reachability (`newTestCloudFrontClient`, driving the real router, not a hand-built request
that could encode the same wrong assumption the handler makes), and every fix was confirmed to
fail against its pre-fix shape by reverting the change by hand and re-running the test before
restoring it -- the same discipline this pass's mandate required for Part 1. Two response-body
wire-shape bugs (`AssociateDistributionTenantWebACL`'s request root/field names,
`ListConnectionGroups`/`ListConnectionFunctions`' response list wrapper) and one structural gap
(the KeyValueStore data-plane ops' host/protocol mismatch) were found as a second layer behind
these routing fixes and were deliberately NOT fixed here -- wire-shape and structural-routing
bugs are a different class of work than the method+path diff this pass's mandate scoped to. All
three were resolved in follow-up passes the same day: the two wire-shape bugs by gopherstack-4ara
(see the Notes entry above), the KeyValueStore structural gap by a further gopherstack-4ara pass
that split the data plane into services/cloudfrontkeyvaluestore.

## 2026-08-23: pagination bug sweep (ListInvalidations, ListInvalidationsForDistributionTenant, ListFunctions)

Discovered while auditing the pagination bug class found in medialive.
`handleListInvalidations`, `handleListInvalidationsForTenant`, and
`handleListFunctions` all ignored the real, per-op `Marker`/`MaxItems`
request members (cloudfront@v1.67.4: every List op's Input has
`Marker *string, MaxItems *int32`) and always returned every item in one
unbounded page — `handleListInvalidations` even hardcoded
`<IsTruncated>false</IsTruncated>`. Fixed all three using a new shared
`paginateByMarkerID` helper (`pagination_helper.go`, generalizing the
existing correct pattern from `handleListDistributions`/
`handleListAnycastIPLists`), sorted by `Invalidation.ID`/`Function.Name`.
Proven with `TestListInvalidations_SDKRoundTrip_Pagination`,
`TestListInvalidationsForDistributionTenant_SDKRoundTrip_Pagination`, and
`TestListFunctions_SDKRoundTrip_Pagination`
(`list_pagination_ignored_test.go`), each driving the real SDK client across
two 10-item pages of 25 seeded items and asserting the pages are disjoint;
all three fail against the unfixed handlers (`should have 10 item(s), but
has 25`), hand-reverted and confirmed.

Audited but NOT fixed this pass, same ignored-pagination pattern, all
low blast radius given CloudFront's own low per-resource-type account quotas
(cache policies, origin request policies, response headers policies ~20;
key groups ~10; public keys ~100; real-time log configs ~20; field-level
encryption configs/profiles, streaming distributions (legacy), trust stores,
connection groups/functions, key value stores, VPC origins, origin access
controls — all similarly small): `handleListCachePolicies`,
`handleListContinuousDeploymentPolicies`, `handleListConnectionGroups`,
`handleListConnectionFunctions`, `handleListFieldLevelEncryptions`,
`handleListFieldLevelEncryptionProfiles`, `handleListPublicKeys`,
`handleListKeyGroups`, `handleListKeyValueStores`, `handleListOAIs`,
`handleListOriginAccessControls`, `handleListOriginRequestPolicies`,
`handleListRealtimeLogConfigs`, `handleListResponseHeadersPolicies`,
`handleListStreamingDistributions`, `handleListTrustStores`,
`handleListVpcOrigins`. None of these apply an artificial default cap while
discarding the client's token (the medialive/true-loop bug shape) — they
simply return every item unbounded, so no data loss and no infinite loop,
just a spec deviation. Follow-up: apply the same `paginateByMarkerID`
helper to each.
