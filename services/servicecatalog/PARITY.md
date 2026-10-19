---
service: servicecatalog
sdk_module: aws-sdk-go-v2/service/servicecatalog@v1.47.4
last_audit_commit: e1a1dfbea
last_audit_date: 2026-10-18
overall: B            # 89 of 90 SDK operations are backed by real state, provisioning runs through the CloudFormation backend and everything is verified through the SDK client in-package and against the full composition; B until the open items below close.
ops:
  AcceptPortfolioShare: {wire: ok, errors: ok, state: ok, persist: ok, note: "imported portfolios become visible to the recipient account"}
  AssociateBudgetWithResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "budget must exist in the Budgets backend when it is wired"}
  AssociatePrincipalWithPortfolio: {wire: ok, errors: ok, state: ok, persist: ok, note: "IAM or IAM_PATTERN (* and ? globs); 1000 per portfolio; recorded per associating account"}
  AssociateProductWithPortfolio: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateServiceActionWithProvisioningArtifact: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateTagOptionWithResource: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchAssociateServiceActionWithProvisioningArtifact: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchDisassociateServiceActionFromProvisioningArtifact: {wire: ok, errors: ok, state: ok, persist: ok}
  CopyProduct: {wire: ok, errors: ok, state: ok, persist: ok, note: "copies a product (or chosen versions into TargetProductId) within the account; CopyTags; status IN_PROGRESS for the lifecycle delay"}
  CreateConstraint: {wire: ok, errors: ok, state: ok, persist: ok, note: "LAUNCH (RoleArn xor LocalRoleName), NOTIFICATION (1-5 SNS ARNs), TEMPLATE (Rules), STACKSET, RESOURCE_UPDATE; one LAUNCH/STACKSET per product and portfolio; other types once"}
  CreatePortfolio: {wire: ok, errors: ok, state: ok, persist: ok, note: "DisplayName 1-100, ProviderName 1-50, 50 tags; IdempotencyToken replays"}
  CreatePortfolioShare: {wire: ok, errors: ok, state: ok, persist: ok, note: "ACCOUNT shares need AcceptPortfolioShare; ORGANIZATION/OU/ACCOUNT nodes need trusted access, resolve member accounts through Organizations and are accepted implicitly; token only for node shares"}
  CreateProduct: {wire: ok, errors: ok, state: ok, persist: ok, note: "CLOUD_FORMATION_TEMPLATE, TERRAFORM_OPEN_SOURCE, TERRAFORM_CLOUD or EXTERNAL; the template behind LoadTemplateFromURL (emulated S3) is fetched and validated unless DisableTemplateValidation; MARKETPLACE products are rejected"}
  CreateProvisionedProductPlan: {wire: ok, errors: ok, state: ok, persist: ok, note: "a CloudFormation change set; a new launch gets a PLAN_IN_PROGRESS placeholder product"}
  CreateProvisioningArtifact: {wire: ok, errors: ok, state: ok, persist: ok, note: "version name is not unique-checked; template validated like CreateProduct"}
  CreateServiceAction: {wire: ok, errors: ok, state: ok, persist: ok, note: "SSM_AUTOMATION; Definition Name and Version required, Parameters a JSON list of {Name, Type TARGET|TEXT_VALUE}"}
  CreateTagOption: {wire: ok, errors: ok, state: ok, persist: ok, note: "Key/Value unique per account and Region; aws: keys rejected"}
  DeleteConstraint: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePortfolio: {wire: ok, errors: ok, state: ok, persist: ok, note: "ResourceInUseException while products, principals or shares remain"}
  DeletePortfolioShare: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteProduct: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteProvisionedProductPlan: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteProvisioningArtifact: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteServiceAction: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTagOption: {wire: ok, errors: ok, state: ok, persist: ok, note: "ResourceInUseException while associated"}
  DescribeConstraint: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeCopyProductStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribePortfolio: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribePortfolioShareStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribePortfolioShares: {wire: ok, errors: ok, state: ok, persist: ok, note: "Type ACCOUNT, ORGANIZATION, ORGANIZATIONAL_UNIT or ORGANIZATION_MEMBER_ACCOUNT"}
  DescribeProduct: {wire: ok, errors: ok, state: ok, persist: ok, note: "only active versions; LaunchPaths are the portfolios the caller can launch from"}
  DescribeProductAsAdmin: {wire: ok, errors: ok, state: ok, persist: ok, note: "owner account only; SourcePortfolioId reads through an imported portfolio"}
  DescribeProductView: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeProvisionedProduct: {wire: ok, errors: ok, state: ok, persist: ok, note: "status is UNDER_CHANGE while the last record is IN_PROGRESS"}
  DescribeProvisionedProductPlan: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeProvisioningArtifact: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeProvisioningParameters: {wire: ok, errors: ok, state: ok, persist: ok, note: "parameters and outputs come from the template; constraint summaries, TEMPLATE rules as usage instructions and active tag options of the path"}
  DescribeRecord: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeServiceAction: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeServiceActionExecutionParameters: {wire: ok, errors: ok, state: ok, persist: ok, note: "TARGET defaults are the physical IDs of the stack resources"}
  DescribeTagOption: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableAWSOrganizationsAccess: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateBudgetFromResource: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociatePrincipalFromPortfolio: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateProductFromPortfolio: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateServiceActionFromProvisioningArtifact: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateTagOptionFromResource: {wire: ok, errors: ok, state: ok, persist: ok}
  EnableAWSOrganizationsAccess: {wire: ok, errors: ok, state: ok, persist: ok, note: "toggles the servicecatalog.amazonaws.com trusted service in the Organizations backend (a local flag when none is wired)"}
  ExecuteProvisionedProductPlan: {wire: ok, errors: ok, state: ok, persist: ok, note: "executes the change set and records a PROVISION or UPDATE record"}
  ExecuteProvisionedProductServiceAction: {wire: ok, errors: ok, state: ok, persist: ok, note: "starts the SSM automation through the SSM backend; TARGET parameters are required; record EXECUTE_PROFILE_SERVICE_ACTION"}
  GetAWSOrganizationsAccessStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  GetProvisionedProductOutputs: {wire: ok, errors: ok, state: ok, persist: ok, note: "the stack outputs, filtered by OutputKeys"}
  ImportAsProvisionedProduct: {wire: ok, errors: ok, state: ok, persist: ok, note: "adopts a healthy existing stack (PhysicalId) once"}
  ListAcceptedPortfolioShares: {wire: ok, errors: ok, state: ok, persist: ok, note: "IMPORTED (account shares) and AWS_ORGANIZATIONS (node shares); AWS_SERVICECATALOG never matches"}
  ListBudgetsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListConstraintsForPortfolio: {wire: ok, errors: ok, state: ok, persist: ok}
  ListLaunchPaths: {wire: ok, errors: ok, state: ok, persist: ok, note: "a launch path is a portfolio the caller can launch the product from"}
  ListOrganizationPortfolioAccess: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPortfolioAccess: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPortfolios: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPortfoliosForProduct: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPrincipalsForPortfolio: {wire: ok, errors: ok, state: ok, persist: ok}
  ListProvisionedProductPlans: {wire: ok, errors: ok, state: ok, persist: ok}
  ListProvisioningArtifacts: {wire: ok, errors: ok, state: ok, persist: ok}
  ListProvisioningArtifactsForServiceAction: {wire: ok, errors: ok, state: ok, persist: ok}
  ListRecordHistory: {wire: ok, errors: ok, state: ok, persist: ok, note: "newest first; SearchFilter keys identifier, provisionedproduct, product, provisioningartifact, operation, status"}
  ListResourcesForTagOption: {wire: ok, errors: ok, state: ok, persist: ok}
  ListServiceActions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListServiceActionsForProvisioningArtifact: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagOptions: {wire: ok, errors: ok, state: ok, persist: ok}
  NotifyProvisionProductEngineWorkflowResult: {wire: ok, errors: ok, state: ok, persist: ok, note: "completes the record of an EXTERNAL or Terraform product; the workflow token is wf-<RecordId>"}
  NotifyTerminateProvisionedProductEngineWorkflowResult: {wire: ok, errors: ok, state: ok, persist: ok}
  NotifyUpdateProvisionedProductEngineWorkflowResult: {wire: ok, errors: ok, state: ok, persist: ok}
  ProvisionProduct: {wire: ok, errors: ok, state: ok, persist: ok, note: "resolves product, version and launch path, validates parameters against the template, applies LAUNCH role, NOTIFICATION ARNs and TEMPLATE rules, then creates the stack SC-<account>-<id> through the CloudFormation backend; record CREATED in the response, IN_PROGRESS for the lifecycle delay, then SUCCEEDED or FAILED; failure leaves the product in ERROR; ProvisionToken replays"}
  RejectPortfolioShare: {wire: ok, errors: ok, state: ok, persist: ok, note: "share stays offered; recipient principal associations are dropped"}
  ScanProvisionedProducts: {wire: ok, errors: ok, state: ok, persist: ok}
  SearchProducts: {wire: ok, errors: ok, state: ok, persist: ok, note: "FullTextSearch, Owner, ProductType, SourceProductId filters, Owner/ProductType/Vendor aggregations; visibility follows portfolio access and principal association"}
  SearchProductsAsAdmin: {wire: ok, errors: ok, state: ok, persist: ok}
  SearchProvisionedProducts: {wire: ok, errors: ok, state: ok, persist: ok, note: "SearchQuery tokens such as name:, status:, productId: and tags:key=value plus free text; sort and TotalResultsCount"}
  TerminateProvisionedProduct: {wire: ok, errors: ok, state: ok, persist: ok, note: "deletes the stack and the provisioned product; RetainPhysicalResources keeps the stack; IgnoreErrors drops the product despite a stack failure"}
  UpdateConstraint: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePortfolio: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePortfolioShare: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateProduct: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateProvisionedProduct: {wire: ok, errors: ok, state: ok, persist: ok, note: "new version, parameters (UsePreviousValue) or tags; RESOURCE_UPDATE can forbid tag changes; failure leaves the product TAINTED"}
  UpdateProvisionedProductProperties: {wire: ok, errors: ok, state: ok, persist: ok, note: "OWNER and LAUNCH_ROLE"}
  UpdateProvisioningArtifact: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateServiceAction: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateTagOption: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  Portfolio: {status: ok, note: "Owned by one account in one Region; imported portfolios are visible to recipients through accepted shares. Delete is blocked by products, principals and shares."}
  Product: {status: ok, note: "Product views have their own prodview- ID; the first version is created with the product. Versions can be deactivated, which hides them from end users."}
  Constraint: {status: ok, note: "Parameters are validated per type. LAUNCH, NOTIFICATION, TEMPLATE (rule subset) and RESOURCE_UPDATE affect launches; STACKSET is stored but launches are refused."}
  Share: {status: ok, note: "Account shares are offered then accepted; node shares expand through the Organizations backend into per-account member shares accepted on creation."}
  TagOption: {status: ok, note: "Associations are portfolio or product only; offered through DescribeProvisioningParameters, never applied to launches automatically."}
  ServiceAction: {status: ok, note: "SSM Automation is started through the SSM backend; the document must exist."}
  Provisioning: {status: ok, note: "Stack creation is synchronous inside the request; the record then reports IN_PROGRESS for the lifecycle delay (default 0, Backend.SetLifecycleDelay). Product status is derived from the last record."}
  Plan: {status: ok, note: "Backed by CloudFormation change sets."}
gaps: []
items_still_open:
  - ListStackInstancesForProvisionedProduct: not implemented; STACKSET products are not provisioned
  - CreateConstraint STACKSET: accepted and stored, but ProvisionProduct and CreateProvisionedProductPlan on such a path return OperationNotSupportedException
  - CreateConstraint TEMPLATE: only Fn::Equals/Not/And/Or/Contains/EachMemberEquals/EachMemberIn/Ref rules are evaluated; an assertion using any other function passes
  - CreateConstraint LAUNCH: the role is passed to CloudFormation as RoleARN but never assumed or checked in IAM
  - DescribeProvisionedProduct: CloudWatchDashboards is always empty
  - CreateProduct SourceConnection: the CodeStar repository is stored but never synced, so LastSync and SourceRevision stay empty
  - Notify*EngineWorkflowResult: no channel hands the workflow token to an engine; it is wf-<RecordId>
  - CreatePortfolioShare: SharePrincipals and ShareTagOptions are recorded but recipients get no copy of principals or tag options
  - AcceptPortfolioShare: AWS_SERVICECATALOG shares do not exist, so only IMPORTED and AWS_ORGANIZATIONS are acceptable
  - SearchProducts: a caller with an unresolved identity (no principal) sees every accessible portfolio; a resolved non-root identity needs a principal association
  - ProvisionProduct ProvisioningPreferences: accepted and ignored (StackSet-only)
  - CreateProduct MARKETPLACE: rejected with InvalidParametersException
  - TagOptions: not applied as tags on launched stacks
  - ErrorClasses: several error classes (ResourceInUse on disassociating a constrained product, InvalidParameters for an under-change provisioned product, DuplicateResource on re-imports) are chosen from documented behaviour, not SDK evidence
structural_gaps:
  - "Terraform, EXTERNAL and CodeStar-sourced products have no engine inside the emulator: an engine would have to call Notify*EngineWorkflowResult itself."
deferred: []
leaks: {status: clean, note: "leak_main_test.go runs goleak over the package; record and copy transitions are lazy deadlines read under the backend lock and the CloudFormation calls run in the request goroutine."}
---

## Notes

Protocol is awsjson1.1 with target prefix `AWS242ServiceCatalogService.`; timestamps are epoch-seconds numbers (`awstime.Epoch`). The service is regional: every record carries its account and Region and a resource of another Region or account is ResourceNotFoundException. Shapes and the exception set were read from the pinned SDK's types, serializers and deserializers.

Emulator choices the SDK does not document:

- IDs are the AWS prefixes (`port-`, `prod-`, `prodview-`, `pa-`, `cons-`, `tag-`, `act-`, `pp-`, `rec-`, `plan-`, `share-`, `copy-`) plus 13 hex characters. Provisioned product stacks are named `SC-<account>-<provisioned product id>` and carry the product tags, the caller tags and `aws:servicecatalog:*` tags.
- Update semantics follow CloudFormation: a parameter not mentioned reverts to its template default, `UsePreviousValue` keeps the stored value, a parameter with neither fails.
- A product with several launch paths needs PathId or PathName; an unidentified caller is not restricted by principal associations.
- Wiring (`internal/app/servicecatalog_wiring.go`): CloudFormation (stacks, change sets, template inspection, S3 template fetch), Organizations (node resolution and trusted access), SSM (automation) and Budgets (existence check).
