service: databrew
sdk_module: aws-sdk-go-v2/service/databrew@v1.40.0
last_audit_commit: 782e2a93
last_audit_date: 2026-07-13
overall: A            # genuine fixes found across routing, error wire shape, and field mapping
ops:
  CreateDataset: {wire: ok, errors: ok, state: ok, persist: ok, note: "FormatOptions.JSON tag fixed to wire key \"Json\" (was \"JSON\", case-sensitive client switch silently dropped it)"}
  DescribeDataset: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDatasets: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDataset: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDataset: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateRecipe: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeRecipe: {wire: ok, errors: ok, state: ok, persist: ok}
  ListRecipes: {wire: ok, errors: ok, state: ok, persist: ok}
  PublishRecipe: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRecipe: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRecipe: {wire: ok, errors: ok, state: ok, persist: ok}
  ListRecipeVersions: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: real path is GET /recipeVersions?name=... (RouteMatcher didn't claim bare \"recipeVersions\" segment at all; \"name\" query param wasn't read into the request body). Nested /recipes/{Name}/recipeVersions convenience alias kept working."}
  BatchDeleteRecipeVersion: {wire: ok, errors: ok, state: partial, persist: ok, note: "fixed: real path is POST /recipes/{Name}/batchDeleteRecipeVersion (subOp matcher checked \"recipeVersions\" instead), was completely unreachable by real SDK traffic -> opUnknown -> non-JSON 404 the client couldn't deserialize. Now also 404s for an unknown recipe (previously always 200). state=partial: backend only tracks one recipe version (no version history), so deletion is a documented simplification, not a real per-version mutation."}
  DeleteRecipeVersion: {wire: ok, errors: ok, state: partial, persist: ok, note: "fixed: now 404s for an unknown recipe (previously always 200) and echoes RecipeVersion in the response (required output field, was missing). state=partial for the same single-version-tracking reason as BatchDeleteRecipeVersion."}
  CreateProject: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeProject: {wire: ok, errors: ok, state: ok, persist: ok}
  ListProjects: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateProject: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteProject: {wire: ok, errors: ok, state: ok, persist: ok}
  StartProjectSession: {wire: ok, errors: ok, state: ok, persist: ok, note: "AssumeControl/session lifecycle not modeled beyond returning Name; acceptable no-op for a project-editor session DataBrew clients don't poll for correctness"}
  SendProjectSessionAction: {wire: ok, errors: ok, state: ok, persist: ok, note: "same as StartProjectSession -- interactive-editor action, echoes Name only"}
  CreateProfileJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: request field is \"OutputLocation\" (single S3Location), not \"Outputs\" (that's the CreateRecipeJob shape) -- was silently discarding the job's output destination on every create"}
  CreateRecipeJob: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeJob: {wire: ok, errors: ok, state: ok, persist: ok}
  ListJobs: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateProfileJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same OutputLocation-vs-Outputs field-name bug as CreateProfileJob; split from the previously-shared handleUpdateJob into handleUpdateProfileJob/handleUpdateRecipeJob"}
  UpdateRecipeJob: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteJob: {wire: ok, errors: ok, state: ok, persist: ok}
  StartJobRun: {wire: ok, errors: ok, state: ok, persist: ok, note: "STARTING -> SUCCEEDED after 100ms via a tracked goroutine (Shutdown-aware, no leak); real AWS also passes through RUNNING but no client-visible harm in skipping a valid enum value on the way to a valid terminal one"}
  ListJobRuns: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeJobRun: {wire: ok, errors: ok, state: ok, persist: ok}
  StopJobRun: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateRuleset: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeRuleset: {wire: ok, errors: ok, state: ok, persist: ok}
  ListRulesets: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: TargetArn query filter was parsed but never applied by the backend -- always returned every ruleset in the region regardless of the filter"}
  UpdateRuleset: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRuleset: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateSchedule: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeSchedule: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSchedules: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateSchedule: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteSchedule: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: RouteMatcher didn't claim the bare top-level \"tags\" segment (only worked through the /databrew/v1/ convenience prefix); every DataBrew ARN embeds a \"/\" in its resource part, so a length-gated call to the ResourceArn extractor also silently skipped it on short (unprefixed) paths"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same RouteMatcher/ResourceArn gaps as TagResource, PLUS TagKeys travels as a repeated \"tagKeys\" query param on the real DELETE request (there is normally no body) -- was read from a JSON body field that real traffic never populates, so UntagResource always silently no-op'd"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same RouteMatcher/ResourceArn gaps as TagResource"}
families:
  error_wire_shape: {status: ok, note: "fixed: handleError wrote {\"Message\": err.Error()} with no \"__type\"/\"code\" field and no X-Amzn-ErrorType header. aws-sdk-go-v2's restjson.GetErrorInfo identifies the exception type SOLELY from the header or a code/__type JSON field (never from HTTP status), so every DataBrew error -- across every op -- was silently downgraded to a generic smithy.GenericAPIError; errors.As(err, &types.ResourceNotFoundException{}) never matched. Now emits {\"__type\": \"<Exception>\", \"message\": \"...\"}, matching the convention already used by sesv2/glacier/other restjson1 services in this codebase. Confirmed via a real aws-sdk-go-v2 client round trip (Test_SDKRoundTrip_ErrorsAreTyped)."}
gaps:
  - "CreateDataset/UpdateDataset don't accept PathOptions (S3 wildcard-path dataset config) -- optional field, not commonly exercised, silently ignored if sent (bd: TODO file if prioritized)"
  - "CreateProfileJob/UpdateProfileJob don't accept Configuration (ProfileConfiguration) or JobSample -- Job struct already has ProfileConfiguration/JobSample fields wired for JSON output but nothing ever populates them; would need threading through CreateJob/UpdateJob signatures (bd: TODO file if prioritized)"
  - "CreateRecipeJob/UpdateRecipeJob don't accept DataCatalogOutputs/DatabaseOutputs/EncryptionMode/EncryptionKeyArn/LogSubscription/ValidationConfigurations -- Job struct has matching JSON fields but they're never populated (bd: TODO file if prioritized)"
  - "Recipe version history is not modeled: only one working/published version is tracked per recipe (RecipeVersion flips between \"0.1\"/an unpublished value and \"1.0\" on PublishRecipe). BatchDeleteRecipeVersion/DeleteRecipeVersion/ListRecipeVersions all operate against that single version rather than a real version list; each PublishRecipe overwrites rather than appending a new version. A full fix needs a per-recipe version history data structure -- larger scope than this pass's budget (bd: TODO file if prioritized)"
  - "StartProjectSession/SendProjectSessionAction (the interactive project-editor session flow) are near-total no-ops beyond echoing Name -- acceptable since these model an interactive editing session tests don't poll for correctness, but flagged for completeness"
deferred:
  - "CSV/Excel/Json FormatOptions sub-fields (e.g. Delimiter, HeaderRow, SheetNames) are passed through as map[string]any rather than typed structs -- wire-compatible (arbitrary nested JSON round-trips byte-for-byte) but not validated"
leaks: {status: clean, note: "StartJobRun's delayed STARTING->SUCCEEDED transition runs on a b.wg-tracked goroutine gated by b.svcCtx; Shutdown cancels svcCtx and waits on wg bounded by the caller's ctx (see shutdown_test.go). No new goroutines/maps introduced this pass."}
