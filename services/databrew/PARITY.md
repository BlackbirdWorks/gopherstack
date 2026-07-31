service: databrew
sdk_module: aws-sdk-go-v2/service/databrew@v1.40.0
last_audit_commit: 782e2a93
last_audit_date: 2026-07-31
overall: A            # 2026-07-23: genuine fixes found across recipe version history, job/dataset field gaps, and an invented UpdateProject field
                      # 2026-07-31: pkgs/sdkcheck reverse check found DeleteRecipe wrongly advertised/documented as a real SDK op (it isn't -- see its ops-block note); corrected, route left wired as internal test/tooling scaffolding. Grade held at A: a documentation defect, not a served-client bug.
ops:
  CreateDataset: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now accepts PathOptions (S3 wildcard-path dataset config: FilesLimit/LastModifiedDateCondition/Parameters, incl. DatasetParameter.DatetimeOptions) -- was previously silently discarded. Also fixed: Dataset now carries AccountId (aws-sdk-go-v2/service/databrew/types.Dataset has an AccountId member; ListDatasets items were always echoing it empty)."}
  DescribeDataset: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDatasets: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDataset: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now accepts PathOptions, same gap as CreateDataset"}
  DeleteDataset: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateRecipe: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: RecipeVersion on the working draft is now the literal string \"LATEST_WORKING\" (was \"0.1\", a gopherstack-invented value) -- aws-sdk-go-v2/service/databrew/types.Recipe's RecipeVersion doc comment documents only numeric X.Y or the literal LATEST_WORKING/LATEST_PUBLISHED; the codebase's own CreateRecipeJob handler already defaulted unpublished RecipeReference.RecipeVersion to \"LATEST_WORKING\", confirming this is the real value."}
  DescribeRecipe: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: RecipeVersion is now a real parameter (was previously accepted on the wire via the recipeVersion query param -- confirmed against awsRestjson1_serializeOpHttpBindingsDescribeRecipeInput -- but silently ignored, always returning the single tracked version). Resolves \"\"/LATEST_PUBLISHED/LATEST_WORKING/a numeric version against the new real per-recipe version history (see families.recipe_version_history below)."}
  ListRecipes: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: RecipeVersion filter (query param \"recipeVersion\") is now read and applied. Default (no filter) now matches the documented real behavior -- \"If RecipeVersion is omitted, ListRecipes returns all of the LATEST_PUBLISHED recipe versions\" -- so a never-published recipe no longer appears in a default listing; RecipeVersion=LATEST_WORKING lists every recipe's working draft regardless of publish state. Previously this filter didn't exist at all and every recipe was always listed once."}
  PublishRecipe: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now appends a new numbered version (\"N.0\") to the recipe's real version history on every call instead of overwriting a single tracked \"1.0\" -- see families.recipe_version_history."}
  UpdateRecipe: {wire: ok, errors: ok, state: ok, persist: ok}
  # DeleteRecipe is intentionally NOT listed as an advertised SDK op here.
  # 2026-07-31 CORRECTION: the row that used to live at this position ("wire:
  # ok, ...") was inaccurate -- DeleteRecipe is not a real AWS DataBrew SDK
  # operation at all (verified against botocore's databrew service-2.json:
  # the only DELETE route under /recipes is DELETE
  # /recipes/{name}/recipeVersion/{recipeVersion}, i.e. DeleteRecipeVersion;
  # there is no bare "DELETE /recipes/{name}". Real clients delete an entire
  # recipe by calling BatchDeleteRecipeVersion with every version including
  # LATEST_WORKING). Caught by pkgs/sdkcheck's reverse check (commit
  # 12cfe14d5; gopherstack-vhw2 category A). The route (DELETE /recipes/{name}
  # with no sub-path -> handleDeleteRecipe, which still cascades to delete the
  # recipe's entire published version history) stays wired as internal
  # test/tooling scaffolding -- parseRecipeOp matches the real
  # "recipeVersion/{version}" suffix first, so this fallback never shadows a
  # real client's DeleteRecipeVersion/BatchDeleteRecipeVersion call.
  # GetSupportedOperations() no longer advertises it; see opDeleteRecipe's doc
  # comment in handler.go. Same resolution as CloudFront's
  # GetFunctionAssociations/SetFunctionAssociations and EMR's
  # ListTagsForResource.
  ListRecipeVersions: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now backed by a real per-recipe published-version history (see families.recipe_version_history) instead of echoing the single tracked recipe row; excludes LATEST_WORKING per the real op's doc comment (\"except for LATEST_WORKING\"); a never-published recipe now correctly returns an empty (non-nil) Recipes list instead of one containing the working draft -- confirmed via a real aws-sdk-go-v2 client round trip (Test_SDKRoundTrip_ListRecipeVersions_BarePath, updated this pass)."}
  BatchDeleteRecipeVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now a real backend op (was previously a bare DescribeRecipe existence check with a no-op body) operating on the real version history. Implements the documented split between whole-request rejection (empty/oversized/duplicate/syntactically-invalid version list -> ValidationException, nothing deleted) and per-version partial failure (a version that doesn't exist, or LATEST_WORKING while other versions still exist -> reported in the response's Errors list, call still succeeds) -- confirmed against aws-sdk-go-v2/service/databrew's BatchDeleteRecipeVersionInput/Output doc comments and types.RecipeVersionErrorDetail (RecipeVersion/ErrorCode/ErrorMessage). state=ok (was state=partial): no longer a single-version simplification."}
  DeleteRecipeVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now a real backend op deleting one entry from the real version history; 404s for a version that doesn't exist (previously always 200 no-op'd); rejects LATEST_PUBLISHED and syntactically invalid identifiers with ValidationException (\"LATEST_PUBLISHED is not supported\" per the real op's doc comment); LATEST_WORKING only deletes (removing the whole recipe) when no published versions remain. state=ok (was state=partial)."}
  CreateProject: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: Project now carries AccountId, same gap class as Dataset"}
  DescribeProject: {wire: ok, errors: ok, state: ok, persist: ok}
  ListProjects: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateProject: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: DELETED a gopherstack-invented DatasetName field -- aws-sdk-go-v2/service/databrew's UpdateProjectInput has only Name/RoleArn/Sample, no DatasetName (a project's dataset is fixed at creation); the handler/backend previously accepted and applied a DatasetName update with no basis in the real wire shape, making a project's dataset appear mutable in our own emulation. Now DatasetName is immutable after CreateProject, matching the real API."}
  DeleteProject: {wire: ok, errors: ok, state: ok, persist: ok}
  StartProjectSession: {wire: ok, errors: ok, state: ok, persist: ok, note: "unchanged from prior audit: AssumeControl/session lifecycle not modeled beyond returning Name; acceptable no-op for a project-editor session DataBrew clients don't poll for correctness"}
  SendProjectSessionAction: {wire: ok, errors: ok, state: ok, persist: ok, note: "unchanged from prior audit: same as StartProjectSession -- interactive-editor action, echoes Name only"}
  CreateProfileJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now accepts and stores Configuration (-> Job.ProfileConfiguration), JobSample, ValidationConfigurations, EncryptionMode, EncryptionKeyArn, LogSubscription, MaxCapacity, MaxRetries, Timeout -- all previously either parsed into a local var and silently dropped (MaxCapacity/MaxRetries/Timeout -- CreateJob had no signature slot for them at all) or not parsed from the request body in the first place (the rest), despite Job already having matching JSON output fields. Also: Job now carries AccountId."}
  CreateRecipeJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same MaxCapacity/MaxRetries/Timeout-silently-dropped-on-create bug as CreateProfileJob, plus now accepts DataCatalogOutputs, DatabaseOutputs, EncryptionMode, EncryptionKeyArn, LogSubscription (previously not parsed from the request body at all)."}
  DescribeJob: {wire: ok, errors: ok, state: ok, persist: ok}
  ListJobs: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateProfileJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now accepts Configuration/JobSample/ValidationConfigurations/EncryptionMode/EncryptionKeyArn/LogSubscription, same gap as CreateProfileJob"}
  UpdateRecipeJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now accepts DataCatalogOutputs/DatabaseOutputs/EncryptionMode/EncryptionKeyArn/LogSubscription, same gap as CreateRecipeJob"}
  DeleteJob: {wire: ok, errors: ok, state: ok, persist: ok}
  StartJobRun: {wire: ok, errors: ok, state: ok, persist: ok, note: "unchanged from prior audit: STARTING -> SUCCEEDED after 100ms via a tracked goroutine (Shutdown-aware, no leak)"}
  ListJobRuns: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeJobRun: {wire: ok, errors: ok, state: ok, persist: ok}
  StopJobRun: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateRuleset: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: Ruleset now carries AccountId AND RuleCount, kept in sync with Rules on every Create/Update -- see families.ruleset_list_shape below."}
  DescribeRuleset: {wire: ok, errors: ok, state: ok, persist: ok}
  ListRulesets: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: see families.ruleset_list_shape -- the real ListRulesetsOutput.Rulesets is []types.RulesetItem, whose deserializer reads \"RuleCount\" (an int), not \"Rules\" (the full list); every ruleset was silently reporting as having 0 rules to a real client's ListRulesets call."}
  UpdateRuleset: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: RuleCount now kept in sync with Rules on update, same gap as CreateRuleset"}
  DeleteRuleset: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateSchedule: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: Schedule now carries AccountId, same gap class as Dataset"}
  DescribeSchedule: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSchedules: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateSchedule: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteSchedule: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  error_wire_shape: {status: ok, note: "unchanged from prior audit: emits {\"__type\": \"<Exception>\", \"message\": \"...\"}, confirmed via a real aws-sdk-go-v2 client round trip (Test_SDKRoundTrip_ErrorsAreTyped)."}
  recipe_version_history: {status: ok, note: "NEW this pass, replaces the prior single-tracked-version simplification. InMemoryBackend now holds a real per-region, per-recipe ordered list of published version snapshots (recipeVersions, same order-sensitive-map pattern as jobRuns -- see store.go/store_setup.go doc comments), persisted via backendSnapshot.RecipeVersions. PublishRecipe appends a new numbered snapshot (\"N.0\", N = prior published count + 1) each call instead of overwriting a single \"1.0\"; the working draft (b.recipes table row) always keeps RecipeVersion=\"LATEST_WORKING\" and is independent of publish state. DeleteRecipe cascades to delete the recipe's entire version history (no ghost rows). Field-diffed against CreateRecipe/DescribeRecipe/ListRecipes/PublishRecipe/UpdateRecipe/DeleteRecipeVersion/BatchDeleteRecipeVersion/ListRecipeVersions doc comments in aws-sdk-go-v2/service/databrew/api_op_*.go and types.Recipe's RecipeVersion doc comment."}
  ruleset_list_shape: {status: ok, note: "NEW finding this pass: DescribeRulesetOutput and ListRulesetsOutput are genuinely DIFFERENT shapes in the real SDK -- Describe returns Rules (the full list) with no AccountId/RuleCount, List returns []types.RulesetItem (AccountId + RuleCount, an int, with NO Rules field at all; confirmed against awsRestjson1_deserializeDocumentRulesetItem's key switch). gopherstack shares one Ruleset Go struct for both responses (documented in its doc comment) rather than maintaining two marshal shapes -- this is wire-safe both ways since restjson1 clients silently ignore unrecognized JSON keys (Describe's real client ignores the extra RuleCount/AccountId keys; List's real client ignores the extra Rules key), and RuleCount is kept authoritatively in sync with len(Rules) on every Create/Update."}
  account_id_field: {status: ok, note: "NEW finding this pass: aws-sdk-go-v2/service/databrew/types' Dataset/Job/Project/RulesetItem/Schedule (NOT Recipe -- it has no AccountId member) all carry an AccountId member that gopherstack's models previously omitted entirely, so ListDatasets/ListJobs/ListProjects/ListRulesets/ListSchedules always echoed an empty AccountId to real clients. Now populated from the backend's account ID at Create time on all five entities. Also included (harmlessly, per the same silently-ignored-unknown-key reasoning as ruleset_list_shape) on the corresponding Describe* responses, which don't have AccountId in their real output shape -- avoids needing five more split marshal types for a field real Describe clients would just ignore if present anyway."}
gaps:
  - "CreateProfileJob/UpdateProfileJob's Configuration (ProfileConfiguration) and JobSample are stored as map[string]any pass-through rather than typed structs -- wire-compatible (arbitrary nested JSON round-trips byte-for-byte) but not validated; same for CreateRecipeJob/UpdateRecipeJob's DataCatalogOutputs/DatabaseOutputs. This mirrors the FormatOptions sub-fields deferral below and was a deliberate scope choice this pass (the fields are now at least threaded through and stored/returned, closing the actual data-loss gap; typed validation is a separate, lower-priority refinement -- bd: TODO file if prioritized)."
  - "StartProjectSession/SendProjectSessionAction (the interactive project-editor session flow) remain near-total no-ops beyond echoing Name -- acceptable since these model an interactive editing session tests don't poll for correctness, but flagged for completeness (unchanged from prior audit)."
deferred:
  - "CSV/Excel/Json FormatOptions sub-fields (e.g. Delimiter, HeaderRow, SheetNames) are passed through as map[string]any rather than typed structs -- wire-compatible (arbitrary nested JSON round-trips byte-for-byte) but not validated (unchanged from prior audit)."
leaks: {status: clean, note: "StartJobRun's delayed STARTING->SUCCEEDED transition runs on a b.wg-tracked goroutine gated by b.svcCtx; Shutdown cancels svcCtx and waits on wg bounded by the caller's ctx (see shutdown_test.go). This pass added no new goroutines/tickers. The new recipeVersions map follows jobRuns' existing lifecycle pattern (Reset/Snapshot/Restore-wired, see store.go) and DeleteRecipe now cascade-deletes it so no ghost rows survive a deleted recipe."}
