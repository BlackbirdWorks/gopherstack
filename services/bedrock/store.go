package bedrock

import (
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	textTypeText = "TEXT"
)

// Inference type constants mirroring the AWS Bedrock API values.
const (
	inferenceTypeOnDemand    = "ON_DEMAND"
	inferenceTypeProvisioned = "PROVISIONED"
)

// Customization type constants mirroring the AWS Bedrock API values
// (bedrock@v1.66.4 types/enums.go CustomizationType).
const (
	customizationTypeFineTuning = "FINE_TUNING"
	customizationTypeImported   = "IMPORTED"
)

const bedrockDefaultPageSize = 100

// maxProvisionedModelUnits is the upper bound AWS enforces on the modelUnits value
// for a single CreateProvisionedModelThroughput request.
const maxProvisionedModelUnits = 1000

// Resource lifecycle status constants.
const (
	statusCreating   = "Creating"
	statusInService  = "InService"
	statusInProgress = "InProgress"
	statusCompleted  = "Completed"
	statusStopped    = "Stopped"
	statusAvailable  = "AVAILABLE"
	statusActive     = "Active"
	lifecycleActive  = "ACTIVE"
)

// draftVersion is the AWS DRAFT version marker shared by several resource
// families (guardrails' mutable current version, e.g.).
const draftVersion = "DRAFT"

// sortOrderDescending is the real AWS SortOrder value used by every List op
// in this package that supports Ascending (default)|Descending sorting
// (ListEvaluationJobs, ListModelInvocationJobs, ListAdvancedPromptOptimizationJobs).
const sortOrderDescending = "Descending"

// InMemoryBackend stores Amazon Bedrock state in memory.
type InMemoryBackend struct {
	appConfig                   any
	guardrails                  *store.Table[Guardrail]
	guardrailVersions           *store.Table[GuardrailVersion] // guardrailID+":"+version → version
	provisionedModelThroughputs *store.Table[ProvisionedModelThroughput]
	evaluationJobs              *store.Table[EvaluationJob]
	automatedReasoningPolicies  *store.Table[AutomatedReasoningPolicy]
	arpBuildWorkflows           *store.Table[AutomatedReasoningPolicyBuildWorkflow] // workflowID → workflow
	arpTestCases                *store.Table[AutomatedReasoningPolicyTestCase]      // testCaseID → test case
	arpVersions                 *store.Table[AutomatedReasoningPolicyVersion]       // policyArn+":"+version → version
	arpVersionCountByPolicy     map[string]int                                      // policyARN → version counter
	customModels                *store.Table[CustomModel]                           // modelArn → model
	customModelDeployments      *store.Table[CustomModelDeployment]                 // deploymentArn → deployment
	foundationModelAgreements   *store.Table[FoundationModelAgreement]              // modelID → agreement
	modelCustomizationJobs      *store.Table[ModelCustomizationJob]                 // jobArn → job
	modelCopyJobs               *store.Table[ModelCopyJob]                          // jobArn → job
	modelImportJobs             *store.Table[ModelImportJob]                        // jobArn → job
	inferenceProfiles           *store.Table[InferenceProfile]                      // profileArn → profile
	marketplaceEndpoints        *store.Table[MarketplaceModelEndpoint]              // endpointArn → endpoint
	loggingConfig               *ModelInvocationLoggingConfiguration
	modelInvocationJobs         *store.Table[ModelInvocationJob]             // jobArn → job
	promptRouters               *store.Table[PromptRouter]                   // routerArn → router
	enforcedGuardrailConfigs    *store.Table[AccountEnforcedGuardrailConfig] // configID → config
	arpAnnotations              map[string][]any                             // policyARN+":"+buildWorkflowID → annotations
	// arpAnnotationSetHash is the optimistic-concurrency token
	// UpdateAutomatedReasoningPolicyAnnotations's required
	// lastUpdatedAnnotationSetHash checks against and
	// GetAutomatedReasoningPolicyAnnotations's required annotationSetHash
	// returns (bedrock@v1.66.4 api_op_GetAutomatedReasoningPolicyAnnotations.go:54,
	// api_op_UpdateAutomatedReasoningPolicyAnnotations.go). Deliberately NOT
	// part of backendSnapshot/restoreRawMaps: losing it across a restore only
	// means the next Get lazily mints a fresh opaque token, unlike
	// GuardrailVersionCounters where losing state risks a real key collision.
	arpAnnotationSetHash map[string]string // policyARN+":"+buildWorkflowID → hash
	// arpAnnotationsUpdatedAt backs GetAutomatedReasoningPolicyAnnotationsOutput's
	// required updatedAt (bedrock@v1.66.4
	// api_op_GetAutomatedReasoningPolicyAnnotations.go:80), lazily minted
	// alongside arpAnnotationSetHash on first Get and bumped by Update -- same
	// not-persisted rationale as arpAnnotationSetHash above.
	arpAnnotationsUpdatedAt map[string]time.Time // policyARN+":"+buildWorkflowID → updatedAt
	useCaseFormData         []byte               // raw FormData for PutUseCaseForModelAccess
	// parity-4 additions.
	advancedPromptOptimizationJobs *store.Table[AdvancedPromptOptimizationJob] // jobArn → job
	resourcePolicies               *store.Table[ResourcePolicy]                // resourceArn → policy
	accountDataRetention           *AccountDataRetention
	guardrailsByName               map[string]string // guardrail name → ID
	guardrailsByARN                map[string]string // guardrail ARN → ID
	pmtsByName                     map[string]string // PMT name → ARN
	arpByName                      map[string]string // policy name → ARN
	customModelsByName             map[string]string // model name → ARN
	customModelDeployByName        map[string]string // deployment name → ARN
	evaluationJobsByName           map[string]string // job name → ARN
	customizationJobsByName        map[string]string // job name → ARN
	inferenceProfilesByName        map[string]string // profile name → ARN
	marketplaceEndpointsByName     map[string]string // endpoint name → ARN
	promptRoutersByName            map[string]string // router name → ARN
	registry                       *store.Registry
	mu                             *lockmetrics.RWMutex
	accountID                      string
	region                         string
	foundationModels               []*FoundationModelSummary
	guardrailCounter               int
	guardrailVersionCounter        int
	provisionedCounter             int
	evaluationJobCounter           int
	arpCounter                     int
	arpWorkflowCounter             int
	arpTestCaseCounter             int
	customModelCounter             int
	customModelDeployCounter       int
	customizationJobCounter        int
	copyJobCounter                 int
	importJobCounter               int
	inferenceProfileCounter        int
	marketplaceEndpointCounter     int
	modelInvocationJobCounter      int
	promptRouterCounter            int
	enforcedGuardrailConfigCounter int
	// parity-4 counters.
	advancedPromptOptJobCounter   int
	resourcePolicyRevisionCounter int
	jobDelay                      atomic.Int64
}

// NewInMemoryBackend creates a new InMemoryBackend pre-seeded with foundation models.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		arpVersionCountByPolicy:    make(map[string]int),
		guardrailsByName:           make(map[string]string),
		guardrailsByARN:            make(map[string]string),
		pmtsByName:                 make(map[string]string),
		arpByName:                  make(map[string]string),
		customModelsByName:         make(map[string]string),
		customModelDeployByName:    make(map[string]string),
		evaluationJobsByName:       make(map[string]string),
		customizationJobsByName:    make(map[string]string),
		inferenceProfilesByName:    make(map[string]string),
		marketplaceEndpointsByName: make(map[string]string),
		arpAnnotations:             make(map[string][]any),
		arpAnnotationSetHash:       make(map[string]string),
		arpAnnotationsUpdatedAt:    make(map[string]time.Time),
		promptRoutersByName:        make(map[string]string),
		accountID:                  accountID,
		region:                     region,
		mu:                         lockmetrics.New("bedrock"),
		registry:                   store.NewRegistry(),
	}
	registerAllTables(b)
	b.seedFoundationModels()

	return b
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all state, returning the backend to its initial seeded state.
// The accountID, region, and seeded foundation models are preserved.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.resetIndexMaps()
	b.resetCounters()
	b.resetAuxState()
}

// resetIndexMaps clears the name/ARN lookup indexes used to resolve resources
// by user-supplied identifiers.
func (b *InMemoryBackend) resetIndexMaps() {
	b.guardrailsByName = make(map[string]string)
	b.guardrailsByARN = make(map[string]string)
	b.pmtsByName = make(map[string]string)
	b.arpByName = make(map[string]string)
	b.customModelsByName = make(map[string]string)
	b.customModelDeployByName = make(map[string]string)
	b.evaluationJobsByName = make(map[string]string)
	b.customizationJobsByName = make(map[string]string)
	b.inferenceProfilesByName = make(map[string]string)
	b.marketplaceEndpointsByName = make(map[string]string)
	b.promptRoutersByName = make(map[string]string)
}

// resetCounters clears every monotonic ID counter (and per-parent counter map)
// back to its zero value.
func (b *InMemoryBackend) resetCounters() {
	b.arpVersionCountByPolicy = make(map[string]int)
	b.guardrailCounter = 0
	b.guardrailVersionCounter = 0
	b.provisionedCounter = 0
	b.evaluationJobCounter = 0
	b.arpCounter = 0
	b.arpWorkflowCounter = 0
	b.arpTestCaseCounter = 0
	b.customModelCounter = 0
	b.customModelDeployCounter = 0
	b.customizationJobCounter = 0
	b.copyJobCounter = 0
	b.importJobCounter = 0
	b.inferenceProfileCounter = 0
	b.marketplaceEndpointCounter = 0
	b.modelInvocationJobCounter = 0
	b.promptRouterCounter = 0
	b.enforcedGuardrailConfigCounter = 0
	b.advancedPromptOptJobCounter = 0
	b.resourcePolicyRevisionCounter = 0
}

// resetAuxState clears miscellaneous non-table backend state that is not part
// of the registered table registry.
func (b *InMemoryBackend) resetAuxState() {
	b.loggingConfig = nil
	b.arpAnnotations = make(map[string][]any)
	b.arpAnnotationSetHash = make(map[string]string)
	b.arpAnnotationsUpdatedAt = make(map[string]time.Time)
	b.useCaseFormData = nil
	b.accountDataRetention = nil
}

func (b *InMemoryBackend) seedFoundationModels() {
	// Real AWS foundation model ARNs use region but NOT account ID:
	// arn:{partition}:bedrock:{region}::foundation-model/{modelId}
	prefix := "arn:aws:bedrock:" + b.region + "::foundation-model/"
	active := &FoundationModelLifecycle{Status: lifecycleActive}

	b.foundationModels = []*FoundationModelSummary{
		{
			ModelID:                    "amazon.titan-text-express-v1",
			ModelName:                  "Titan Text G1 - Express",
			ProviderName:               "Amazon",
			ModelArn:                   prefix + "amazon.titan-text-express-v1",
			InputModalities:            []string{textTypeText},
			OutputModalities:           []string{textTypeText},
			InferenceTypesSupported:    []string{inferenceTypeOnDemand, inferenceTypeProvisioned},
			CustomizationsSupported:    []string{customizationTypeFineTuning},
			ResponseStreamingSupported: true,
			ModelLifecycle:             active,
		},
		{
			ModelID:                    "amazon.titan-embed-text-v1",
			ModelName:                  "Titan Embeddings G1 - Text",
			ProviderName:               "Amazon",
			ModelArn:                   prefix + "amazon.titan-embed-text-v1",
			InputModalities:            []string{textTypeText},
			OutputModalities:           []string{"EMBEDDING"},
			InferenceTypesSupported:    []string{inferenceTypeOnDemand},
			CustomizationsSupported:    []string{},
			ResponseStreamingSupported: false,
			ModelLifecycle:             active,
		},
		{
			ModelID:                    "anthropic.claude-v2",
			ModelName:                  "Claude",
			ProviderName:               "Anthropic",
			ModelArn:                   prefix + "anthropic.claude-v2",
			InputModalities:            []string{textTypeText},
			OutputModalities:           []string{textTypeText},
			InferenceTypesSupported:    []string{inferenceTypeOnDemand, inferenceTypeProvisioned},
			CustomizationsSupported:    []string{},
			ResponseStreamingSupported: true,
			ModelLifecycle:             active,
		},
		{
			ModelID:                    "anthropic.claude-3-sonnet-20240229-v1:0",
			ModelName:                  "Claude 3 Sonnet",
			ProviderName:               "Anthropic",
			ModelArn:                   prefix + "anthropic.claude-3-sonnet-20240229-v1:0",
			InputModalities:            []string{textTypeText, "IMAGE"},
			OutputModalities:           []string{textTypeText},
			InferenceTypesSupported:    []string{inferenceTypeOnDemand, inferenceTypeProvisioned},
			CustomizationsSupported:    []string{},
			ResponseStreamingSupported: true,
			ModelLifecycle:             active,
		},
		{
			ModelID:                    "meta.llama3-8b-instruct-v1:0",
			ModelName:                  "Llama 3 8B Instruct",
			ProviderName:               "Meta",
			ModelArn:                   prefix + "meta.llama3-8b-instruct-v1:0",
			InputModalities:            []string{textTypeText},
			OutputModalities:           []string{textTypeText},
			InferenceTypesSupported:    []string{inferenceTypeOnDemand},
			CustomizationsSupported:    []string{customizationTypeFineTuning},
			ResponseStreamingSupported: true,
			ModelLifecycle:             active,
		},
	}
}

// paginate pages list by an integer-offset token; maxResults <= 0 means bedrockDefaultPageSize.
func paginate[T any](list []T, maxResults int, nextToken string) ([]T, string) {
	if maxResults <= 0 {
		maxResults = bedrockDefaultPageSize
	}

	startIdx := page.DecodeToken(nextToken)

	if startIdx >= len(list) {
		return []T{}, ""
	}

	end := startIdx + maxResults
	var outToken string

	if end < len(list) {
		outToken = page.EncodeToken(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}

// queryMaxResults reads the "maxResults" query member; absent or malformed means 0 (default).
func queryMaxResults(q url.Values) int {
	n, err := strconv.Atoi(q.Get("maxResults"))
	if err != nil {
		return 0
	}

	return n
}

func withNextToken(resp map[string]any, token string) map[string]any {
	if token != "" {
		resp["nextToken"] = token
	}

	return resp
}
