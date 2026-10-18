package bedrockagentcorecontrol

import (
	"regexp"
	"slices"
)

const (
	stateReady    = "READY"
	stateActive   = "ACTIVE"
	stateCreating = "CREATING"
	stateUpdating = "UPDATING"
	stateDeleting = "DELETING"
	stateDeleted  = "DELETED"
)

const (
	kindRuntime          = "runtime"
	kindRuntimeEndpoint  = "runtimeEndpoint"
	kindGateway          = "gateway"
	kindGatewayTarget    = "gatewayTarget"
	kindGatewayRule      = "gatewayRule"
	kindGatewayRateLimit = "gatewayRateLimit"
	kindMemory           = keyMemory
	kindCodeInterpreter  = "codeInterpreter"
	kindBrowser          = "browser"
	kindBrowserProfile   = "browserProfile"
	kindCapacityProvider = "capacityProvider"
	kindBundle           = "configurationBundle"
	kindConsentPortal    = "consentPortal"
	kindDataset          = "dataset"
	kindEvaluator        = "evaluator"
	kindHarness          = "harness"
	kindHarnessEndpoint  = "harnessEndpoint"
	kindOnlineEval       = "onlineEvaluationConfig"
	kindPaymentManager   = "paymentManager"
	kindPaymentConnector = "paymentConnector"
	kindPaymentProvider  = "paymentCredentialProvider"
	kindAPIKeyProvider   = "apiKeyCredentialProvider" //nolint:gosec // resource kind name, not a credential
	kindOAuthProvider    = "oauth2CredentialProvider"
	kindPolicyEngine     = "policyEngine"
	kindPolicy           = keyPolicy
	kindPolicyGeneration = "policyGeneration"
	kindRegistry         = "registry"
	kindRegistryRecord   = "registryRecord"
	kindWorkloadIdentity = "workloadIdentity"
)

var (
	namePatternSnake = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]{0,47}$`)
	namePatternDash  = regexp.MustCompile(`^([0-9a-zA-Z][-]?){1,100}$`)
	namePatternWord  = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,99}$`)
)

// kindSpec binds one resource family to its SDK operations and wire member names.
type kindSpec struct {
	namePattern *regexp.Regexp
	Aliases     map[string]string
	Wrap        string
	Ready       string
	ParentLabel string
	IDLabel     string
	IDKey       string
	ARNKey      string
	NameKey     string
	ARNType     string
	Name        string
	Parent      string
	IDMode      string
	ClientID    string
	Create      string
	Update      string
	Delete      string
	VersionKey  string
	Lists       []string
	Gets        []string
	Terminal    bool
}

func kindTable() []*kindSpec {
	return slices.Concat(
		runtimeAndGatewayKinds(),
		memoryAndToolKinds(),
		dataKinds(),
		identityAndPaymentKinds(),
		policyAndRegistryKinds(),
	)
}

func runtimeAndGatewayKinds() []*kindSpec {
	return []*kindSpec{
		{
			Name:        kindRuntime,
			Create:      "CreateAgentRuntime",
			Update:      "UpdateAgentRuntime",
			Delete:      "DeleteAgentRuntime",
			Gets:        []string{"GetAgentRuntime"},
			Lists:       []string{"ListAgentRuntimes"},
			IDKey:       keyAgentRuntimeID,
			IDLabel:     keyAgentRuntimeID,
			ARNKey:      "agentRuntimeArn",
			NameKey:     "agentRuntimeName",
			ARNType:     "runtime",
			Ready:       stateReady,
			IDMode:      idModeName,
			namePattern: namePatternSnake,
			VersionKey:  "agentRuntimeVersion",
		},
		{
			Name:        kindRuntimeEndpoint,
			Parent:      kindRuntime,
			ParentLabel: keyAgentRuntimeID,
			Create:      "CreateAgentRuntimeEndpoint",
			Update:      "UpdateAgentRuntimeEndpoint",
			Delete:      "DeleteAgentRuntimeEndpoint",
			Gets:        []string{"GetAgentRuntimeEndpoint"},
			Lists:       []string{"ListAgentRuntimeEndpoints"},
			IDKey:       keyName,
			IDLabel:     keyEndpointName,
			ARNKey:      "agentRuntimeEndpointArn",
			NameKey:     keyName,
			ARNType:     "runtime/{parent}/runtime-endpoint",
			Ready:       stateReady,
			IDMode:      idModeNameKeyed,
			namePattern: namePatternSnake,
			Aliases:     map[string]string{keyEndpointName: keyName, "id": "endpointId"},
		},
		{
			Name: kindGateway, Create: "CreateGateway", Update: "UpdateGateway", Delete: "DeleteGateway",
			Gets: []string{"GetGateway"}, Lists: []string{"ListGateways"},
			IDKey: "gatewayId", IDLabel: keyGatewayIdentifier, ARNKey: "gatewayArn", NameKey: keyName,
			ARNType: "gateway", Ready: stateReady, IDMode: idModeName, namePattern: namePatternDash,
		},
		{
			Name: kindGatewayTarget, Parent: kindGateway, ParentLabel: keyGatewayIdentifier,
			Create: "CreateGatewayTarget", Update: "UpdateGatewayTarget", Delete: "DeleteGatewayTarget",
			Gets: []string{"GetGatewayTarget"}, Lists: []string{"ListGatewayTargets"},
			IDKey: "targetId", IDLabel: "targetId", NameKey: keyName, ARNType: "target", Ready: stateReady,
			IDMode: idModeToken, namePattern: namePatternDash,
		},
		{
			Name: kindGatewayRule, Parent: kindGateway, ParentLabel: keyGatewayIdentifier,
			Create: "CreateGatewayRule", Update: "UpdateGatewayRule", Delete: "DeleteGatewayRule",
			Gets: []string{"GetGatewayRule"}, Lists: []string{"ListGatewayRules"},
			IDKey: "ruleId", IDLabel: "ruleId", ARNType: "rule", Ready: stateActive, IDMode: idModeToken,
		},
	}
}

func memoryAndToolKinds() []*kindSpec {
	return []*kindSpec{
		{
			Name:        kindGatewayRateLimit,
			Parent:      kindGateway,
			ParentLabel: keyGatewayIdentifier,
			Create:      "CreateGatewayRateLimit",
			Update:      "UpdateGatewayRateLimit",
			Delete:      "DeleteGatewayRateLimit",
			Gets:        []string{"GetGatewayRateLimit"},
			Lists:       []string{"ListGatewayRateLimits"},
			IDKey:       keyRateLimitID,
			IDLabel:     keyRateLimitID,
			ARNType:     "rate-limit",
			Ready:       stateActive,
			IDMode:      idModeToken,
			ClientID:    keyRateLimitID,
		},
		{
			Name: kindMemory, Create: "CreateMemory", Update: "UpdateMemory", Delete: "DeleteMemory",
			Gets: []string{"GetMemory"}, Lists: []string{"ListMemories"},
			IDKey: "id", IDLabel: "memoryId", ARNKey: keyARN, NameKey: keyName, ARNType: keyMemory, Wrap: keyMemory,
			Ready: stateActive, IDMode: idModeName, namePattern: namePatternSnake,
		},
		{
			Name: kindCodeInterpreter, Create: "CreateCodeInterpreter", Delete: "DeleteCodeInterpreter",
			Gets: []string{"GetCodeInterpreter"}, Lists: []string{"ListCodeInterpreters"},
			IDKey: "codeInterpreterId", IDLabel: "codeInterpreterId", ARNKey: "codeInterpreterArn", NameKey: keyName,
			ARNType: "code-interpreter-custom", Ready: stateReady, IDMode: idModeName, namePattern: namePatternSnake,
		},
		{
			Name: kindBrowser, Create: "CreateBrowser", Delete: "DeleteBrowser",
			Gets: []string{"GetBrowser"}, Lists: []string{"ListBrowsers"},
			IDKey: "browserId", IDLabel: "browserId", ARNKey: "browserArn", NameKey: keyName,
			ARNType: "browser-custom", Ready: stateReady, IDMode: idModeName, namePattern: namePatternSnake,
		},
		{
			Name: kindBrowserProfile, Create: "CreateBrowserProfile", Delete: "DeleteBrowserProfile",
			Gets: []string{"GetBrowserProfile"}, Lists: []string{"ListBrowserProfiles"},
			IDKey: "profileId", IDLabel: "profileId", ARNKey: "profileArn", NameKey: keyName,
			ARNType: "browser-profile", Ready: stateReady, IDMode: idModeName, namePattern: namePatternSnake,
		},
		{
			Name:        kindCapacityProvider,
			Create:      "CreateCapacityProvider",
			Update:      "UpdateCapacityProvider",
			Delete:      "DeleteCapacityProvider",
			Gets:        []string{"GetCapacityProvider"},
			Lists:       []string{"ListCapacityProviders"},
			IDKey:       "capacityProviderId",
			IDLabel:     "capacityProviderId",
			ARNKey:      "capacityProviderArn",
			NameKey:     keyName,
			ARNType:     "capacity-provider",
			Ready:       stateReady,
			IDMode:      keyName,
			namePattern: namePatternDash,
		},
		{
			Name: kindBundle, Create: "CreateConfigurationBundle", Update: "UpdateConfigurationBundle",
			Delete: "DeleteConfigurationBundle", Gets: []string{"GetConfigurationBundle"},
			Lists: []string{"ListConfigurationBundles"},
			IDKey: "bundleId", IDLabel: "bundleId", ARNKey: "bundleArn", NameKey: "bundleName",
			ARNType: "configuration-bundle", Ready: stateActive, IDMode: idModeName, namePattern: namePatternWord,
		},
	}
}

func dataKinds() []*kindSpec {
	return []*kindSpec{
		{
			Name: kindConsentPortal, Create: "CreateConsentPortal", Update: "UpdateConsentPortal",
			Delete: "DeleteConsentPortal", Gets: []string{"GetConsentPortal"}, Lists: []string{"ListConsentPortals"},
			IDKey: "consentPortalId", IDLabel: "consentPortalIdentifier", ARNKey: "consentPortalArn", NameKey: keyName,
			ARNType: "consent-portal", Ready: stateActive, IDMode: idModeName, namePattern: namePatternWord,
		},
		{
			Name: kindDataset, Create: "CreateDataset", Update: "UpdateDataset", Delete: "DeleteDataset",
			Gets: []string{"GetDataset"}, Lists: []string{"ListDatasets"},
			IDKey: keyDatasetID, IDLabel: keyDatasetID, ARNKey: "datasetArn", NameKey: "datasetName",
			ARNType: "dataset", Ready: stateActive, IDMode: idModeName, namePattern: namePatternWord,
		},
		{
			Name: kindEvaluator, Create: "CreateEvaluator", Update: "UpdateEvaluator", Delete: "DeleteEvaluator",
			Gets: []string{"GetEvaluator"}, Lists: []string{"ListEvaluators"},
			IDKey: "evaluatorId", IDLabel: "evaluatorId", ARNKey: "evaluatorArn", NameKey: "evaluatorName",
			ARNType: "evaluator", Ready: stateActive, IDMode: idModeName, namePattern: namePatternSnake,
		},
		{
			Name: kindHarness, Create: "CreateHarness", Update: "UpdateHarness", Delete: "DeleteHarness",
			Gets: []string{"GetHarness"}, Lists: []string{"ListHarnesses"},
			IDKey: keyHarnessID, IDLabel: keyHarnessID, ARNKey: keyARN, NameKey: "harnessName", ARNType: "harness",
			Wrap: "harness", Ready: stateReady, IDMode: idModeName, namePattern: namePatternSnake,
			VersionKey: "harnessVersion",
		},
		{
			Name: kindHarnessEndpoint, Parent: kindHarness, ParentLabel: keyHarnessID,
			Create: "CreateHarnessEndpoint", Update: "UpdateHarnessEndpoint", Delete: "DeleteHarnessEndpoint",
			Gets: []string{"GetHarnessEndpoint"}, Lists: []string{"ListHarnessEndpoints"},
			IDKey: keyEndpointName, IDLabel: keyEndpointName, ARNKey: keyARN, NameKey: keyEndpointName,
			ARNType: "harness/{parent}/endpoint", Wrap: "endpoint", Ready: stateReady, IDMode: idModeNameKeyed,
			namePattern: namePatternSnake,
		},
		{
			Name: kindOnlineEval, Create: "CreateOnlineEvaluationConfig", Update: "UpdateOnlineEvaluationConfig",
			Delete: "DeleteOnlineEvaluationConfig", Gets: []string{"GetOnlineEvaluationConfig"},
			Lists: []string{"ListOnlineEvaluationConfigs"},
			IDKey: "onlineEvaluationConfigId", IDLabel: "onlineEvaluationConfigId", ARNKey: "onlineEvaluationConfigArn",
			NameKey: "onlineEvaluationConfigName", ARNType: "online-evaluation-config", Ready: stateActive,
			IDMode: idModeName, namePattern: namePatternSnake,
		},
		{
			Name: kindPaymentManager, Create: "CreatePaymentManager", Update: "UpdatePaymentManager",
			Delete: "DeletePaymentManager", Gets: []string{"GetPaymentManager"}, Lists: []string{"ListPaymentManagers"},
			IDKey: keyPaymentManagerID, IDLabel: keyPaymentManagerID, ARNKey: "paymentManagerArn", NameKey: keyName,
			ARNType: "payment-manager", Ready: stateReady, IDMode: idModeName, namePattern: namePatternDash,
		},
	}
}

func identityAndPaymentKinds() []*kindSpec {
	return []*kindSpec{
		{
			Name: kindPaymentConnector, Parent: kindPaymentManager, ParentLabel: keyPaymentManagerID,
			Create: "CreatePaymentConnector", Update: "UpdatePaymentConnector", Delete: "DeletePaymentConnector",
			Gets: []string{"GetPaymentConnector"}, Lists: []string{"ListPaymentConnectors"},
			IDKey: "paymentConnectorId", IDLabel: "paymentConnectorId", NameKey: keyName,
			ARNType: "payment-connector", Ready: stateReady, IDMode: idModeName, namePattern: namePatternDash,
		},
		{
			Name:        kindPaymentProvider,
			Create:      "CreatePaymentCredentialProvider",
			Update:      "UpdatePaymentCredentialProvider",
			Delete:      "DeletePaymentCredentialProvider",
			Gets:        []string{"GetPaymentCredentialProvider"},
			Lists:       []string{"ListPaymentCredentialProviders"},
			IDKey:       keyName,
			IDLabel:     keyName,
			ARNKey:      keyCredProviderARN,
			NameKey:     keyName,
			ARNType:     "token-vault/default/paymentcredentialprovider",
			IDMode:      idModeNameKeyed,
			Terminal:    true,
			namePattern: namePatternWord,
		},
		{
			Name:        kindAPIKeyProvider,
			Create:      "CreateApiKeyCredentialProvider",
			Update:      "UpdateApiKeyCredentialProvider",
			Delete:      "DeleteApiKeyCredentialProvider",
			Gets:        []string{"GetApiKeyCredentialProvider"},
			Lists:       []string{"ListApiKeyCredentialProviders"},
			IDKey:       keyName,
			IDLabel:     keyName,
			ARNKey:      keyCredProviderARN,
			NameKey:     keyName,
			ARNType:     "token-vault/default/apikeycredentialprovider",
			IDMode:      idModeNameKeyed,
			Terminal:    true,
			namePattern: namePatternWord,
		},
		{
			Name: kindOAuthProvider, Create: "CreateOauth2CredentialProvider", Update: "UpdateOauth2CredentialProvider",
			Delete: "DeleteOauth2CredentialProvider", Gets: []string{"GetOauth2CredentialProvider"},
			Lists: []string{"ListOauth2CredentialProviders"},
			IDKey: keyName, IDLabel: keyName, ARNKey: keyCredProviderARN, NameKey: keyName,
			ARNType: "token-vault/default/oauth2credentialprovider", Ready: stateReady, IDMode: idModeNameKeyed,
			namePattern: namePatternWord,
		},
		{
			Name:        kindPolicyEngine,
			Create:      "CreatePolicyEngine",
			Update:      "UpdatePolicyEngine",
			Delete:      "DeletePolicyEngine",
			Gets:        []string{"GetPolicyEngine", "GetPolicyEngineSummary"},
			Lists:       []string{"ListPolicyEngines", "ListPolicyEngineSummaries"},
			IDKey:       keyPolicyEngineID,
			IDLabel:     keyPolicyEngineID,
			ARNKey:      "policyEngineArn",
			NameKey:     keyName,
			ARNType:     "policy-engine",
			Ready:       stateActive,
			IDMode:      keyName,
			namePattern: namePatternSnake,
		},
	}
}

func policyAndRegistryKinds() []*kindSpec {
	return []*kindSpec{
		{
			Name:        kindPolicy,
			Parent:      kindPolicyEngine,
			ParentLabel: keyPolicyEngineID,
			Create:      "CreatePolicy",
			Update:      "UpdatePolicy",
			Delete:      "DeletePolicy",
			Gets:        []string{"GetPolicy", "GetPolicySummary"},
			Lists:       []string{"ListPolicies", "ListPolicySummaries"},
			IDKey:       "policyId",
			IDLabel:     "policyId",
			ARNKey:      "policyArn",
			NameKey:     keyName,
			ARNType:     "policy-engine/{parent}/policy",
			Ready:       stateActive,
			IDMode:      idModeName,
			namePattern: namePatternSnake,
		},
		{
			Name: kindPolicyGeneration, Parent: kindPolicyEngine, ParentLabel: keyPolicyEngineID,
			Create: "StartPolicyGeneration", Gets: []string{"GetPolicyGeneration", "GetPolicyGenerationSummary"},
			Lists: []string{"ListPolicyGenerations", "ListPolicyGenerationSummaries"},
			IDKey: "policyGenerationId", IDLabel: "policyGenerationId", ARNKey: "policyGenerationArn", NameKey: keyName,
			ARNType: "policy-engine/{parent}/policy-generation", Ready: "GENERATED", IDMode: idModeName,
			namePattern: namePatternSnake,
		},
		{
			Name: kindRegistry, Create: "CreateRegistry", Update: "UpdateRegistry", Delete: "DeleteRegistry",
			Gets: []string{"GetRegistry"}, Lists: []string{"ListRegistries"},
			IDKey: keyRegistryID, IDLabel: keyRegistryID, ARNKey: "registryArn", NameKey: keyName, ARNType: "registry",
			Ready: stateReady, IDMode: idModeToken12, namePattern: namePatternWord,
		},
		{
			Name: kindRegistryRecord, Parent: kindRegistry, ParentLabel: keyRegistryID,
			Create: "CreateRegistryRecord", Update: "UpdateRegistryRecord", Delete: "DeleteRegistryRecord",
			Gets: []string{"GetRegistryRecord"}, Lists: []string{"ListRegistryRecords"},
			IDKey: "recordId", IDLabel: "recordId", ARNKey: "recordArn", NameKey: keyName,
			ARNType: "registry/{parent}/record", Ready: "DRAFT", IDMode: idModeToken12, namePattern: namePatternWord,
		},
		{
			Name:        kindWorkloadIdentity,
			Create:      "CreateWorkloadIdentity",
			Update:      "UpdateWorkloadIdentity",
			Delete:      "DeleteWorkloadIdentity",
			Gets:        []string{"GetWorkloadIdentity"},
			Lists:       []string{"ListWorkloadIdentities"},
			IDKey:       keyName,
			IDLabel:     keyName,
			ARNKey:      "workloadIdentityArn",
			NameKey:     keyName,
			ARNType:     "workload-identity-directory/default/workload-identity",
			IDMode:      idModeNameKeyed,
			Terminal:    true,
			namePattern: namePatternWord,
		},
	}
}
