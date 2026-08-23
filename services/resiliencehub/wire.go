package resiliencehub

// Wire request/response shapes for every one of the 63 operations. JSON
// members are lowerCamel throughout (confirmed by reading the SDK's own
// serializers.go/deserializers.go key literals, e.g.
// awsRestjson1_serializeOpDocumentCreateAppInput's "assessmentSchedule",
// "awsApplicationArn", ... -- this service's convention matches
// services/grafana's lowerCamel, NOT services/outposts' PascalCase). Every
// timestamp is epoch-seconds (smithytime.ParseEpochSeconds/FormatEpochSeconds
// in the SDK), never an ISO8601 string -- see pkgs/awstime.Epoch.

// --- shared building blocks -------------------------------------------------

type eventSubscriptionWire struct {
	EventType   string `json:"eventType,omitempty"`
	Name        string `json:"name,omitempty"`
	SnsTopicArn string `json:"snsTopicArn,omitempty"`
}

type permissionModelWire struct {
	Type                 string   `json:"type,omitempty"`
	InvokerRoleName      string   `json:"invokerRoleName,omitempty"`
	CrossAccountRoleArns []string `json:"crossAccountRoleArns,omitempty"`
}

type failurePolicyWire struct {
	RpoInSecs int32 `json:"rpoInSecs"`
	RtoInSecs int32 `json:"rtoInSecs"`
}

type logicalResourceIDWire struct {
	Identifier          string `json:"identifier,omitempty"`
	EksSourceName       string `json:"eksSourceName,omitempty"`
	LogicalStackName    string `json:"logicalStackName,omitempty"`
	ResourceGroupName   string `json:"resourceGroupName,omitempty"`
	TerraformSourceName string `json:"terraformSourceName,omitempty"`
}

type physicalResourceIDWire struct {
	Identifier   string `json:"identifier"`
	Type         string `json:"type"`
	AwsAccountID string `json:"awsAccountId,omitempty"`
	AwsRegion    string `json:"awsRegion,omitempty"`
}

type appComponentWire struct {
	AdditionalInfo map[string][]string `json:"additionalInfo,omitempty"`
	Name           string              `json:"name"`
	Type           string              `json:"type"`
	ID             string              `json:"id,omitempty"`
}

type physicalResourceWire struct {
	LogicalResourceID  *logicalResourceIDWire  `json:"logicalResourceId,omitempty"`
	PhysicalResourceID *physicalResourceIDWire `json:"physicalResourceId,omitempty"`
	AdditionalInfo     map[string][]string     `json:"additionalInfo,omitempty"`
	Excluded           *bool                   `json:"excluded,omitempty"`
	ResourceType       string                  `json:"resourceType"`
	ParentResourceName string                  `json:"parentResourceName,omitempty"`
	ResourceName       string                  `json:"resourceName,omitempty"`
	SourceType         string                  `json:"sourceType,omitempty"`
	AppComponents      []appComponentWire      `json:"appComponents,omitempty"`
}

type resourceMappingWire struct {
	PhysicalResourceID  *physicalResourceIDWire `json:"physicalResourceId,omitempty"`
	MappingType         string                  `json:"mappingType"`
	AppRegistryAppName  string                  `json:"appRegistryAppName,omitempty"`
	EksSourceName       string                  `json:"eksSourceName,omitempty"`
	LogicalStackName    string                  `json:"logicalStackName,omitempty"`
	ResourceGroupName   string                  `json:"resourceGroupName,omitempty"`
	ResourceName        string                  `json:"resourceName,omitempty"`
	TerraformSourceName string                  `json:"terraformSourceName,omitempty"`
}

type eksSourceClusterNamespaceWire struct {
	EksClusterArn string `json:"eksClusterArn,omitempty"`
	Namespace     string `json:"namespace,omitempty"`
}

type eksSourceWire struct {
	EksClusterArn string   `json:"eksClusterArn,omitempty"`
	Namespaces    []string `json:"namespaces,omitempty"`
}

type terraformSourceWire struct {
	S3StateFileURL string `json:"s3StateFileUrl,omitempty"`
}

type appInputSourceWire struct {
	EksSourceClusterNamespace *eksSourceClusterNamespaceWire `json:"eksSourceClusterNamespace,omitempty"`
	TerraformSource           *terraformSourceWire           `json:"terraformSource,omitempty"`
	ImportType                string                         `json:"importType,omitempty"`
	SourceArn                 string                         `json:"sourceArn,omitempty"`
	SourceName                string                         `json:"sourceName,omitempty"`
	ResourceCount             int32                          `json:"resourceCount,omitempty"`
}

type disruptionComplianceWire struct {
	ComplianceStatus    string `json:"complianceStatus,omitempty"`
	Message             string `json:"message,omitempty"`
	RpoDescription      string `json:"rpoDescription,omitempty"`
	RpoReferenceID      string `json:"rpoReferenceId,omitempty"`
	RtoDescription      string `json:"rtoDescription,omitempty"`
	RtoReferenceID      string `json:"rtoReferenceId,omitempty"`
	AchievableRpoInSecs int32  `json:"achievableRpoInSecs,omitempty"`
	AchievableRtoInSecs int32  `json:"achievableRtoInSecs,omitempty"`
	CurrentRpoInSecs    int32  `json:"currentRpoInSecs,omitempty"`
	CurrentRtoInSecs    int32  `json:"currentRtoInSecs,omitempty"`
}

type scoringComponentResiliencyScoreWire struct {
	ExcludedCount    int64   `json:"excludedCount,omitempty"`
	OutstandingCount int64   `json:"outstandingCount,omitempty"`
	PossibleScore    float64 `json:"possibleScore,omitempty"`
	Score            float64 `json:"score,omitempty"`
}

type resiliencyScoreWire struct {
	DisruptionScore map[string]float64                             `json:"disruptionScore,omitempty"`
	ComponentScore  map[string]scoringComponentResiliencyScoreWire `json:"componentScore,omitempty"`
	Score           float64                                        `json:"score"`
}

type resourceErrorWire struct {
	LogicalResourceID  string `json:"logicalResourceId,omitempty"`
	PhysicalResourceID string `json:"physicalResourceId,omitempty"`
	Reason             string `json:"reason,omitempty"`
}

type resourceErrorsDetailsWire struct {
	HasMoreErrors  *bool               `json:"hasMoreErrors,omitempty"`
	ResourceErrors []resourceErrorWire `json:"resourceErrors,omitempty"`
}

type s3LocationWire struct {
	Bucket string `json:"bucket,omitempty"`
	Prefix string `json:"prefix,omitempty"`
}

// --- App -------------------------------------------------------------------

type appWire struct {
	CreationTime                      *float64                `json:"creationTime,omitempty"`
	LastAppComplianceEvaluationTime   *float64                `json:"lastAppComplianceEvaluationTime,omitempty"`
	LastDriftEvaluationTime           *float64                `json:"lastDriftEvaluationTime,omitempty"`
	LastResiliencyScoreEvaluationTime *float64                `json:"lastResiliencyScoreEvaluationTime,omitempty"`
	PermissionModel                   *permissionModelWire    `json:"permissionModel,omitempty"`
	RpoInSecs                         *int32                  `json:"rpoInSecs,omitempty"`
	RtoInSecs                         *int32                  `json:"rtoInSecs,omitempty"`
	Tags                              map[string]string       `json:"tags,omitempty"`
	AppArn                            string                  `json:"appArn"`
	Name                              string                  `json:"name"`
	AssessmentSchedule                string                  `json:"assessmentSchedule,omitempty"`
	AwsApplicationArn                 string                  `json:"awsApplicationArn,omitempty"`
	ComplianceStatus                  string                  `json:"complianceStatus,omitempty"`
	Description                       string                  `json:"description,omitempty"`
	DriftStatus                       string                  `json:"driftStatus,omitempty"`
	PolicyArn                         string                  `json:"policyArn,omitempty"`
	Status                            string                  `json:"status,omitempty"`
	EventSubscriptions                []eventSubscriptionWire `json:"eventSubscriptions,omitempty"`
	ResiliencyScore                   float64                 `json:"resiliencyScore"`
}

type appSummaryWire struct {
	CreationTime                    *float64 `json:"creationTime,omitempty"`
	LastAppComplianceEvaluationTime *float64 `json:"lastAppComplianceEvaluationTime,omitempty"`
	RpoInSecs                       *int32   `json:"rpoInSecs,omitempty"`
	RtoInSecs                       *int32   `json:"rtoInSecs,omitempty"`
	AppArn                          string   `json:"appArn"`
	Name                            string   `json:"name"`
	AssessmentSchedule              string   `json:"assessmentSchedule,omitempty"`
	AwsApplicationArn               string   `json:"awsApplicationArn,omitempty"`
	ComplianceStatus                string   `json:"complianceStatus,omitempty"`
	Description                     string   `json:"description,omitempty"`
	DriftStatus                     string   `json:"driftStatus,omitempty"`
	Status                          string   `json:"status,omitempty"`
	ResiliencyScore                 float64  `json:"resiliencyScore"`
}

type createAppRequest struct {
	PermissionModel    *permissionModelWire    `json:"permissionModel,omitempty"`
	Tags               map[string]string       `json:"tags,omitempty"`
	Name               string                  `json:"name"`
	AssessmentSchedule string                  `json:"assessmentSchedule,omitempty"`
	AwsApplicationArn  string                  `json:"awsApplicationArn,omitempty"`
	ClientToken        string                  `json:"clientToken,omitempty"`
	Description        string                  `json:"description,omitempty"`
	PolicyArn          string                  `json:"policyArn,omitempty"`
	EventSubscriptions []eventSubscriptionWire `json:"eventSubscriptions,omitempty"`
}

type updateAppRequest struct {
	PermissionModel          *permissionModelWire    `json:"permissionModel,omitempty"`
	ClearResiliencyPolicyArn *bool                   `json:"clearResiliencyPolicyArn,omitempty"`
	AssessmentSchedule       string                  `json:"assessmentSchedule,omitempty"`
	Description              string                  `json:"description,omitempty"`
	PolicyArn                string                  `json:"policyArn,omitempty"`
	EventSubscriptions       []eventSubscriptionWire `json:"eventSubscriptions,omitempty"`
}

type deleteAppRequest struct {
	ForceDelete *bool `json:"forceDelete,omitempty"`
}

type appEnvelope struct {
	App *appWire `json:"app"`
}

type appArnRequest struct {
	AppArn string `json:"appArn"`
}

type listAppsResponse struct {
	NextToken    string           `json:"nextToken,omitempty"`
	AppSummaries []appSummaryWire `json:"appSummaries"`
}

// --- AppVersion --------------------------------------------------------------

type appVersionSummaryWire struct {
	CreationTime *float64 `json:"creationTime,omitempty"`
	Identifier   *int64   `json:"identifier,omitempty"`
	AppVersion   string   `json:"appVersion"`
	VersionName  string   `json:"versionName,omitempty"`
}

type listAppVersionsResponse struct {
	NextToken   string                  `json:"nextToken,omitempty"`
	AppVersions []appVersionSummaryWire `json:"appVersions"`
}

type describeAppVersionResponse struct {
	AdditionalInfo map[string][]string `json:"additionalInfo,omitempty"`
	AppArn         string              `json:"appArn"`
	AppVersion     string              `json:"appVersion"`
}

type updateAppVersionRequest struct {
	AdditionalInfo map[string][]string `json:"additionalInfo,omitempty"`
}

type publishAppVersionRequest struct {
	VersionName string `json:"versionName,omitempty"`
}

type publishAppVersionResponse struct {
	Identifier  *int64 `json:"identifier,omitempty"`
	AppArn      string `json:"appArn"`
	AppVersion  string `json:"appVersion"`
	VersionName string `json:"versionName,omitempty"`
}

type describeAppVersionTemplateResponse struct {
	AppArn          string `json:"appArn"`
	AppTemplateBody string `json:"appTemplateBody"`
	AppVersion      string `json:"appVersion"`
}

type putDraftAppVersionTemplateRequest struct {
	AppTemplateBody string `json:"appTemplateBody"`
}

type appArnVersionResponse struct {
	AppArn     string `json:"appArn"`
	AppVersion string `json:"appVersion"`
}

// --- AppComponent ------------------------------------------------------------

type createAppVersionAppComponentRequest struct {
	AdditionalInfo map[string][]string `json:"additionalInfo,omitempty"`
	Name           string              `json:"name"`
	Type           string              `json:"type"`
	ClientToken    string              `json:"clientToken,omitempty"`
	ID             string              `json:"id,omitempty"`
}

type updateAppVersionAppComponentRequest struct {
	AdditionalInfo map[string][]string `json:"additionalInfo,omitempty"`
	Name           string              `json:"name,omitempty"`
	Type           string              `json:"type,omitempty"`
}

type appComponentEnvelope struct {
	AppComponent *appComponentWire `json:"appComponent,omitempty"`
	AppArn       string            `json:"appArn"`
	AppVersion   string            `json:"appVersion,omitempty"`
}

type deleteAppVersionAppComponentRequest struct {
	ID string `json:"id"`
}

type listAppVersionAppComponentsResponse struct {
	NextToken     string             `json:"nextToken,omitempty"`
	AppArn        string             `json:"appArn"`
	AppVersion    string             `json:"appVersion"`
	AppComponents []appComponentWire `json:"appComponents"`
}

// --- AppVersion resources ------------------------------------------------------

type createAppVersionResourceRequest struct {
	LogicalResourceID  *logicalResourceIDWire `json:"logicalResourceId,omitempty"`
	AdditionalInfo     map[string][]string    `json:"additionalInfo,omitempty"`
	PhysicalResourceID string                 `json:"physicalResourceId"`
	ResourceType       string                 `json:"resourceType"`
	AwsAccountID       string                 `json:"awsAccountId,omitempty"`
	AwsRegion          string                 `json:"awsRegion,omitempty"`
	ClientToken        string                 `json:"clientToken,omitempty"`
	ResourceName       string                 `json:"resourceName,omitempty"`
	AppComponents      []string               `json:"appComponents"`
}

type updateAppVersionResourceRequest struct {
	LogicalResourceID  *logicalResourceIDWire `json:"logicalResourceId,omitempty"`
	AdditionalInfo     map[string][]string    `json:"additionalInfo,omitempty"`
	Excluded           *bool                  `json:"excluded,omitempty"`
	PhysicalResourceID string                 `json:"physicalResourceId,omitempty"`
	ResourceType       string                 `json:"resourceType,omitempty"`
	AwsAccountID       string                 `json:"awsAccountId,omitempty"`
	AwsRegion          string                 `json:"awsRegion,omitempty"`
	ResourceName       string                 `json:"resourceName,omitempty"`
	AppComponents      []string               `json:"appComponents,omitempty"`
}

type deleteAppVersionResourceRequest struct {
	LogicalResourceID  *logicalResourceIDWire `json:"logicalResourceId,omitempty"`
	PhysicalResourceID string                 `json:"physicalResourceId,omitempty"`
	AwsAccountID       string                 `json:"awsAccountId,omitempty"`
	AwsRegion          string                 `json:"awsRegion,omitempty"`
	ResourceName       string                 `json:"resourceName,omitempty"`
}

type describeAppVersionResourceRequest struct {
	LogicalResourceID  *logicalResourceIDWire `json:"logicalResourceId,omitempty"`
	AppVersion         string                 `json:"appVersion,omitempty"`
	PhysicalResourceID string                 `json:"physicalResourceId,omitempty"`
	AwsAccountID       string                 `json:"awsAccountId,omitempty"`
	AwsRegion          string                 `json:"awsRegion,omitempty"`
	ResourceName       string                 `json:"resourceName,omitempty"`
}

type physicalResourceEnvelope struct {
	PhysicalResource *physicalResourceWire `json:"physicalResource,omitempty"`
	AppArn           string                `json:"appArn"`
	AppVersion       string                `json:"appVersion,omitempty"`
}

type listAppVersionResourcesResponse struct {
	// ResolutionID is required on the real ListAppVersionResourcesOutput
	// (resiliencehub@v1.38.3 api_op_ListAppVersionResources.go:67-70) even
	// when the app version has never been resolved -- emitted present but
	// empty in that case, never omitted.
	ResolutionID      string                 `json:"resolutionId"`
	NextToken         string                 `json:"nextToken,omitempty"`
	PhysicalResources []physicalResourceWire `json:"physicalResources"`
}

type unsupportedResourceWire struct {
	LogicalResourceID         *logicalResourceIDWire  `json:"logicalResourceId,omitempty"`
	PhysicalResourceID        *physicalResourceIDWire `json:"physicalResourceId,omitempty"`
	ResourceType              string                  `json:"resourceType"`
	UnsupportedResourceStatus string                  `json:"unsupportedResourceStatus,omitempty"`
}

type listUnsupportedAppVersionResourcesResponse struct {
	// ResolutionID is required on the real ListUnsupportedAppVersionResourcesOutput
	// (resiliencehub@v1.38.3 api_op_ListUnsupportedAppVersionResources.go)
	// even when the app version has never been resolved -- emitted present
	// but empty in that case, never omitted.
	ResolutionID         string                    `json:"resolutionId"`
	NextToken            string                    `json:"nextToken,omitempty"`
	UnsupportedResources []unsupportedResourceWire `json:"unsupportedResources"`
}

// --- Resource mappings -------------------------------------------------------

type addDraftAppVersionResourceMappingsRequest struct {
	ResourceMappings []resourceMappingWire `json:"resourceMappings"`
}

type resourceMappingsEnvelope struct {
	AppArn           string                `json:"appArn"`
	AppVersion       string                `json:"appVersion,omitempty"`
	ResourceMappings []resourceMappingWire `json:"resourceMappings"`
}

type removeDraftAppVersionResourceMappingsRequest struct {
	AppRegistryAppNames  []string `json:"appRegistryAppNames,omitempty"`
	EksSourceNames       []string `json:"eksSourceNames,omitempty"`
	LogicalStackNames    []string `json:"logicalStackNames,omitempty"`
	ResourceGroupNames   []string `json:"resourceGroupNames,omitempty"`
	ResourceNames        []string `json:"resourceNames,omitempty"`
	TerraformSourceNames []string `json:"terraformSourceNames,omitempty"`
}

type listAppVersionResourceMappingsResponse struct {
	NextToken        string                `json:"nextToken,omitempty"`
	ResourceMappings []resourceMappingWire `json:"resourceMappings"`
}

// --- Resource resolution / import --------------------------------------------

type resolveAppVersionResourcesResponse struct {
	AppArn       string `json:"appArn"`
	AppVersion   string `json:"appVersion"`
	ResolutionID string `json:"resolutionId"`
	Status       string `json:"status"`
}

type describeAppVersionResourcesResolutionStatusResponse struct {
	AppArn       string `json:"appArn"`
	AppVersion   string `json:"appVersion"`
	ErrorMessage string `json:"errorMessage,omitempty"`
	ResolutionID string `json:"resolutionId"`
	Status       string `json:"status"`
}

type importResourcesToDraftAppVersionRequest struct {
	ImportStrategy   string                `json:"importStrategy,omitempty"`
	EksSources       []eksSourceWire       `json:"eksSources,omitempty"`
	TerraformSources []terraformSourceWire `json:"terraformSources,omitempty"`
	SourceArns       []string              `json:"sourceArns,omitempty"`
}

type importResourcesToDraftAppVersionResponse struct {
	AppArn           string                `json:"appArn"`
	AppVersion       string                `json:"appVersion"`
	Status           string                `json:"status"`
	EksSources       []eksSourceWire       `json:"eksSources,omitempty"`
	TerraformSources []terraformSourceWire `json:"terraformSources,omitempty"`
	SourceArns       []string              `json:"sourceArns,omitempty"`
}

type describeDraftAppVersionResourcesImportStatusResponse struct {
	StatusChangeTime *float64 `json:"statusChangeTime,omitempty"`
	ErrorMessage     string   `json:"errorMessage,omitempty"`
	AppArn           string   `json:"appArn"`
	AppVersion       string   `json:"appVersion"`
	Status           string   `json:"status"`
	ErrorDetails     []struct {
		ErrorMessage string `json:"errorMessage,omitempty"`
	} `json:"errorDetails,omitempty"`
}

type deleteAppInputSourceRequest struct {
	EksSourceClusterNamespace *eksSourceClusterNamespaceWire `json:"eksSourceClusterNamespace,omitempty"`
	TerraformSource           *terraformSourceWire           `json:"terraformSource,omitempty"`
	SourceArn                 string                         `json:"sourceArn,omitempty"`
}

type appInputSourceEnvelope struct {
	AppInputSource *appInputSourceWire `json:"appInputSource,omitempty"`
	AppArn         string              `json:"appArn"`
}

type listAppInputSourcesResponse struct {
	NextToken       string               `json:"nextToken,omitempty"`
	AppInputSources []appInputSourceWire `json:"appInputSources"`
}

// --- ResiliencyPolicy ----------------------------------------------------------

type resiliencyPolicyWire struct {
	CreationTime           *float64                     `json:"creationTime,omitempty"`
	Policy                 map[string]failurePolicyWire `json:"policy"`
	Tags                   map[string]string            `json:"tags,omitempty"`
	PolicyArn              string                       `json:"policyArn"`
	PolicyName             string                       `json:"policyName"`
	Tier                   string                       `json:"tier"`
	DataLocationConstraint string                       `json:"dataLocationConstraint,omitempty"`
	EstimatedCostTier      string                       `json:"estimatedCostTier,omitempty"`
	PolicyDescription      string                       `json:"policyDescription,omitempty"`
}

type createResiliencyPolicyRequest struct {
	Policy                 map[string]failurePolicyWire `json:"policy"`
	Tags                   map[string]string            `json:"tags,omitempty"`
	PolicyName             string                       `json:"policyName"`
	Tier                   string                       `json:"tier"`
	ClientToken            string                       `json:"clientToken,omitempty"`
	DataLocationConstraint string                       `json:"dataLocationConstraint,omitempty"`
	PolicyDescription      string                       `json:"policyDescription,omitempty"`
}

type updateResiliencyPolicyRequest struct {
	Policy                 map[string]failurePolicyWire `json:"policy,omitempty"`
	DataLocationConstraint string                       `json:"dataLocationConstraint,omitempty"`
	PolicyDescription      string                       `json:"policyDescription,omitempty"`
	PolicyName             string                       `json:"policyName,omitempty"`
	Tier                   string                       `json:"tier,omitempty"`
}

type policyEnvelope struct {
	Policy *resiliencyPolicyWire `json:"policy"`
}

type policyArnRequest struct {
	PolicyArn string `json:"policyArn"`
}

type listResiliencyPoliciesResponse struct {
	NextToken          string                 `json:"nextToken,omitempty"`
	ResiliencyPolicies []resiliencyPolicyWire `json:"resiliencyPolicies"`
}

// --- AppAssessment -------------------------------------------------------------

type costWire struct {
	Currency  string  `json:"currency,omitempty"`
	Frequency string  `json:"frequency,omitempty"`
	Amount    float64 `json:"amount,omitempty"`
}

type assessmentRiskRecommendationWire struct {
	Recommendation string   `json:"recommendation,omitempty"`
	Risk           string   `json:"risk,omitempty"`
	AppComponents  []string `json:"appComponents,omitempty"`
}

type assessmentSummaryWire struct {
	Summary             string                             `json:"summary,omitempty"`
	RiskRecommendations []assessmentRiskRecommendationWire `json:"riskRecommendations,omitempty"`
}

type appAssessmentWire struct {
	StartTime             *float64                            `json:"startTime,omitempty"`
	EndTime               *float64                            `json:"endTime,omitempty"`
	Compliance            map[string]disruptionComplianceWire `json:"compliance,omitempty"`
	Policy                *resiliencyPolicyWire               `json:"policy,omitempty"`
	Cost                  *costWire                           `json:"cost,omitempty"`
	ResiliencyScore       *resiliencyScoreWire                `json:"resiliencyScore,omitempty"`
	ResourceErrorsDetails *resourceErrorsDetailsWire          `json:"resourceErrorsDetails,omitempty"`
	Summary               *assessmentSummaryWire              `json:"summary,omitempty"`
	Tags                  map[string]string                   `json:"tags,omitempty"`
	AssessmentArn         string                              `json:"assessmentArn"`
	AssessmentStatus      string                              `json:"assessmentStatus"`
	Invoker               string                              `json:"invoker"`
	AppArn                string                              `json:"appArn,omitempty"`
	AppVersion            string                              `json:"appVersion,omitempty"`
	AssessmentName        string                              `json:"assessmentName,omitempty"`
	ComplianceStatus      string                              `json:"complianceStatus,omitempty"`
	DriftStatus           string                              `json:"driftStatus,omitempty"`
	Message               string                              `json:"message,omitempty"`
	VersionName           string                              `json:"versionName,omitempty"`
}

type appAssessmentSummaryWire struct {
	StartTime        *float64  `json:"startTime,omitempty"`
	EndTime          *float64  `json:"endTime,omitempty"`
	Cost             *costWire `json:"cost,omitempty"`
	AssessmentArn    string    `json:"assessmentArn"`
	AssessmentStatus string    `json:"assessmentStatus"`
	Invoker          string    `json:"invoker,omitempty"`
	AppArn           string    `json:"appArn,omitempty"`
	AppVersion       string    `json:"appVersion,omitempty"`
	AssessmentName   string    `json:"assessmentName,omitempty"`
	ComplianceStatus string    `json:"complianceStatus,omitempty"`
	DriftStatus      string    `json:"driftStatus,omitempty"`
	Message          string    `json:"message,omitempty"`
	VersionName      string    `json:"versionName,omitempty"`
	ResiliencyScore  float64   `json:"resiliencyScore,omitempty"`
}

type startAppAssessmentRequest struct {
	Tags           map[string]string `json:"tags,omitempty"`
	AppArn         string            `json:"appArn"`
	AppVersion     string            `json:"appVersion"`
	AssessmentName string            `json:"assessmentName"`
	ClientToken    string            `json:"clientToken,omitempty"`
}

type assessmentEnvelope struct {
	Assessment *appAssessmentWire `json:"assessment"`
}

type assessmentArnRequest struct {
	AssessmentArn string `json:"assessmentArn"`
}

type deleteAppAssessmentResponse struct {
	AssessmentArn    string `json:"assessmentArn"`
	AssessmentStatus string `json:"assessmentStatus"`
}

type listAppAssessmentsResponse struct {
	NextToken           string                     `json:"nextToken,omitempty"`
	AssessmentSummaries []appAssessmentSummaryWire `json:"assessmentSummaries"`
}

type complianceDriftWire struct {
	ActualValue         map[string]disruptionComplianceWire `json:"actualValue,omitempty"`
	ExpectedValue       map[string]disruptionComplianceWire `json:"expectedValue,omitempty"`
	ActualReferenceID   string                              `json:"actualReferenceId,omitempty"`
	AppID               string                              `json:"appId,omitempty"`
	AppVersion          string                              `json:"appVersion,omitempty"`
	DiffType            string                              `json:"diffType,omitempty"`
	DriftType           string                              `json:"driftType,omitempty"`
	EntityID            string                              `json:"entityId,omitempty"`
	EntityType          string                              `json:"entityType,omitempty"`
	ExpectedReferenceID string                              `json:"expectedReferenceId,omitempty"`
}

type listAppAssessmentComplianceDriftsResponse struct {
	NextToken        string                `json:"nextToken,omitempty"`
	ComplianceDrifts []complianceDriftWire `json:"complianceDrifts"`
}

type resourceIdentifierWire struct {
	LogicalResourceID *logicalResourceIDWire `json:"logicalResourceId,omitempty"`
	ResourceType      string                 `json:"resourceType,omitempty"`
}

type resourceDriftWire struct {
	ResourceIdentifier *resourceIdentifierWire `json:"resourceIdentifier,omitempty"`
	AppArn             string                  `json:"appArn,omitempty"`
	AppVersion         string                  `json:"appVersion,omitempty"`
	DiffType           string                  `json:"diffType,omitempty"`
	ReferenceID        string                  `json:"referenceId,omitempty"`
}

type listAppAssessmentResourceDriftsResponse struct {
	NextToken      string              `json:"nextToken,omitempty"`
	ResourceDrifts []resourceDriftWire `json:"resourceDrifts"`
}

type appComponentComplianceWire struct {
	Compliance       map[string]disruptionComplianceWire `json:"compliance,omitempty"`
	Cost             *costWire                           `json:"cost,omitempty"`
	ResiliencyScore  *resiliencyScoreWire                `json:"resiliencyScore,omitempty"`
	AppComponentName string                              `json:"appComponentName,omitempty"`
	Message          string                              `json:"message,omitempty"`
	Status           string                              `json:"status,omitempty"`
}

type listAppComponentCompliancesResponse struct {
	NextToken            string                       `json:"nextToken,omitempty"`
	ComponentCompliances []appComponentComplianceWire `json:"componentCompliances"`
}

// --- Recommendation families (always empty -- see PARITY.md) -----------------

type listAlarmRecommendationsResponse struct {
	NextToken            string     `json:"nextToken,omitempty"`
	AlarmRecommendations []struct{} `json:"alarmRecommendations"`
}

type listSopRecommendationsResponse struct {
	NextToken          string     `json:"nextToken,omitempty"`
	SopRecommendations []struct{} `json:"sopRecommendations"`
}

type listTestRecommendationsResponse struct {
	NextToken           string     `json:"nextToken,omitempty"`
	TestRecommendations []struct{} `json:"testRecommendations"`
}

type listAppComponentRecommendationsResponse struct {
	NextToken                string     `json:"nextToken,omitempty"`
	ComponentRecommendations []struct{} `json:"componentRecommendations"`
}

type updateRecommendationStatusItemWire struct {
	ResourceID      string `json:"resourceId,omitempty"`
	TargetAccountID string `json:"targetAccountId,omitempty"`
	TargetRegion    string `json:"targetRegion,omitempty"`
}

type updateRecommendationStatusRequestEntryWire struct {
	Item           *updateRecommendationStatusItemWire `json:"item,omitempty"`
	Excluded       *bool                               `json:"excluded,omitempty"`
	AppComponentID string                              `json:"appComponentId,omitempty"`
	EntryID        string                              `json:"entryId"`
	ExcludeReason  string                              `json:"excludeReason,omitempty"`
	ReferenceID    string                              `json:"referenceId,omitempty"`
}

type batchUpdateRecommendationStatusRequest struct {
	AppArn         string                                       `json:"appArn"`
	RequestEntries []updateRecommendationStatusRequestEntryWire `json:"requestEntries"`
}

type batchUpdateRecommendationStatusFailedEntryWire struct {
	EntryID      string `json:"entryId"`
	ErrorMessage string `json:"errorMessage"`
}

type batchUpdateRecommendationStatusSuccessfulEntryWire struct {
	Item           *updateRecommendationStatusItemWire `json:"item,omitempty"`
	Excluded       *bool                               `json:"excluded,omitempty"`
	AppComponentID string                              `json:"appComponentId,omitempty"`
	EntryID        string                              `json:"entryId"`
	ExcludeReason  string                              `json:"excludeReason,omitempty"`
	ReferenceID    string                              `json:"referenceId,omitempty"`
}

type batchUpdateRecommendationStatusResponse struct {
	AppArn            string                                               `json:"appArn"`
	FailedEntries     []batchUpdateRecommendationStatusFailedEntryWire     `json:"failedEntries"`
	SuccessfulEntries []batchUpdateRecommendationStatusSuccessfulEntryWire `json:"successfulEntries"`
}

// --- Recommendation templates --------------------------------------------------

type recommendationTemplateWire struct {
	StartTime                 *float64          `json:"startTime,omitempty"`
	EndTime                   *float64          `json:"endTime,omitempty"`
	TemplatesLocation         *s3LocationWire   `json:"templatesLocation,omitempty"`
	Tags                      map[string]string `json:"tags,omitempty"`
	NeedsReplacements         *bool             `json:"needsReplacements,omitempty"`
	Name                      string            `json:"name"`
	Format                    string            `json:"format"`
	RecommendationTemplateArn string            `json:"recommendationTemplateArn"`
	Status                    string            `json:"status"`
	AppArn                    string            `json:"appArn,omitempty"`
	Message                   string            `json:"message,omitempty"`
	AssessmentArn             string            `json:"assessmentArn"`
	RecommendationTypes       []string          `json:"recommendationTypes"`
	RecommendationIDs         []string          `json:"recommendationIds,omitempty"`
}

type createRecommendationTemplateRequest struct {
	Tags                map[string]string `json:"tags,omitempty"`
	AssessmentArn       string            `json:"assessmentArn"`
	Name                string            `json:"name"`
	BucketName          string            `json:"bucketName,omitempty"`
	ClientToken         string            `json:"clientToken,omitempty"`
	Format              string            `json:"format,omitempty"`
	RecommendationIDs   []string          `json:"recommendationIds,omitempty"`
	RecommendationTypes []string          `json:"recommendationTypes,omitempty"`
}

type recommendationTemplateEnvelope struct {
	RecommendationTemplate *recommendationTemplateWire `json:"recommendationTemplate"`
}

type recommendationTemplateArnRequest struct {
	RecommendationTemplateArn string `json:"recommendationTemplateArn"`
}

type deleteRecommendationTemplateResponse struct {
	RecommendationTemplateArn string `json:"recommendationTemplateArn"`
	Status                    string `json:"status"`
}

type listRecommendationTemplatesResponse struct {
	NextToken               string                       `json:"nextToken,omitempty"`
	RecommendationTemplates []recommendationTemplateWire `json:"recommendationTemplates"`
}

// --- Resource grouping recommendations (always empty -- see PARITY.md) ------

type acceptGroupingRecommendationEntryWire struct {
	GroupingRecommendationID string `json:"groupingRecommendationId"`
}

type acceptResourceGroupingRecommendationsRequest struct {
	AppArn  string                                  `json:"appArn"`
	Entries []acceptGroupingRecommendationEntryWire `json:"entries"`
}

type rejectGroupingRecommendationEntryWire struct {
	GroupingRecommendationID string `json:"groupingRecommendationId"`
	RejectionReason          string `json:"rejectionReason,omitempty"`
}

type rejectResourceGroupingRecommendationsRequest struct {
	AppArn  string                                  `json:"appArn"`
	Entries []rejectGroupingRecommendationEntryWire `json:"entries"`
}

type failedGroupingRecommendationEntryWire struct {
	ErrorMessage             string `json:"errorMessage"`
	GroupingRecommendationID string `json:"groupingRecommendationId"`
}

type groupingRecommendationsFailedResponse struct {
	AppArn        string                                  `json:"appArn"`
	FailedEntries []failedGroupingRecommendationEntryWire `json:"failedEntries"`
}

type groupingIDRequest struct {
	AppArn     string `json:"appArn"`
	GroupingID string `json:"groupingId"`
}

type startResourceGroupingRecommendationTaskRequest struct {
	AppArn string `json:"appArn"`
}

type groupingTaskResponse struct {
	AppArn       string `json:"appArn"`
	ErrorMessage string `json:"errorMessage,omitempty"`
	GroupingID   string `json:"groupingId"`
	Status       string `json:"status"`
}

type listResourceGroupingRecommendationsResponse struct {
	NextToken               string     `json:"nextToken,omitempty"`
	GroupingRecommendations []struct{} `json:"groupingRecommendations"`
}

// --- Metrics -----------------------------------------------------------------

type conditionWire struct {
	Field    string `json:"field,omitempty"`
	Operator string `json:"operator,omitempty"`
	Value    string `json:"value,omitempty"`
}

type fieldWire struct {
	Name        string `json:"name,omitempty"`
	Aggregation string `json:"aggregation,omitempty"`
}

type sortWire struct {
	Ascending *bool  `json:"ascending,omitempty"`
	Field     string `json:"field,omitempty"`
}

type listMetricsRequest struct {
	DataSource string          `json:"dataSource,omitempty"`
	Conditions []conditionWire `json:"conditions,omitempty"`
	Fields     []fieldWire     `json:"fields,omitempty"`
	Sorts      []sortWire      `json:"sorts,omitempty"`
}

type listMetricsResponse struct {
	NextToken string     `json:"nextToken,omitempty"`
	Rows      [][]string `json:"rows"`
}

type startMetricsExportRequest struct {
	BucketName  string `json:"bucketName,omitempty"`
	ClientToken string `json:"clientToken,omitempty"`
}

type startMetricsExportResponse struct {
	MetricsExportID string `json:"metricsExportId"`
	Status          string `json:"status"`
}

type metricsExportIDRequest struct {
	MetricsExportID string `json:"metricsExportId"`
}

type describeMetricsExportResponse struct {
	ExportLocation  *s3LocationWire `json:"exportLocation,omitempty"`
	ErrorMessage    string          `json:"errorMessage,omitempty"`
	MetricsExportID string          `json:"metricsExportId"`
	Status          string          `json:"status"`
}

// --- Tags ----------------------------------------------------------------------

type tagResourceRequest struct {
	Tags map[string]string `json:"tags"`
}

type listTagsForResourceResponse struct {
	Tags map[string]string `json:"tags"`
}
