package cleanrooms

// StorageBackend defines the interface for all Clean Rooms backend operations.
type StorageBackend interface {
	Region() string
	AccountID() string
	Reset()

	// Collaboration operations.
	CreateCollaboration(
		name, description, creatorDisplayName string,
		creatorMemberAbilities []string,
		members []MemberSpec,
		queryLogStatus string,
		tags map[string]string,
	) (*Collaboration, error)
	GetCollaboration(id string) (*Collaboration, error)
	ListCollaborations(memberStatus, maxResults, nextToken string) ([]*CollaborationSummary, string)
	UpdateCollaboration(id, name, description string) (*Collaboration, error)
	DeleteCollaboration(id string) error
	ListMembers(
		collaborationID string,
		maxResults, nextToken string,
	) ([]*MemberSummary, string, error)
	DeleteMember(collaborationID, accountID string) error

	// Membership operations.
	CreateMembership(
		collaborationID, queryLogStatus string,
		defaultResultConfiguration map[string]any,
		paymentConfiguration map[string]any,
		tags map[string]string,
	) (*Membership, error)
	GetMembership(id string) (*Membership, error)
	ListMemberships(status, maxResults, nextToken string) ([]*MembershipSummary, string)
	UpdateMembership(
		id, queryLogStatus string,
		defaultResultConfiguration map[string]any,
	) (*Membership, error)
	DeleteMembership(id string) error

	// ConfiguredTable operations.
	CreateConfiguredTable(
		name, description string,
		tableReference map[string]any,
		allowedColumns []string,
		analysisMethod string,
		tags map[string]string,
	) (*ConfiguredTable, error)
	GetConfiguredTable(id string) (*ConfiguredTable, error)
	ListConfiguredTables(maxResults, nextToken string) ([]*ConfiguredTableSummary, string)
	UpdateConfiguredTable(id, name, description string) (*ConfiguredTable, error)
	DeleteConfiguredTable(id string) error

	// ConfiguredTableAnalysisRule operations.
	CreateConfiguredTableAnalysisRule(
		configuredTableID, analysisRuleType string,
		policy map[string]any,
	) (*ConfiguredTableAnalysisRule, error)
	GetConfiguredTableAnalysisRule(
		configuredTableID, analysisRuleType string,
	) (*ConfiguredTableAnalysisRule, error)
	UpdateConfiguredTableAnalysisRule(
		configuredTableID, analysisRuleType string,
		policy map[string]any,
	) (*ConfiguredTableAnalysisRule, error)
	DeleteConfiguredTableAnalysisRule(configuredTableID, analysisRuleType string) error

	// ConfiguredTableAssociation operations.
	CreateConfiguredTableAssociation(
		membershipID, name, description, configuredTableID, roleArn string,
		tags map[string]string,
	) (*ConfiguredTableAssociation, error)
	GetConfiguredTableAssociation(membershipID, assocID string) (*ConfiguredTableAssociation, error)
	ListConfiguredTableAssociations(
		membershipID, maxResults, nextToken string,
	) ([]*ConfiguredTableAssociationSummary, string, error)
	UpdateConfiguredTableAssociation(
		membershipID, assocID, description, roleArn string,
	) (*ConfiguredTableAssociation, error)
	DeleteConfiguredTableAssociation(membershipID, assocID string) error

	// ConfiguredTableAssociationAnalysisRule operations.
	CreateConfiguredTableAssociationAnalysisRule(
		membershipID, assocID, ruleType string,
		policy map[string]any,
	) (*ConfiguredTableAssociationAnalysisRule, error)
	GetConfiguredTableAssociationAnalysisRule(
		membershipID, assocID, ruleType string,
	) (*ConfiguredTableAssociationAnalysisRule, error)
	UpdateConfiguredTableAssociationAnalysisRule(
		membershipID, assocID, ruleType string,
		policy map[string]any,
	) (*ConfiguredTableAssociationAnalysisRule, error)
	DeleteConfiguredTableAssociationAnalysisRule(membershipID, assocID, ruleType string) error

	// AnalysisTemplate operations.
	CreateAnalysisTemplate(
		membershipID, name, description, format string,
		source map[string]any,
		analysisParameters []map[string]any,
		tags map[string]string,
	) (*AnalysisTemplate, error)
	GetAnalysisTemplate(membershipID, templateID string) (*AnalysisTemplate, error)
	ListAnalysisTemplates(
		membershipID, maxResults, nextToken string,
	) ([]*AnalysisTemplateSummary, string, error)
	UpdateAnalysisTemplate(membershipID, templateID, description string) (*AnalysisTemplate, error)
	DeleteAnalysisTemplate(membershipID, templateID string) error

	// Collaboration AnalysisTemplate operations (read-only views).
	GetCollaborationAnalysisTemplate(collaborationID, templateArn string) (*AnalysisTemplate, error)
	ListCollaborationAnalysisTemplates(
		collaborationID, maxResults, nextToken string,
	) ([]*AnalysisTemplateSummary, string, error)
	BatchGetCollaborationAnalysisTemplate(
		collaborationID string,
		templateArns []string,
	) ([]*AnalysisTemplate, []BatchError, error)

	// Schema operations.
	GetSchema(collaborationID, name string) (*Schema, error)
	ListSchemas(
		collaborationID, schemaType, maxResults, nextToken string,
	) ([]*SchemaSummary, string, error)
	BatchGetSchema(collaborationID string, names []string) ([]*Schema, []BatchError, error)
	GetSchemaAnalysisRule(collaborationID, name, ruleType string) (*SchemaAnalysisRule, error)
	BatchGetSchemaAnalysisRule(
		collaborationID string,
		names []string,
		ruleType string,
	) ([]*SchemaAnalysisRule, []BatchError, error)

	// ProtectedQuery operations.
	StartProtectedQuery(
		membershipID, sqlText string,
		resultConfig map[string]any,
		computeConfiguration map[string]any,
	) (*ProtectedQuery, error)
	GetProtectedQuery(membershipID, queryID string) (*ProtectedQuery, error)
	ListProtectedQueries(
		membershipID, status, maxResults, nextToken string,
	) ([]*ProtectedQuerySummary, string, error)
	UpdateProtectedQuery(membershipID, queryID, status string) (*ProtectedQuery, error)

	// ProtectedJob operations.
	StartProtectedJob(
		membershipID, jobType string,
		jobParameters map[string]any,
		resultConfig map[string]any,
	) (*ProtectedJob, error)
	GetProtectedJob(membershipID, jobID string) (*ProtectedJob, error)
	ListProtectedJobs(
		membershipID, status, maxResults, nextToken string,
	) ([]*ProtectedJobSummary, string, error)
	UpdateProtectedJob(membershipID, jobID, status string) (*ProtectedJob, error)

	// PrivacyBudgetTemplate operations.
	CreatePrivacyBudgetTemplate(
		membershipID, privacyBudgetType, autoRefresh string,
		parameters map[string]any,
		tags map[string]string,
	) (*PrivacyBudgetTemplate, error)
	GetPrivacyBudgetTemplate(membershipID, templateID string) (*PrivacyBudgetTemplate, error)
	ListPrivacyBudgetTemplates(
		membershipID, privacyBudgetType, maxResults, nextToken string,
	) ([]*PrivacyBudgetTemplateSummary, string, error)
	UpdatePrivacyBudgetTemplate(
		membershipID, templateID, autoRefresh string,
		parameters map[string]any,
	) (*PrivacyBudgetTemplate, error)
	DeletePrivacyBudgetTemplate(membershipID, templateID string) error

	// PrivacyBudget operations (read-only).
	ListPrivacyBudgets(
		membershipID, privacyBudgetType, maxResults, nextToken string,
	) ([]*PrivacyBudget, string, error)
	ListCollaborationPrivacyBudgets(
		collaborationID, privacyBudgetType, maxResults, nextToken string,
	) ([]*PrivacyBudget, string, error)
	GetCollaborationPrivacyBudgetTemplate(
		collaborationID, templateID string,
	) (*PrivacyBudgetTemplate, error)
	ListCollaborationPrivacyBudgetTemplates(
		collaborationID, maxResults, nextToken string,
	) ([]*PrivacyBudgetTemplateSummary, string, error)
	PreviewPrivacyImpact(membershipID string, parameters map[string]any) (map[string]any, error)

	// IdMappingTable operations.
	CreateIdMappingTable(
		membershipID, name, description string,
		inputReferenceConfig map[string]any,
		kmsKeyArn string,
		tags map[string]string,
	) (*IdMappingTable, error)
	GetIdMappingTable(membershipID, tableID string) (*IdMappingTable, error)
	ListIdMappingTables(
		membershipID, maxResults, nextToken string,
	) ([]*IdMappingTableSummary, string, error)
	UpdateIdMappingTable(
		membershipID, tableID, description, kmsKeyArn string,
	) (*IdMappingTable, error)
	DeleteIdMappingTable(membershipID, tableID string) error
	PopulateIdMappingTable(membershipID, tableID string) (map[string]any, error)

	// IdNamespaceAssociation operations.
	CreateIdNamespaceAssociation(
		membershipID, name, description string,
		inputReferenceConfig map[string]any,
		idMappingConfig map[string]any,
		tags map[string]string,
	) (*IdNamespaceAssociation, error)
	GetIdNamespaceAssociation(membershipID, assocID string) (*IdNamespaceAssociation, error)
	ListIdNamespaceAssociations(
		membershipID, maxResults, nextToken string,
	) ([]*IdNamespaceAssociationSummary, string, error)
	UpdateIdNamespaceAssociation(
		membershipID, assocID, description string,
		idMappingConfig map[string]any,
	) (*IdNamespaceAssociation, error)
	DeleteIdNamespaceAssociation(membershipID, assocID string) error
	GetCollaborationIdNamespaceAssociation(
		collaborationID, assocID string,
	) (*IdNamespaceAssociation, error)
	ListCollaborationIdNamespaceAssociations(
		collaborationID, maxResults, nextToken string,
	) ([]*IdNamespaceAssociationSummary, string, error)

	// ConfiguredAudienceModelAssociation operations.
	CreateConfiguredAudienceModelAssociation(
		membershipID, configuredAudienceModelArn, name, description string,
		manageResourcePolicies bool,
		tags map[string]string,
	) (*ConfiguredAudienceModelAssociation, error)
	GetConfiguredAudienceModelAssociation(
		membershipID, assocID string,
	) (*ConfiguredAudienceModelAssociation, error)
	ListConfiguredAudienceModelAssociations(
		membershipID, maxResults, nextToken string,
	) ([]*ConfiguredAudienceModelAssociationSummary, string, error)
	UpdateConfiguredAudienceModelAssociation(
		membershipID, assocID, name, description string,
	) (*ConfiguredAudienceModelAssociation, error)
	DeleteConfiguredAudienceModelAssociation(membershipID, assocID string) error
	GetCollaborationConfiguredAudienceModelAssociation(
		collaborationID, assocID string,
	) (*ConfiguredAudienceModelAssociation, error)
	ListCollaborationConfiguredAudienceModelAssociations(
		collaborationID, maxResults, nextToken string,
	) ([]*ConfiguredAudienceModelAssociationSummary, string, error)

	// CollaborationChangeRequest operations.
	CreateCollaborationChangeRequest(
		collaborationID, changeRequestType string,
		details map[string]any,
	) (*CollaborationChangeRequest, error)
	GetCollaborationChangeRequest(
		collaborationID, changeRequestID string,
	) (*CollaborationChangeRequest, error)
	ListCollaborationChangeRequests(
		collaborationID, maxResults, nextToken string,
	) ([]*CollaborationChangeRequest, string, error)
	UpdateCollaborationChangeRequest(
		collaborationID, changeRequestID, status string,
	) (*CollaborationChangeRequest, error)

	// Tag operations.
	ListTagsForResource(resourceArn string) (map[string]string, error)
	TagResource(resourceArn string, tags map[string]string) error
	UntagResource(resourceArn string, tagKeys []string) error
}

// compile-time assertion that InMemoryBackend implements StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
