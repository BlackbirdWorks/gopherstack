package connect

import (
	"net/http"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

const (
	pathHoursOfOperationsInstanceidHoursofoperat = "/hours-of-operations/{InstanceId}/{HoursOfOperationId}"
	pathInstanceInstanceidStorageConfigAssociati = "/instance/{InstanceId}/storage-config/{AssociationId}"
	pathPhoneNumberPhonenumberid                 = "/phone-number/{PhoneNumberId}"
	pathPromptsInstanceidPromptid                = "/prompts/{InstanceId}/{PromptId}"
	pathSecurityProfilesInstanceidSecurityprofil = "/security-profiles/{InstanceId}/{SecurityProfileId}"
	pathTagsResourcearn                          = "/tags/{resourceArn}"
)

func routes() []restroute.Route {
	return slices.Concat(
		instanceRoutes(),
		peopleRoutes(),
		routingRoutes(),
		profileRoutes(),
		flowsRoutes(),
		contactsRoutes(),
		extraRoutes(),
	)
}

func instanceRoutes() []restroute.Route {
	return []restroute.Route{
		{Method: http.MethodPut, Path: "/instance/{InstanceId}/approved-origin", Op: "AssociateApprovedOrigin"},
		{Method: http.MethodPut, Path: "/instance/{InstanceId}/bot", Op: "AssociateBot"},
		{Method: http.MethodPut, Path: "/instance/{InstanceId}/storage-config", Op: "AssociateInstanceStorageConfig"},
		{Method: http.MethodPut, Path: "/instance/{InstanceId}/lambda-function", Op: "AssociateLambdaFunction"},
		{Method: http.MethodPut, Path: "/instance/{InstanceId}/lex-bot", Op: "AssociateLexBot"},
		{Method: http.MethodPut, Path: "/instance/{InstanceId}/security-key", Op: "AssociateSecurityKey"},
		{Method: http.MethodPut, Path: pathInstance, Op: "CreateInstance"},
		{
			Method: http.MethodPut,
			Path:   "/instance/{InstanceId}/integration-associations",
			Op:     "CreateIntegrationAssociation",
		},
		{
			Method: http.MethodPut,
			Path:   "/instance/{InstanceId}/integration-associations/{IntegrationAssociationId}/use-cases",
			Op:     "CreateUseCase",
		},
		{Method: http.MethodDelete, Path: "/instance/{InstanceId}", Op: "DeleteInstance"},
		{
			Method: http.MethodDelete,
			Path:   "/instance/{InstanceId}/integration-associations/{IntegrationAssociationId}",
			Op:     "DeleteIntegrationAssociation",
		},
		{
			Method: http.MethodDelete,
			Path:   "/instance/{InstanceId}/integration-associations/{IntegrationAssociationId}/use-cases/{UseCaseId}",
			Op:     "DeleteUseCase",
		},
		{Method: http.MethodGet, Path: "/instance/{InstanceId}", Op: "DescribeInstance"},
		{
			Method: http.MethodGet,
			Path:   "/instance/{InstanceId}/attribute/{AttributeType}",
			Op:     "DescribeInstanceAttribute",
		},
		{
			Method: http.MethodGet,
			Path:   pathInstanceInstanceidStorageConfigAssociati,
			Op:     "DescribeInstanceStorageConfig",
		},
		{Method: http.MethodDelete, Path: "/instance/{InstanceId}/approved-origin", Op: "DisassociateApprovedOrigin"},
		{Method: http.MethodPost, Path: "/instance/{InstanceId}/bot", Op: "DisassociateBot"},
		{
			Method: http.MethodDelete,
			Path:   pathInstanceInstanceidStorageConfigAssociati,
			Op:     "DisassociateInstanceStorageConfig",
		},
		{Method: http.MethodDelete, Path: "/instance/{InstanceId}/lambda-function", Op: "DisassociateLambdaFunction"},
		{Method: http.MethodDelete, Path: "/instance/{InstanceId}/lex-bot", Op: "DisassociateLexBot"},
		{
			Method: http.MethodDelete,
			Path:   "/instance/{InstanceId}/security-key/{AssociationId}",
			Op:     "DisassociateSecurityKey",
		},
		{Method: http.MethodGet, Path: "/instance/{InstanceId}/approved-origins", Op: "ListApprovedOrigins"},
		{Method: http.MethodGet, Path: "/instance/{InstanceId}/bots", Op: "ListBots"},
		{Method: http.MethodGet, Path: "/instance/{InstanceId}/attributes", Op: "ListInstanceAttributes"},
		{Method: http.MethodGet, Path: "/instance/{InstanceId}/storage-configs", Op: "ListInstanceStorageConfigs"},
		{Method: http.MethodGet, Path: pathInstance, Op: "ListInstances"},
		{
			Method: http.MethodGet,
			Path:   "/instance/{InstanceId}/integration-associations",
			Op:     "ListIntegrationAssociations",
		},
		{Method: http.MethodGet, Path: "/instance/{InstanceId}/lambda-functions", Op: "ListLambdaFunctions"},
		{Method: http.MethodGet, Path: "/instance/{InstanceId}/lex-bots", Op: "ListLexBots"},
		{Method: http.MethodGet, Path: "/instance/{InstanceId}/security-keys", Op: "ListSecurityKeys"},
		{
			Method: http.MethodGet,
			Path:   "/instance/{InstanceId}/integration-associations/{IntegrationAssociationId}/use-cases",
			Op:     "ListUseCases",
		},
		{
			Method: http.MethodPost,
			Path:   "/instance/{InstanceId}/attribute/{AttributeType}",
			Op:     "UpdateInstanceAttribute",
		},
		{
			Method: http.MethodPost,
			Path:   pathInstanceInstanceidStorageConfigAssociati,
			Op:     "UpdateInstanceStorageConfig",
		},
	}
}

func peopleRoutes() []restroute.Route {
	return []restroute.Route{
		{Method: http.MethodPut, Path: "/agent-status/{InstanceId}", Op: "CreateAgentStatus"},
		{Method: http.MethodPut, Path: "/prompts/{InstanceId}", Op: "CreatePrompt"},
		{Method: http.MethodPut, Path: "/security-profiles/{InstanceId}", Op: "CreateSecurityProfile"},
		{Method: http.MethodPut, Path: "/users/{InstanceId}", Op: "CreateUser"},
		{Method: http.MethodPut, Path: "/user-hierarchy-groups/{InstanceId}", Op: "CreateUserHierarchyGroup"},
		{Method: http.MethodDelete, Path: pathPromptsInstanceidPromptid, Op: "DeletePrompt"},
		{Method: http.MethodDelete, Path: pathSecurityProfilesInstanceidSecurityprofil, Op: "DeleteSecurityProfile"},
		{Method: http.MethodDelete, Path: "/users/{InstanceId}/{UserId}", Op: "DeleteUser"},
		{
			Method: http.MethodDelete,
			Path:   "/user-hierarchy-groups/{InstanceId}/{HierarchyGroupId}",
			Op:     "DeleteUserHierarchyGroup",
		},
		{Method: http.MethodGet, Path: "/agent-status/{InstanceId}/{AgentStatusId}", Op: "DescribeAgentStatus"},
		{Method: http.MethodGet, Path: pathPromptsInstanceidPromptid, Op: "DescribePrompt"},
		{Method: http.MethodGet, Path: pathSecurityProfilesInstanceidSecurityprofil, Op: "DescribeSecurityProfile"},
		{Method: http.MethodGet, Path: "/users/{InstanceId}/{UserId}", Op: "DescribeUser"},
		{
			Method: http.MethodGet,
			Path:   "/user-hierarchy-groups/{InstanceId}/{HierarchyGroupId}",
			Op:     "DescribeUserHierarchyGroup",
		},
		{
			Method: http.MethodGet,
			Path:   "/user-hierarchy-structure/{InstanceId}",
			Op:     "DescribeUserHierarchyStructure",
		},
		{Method: http.MethodGet, Path: "/agent-status/{InstanceId}", Op: "ListAgentStatuses"},
		{Method: http.MethodGet, Path: "/prompts-summary/{InstanceId}", Op: "ListPrompts"},
		{
			Method: http.MethodGet,
			Path:   "/security-profiles-permissions/{InstanceId}/{SecurityProfileId}",
			Op:     "ListSecurityProfilePermissions",
		},
		{Method: http.MethodGet, Path: "/security-profiles-summary/{InstanceId}", Op: "ListSecurityProfiles"},
		{Method: http.MethodGet, Path: "/user-hierarchy-groups-summary/{InstanceId}", Op: "ListUserHierarchyGroups"},
		{Method: http.MethodGet, Path: "/users-summary/{InstanceId}", Op: "ListUsers"},
		{Method: http.MethodPut, Path: "/users/{InstanceId}/{UserId}/status", Op: "PutUserStatus"},
		{Method: http.MethodPost, Path: "/agent-status/{InstanceId}/{AgentStatusId}", Op: "UpdateAgentStatus"},
		{Method: http.MethodPost, Path: pathPromptsInstanceidPromptid, Op: "UpdatePrompt"},
		{Method: http.MethodPost, Path: pathSecurityProfilesInstanceidSecurityprofil, Op: "UpdateSecurityProfile"},
		{Method: http.MethodPost, Path: "/users/{InstanceId}/{UserId}/hierarchy", Op: "UpdateUserHierarchy"},
		{
			Method: http.MethodPost,
			Path:   "/user-hierarchy-groups/{InstanceId}/{HierarchyGroupId}/name",
			Op:     "UpdateUserHierarchyGroupName",
		},
		{Method: http.MethodPost, Path: "/user-hierarchy-structure/{InstanceId}", Op: "UpdateUserHierarchyStructure"},
		{Method: http.MethodPost, Path: "/users/{InstanceId}/{UserId}/identity-info", Op: "UpdateUserIdentityInfo"},
		{Method: http.MethodPost, Path: "/users/{InstanceId}/{UserId}/phone-config", Op: "UpdateUserPhoneConfig"},
		{
			Method: http.MethodPost,
			Path:   "/users/{InstanceId}/{UserId}/routing-profile",
			Op:     "UpdateUserRoutingProfile",
		},
		{
			Method: http.MethodPost,
			Path:   "/users/{InstanceId}/{UserId}/security-profiles",
			Op:     "UpdateUserSecurityProfiles",
		},
	}
}

func routingRoutes() []restroute.Route {
	return []restroute.Route{
		{
			Method: http.MethodPost,
			Path:   "/queues/{InstanceId}/{QueueId}/associate-quick-connects",
			Op:     "AssociateQueueQuickConnects",
		},
		{Method: http.MethodPut, Path: "/hours-of-operations/{InstanceId}", Op: "CreateHoursOfOperation"},
		{Method: http.MethodPut, Path: "/queues/{InstanceId}", Op: "CreateQueue"},
		{Method: http.MethodPut, Path: "/quick-connects/{InstanceId}", Op: "CreateQuickConnect"},
		{Method: http.MethodPut, Path: "/routing-profiles/{InstanceId}", Op: "CreateRoutingProfile"},
		{Method: http.MethodDelete, Path: pathHoursOfOperationsInstanceidHoursofoperat, Op: "DeleteHoursOfOperation"},
		{Method: http.MethodDelete, Path: "/queues/{InstanceId}/{QueueId}", Op: "DeleteQueue"},
		{Method: http.MethodDelete, Path: "/quick-connects/{InstanceId}/{QuickConnectId}", Op: "DeleteQuickConnect"},
		{
			Method: http.MethodDelete,
			Path:   "/routing-profiles/{InstanceId}/{RoutingProfileId}",
			Op:     "DeleteRoutingProfile",
		},
		{Method: http.MethodGet, Path: pathHoursOfOperationsInstanceidHoursofoperat, Op: "DescribeHoursOfOperation"},
		{Method: http.MethodGet, Path: "/queues/{InstanceId}/{QueueId}", Op: "DescribeQueue"},
		{Method: http.MethodGet, Path: "/quick-connects/{InstanceId}/{QuickConnectId}", Op: "DescribeQuickConnect"},
		{
			Method: http.MethodGet,
			Path:   "/routing-profiles/{InstanceId}/{RoutingProfileId}",
			Op:     "DescribeRoutingProfile",
		},
		{
			Method: http.MethodPost,
			Path:   "/queues/{InstanceId}/{QueueId}/disassociate-quick-connects",
			Op:     "DisassociateQueueQuickConnects",
		},
		{Method: http.MethodGet, Path: "/hours-of-operations-summary/{InstanceId}", Op: "ListHoursOfOperations"},
		{Method: http.MethodGet, Path: "/queues/{InstanceId}/{QueueId}/quick-connects", Op: "ListQueueQuickConnects"},
		{Method: http.MethodGet, Path: "/queues-summary/{InstanceId}", Op: "ListQueues"},
		{Method: http.MethodGet, Path: "/quick-connects/{InstanceId}", Op: "ListQuickConnects"},
		{
			Method: http.MethodGet,
			Path:   "/routing-profiles/{InstanceId}/{RoutingProfileId}/queues",
			Op:     "ListRoutingProfileQueues",
		},
		{Method: http.MethodGet, Path: "/routing-profiles-summary/{InstanceId}", Op: "ListRoutingProfiles"},
		{Method: http.MethodPost, Path: pathHoursOfOperationsInstanceidHoursofoperat, Op: "UpdateHoursOfOperation"},
		{
			Method: http.MethodPost,
			Path:   "/queues/{InstanceId}/{QueueId}/hours-of-operation",
			Op:     "UpdateQueueHoursOfOperation",
		},
		{Method: http.MethodPost, Path: "/queues/{InstanceId}/{QueueId}/max-contacts", Op: "UpdateQueueMaxContacts"},
		{Method: http.MethodPost, Path: "/queues/{InstanceId}/{QueueId}/name", Op: "UpdateQueueName"},
		{
			Method: http.MethodPost,
			Path:   "/queues/{InstanceId}/{QueueId}/outbound-caller-config",
			Op:     "UpdateQueueOutboundCallerConfig",
		},
		{
			Method: http.MethodPost,
			Path:   "/queues/{InstanceId}/{QueueId}/outbound-email-config",
			Op:     "UpdateQueueOutboundEmailConfig",
		},
		{Method: http.MethodPost, Path: "/queues/{InstanceId}/{QueueId}/status", Op: "UpdateQueueStatus"},
		{
			Method: http.MethodPost,
			Path:   "/quick-connects/{InstanceId}/{QuickConnectId}/config",
			Op:     "UpdateQuickConnectConfig",
		},
		{
			Method: http.MethodPost,
			Path:   "/routing-profiles/{InstanceId}/{RoutingProfileId}/queues",
			Op:     "UpdateRoutingProfileQueues",
		},
	}
}

func profileRoutes() []restroute.Route {
	return []restroute.Route{
		{
			Method: http.MethodPost,
			Path:   "/routing-profiles/{InstanceId}/{RoutingProfileId}/associate-queues",
			Op:     "AssociateRoutingProfileQueues",
		},
		{
			Method: http.MethodPost,
			Path:   "/routing-profiles/{InstanceId}/{RoutingProfileId}/disassociate-queues",
			Op:     "DisassociateRoutingProfileQueues",
		},
		{
			Method: http.MethodPost,
			Path:   "/quick-connects/{InstanceId}/{QuickConnectId}/name",
			Op:     "UpdateQuickConnectName",
		},
		{
			Method: http.MethodPost,
			Path:   "/routing-profiles/{InstanceId}/{RoutingProfileId}/agent-availability-timer",
			Op:     "UpdateRoutingProfileAgentAvailabilityTimer",
		},
		{
			Method: http.MethodPost,
			Path:   "/routing-profiles/{InstanceId}/{RoutingProfileId}/concurrency",
			Op:     "UpdateRoutingProfileConcurrency",
		},
		{
			Method: http.MethodPost,
			Path:   "/routing-profiles/{InstanceId}/{RoutingProfileId}/default-outbound-queue",
			Op:     "UpdateRoutingProfileDefaultOutboundQueue",
		},
		{
			Method: http.MethodPost,
			Path:   "/routing-profiles/{InstanceId}/{RoutingProfileId}/name",
			Op:     "UpdateRoutingProfileName",
		},
	}
}

func flowsRoutes() []restroute.Route {
	return []restroute.Route{
		{
			Method: http.MethodPut,
			Path:   "/phone-number/{PhoneNumberId}/contact-flow",
			Op:     "AssociatePhoneNumberContactFlow",
		},
		{Method: http.MethodPost, Path: "/phone-number/claim", Op: "ClaimPhoneNumber"},
		{Method: http.MethodPut, Path: "/contact-flows/{InstanceId}", Op: "CreateContactFlow"},
		{Method: http.MethodPut, Path: "/contact-flow-modules/{InstanceId}", Op: "CreateContactFlowModule"},
		{Method: http.MethodDelete, Path: pathFlowBase, Op: "DeleteContactFlow"},
		{
			Method: http.MethodDelete,
			Path:   pathModuleBase,
			Op:     "DeleteContactFlowModule",
		},
		{Method: http.MethodGet, Path: pathFlowBase, Op: "DescribeContactFlow"},
		{
			Method: http.MethodGet,
			Path:   pathModuleBase,
			Op:     "DescribeContactFlowModule",
		},
		{Method: http.MethodGet, Path: pathPhoneNumberPhonenumberid, Op: "DescribePhoneNumber"},
		{
			Method: http.MethodDelete,
			Path:   "/phone-number/{PhoneNumberId}/contact-flow",
			Op:     "DisassociatePhoneNumberContactFlow",
		},
		{Method: http.MethodGet, Path: "/contact-flow-modules-summary/{InstanceId}", Op: "ListContactFlowModules"},
		{Method: http.MethodGet, Path: "/contact-flows-summary/{InstanceId}", Op: "ListContactFlows"},
		{Method: http.MethodGet, Path: "/phone-numbers-summary/{InstanceId}", Op: "ListPhoneNumbers"},
		{Method: http.MethodPost, Path: "/phone-number/list", Op: "ListPhoneNumbersV2"},
		{Method: http.MethodDelete, Path: pathPhoneNumberPhonenumberid, Op: "ReleasePhoneNumber"},
		{Method: http.MethodPost, Path: "/phone-number/search-available", Op: "SearchAvailablePhoneNumbers"},
		{
			Method: http.MethodPost,
			Path:   "/contact-flows/{InstanceId}/{ContactFlowId}/content",
			Op:     "UpdateContactFlowContent",
		},
		{
			Method: http.MethodPost,
			Path:   "/contact-flows/{InstanceId}/{ContactFlowId}/metadata",
			Op:     "UpdateContactFlowMetadata",
		},
		{
			Method: http.MethodPost,
			Path:   "/contact-flow-modules/{InstanceId}/{ContactFlowModuleId}/content",
			Op:     "UpdateContactFlowModuleContent",
		},
		{
			Method: http.MethodPost,
			Path:   "/contact-flow-modules/{InstanceId}/{ContactFlowModuleId}/metadata",
			Op:     "UpdateContactFlowModuleMetadata",
		},
		{
			Method: http.MethodPost,
			Path:   "/contact-flows/{InstanceId}/{ContactFlowId}/name",
			Op:     "UpdateContactFlowName",
		},
		{Method: http.MethodPut, Path: pathPhoneNumberPhonenumberid, Op: "UpdatePhoneNumber"},
		{Method: http.MethodPut, Path: "/phone-number/{PhoneNumberId}/metadata", Op: "UpdatePhoneNumberMetadata"},
	}
}

func contactsRoutes() []restroute.Route {
	return []restroute.Route{
		{Method: http.MethodGet, Path: "/contacts/{InstanceId}/{ContactId}", Op: "DescribeContact"},
		{
			Method: http.MethodGet,
			Path:   "/contact/attributes/{InstanceId}/{InitialContactId}",
			Op:     "GetContactAttributes",
		},
		{Method: http.MethodPost, Path: "/metrics/current/{InstanceId}", Op: "GetCurrentMetricData"},
		{Method: http.MethodPost, Path: "/metrics/userdata/{InstanceId}", Op: "GetCurrentUserData"},
		{Method: http.MethodPost, Path: "/metrics/historical/{InstanceId}", Op: "GetMetricData"},
		{Method: http.MethodGet, Path: pathTagsResourcearn, Op: "ListTagsForResource"},
		{Method: http.MethodPost, Path: "/search-contacts", Op: "SearchContacts"},
		{Method: http.MethodPut, Path: "/contact/chat", Op: "StartChatContact"},
		{Method: http.MethodPut, Path: "/contact/outbound-voice", Op: "StartOutboundVoiceContact"},
		{Method: http.MethodPut, Path: "/contact/task", Op: "StartTaskContact"},
		{Method: http.MethodPost, Path: "/contact/stop", Op: "StopContact"},
		{Method: http.MethodPost, Path: "/contact/tags", Op: "TagContact"},
		{Method: http.MethodPost, Path: pathTagsResourcearn, Op: "TagResource"},
		{Method: http.MethodDelete, Path: "/contact/tags/{InstanceId}/{ContactId}", Op: "UntagContact"},
		{Method: http.MethodDelete, Path: pathTagsResourcearn, Op: "UntagResource"},
		{Method: http.MethodPost, Path: "/contacts/{InstanceId}/{ContactId}", Op: "UpdateContact"},
		{Method: http.MethodPost, Path: "/contact/attributes", Op: "UpdateContactAttributes"},
	}
}
