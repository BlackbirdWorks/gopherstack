package connect

import "net/http"

const (
	jsonNull         = "null"
	fieldDescription = "description"
	fieldState       = "state"
	fieldName        = "name"
	fieldStatus      = "status"
	fieldType        = "type"
	emailValue       = "EMAIL"
	entityUser       = "USER"
	pathFlowBase     = "/contact-flows/{InstanceId}/{ContactFlowId}"
	pathModuleBase   = "/contact-flow-modules/{InstanceId}/{ContactFlowModuleId}"
	pathRuleByID     = "/rules/{InstanceId}/{RuleId}"
	pathPredefByName = "/predefined-attributes/{InstanceId}/{Name}"
	pathViewByID     = "/views/{InstanceId}/{ViewId}"
	labelModule      = "ContactFlowModule"
	channelVoice     = "VOICE"
	channelChat      = "CHAT"
	channelTask      = "TASK"

	integrationEvent       = "EVENT"
	integrationApplication = "APPLICATION"

	permBasicAgent    = "BasicAgentAccess"
	permOutboundCalls = "OutboundCallAccess"
	permMetrics       = "AccessMetrics"

	metricScheduled = "CONTACTS_SCHEDULED"

	pathInstance = "/instance"

	statusForbidden = http.StatusForbidden
	statusConflict  = http.StatusConflict
	statusNotFound  = http.StatusNotFound

	assocIDHexLen = 64
	pairCount     = 2

	orderAvailable  = 1
	orderOffline    = 2
	chatConcurrency = 2

	tsScheduled  = "SCHEDULED_TIMESTAMP"
	tsDisconnect = "DISCONNECT_TIMESTAMP"
	tsConnected  = "CONNECTED_TO_AGENT_TIMESTAMP"
	tsInitiation = "INITIATION_TIMESTAMP"
	metricOldest = "OLDEST_CONTACT_AGE"
)
