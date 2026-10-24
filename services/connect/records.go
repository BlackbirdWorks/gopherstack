package connect

import (
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

type instanceRec struct {
	Attributes   map[string]string    `json:"attributes"`
	Instance     Instance             `json:"instance"`
	Origins      []string             `json:"origins,omitempty"`
	Lambdas      []string             `json:"lambdas,omitempty"`
	LexBots      []LexBotConfig       `json:"lexBots,omitempty"`
	SecurityKeys []SecurityKey        `json:"securityKeys,omitempty"`
	Storage      []StorageAssociation `json:"storage,omitempty"`
}

type userRec struct {
	StatusStart   *awstime.EpochTime `json:"statusStart,omitempty"`
	InstanceID    string             `json:"instanceId"`
	AgentStatusID string             `json:"agentStatusId,omitempty"`
	User          User               `json:"user"`
	Proficiencies []UserProficiency  `json:"proficiencies,omitempty"`
}

type routingProfileRec struct {
	InstanceID string                      `json:"instanceId"`
	Queues     []RoutingProfileQueueConfig `json:"queues,omitempty"`
	Manual     []RoutingProfileQueueConfig `json:"manualQueues,omitempty"`
	Profile    RoutingProfile              `json:"profile"`
}

type securityProfileRec struct {
	InstanceID  string          `json:"instanceId"`
	Profile     SecurityProfile `json:"profile"`
	Permissions []string        `json:"permissions,omitempty"`
	Extras      securityExtras  `json:"extras"`
}

type hierarchyGroupRec struct {
	InstanceID string         `json:"instanceId"`
	ParentID   string         `json:"parentId,omitempty"`
	Group      HierarchyGroup `json:"group"`
}

type hierarchyStructureRec struct {
	Structure  HierarchyStructure `json:"structure"`
	InstanceID string             `json:"instanceId"`
}

type queueRec struct {
	InstanceID     string   `json:"instanceId"`
	Queue          Queue    `json:"queue"`
	QuickConnects  []string `json:"quickConnects,omitempty"`
	EmailAddresses []string `json:"emailAddresses,omitempty"`
}

type hoursRec struct {
	InstanceID string           `json:"instanceId"`
	Hours      HoursOfOperation `json:"hours"`
	Parents    []string         `json:"parents,omitempty"`
}

type quickConnectRec struct {
	InstanceID string       `json:"instanceId"`
	Connect    QuickConnect `json:"connect"`
}

type flowRec struct {
	InstanceID string      `json:"instanceId"`
	Flow       ContactFlow `json:"flow"`
}

type moduleRec struct {
	InstanceID string            `json:"instanceId"`
	Module     ContactFlowModule `json:"module"`
}

type agentStatusRec struct {
	InstanceID string      `json:"instanceId"`
	Status     AgentStatus `json:"status"`
}

type promptRec struct {
	InstanceID string `json:"instanceId"`
	S3URI      string `json:"s3Uri"`
	Prompt     Prompt `json:"prompt"`
}

type contactRec struct {
	InstanceID       string          `json:"instanceId"`
	ParticipantID    string          `json:"participantId,omitempty"`
	ParticipantToken string          `json:"participantToken,omitempty"`
	FlowID           string          `json:"flowId,omitempty"`
	TaskTemplateID   string          `json:"taskTemplateId,omitempty"`
	Contact          Contact         `json:"contact"`
	References       json.RawMessage `json:"references,omitempty"`
}

type integrationRec struct {
	Tags        map[string]string            `json:"tags,omitempty"`
	UseCaseTags map[string]map[string]string `json:"useCaseTags,omitempty"`
	InstanceID  string                       `json:"instanceId"`
	ExternalID  string                       `json:"externalId,omitempty"`
	Association IntegrationAssociation       `json:"association"`
	UseCases    []UseCase                    `json:"useCases,omitempty"`
}

type phoneNumberRec struct {
	Phone      ClaimedPhoneNumberSummary `json:"phone"`
	InstanceID string                    `json:"instanceId"`
	FlowID     string                    `json:"flowId,omitempty"`
}

// extras holds the resource families added after the original service surface.
type extras struct {
	rules             family[ruleRec]
	taskTemplates     family[taskTemplateRec]
	predefined        family[predefinedRec]
	hoursOverrides    family[hoursOverrideRec]
	vocabularies      family[vocabularyRec]
	defaultVocabs     family[defaultVocabRec]
	flowVersions      family[flowVersionRec]
	flowAliases       family[flowAliasRec]
	views             family[viewRec]
	evalForms         family[evalFormRec]
	trafficGroups     family[tdgRec]
	flowAssocs        family[flowAssocRec]
	entityProfileRecs family[entityProfilesRec]
	viewSnapshots     family[viewVersionRec]
	dataTables        family[dataTableRec]
	emails            family[emailAddressRec]
	workspaces        family[workspaceRec]
}

func (b *Backend) registerTables() {
	r := b.registry
	b.instances = store.Register(r, "instances", store.New(func(v *instanceRec) string { return v.Instance.ID }))
	b.users = newFamily(r, "users", "User",
		func(v *userRec) string { return v.InstanceID }, func(v *userRec) string { return v.User.ID })
	b.routingProfiles = newFamily(r, "routingProfiles", "RoutingProfile",
		func(v *routingProfileRec) string { return v.InstanceID },
		func(v *routingProfileRec) string { return v.Profile.RoutingProfileID })
	b.securityProfiles = newFamily(r, "securityProfiles", "SecurityProfile",
		func(v *securityProfileRec) string { return v.InstanceID },
		func(v *securityProfileRec) string { return v.Profile.ID })
	b.hierarchyGroups = newFamily(r, "hierarchyGroups", "HierarchyGroup",
		func(v *hierarchyGroupRec) string { return v.InstanceID },
		func(v *hierarchyGroupRec) string { return v.Group.ID })
	b.hierarchyLevels = store.Register(r, "hierarchyStructures",
		store.New(func(v *hierarchyStructureRec) string { return v.InstanceID }))
	b.queues = newFamily(r, "queues", "Queue",
		func(v *queueRec) string { return v.InstanceID }, func(v *queueRec) string { return v.Queue.QueueID })
	b.hours = newFamily(r, "hours", "HoursOfOperation",
		func(v *hoursRec) string { return v.InstanceID },
		func(v *hoursRec) string { return v.Hours.HoursOfOperationID })
	b.quickConnects = newFamily(r, "quickConnects", "QuickConnect",
		func(v *quickConnectRec) string { return v.InstanceID },
		func(v *quickConnectRec) string { return v.Connect.QuickConnectID })
	b.flows = newFamily(r, "flows", "ContactFlow",
		func(v *flowRec) string { return v.InstanceID }, func(v *flowRec) string { return v.Flow.ID })
	b.modules = newFamily(r, "modules", "ContactFlowModule",
		func(v *moduleRec) string { return v.InstanceID }, func(v *moduleRec) string { return v.Module.ID })
	b.agentStatuses = newFamily(r, "agentStatuses", "AgentStatus",
		func(v *agentStatusRec) string { return v.InstanceID },
		func(v *agentStatusRec) string { return v.Status.AgentStatusID })
	b.prompts = newFamily(r, "prompts", "Prompt",
		func(v *promptRec) string { return v.InstanceID }, func(v *promptRec) string { return v.Prompt.PromptID })
	b.contacts = newFamily(r, "contacts", "Contact",
		func(v *contactRec) string { return v.InstanceID }, func(v *contactRec) string { return v.Contact.ID })
	b.integrations = newFamily(r, "integrations", "IntegrationAssociation",
		func(v *integrationRec) string { return v.InstanceID },
		func(v *integrationRec) string { return v.Association.IntegrationAssociationID })
	b.phoneNumbers = store.Register(r, "phoneNumbers",
		store.New(func(v *phoneNumberRec) string { return v.Phone.PhoneNumberID }))

	b.registerExtras()
	b.registerTagAccessors()
}

func (f *family[V]) taggable(arnOf func(*V) string, tagsOf func(*V) *map[string]string) {
	f.arnOf, f.tagsOf = arnOf, tagsOf
}

func (b *Backend) registerTagAccessors() {
	b.users.taggable(
		func(v *userRec) string { return v.User.Arn },
		func(v *userRec) *map[string]string { return &v.User.Tags },
	)
	b.routingProfiles.taggable(
		func(v *routingProfileRec) string { return v.Profile.RoutingProfileArn },
		func(v *routingProfileRec) *map[string]string { return &v.Profile.Tags })
	b.securityProfiles.taggable(
		func(v *securityProfileRec) string { return v.Profile.Arn },
		func(v *securityProfileRec) *map[string]string { return &v.Profile.Tags })
	b.hierarchyGroups.taggable(
		func(v *hierarchyGroupRec) string { return v.Group.Arn },
		func(v *hierarchyGroupRec) *map[string]string { return &v.Group.Tags })
	b.queues.taggable(
		func(v *queueRec) string { return v.Queue.QueueArn },
		func(v *queueRec) *map[string]string { return &v.Queue.Tags },
	)
	b.hours.taggable(
		func(v *hoursRec) string { return v.Hours.HoursOfOperationArn },
		func(v *hoursRec) *map[string]string { return &v.Hours.Tags })
	b.quickConnects.taggable(
		func(v *quickConnectRec) string { return v.Connect.QuickConnectARN },
		func(v *quickConnectRec) *map[string]string { return &v.Connect.Tags })
	b.flows.taggable(
		func(v *flowRec) string { return v.Flow.Arn },
		func(v *flowRec) *map[string]string { return &v.Flow.Tags },
	)
	b.modules.taggable(
		func(v *moduleRec) string { return v.Module.Arn },
		func(v *moduleRec) *map[string]string { return &v.Module.Tags },
	)
	b.agentStatuses.taggable(
		func(v *agentStatusRec) string { return v.Status.AgentStatusARN },
		func(v *agentStatusRec) *map[string]string { return &v.Status.Tags })
	b.prompts.taggable(
		func(v *promptRec) string { return v.Prompt.PromptARN },
		func(v *promptRec) *map[string]string { return &v.Prompt.Tags })
	b.contacts.taggable(
		func(v *contactRec) string { return v.Contact.Arn },
		func(v *contactRec) *map[string]string { return &v.Contact.Tags },
	)
	b.integrations.taggable(
		func(v *integrationRec) string { return v.Association.IntegrationAssociationArn },
		func(v *integrationRec) *map[string]string { return &v.Tags })
}
