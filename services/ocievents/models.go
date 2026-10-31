package ocievents

import "time"

// Rule lifecycle states.
const (
	stateActive   = "ACTIVE"
	stateInactive = "INACTIVE"
	stateDeleted  = "DELETED"
)

// Action types.
const (
	actionONS  = "ONS"
	actionOSS  = "OSS"
	actionFAAS = "FAAS"
)

// Tags are the shared freeform and defined tag sets of a resource.
type Tags struct {
	FreeformTags map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags  map[string]map[string]any `json:"definedTags,omitempty"`
}

func (t *Tags) merge(in Tags) {
	if in.FreeformTags != nil {
		t.FreeformTags = in.FreeformTags
	}

	if in.DefinedTags != nil {
		t.DefinedTags = in.DefinedTags
	}
}

// Action is one destination of a rule.
type Action struct {
	ID          string `json:"id"`
	ActionType  string `json:"actionType"`
	State       string `json:"lifecycleState"`
	Message     string `json:"lifecycleMessage"`
	Description string `json:"description,omitempty"`
	TopicID     string `json:"topicId,omitempty"`
	StreamID    string `json:"streamId,omitempty"`
	FunctionID  string `json:"functionId,omitempty"`
	IsEnabled   bool   `json:"isEnabled"`
}

// ActionList is the wire wrapper of a rule's actions.
type ActionList struct {
	Actions []*Action `json:"actions"`
}

// Rule is an Events rule.
type Rule struct {
	TimeCreated   time.Time `json:"timeCreated"`
	Tags          Tags      `json:"tags"`
	ID            string    `json:"id"`
	DisplayName   string    `json:"displayName"`
	Description   string    `json:"description,omitempty"`
	Condition     string    `json:"condition"`
	CompartmentID string    `json:"compartmentId"`
	State         string    `json:"state"`
	Etag          string    `json:"etag"`
	Actions       []*Action `json:"actions"`
	Seq           int64     `json:"seq"`
	IsEnabled     bool      `json:"isEnabled"`
}

// RuleWire is the wire form of a rule.
type RuleWire struct {
	TimeCreated      time.Time                 `json:"timeCreated"`
	DefinedTags      map[string]map[string]any `json:"definedTags,omitempty"`
	FreeformTags     map[string]string         `json:"freeformTags,omitempty"`
	ID               string                    `json:"id"`
	DisplayName      string                    `json:"displayName"`
	Description      string                    `json:"description,omitempty"`
	Condition        string                    `json:"condition"`
	CompartmentID    string                    `json:"compartmentId"`
	LifecycleState   string                    `json:"lifecycleState"`
	LifecycleMessage string                    `json:"lifecycleMessage"`
	Actions          ActionList                `json:"actions"`
	IsEnabled        bool                      `json:"isEnabled"`
}

// RuleSummaryWire is the wire form of a rule in a list.
type RuleSummaryWire struct {
	TimeCreated    time.Time                 `json:"timeCreated"`
	DefinedTags    map[string]map[string]any `json:"definedTags,omitempty"`
	FreeformTags   map[string]string         `json:"freeformTags,omitempty"`
	ID             string                    `json:"id"`
	DisplayName    string                    `json:"displayName"`
	Description    string                    `json:"description,omitempty"`
	Condition      string                    `json:"condition"`
	CompartmentID  string                    `json:"compartmentId"`
	LifecycleState string                    `json:"lifecycleState"`
	IsEnabled      bool                      `json:"isEnabled"`
}

func ruleView(r *Rule) RuleWire {
	acts := make([]*Action, len(r.Actions))
	copy(acts, r.Actions)

	return RuleWire{
		ID: r.ID, DisplayName: r.DisplayName, Description: r.Description, Condition: r.Condition,
		CompartmentID: r.CompartmentID, LifecycleState: r.State, IsEnabled: r.IsEnabled,
		Actions: ActionList{Actions: acts}, TimeCreated: r.TimeCreated,
		FreeformTags: r.Tags.FreeformTags, DefinedTags: r.Tags.DefinedTags,
	}
}

func summaryView(r *Rule) RuleSummaryWire {
	return RuleSummaryWire{
		ID: r.ID, DisplayName: r.DisplayName, Description: r.Description, Condition: r.Condition,
		CompartmentID: r.CompartmentID, LifecycleState: r.State, IsEnabled: r.IsEnabled,
		TimeCreated: r.TimeCreated, FreeformTags: r.Tags.FreeformTags, DefinedTags: r.Tags.DefinedTags,
	}
}
