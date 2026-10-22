package connect

import (
	"context"
	"encoding/json"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const (
	maxRuleNameLen  = 200
	maxRuleFunction = 4096
	maxRuleActions  = 25
	ruleDraft       = "DRAFT"
	rulePublished   = "PUBLISHED"
)

// ruleActionMembers maps each ActionType to the RuleAction member that carries its definition.
var ruleActionMembers = map[string]string{ //nolint:gochecknoglobals // immutable lookup table
	"CREATE_TASK":                "TaskAction",
	"ASSIGN_CONTACT_CATEGORY":    "AssignContactCategoryAction",
	"GENERATE_EVENTBRIDGE_EVENT": "EventBridgeAction",
	"SEND_NOTIFICATION":          "SendNotificationAction",
	"CREATE_CASE":                "CreateCaseAction",
	"UPDATE_CASE":                "UpdateCaseAction",
	"ASSIGN_SLA":                 "AssignSlaAction",
	"END_ASSOCIATED_TASKS":       "EndAssociatedTasksAction",
	"SUBMIT_AUTO_EVALUATION":     "SubmitAutoEvaluationAction",
	"EXTRACT_INFORMATION":        "ExtractInformationAction",
	"SEND_IN_APP_NOTIFICATION":   "SendInAppNotificationAction",
}

var ruleEventSources = []string{ //nolint:gochecknoglobals // immutable lookup table
	"OnPostCallAnalysisAvailable", "OnRealTimeCallAnalysisAvailable", "OnRealTimeChatAnalysisAvailable",
	"OnPostChatAnalysisAvailable", "OnAfterCallWorkAvailable", "OnAfterChatWorkAvailable",
	"OnEmailAnalysisAvailable", "OnZendeskTicketCreate", "OnZendeskTicketStatusUpdate", "OnSalesforceCaseCreate",
	"OnContactEvaluationSubmit", "OnMetricDataUpdate", "OnCaseCreate", "OnCaseUpdate", "OnSlaBreach",
	"OnAlertUpdate", "OnSchedulePublish", "OnScheduleUpdate", "OnScheduleTimeOffRequestActivity",
}

// RuleTriggerEventSource is the event a rule reacts to.
type RuleTriggerEventSource struct {
	EventSourceName          string `json:"EventSourceName"`
	IntegrationAssociationID string `json:"IntegrationAssociationId,omitempty"`
}

// Rule is a Connect rule.
type Rule struct {
	CreatedTime          awstime.EpochTime       `json:"CreatedTime"`
	LastUpdatedTime      awstime.EpochTime       `json:"LastUpdatedTime"`
	TriggerEventSource   *RuleTriggerEventSource `json:"TriggerEventSource"`
	Tags                 map[string]string       `json:"Tags,omitempty"`
	Function             string                  `json:"Function"`
	LastUpdatedBy        string                  `json:"LastUpdatedBy"`
	Name                 string                  `json:"Name"`
	PublishStatus        string                  `json:"PublishStatus"`
	RuleArn              string                  `json:"RuleArn"`
	RuleID               string                  `json:"RuleId"`
	Actions              []json.RawMessage       `json:"Actions"`
	PreEvaluationFilters json.RawMessage         `json:"PreEvaluationFilters,omitempty"`
}

// ActionSummary is the list shape of a rule action.
type ActionSummary struct {
	ActionType string `json:"ActionType"`
}

// RuleSummary is the list shape of a Rule.
type RuleSummary struct {
	CreatedTime     awstime.EpochTime `json:"CreatedTime"`
	LastUpdatedTime awstime.EpochTime `json:"LastUpdatedTime"`
	EventSourceName string            `json:"EventSourceName"`
	Name            string            `json:"Name"`
	PublishStatus   string            `json:"PublishStatus"`
	RuleArn         string            `json:"RuleArn"`
	RuleID          string            `json:"RuleId"`
	ActionSummaries []ActionSummary   `json:"ActionSummaries"`
}

type ruleRec struct {
	InstanceID string `json:"instanceId"`
	Rule       Rule   `json:"rule"`
}

// CreateRuleInput is the CreateRule request.
type CreateRuleInput struct {
	Tags               map[string]string
	TriggerEventSource *RuleTriggerEventSource
	Name               string
	Function           string
	PublishStatus      string
	ClientToken        string
	instPath
	PreEvaluationFilters json.RawMessage
	Actions              []json.RawMessage
}

// UpdateRuleInput is the UpdateRule request.
type UpdateRuleInput struct {
	Name          string
	Function      string
	PublishStatus string
	RuleID        string `json:"-" path:"RuleId"`
	instPath
	PreEvaluationFilters json.RawMessage
	Actions              []json.RawMessage
}

type ruleOut struct {
	RuleArn string `json:"RuleArn"`
	RuleID  string `json:"RuleId"`
}

func ruleActionType(raw json.RawMessage) (string, error) {
	var a map[string]json.RawMessage
	if err := json.Unmarshal(raw, &a); err != nil {
		return "", invalidParamErr("Actions contains an invalid action")
	}

	var t string
	if err := json.Unmarshal(a["ActionType"], &t); err != nil {
		return "", invalidParamErr("every action needs an ActionType")
	}

	member, ok := ruleActionMembers[t]
	if !ok {
		return "", invalidParamErr("ActionType %q is not valid", t)
	}

	if v, present := a[member]; !present || string(v) == jsonNull {
		return "", invalidParamErr("ActionType %s requires %s", t, member)
	}

	return t, nil
}

func (b *Backend) checkRuleActions(inst string, actions []json.RawMessage) error {
	if len(actions) == 0 || len(actions) > maxRuleActions {
		return invalidParamErr("Actions must contain 1-%d actions", maxRuleActions)
	}

	for _, raw := range actions {
		t, err := ruleActionType(raw)
		if err != nil {
			return err
		}

		if t == "CREATE_TASK" {
			if err = b.checkTaskActionFlow(inst, raw); err != nil {
				return err
			}
		}
	}

	return nil
}

func (b *Backend) checkTaskActionFlow(inst string, raw json.RawMessage) error {
	var a struct {
		TaskAction struct {
			ContactFlowID string `json:"ContactFlowId"`
		} `json:"TaskAction"`
	}

	if err := json.Unmarshal(raw, &a); err != nil {
		return invalidParamErr("TaskAction is not valid")
	}

	if !b.flows.has(inst, a.TaskAction.ContactFlowID) {
		return notFoundErr("ContactFlow", a.TaskAction.ContactFlowID)
	}

	return nil
}

func (b *Backend) checkRuleFields(name, fn, status string) error {
	return firstError(
		checkName("Name", name, maxRuleNameLen),
		checkName("Function", fn, maxRuleFunction),
		oneOf("PublishStatus", status, ruleDraft, rulePublished),
	)
}

func (b *Backend) checkRuleSource(inst string, src *RuleTriggerEventSource) error {
	if src == nil {
		return invalidParamErr("TriggerEventSource is required")
	}

	if err := oneOf("TriggerEventSource.EventSourceName", src.EventSourceName, ruleEventSources...); err != nil {
		return err
	}

	needsAssoc := slices.Contains(
		[]string{"OnZendeskTicketCreate", "OnZendeskTicketStatusUpdate", "OnSalesforceCaseCreate"}, src.EventSourceName)
	if needsAssoc && src.IntegrationAssociationID == "" {
		return invalidParamErr("IntegrationAssociationId is required for %s", src.EventSourceName)
	}

	if src.IntegrationAssociationID != "" && !b.integrations.has(inst, src.IntegrationAssociationID) {
		return notFoundErr("IntegrationAssociation", src.IntegrationAssociationID)
	}

	return nil
}

func ruleNameTaken(b *Backend, inst, name, except string) bool {
	return nameExists(b.rules, inst, func(r *ruleRec) string { return r.Rule.Name }, name, except)
}

// CreateRule creates a rule.
func (b *Backend) CreateRule(ctx context.Context, in *CreateRuleInput) (*ruleOut, error) {
	if err := firstError(b.checkRuleFields(in.Name, in.Function, in.PublishStatus), checkTags(in.Tags)); err != nil {
		return nil, err
	}

	return writeInstance(b, "CreateRule", in.InstanceID, func(inst string) (*ruleOut, error) {
		if err := firstError(
			b.checkRuleSource(inst, in.TriggerEventSource),
			b.checkRuleActions(inst, in.Actions),
		); err != nil {
			return nil, err
		}

		if ruleNameTaken(b, inst, in.Name, "") {
			return nil, resourceConflictErr("Rule %q already exists", in.Name)
		}

		id := newID()
		now := awstime.NewEpochTime(b.now())
		rec := &ruleRec{InstanceID: inst, Rule: Rule{
			CreatedTime: now, LastUpdatedTime: now, TriggerEventSource: in.TriggerEventSource,
			Tags: cloneTags(in.Tags), Actions: in.Actions, PreEvaluationFilters: in.PreEvaluationFilters,
			Function: in.Function, LastUpdatedBy: b.callerARN(ctx), Name: in.Name, PublishStatus: in.PublishStatus,
			RuleArn: b.buildARN(ctx, "instance/"+inst+"/rule/"+id), RuleID: id,
		}}
		b.rules.put(rec)

		return &ruleOut{RuleArn: rec.Rule.RuleArn, RuleID: id}, nil
	})
}

// DescribeRule returns a rule.
func (b *Backend) DescribeRule(inst, id string) (*Rule, error) {
	return readInstance(b, "DescribeRule", inst, func(inst string) (*Rule, error) {
		r, err := b.rules.get(inst, id)
		if err != nil {
			return nil, err
		}

		out := r.Rule
		out.Tags = cloneTags(out.Tags)

		return &out, nil
	})
}

// ListRules pages through rules, optionally filtered by event source and publish status.
func (b *Backend) ListRules(inst, source, status, token string, maxResults int32) ([]RuleSummary, string, error) {
	if status != "" {
		if err := oneOf("publishStatus", status, ruleDraft, rulePublished); err != nil {
			return nil, "", err
		}
	}

	if source != "" {
		if err := oneOf("eventSourceName", source, ruleEventSources...); err != nil {
			return nil, "", err
		}
	}

	return listIn(b, "ListRules", inst, b.rules, func(r *ruleRec) (RuleSummary, bool) {
		if (source != "" && r.Rule.TriggerEventSource.EventSourceName != source) ||
			(status != "" && r.Rule.PublishStatus != status) {
			return RuleSummary{}, false
		}

		return ruleSummary(&r.Rule), true
	}, token, maxResults)
}

func ruleSummary(r *Rule) RuleSummary {
	s := RuleSummary{
		CreatedTime: r.CreatedTime, LastUpdatedTime: r.LastUpdatedTime, Name: r.Name,
		PublishStatus: r.PublishStatus, RuleArn: r.RuleArn, RuleID: r.RuleID,
		EventSourceName: r.TriggerEventSource.EventSourceName, ActionSummaries: []ActionSummary{},
	}

	for _, raw := range r.Actions {
		t, _ := ruleActionType(raw)
		s.ActionSummaries = append(s.ActionSummaries, ActionSummary{ActionType: t})
	}

	return s
}

// UpdateRule replaces the definition of a rule.
func (b *Backend) UpdateRule(ctx context.Context, in *UpdateRuleInput) (*empty, error) {
	if err := b.checkRuleFields(in.Name, in.Function, in.PublishStatus); err != nil {
		return nil, err
	}

	return writeInstance(b, "UpdateRule", in.InstanceID, func(inst string) (*empty, error) {
		r, err := b.rules.get(inst, in.RuleID)
		if err != nil {
			return nil, err
		}

		if err = b.checkRuleActions(inst, in.Actions); err != nil {
			return nil, err
		}

		if ruleNameTaken(b, inst, in.Name, in.RuleID) {
			return nil, resourceConflictErr("Rule %q already exists", in.Name)
		}

		r.Rule.Name, r.Rule.Function, r.Rule.PublishStatus = in.Name, in.Function, in.PublishStatus
		r.Rule.Actions, r.Rule.PreEvaluationFilters = in.Actions, in.PreEvaluationFilters
		r.Rule.LastUpdatedTime = awstime.NewEpochTime(b.now())
		r.Rule.LastUpdatedBy = b.callerARN(ctx)

		return &empty{}, nil
	})
}

// DeleteRule removes a rule.
func (b *Backend) DeleteRule(inst, id string) (*empty, error) {
	return writeInstance(b, "DeleteRule", inst, func(inst string) (*empty, error) {
		return nil, b.rules.del(inst, id)
	})
}
