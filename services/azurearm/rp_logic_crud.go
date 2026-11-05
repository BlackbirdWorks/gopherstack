package azurearm

import (
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"time"
)

var logicExtraProps = []string{ //nolint:gochecknoglobals // static property list
	"accessControl", "endpointsConfiguration", "integrationAccount", "integrationServiceEnvironment", logicKSKU,
}

type logicPutInput struct {
	definition map[string]any
	parameters map[string]any
	extra      map[string]any
	identity   map[string]any
	tags       map[string]string
	location   string
	state      string
	hasTags    bool
	hasDef     bool
	hasParams  bool
}

func parseLogicPut(body map[string]any, patch bool) (logicPutInput, error) {
	in := logicPutInput{location: stringOf(body[fieldLocation])}

	if !patch && in.location == "" {
		return in, logicBadRequest("LocationRequired", "The location property is required for this definition.")
	}

	props, ok := objectOf(body["properties"])
	if !ok && !patch {
		return in, logicBadRequest("InvalidRequestContent", "The 'properties' property is required.")
	}

	if err := in.parseDefinition(props, patch); err != nil {
		return in, err
	}

	if pr, has := objectOf(props["parameters"]); has {
		in.parameters, in.hasParams = pr, true
	}

	state, err := parseLogicState(stringOf(props[logicKState]))
	if err != nil {
		return in, err
	}

	in.state = state
	in.extra = map[string]any{}

	for _, k := range logicExtraProps {
		if v, has := props[k]; has {
			in.extra[k] = v
		}
	}

	if t, has := objectOf(body["tags"]); has {
		in.hasTags, in.tags = true, map[string]string{}
		for k, v := range t {
			in.tags[k] = stringOf(v)
		}
	}

	in.identity, _ = objectOf(body["identity"])

	return in, nil
}

func (in *logicPutInput) parseDefinition(props map[string]any, patch bool) error {
	def, has := props["definition"]
	if !has {
		if patch {
			return nil
		}

		return logicBadRequest("InvalidRequestContent", "The 'definition' property is required.")
	}

	d, isObj := objectOf(def)
	if !isObj {
		return invalidTemplate("the workflow definition must be an object")
	}

	in.definition, in.hasDef = d, true

	return nil
}

func parseLogicState(st string) (string, error) {
	switch {
	case st == "":
		return "", nil
	case strings.EqualFold(st, logicStateEnabled):
		return logicStateEnabled, nil
	case strings.EqualFold(st, logicStateDisabled):
		return logicStateDisabled, nil
	}

	return "", logicBadRequest(
		"InvalidRequestContent",
		"The workflow state '%s' is not valid; use Enabled or Disabled.",
		st,
	)
}

// putWorkflow creates or updates a workflow and returns its wire body and whether it was created.
func (p *LogicProvider) putWorkflow(id ResourceID, body map[string]any, patch bool, base string,
) (map[string]any, bool, error) {
	if len(id.Names) != 1 || !strings.EqualFold(id.Types[0], logicWorkflowsType) {
		return nil, false, errUnsupportedLogicType(id)
	}

	if err := validateLogicWorkflowName(id.Names[0]); err != nil {
		return nil, false, err
	}

	in, err := parseLogicPut(body, patch)
	if err != nil {
		return nil, false, err
	}

	if in.hasDef {
		if err = validateLogicDefinition(in.definition); err != nil {
			return nil, false, err
		}
	}

	p.mu.Lock("putWorkflow")
	defer p.mu.Unlock()

	key := logicKey(id.SubscriptionID, id.ResourceGroup, id.Names[0])
	now := p.clock.Now().UTC()
	w, exists := p.wf[key]

	switch {
	case !exists && patch:
		return nil, false, ErrLogicWorkflowNotFound
	case !exists:
		w = p.newWorkflow(id, in, now)
		p.wf[key] = w
	default:
		p.applyLogicUpdate(w, in, patch, now)
	}

	p.reconcileTriggersLocked(w, now)
	p.armWorkflowLocked(w)

	return p.workflowBody(w, base), !exists, nil
}

func errUnsupportedLogicType(id ResourceID) error {
	return fmt.Errorf("%w: unsupported Microsoft.Logic resource type %q", ErrResourceNotFound, id.ResourceType())
}

func validateLogicWorkflowName(name string) error {
	if name == "" || len(name) > logicNameMaxLen || !logicNameRE.MatchString(name) {
		return logicBadRequest("InvalidResourceName", "The workflow name '%s' is not valid.", name)
	}

	return nil
}

func (p *LogicProvider) newWorkflow(id ResourceID, in logicPutInput, now time.Time) *logicWorkflow {
	w := &logicWorkflow{
		Name: id.Names[0], Subscription: id.SubscriptionID, ResourceGrp: id.ResourceGroup, Location: in.location,
		FlowID: randomLogicHex(logicIDBytes), State: logicStateEnabled, Created: now, Changed: now,
		KeyPrimary: randomLogicToken(), KeySecondary: randomLogicToken(),
		Definition: in.definition, Parameters: in.parameters, Tags: in.tags, Identity: in.identity, Extra: in.extra,
		Triggers: map[string]*logicTriggerState{},
	}

	if in.state != "" {
		w.State = in.state
	}

	w.Version = p.newVersionID(now)
	w.Versions = []*logicVersion{
		{Version: w.Version, Created: now, Definition: in.definition, Parameters: in.parameters},
	}

	return w
}

func (p *LogicProvider) applyLogicUpdate(w *logicWorkflow, in logicPutInput, patch bool, now time.Time) {
	changed := false

	if in.location != "" {
		w.Location = in.location
	}

	if in.hasDef && !reflect.DeepEqual(normLogic(in.definition), normLogic(w.Definition)) {
		w.Definition, changed = in.definition, true
	}

	if (in.hasParams || !patch) && !reflect.DeepEqual(normLogic(in.parameters), normLogic(w.Parameters)) {
		w.Parameters, changed = in.parameters, true
	}

	if in.state != "" {
		w.State = in.state
	}

	applyLogicMeta(w, in, patch)
	w.Changed = now

	if changed {
		w.Version = p.newVersionID(now)
		w.Versions = append(w.Versions, &logicVersion{
			Version: w.Version, Created: now, Definition: w.Definition, Parameters: w.Parameters,
		})

		if len(w.Versions) > logicMaxVersions {
			w.Versions = w.Versions[len(w.Versions)-logicMaxVersions:]
		}
	}
}

func applyLogicMeta(w *logicWorkflow, in logicPutInput, patch bool) {
	if in.hasTags || !patch {
		w.Tags = in.tags
	}

	if len(in.extra) > 0 || !patch {
		w.Extra = in.extra
	}

	if in.identity != nil {
		w.Identity = in.identity
	}
}

// reconcileTriggersLocked aligns trigger state with the definition: new triggers start enabled, removed ones go.
func (p *LogicProvider) reconcileTriggersLocked(w *logicWorkflow, now time.Time) {
	defs, _ := objectOf(w.Definition[logicSegTriggers])

	for name := range w.Triggers {
		if _, ok := defs[name]; !ok {
			delete(w.Triggers, name)
		}
	}

	for name := range defs {
		if _, ok := w.Triggers[name]; !ok {
			w.Triggers[name] = &logicTriggerState{Created: now, Changed: now, State: logicStateEnabled, Anchor: now}
		}
	}
}

func logicRef(id, name, typ string) map[string]any {
	return map[string]any{"id": id, fieldName: name, fieldType: typ}
}

func logicTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func (p *LogicProvider) workflowBody(w *logicWorkflow, base string) map[string]any {
	props := map[string]any{
		fieldProvisioningState: logicStSucceeded,
		logicKCreatedTime:      logicTime(w.Created),
		logicKChangedTime:      logicTime(w.Changed),
		logicKState:            w.State,
		logicKVersion:          w.Version,
		"definition":           w.Definition,
		"endpointsConfiguration": map[string]any{
			logicKWorkflow: map[string]any{"outgoingIpAddresses": []any{}, "accessEndpointIpAddresses": []any{}},
			"connector":    map[string]any{"outgoingIpAddresses": []any{}},
		},
	}

	if base != "" {
		props["accessEndpoint"] = base + logicDataPrefix + w.FlowID
	}

	if w.Parameters != nil {
		props["parameters"] = w.Parameters
	}

	maps.Copy(props, w.Extra)

	out := map[string]any{
		logicFieldID: w.armID(), fieldName: w.Name, fieldType: namespaceMicrosoftLogic + "/" + logicWorkflowsType,
		fieldLocation: w.Location, fieldProperties: props,
	}

	if len(w.Tags) > 0 {
		out["tags"] = cloneStringMap(w.Tags)
	}

	if w.Identity != nil {
		out["identity"] = logicIdentityBody(w.Identity)
	}

	return out
}

func logicIdentityBody(in map[string]any) map[string]any {
	return maps.Clone(in)
}

func (p *LogicProvider) versionBody(w *logicWorkflow, v *logicVersion, base string) map[string]any {
	props := map[string]any{
		fieldProvisioningState: logicStSucceeded,
		logicKCreatedTime:      logicTime(v.Created),
		logicKChangedTime:      logicTime(v.Created),
		logicKState:            w.State,
		logicKVersion:          v.Version,
		"definition":           v.Definition,
	}

	if v.Parameters != nil {
		props["parameters"] = v.Parameters
	}

	if base != "" {
		props["accessEndpoint"] = base + logicDataPrefix + w.FlowID
	}

	return map[string]any{
		logicFieldID: w.armID() + "/versions/" + v.Version, fieldName: v.Version,
		fieldType: namespaceMicrosoftLogic + "/" + logicWorkflowsType + "/versions", fieldLocation: w.Location,
		fieldProperties: props,
	}
}

func (w *logicWorkflow) ref() map[string]any {
	return logicRef(w.armID(), w.Name, namespaceMicrosoftLogic+"/"+logicWorkflowsType)
}

func (w *logicWorkflow) triggerType(name string) string {
	defs, _ := objectOf(w.Definition[logicSegTriggers])

	return typeOf(defs[name])
}

func (p *LogicProvider) triggerBody(w *logicWorkflow, name string) map[string]any {
	ts := w.Triggers[name]
	defs, _ := objectOf(w.Definition[logicSegTriggers])
	def, _ := objectOf(defs[name])

	state := ts.State
	if w.State != logicStateEnabled {
		state = logicStateDisabled
	}

	props := map[string]any{
		fieldProvisioningState: logicStSucceeded,
		logicKCreatedTime:      logicTime(ts.Created),
		logicKChangedTime:      logicTime(ts.Changed),
		logicKState:            state,
		logicKWorkflow:         w.ref(),
	}

	if ts.Last != nil {
		props["lastExecutionTime"] = logicTime(*ts.Last)
	}

	if ts.Next != nil && state == logicStateEnabled {
		props["nextExecutionTime"] = logicTime(*ts.Next)
	}

	if rc, ok := objectOf(def["recurrence"]); ok {
		props["recurrence"] = rc
	}

	return map[string]any{
		logicFieldID: w.armID() + "/triggers/" + name, fieldName: name,
		fieldType: namespaceMicrosoftLogic + "/" + logicWorkflowsType + "/triggers", fieldProperties: props,
	}
}

func marshalLogicSize(v any) int64 {
	b, err := json.Marshal(v)
	if err != nil {
		return 0
	}

	return int64(len(b))
}
