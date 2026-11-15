package azurearm

import (
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const logicKConditions = "conditions"

var errLogicTriggerSkipped = errors.New("azurearm: logic trigger conditions not met")

// validateLogicConditions checks the trigger conditions array
// (learn.microsoft.com/azure/logic-apps/logic-apps-workflow-actions-triggers).
func validateLogicConditions(name string, m map[string]any) error {
	raw, has := m[logicKConditions]
	if !has {
		return nil
	}

	arr, ok := raw.([]any)
	if !ok {
		return invalidTemplate("the trigger '%s' conditions must be an array", name)
	}

	for _, c := range arr {
		obj, isObj := objectOf(c)
		if !isObj || stringOf(obj["expression"]) == "" {
			return invalidTemplate("each condition of the trigger '%s' must be an object with an 'expression'", name)
		}
	}

	return nil
}

// conditionsMet evaluates the trigger's conditions; an evaluation error is returned with met false.
func (p *LogicProvider) conditionsMet(w *logicWorkflow, f logicFire, now time.Time) (bool, error) {
	arr, _ := w.triggerDef(f.trigger)[logicKConditions].([]any)
	if len(arr) == 0 {
		return true, nil
	}

	scratch := &LogicProvider{mu: lockmetrics.New("azurearm.logiccondition"), clock: p.clock}
	rs := &logicRunState{
		p: scratch, params: effectiveLogicParams(w), vars: map[string]*logicVar{}, def: w.Definition,
		clock: p.clock.Now, wfKey: w.key(), wfName: w.Name,
		trigger: map[string]any{
			fieldName: f.trigger, logicKOutputs: f.outputs,
			logicKStartTime: logicTime(now), logicKEndTime: logicTime(now),
			logicKScheduledTime: logicTime(f.scheduled), logicKStatus: logicStSucceeded,
		},
	}

	for _, c := range arr {
		obj, _ := objectOf(c)

		v, err := evalLogicValue(stringOf(obj["expression"]), rs.evalCtx(nil))
		if err != nil {
			return false, err
		}

		ok, isBool := v.(bool)
		if !isBool {
			return false, exprErr("the condition '%s' did not evaluate to a boolean", stringOf(obj["expression"]))
		}

		if !ok {
			return false, nil
		}
	}

	return true, nil
}

// recordSkippedTrigger adds a trigger history entry for a firing that started no run.
func (p *LogicProvider) recordSkippedTrigger(w *logicWorkflow, f logicFire, now time.Time, cause error) {
	h := &logicHistory{
		Name: p.newRunName(now), Trigger: f.trigger, Start: now, End: now, Scheduled: f.scheduled,
		Status: logicStSkipped, Tracking: uuid.NewString(),
		Outputs: logicContent{Value: f.outputs, Set: true},
	}

	if cause != nil {
		h.Status, h.Code, h.Error = logicStFailed, "TriggerConditionError", map[string]any{fieldMessage: cause.Error()}
	}

	if f.hasInputs {
		h.Inputs = logicContent{Value: f.inputs, Set: true}
	}

	w.Histories = append(w.Histories, h)
	trimLogicHistory(w)
}
