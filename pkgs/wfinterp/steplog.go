package wfinterp

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"
)

// Step entry states.
const (
	StepInProgress = "STATE_IN_PROGRESS"
	StepSucceeded  = "STATE_SUCCEEDED"
	StepFailed     = "STATE_FAILED"
	StepCancelled  = "STATE_CANCELLED"
)

// Step types, as in google.cloud.workflows.executions.v1.StepEntry.StepType.
const (
	StepTypeAssign        = "STEP_ASSIGN"
	StepTypeStdLibCall    = "STEP_STD_LIB_CALL"
	StepTypeConnectorCall = "STEP_CONNECTOR_CALL"
	StepTypeSubworkflow   = "STEP_SUBWORKFLOW_CALL"
	StepTypeSwitch        = "STEP_SWITCH"
	StepTypeFor           = "STEP_FOR"
	StepTypeParallelFor   = "STEP_PARALLEL_FOR"
	StepTypeParallel      = "STEP_PARALLEL_BRANCH"
	StepTypeTry           = "STEP_TRY_RETRY_EXCEPT"
	StepTypeReturn        = "STEP_RETURN"
	StepTypeRaise         = "STEP_RAISE"
	StepTypeGoto          = "STEP_GOTO"
)

// StepEvent reports a step starting (State StepInProgress) or finishing; both carry the same ID.
type StepEvent struct {
	Time time.Time
	// Vars holds the variables visible to the step when it finished, only when Host.StepVariables is set.
	Vars      map[string]any
	State     string
	Routine   string
	Step      string
	Type      string
	Exception string
	ID        int64
	// Parent is the entry of the step that encloses this one or called its subworkflow; 0 at the top.
	Parent int64
	// Prev is the entry of the step that ran just before this one in the same block; 0 for the first.
	Prev   int64
	Thread int
}

func (r *rctx) stepType(s *step) string {
	switch {
	case len(s.assigns) > 0:
		return StepTypeAssign
	case s.call != nil:
		switch fn := s.call.fn; {
		case r.x.prog.subs[fn] != nil:
			return StepTypeSubworkflow
		case strings.HasPrefix(fn, "googleapis."):
			return StepTypeConnectorCall
		default:
			return StepTypeStdLibCall
		}
	case s.hasSwitch:
		return StepTypeSwitch
	case s.loop != nil:
		return StepTypeFor
	case s.par != nil:
		if s.par.loop != nil {
			return StepTypeParallelFor
		}

		return StepTypeParallel
	case s.try != nil:
		return StepTypeTry
	case s.hasRaise:
		return StepTypeRaise
	case s.hasRet:
		return StepTypeReturn
	case s.next != "":
		return StepTypeGoto
	default:
		return ""
	}
}

func (r *rctx) loggedStep(sc *scope, s *step) (outcome, error) {
	h := r.x.host

	logOn, vars := true, h.StepVariables
	if h.StepBudget != nil {
		var wantVars bool

		logOn, wantVars = h.StepBudget()
		vars = vars && wantVars
	}

	if !logOn {
		return r.execStep(sc, s)
	}

	id := r.x.entry.Add(1)
	parent, prev := r.cur, r.prev

	ev := StepEvent{
		ID: id, Parent: parent, Prev: prev, Thread: r.tid, Routine: r.routine.name, Step: s.name,
		Type: r.stepType(s), State: StepInProgress, Time: h.clock().Now(),
	}
	h.StepLog(ev)

	r.cur = id
	out, err := r.execStep(sc, s)
	r.cur, r.prev = parent, id

	ev.State, ev.Time = StepSucceeded, h.clock().Now()

	if err != nil {
		ev.State, ev.Exception = StepFailed, errorPayload(err)

		var werr *Error
		if errors.Is(err, context.Canceled) || (errors.As(err, &werr) && werr.Cancelled()) {
			ev.State = StepCancelled
		}
	}

	if vars {
		ev.Vars = sc.snapshot()
	}

	h.StepLog(ev)

	return out, err
}

func errorPayload(err error) string {
	if werr, ok := errors.AsType[*Error](err); ok {
		return werr.Payload()
	}

	raw, jerr := ToJSON(err.Error())
	if jerr != nil {
		return typeNull
	}

	return string(raw)
}

// snapshot copies every variable visible from s, inner scopes shadowing outer ones.
func (s *scope) snapshot() map[string]any {
	var chain []*scope

	for cur := s; cur != nil; cur = cur.parent {
		chain = append(chain, cur)
	}

	out := map[string]any{}

	for _, c := range slices.Backward(chain) {
		for k, v := range c.vars {
			out[k] = deepCopy(v)
		}
	}

	return out
}
