package iam

import (
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/iampolicystream"
)

const (
	reasonIdentityPolicy       = iampolicystream.ReasonIdentityPolicy
	reasonResourcePolicy       = iampolicystream.ReasonResourcePolicy
	reasonPermissionBoundary   = iampolicystream.ReasonPermissionBound
	reasonImplicitDeny         = iampolicystream.ReasonImplicitDeny
	reasonUnresolvedCaller     = iampolicystream.ReasonUnresolvedCaller
	reasonServiceControlPolicy = iampolicystream.ReasonServiceControl
)

// requestDecision is the outcome of evaluating one request. For a deny, reason
// names the policy type responsible; for an allow, the type that granted it.
type requestDecision struct {
	policy      *iampolicystream.PolicyRef
	reason      string
	boundaryArn string
	match       EvalMatch
	boundary    boundaryTrace
	source      decisionSource
	allow       bool
	explicit    bool
}

// record publishes dec to cfg.Recorder; a deny that an identity Allow could fix
// carries the minimal suggested statement.
func (cfg EnforcementConfig) record(
	c *echo.Context, dec requestDecision, action, resource, principal, callerName string,
) {
	if cfg.Recorder == nil {
		return
	}

	if principal == "" {
		principal = callerName
	}

	svc, op, _ := strings.Cut(action, ":")
	reqID := c.Response().Header().Get("X-Amz-Request-Id")

	if reqID == "" {
		reqID = awsid.New("iam-eval-")
	}

	ev := iampolicystream.Event{
		Time:         time.Now().UTC(),
		RequestID:    reqID,
		Service:      svc,
		Operation:    op,
		Action:       action,
		Resource:     resource,
		Principal:    principal,
		Decision:     iampolicystream.DecisionDeny,
		Reason:       dec.reason,
		MatchedSid:   dec.match.Sid,
		ConditionKey: dec.match.ConditionKeys,
		StatementIdx: dec.match.StatementIdx,
		PolicyIdx:    dec.match.PolicyIndex,
		Enforced:     !cfg.Audit,
		DenyType:     dec.denyType(),
		Policy:       dec.policy,
		Boundary:     dec.boundaryRef(),
	}

	if dec.allow {
		ev.Decision = iampolicystream.DecisionAllow
	}

	if dec.match.Matched {
		ev.MatchedBy = dec.reason
	}

	if !dec.match.Matched {
		ev.StatementIdx, ev.PolicyIdx = -1, -1
	}

	if !dec.allow && (dec.reason == reasonImplicitDeny || dec.reason == reasonUnresolvedCaller) {
		ev.Suggested = iampolicystream.StatementFor(action, resource)
	}

	cfg.Recorder.Record(ev)
}
