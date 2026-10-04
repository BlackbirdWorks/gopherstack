package iot

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/roleauth"
)

const (
	queueURLSegments = 2
	keyRuleName      = "ruleName"
)

var errRuleActionDenied = errors.New("rule action not authorized")

// SetRoleAuthorizer makes rule actions run under the action's roleArn (SQS) or the target
// function's resource policy (Lambda).
func (b *InMemoryBackend) SetRoleAuthorizer(a roleauth.Authorizer) {
	b.mu.Lock("SetRoleAuthorizer")
	defer b.mu.Unlock()

	b.roleAuth = a
}

func (b *InMemoryBackend) ruleAuthorizer() roleauth.Authorizer {
	b.mu.RLock("ruleAuthorizer")
	defer b.mu.RUnlock()

	return b.roleAuth
}

// dispatchActions runs each action of a matched rule, routing a failed action to the rule's errorAction.
func (h *ruleHook) dispatchActions(rule *TopicRule, dispatcher RuleDispatcher, msg *ruleMessage) {
	for _, action := range rule.Actions {
		name, err := h.runAction(rule, action, dispatcher, msg)
		if err == nil || name == "" {
			continue
		}

		logger.Load(h.ctx).Error("iot rule action failed", "rule", rule.RuleName, "action", name, "error", err)

		h.runErrorAction(rule, dispatcher, msg, name, err)
	}
}

// runAction runs one action and returns its IoT action name and any failure.
func (h *ruleHook) runAction(
	rule *TopicRule, action RuleAction, dispatcher RuleDispatcher, msg *ruleMessage,
) (string, error) {
	switch {
	case action.SQS != nil:
		if dispatcher == nil {
			return "", nil
		}

		if err := h.authorizeSQSAction(action.SQS); err != nil {
			return "SqsAction", err
		}

		return "SqsAction", dispatcher.SendToSQS(msg.region, action.SQS.QueueURL, string(msg.payload))
	case action.Lambda != nil:
		if dispatcher == nil {
			return "", nil
		}

		auth := h.backend.ruleAuthorizer()
		denied := roleauth.AuthorizeResource(auth, roleauth.PrincipalIoT, "lambda:InvokeFunction",
			action.Lambda.FunctionARN, rule.ARN)
		if denied != nil {
			return "LambdaAction", fmt.Errorf("%w: %w", errRuleActionDenied, denied)
		}

		return "LambdaAction", dispatcher.InvokeLambda(h.ctx, action.Lambda.FunctionARN, msg.payload)
	case action.SNS != nil:
		return "SnsAction", h.runSNS(rule, action.SNS, msg)
	default:
		return h.runOtherAction(rule, action, msg)
	}
}

func (h *ruleHook) authorizeSQSAction(a *SQSAction) error {
	auth := h.backend.ruleAuthorizer()
	if auth == nil {
		return nil
	}

	queueARN := h.backend.queueARNFromURL(a.QueueURL)
	if err := auth.AuthorizeRole(roleauth.PrincipalIoT, a.RoleARN, "sqs:SendMessage", queueARN); err != nil {
		return fmt.Errorf("%w: %w", errRuleActionDenied, err)
	}

	return nil
}

// queueARNFromURL maps a <scheme>://<host>/<account>/<queue> URL to its queue ARN.
func (b *InMemoryBackend) queueARNFromURL(queueURL string) string {
	segments := strings.Split(strings.TrimRight(queueURL, "/"), "/")
	if len(segments) < queueURLSegments {
		return queueURL
	}

	return arn.Build("sqs", b.region, segments[len(segments)-2], segments[len(segments)-1])
}

// runErrorAction delivers the documented error envelope to the rule's errorAction.
func (h *ruleHook) runErrorAction(
	rule *TopicRule, dispatcher RuleDispatcher, msg *ruleMessage, failedAction string, cause error,
) {
	if rule.ErrorAction == nil {
		return
	}

	body, err := json.Marshal(map[string]string{
		keyRuleName:             rule.RuleName,
		"topic":                 msg.topic,
		"base64OriginalPayload": base64.StdEncoding.EncodeToString(msg.original),
		"failedAction":          failedAction,
		"failedActionReason":    "Failed to run action. Message: " + cause.Error(),
	})
	if err != nil {
		return
	}

	errMsg := *msg
	errMsg.payload = body

	if _, runErr := h.runAction(rule, *rule.ErrorAction, dispatcher, &errMsg); runErr != nil {
		logger.Load(h.ctx).Error("iot error action failed", "rule", rule.RuleName, "error", runErr)
	}
}
