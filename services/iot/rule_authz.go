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
func (h *ruleHook) dispatchActions(rule *TopicRule, dispatcher RuleDispatcher, topic string, payload []byte) {
	if dispatcher == nil {
		return
	}

	for _, action := range rule.Actions {
		name, err := h.runAction(rule, action, dispatcher, payload)
		if err == nil || name == "" {
			continue
		}

		logger.Load(h.ctx).Error("iot rule action failed", "rule", rule.RuleName, "action", name, "error", err)

		h.runErrorAction(rule, dispatcher, topic, payload, name, err)
	}
}

// runAction runs one action and returns its IoT action name and any failure.
func (h *ruleHook) runAction(
	rule *TopicRule, action RuleAction, dispatcher RuleDispatcher, payload []byte,
) (string, error) {
	switch {
	case action.SQS != nil:
		if err := h.authorizeSQSAction(action.SQS); err != nil {
			return "SqsAction", err
		}

		return "SqsAction", dispatcher.SendToSQS(action.SQS.QueueURL, string(payload))
	case action.Lambda != nil:
		auth := h.backend.ruleAuthorizer()
		denied := roleauth.AuthorizeResource(auth, roleauth.PrincipalIoT, "lambda:InvokeFunction",
			action.Lambda.FunctionARN, rule.ARN)
		if denied != nil {
			return "LambdaAction", fmt.Errorf("%w: %w", errRuleActionDenied, denied)
		}

		return "LambdaAction", dispatcher.InvokeLambda(h.ctx, action.Lambda.FunctionARN, payload)
	default:
		return "", nil
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
	rule *TopicRule, dispatcher RuleDispatcher, topic string, payload []byte, failedAction string, cause error,
) {
	if rule.ErrorAction == nil {
		return
	}

	body, err := json.Marshal(map[string]string{
		keyRuleName:             rule.RuleName,
		"topic":                 topic,
		"base64OriginalPayload": base64.StdEncoding.EncodeToString(payload),
		"failedAction":          failedAction,
		"failedActionReason":    "Failed to run action. Message: " + cause.Error(),
	})
	if err != nil {
		return
	}

	if _, runErr := h.runAction(rule, *rule.ErrorAction, dispatcher, body); runErr != nil {
		logger.Load(h.ctx).Error("iot error action failed", "rule", rule.RuleName, "error", runErr)
	}
}
