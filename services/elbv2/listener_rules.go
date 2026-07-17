package elbv2

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

func (b *InMemoryBackend) ruleARN(listenerArn, idx string) string {
	// Extract the load-balancer/listener path from the listener ARN resource to build
	// a rule ARN in the standard form: listener-rule/app/<lb-name>/<lb-id>/<listener-id>/<rule-id>.
	// The listener ARN resource looks like: listener/app/<lb-name>/<lb-id>/<listener-port>.
	resource := "listener-rule/app/lb/0123456789abcdef/0000000000000000/" + idx
	if listenerArn != "" {
		if i := strings.Index(listenerArn, ":listener/"); i >= 0 {
			path := listenerArn[i+len(":"):]
			resource = "listener-rule/" + strings.TrimPrefix(path, "listener/") + "/" + idx
		}
	}

	return arn.Build("elasticloadbalancing", b.region, b.accountID, resource)
}

// checkAllRuleArnsFound returns ErrRuleNotFound if any queried ARN is absent from result.
func checkAllRuleArnsFound(arns []string, result []Rule) error {
	for _, a := range arns {
		found := false
		for _, r := range result {
			if r.RuleArn == a {
				found = true

				break
			}
		}

		if !found {
			return ErrRuleNotFound
		}
	}

	return nil
}

// syncDefaultRuleActions updates the default rule's actions to match the listener's new default actions.
// Caller must hold b.mu (write).
func (b *InMemoryBackend) syncDefaultRuleActions(listenerArn string, actions []Action) {
	for _, r := range b.rulesByListener.Get(listenerArn) {
		if r.IsDefault {
			actsCopy := make([]Action, len(actions))
			copy(actsCopy, actions)
			r.Actions = actsCopy

			break
		}
	}
}

// CreateRule creates a new rule on a listener.
func (b *InMemoryBackend) CreateRule(input CreateRuleInput) (*Rule, error) {
	b.mu.Lock("CreateRule")
	defer b.mu.Unlock()

	if _, ok := b.listeners.Get(input.ListenerArn); !ok {
		return nil, ErrListenerNotFound
	}

	// Validate and check for duplicate priority.
	if input.Priority != "" && input.Priority != priorityDefault {
		p, parseErr := strconv.ParseInt(input.Priority, 10, 32)
		if parseErr != nil || p < 1 || p > 50000 {
			return nil, fmt.Errorf(
				"%w: priority must be an integer between 1 and 50000",
				ErrInvalidParameter,
			)
		}

		for _, r := range b.rulesByListener.Get(input.ListenerArn) {
			if r.Priority == input.Priority {
				return nil, fmt.Errorf(
					"%w: priority %s already in use",
					ErrDuplicateRulePriority,
					input.Priority,
				)
			}
		}
	}

	b.ruleCounter++
	ruleArn := b.ruleARN(input.ListenerArn, strconv.Itoa(b.ruleCounter))

	t := tags.New("elbv2.rule." + ruleArn + ".tags")
	for _, kv := range input.Tags {
		t.Set(kv.Key, kv.Value)
	}

	rule := &Rule{
		RuleArn:     ruleArn,
		ListenerArn: input.ListenerArn,
		Priority:    input.Priority,
		IsDefault:   false,
		Actions:     input.Actions,
		Conditions:  input.Conditions,
		Tags:        t,
	}

	b.rules.Put(rule)

	cp := *rule

	return &cp, nil
}

// DescribeRules returns rules filtered by listener ARN and/or rule ARNs.
//
// Fast path: when only rule ARNs are supplied (no listenerArn filter), look
// them up directly in the ARN-keyed map instead of scanning every rule.
func (b *InMemoryBackend) DescribeRules(listenerArn string, ruleArns []string) ([]Rule, error) {
	b.mu.RLock("DescribeRules")
	defer b.mu.RUnlock()

	if listenerArn == "" && len(ruleArns) > 0 {
		result := make([]Rule, 0, len(ruleArns))

		for _, a := range ruleArns {
			if r, ok := b.rules.Get(a); ok {
				result = append(result, *r)
			}
		}

		sortRulesByPriority(result)

		if err := checkAllRuleArnsFound(ruleArns, result); err != nil {
			return nil, err
		}

		return result, nil
	}

	arnSet := make(map[string]bool, len(ruleArns))
	for _, a := range ruleArns {
		arnSet[a] = true
	}

	result := make([]Rule, 0, b.rules.Len())

	for _, r := range b.rules.All() {
		if listenerArn != "" && r.ListenerArn != listenerArn {
			continue
		}

		if len(ruleArns) > 0 && !arnSet[r.RuleArn] {
			continue
		}

		result = append(result, *r)
	}

	sortRulesByPriority(result)

	return result, nil
}

// sortRulesByPriority sorts rules numerically by priority; "default" sorts last
// (highest priority number). Non-numeric priorities fall back to string compare.
func sortRulesByPriority(result []Rule) {
	sort.Slice(result, func(i, j int) bool {
		pi, pj := result[i].Priority, result[j].Priority
		if pi == pj {
			return false
		}

		if pi == priorityDefault {
			return false
		}

		if pj == priorityDefault {
			return true
		}

		ni, errI := strconv.Atoi(pi)
		nj, errJ := strconv.Atoi(pj)
		if errI != nil || errJ != nil {
			return pi < pj
		}

		return ni < nj
	})
}

// DeleteRule deletes a rule by ARN.
func (b *InMemoryBackend) DeleteRule(ruleArn string) error {
	b.mu.Lock("DeleteRule")
	defer b.mu.Unlock()

	rule, ok := b.rules.Get(ruleArn)
	if !ok {
		return ErrRuleNotFound
	}

	if rule.IsDefault {
		return fmt.Errorf(
			"%w: cannot delete the default rule of a listener",
			ErrOperationNotPermitted,
		)
	}

	rule.Tags.Close()
	b.rules.Delete(ruleArn)

	return nil
}

// ModifyRule updates the actions and/or conditions of an existing rule.
func (b *InMemoryBackend) ModifyRule(
	ruleArn string,
	actions []Action,
	conditions []Condition,
) (*Rule, error) {
	b.mu.Lock("ModifyRule")
	defer b.mu.Unlock()

	rule, ok := b.rules.Get(ruleArn)
	if !ok {
		return nil, ErrRuleNotFound
	}

	if len(actions) > 0 {
		rule.Actions = actions
	}

	if len(conditions) > 0 {
		rule.Conditions = conditions
	}

	cp := *rule

	return &cp, nil
}

// checkRulePriorityCollisions returns ErrDuplicateRulePriority when an incoming priority
// conflicts with an existing non-batch rule on the same listener.
// Callers must hold a write lock.
func (b *InMemoryBackend) checkRulePriorityCollisions(
	priorities []RulePriority,
	batchArns map[string]bool,
) error {
	incomingPriorities := make(map[string]bool, len(priorities))
	for _, p := range priorities {
		incomingPriorities[p.Priority] = true
	}

	for _, p := range priorities {
		rule, _ := b.rules.Get(p.RuleArn)
		for _, existing := range b.rulesByListener.Get(rule.ListenerArn) {
			if batchArns[existing.RuleArn] || existing.IsDefault {
				continue
			}

			if incomingPriorities[existing.Priority] {
				return fmt.Errorf(
					"%w: priority %s is already in use",
					ErrDuplicateRulePriority,
					existing.Priority,
				)
			}
		}
	}

	return nil
}

// SetRulePriorities updates the priorities of one or more rules.
func (b *InMemoryBackend) SetRulePriorities(priorities []RulePriority) ([]Rule, error) {
	b.mu.Lock("SetRulePriorities")
	defer b.mu.Unlock()

	// Check for duplicates within the request.
	seen := make(map[string]bool, len(priorities))
	for _, p := range priorities {
		if seen[p.Priority] {
			return nil, fmt.Errorf(
				"%w: priority %s specified more than once",
				ErrDuplicateRulePriority,
				p.Priority,
			)
		}

		seen[p.Priority] = true
	}

	// Validate all rules exist and none is a default rule (AWS does not allow reordering defaults).
	batchArns := make(map[string]bool, len(priorities))
	for _, p := range priorities {
		r, ok := b.rules.Get(p.RuleArn)
		if !ok {
			return nil, ErrRuleNotFound
		}

		if r.IsDefault {
			return nil, fmt.Errorf(
				"%w: cannot set priority on the default rule",
				ErrOperationNotPermitted,
			)
		}

		batchArns[p.RuleArn] = true
	}

	if err := b.checkRulePriorityCollisions(priorities, batchArns); err != nil {
		return nil, err
	}

	result := make([]Rule, 0, len(priorities))

	for _, p := range priorities {
		r, _ := b.rules.Get(p.RuleArn)
		r.Priority = p.Priority
		result = append(result, *r)
	}

	return result, nil
}
