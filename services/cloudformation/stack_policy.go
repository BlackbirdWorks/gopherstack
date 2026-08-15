package cloudformation

import "fmt"

// SetStackPolicy sets the stack policy for the given stack. The policy body
// is validated as a well-formed stack policy document (see
// stack_policy_eval.go), so a malformed policy is rejected here rather than
// silently never being enforced by UpdateStack.
func (b *InMemoryBackend) SetStackPolicy(nameOrID, policy string) error {
	b.mu.Lock("SetStackPolicy")
	defer b.mu.Unlock()

	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return ErrStackNotFound
	}

	if _, err := parseStackPolicyDocument(policy); err != nil {
		return err
	}

	b.stackPolicies[stack.StackID] = policy

	return nil
}

// GetStackPolicy returns the stack policy for the given stack.
// Returns an empty string if no policy has been set.
func (b *InMemoryBackend) GetStackPolicy(nameOrID string) (string, error) {
	b.mu.RLock("GetStackPolicy")
	defer b.mu.RUnlock()

	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return "", ErrStackNotFound
	}

	return b.stackPolicies[stack.StackID], nil
}

// checkStackPolicy enforces the stack's policy (or opts's one-shot
// StackPolicyDuringUpdateBody override, which is never persisted) against
// the resource changes an UpdateStack call to newTemplateBody would make.
// Must be called with b.mu already held, before stack.TemplateBody is
// overwritten with newTemplateBody -- computeChanges diffs the stack's
// current (pre-update) template against the proposed one.
func (b *InMemoryBackend) checkStackPolicy(stack *Stack, newTemplateBody string, opts StackOptions) error {
	policy := b.stackPolicies[stack.StackID]
	if opts.StackPolicyDuringUpdateBody != "" {
		policy = opts.StackPolicyDuringUpdateBody
	}
	if policy == "" {
		return nil
	}

	for _, change := range b.computeChanges(newTemplateBody, stack) {
		action, gated := stackPolicyActionForChange(change.ResourceChange)
		if !gated {
			continue
		}

		allowed, err := evaluateStackPolicy(
			policy, change.ResourceChange.LogicalID, change.ResourceChange.ResourceType, action,
		)
		if err != nil {
			return fmt.Errorf("stack policy for %s: %w", stack.StackName, err)
		}
		if !allowed {
			return fmt.Errorf(
				"%w: %s on resource %s is denied by the stack policy for %s",
				ErrStackPolicyDenied, action, change.ResourceChange.LogicalID, stack.StackName,
			)
		}
	}

	return nil
}
