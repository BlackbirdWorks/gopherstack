package cloudformation

import (
	"context"
	"errors"
	"fmt"
	"slices"
)

// UpdateStackSet updates the stack set and then every targeted stack instance's stack. Without
// Accounts/DeploymentTargets and Regions all instances are updated. A template or parameter change
// marks every instance OUTDATED first; each instance that updates cleanly returns to CURRENT
// (cloudformation@v1.82.0 api_op_UpdateStackSet.go:54-67).
func (b *InMemoryBackend) UpdateStackSet(
	name, description, templateBody string,
	opts StackSetOptions,
	opOpts ...StackSetOpOption,
) (*StackSet, string, error) {
	settings, err := resolveOpSettings(opOpts)
	if err != nil {
		return nil, "", err
	}

	b.mu.Lock("UpdateStackSet")
	defer b.mu.Unlock()

	ss, ok := b.stackSets.Get(name)
	if !ok {
		return nil, "", ErrStackSetNotFound
	}

	units, err := b.updateSetUnits(ss, settings.targets)
	if err != nil {
		return nil, "", err
	}

	settings.salt = updateSetSalt(description, templateBody, opts, settings.targets)
	key := opRequestKey("UPDATE", false, units, settings)

	if replay, perr := b.preadmitStackSetOp(name, "UPDATE", settings.operationID, key); replay != "" || perr != nil {
		cp := *ss

		return &cp, replay, perr
	}

	if applySetChanges(ss, description, templateBody, opts) {
		b.markInstancesOutdatedLocked(name)
	}

	apply := func(ctx context.Context, opID string, u stackSetUnit) string {
		return b.updateInstanceStack(ctx, name, opID, u, nil)
	}

	opID, err := b.admitStackSetOp(context.Background(), name, "UPDATE", settings, false, units, apply)
	cp := *ss

	return &cp, opID, err
}

func updateSetSalt(description, templateBody string, opts StackSetOptions, t *stackSetTargetSpec) string {
	var auto, managed string
	if opts.AutoDeployment != nil {
		auto = fmt.Sprint(*opts.AutoDeployment)
	}

	if opts.ManagedExecution != nil {
		managed = fmt.Sprint(*opts.ManagedExecution)
	}

	return fmt.Sprintf("%q|%q|%q|%q|%q|%q|%v|%v|%v|%s|%s|%v", description, templateBody,
		opts.AdministrationRoleARN, opts.ExecutionRoleName, opts.PermissionModel, opts.OrganizationalUnitIDs,
		opts.Capabilities, opts.Parameters, opts.Tags, auto, managed, t)
}

// updateSetUnits is the (account, region) set UpdateStackSet applies to.
func (b *InMemoryBackend) updateSetUnits(ss *StackSet, spec *stackSetTargetSpec) ([]stackSetUnit, error) {
	if spec == nil {
		all := b.stackInstances[ss.StackSetName]
		units := make([]stackSetUnit, 0, len(all))

		for _, inst := range all {
			units = append(units, stackSetUnit{
				account: inst.Account, region: inst.Region, ouID: inst.OrganizationalUnitID,
			})
		}

		return units, nil
	}

	if len(spec.regions) == 0 || (len(spec.accounts) == 0 && len(spec.ouIDs) == 0) {
		return nil, fmt.Errorf(
			"%w: specify both Regions and Accounts or DeploymentTargets, or neither",
			ErrInvalidOperation,
		)
	}

	return b.existingInstanceUnits(ss, spec.accounts, spec.ouIDs, spec.regions, spec.filterType)
}

// existingInstanceUnits resolves targets and requires each (account, region) to be an existing instance.
func (b *InMemoryBackend) existingInstanceUnits(
	ss *StackSet, accounts, ouIDs, regions []string, filterType string,
) ([]stackSetUnit, error) {
	targets, err := b.resolveInstanceTargets(ss, accounts, ouIDs, filterType)
	if err != nil {
		return nil, err
	}

	units := targetUnits(targets, regions)

	for i, u := range units {
		idx := b.instanceIndexLocked(ss.StackSetName, u)
		if idx < 0 {
			return nil, fmt.Errorf("%w: %s/%s", ErrStackInstanceNotFound, u.account, u.region)
		}

		units[i].ouID = b.stackInstances[ss.StackSetName][idx].OrganizationalUnitID
	}

	return units, nil
}

// applySetChanges stores the supplied fields and reports whether the template or parameters changed.
func applySetChanges(ss *StackSet, description, templateBody string, opts StackSetOptions) bool {
	changed := false

	if description != "" {
		ss.Description = description
	}

	if templateBody != "" {
		changed = changed || templateBody != ss.TemplateBody
		ss.TemplateBody = templateBody
	}

	if opts.Parameters != nil {
		params := resolvePreviousSetParameters(ss.Parameters, opts.Parameters)
		changed = changed || !slices.Equal(params, ss.Parameters)
		ss.Parameters = params
	}

	if opts.AdministrationRoleARN != "" {
		ss.AdministrationRoleARN = opts.AdministrationRoleARN
	}

	if opts.ExecutionRoleName != "" {
		ss.ExecutionRoleName = opts.ExecutionRoleName
	}

	if opts.PermissionModel != "" {
		ss.PermissionModel = opts.PermissionModel
	}

	if opts.Capabilities != nil {
		ss.Capabilities = opts.Capabilities
	}

	if opts.Tags != nil {
		ss.Tags = opts.Tags
	}

	if opts.OrganizationalUnitIDs != nil {
		ss.OrganizationalUnitIDs = opts.OrganizationalUnitIDs
	}

	if opts.AutoDeployment != nil {
		ss.AutoDeployment = opts.AutoDeployment
	}

	if opts.ManagedExecution != nil {
		ss.ManagedExecution = opts.ManagedExecution
	}

	return changed
}

// resolvePreviousSetParameters replaces UsePreviousValue entries with the set's current value.
func resolvePreviousSetParameters(current, next []Parameter) []Parameter {
	out := make([]Parameter, 0, len(next))

	for _, p := range next {
		if p.UsePreviousValue {
			idx := slices.IndexFunc(current, func(c Parameter) bool { return c.ParameterKey == p.ParameterKey })
			if idx < 0 {
				continue
			}

			p = current[idx]
		}

		out = append(out, p)
	}

	return out
}

func (b *InMemoryBackend) markInstancesOutdatedLocked(stackSetName string) {
	for i := range b.stackInstances[stackSetName] {
		if b.stackInstances[stackSetName][i].Status == statusCurrent {
			b.stackInstances[stackSetName][i].Status = statusOutdated
		}
	}
}

// UpdateStackInstances changes ParameterOverrides for the targeted instances and updates their stacks.
func (b *InMemoryBackend) UpdateStackInstances(
	stackSetName string,
	accounts, ouIDs, regions []string,
	filterType string,
	opOpts ...StackSetOpOption,
) (string, error) {
	settings, err := resolveOpSettings(opOpts)
	if err != nil {
		return "", err
	}

	b.mu.Lock("UpdateStackInstances")
	defer b.mu.Unlock()

	ss, ok := b.stackSets.Get(stackSetName)
	if !ok {
		return "", ErrStackSetNotFound
	}

	units, err := b.existingInstanceUnits(ss, accounts, ouIDs, regions, filterType)
	if err != nil {
		return "", err
	}

	if settings.overrides != nil {
		if err = validateOverrides(ss, *settings.overrides); err != nil {
			return "", err
		}
	}

	apply := func(ctx context.Context, opID string, u stackSetUnit) string {
		return b.updateInstanceStack(ctx, stackSetName, opID, u, settings.overrides)
	}

	return b.admitStackSetOp(context.Background(), stackSetName, "UPDATE", settings, false, units, apply)
}

// updateInstanceStack updates one instance's stack from the set's current definition. A failure
// leaves the instance OUTDATED (INOPERABLE if the rollback failed) with the reason; the previous
// overrides stay. Must be called with b.mu held.
func (b *InMemoryBackend) updateInstanceStack(
	ctx context.Context, stackSetName, opID string, u stackSetUnit, overrides *[]Parameter,
) string {
	ss, ok := b.stackSets.Get(stackSetName)
	if !ok {
		return "stack set no longer exists"
	}

	i := b.instanceIndexLocked(stackSetName, u)
	if i < 0 {
		return "stack instance no longer exists"
	}

	prev := b.stackInstances[stackSetName][i]
	next := prev

	if overrides != nil {
		next.ParameterOverrides = mergeOverrides(prev.ParameterOverrides, *overrides)
	}

	reason, unstable := b.deployInstanceStack(ctx, ss, &next)

	cur := &b.stackInstances[stackSetName][i]
	cur.StackID = next.StackID
	cur.LastOperationID = opID

	if reason == "" {
		cur.ParameterOverrides = next.ParameterOverrides
		cur.Status = statusCurrent
		cur.StatusReason = ""

		return ""
	}

	cur.Status = statusOutdated
	if unstable {
		cur.Status = statusInoperable
	}

	cur.StatusReason = reason

	return reason
}

// deployInstanceStack brings the instance's child stack to the set's definition and returns a
// failure reason ("" on success) and whether the stack was left unstable by a failed rollback.
func (b *InMemoryBackend) deployInstanceStack(ctx context.Context, ss *StackSet, inst *StackInstance) (string, bool) {
	if ss.TemplateBody == "" {
		return "", false
	}

	params := effectiveParameters(ss.Parameters, inst.ParameterOverrides)

	childName, ok := b.stackIDIndex[inst.StackID]
	if !ok {
		fresh := b.provisionStackInstance(
			ctx,
			ss,
			inst.Account,
			inst.Region,
			inst.LastOperationID,
			inst.ParameterOverrides,
		)
		inst.StackID = fresh.StackID

		return fresh.StatusReason, false
	}

	stack, err := b.updateStackLocked(
		ctx, childName, ss.TemplateBody, params, StackOptions{Capabilities: ss.Capabilities, Tags: ss.Tags}, false,
	)

	switch {
	case errors.Is(err, ErrNoUpdates):
		return "", false
	case err != nil:
		return err.Error(), false
	case stack.StackStatus == statusUpdateComplete:
		return "", false
	}

	reason := stack.StackStatusReason
	if reason == "" {
		reason = "stack update ended in " + stack.StackStatus
	}

	return reason, stack.StackStatus == statusUpdateRollbackFailed
}

// effectiveParameters overlays an instance's overrides on the set's parameters.
func effectiveParameters(base, overrides []Parameter) []Parameter {
	out := slices.Clone(base)

	for _, o := range overrides {
		idx := slices.IndexFunc(out, func(p Parameter) bool { return p.ParameterKey == o.ParameterKey })
		if idx >= 0 {
			out[idx] = o

			continue
		}

		out = append(out, o)
	}

	return out
}

// mergeOverrides applies an UpdateStackInstances override list: listed parameters take the new
// value (or keep the current override with UsePreviousValue), unlisted ones revert to the set value.
func mergeOverrides(current, spec []Parameter) []Parameter {
	out := make([]Parameter, 0, len(spec))

	for _, p := range spec {
		if p.UsePreviousValue {
			idx := slices.IndexFunc(current, func(c Parameter) bool { return c.ParameterKey == p.ParameterKey })
			if idx >= 0 {
				out = append(out, current[idx])
			}

			continue
		}

		out = append(out, p)
	}

	return out
}

// validateOverrides rejects overrides that name a parameter the stack set does not define or that
// give both a value and UsePreviousValue (api_op_UpdateStackInstances.go ParameterOverrides doc).
func validateOverrides(ss *StackSet, spec []Parameter) error {
	declared := make(map[string]bool, len(ss.Parameters))
	for _, p := range ss.Parameters {
		declared[p.ParameterKey] = true
	}

	if tmpl, err := ParseTemplate(ss.TemplateBody); err == nil {
		for name := range tmpl.Parameters {
			declared[name] = true
		}
	}

	for _, p := range spec {
		switch {
		case p.UsePreviousValue && p.ParameterValue != "":
			return fmt.Errorf(
				"%w: parameter %s sets both a value and UsePreviousValue",
				ErrInvalidParameterOverrides,
				p.ParameterKey,
			)
		case !declared[p.ParameterKey]:
			return fmt.Errorf(
				"%w: parameter %s is not defined by the stack set",
				ErrInvalidParameterOverrides,
				p.ParameterKey,
			)
		}
	}

	return nil
}

// PreviewStackSetChanges lists the resource changes of moving a stack set to templateBody; an empty
// name previews a new stack set, where every resource is an addition.
func (b *InMemoryBackend) PreviewStackSetChanges(name, templateBody string, params []Parameter) ([]Change, error) {
	if _, err := ParseTemplate(templateBody); err != nil {
		return nil, err
	}

	b.mu.RLock("PreviewStackSetChanges")
	defer b.mu.RUnlock()

	if name == "" {
		return b.computeChanges(templateBody, nil, params), nil
	}

	ss, ok := b.stackSets.Get(name)
	if !ok {
		return nil, ErrStackSetNotFound
	}

	current := &Stack{TemplateBody: ss.TemplateBody, Parameters: ss.Parameters}

	return b.computeChanges(templateBody, current, params), nil
}
