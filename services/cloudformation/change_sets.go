package cloudformation

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/google/uuid"
)

// CreateChangeSet creates a change set for a stack.
func (b *InMemoryBackend) CreateChangeSet(
	_ context.Context,
	stackName, changeSetName, templateBody, description string,
	params []Parameter,
	capabilities []string,
) (*ChangeSet, error) {
	b.mu.Lock("CreateChangeSet")
	defer b.mu.Unlock()

	if b.changeSets[stackName] == nil {
		b.changeSets[stackName] = make(map[string]*ChangeSet)
	}

	if _, exists := b.changeSets[stackName][changeSetName]; exists {
		return nil, ErrChangeSetExists
	}

	stack, _ := b.resolveStack(stackName)

	csID := uuid.New().String()
	stackID := ""
	changeSetType := "CREATE"
	if stack != nil {
		stackID = stack.StackID
		changeSetType = "UPDATE"
	}

	cs := &ChangeSet{
		ChangeSetID: arn.Build(
			"cloudformation",
			b.region,
			b.accountID,
			"changeSet/"+changeSetName+"/"+csID,
		),
		ChangeSetName:   changeSetName,
		StackID:         stackID,
		StackName:       stackName,
		Status:          statusCreateComplete,
		ExecutionStatus: "AVAILABLE",
		ChangeSetType:   changeSetType,
		Description:     description,
		CreationTime:    time.Now(),
		TemplateBody:    templateBody,
		Parameters:      params,
		Capabilities:    capabilities,
	}

	cs.Changes = b.computeChanges(templateBody, stack)

	// AWS marks a change set with no actual changes as FAILED / UNAVAILABLE so
	// it cannot be executed; only a change set that contains changes is
	// AVAILABLE for execution.
	if len(cs.Changes) == 0 {
		cs.Status = "FAILED"
		cs.StatusReason = "The submitted information didn't contain changes. " +
			"Submit different information to create a change set."
		cs.ExecutionStatus = "UNAVAILABLE"
	}

	b.changeSets[stackName][changeSetName] = cs

	return cs, nil
}

// computeChanges computes the change set diff from a template body against an
// existing stack. It reports Add for new resources, Remove for resources dropped
// from the template, and Modify (with property-level Details, Scope and a
// Replacement flag) for resources whose type or properties changed. Pre-existing
// resources that are unchanged are omitted entirely, matching AWS.
func (b *InMemoryBackend) computeChanges(templateBody string, stack *Stack) []Change {
	if templateBody == "" {
		return nil
	}

	newTmpl, err := ParseTemplate(templateBody)
	if err != nil {
		return nil
	}

	// CREATE change set: no prior stack, so every resource is an Add.
	if stack == nil {
		return addChangesForTemplate(newTmpl)
	}

	var oldTmpl *Template
	if stack.TemplateBody != "" {
		oldTmpl, _ = ParseTemplate(stack.TemplateBody)
	}

	return diffTemplates(oldTmpl, newTmpl, b.resources[stack.StackID])
}

// DescribeChangeSet returns details for a change set.
func (b *InMemoryBackend) DescribeChangeSet(stackName, changeSetName string) (*ChangeSet, error) {
	b.mu.RLock("DescribeChangeSet")
	defer b.mu.RUnlock()

	csMap, ok := b.changeSets[stackName]
	if !ok {
		return nil, ErrChangeSetNotFound
	}
	cs, ok := csMap[changeSetName]
	if !ok {
		return nil, ErrChangeSetNotFound
	}

	return cs, nil
}

// ExecuteChangeSet applies a change set to a stack. Only a change set whose
// ExecutionStatus is AVAILABLE can be executed — e.g. a change set created with
// no actual changes is FAILED/UNAVAILABLE and AWS rejects execution of it with
// InvalidChangeSetStatus. On success, AWS deletes every other change set
// associated with the stack because none remain valid against the now-updated
// template; this backend clears the whole per-stack change-set map to match.
func (b *InMemoryBackend) ExecuteChangeSet(
	ctx context.Context,
	stackName, changeSetName string,
) error {
	var cs *ChangeSet
	lockErr := func() error {
		b.mu.Lock("ExecuteChangeSet")
		defer b.mu.Unlock()

		var ok bool
		cs, ok = b.changeSets[stackName][changeSetName]
		if !ok {
			return ErrChangeSetNotFound
		}
		if cs.ExecutionStatus != "AVAILABLE" {
			return fmt.Errorf(
				"%w: ChangeSet [%s] cannot be executed in its current status of %s",
				ErrChangeSetNotExecutable, changeSetName, cs.ExecutionStatus,
			)
		}
		cs.ExecutionStatus = "EXECUTE_IN_PROGRESS"

		return nil
	}()
	if lockErr != nil {
		return lockErr
	}

	var execErr error
	opts := StackOptions{Capabilities: cs.Capabilities}
	_, err := b.UpdateStack(ctx, stackName, cs.TemplateBody, cs.Parameters, opts)
	if err != nil {
		// Stack may not exist yet — create it.
		_, err = b.CreateStack(ctx, stackName, cs.TemplateBody, cs.Parameters, opts)
		if err != nil {
			execErr = err
		}
	}

	func() {
		b.mu.Lock("ExecuteChangeSet")
		defer b.mu.Unlock()

		if execErr != nil {
			if cs2, ok2 := b.changeSets[stackName][changeSetName]; ok2 {
				cs2.ExecutionStatus = "EXECUTE_FAILED"
			}
		} else {
			b.changeSets[stackName] = make(map[string]*ChangeSet)
		}
	}()

	return execErr
}

// DeleteChangeSet removes a change set.
func (b *InMemoryBackend) DeleteChangeSet(stackName, changeSetName string) error {
	b.mu.Lock("DeleteChangeSet")
	defer b.mu.Unlock()

	if b.changeSets[stackName] == nil {
		return ErrChangeSetNotFound
	}
	if _, ok := b.changeSets[stackName][changeSetName]; !ok {
		return ErrChangeSetNotFound
	}
	delete(b.changeSets[stackName], changeSetName)

	return nil
}

// ListChangeSets returns paginated summaries of change sets for a stack.
func (b *InMemoryBackend) ListChangeSets(
	stackName, nextToken string,
) (page.Page[ChangeSetSummary], error) {
	b.mu.RLock("ListChangeSets")
	defer b.mu.RUnlock()

	csMap := b.changeSets[stackName]
	summaries := make([]ChangeSetSummary, 0, len(csMap))
	for _, cs := range csMap {
		summaries = append(summaries, ChangeSetSummary{
			ChangeSetID:   cs.ChangeSetID,
			ChangeSetName: cs.ChangeSetName,
			StackID:       cs.StackID,
			StackName:     cs.StackName,
			Status:        cs.Status,
			CreationTime:  cs.CreationTime,
			Description:   cs.Description,
		})
	}

	sort.Slice(
		summaries,
		func(i, j int) bool { return summaries[i].ChangeSetName < summaries[j].ChangeSetName },
	)

	return page.New(summaries, nextToken, 0, cfnDefaultPageSize), nil
}
