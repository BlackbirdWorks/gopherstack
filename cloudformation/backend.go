package cloudformation

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

var (
	ErrStackNotFound      = errors.New("Stack with id does not exist")
	ErrStackAlreadyExists = errors.New("Stack already exists")
	ErrChangeSetNotFound  = errors.New("ChangeSet not found")
	ErrChangeSetExists    = errors.New("ChangeSet already exists")
)

// StorageBackend defines the interface for the CloudFormation in-memory backend.
type StorageBackend interface {
	CreateStack(ctx context.Context, name, templateBody string, params []Parameter, tags []Tag) (*Stack, error)
	UpdateStack(ctx context.Context, nameOrID, templateBody string, params []Parameter) (*Stack, error)
	DeleteStack(ctx context.Context, nameOrID string) error
	DescribeStack(nameOrID string) (*Stack, error)
	ListStacks(statusFilter []string) []StackSummary
	DescribeStackEvents(nameOrID string) ([]StackEvent, error)
	CreateChangeSet(
		ctx context.Context,
		stackName, changeSetName, templateBody, description string,
		params []Parameter,
	) (*ChangeSet, error)
	DescribeChangeSet(stackName, changeSetName string) (*ChangeSet, error)
	ExecuteChangeSet(ctx context.Context, stackName, changeSetName string) error
	DeleteChangeSet(stackName, changeSetName string) error
	ListChangeSets(stackName string) ([]ChangeSetSummary, error)
	GetTemplate(nameOrID string) (string, error)
	ListAll() []*Stack
}

// InMemoryBackend is a concurrency-safe in-memory CloudFormation backend.
type InMemoryBackend struct {
	stacks     map[string]*Stack                    // key = stackName
	events     map[string][]StackEvent              // key = stackID
	resources  map[string]map[string]*StackResource // key = stackID, logicalID
	changeSets map[string]map[string]*ChangeSet     // key = stackName, changeSetName
	creator    *ResourceCreator
	accountID  string
	region     string
	mu         sync.RWMutex
}

const (
	MockAccountID = "000000000000"
	MockRegion    = "us-east-1"

	cfnStackType             = "AWS::CloudFormation::Stack"
	statusCreateInProgress   = "CREATE_IN_PROGRESS"
	statusCreateComplete     = "CREATE_COMPLETE"
	statusCreateFailed       = "CREATE_FAILED"
	statusUpdateInProgress   = "UPDATE_IN_PROGRESS"
	statusUpdateComplete     = "UPDATE_COMPLETE"
	statusUpdateFailed       = "UPDATE_FAILED"
	statusDeleteInProgress   = "DELETE_IN_PROGRESS"
	statusDeleteComplete     = "DELETE_COMPLETE"
	statusRollbackInProgress = "ROLLBACK_IN_PROGRESS"
	statusRollbackComplete   = "ROLLBACK_COMPLETE"
	reasonUserInitiated      = "User Initiated"
)

// NewInMemoryBackend creates a new empty CloudFormation backend.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(MockAccountID, MockRegion, nil)
}

// NewInMemoryBackendWithConfig creates a new backend with the given config and resource creator.
func NewInMemoryBackendWithConfig(accountID, region string, creator *ResourceCreator) *InMemoryBackend {
	return &InMemoryBackend{
		stacks:     make(map[string]*Stack),
		events:     make(map[string][]StackEvent),
		resources:  make(map[string]map[string]*StackResource),
		changeSets: make(map[string]map[string]*ChangeSet),
		creator:    creator,
		accountID:  accountID,
		region:     region,
	}
}

func (b *InMemoryBackend) buildStackARN(stackName, stackID string) string {
	return arn.Build("cloudformation", b.region, b.accountID, "stack/"+stackName+"/"+stackID)
}

func (b *InMemoryBackend) resolveStack(nameOrID string) (*Stack, bool) {
	if s, ok := b.stacks[nameOrID]; ok {
		return s, true
	}
	for _, s := range b.stacks {
		if s.StackID == nameOrID {
			return s, true
		}
	}

	return nil, false
}

// addEvent appends an event to the stack's event history.
func (b *InMemoryBackend) addEvent(stackID, stackName, logicalID, physicalID, resourceType, status, reason string) {
	evt := StackEvent{
		EventID:              uuid.New().String(),
		StackID:              stackID,
		StackName:            stackName,
		LogicalResourceID:    logicalID,
		PhysicalResourceID:   physicalID,
		ResourceType:         resourceType,
		ResourceStatus:       status,
		ResourceStatusReason: reason,
		Timestamp:            time.Now(),
	}
	b.events[stackID] = append(b.events[stackID], evt)
}

// CreateStack creates a new stack from a template.
func (b *InMemoryBackend) CreateStack(
	ctx context.Context,
	name, templateBody string,
	params []Parameter,
	tags []Tag,
) (*Stack, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if existing, ok := b.stacks[name]; ok {
		if existing.StackStatus != statusDeleteComplete {
			return nil, ErrStackAlreadyExists
		}
	}

	stackID := uuid.New().String()
	arn := b.buildStackARN(name, stackID)
	now := time.Now()

	stack := &Stack{
		StackID:      arn,
		StackName:    name,
		StackStatus:  statusCreateInProgress,
		CreationTime: now,
		Parameters:   params,
		Tags:         tags,
		TemplateBody: templateBody,
	}

	b.stacks[name] = stack
	b.events[arn] = nil
	b.resources[arn] = make(map[string]*StackResource)

	b.addEvent(arn, name, name, arn, cfnStackType, statusCreateInProgress, reasonUserInitiated)

	// Parse and provision resources.
	if templateBody != "" {
		tmpl, err := ParseTemplate(templateBody)
		if err != nil {
			stack.StackStatus = statusCreateFailed
			stack.StackStatusReason = err.Error()
			b.addEvent(arn, name, name, arn, cfnStackType, statusCreateFailed, err.Error())

			return stack, nil
		}
		stack.Description = tmpl.Description

		resolvedParams := ResolveParameters(tmpl, params)
		physicalIDs := make(map[string]string)

		for logicalID, res := range tmpl.Resources {
			b.addEvent(arn, name, logicalID, "", res.Type, statusCreateInProgress, "")
			physicalID, cerr := b.creator.Create(ctx, logicalID, res.Type, res.Properties, resolvedParams, physicalIDs)
			if cerr != nil {
				stack.StackStatus = statusCreateFailed
				stack.StackStatusReason = fmt.Sprintf("resource %s: %v", logicalID, cerr)
				b.addEvent(arn, name, logicalID, "", res.Type, statusCreateFailed, cerr.Error())
				b.addEvent(arn, name, name, arn, cfnStackType, statusRollbackInProgress, cerr.Error())
				b.addEvent(arn, name, name, arn, cfnStackType, statusRollbackComplete, "")

				return stack, nil
			}
			physicalIDs[logicalID] = physicalID
			b.resources[arn][logicalID] = &StackResource{
				LogicalID:  logicalID,
				PhysicalID: physicalID,
				Type:       res.Type,
				Status:     statusCreateComplete,
				Properties: res.Properties,
			}
			b.addEvent(arn, name, logicalID, physicalID, res.Type, statusCreateComplete, "")
		}

		stack.Outputs = resolveOutputs(tmpl, resolvedParams, physicalIDs)
	}

	stack.StackStatus = statusCreateComplete
	b.addEvent(arn, name, name, arn, cfnStackType, statusCreateComplete, "")

	return stack, nil
}

// UpdateStack updates an existing stack.
func (b *InMemoryBackend) UpdateStack(
	ctx context.Context,
	nameOrID, templateBody string,
	params []Parameter,
) (*Stack, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return nil, ErrStackNotFound
	}

	now := time.Now()
	stack.LastUpdatedTime = &now
	stack.StackStatus = statusUpdateInProgress

	if templateBody != "" {
		stack.TemplateBody = templateBody
	}
	if params != nil {
		stack.Parameters = params
	}

	b.addEvent(
		stack.StackID, stack.StackName, stack.StackName, stack.StackID,
		cfnStackType, statusUpdateInProgress, reasonUserInitiated,
	)

	if !b.applyTemplateToStack(ctx, stack) {
		return stack, nil
	}

	stack.StackStatus = statusUpdateComplete
	b.addEvent(
		stack.StackID, stack.StackName, stack.StackName, stack.StackID,
		cfnStackType, statusUpdateComplete, "",
	)

	return stack, nil
}

// applyTemplateToStack parses the stack's template and creates or updates resources.
// Returns true on success; on failure it sets the stack status and returns false.
func (b *InMemoryBackend) applyTemplateToStack(ctx context.Context, stack *Stack) bool {
	if stack.TemplateBody == "" {
		return true
	}

	tmpl, err := ParseTemplate(stack.TemplateBody)
	if err != nil {
		stack.StackStatus = statusUpdateFailed
		stack.StackStatusReason = err.Error()

		return false
	}

	stack.Description = tmpl.Description

	resolvedParams := ResolveParameters(tmpl, stack.Parameters)
	physicalIDs := make(map[string]string)

	for logicalID, res := range b.resources[stack.StackID] {
		physicalIDs[logicalID] = res.PhysicalID
	}

	for logicalID, res := range tmpl.Resources {
		if existing, exists := b.resources[stack.StackID][logicalID]; exists {
			existing.Status = statusUpdateComplete
			b.addEvent(
				stack.StackID,
				stack.StackName,
				logicalID,
				existing.PhysicalID,
				res.Type,
				statusUpdateComplete,
				"",
			)
		} else {
			b.addEvent(stack.StackID, stack.StackName, logicalID, "", res.Type, statusCreateInProgress, "")
			physicalID, cerr := b.creator.Create(ctx, logicalID, res.Type, res.Properties, resolvedParams, physicalIDs)
			if cerr != nil {
				stack.StackStatus = statusUpdateFailed
				stack.StackStatusReason = fmt.Sprintf("resource %s: %v", logicalID, cerr)

				return false
			}

			physicalIDs[logicalID] = physicalID
			b.resources[stack.StackID][logicalID] = &StackResource{
				LogicalID:  logicalID,
				PhysicalID: physicalID,
				Type:       res.Type,
				Status:     statusCreateComplete,
				Properties: res.Properties,
			}
			b.addEvent(stack.StackID, stack.StackName, logicalID, physicalID, res.Type, statusCreateComplete, "")
		}
	}

	stack.Outputs = resolveOutputs(tmpl, resolvedParams, physicalIDs)

	return true
}

// DeleteStack marks a stack as deleted and deletes its resources.
func (b *InMemoryBackend) DeleteStack(ctx context.Context, nameOrID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return ErrStackNotFound
	}

	stack.StackStatus = statusDeleteInProgress
	b.addEvent(
		stack.StackID, stack.StackName, stack.StackName, stack.StackID,
		cfnStackType, statusDeleteInProgress, reasonUserInitiated,
	)

	for logicalID, res := range b.resources[stack.StackID] {
		b.addEvent(stack.StackID, stack.StackName, logicalID, res.PhysicalID, res.Type, statusDeleteInProgress, "")
		_ = b.creator.Delete(ctx, res.Type, res.PhysicalID, res.Properties)
		b.addEvent(stack.StackID, stack.StackName, logicalID, res.PhysicalID, res.Type, statusDeleteComplete, "")
	}

	now := time.Now()
	stack.DeletionTime = &now
	stack.StackStatus = statusDeleteComplete
	b.addEvent(
		stack.StackID, stack.StackName, stack.StackName, stack.StackID,
		cfnStackType, statusDeleteComplete, "",
	)

	return nil
}

// DescribeStack returns details for a single stack.
func (b *InMemoryBackend) DescribeStack(nameOrID string) (*Stack, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return nil, ErrStackNotFound
	}

	return stack, nil
}

// ListStacks returns stack summaries, optionally filtered by status.
func (b *InMemoryBackend) ListStacks(statusFilter []string) []StackSummary {
	b.mu.RLock()
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(statusFilter))
	for _, s := range statusFilter {
		filter[s] = true
	}

	summaries := make([]StackSummary, 0, len(b.stacks))
	for _, stack := range b.stacks {
		if len(filter) > 0 && !filter[stack.StackStatus] {
			continue
		}
		summaries = append(summaries, StackSummary{
			StackID:      stack.StackID,
			StackName:    stack.StackName,
			StackStatus:  stack.StackStatus,
			CreationTime: stack.CreationTime,
			DeletionTime: stack.DeletionTime,
		})
	}

	return summaries
}

// DescribeStackEvents returns events for a stack.
func (b *InMemoryBackend) DescribeStackEvents(nameOrID string) ([]StackEvent, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return nil, ErrStackNotFound
	}

	evts := b.events[stack.StackID]
	result := make([]StackEvent, len(evts))
	// Return in reverse chronological order (most recent first).
	for i, e := range evts {
		result[len(evts)-1-i] = e
	}

	return result, nil
}

// CreateChangeSet creates a change set for a stack.
func (b *InMemoryBackend) CreateChangeSet(
	_ context.Context,
	stackName, changeSetName, templateBody, description string,
	params []Parameter,
) (*ChangeSet, error) {
	b.mu.Lock()
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
	if stack != nil {
		stackID = stack.StackID
	}

	cs := &ChangeSet{
		ChangeSetID: fmt.Sprintf(
			"arn:aws:cloudformation:%s:%s:changeSet/%s/%s",
			b.region, b.accountID, changeSetName, csID,
		),
		ChangeSetName: changeSetName,
		StackID:       stackID,
		StackName:     stackName,
		Status:        statusCreateComplete,
		Description:   description,
		CreationTime:  time.Now(),
		TemplateBody:  templateBody,
		Parameters:    params,
	}

	cs.Changes = b.computeChanges(templateBody, stack)

	b.changeSets[stackName][changeSetName] = cs

	return cs, nil
}

// computeChanges computes the change set diff from a template body against an existing stack.
func (b *InMemoryBackend) computeChanges(templateBody string, stack *Stack) []Change {
	if templateBody == "" {
		return nil
	}

	tmpl, err := ParseTemplate(templateBody)
	if err != nil {
		return nil
	}

	changes := make([]Change, 0, len(tmpl.Resources))

	for logicalID, res := range tmpl.Resources {
		action := "Add"
		if stack != nil && b.resources[stack.StackID] != nil {
			if _, exists := b.resources[stack.StackID][logicalID]; exists {
				action = "Modify"
			}
		}

		changes = append(changes, Change{
			Type: "Resource",
			ResourceChange: ResourceChange{
				Action:       action,
				LogicalID:    logicalID,
				ResourceType: res.Type,
			},
		})
	}

	return changes
}

// DescribeChangeSet returns details for a change set.
func (b *InMemoryBackend) DescribeChangeSet(stackName, changeSetName string) (*ChangeSet, error) {
	b.mu.RLock()
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

// ExecuteChangeSet applies a change set to a stack.
func (b *InMemoryBackend) ExecuteChangeSet(ctx context.Context, stackName, changeSetName string) error {
	b.mu.Lock()
	cs, ok := b.changeSets[stackName][changeSetName]
	b.mu.Unlock()

	if !ok {
		return ErrChangeSetNotFound
	}

	_, err := b.UpdateStack(ctx, stackName, cs.TemplateBody, cs.Parameters)
	if err != nil {
		// Stack may not exist yet — create it.
		_, err = b.CreateStack(ctx, stackName, cs.TemplateBody, cs.Parameters, nil)
		if err != nil {
			return err
		}
	}

	b.mu.Lock()
	delete(b.changeSets[stackName], changeSetName)
	b.mu.Unlock()

	return nil
}

// DeleteChangeSet removes a change set.
func (b *InMemoryBackend) DeleteChangeSet(stackName, changeSetName string) error {
	b.mu.Lock()
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

// ListChangeSets returns summaries of change sets for a stack.
func (b *InMemoryBackend) ListChangeSets(stackName string) ([]ChangeSetSummary, error) {
	b.mu.RLock()
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

	return summaries, nil
}

// GetTemplate returns the template body for a stack.
func (b *InMemoryBackend) GetTemplate(nameOrID string) (string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return "", ErrStackNotFound
	}

	return stack.TemplateBody, nil
}

// ListAll returns all stacks (for dashboard).
func (b *InMemoryBackend) ListAll() []*Stack {
	b.mu.RLock()
	defer b.mu.RUnlock()

	stacks := make([]*Stack, 0, len(b.stacks))
	for _, s := range b.stacks {
		stacks = append(stacks, s)
	}

	return stacks
}
