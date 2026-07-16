package ssm

import (
	"context"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func (b *InMemoryBackend) commandsStore(region string) *store.Table[Command] {
	return getOrCreateTable(b, b.commands, "commands", region, commandKeyFn)
}
func (b *InMemoryBackend) commandInvocationsStore(region string) map[string][]CommandInvocation {
	return b.commandInvocations[region]
}

// SendCommand creates a command and drives it through the AWS state machine:
// Pending → InProgress → Success (synchronous no-op runner path).
func (b *InMemoryBackend) SendCommand(
	ctx context.Context,
	input *SendCommandInput,
) (*SendCommandOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("SendCommand")
	defer b.mu.Unlock()

	// AWS-RunPatchBaseline is one of AWS's ~150 built-in Systems Manager
	// documents that exist account-wide without needing to be created first;
	// only it (of the ones this emulator can act on) is recognised implicitly
	// here rather than requiring pre-registration like customer documents.
	exists := b.documentsStore(region).Has(input.DocumentName)
	if !exists && input.DocumentName != docRunPatchBaseline {
		return nil, ErrDocumentNotFound
	}

	now := UnixTimeFloat(time.Now())
	cmdID := uuid.NewString()

	timeoutSecs := input.TimeoutSeconds
	if timeoutSecs == 0 {
		timeoutSecs = 3600
	}

	// Start in Pending state; transition through InProgress to Success so callers
	// that snapshot state between transitions observe correct intermediate values.
	cmd := Command{
		CommandID:          cmdID,
		DocumentName:       input.DocumentName,
		Parameters:         input.Parameters,
		Status:             commandStatusPending,
		StatusDetails:      commandStatusPending,
		RequestedDateTime:  now,
		ExpiresAfter:       now + b.commandExpirySecs,
		InstanceIDs:        input.InstanceIDs,
		Targets:            input.Targets,
		Comment:            input.Comment,
		TimeoutSeconds:     timeoutSecs,
		OutputS3BucketName: input.OutputS3BucketName,
		OutputS3KeyPrefix:  input.OutputS3KeyPrefix,
		OutputS3Region:     input.OutputS3Region,
	}

	b.commandsStore(region).Put(&cmd)

	stdout, stderr, finalStatus := renderCommandOutput(input.DocumentName, input.Parameters)

	if input.DocumentName == docRunPatchBaseline {
		for _, instanceID := range input.InstanceIDs {
			b.applyPatchBaselineOperation(region, instanceID, input.Parameters)
		}
	}

	invocations := make([]CommandInvocation, 0, len(input.InstanceIDs))
	for _, instanceID := range input.InstanceIDs {
		inv := CommandInvocation{
			CommandID:         cmdID,
			InstanceID:        instanceID,
			DocumentName:      input.DocumentName,
			Status:            commandStatusPending,
			StatusDetails:     commandStatusPending,
			RequestedDateTime: now,
			Comment:           input.Comment,
			pendingStdout:     stdout,
			pendingStderr:     stderr,
			finalStatus:       finalStatus,
		}
		invocations = append(invocations, inv)
	}
	if b.commandInvocations[region] == nil {
		b.commandInvocations[region] = make(map[string][]CommandInvocation)
	}
	b.commandInvocationsStore(region)[cmdID] = invocations

	// Drive Pending → InProgress immediately so the InProgress window is always
	// observable. When no exec delay is configured the command then completes
	// synchronously (revealing output); otherwise it stays InProgress and is
	// lazily completed by reads once b.commandExecDelaySecs has elapsed.
	b.setCommandStatus(region, cmdID, commandStatusInProgress)

	if b.commandExecDelaySecs <= 0 {
		b.completeCommand(region, cmdID)
	} else {
		pendingPtr, _ := b.commandsStore(region).Get(cmdID)
		pending := *pendingPtr
		pending.completeAfter = now + b.commandExecDelaySecs
		b.commandsStore(region).Put(&pending)
	}

	// Return a snapshot of the current state.
	finalCmdPtr, _ := b.commandsStore(region).Get(cmdID)
	finalCmd := *finalCmdPtr

	return &SendCommandOutput{Command: finalCmd}, nil
}

// setCommandStatus mutates the command and all its invocations to the given
// non-terminal status. Must be called with b.mu held for writing.
func (b *InMemoryBackend) setCommandStatus(region, cmdID, status string) {
	cmdTable := b.commandsStore(region)

	cmdPtr, ok := cmdTable.Get(cmdID)
	if !ok {
		return
	}

	cmd := *cmdPtr
	cmd.Status = status
	cmd.StatusDetails = status
	cmdTable.Put(&cmd)

	invStore := b.commandInvocationsStore(region)
	invs := invStore[cmdID]

	for i := range invs {
		invs[i].Status = status
		invs[i].StatusDetails = status
	}

	invStore[cmdID] = invs
}

// completeCommand transitions an InProgress command to its terminal status and
// reveals the rendered output on each invocation. The command status is the
// worst per-invocation status (Failed dominates Success). Must be called with
// b.mu held for writing.
func (b *InMemoryBackend) completeCommand(region, cmdID string) {
	cmdTable := b.commandsStore(region)

	cmdPtr, ok := cmdTable.Get(cmdID)
	if !ok {
		return
	}

	cmd := *cmdPtr

	invStore := b.commandInvocationsStore(region)
	invs := invStore[cmdID]

	overall := commandStatusSuccess

	for i := range invs {
		final := invs[i].finalStatus
		if final == "" {
			final = commandStatusSuccess
		}

		invs[i].Status = final
		invs[i].StatusDetails = final
		invs[i].StandardOutputContent = invs[i].pendingStdout
		invs[i].StandardErrorContent = invs[i].pendingStderr

		if final != commandStatusSuccess {
			overall = final
		}
	}

	invStore[cmdID] = invs

	cmd.Status = overall
	cmd.StatusDetails = overall
	cmd.completeAfter = 0
	cmdTable.Put(&cmd)
}

// materializeCommandLocked lazily completes an InProgress command whose exec
// delay has elapsed. Must be called with b.mu held for writing.
func (b *InMemoryBackend) materializeCommandLocked(region, cmdID string, nowUnix float64) {
	cmdPtr, ok := b.commandsStore(region).Get(cmdID)
	if !ok || cmdPtr.Status != commandStatusInProgress {
		return
	}

	if cmdPtr.completeAfter == 0 || nowUnix >= cmdPtr.completeAfter {
		b.completeCommand(region, cmdID)
	}
}

// materializeCommandsLocked lazily completes every eligible InProgress command
// in the region. Must be called with b.mu held for writing.
func (b *InMemoryBackend) materializeCommandsLocked(region string, nowUnix float64) {
	for _, cmdPtr := range b.commandsStore(region).All() {
		b.materializeCommandLocked(region, cmdPtr.CommandID, nowUnix)
	}
}

// ListCommands returns recorded commands.
func (b *InMemoryBackend) ListCommands(
	ctx context.Context,
	input *ListCommandsInput,
) (*ListCommandsOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("ListCommands")
	defer b.mu.Unlock()

	b.materializeCommandsLocked(region, UnixTimeFloat(timeNow()))

	cmdTable := b.commandsStore(region)
	all := make([]Command, 0, cmdTable.Len())
	for _, cmdPtr := range cmdTable.All() {
		if input.CommandID != "" && cmdPtr.CommandID != input.CommandID {
			continue
		}
		all = append(all, *cmdPtr)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].CommandID < all[j].CommandID })

	startIdx := parseNextToken(input.NextToken)

	maxResults := int64(defaultListDocMaxResults)
	if input.MaxResults != nil && *input.MaxResults > 0 {
		maxResults = *input.MaxResults
	}

	if startIdx >= len(all) {
		return &ListCommandsOutput{Commands: []Command{}}, nil
	}

	end := startIdx + int(maxResults)

	var nextToken string

	if end < len(all) {
		nextToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return &ListCommandsOutput{
		Commands:  all[startIdx:end],
		NextToken: nextToken,
	}, nil
}

// GetCommandInvocation returns the stored invocation for the given command and instance.
func (b *InMemoryBackend) GetCommandInvocation(
	ctx context.Context,
	input *GetCommandInvocationInput,
) (*GetCommandInvocationOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("GetCommandInvocation")
	defer b.mu.Unlock()

	if !b.commandsStore(region).Has(input.CommandID) {
		return nil, ErrCommandNotFound
	}

	b.materializeCommandLocked(region, input.CommandID, UnixTimeFloat(timeNow()))

	for _, inv := range b.commandInvocationsStore(region)[input.CommandID] {
		if inv.InstanceID == input.InstanceID {
			return &GetCommandInvocationOutput{
				CommandID:             input.CommandID,
				InstanceID:            input.InstanceID,
				DocumentName:          inv.DocumentName,
				Status:                inv.Status,
				StatusDetails:         inv.StatusDetails,
				StandardOutputContent: inv.StandardOutputContent,
				StandardErrorContent:  inv.StandardErrorContent,
				StandardOutputURL:     inv.StandardOutputURL,
				StandardErrorURL:      inv.StandardErrorURL,
				Comment:               inv.Comment,
			}, nil
		}
	}

	return nil, ErrCommandNotFound
}

// ListCommandInvocations returns invocations for a given command.
func (b *InMemoryBackend) ListCommandInvocations(
	ctx context.Context,
	input *ListCommandInvocationsInput,
) (*ListCommandInvocationsOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("ListCommandInvocations")
	defer b.mu.Unlock()

	b.materializeCommandsLocked(region, UnixTimeFloat(timeNow()))

	all := make([]CommandInvocation, 0, len(b.commandInvocationsStore(region)))
	for cmdID, invs := range b.commandInvocationsStore(region) {
		if input.CommandID != "" && cmdID != input.CommandID {
			continue
		}
		for _, inv := range invs {
			if input.InstanceID != "" && inv.InstanceID != input.InstanceID {
				continue
			}
			all = append(all, inv)
		}
	}

	sort.Slice(all, func(i, j int) bool {
		if all[i].CommandID != all[j].CommandID {
			return all[i].CommandID < all[j].CommandID
		}

		return all[i].InstanceID < all[j].InstanceID
	})

	startIdx := parseNextToken(input.NextToken)

	maxResults := int64(defaultListDocMaxResults)
	if input.MaxResults != nil && *input.MaxResults > 0 {
		maxResults = *input.MaxResults
	}

	if startIdx >= len(all) {
		return &ListCommandInvocationsOutput{CommandInvocations: []CommandInvocation{}}, nil
	}

	end := startIdx + int(maxResults)

	var nextToken string

	if end < len(all) {
		nextToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return &ListCommandInvocationsOutput{
		CommandInvocations: all[startIdx:end],
		NextToken:          nextToken,
	}, nil
}

// CancelCommand cancels a running command (sets status to Cancelled).
func (b *InMemoryBackend) CancelCommand(
	ctx context.Context,
	input *CancelCommandInput,
) (*CancelCommandOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("CancelCommand")
	defer b.mu.Unlock()

	cmdTable := b.commandsStore(region)
	cmdPtr, exists := cmdTable.Get(input.CommandID)
	if !exists {
		return nil, ErrCommandNotFound
	}

	cmd := *cmdPtr
	cmd.Status = commandStatusCancelled
	cmdTable.Put(&cmd)

	invStore := b.commandInvocationsStore(region)
	invs := invStore[input.CommandID]
	for i := range invs {
		invs[i].Status = commandStatusCancelled
		invs[i].StatusDetails = commandStatusCancelled
	}
	invStore[input.CommandID] = invs

	return &CancelCommandOutput{}, nil
}
