package ssm

import (
	"context"
	"slices"
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
		DocumentVersion:    input.DocumentVersion,
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
		ServiceRole:        input.ServiceRoleArn,
		MaxConcurrency:     input.MaxConcurrency,
		MaxErrors:          input.MaxErrors,
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
			DocumentVersion:   input.DocumentVersion,
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
	finalCmd.TargetCount, finalCmd.CompletedCount, finalCmd.ErrorCount = commandCounts(invocations)

	return &SendCommandOutput{Command: finalCmd}, nil
}

// commandCounts computes real types.Command members TargetCount/
// CompletedCount/ErrorCount (api_op_SendCommand.go/api_op_ListCommands.go)
// from a command's invocations rather than storing them redundantly, so
// they can never drift out of sync with the invocations they summarize.
func commandCounts(invs []CommandInvocation) (int32, int32, int32) {
	var completed, errorCount int32

	for _, inv := range invs {
		switch inv.Status {
		case commandStatusSuccess:
			completed++
		case commandStatusFailed:
			completed++
			errorCount++
		}
	}

	target := int32(len(invs)) // #nosec G115 -- bounded by one SendCommand's InstanceIds, never near int32 max

	return target, completed, errorCount
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
	completionTime := UnixTimeFloat(time.Now())

	for i := range invs {
		final := invs[i].finalStatus
		if final == "" {
			final = commandStatusSuccess
		}

		invs[i].Status = final
		invs[i].StatusDetails = final
		invs[i].StandardOutputContent = invs[i].pendingStdout
		invs[i].StandardErrorContent = invs[i].pendingStderr
		invs[i].executionEndUnix = completionTime

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
	invStore := b.commandInvocationsStore(region)
	all := make([]Command, 0, cmdTable.Len())
	for _, cmdPtr := range cmdTable.All() {
		if input.CommandID != "" && cmdPtr.CommandID != input.CommandID {
			continue
		}
		if input.InstanceID != "" && !slices.Contains(cmdPtr.InstanceIDs, input.InstanceID) {
			continue
		}
		cmd := *cmdPtr
		cmd.TargetCount, cmd.CompletedCount, cmd.ErrorCount = commandCounts(invStore[cmd.CommandID])
		all = append(all, cmd)
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
			out := &GetCommandInvocationOutput{
				CommandID:              input.CommandID,
				InstanceID:             input.InstanceID,
				DocumentName:           inv.DocumentName,
				DocumentVersion:        inv.DocumentVersion,
				PluginName:             input.PluginName,
				Status:                 inv.Status,
				StatusDetails:          inv.StatusDetails,
				StandardOutputContent:  inv.StandardOutputContent,
				StandardErrorContent:   inv.StandardErrorContent,
				StandardOutputURL:      inv.StandardOutputURL,
				StandardErrorURL:       inv.StandardErrorURL,
				Comment:                inv.Comment,
				ExecutionStartDateTime: formatCommandTime(inv.RequestedDateTime),
			}
			if inv.executionEndUnix != 0 {
				out.ExecutionEndDateTime = formatCommandTime(inv.executionEndUnix)
			}

			return out, nil
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

	// InstanceIds scopes cancellation to specific managed nodes -- if
	// omitted, every node the command was requested on is canceled
	// (api_op_CancelCommand.go: "If not provided, the command is canceled
	// on every node on which it was requested").
	invStore := b.commandInvocationsStore(region)
	invs := invStore[input.CommandID]
	allCancelled := true

	for i := range invs {
		if len(input.InstanceIDs) > 0 && !slices.Contains(input.InstanceIDs, invs[i].InstanceID) {
			if invs[i].Status != commandStatusCancelled {
				allCancelled = false
			}

			continue
		}

		invs[i].Status = commandStatusCancelled
		invs[i].StatusDetails = commandStatusCancelled
	}

	invStore[input.CommandID] = invs

	if allCancelled {
		cmd := *cmdPtr
		cmd.Status = commandStatusCancelled
		cmdTable.Put(&cmd)
	}

	return &CancelCommandOutput{}, nil
}

// formatCommandTime renders a Unix-seconds float64 as the ISO 8601 string
// GetCommandInvocationOutput's ExecutionStartDateTime/ExecutionEndDateTime
// use (api_op_GetCommandInvocation.go:90-109) -- these two members are real
// wire strings, not epoch numbers like every other timestamp in this
// package.
func formatCommandTime(unixSeconds float64) string {
	return time.Unix(int64(unixSeconds), 0).UTC().Format(time.RFC3339)
}
