package ssm

import (
	"cmp"
	"context"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// validMaxConcurrency matches SendCommandInput.MaxConcurrency and
// CreateAssociationInput.MaxConcurrency: an absolute node count with no
// leading zero, or a 1-100% percentage. ssm/2014-11-06/service-2.json
// MaxConcurrency shape: pattern "^([1-9][0-9]*|[1-9][0-9]%|[1-9]%|100%)$",
// min 1, max 7.
var validMaxConcurrency = regexp.MustCompile(`^([1-9][0-9]*|[1-9][0-9]%|[1-9]%|100%)$`)

// validMaxErrors matches SendCommandInput.MaxErrors and
// CreateAssociationInput.MaxErrors: like MaxConcurrency but also allows the
// literal "0" and "0%" (AWS: "If you specify 0, then the system stops
// sending requests after the first error"). ssm/2014-11-06/service-2.json
// MaxErrors shape: pattern "^([1-9][0-9]*|[0]|[1-9][0-9]%|[0-9]%|100%)$",
// min 1, max 7.
var validMaxErrors = regexp.MustCompile(`^([1-9][0-9]*|[0]|[1-9][0-9]%|[0-9]%|100%)$`)

const maxConcurrencyErrorsLen = 7

// validateMaxConcurrency rejects a MaxConcurrency value that doesn't match
// the wire model's pattern. An empty string (unset) is valid.
func validateMaxConcurrency(v string) error {
	if v == "" {
		return nil
	}

	if len(v) > maxConcurrencyErrorsLen || !validMaxConcurrency.MatchString(v) {
		return fmt.Errorf(
			"%w: MaxConcurrency must be an absolute number (e.g. 10) or a percentage (e.g. 10%%)",
			ErrValidationException,
		)
	}

	return nil
}

// validateMaxErrors rejects a MaxErrors value that doesn't match the wire
// model's pattern. An empty string (unset) is valid.
func validateMaxErrors(v string) error {
	if v == "" {
		return nil
	}

	if len(v) > maxConcurrencyErrorsLen || !validMaxErrors.MatchString(v) {
		return fmt.Errorf(
			"%w: MaxErrors must be an absolute number (e.g. 10) or a percentage (e.g. 10%%)",
			ErrValidationException,
		)
	}

	return nil
}

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
	if err := validateMaxConcurrency(input.MaxConcurrency); err != nil {
		return nil, err
	}

	if err := validateMaxErrors(input.MaxErrors); err != nil {
		return nil, err
	}

	region := getRegion(ctx)
	runner := b.commandRunner()

	var containers []ManagedContainer
	if runner != nil && runner.Instances != nil {
		containers = runner.Instances.ContainerInstances(region, nil)
	}

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

	// resolvedInstanceIDs drives the actual invocation set: InstanceIds plus
	// whatever Targets resolves to (AWS: "Targets is required if you don't
	// provide one or more managed node IDs in the call"). A Targets-only
	// caller -- the pattern AWS documents as required for at-scale sends --
	// must still get real invocations, not zero.
	resolvedInstanceIDs := mergeUniqueInstanceIDs(
		input.InstanceIDs,
		commandTargetInstanceIDs(input.Targets, containers),
	)

	cmd := newCommandRecord(cmdID, input, now, now+b.commandExpirySecs)

	b.commandsStore(region).Put(&cmd)

	job := b.planDirectJob(region, runner, containers, input, resolvedInstanceIDs)
	if job != nil {
		job.cmdID = cmdID
	}

	stdout, stderr, finalStatus, exitCode := renderCommandOutput(input.DocumentName, input.Parameters)

	if input.DocumentName == docRunPatchBaseline {
		for _, instanceID := range resolvedInstanceIDs {
			b.applyPatchBaselineOperation(region, instanceID, input.Parameters)
		}
	}

	rendered := renderedOutput{stdout: stdout, stderr: stderr, status: finalStatus, code: exitCode}
	invocations := buildInvocations(cmdID, input, resolvedInstanceIDs, now, rendered, job)
	if b.commandInvocations[region] == nil {
		b.commandInvocations[region] = make(map[string][]CommandInvocation)
	}
	b.commandInvocationsStore(region)[cmdID] = invocations

	// Drive Pending → InProgress immediately so the InProgress window is always
	// observable. When no exec delay is configured the command then completes
	// synchronously (revealing output); otherwise it stays InProgress and is
	// lazily completed by reads once b.commandExecDelaySecs has elapsed.
	b.setCommandStatus(region, cmdID, commandStatusInProgress)

	if job != nil {
		go b.runDirectJob(job)
	}

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

// commandTargetInstanceIDs flattens a SendCommand Targets list into instance IDs. "tag:<key>" targets
// select running managed containers carrying that tag value; any other key's Values are literal IDs.
func commandTargetInstanceIDs(targets []CommandTarget, containers []ManagedContainer) []string {
	ids := make([]string, 0, len(targets))

	for _, t := range targets {
		tagKey, isTag := strings.CutPrefix(t.Key, "tag:")
		if !isTag || containers == nil {
			ids = append(ids, t.Values...)

			continue
		}

		for _, c := range containers {
			if v, ok := c.Tags[tagKey]; ok && slices.Contains(t.Values, v) {
				ids = append(ids, c.InstanceID)
			}
		}
	}

	return ids
}

// mergeUniqueInstanceIDs unions a and b, preserving first-seen order and
// dropping duplicates so a node named in both InstanceIds and Targets is
// only invoked once.
func mergeUniqueInstanceIDs(a, b []string) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))

	for _, id := range slices.Concat(a, b) {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}

	return out
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
		if invs[i].direct {
			continue
		}

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
	hasDirect := false

	for i := range invs {
		if invs[i].direct {
			hasDirect = true

			continue
		}

		final := invs[i].finalStatus
		if final == "" {
			final = commandStatusSuccess
		}

		invs[i].Status = final
		invs[i].StatusDetails = final
		invs[i].StandardOutputContent = invs[i].pendingStdout
		invs[i].StandardErrorContent = invs[i].pendingStderr
		invs[i].executionEndUnix = completionTime
		invs[i].startedUnix = invs[i].RequestedDateTime

		if final != commandStatusSuccess {
			overall = final
		}
	}

	invStore[cmdID] = invs

	if hasDirect {
		cmd.completeAfter = 0
		cmdTable.Put(&cmd)
		b.settleCommandLocked(region, cmdID)

		return
	}

	cmd.Status = overall
	cmd.StatusDetails = overall
	cmd.completeAfter = 0
	cmd.terminalAt = completionTime
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

// filterKeyStatus is the "Status" filter-key literal shared by every
// per-resource filter switch in this package (CommandFilter, OpsItemFilter,
// SessionFilter) -- pulled out once goconst flagged three independent
// literals as a repeated constant.
const filterKeyStatus = "Status"

// matchesCommandFilters applies ListCommands/ListCommandInvocations Filters
// (api_op_ListCommands.go types.CommandFilter doc comment): Status
// (case-insensitive exact match), DocumentName (exact match), InvokedAfter/
// InvokedBefore (RFC3339 timestamp bounds on RequestedDateTime, inclusive
// per "occurring July 7, 2021, and later"). An unparseable timestamp value is
// ignored (filter doesn't exclude). ExecutionStage is ListCommands-only, see commandStageMatches.
func matchesCommandFilters(status, documentName string, requestedDateTime float64, filters []CommandFilter) bool {
	for _, f := range filters {
		switch f.Key {
		case filterKeyStatus:
			if !strings.EqualFold(status, f.Value) {
				return false
			}
		case "DocumentName":
			if documentName != f.Value {
				return false
			}
		case "InvokedAfter":
			if t, err := time.Parse(time.RFC3339, f.Value); err == nil && requestedDateTime < float64(t.Unix()) {
				return false
			}
		case "InvokedBefore":
			if t, err := time.Parse(time.RFC3339, f.Value); err == nil && requestedDateTime > float64(t.Unix()) {
				return false
			}
		}
	}

	return true
}

const (
	commandStageComplete  = "Complete"
	commandStageExecuting = "Executing"
)

// commandStageMatches applies the ListCommands-only ExecutionStage filter: Executing is a
// still-running command, Complete a finished one (types.CommandFilter doc).
func commandStageMatches(status string, filters []CommandFilter) bool {
	complete := status != commandStatusPending && status != commandStatusInProgress

	for _, f := range filters {
		if f.Key != "ExecutionStage" {
			continue
		}

		if (f.Value == commandStageComplete && !complete) || (f.Value == commandStageExecuting && complete) {
			return false
		}
	}

	return true
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
		if !matchesCommandFilters(cmdPtr.Status, cmdPtr.DocumentName, cmdPtr.RequestedDateTime, input.Filters) ||
			!commandStageMatches(cmdPtr.Status, input.Filters) {
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

	cmdPtr, found := b.commandsStore(region).Get(input.CommandID)
	if !found {
		return nil, ErrCommandNotFound
	}

	b.materializeCommandLocked(region, input.CommandID, UnixTimeFloat(timeNow()))

	for _, inv := range b.commandInvocationsStore(region)[input.CommandID] {
		if inv.InstanceID == input.InstanceID {
			if p, ok, err := pluginView(inv, input.PluginName); err != nil {
				return nil, err
			} else if ok {
				inv = p
			}

			out := &GetCommandInvocationOutput{
				ResponseCode:           inv.ResponseCode,
				CloudWatchOutputConfig: cmdPtr.CloudWatchOutputConfig,
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
				ExecutionStartDateTime: formatCommandTime(cmp.Or(inv.startedUnix, inv.RequestedDateTime)),
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

	all := collectInvocations(b.commandInvocationsStore(region), input)

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

		if invs[i].direct && invs[i].Status != commandStatusPending && invs[i].Status != commandStatusInProgress {
			continue
		}

		invs[i].Status = commandStatusCancelled
		invs[i].StatusDetails = commandStatusCancelled
		b.cancelExec(execKey(region, input.CommandID, invs[i].InstanceID))
	}

	invStore[input.CommandID] = invs

	if allCancelled {
		cmd := *cmdPtr
		cmd.Status = commandStatusCancelled
		cmd.terminalAt = UnixTimeFloat(time.Now())
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

// pluginView returns the invocation narrowed to the named plugin; ok is false when the
// invocation has no per-plugin results. A name matching no plugin is InvalidPluginName.
func pluginView(inv CommandInvocation, name string) (CommandInvocation, bool, error) {
	if len(inv.CommandPlugins) == 0 || name == "" {
		return inv, false, nil
	}

	for _, p := range inv.CommandPlugins {
		if p.Name != name {
			continue
		}

		inv.Status, inv.StatusDetails, inv.ResponseCode = p.Status, p.StatusDetails, p.ResponseCode
		inv.StandardOutputContent = truncateChars(p.stdout, stdoutLimitChars)
		inv.StandardErrorContent = truncateChars(p.stderr, stderrLimitChars)
		inv.StandardOutputURL, inv.StandardErrorURL = p.StandardOutputURL, p.StandardErrorURL

		return inv, true, nil
	}

	return inv, false, ErrInvalidPluginName
}

type renderedOutput struct {
	stdout string
	stderr string
	status string
	code   int
}

func buildInvocations(
	cmdID string, input *SendCommandInput, ids []string, now float64, out renderedOutput, job *directJob,
) []CommandInvocation {
	invocations := make([]CommandInvocation, 0, len(ids))

	for _, instanceID := range ids {
		inv := CommandInvocation{
			CommandID:         cmdID,
			InstanceID:        instanceID,
			DocumentName:      input.DocumentName,
			DocumentVersion:   input.DocumentVersion,
			Status:            commandStatusPending,
			StatusDetails:     commandStatusPending,
			RequestedDateTime: now,
			Comment:           input.Comment,
			pendingStdout:     out.stdout,
			pendingStderr:     out.stderr,
			finalStatus:       out.status,
			ResponseCode:      int32(out.code), // #nosec G115 -- parsed from a script exit statement
		}
		if job != nil && job.hasTarget(instanceID) {
			inv.direct, inv.ResponseCode = true, responseCodeNotRun
		}

		invocations = append(invocations, inv)
	}

	return invocations
}

// collectInvocations returns the invocations matching the ListCommandInvocations request.
func collectInvocations(all map[string][]CommandInvocation, input *ListCommandInvocationsInput) []CommandInvocation {
	out := make([]CommandInvocation, 0, len(all))

	for cmdID, invs := range all {
		if input.CommandID != "" && cmdID != input.CommandID {
			continue
		}

		for _, inv := range invs {
			if input.InstanceID != "" && inv.InstanceID != input.InstanceID {
				continue
			}

			if !matchesCommandFilters(inv.Status, inv.DocumentName, inv.RequestedDateTime, input.Filters) {
				continue
			}

			if !input.Details {
				inv.CommandPlugins = nil
			}

			out = append(out, inv)
		}
	}

	return out
}

func newCommandRecord(cmdID string, input *SendCommandInput, now, expires float64) Command {
	timeoutSecs := input.TimeoutSeconds
	if timeoutSecs == 0 {
		timeoutSecs = 3600
	}

	return Command{
		CommandID:              cmdID,
		DocumentName:           input.DocumentName,
		DocumentVersion:        input.DocumentVersion,
		Parameters:             input.Parameters,
		Status:                 commandStatusPending,
		StatusDetails:          commandStatusPending,
		RequestedDateTime:      now,
		ExpiresAfter:           expires,
		InstanceIDs:            input.InstanceIDs,
		Targets:                input.Targets,
		Comment:                input.Comment,
		TimeoutSeconds:         timeoutSecs,
		OutputS3BucketName:     input.OutputS3BucketName,
		OutputS3KeyPrefix:      input.OutputS3KeyPrefix,
		OutputS3Region:         input.OutputS3Region,
		ServiceRole:            input.ServiceRoleArn,
		MaxConcurrency:         input.MaxConcurrency,
		MaxErrors:              input.MaxErrors,
		CloudWatchOutputConfig: input.CloudWatchOutputConfig,
	}
}
