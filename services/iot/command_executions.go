package iot

import (
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// Command execution statuses.
const (
	cmdExecCreated    = "CREATED"
	cmdExecInProgress = "IN_PROGRESS"
	cmdExecSucceeded  = "SUCCEEDED"
	cmdExecFailed     = "FAILED"
	cmdExecRejected   = "REJECTED"
	cmdExecTimedOut   = "TIMED_OUT"
)

const (
	cmdDefaultTimeoutSeconds = 10
	cmdMaxTimeoutSeconds     = 12 * 60 * 60
	cmdReasonCodeMax         = 64
	cmdReasonDescMax         = 1024
	cmdReasonNoResponse      = "$NO_RESPONSE_FROM_DEVICE"
	cmdTemplatePrefix        = "${aws:iot:commandexecution::parameter:"
	cmdArnParts              = 6
)

var errCommandCondition = errors.New("unsupported value condition")

var cmdReasonCodeRE = regexp.MustCompile(`^[A-Z0-9_-]+$`)

// ErrCommandExecutionConflict is a ConflictException on a command execution.
var ErrCommandExecutionConflict = errors.New("command execution conflict")

// StartCommandExecutionInput is a StartCommandExecution request.
type StartCommandExecutionInput struct {
	TimeoutSeconds *int64
	Parameters     map[string]map[string]any
	CommandARN     string
	TargetARN      string
	ClientToken    string
}

// StartedCommandExecution is what the broker needs to deliver a started execution to its device.
type StartedCommandExecution struct {
	ExecutionID string
	DeviceKind  string
	DeviceID    string
	Format      string
	Payload     []byte
}

// CommandExecutionUpdate is a device-reported status/result for an execution.
type CommandExecutionUpdate struct {
	Result       map[string]map[string]any
	StatusReason *CommandStatusReason
	ExecutionID  string
	DeviceKind   string
	DeviceID     string
	Status       string
}

func cloneCommandExecution(ex *IoTCommandExecution) *IoTCommandExecution {
	cp := *ex
	cp.Parameters = maps.Clone(ex.Parameters)
	cp.Result = maps.Clone(ex.Result)

	if ex.StatusReason != nil {
		r := *ex.StatusReason
		cp.StatusReason = &r
	}

	return &cp
}

func cmdNowEpoch() float64 { return awstime.Epoch(time.Now()) }

// sweepCommandExecutionsLocked times out executions whose deadline passed; the cloud-set TIMED_OUT stays non-terminal.
func (b *InMemoryBackend) sweepCommandExecutionsLocked() {
	now := cmdNowEpoch()

	for _, ex := range b.commandExecutions {
		if ex.TimeoutSeconds <= 0 || (ex.Status != cmdExecCreated && ex.Status != cmdExecInProgress) {
			continue
		}

		deadline := ex.CreationDate + float64(ex.TimeoutSeconds)
		if now < deadline {
			continue
		}

		ex.Status = cmdExecTimedOut
		ex.LastUpdatedAt = deadline
		ex.StatusReason = &CommandStatusReason{ReasonCode: cmdReasonNoResponse}
	}
}

// parseCommandTargetARN splits arn:...:thing/NAME or client/NAME into kind (things|clients) and id.
func parseCommandTargetARN(targetARN string) (string, string, bool) {
	parts := strings.SplitN(targetARN, ":", cmdArnParts)
	if len(parts) != cmdArnParts || parts[0] != keyArn || parts[2] != "iot" {
		return "", "", false
	}

	kind, id, ok := strings.Cut(parts[5], "/")
	if !ok || id == "" {
		return "", "", false
	}

	switch kind {
	case keyThing:
		return keyThings, id, true
	case "client":
		return "clients", id, true
	}

	return "", "", false
}

func commandIDFromARN(commandARN string) (string, bool) {
	parts := strings.SplitN(commandARN, ":", cmdArnParts)
	if len(parts) != cmdArnParts || parts[0] != keyArn || parts[2] != "iot" {
		return "", false
	}

	id, ok := strings.CutPrefix(parts[5], "command/")

	return id, ok && id != ""
}

// StartCommandExecution creates a CREATED execution and returns what must be published to the device.
func (b *InMemoryBackend) StartCommandExecution(in *StartCommandExecutionInput) (*StartedCommandExecution, error) {
	commandID, ok := commandIDFromARN(in.CommandARN)
	if !ok {
		return nil, fmt.Errorf("%w: invalid commandArn", ErrValidation)
	}

	kind, deviceID, ok := parseCommandTargetARN(in.TargetARN)
	if !ok {
		return nil, fmt.Errorf("%w: invalid targetArn", ErrValidation)
	}

	timeout := int64(cmdDefaultTimeoutSeconds)

	if in.TimeoutSeconds != nil {
		timeout = *in.TimeoutSeconds
		if timeout < 1 || timeout > cmdMaxTimeoutSeconds {
			return nil, fmt.Errorf(
				"%w: executionTimeoutSeconds must be between 1 and %d",
				ErrValidation,
				cmdMaxTimeoutSeconds,
			)
		}
	}

	b.mu.Lock("StartCommandExecution")
	defer b.mu.Unlock()

	cmd, found := b.commands.Get(commandID)
	if !found {
		return nil, fmt.Errorf("command %q not found: %w", commandID, ErrResourceNotFound)
	}

	if cmd.Deprecated {
		return nil, fmt.Errorf("%w: command %q is deprecated", ErrValidation, commandID)
	}

	params, err := resolveCommandParameters(cmd, in.Parameters)
	if err != nil {
		return nil, err
	}

	payload, format, err := renderCommandPayload(cmd, params)
	if err != nil {
		return nil, err
	}

	if replay := b.findCommandExecutionByTokenLocked(in); replay != nil {
		if replay.CommandARN != in.CommandARN || replay.ThingARN != in.TargetARN ||
			replay.TimeoutSeconds != timeout || !paramsEqual(replay.Parameters, params) {
			return nil, fmt.Errorf(
				"%w: client token already used with different parameters",
				ErrCommandExecutionConflict,
			)
		}

		return &StartedCommandExecution{ExecutionID: replay.ExecutionID}, nil
	}

	now := cmdNowEpoch()
	ex := &IoTCommandExecution{
		CommandARN:     cmd.CommandARN,
		ExecutionID:    uuid.NewString(),
		ThingARN:       in.TargetARN,
		Status:         cmdExecCreated,
		ClientToken:    in.ClientToken,
		Parameters:     params,
		CreationDate:   now,
		StartedAt:      now,
		LastUpdatedAt:  now,
		TimeoutSeconds: timeout,
	}
	b.commandExecutions[b.commandExecutionKey(commandID, ex.ExecutionID)] = ex

	return &StartedCommandExecution{
		ExecutionID: ex.ExecutionID, DeviceKind: kind, DeviceID: deviceID, Format: format, Payload: payload,
	}, nil
}

func (b *InMemoryBackend) findCommandExecutionByTokenLocked(in *StartCommandExecutionInput) *IoTCommandExecution {
	if in.ClientToken == "" {
		return nil
	}

	for _, ex := range b.commandExecutions {
		if ex.ClientToken == in.ClientToken {
			return ex
		}
	}

	return nil
}

func paramsEqual(a, c map[string]map[string]any) bool {
	if len(a) != len(c) {
		return false
	}

	for k, v := range a {
		if fmt.Sprint(v) != fmt.Sprint(c[k]) {
			return false
		}
	}

	return true
}

// commandParamValue returns the single typed member of a CommandParameterValue.
func commandParamValue(v map[string]any) (string, any, bool) {
	if len(v) != 1 {
		return "", nil, false
	}

	for k, val := range v {
		return k, val, true
	}

	return "", nil, false
}

func commandParamTypeKey(typ string) string {
	switch typ {
	case "STRING":
		return "S"
	case "INTEGER":
		return "I"
	case "LONG":
		return "L"
	case "DOUBLE":
		return "D"
	case "UNSIGNEDLONG":
		return "UL"
	case "BOOLEAN":
		return "B"
	case "BINARY":
		return keyBIN
	}

	return ""
}

// resolveCommandParameters applies request > command value > command default, then checks types and conditions.
func resolveCommandParameters(cmd *IoTCommand, req map[string]map[string]any) (map[string]map[string]any, error) {
	declared := map[string]map[string]any{}

	for _, p := range cmd.MandatoryParameters {
		if name, _ := p["name"].(string); name != "" {
			declared[name] = p
		}
	}

	for name := range req {
		if _, ok := declared[name]; !ok {
			return nil, fmt.Errorf("%w: parameter %q is not defined by the command", ErrValidation, name)
		}
	}

	out := map[string]map[string]any{}

	for name, p := range declared {
		val, has := req[name]
		if !has {
			val, has = paramValueMap(p["value"])
		}

		if !has {
			val, has = paramValueMap(p["defaultValue"])
		}

		if !has {
			return nil, fmt.Errorf("%w: no value for parameter %q", ErrValidation, name)
		}

		if err := checkCommandParameter(name, p, val); err != nil {
			return nil, err
		}

		out[name] = val
	}

	return out, nil
}

func paramValueMap(v any) (map[string]any, bool) {
	m, ok := v.(map[string]any)

	return m, ok && len(m) > 0
}

func checkCommandParameter(name string, decl, val map[string]any) error {
	key, raw, ok := commandParamValue(val)
	if !ok {
		return fmt.Errorf("%w: parameter %q must set exactly one value member", ErrValidation, name)
	}

	if typ, _ := decl["type"].(string); typ != "" && commandParamTypeKey(typ) != key {
		return fmt.Errorf("%w: parameter %q must be of type %s", ErrValidation, name, typ)
	}

	conds, _ := decl["valueConditions"].([]any)
	for _, c := range conds {
		cond, _ := c.(map[string]any)

		okCond, err := evalCommandCondition(key, raw, cond)
		if err != nil {
			return fmt.Errorf("%w: parameter %q: %w", ErrValidation, name, err)
		}

		if !okCond {
			return fmt.Errorf("%w: parameter %q does not satisfy its value condition", ErrValidation, name)
		}
	}

	return nil
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case string:
		f, err := strconv.ParseFloat(n, 64)

		return f, err == nil
	}

	return 0, false
}

func evalCommandCondition(key string, raw any, cond map[string]any) (bool, error) {
	op, _ := cond["comparisonOperator"].(string)
	operand, _ := cond["operand"].(map[string]any)

	if key == "S" {
		return evalStringCondition(op, raw, operand)
	}

	n, isNum := toFloat(raw)
	if !isNum || key == "B" || key == keyBIN {
		return false, fmt.Errorf("%w: not supported for this type", errCommandCondition)
	}

	return evalNumberCondition(op, n, operand)
}

func evalStringCondition(op string, raw any, operand map[string]any) (bool, error) {
	s, _ := raw.(string)
	one, _ := operand["string"].(string)
	set := stringList(operand["strings"])

	switch op {
	case "EQUALS":
		return s == one, nil
	case "NOT_EQUALS":
		return s != one, nil
	case "IN_SET":
		return slices.Contains(set, s), nil
	case "NOT_IN_SET":
		return !slices.Contains(set, s), nil
	}

	return false, fmt.Errorf("%w: operator %s on strings", errCommandCondition, op)
}

func stringList(v any) []string {
	items, _ := v.([]any)
	out := make([]string, 0, len(items))

	for _, i := range items {
		if s, ok := i.(string); ok {
			out = append(out, s)
		}
	}

	return out
}

func evalNumberCondition(op string, n float64, operand map[string]any) (bool, error) {
	one, _ := toFloat(operand["number"])

	switch op {
	case "EQUALS":
		return n == one, nil
	case "NOT_EQUALS":
		return n != one, nil
	case "GREATER_THAN":
		return n > one, nil
	case "GREATER_THAN_EQUALS":
		return n >= one, nil
	case "LESS_THAN":
		return n < one, nil
	case "LESS_THAN_EQUALS":
		return n <= one, nil
	}

	return evalNumberSetCondition(op, n, operand)
}

func evalNumberSetCondition(op string, n float64, operand map[string]any) (bool, error) {
	rng, _ := operand["numberRange"].(map[string]any)
	lo, _ := toFloat(rng["min"])
	hi, _ := toFloat(rng["max"])
	inSet := false

	for _, s := range stringList(operand["numbers"]) {
		if f, err := strconv.ParseFloat(s, 64); err == nil && f == n {
			inSet = true
		}
	}

	switch op {
	case "IN_RANGE":
		return n >= lo && n <= hi, nil
	case "NOT_IN_RANGE":
		return n < lo || n > hi, nil
	case "IN_SET":
		return inSet, nil
	case "NOT_IN_SET":
		return !inSet, nil
	}

	return false, fmt.Errorf("%w: operator %s on numbers", errCommandCondition, op)
}

func renderParamValue(v map[string]any) string {
	_, raw, _ := commandParamValue(v)

	switch t := raw.(type) {
	case string:
		return t
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	}

	return fmt.Sprint(raw)
}

const (
	contentTypeJSON = "application/json"
	contentTypeCBOR = "application/cbor"
)

// renderCommandPayload returns the request payload and the topic format ("json", "cbor" or "").
func renderCommandPayload(cmd *IoTCommand, params map[string]map[string]any) ([]byte, string, error) {
	if cmd.PayloadTemplate != "" {
		if out, _ := cmd.Preprocessor["awsJsonSubstitution"].(map[string]any); out != nil {
			if f, _ := out["outputFormat"].(string); f == "CBOR" {
				return nil, "", fmt.Errorf("%w: CBOR output of payload templates is not supported", ErrValidation)
			}
		}

		tpl := cmd.PayloadTemplate
		for name, v := range params {
			tpl = strings.ReplaceAll(tpl, cmdTemplatePrefix+name+"}", renderParamValue(v))
		}

		return []byte(tpl), keyJSON, nil
	}

	content, _ := cmd.Payload["content"].(string)
	contentType, _ := cmd.Payload["contentType"].(string)

	raw, err := base64.StdEncoding.DecodeString(content)
	if err != nil {
		return nil, "", fmt.Errorf("%w: command payload content is not valid base64", ErrValidation)
	}

	switch contentType {
	case contentTypeJSON:
		return raw, keyJSON, nil
	case contentTypeCBOR:
		return raw, keyCBOR, nil
	}

	return raw, "", nil
}

// ListCommandExecutionsFilter narrows ListCommandExecutionsByFilter.
type ListCommandExecutionsFilter struct {
	CommandARN, TargetARN, Status, Namespace string
}

// GetCommandExecutionByID looks up an execution by id, optionally scoped by target ARN.
func (b *InMemoryBackend) GetCommandExecutionByID(executionID, targetARN string) (*IoTCommandExecution, error) {
	b.mu.Lock("GetCommandExecutionByID")
	defer b.mu.Unlock()

	b.sweepCommandExecutionsLocked()

	for _, ex := range b.commandExecutions {
		if ex.ExecutionID == executionID && (targetARN == "" || ex.ThingARN == targetARN) {
			return cloneCommandExecution(ex), nil
		}
	}

	return nil, fmt.Errorf("command execution %q not found: %w", executionID, ErrResourceNotFound)
}

// GetCommandExecution looks up an execution by command id and execution id.
func (b *InMemoryBackend) GetCommandExecution(commandID, executionID string) (*IoTCommandExecution, error) {
	b.mu.Lock("GetCommandExecution")
	defer b.mu.Unlock()

	b.sweepCommandExecutionsLocked()

	ex, ok := b.commandExecutions[b.commandExecutionKey(commandID, executionID)]
	if !ok {
		return nil, fmt.Errorf("command execution %q/%q not found: %w", commandID, executionID, ErrResourceNotFound)
	}

	return cloneCommandExecution(ex), nil
}

// ListCommandExecutions lists the executions of one command by execution id.
func (b *InMemoryBackend) ListCommandExecutions(commandID string) []*IoTCommandExecution {
	b.mu.Lock("ListCommandExecutions")
	defer b.mu.Unlock()

	b.sweepCommandExecutionsLocked()

	prefix := commandID + "/"

	var out []*IoTCommandExecution

	for k, ex := range b.commandExecutions {
		if strings.HasPrefix(k, prefix) && len(k) > len(prefix) {
			out = append(out, cloneCommandExecution(ex))
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ExecutionID < out[j].ExecutionID })

	return out
}

// ListCommandExecutionsByFilter returns executions matching the optional filters.
func (b *InMemoryBackend) ListCommandExecutionsByFilter(f ListCommandExecutionsFilter) []*IoTCommandExecution {
	b.mu.Lock("ListCommandExecutionsByFilter")
	defer b.mu.Unlock()

	b.sweepCommandExecutionsLocked()

	var out []*IoTCommandExecution

	for _, ex := range b.commandExecutions {
		if f.CommandARN != "" && ex.CommandARN != f.CommandARN {
			continue
		}

		if f.TargetARN != "" && ex.ThingARN != f.TargetARN {
			continue
		}

		if f.Status != "" && ex.Status != f.Status {
			continue
		}

		if f.Namespace != "" && b.commandNamespaceLocked(ex.CommandARN) != f.Namespace {
			continue
		}

		out = append(out, cloneCommandExecution(ex))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ExecutionID < out[j].ExecutionID })

	return out
}

func (b *InMemoryBackend) commandNamespaceLocked(commandARN string) string {
	id, ok := commandIDFromARN(commandARN)
	if !ok {
		return ""
	}

	cmd, found := b.commands.Get(id)
	if !found {
		return ""
	}

	if cmd.Namespace == "" {
		return "AWS-IoT"
	}

	return cmd.Namespace
}

// HasCommandExecution reports whether the backend holds the execution.
func (b *InMemoryBackend) HasCommandExecution(executionID string) bool {
	b.mu.RLock("HasCommandExecution")
	defer b.mu.RUnlock()

	for _, ex := range b.commandExecutions {
		if ex.ExecutionID == executionID {
			return true
		}
	}

	return false
}

// UpdateCommandExecution applies a device-reported status, reason and result.
func (b *InMemoryBackend) UpdateCommandExecution(u *CommandExecutionUpdate) error {
	if err := validateCommandUpdate(u); err != nil {
		return err
	}

	b.mu.Lock("UpdateCommandExecution")
	defer b.mu.Unlock()

	b.sweepCommandExecutionsLocked()

	var ex *IoTCommandExecution

	for _, e := range b.commandExecutions {
		if e.ExecutionID == u.ExecutionID {
			ex = e

			break
		}
	}

	if ex == nil || !commandTargetMatches(ex.ThingARN, u.DeviceKind, u.DeviceID) {
		return fmt.Errorf("command execution %q: %w", u.ExecutionID, ErrResourceNotFound)
	}

	if err := checkCommandTransition(ex, u.Status); err != nil {
		return err
	}

	now := cmdNowEpoch()
	ex.Status = u.Status
	ex.LastUpdatedAt = now
	ex.StatusReason = u.StatusReason

	if u.Result != nil {
		ex.Result = u.Result
	}

	switch u.Status {
	case cmdExecSucceeded, cmdExecFailed, cmdExecRejected:
		ex.CompletedAt = now
	case cmdExecTimedOut:
		ex.CompletedAt = now
		ex.TimedOutByDevice = true
	}

	return nil
}

func commandTargetMatches(targetARN, kind, id string) bool {
	k, i, ok := parseCommandTargetARN(targetARN)

	return ok && k == kind && i == id
}

func checkCommandTransition(ex *IoTCommandExecution, to string) error {
	switch ex.Status {
	case cmdExecSucceeded, cmdExecFailed, cmdExecRejected:
		return fmt.Errorf("%w: command execution is already %s", ErrTerminalState, ex.Status)
	case cmdExecTimedOut:
		if ex.TimedOutByDevice || to == cmdExecInProgress {
			return fmt.Errorf("%w: cannot move %s to %s", ErrInvalidStateTransition, ex.Status, to)
		}
	}

	return nil
}

func validateCommandUpdate(u *CommandExecutionUpdate) error {
	switch u.Status {
	case cmdExecInProgress, cmdExecSucceeded, cmdExecFailed, cmdExecRejected, cmdExecTimedOut:
	default:
		return fmt.Errorf("%w: status %q cannot be reported by a device", ErrInvalidStateTransition, u.Status)
	}

	if r := u.StatusReason; r != nil {
		if len(r.ReasonCode) > cmdReasonCodeMax || !cmdReasonCodeRE.MatchString(r.ReasonCode) {
			return fmt.Errorf("%w: reasonCode must match [A-Z0-9_-]+ and be at most %d characters",
				ErrValidation, cmdReasonCodeMax)
		}

		if len(r.ReasonDescription) > cmdReasonDescMax {
			return fmt.Errorf("%w: reasonDescription exceeds %d characters", ErrValidation, cmdReasonDescMax)
		}
	}

	if u.Result != nil {
		if len(u.Result) == 0 {
			return fmt.Errorf("%w: result requires at least one entry", ErrValidation)
		}

		for k, v := range u.Result {
			if _, _, ok := commandParamValue(v); !ok {
				return fmt.Errorf("%w: result entry %q must set exactly one of s, b, bin", ErrValidation, k)
			}
		}
	}

	return nil
}

// DeleteCommandExecution removes an execution that reached a terminal status.
func (b *InMemoryBackend) DeleteCommandExecution(executionID, targetARN string) error {
	b.mu.Lock("DeleteCommandExecution")
	defer b.mu.Unlock()

	b.sweepCommandExecutionsLocked()

	for key, ex := range b.commandExecutions {
		if ex.ExecutionID != executionID || (targetARN != "" && ex.ThingARN != targetARN) {
			continue
		}

		if !isTerminalCommandExecution(ex) {
			return fmt.Errorf("%w: command execution is %s", ErrCommandExecutionConflict, ex.Status)
		}

		delete(b.commandExecutions, key)

		return nil
	}

	return fmt.Errorf("command execution %q not found: %w", executionID, ErrResourceNotFound)
}

func isTerminalCommandExecution(ex *IoTCommandExecution) bool {
	switch ex.Status {
	case cmdExecSucceeded, cmdExecFailed, cmdExecRejected:
		return true
	case cmdExecTimedOut:
		return ex.TimedOutByDevice
	}

	return false
}

// AddCommandExecutionInternal seeds an execution directly (tests have no other way to set timestamps).
func (b *InMemoryBackend) AddCommandExecutionInternal(commandID, executionID string, ex IoTCommandExecution) {
	b.mu.Lock("AddCommandExecutionInternal")
	defer b.mu.Unlock()

	cp := ex
	cp.ExecutionID = executionID
	b.commandExecutions[b.commandExecutionKey(commandID, executionID)] = &cp
}

func (b *InMemoryBackend) commandExecutionKey(commandID, executionID string) string {
	return commandID + "/" + executionID
}
