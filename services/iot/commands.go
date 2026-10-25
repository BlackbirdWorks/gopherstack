package iot

import (
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// IoTCommand represents an AWS IoT command.
//
// MandatoryParameters is optional (real GetCommandOutput has no "This member
// is required" on it -- iot@v1.83.0 api_op_GetCommand.go:74-75), so a
// command created without any legitimately omits the key. Each entry is
// stored opaquely (whatever JSON object the caller sent under
// "mandatoryParameters") rather than modeled field-by-field --
// types.CommandParameter's Value/DefaultValue are their own deep union
// type -- and echoed back byte-for-byte, matching real caller-supplied
// state instead of fabricating a shape.
//
//nolint:revive // IoTCommand is intentional to maintain AWS API naming clarity
type IoTCommand struct {
	Tags                map[string]string `json:"tags,omitempty"`
	Payload             map[string]any    `json:"payload,omitempty"`
	CommandARN          string            `json:"commandArn"`
	CommandID           string            `json:"commandId"`
	DisplayName         string            `json:"displayName,omitempty"`
	Description         string            `json:"description,omitempty"`
	Namespace           string            `json:"namespace,omitempty"`
	PayloadTemplate     string            `json:"payloadTemplate,omitempty"`
	RoleARN             string            `json:"roleArn,omitempty"`
	Preprocessor        map[string]any    `json:"preprocessor,omitempty"`
	MandatoryParameters []map[string]any  `json:"mandatoryParameters,omitempty"`
	CreationDate        float64           `json:"creationDate,omitempty"`
	LastUpdated         float64           `json:"lastUpdatedAt,omitempty"`
	Deprecated          bool              `json:"deprecated"`
	PendingDeletion     bool              `json:"pendingDeletion"`
}

func cloneIoTCommand(cmd *IoTCommand) *IoTCommand {
	cp := *cmd
	cp.Tags = make(map[string]string, len(cmd.Tags))
	maps.Copy(cp.Tags, cmd.Tags)
	cp.Payload = make(map[string]any, len(cmd.Payload))
	maps.Copy(cp.Payload, cmd.Payload)
	cp.Preprocessor = maps.Clone(cmd.Preprocessor)
	cp.MandatoryParameters = append([]map[string]any(nil), cmd.MandatoryParameters...)

	return &cp
}

func (b *InMemoryBackend) commandARN(id string) string {
	return arn.Build("iot", b.region, b.accountID, fmt.Sprintf("command/%s", id))
}

// AddCommandInternal seeds an IoTCommand with a caller-chosen CreationDate
// directly into the backend for testing (mirrors AddCommandExecutionInternal/
// AddAuditTaskInternal), letting tests control ListCommands' sortOrder
// without depending on real-clock second-resolution timing.
func (b *InMemoryBackend) AddCommandInternal(cmd IoTCommand) {
	b.mu.Lock("AddCommandInternal")
	defer b.mu.Unlock()

	if cmd.CommandARN == "" {
		cmd.CommandARN = b.commandARN(cmd.CommandID)
	}

	cp := cloneIoTCommand(&cmd)
	b.commands.Put(cp)
}

// CommandExtras holds CreateCommand's dynamic-command members.
type CommandExtras struct {
	Preprocessor    map[string]any
	PayloadTemplate string
	RoleARN         string
}

func (b *InMemoryBackend) CreateCommand(
	id, displayName, description, namespace string,
	payload map[string]any,
	mandatoryParameters []map[string]any,
	tags map[string]string,
	extras CommandExtras,
) (*IoTCommand, error) {
	b.mu.Lock("CreateCommand")
	defer b.mu.Unlock()

	if b.commands.Has(id) {
		return nil, fmt.Errorf("command %q already exists: %w", id, ErrAlreadyExists)
	}
	now := float64(time.Now().Unix())
	cmd := &IoTCommand{
		CommandID:           id,
		CommandARN:          b.commandARN(id),
		DisplayName:         displayName,
		Description:         description,
		Namespace:           namespace,
		Tags:                make(map[string]string),
		Payload:             make(map[string]any),
		CreationDate:        now,
		LastUpdated:         now,
		MandatoryParameters: append([]map[string]any(nil), mandatoryParameters...),
		PayloadTemplate:     extras.PayloadTemplate,
		RoleARN:             extras.RoleARN,
		Preprocessor:        maps.Clone(extras.Preprocessor),
	}
	maps.Copy(cmd.Tags, tags)
	maps.Copy(cmd.Payload, payload)
	b.commands.Put(cmd)
	b.putResourceTagsLocked(cmd.CommandARN, cmd.Tags)

	return cloneIoTCommand(cmd), nil
}

func (b *InMemoryBackend) GetCommand(id string) (*IoTCommand, error) {
	b.mu.RLock("GetCommand")
	defer b.mu.RUnlock()

	cmd, ok := b.commands.Get(id)
	if !ok {
		return nil, fmt.Errorf("command %q not found: %w", id, ErrResourceNotFound)
	}

	return cloneIoTCommand(cmd), nil
}

func (b *InMemoryBackend) UpdateCommand(id, displayName, description string, deprecated bool) error {
	b.mu.Lock("UpdateCommand")
	defer b.mu.Unlock()

	cmd, ok := b.commands.Get(id)
	if !ok {
		return fmt.Errorf("command %q not found: %w", id, ErrResourceNotFound)
	}
	if displayName != "" {
		cmd.DisplayName = displayName
	}
	if description != "" {
		cmd.Description = description
	}
	cmd.Deprecated = deprecated
	cmd.LastUpdated = float64(time.Now().Unix())

	return nil
}

func (b *InMemoryBackend) DeleteCommand(id string) error {
	b.mu.Lock("DeleteCommand")
	defer b.mu.Unlock()

	if !b.commands.Has(id) {
		return fmt.Errorf("command %q not found: %w", id, ErrResourceNotFound)
	}
	b.commands.Delete(id)
	delete(b.resourceTags, b.commandARN(id))

	return nil
}

func (b *InMemoryBackend) ListCommands() []*IoTCommand {
	b.mu.RLock("ListCommands")
	defer b.mu.RUnlock()

	items := b.commands.Snapshot()
	out := make([]*IoTCommand, 0, len(items))
	for _, v := range items {
		out = append(out, cloneIoTCommand(v))
	}

	return out
}

// CommandStatusReason is a command execution's statusReason.
type CommandStatusReason struct {
	ReasonCode        string `json:"reasonCode"`
	ReasonDescription string `json:"reasonDescription,omitempty"`
}

// IoTCommandExecution represents an execution of an IoT command.
//
//nolint:revive // IoTCommandExecution is intentional to maintain AWS API naming clarity
type IoTCommandExecution struct {
	Parameters       map[string]map[string]any `json:"parameters,omitempty"`
	Result           map[string]map[string]any `json:"result,omitempty"`
	StatusReason     *CommandStatusReason      `json:"statusReason,omitempty"`
	CommandARN       string                    `json:"commandArn"`
	ExecutionID      string                    `json:"executionId"`
	ThingARN         string                    `json:"thingArn"`
	Status           string                    `json:"status"`
	ClientToken      string                    `json:"clientToken,omitempty"`
	CreationDate     float64                   `json:"createdAt,omitempty"`
	StartedAt        float64                   `json:"startedAt,omitempty"`
	CompletedAt      float64                   `json:"completedAt,omitempty"`
	LastUpdatedAt    float64                   `json:"lastUpdatedAt,omitempty"`
	TimeoutSeconds   int64                     `json:"executionTimeoutSeconds,omitempty"`
	TimedOutByDevice bool                      `json:"timedOutByDevice,omitempty"`
}
