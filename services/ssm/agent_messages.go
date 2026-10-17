package ssm

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// The ec2messages wire shapes below follow the open-source amazon-ssm-agent
// (agent/runcommand/contracts and the vendored ec2messages client); AWS publishes no SDK for them.
const (
	topicSendCommand   = "aws.ssm.sendCommand"
	topicCancelCommand = "aws.ssm.cancelCommand"

	messageIDPrefix = "aws.ssm."
	cancelIDPrefix  = messageIDPrefix + "cancelCommand."

	agentPollWait          = 20 * time.Second
	defaultAgentVisibility = 30 * time.Second
	messageTimeFormat      = "2006-01-02T15:04:05.000Z"
)

// AgentBackend is the ec2messages side of the SSM backend.
type AgentBackend interface {
	AgentRegister(ctx context.Context, destination string)
	AgentGetMessages(ctx context.Context, input *AgentGetMessagesInput) (*AgentGetMessagesOutput, error)
	AgentAcknowledgeMessage(ctx context.Context, messageID string) error
	AgentDeleteMessage(ctx context.Context, messageID string) error
	AgentFailMessage(ctx context.Context, messageID, failureType string) error
	AgentSendReply(ctx context.Context, messageID, payload string) error
}

// AgentMessage is one ec2messages Message.
type AgentMessage struct {
	CreatedDate string `json:"CreatedDate"`
	Destination string `json:"Destination"`
	MessageID   string `json:"MessageId"`
	Payload     string `json:"Payload"`
	Topic       string `json:"Topic"`
}

// AgentGetMessagesInput is the ec2messages GetMessages request.
type AgentGetMessagesInput struct {
	VisibilityTimeout *int64 `json:"VisibilityTimeout,omitempty"`
	Destination       string `json:"Destination"`
	MessagesRequestID string `json:"MessagesRequestId"`
}

// AgentGetMessagesOutput is the ec2messages GetMessages response.
type AgentGetMessagesOutput struct {
	Destination string         `json:"Destination"`
	MessageID   string         `json:"MessageId"`
	Messages    []AgentMessage `json:"Messages"`
}

type agentEnvelope struct {
	visibleAt time.Time
	cmdID     string
	msg       AgentMessage
}

type agentQueue struct {
	wake chan struct{}
	msgs []*agentEnvelope
}

func agentKey(region, instanceID string) string { return region + "/" + instanceID }

// agentQueueLocked returns the instance's queue, creating it. Must be called with b.mu held for writing.
func (b *InMemoryBackend) agentQueueLocked(region, instanceID string) *agentQueue {
	if b.agents == nil {
		b.agents = make(map[string]*agentQueue)
	}

	key := agentKey(region, instanceID)

	q, ok := b.agents[key]
	if !ok {
		q = &agentQueue{wake: make(chan struct{})}
		b.agents[key] = q
	}

	return q
}

func (q *agentQueue) push(m *agentEnvelope) {
	q.msgs = append(q.msgs, m)
	close(q.wake)
	q.wake = make(chan struct{})
}

func sendMessageID(cmdID, instanceID string) string {
	return messageIDPrefix + cmdID + "." + instanceID
}

// parseMessageID splits "aws.ssm.<commandId>.<instanceId>" (or the cancel variant).
func parseMessageID(id string) (string, string, bool, bool) {
	rest, found := strings.CutPrefix(id, messageIDPrefix)
	if !found {
		return "", "", false, false
	}

	rest, cancel := strings.CutPrefix(rest, "cancelCommand.")

	cmdID, instanceID, ok := strings.Cut(rest, ".")

	return cmdID, instanceID, cancel, ok && cmdID != "" && instanceID != ""
}

func messageNow() string { return timeNow().UTC().Format(messageTimeFormat) }

// AgentRegister marks the instance as served by an external SSM agent so SendCommand queues for it.
func (b *InMemoryBackend) AgentRegister(ctx context.Context, destination string) {
	b.mu.Lock("AgentRegister")
	defer b.mu.Unlock()

	b.agentQueueLocked(getRegion(ctx), destination)
}

// AgentGetMessages long-polls for the instance's pending messages.
func (b *InMemoryBackend) AgentGetMessages(
	ctx context.Context,
	input *AgentGetMessagesInput,
) (*AgentGetMessagesOutput, error) {
	if input.Destination == "" {
		return nil, fmt.Errorf("%w: Destination is required", ErrValidationException)
	}

	region := getRegion(ctx)

	visibility := defaultAgentVisibility
	if input.VisibilityTimeout != nil && *input.VisibilityTimeout > 0 {
		visibility = time.Duration(*input.VisibilityTimeout) * time.Second
	}

	out := &AgentGetMessagesOutput{
		Destination: input.Destination, MessageID: input.MessagesRequestID, Messages: []AgentMessage{},
	}

	deadline := time.NewTimer(agentPollWait)
	defer deadline.Stop()

	for {
		msgs, wake := b.takeVisibleMessages(region, input.Destination, visibility)
		if len(msgs) > 0 {
			out.Messages = msgs

			return out, nil
		}

		select {
		case <-wake:
		case <-deadline.C:
			return out, nil
		case <-ctx.Done():
			return out, nil
		}
	}
}

// takeVisibleMessages hides and returns the deliverable messages, or the channel that signals new ones.
func (b *InMemoryBackend) takeVisibleMessages(
	region, instanceID string,
	visibility time.Duration,
) ([]AgentMessage, <-chan struct{}) {
	b.mu.Lock("AgentGetMessages")
	defer b.mu.Unlock()

	q := b.agentQueueLocked(region, instanceID)
	now := timeNow()

	var out []AgentMessage

	live := q.msgs[:0]

	for _, m := range q.msgs {
		if m.cmdID != "" && !b.commandsStore(region).Has(m.cmdID) {
			continue
		}

		live = append(live, m)

		if !m.visibleAt.After(now) {
			m.visibleAt = now.Add(visibility)
			out = append(out, m.msg)
		}
	}

	clear(q.msgs[len(live):])
	q.msgs = live

	return out, q.wake
}

// queueAgentCommandsLocked turns invocations on agent-served instances into pending messages.
// Must be called with b.mu held for writing.
func (b *InMemoryBackend) queueAgentCommandsLocked(
	region string,
	input *SendCommandInput,
	invs []CommandInvocation,
	now float64,
) {
	var payload string

	for i := range invs {
		inv := &invs[i]
		if inv.direct {
			continue
		}

		q, ok := b.agents[agentKey(region, inv.InstanceID)]
		if !ok {
			continue
		}

		if payload == "" {
			payload = b.sendCommandPayload(region, input, inv.CommandID)
		}

		inv.direct, inv.agent, inv.ResponseCode = true, true, responseCodeNotRun
		q.push(&agentEnvelope{
			cmdID: inv.CommandID,
			msg: AgentMessage{
				CreatedDate: time.Unix(int64(now), 0).UTC().Format(messageTimeFormat),
				Destination: inv.InstanceID,
				MessageID:   sendMessageID(inv.CommandID, inv.InstanceID),
				Payload:     payload,
				Topic:       topicSendCommand,
			},
		})
	}
}

func (b *InMemoryBackend) queueAgentCancelLocked(region string, inv CommandInvocation) {
	q, ok := b.agents[agentKey(region, inv.InstanceID)]
	if !ok {
		return
	}

	raw, _ := json.Marshal(map[string]string{"CancelMessageId": sendMessageID(inv.CommandID, inv.InstanceID)})
	q.push(&agentEnvelope{msg: AgentMessage{
		CreatedDate: messageNow(),
		Destination: inv.InstanceID,
		MessageID:   cancelIDPrefix + inv.CommandID + "." + inv.InstanceID,
		Payload:     string(raw),
		Topic:       topicCancelCommand,
	}})
}

// sendCommandPayload renders the SendCommandPayload document the agent executes.
func (b *InMemoryBackend) sendCommandPayload(region string, input *SendCommandInput, cmdID string) string {
	payload := map[string]any{
		"CommandId":    cmdID,
		"DocumentName": input.DocumentName,
		"Parameters":   input.Parameters,
	}

	if content, format, ok := b.commandDocumentContent(region, input.DocumentName, input.DocumentVersion); ok {
		if doc := documentAsJSON(content, format); doc != nil {
			payload["DocumentContent"] = doc
		}
	}

	if input.OutputS3BucketName != "" {
		payload["OutputS3BucketName"] = input.OutputS3BucketName
		payload["OutputS3KeyPrefix"] = input.OutputS3KeyPrefix
	}

	if cw := input.CloudWatchOutputConfig; cw != nil {
		payload["CloudWatchOutputEnabled"] = strconv.FormatBool(cw.CloudWatchOutputEnabled)
		payload["CloudWatchLogGroupName"] = cw.CloudWatchLogGroupName
	}

	raw, _ := json.Marshal(payload)

	return string(raw)
}

func documentAsJSON(content, format string) any {
	var doc any

	var err error
	if format == documentFormatYAML {
		err = yaml.Unmarshal([]byte(content), &doc)
	} else {
		err = json.Unmarshal([]byte(content), &doc)
	}

	if err != nil {
		return nil
	}

	return doc
}

func (b *InMemoryBackend) removeAgentMessageLocked(region, instanceID, messageID string) {
	q, ok := b.agents[agentKey(region, instanceID)]
	if !ok {
		return
	}

	for i, m := range q.msgs {
		if m.msg.MessageID == messageID {
			q.msgs = slices.Delete(q.msgs, i, i+1)

			return
		}
	}
}

func parseAgentMessageID(id string) (string, string, bool, error) {
	cmdID, instanceID, cancel, ok := parseMessageID(id)
	if !ok {
		return "", "", false, fmt.Errorf("%w: invalid MessageId %q", ErrValidationException, id)
	}

	return cmdID, instanceID, cancel, nil
}

// AgentAcknowledgeMessage records that the agent accepted a message; its invocation becomes InProgress.
func (b *InMemoryBackend) AgentAcknowledgeMessage(ctx context.Context, messageID string) error {
	cmdID, instanceID, cancel, err := parseAgentMessageID(messageID)
	if err != nil {
		return err
	}

	region := getRegion(ctx)

	b.mu.Lock("AgentAcknowledgeMessage")
	defer b.mu.Unlock()

	b.removeAgentMessageLocked(region, instanceID, messageID)

	if cancel {
		return nil
	}

	if inv := b.agentInvocationLocked(region, cmdID, instanceID); inv != nil && inv.Status == commandStatusPending {
		inv.Status, inv.StatusDetails = commandStatusInProgress, commandStatusInProgress
		inv.startedUnix = UnixTimeFloat(timeNow())
	}

	return nil
}

// AgentDeleteMessage drops a message without changing its invocation.
func (b *InMemoryBackend) AgentDeleteMessage(ctx context.Context, messageID string) error {
	_, instanceID, _, err := parseAgentMessageID(messageID)
	if err != nil {
		return err
	}

	b.mu.Lock("AgentDeleteMessage")
	defer b.mu.Unlock()

	b.removeAgentMessageLocked(getRegion(ctx), instanceID, messageID)

	return nil
}

// AgentFailMessage records that the agent could not process a message; its invocation fails.
func (b *InMemoryBackend) AgentFailMessage(ctx context.Context, messageID, _ string) error {
	cmdID, instanceID, cancel, err := parseAgentMessageID(messageID)
	if err != nil {
		return err
	}

	region := getRegion(ctx)

	b.mu.Lock("AgentFailMessage")
	defer b.mu.Unlock()

	b.removeAgentMessageLocked(region, instanceID, messageID)

	if cancel {
		return nil
	}

	if inv := b.agentInvocationLocked(region, cmdID, instanceID); inv != nil && !isTerminalStatus(inv.Status) {
		inv.Status, inv.StatusDetails = commandStatusFailed, commandStatusFailed
		inv.executionEndUnix = UnixTimeFloat(timeNow())
		b.settleCommandLocked(region, cmdID)
	}

	return nil
}

func isTerminalStatus(status string) bool {
	return status != commandStatusPending && status != commandStatusInProgress
}

func (b *InMemoryBackend) agentInvocationLocked(region, cmdID, instanceID string) *CommandInvocation {
	invs := b.commandInvocationsStore(region)[cmdID]
	for i := range invs {
		if invs[i].InstanceID == instanceID && invs[i].agent {
			return &invs[i]
		}
	}

	return nil
}

type agentPluginStatus struct {
	Status         string `json:"Status"`
	Name           string `json:"Name"`
	Output         string `json:"Output"`
	StandardOutput string `json:"StandardOutput"`
	StandardError  string `json:"StandardError"`
	StartDateTime  string `json:"StartDateTime"`
	EndDateTime    string `json:"EndDateTime"`
	Code           int    `json:"Code"`
}

type agentReply struct {
	RuntimeStatus  map[string]*agentPluginStatus `json:"RuntimeStatus"`
	DocumentStatus string                        `json:"DocumentStatus"`
}

// agentStatus maps the agent's ResultStatus to an SSM invocation status and details.
func agentStatus(s string) (string, string) {
	switch s {
	case "Success", "SuccessAndReboot":
		return commandStatusSuccess, commandStatusSuccess
	case "Failed":
		return commandStatusFailed, commandStatusFailed
	case "TimedOut":
		return commandStatusTimedOut, detailsExecutionTimeout
	case "Cancelled", "Cancelling":
		return commandStatusCancelled, commandStatusCancelled
	default:
		return commandStatusInProgress, commandStatusInProgress
	}
}

func agentTime(s string) float64 {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return 0
	}

	return UnixTimeFloat(t)
}

// pluginsFromReply converts the agent's per-plugin results, ordered by plugin key.
func pluginsFromReply(rs map[string]*agentPluginStatus) ([]CommandPlugin, string, string) {
	keys := make([]string, 0, len(rs))
	for k := range rs {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	var (
		plugins     []CommandPlugin
		out, errOut strings.Builder
	)

	for _, k := range keys {
		p := rs[k]
		if p == nil {
			continue
		}

		so, se := p.StandardOutput, p.StandardError
		if so == "" && se == "" {
			so = p.Output
		}

		status, details := agentStatus(p.Status)
		plugins = append(plugins, CommandPlugin{
			Name: cmp.Or(p.Name, k), Status: status, StatusDetails: details,
			Output:                 truncateChars(so+se, pluginOutputChars),
			ResponseCode:           clampInt32(p.Code),
			ResponseStartDateTime:  agentTime(p.StartDateTime),
			ResponseFinishDateTime: agentTime(p.EndDateTime),
		})
		out.WriteString(so)
		errOut.WriteString(se)
	}

	return plugins, truncateChars(out.String(), stdoutLimitChars), truncateChars(errOut.String(), stderrLimitChars)
}

// AgentSendReply applies the agent's runtime status for a sendCommand message to its invocation.
func (b *InMemoryBackend) AgentSendReply(ctx context.Context, messageID, payload string) error {
	cmdID, instanceID, cancel, err := parseAgentMessageID(messageID)
	if err != nil {
		return err
	}

	var reply agentReply
	if uerr := json.Unmarshal([]byte(payload), &reply); uerr != nil {
		return fmt.Errorf("%w: Payload is not a valid reply document", ErrValidationException)
	}

	region := getRegion(ctx)

	b.mu.Lock("AgentSendReply")
	defer b.mu.Unlock()

	inv := b.agentInvocationLocked(region, cmdID, instanceID)
	if inv == nil {
		return ErrCommandNotFound
	}

	if cancel || inv.Status == commandStatusCancelled {
		return nil
	}

	status, details := agentStatus(reply.DocumentStatus)
	plugins, stdout, stderr := pluginsFromReply(reply.RuntimeStatus)

	inv.Status, inv.StatusDetails = status, details
	inv.StandardOutputContent, inv.StandardErrorContent = stdout, stderr
	inv.CommandPlugins = plugins

	if inv.startedUnix == 0 {
		inv.startedUnix = UnixTimeFloat(timeNow())
	}

	if !isTerminalStatus(status) {
		return nil
	}

	inv.ResponseCode = 0

	for _, p := range plugins {
		if p.Status != commandStatusSuccess {
			inv.ResponseCode = p.ResponseCode

			break
		}
	}

	inv.executionEndUnix = UnixTimeFloat(timeNow())
	b.settleCommandLocked(region, cmdID)

	return nil
}
